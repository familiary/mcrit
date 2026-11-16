"""MCRIT_API_COLLECTION_FILES: ApiScout DBs handed to SMDA, so that the WinAPI references of memory dumps are resolved."""

import json
import os
import struct
import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest
from smda.Disassembler import Disassembler
from smda.SmdaConfig import SmdaConfig

from mcrit.client import McritConsole
from mcrit.client.McritClient import McritClient
from mcrit.config import SmdaConfigFactory
from mcrit.config.McritConfig import McritConfig
from mcrit.config.SmdaConfigFactory import createSmdaConfig, getApiCollectionFiles, parseApiCollectionFiles
from mcrit.Worker import Worker

API_ADDRESS = 0x7C801234
BASE_ADDRESS = 0x400000
# push ebp; mov ebp, esp; call dword ptr [0x400100]; pop ebp; ret - and the slot it calls through
DUMP = bytes.fromhex("558bec ff1500014000 5dc3").ljust(0x100, b"\xcc") + struct.pack("<I", API_ADDRESS) + b"\x00" * 0x100


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    monkeypatch.delenv("MCRIT_API_COLLECTION_FILES", raising=False)
    monkeypatch.setattr(SmdaConfigFactory, "_USABLE_API_COLLECTION_FILES", {})


@pytest.fixture
def api_db(tmp_path):
    db = {
        "os_name": "TestOS",
        "dlls": {
            "32_1.0_kernel32.dll_0x7c800000": {
                "base_address": 0x7C800000,
                "aslr_offset": 0,
                "bitness": 32,
                "exports": [{"address": 0x1234, "name": "Sleep", "ordinal": 1}],
            }
        },
    }
    path = tmp_path / "testos.json"
    path.write_text(json.dumps(db))
    return str(path)


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, {}),
        ("", {}),
        (" ; , ", {}),
        ("win7=/dbs/win7.json", {"win7": "/dbs/win7.json"}),
        ("win7=/dbs/win7.json;winxp=/dbs/xp.json", {"win7": "/dbs/win7.json", "winxp": "/dbs/xp.json"}),
        (" win7 = /dbs/win7.json , winxp=/dbs/xp.json ;", {"win7": "/dbs/win7.json", "winxp": "/dbs/xp.json"}),
        # a bare path is named after its file
        ("/dbs/win7_prof-n_sp1_example.json", {"win7_prof-n_sp1_example": "/dbs/win7_prof-n_sp1_example.json"}),
        # only the first "=" separates, and a Windows path keeps its drive
        ("win7=C:\\dbs\\a=b.json", {"win7": "C:\\dbs\\a=b.json"}),
        # half an entry is dropped, the rest is kept
        ("=/dbs/a.json;win7=;winxp=/dbs/xp.json", {"winxp": "/dbs/xp.json"}),
        ("win7=/dbs/old.json;win7=/dbs/new.json", {"win7": "/dbs/new.json"}),
    ],
)
def test_parse(value, expected):
    assert parseApiCollectionFiles(value) == expected


def test_default_is_off():
    assert createSmdaConfig().API_COLLECTION_FILES == {}
    assert createSmdaConfig("").API_COLLECTION_FILES == {}


def test_setting_is_read_from_environment_at_call_time(api_db, monkeypatch):
    # the CLI loads its .env after the modules were imported
    monkeypatch.setenv("MCRIT_API_COLLECTION_FILES", "testos=" + api_db)
    assert createSmdaConfig().API_COLLECTION_FILES == {"testos": api_db}
    # an explicit value, even the empty one, outranks the environment
    assert createSmdaConfig("").API_COLLECTION_FILES == {}


def test_mcrit_config_reads_environment(api_db):
    code = "from mcrit.config.McritConfig import McritConfig; print(McritConfig.API_COLLECTION_FILES)"
    env = {**os.environ, "MCRIT_API_COLLECTION_FILES": "testos=" + api_db}
    assert subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, check=True).stdout.decode().strip() == "testos=" + api_db
    env.pop("MCRIT_API_COLLECTION_FILES")
    assert subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, check=True).stdout.decode().strip() == ""


def test_files_are_set_on_the_instance_only(api_db):
    smda_config = createSmdaConfig({"testos": api_db})
    assert smda_config.API_COLLECTION_FILES == {"testos": api_db}
    assert SmdaConfig.API_COLLECTION_FILES == {}
    assert SmdaConfig().API_COLLECTION_FILES == {}


def test_relative_path_is_made_absolute(api_db, monkeypatch):
    monkeypatch.chdir(os.path.dirname(api_db))
    assert getApiCollectionFiles("testos=testos.json") == {"testos": api_db}


def test_unusable_files_are_dropped_with_one_warning_each(api_db, tmp_path):
    missing = str(tmp_path / "missing.json")
    not_json = tmp_path / "not.json"
    not_json.write_text("MZ")
    not_a_db = tmp_path / "list.json"
    not_a_db.write_text('{"dlls": []}')
    setting = {"missing": missing, "not_json": str(not_json), "not_a_db": str(not_a_db), "testos": api_db}
    # patch the logger rather than using caplog: other test modules call logging.disable(logging.CRITICAL)
    with patch.object(SmdaConfigFactory.LOGGER, "warning") as warning:
        assert getApiCollectionFiles(setting) == {"testos": api_db}
        assert getApiCollectionFiles(setting) == {"testos": api_db}
    assert warning.call_count == 3
    assert sorted(call.args[1] for call in warning.call_args_list) == sorted([missing, str(not_json), str(not_a_db)])


def _worker(setting):
    worker = Worker.__new__(Worker)
    worker.config = McritConfig()
    worker.config.API_COLLECTION_FILES = setting
    return worker


def test_worker_hands_files_to_every_disassembly(api_db):
    worker = _worker("testos=" + api_db)
    worker._storage = MagicMock()
    worker._storage.getSampleBySha256.return_value = None
    worker._storage_config = MagicMock()
    calls = {
        "addBinarySample (dump)": lambda: worker.addBinarySample(b"\x90", "dump", "family", "", True, 0x1000, 32),
        "addBinarySample (file)": lambda: worker.addBinarySample(b"\x90", "file", "family", "", False, 0, None),
        "getMatchesForMappedBinary": lambda: worker.getMatchesForMappedBinary(b"\x90", 0x1000),
        "getMatchesForUnmappedBinary": lambda: worker.getMatchesForUnmappedBinary(b"\x90"),
    }
    for name, call in calls.items():
        with patch("mcrit.Worker.Disassembler") as disassembler, patch("mcrit.Worker.MatcherQuery"), patch.object(Worker, "_addReport"), patch.object(Worker, "_asJobResult"):
            call()
        assert disassembler.call_args.args[0].API_COLLECTION_FILES == {"testos": api_db}, name


def test_worker_without_setting_uses_plain_smda_config(api_db, monkeypatch):
    assert _worker("")._createSmdaConfig().API_COLLECTION_FILES == {}
    # a config object from before the setting existed: the environment decides
    monkeypatch.setenv("MCRIT_API_COLLECTION_FILES", api_db)
    assert Worker.__new__(Worker)._createSmdaConfig().API_COLLECTION_FILES == {"testos": api_db}


def test_client_and_cli_hand_files_to_local_disassembly(api_db, monkeypatch, tmp_path):
    monkeypatch.setenv("MCRIT_API_COLLECTION_FILES", "testos=" + api_db)
    client = McritClient()
    with patch.object(McritClient, "requestMatchesForSmdaReport"), patch("mcrit.client.McritClient.Disassembler") as disassembler:
        client.requestMatchesForMappedBinary(b"\x90", 0x1000)
        assert disassembler.call_args.args[0].API_COLLECTION_FILES == {"testos": api_db}
        disassembler.reset_mock()
        client.requestMatchesForUnmappedBinary(b"\x90")
        assert disassembler.call_args.args[0].API_COLLECTION_FILES == {"testos": api_db}
    dump_path = tmp_path / "dump_0x00400000"
    dump_path.write_bytes(DUMP)
    with patch.object(McritConsole, "Disassembler") as disassembler:
        McritConsole.getSmdaReportFromFilepath(MagicMock(smda=False, executables_only=False, mode="file", family=None, version=None, library=False, output=None), str(dump_path))
    assert disassembler.call_args.args[0].API_COLLECTION_FILES == {"testos": api_db}


def test_dump_gets_api_reference_end_to_end(api_db):
    plain = Disassembler(createSmdaConfig("")).disassembleBuffer(DUMP, BASE_ADDRESS, 32)
    resolved = Disassembler(createSmdaConfig("testos=" + api_db)).disassembleBuffer(DUMP, BASE_ADDRESS, 32)
    assert plain.status == resolved.status == "ok"
    plain_function, resolved_function = plain.getFunction(BASE_ADDRESS), resolved.getFunction(BASE_ADDRESS)
    assert plain_function is not None and resolved_function is not None
    assert plain_function.apirefs == {}
    assert list(resolved_function.apirefs.items()) == [(BASE_ADDRESS + 3, "kernel32.dll_0x7c800000!Sleep")]
    # the reference is an annotation here: the recovered code is the same
    assert plain_function.pic_hash == resolved_function.pic_hash
    assert plain_function.num_instructions == resolved_function.num_instructions
