"""Builds the SmdaConfig every MCRIT disassembly runs with.

All places that disassemble - the worker's submission and query jobs, the client's local
disassembly and the CLI - get their SmdaConfig from createSmdaConfig(), so that a sample is
disassembled the same way whether it is submitted or queried.
"""

import json
import logging
import os
import re
from typing import Dict, Mapping, Optional, Union

from smda.SmdaConfig import SmdaConfig

LOGGER = logging.getLogger(__name__)

API_COLLECTION_FILES_ENV = "MCRIT_API_COLLECTION_FILES"

# a file is checked once per process: smda caches a parsed DB per path for the life of the process too
_USABLE_API_COLLECTION_FILES: Dict[str, bool] = {}


def parseApiCollectionFiles(value: Optional[str]) -> Dict[str, str]:
    """Parse MCRIT_API_COLLECTION_FILES into {os_name: filepath}, without touching the file system.

    Entries are separated by ";" or ",". An entry is "os_name=path", or a bare path, which is
    named after its file (".json" dropped). A name given twice keeps its last path.
    """
    api_collection_files: Dict[str, str] = {}
    for entry in re.split(r"[;,]", value or ""):
        entry = entry.strip()
        if not entry:
            continue
        os_name, separator, filepath = entry.partition("=")
        if not separator:
            filepath = entry
            os_name = os.path.splitext(os.path.basename(entry))[0]
        os_name, filepath = os_name.strip(), filepath.strip()
        if not os_name or not filepath:
            LOGGER.warning('%s: ignoring entry "%s", expected "os_name=path" or a path.', API_COLLECTION_FILES_ENV, entry)
            continue
        api_collection_files[os_name] = filepath
    return api_collection_files


def _isUsableApiCollectionFile(filepath: str) -> bool:
    if filepath not in _USABLE_API_COLLECTION_FILES:
        problem = None
        if not os.path.isfile(filepath):
            problem = "file not found"
        else:
            try:
                with open(filepath, encoding="utf-8") as f_json:
                    api_db = json.load(f_json)
                if not isinstance(api_db, dict) or not isinstance(api_db.get("dlls"), dict):
                    problem = 'not an ApiScout DB (no "dlls" object)'
            except (OSError, ValueError) as exc:
                problem = f"not readable as JSON ({exc})"
        if problem:
            LOGGER.warning('Ignoring ApiScout DB "%s": %s. Memory dumps are disassembled without WinAPI resolution from it.', filepath, problem)
        _USABLE_API_COLLECTION_FILES[filepath] = problem is None
    return _USABLE_API_COLLECTION_FILES[filepath]


def getApiCollectionFiles(api_collection_files: Union[str, Mapping[str, str], None] = None) -> Dict[str, str]:
    """The usable ApiScout DBs as {os_name: absolute filepath}.

    Takes the setting as its string form or as a mapping; None reads MCRIT_API_COLLECTION_FILES
    from the environment now. A file that is missing or is not an ApiScout DB is dropped with a
    warning instead of failing the disassembly that asked.
    """
    if api_collection_files is None:
        api_collection_files = os.getenv(API_COLLECTION_FILES_ENV, "")
    if isinstance(api_collection_files, str):
        api_collection_files = parseApiCollectionFiles(api_collection_files)
    usable_files = {}
    for os_name, filepath in api_collection_files.items():
        filepath = os.path.abspath(os.path.expanduser(filepath))
        if _isUsableApiCollectionFile(filepath):
            usable_files[os_name] = filepath
    return usable_files


def createSmdaConfig(api_collection_files: Union[str, Mapping[str, str], None] = None) -> SmdaConfig:
    """A SmdaConfig carrying the configured ApiScout DBs, which smda resolves WinAPI references of memory dumps with.

    Without any (the default) this is a plain SmdaConfig(). See getApiCollectionFiles() for the argument.
    """
    smda_config = SmdaConfig()
    # set on the instance: API_COLLECTION_FILES is a class attribute of SmdaConfig, shared by all of them
    smda_config.API_COLLECTION_FILES = getApiCollectionFiles(api_collection_files)
    return smda_config
