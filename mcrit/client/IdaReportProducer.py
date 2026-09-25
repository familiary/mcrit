#!/usr/bin/python

import glob
import hashlib
import importlib
import logging
import os
import shutil
import tempfile
from typing import Any, List, Optional, Tuple

from smda.common.SmdaReport import SmdaReport

LOGGER = logging.getLogger(__name__)

IDA_MISSING_MESSAGE = (
    "Headless IDA export requires IDA Pro 9.1+ and the ida-domain package. Install it with 'pip install \"mcrit[ida]\"' and point IDADIR at the IDA 9.1+ installation directory."
)

GO_SEGMENT_MARKERS = {".gopclntab", "__gopclntab", ".go.buildinfo", "__go_buildinfo"}

# debian architecture suffixes of the linux signature bundle, per (processor family, bitness)
DEBARCHS_BY_PROCESSOR = {
    ("x86", 64): ["amd64"],
    ("x86", 32): ["i386"],
    ("arm", 64): ["arm64"],
    ("arm", 32): ["armhf", "armel", "arm"],
}

# architecture suffixes of the windows signature bundle, per (processor family, bitness)
WINDOWS_ARCHS_BY_PROCESSOR = {
    ("x86", 64): "x64",
    ("x86", 32): "x86",
    ("arm", 64): "arm64",
    ("arm", 32): "arm",
}

GOLANG_ARCHS_BY_PROCESSOR = {
    ("x86", 64): "pc",
    ("x86", 32): "pc",
    ("arm", 64): "arm64",
    ("arm", 32): "arm",
}


def _normalizeProcessor(processor: str) -> str:
    normalized = (processor or "").lower()
    if normalized in ("metapc", "x86", "x86_64", "x64", "i386", "8086") or normalized.startswith("80"):
        return "x86"
    if normalized in ("arm", "arm64", "aarch64", "armb"):
        return "arm"
    return ""


def _normalizeFileFormat(file_format: str) -> str:
    normalized = (file_format or "").lower()
    if "portable executable" in normalized or "(pe)" in normalized:
        return "PE"
    if "elf" in normalized:
        return "ELF"
    if "mach-o" in normalized or "macho" in normalized:
        return "MACHO"
    return ""


def _rustArchMatches(triple_arch: str, processor: str, bitness: int) -> bool:
    if processor == "x86":
        return triple_arch == "x86_64" if bitness == 64 else triple_arch in ("i686", "i586")
    if processor == "arm":
        if bitness == 64:
            return triple_arch in ("aarch64", "arm64ec")
        return triple_arch == "arm" or triple_arch.startswith("armv")
    return False


def _rustOsMatches(triple: str, file_format: str) -> bool:
    if file_format == "PE":
        return "windows" in triple
    if file_format == "ELF":
        return "linux" in triple or "android" in triple
    if file_format == "MACHO":
        return "apple" in triple
    return False


def _findSigs(*path_parts: str) -> List[str]:
    return glob.glob(os.path.join(*path_parts), recursive=True)


def selectCandidateSigs(sig_root: str, file_format: str, processor: str, bitness: int, is_go: bool, is_rust: bool) -> List[str]:
    """Pick the FLIRT signatures of a signature bundle that can plausibly match a binary with the given properties."""
    normalized_format = _normalizeFileFormat(file_format)
    normalized_processor = _normalizeProcessor(processor)
    sig_root = os.path.abspath(sig_root)
    candidates = set()
    if normalized_format == "PE":
        windows_arch = WINDOWS_ARCHS_BY_PROCESSOR.get((normalized_processor, bitness))
        if windows_arch:
            candidates.update(_findSigs(sig_root, "windows", "**", f"*_{windows_arch}.sig"))
    elif normalized_format == "ELF":
        for debarch in DEBARCHS_BY_PROCESSOR.get((normalized_processor, bitness), []):
            candidates.update(_findSigs(sig_root, "linux", "**", f"*-{debarch}.sig"))
    if is_go:
        golang_arch = GOLANG_ARCHS_BY_PROCESSOR.get((normalized_processor, bitness))
        if golang_arch:
            candidates.update(_findSigs(sig_root, "golang", "**", f"golang_std_{golang_arch}_*.sig"))
    if is_rust:
        for path in _findSigs(sig_root, "rust", "rust_bundle_*.sig"):
            triple = os.path.basename(path)[len("rust_bundle_") : -len(".sig")]
            if _rustArchMatches(triple.split("-")[0], normalized_processor, bitness) and _rustOsMatches(triple, normalized_format):
                candidates.add(path)
    return sorted(candidates)


def _getSigIndex(ida_funcs: Any, sig_path: str) -> int:
    """IDA identifies a planned signature by its short name, which is the file name with or without the extension."""
    basename = os.path.basename(sig_path)
    accepted = {sig_path.lower(), basename.lower(), os.path.splitext(basename)[0].lower()}
    for index in range(ida_funcs.get_idasgn_qty()):
        signame, _, _ = ida_funcs.get_idasgn_desc_with_matches(index)
        if signame and signame.lower() in accepted:
            return index
    return -1


def applySigs(sig_paths: List[str], min_matches: int) -> List[Tuple[str, int]]:
    """Apply each signature and keep only those reaching min_matches, reverting the others via IDA's undo stack."""
    ida_funcs = importlib.import_module("ida_funcs")
    ida_auto = importlib.import_module("ida_auto")
    ida_undo = importlib.import_module("ida_undo")
    kept = []
    for sig_path in sig_paths:
        label = os.path.basename(sig_path)
        ida_undo.create_undo_point("mcrit:", label)
        if not ida_funcs.plan_to_apply_idasgn(sig_path):
            LOGGER.warning("plan_to_apply_idasgn() failed for %s", sig_path)
            continue
        index = _getSigIndex(ida_funcs, sig_path)
        if index < 0:
            LOGGER.warning("could not locate planned signature %s", sig_path)
            ida_undo.perform_undo()
            continue
        for _ in range(128):
            state = ida_funcs.calc_idasgn_state(index)
            if state in (ida_funcs.IDASGN_APPLIED, ida_funcs.IDASGN_BADARG):
                break
            if not ida_auto.auto_wait():
                break
        _, _, num_matches = ida_funcs.get_idasgn_desc_with_matches(index)
        num_matches = num_matches or 0
        if num_matches < min_matches:
            ida_undo.perform_undo()
            continue
        LOGGER.info("applied signature %s with %d matches", label, num_matches)
        kept.append((sig_path, num_matches))
    return kept


def _getSegmentNames(interface: Any) -> List[str]:
    database = interface.db
    return [database.segments.get_name(segment) for segment in database.segments.get_all()]


def _isGoBinary(interface: Any) -> bool:
    return any(name in GO_SEGMENT_MARKERS for name in _getSegmentNames(interface))


def _isRustBinary(interface: Any) -> bool:
    ida_name = importlib.import_module("ida_name")
    ida_idaapi = importlib.import_module("ida_idaapi")
    return ida_name.get_name_ea(ida_idaapi.BADADDR, "rust_begin_unwind") != ida_idaapi.BADADDR


def _applyCandidateSigs(interface: Any, sig_root: str, min_matches: int) -> None:
    ida_loader = importlib.import_module("ida_loader")
    sig_paths = selectCandidateSigs(
        sig_root,
        ida_loader.get_file_type_name() or "",
        interface.db.architecture or "",
        interface.getBitness(),
        _isGoBinary(interface),
        _isRustBinary(interface),
    )
    applySigs(sig_paths, min_matches)


def produceIdaReport(filepath: str, sig_root: Optional[str] = None, min_matches: int = 10) -> SmdaReport:
    """Disassemble a file through a headless IDA database and return it as an SmdaReport."""
    from smda.Disassembler import Disassembler
    from smda.ida.IdaInterface import IdaInterface
    from smda.SmdaConfig import SmdaConfig

    with tempfile.TemporaryDirectory() as temp_dir:
        # IDA writes its database files next to the input, so it must never see the original sample
        working_copy = os.path.join(temp_dir, os.path.basename(filepath))
        shutil.copyfile(filepath, working_copy)
        interface = None
        try:
            try:
                interface = IdaInterface.fromPath(working_copy, save_on_close=False)
            except ImportError as exc:
                raise ImportError(IDA_MISSING_MESSAGE) from exc
            # fromPath returns a bare backend without registering it, but IdaExporter resolves its
            # backend through the IdaInterface() singleton, which would otherwise open a second database
            IdaInterface.instance = interface
            if sig_root:
                _applyCandidateSigs(interface, sig_root, min_matches)
            report = Disassembler(backend="IDA").disassembleBuffer(interface.getBinary(), 0)
        finally:
            if interface is not None:
                IdaInterface().close()

    with open(filepath, "rb") as input_file:
        report.sha256 = hashlib.file_digest(input_file, "sha256").hexdigest()
    report.filename = os.path.basename(filepath)
    report.binary_size = os.path.getsize(filepath)
    # MongoDbStorage.recalculateAllPicHashes() parses the trailing token as the smda version
    report.smda_version = f"MCRIT4IDA cli via SMDA {SmdaConfig().VERSION}"
    return report
