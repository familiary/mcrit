from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

import smda.common.SmdaFunction as smda_function_module
from smda.common.BinaryInfo import BinaryInfo
from smda.common.SmdaFunction import SmdaFunction

from mcrit.libs.utility import decode_two_complement, encode_two_complement
from mcrit.minhash.MinHash import MinHash
from mcrit.storage.FunctionLabelEntry import FunctionLabelEntry

if TYPE_CHECKING:  # pragma: no cover
    from smda.common.SmdaReport import SmdaReport

    from mcrit.storage.SampleEntry import SampleEntry

# smda publishes the fields SmdaFunction.fromDict requires since 4.4.5; releases before that read the
# same keys without checking for them first, so the sets below stand in for them there
REQUIRED_FUNCTION_FIELDS = getattr(smda_function_module, "REQUIRED_FUNCTION_FIELDS", frozenset({"offset", "blocks", "apirefs", "blockrefs", "inrefs", "outrefs", "metadata"}))
REQUIRED_FUNCTION_METADATA = getattr(
    smda_function_module,
    "REQUIRED_FUNCTION_METADATA",
    frozenset({"binweight", "characteristics", "confidence", "function_name", "strongly_connected_components", "tfidf"}),
)


def missingXcfgFields(xcfg: Dict) -> List[str]:
    """The fields SmdaFunction.fromDict requires that a stored xcfg lacks, metadata ones as ``metadata.<name>``.

    smda has required the same fields since 4.4.5 (and indexed the same ones directly before
    that), and SmdaFunction.toDict has written every one of them since smda 1.2, so an xcfg any
    smda stored is complete - the smda 1.5.12 reports under tests/ included. A non-empty answer
    means the blob did not come from smda's toDict.
    """
    missing = sorted(REQUIRED_FUNCTION_FIELDS.difference(xcfg))
    metadata = xcfg.get("metadata")
    if isinstance(metadata, dict):
        missing.extend(f"metadata.{field}" for field in sorted(REQUIRED_FUNCTION_METADATA.difference(metadata)))
    return missing


def smdaFunctionFromXcfg(xcfg: Optional[Dict], binary_info: Optional[BinaryInfo] = None) -> Optional[SmdaFunction]:
    """Rebuild the SmdaFunction a stored xcfg describes; None when the function has no stored disassembly.

    Every path that rebuilds functions from storage goes through here. A function can be stored
    without its disassembly: STORAGE_DROP_DISASSEMBLY removes it once the sample is hashed, a
    blob over MongoDB's 16 MiB document limit is dropped at insert (#42), and the readers decode
    a missing blob to ``{}``. smda cannot rebuild a function from ``{}`` - it raises "serialized
    function is incomplete" - so the callers skip such a function instead of failing the whole
    batch it came in. An xcfg that is present but lacks a field smda requires still raises, and
    names the fields, because that is not a state MCRIT writes - but an import stores whatever the
    export holds, so the jobs that rehash the corpus log and skip a function that raises here.
    """
    if not xcfg:
        return None
    missing = missingXcfgFields(xcfg)
    if missing:
        raise ValueError(f"stored disassembly of the function at offset {xcfg.get('offset')} lacks {', '.join(missing)}, which SmdaFunction.fromDict requires")
    return SmdaFunction.fromDict(xcfg, binary_info=binary_info)


def picBlockHashEntries(blockhasher, smda_function: SmdaFunction, lower_addr: int, upper_addr: int) -> List[Dict]:
    """The picblockhashes of a function's blocks as storage keeps them: each block's offset tuple with its hash."""
    entries = []
    for hash_entry in blockhasher.getBlockhashesForFunction(smda_function, lower_addr, upper_addr, hash_size=8):
        for block_entry in hash_entry["offset_tuples"]:
            block_entry["hash"] = hash_entry["hash"]
            entries.append(block_entry)
    return entries


# The labels of a function that differ from its current name, which a plain search term matches
# besides the name (#301). MongoDB keeps them in this derived, indexed field of the function
# document, absent when there are none; it is storage-only and never part of a FunctionEntry.
OTHER_LABELS_FIELD = "_other_labels"


def otherLabels(function_name: Optional[str], function_labels: Iterable[Any]) -> List[str]:
    """The distinct label strings that differ from the function name, in the order first given.

    MongoDbStorage computes the same set on the server (_OTHER_LABELS_STAGE) for the writes that
    change a function's labels or its name, as $setDifference does: compared exactly, case and all.
    """
    others: List[str] = []
    for label in function_labels:
        if isinstance(label, str) and label != function_name and label not in others:
            others.append(label)
    return others


def isValidPicHash(value) -> bool:
    """Whether a value can be stored as a pichash or picblockhash: an int of 0..2**64-1.

    That is what smda and picblocks compute, and all MongoDbStorage reads back (#145): it wrote a
    negative value as "0x-000000000000005", which no reader decodes, one of 2**64 or more wider
    than the 16 digits its ordering relies on, and an operator dict as a sub-document. A bool is
    an int, but no pichash.
    """
    return type(value) is int and 0 <= value < 1 << 64


def checkSubmittedPicHashes(smda_report: "SmdaReport") -> None:
    """Refuse a report stating a pic_hash that cannot be stored, before any of it is written.

    smda keeps the pic_hash a current report states and recomputes it only for older ones, so it
    is whatever was submitted. A function without one is stored with 0, as FunctionEntry does.
    """
    for smda_function in smda_report.getFunctions():
        if not isValidPicHash(smda_function.pic_hash or 0):
            raise ValueError(f"the function at offset {smda_function.offset} states a pic_hash that is not an integer of 0..2**64-1")


# Dataclass, post init
# constructor -> .fromSmdaFunction
# assume sample_entry, smda_function always available


class FunctionEntry:
    # MCRIT specific
    function_id: int
    family_id: int
    sample_id: int
    minhash: bytes
    minhash_shingle_composition: Optional[Dict]
    # inherited from sample
    architecture: str
    # smda information
    function_name: Optional[str]
    function_labels: list
    matches: Dict
    pichash: int
    picblockhashes: list
    num_blocks: int
    num_instructions: int
    binweight: float
    offset: int
    xcfg: Optional[Dict]
    # free labels an analyst attached, normalised by mcrit.libs.tags (#53)
    tags: List[str]

    def __init__(
        self,
        sample_entry: Optional["SampleEntry"],
        smda_function: Optional["SmdaFunction"],
        function_id: int,
        minhash: Optional[MinHash] = None,
    ) -> None:
        self.function_id = function_id
        if sample_entry:
            self.family_id = sample_entry.family_id
            self.sample_id = sample_entry.sample_id
            self.architecture = sample_entry.architecture
        if smda_function:
            self.num_blocks = smda_function.num_blocks
            self.num_instructions = smda_function.num_instructions
            self.binweight = smda_function.binweight
            self.offset = smda_function.offset or 0
            self.xcfg = smda_function.toDict()
            self.function_name = smda_function.function_name
            self.pichash = smda_function.pic_hash or 0
            self.picblockhashes = []
        self.function_labels = []
        self.tags = []
        self.matches = {}
        empty_minhash = MinHash()
        self.minhash = minhash.getMinHash() if minhash else empty_minhash.getMinHash()
        self.minhash_shingle_composition = minhash.getComposition() if minhash else empty_minhash.getComposition()

    def getOtherLabels(self) -> List[str]:
        """The labels that differ from the current name (see otherLabels)."""
        return otherLabels(getattr(self, "function_name", None), (label.function_label for label in self.function_labels))

    def getMinHash(self, minhash_bits=32):
        return MinHash(function_id=self.function_id, minhash_bytes=self.minhash, minhash_bits=minhash_bits)

    def toSmdaFunction(self) -> Optional[SmdaFunction]:
        """The SmdaFunction this entry's disassembly describes; None when it carries none."""
        binary_info = BinaryInfo(b"")
        binary_info.architecture = self.architecture
        return smdaFunctionFromXcfg(self.xcfg, binary_info)

    def toDict(self):
        empty_minhash = MinHash()
        minhash = self.minhash if self.minhash else empty_minhash.getMinHash()
        shingler_composition = self.minhash_shingle_composition if self.minhash_shingle_composition else empty_minhash.getComposition()
        function_entry = {
            "architecture": self.architecture,
            "binweight": self.binweight,
            "family_id": self.family_id,
            "function_id": self.function_id,
            "function_name": self.function_name,
            "function_labels": [label.toDict() for label in self.function_labels],
            "matches": self.matches,
            "minhash": minhash.hex(),
            "minhash_shingle_composition": shingler_composition,
            "num_blocks": self.num_blocks,
            "num_instructions": self.num_instructions,
            "offset": encode_two_complement(self.offset),
            "pichash": self.pichash,
            "picblockhashes": self.picblockhashes,
            "sample_id": self.sample_id,
            "xcfg": self.xcfg,
        }
        # most functions carry no tags, and a whole-corpus export or report holds millions of them,
        # so the key is left out while empty; fromDict reads it missing as no tags (#53)
        if self.tags:
            function_entry["tags"] = list(self.tags)
        return function_entry

    @classmethod
    def fromDict(cls, entry_dict):
        function_entry = cls(None, None, entry_dict["function_id"])
        function_entry.family_id = entry_dict["family_id"]
        # function_entry.function_id = entry_dict["function_id"]
        function_entry.sample_id = entry_dict["sample_id"]
        function_entry.architecture = entry_dict["architecture"]
        function_entry.function_name = entry_dict["function_name"]
        function_entry.function_labels = [FunctionLabelEntry.fromDict(label) for label in entry_dict["function_labels"]] if "function_labels" in entry_dict else []
        for label in function_entry.function_labels:
            label.setFunctionId(entry_dict["function_id"])
        function_entry.matches = entry_dict["matches"]
        function_entry.pichash = entry_dict["pichash"]
        function_entry.picblockhashes = entry_dict["picblockhashes"]
        function_entry.minhash = bytes.fromhex(entry_dict["minhash"])
        function_entry.minhash_shingle_composition = entry_dict["minhash_shingle_composition"]
        function_entry.num_blocks = entry_dict["num_blocks"]
        function_entry.num_instructions = entry_dict["num_instructions"]
        function_entry.binweight = entry_dict["binweight"]
        function_entry.offset = decode_two_complement(entry_dict["offset"])
        function_entry.xcfg = entry_dict["xcfg"] if "xcfg" in entry_dict else None
        # functions stored before #53 carry no tags, and MongoDbStorage leaves the field out while empty
        function_entry.tags = list(entry_dict.get("tags") or [])
        return function_entry

    def __str__(self):
        return "Family: {} Sample: {} Function: {} @ 0x{:08x} - {} blocks ({} hashes), {} instructions - pichash: {}".format(
            self.family_id,
            self.sample_id,
            self.function_id,
            self.offset,
            self.num_blocks,
            len(self.picblockhashes),
            self.num_instructions,
            self.pichash,
        )
