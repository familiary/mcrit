import datetime
import json
import re
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from mcrit.libs.utility import decode_two_complement, encode_two_complement

# how a sample's timestamp is stored and sent; its text order is the chronological order
TIMESTAMP_FORMAT = "%Y-%m-%dT%H-%M-%S"

if TYPE_CHECKING:  # pragma: no cover
    from smda.common.SmdaReport import SmdaReport


# @dataclass
class SampleEntry:
    family_id: int
    sample_id: int
    architecture: str
    base_addr: int
    binary_size: int
    binweight: float
    bitness: int
    component: str
    family: str
    version: str
    is_library: bool
    filename: str
    sha256: str
    # True: MCRIT disassembled the sample from bytes of this sha256 itself; False: the sha256 is
    # what a submitted or imported report states; None: not known, a sample stored before this was
    # recorded, or a query sample (#270)
    sha256_verified: Optional[bool]
    smda_version: str
    summary: Optional[str]
    statistics: Dict[str, int]
    timestamp: Optional[datetime.datetime]
    # free labels an analyst attached, normalised by mcrit.libs.tags (#53)
    tags: List[str]

    SHA256_PATTERN = re.compile(r"[0-9a-fA-F]{64}")

    @staticmethod
    def isValidSha256(sha256) -> bool:
        """Whether a sha256 a submission states can identify a sample: 64 hex digits. smda copies the
        field of a report unchecked, and MongoDB reads an object in its place as a query operator."""
        return isinstance(sha256, str) and SampleEntry.SHA256_PATTERN.fullmatch(sha256) is not None

    def __init__(self, smda_report: Optional["SmdaReport"], sample_id=-1, family_id=0, sha256_verified: Optional[bool] = None):
        self.sample_id = sample_id
        self.family_id = family_id
        self.sha256_verified = sha256_verified
        self.tags = []
        if smda_report:
            self.architecture = smda_report.architecture or ""
            self.base_addr = smda_report.base_addr or 0
            self.binary_size = smda_report.binary_size or 0
            self.binweight = smda_report.binweight or 0
            self.bitness = smda_report.bitness or 0
            self.component = smda_report.component or ""
            self.family = smda_report.family or ""
            self.filename = smda_report.filename or ""
            self.is_library = bool(smda_report.is_library)
            self.sha256 = smda_report.sha256 or ""
            self.smda_version = smda_report.smda_version or ""
            self.statistics = smda_report.statistics.toDict() if smda_report.statistics is not None else {}
            self.timestamp = smda_report.timestamp
            self.version = smda_report.version or ""

    # the report fields SampleEntry represents in fields of its own, or that are the functions
    _REPORT_FIELDS_HELD_ELSEWHERE = ("architecture", "base_addr", "binary_size", "bitness", "metadata", "sha256", "smda_version", "statistics", "timestamp", "xcfg")
    _METADATA_FIELDS_HELD_ELSEWHERE = ("binweight", "component", "family", "filename", "is_library", "version")
    # smda's `buffer` is the analysed binary itself (a report made with smda's STORE_BUFFER). It is
    # never kept with the extras: the bytes a sample was submitted as are kept and handed out only
    # under STORAGE_KEEP_/STORAGE_SERVE_SUBMITTED_BINARIES (#95), never by GET /samples/{id}/smda
    _REPORT_FIELDS_NOT_KEPT = ("buffer",)

    @classmethod
    def smdaExtrasOf(cls, smda_report: "SmdaReport") -> Dict[str, Any]:
        """The SMDA report's fields this entry and the functions hold no field for (#94), but its buffer.

        Never part of the entry itself: the whole-binary data references and xmetadata grow with
        the binary and have no cap, and the entry is the wire format of every sample listing, job
        result and export. The storage keeps them apart, beside the functions' disassembly.
        """
        report_dict = smda_report.toDict()
        # as JSON would carry them: the data references are keyed by integer addresses, which
        # a report file cannot hold and MongoDB refuses; SmdaReport.fromDict reads the string
        # keys back, exactly as it does for a report loaded from disk
        left_out = cls._REPORT_FIELDS_HELD_ELSEWHERE + cls._REPORT_FIELDS_NOT_KEPT
        extras = json.loads(json.dumps({key: value for key, value in report_dict.items() if key not in left_out}))
        metadata = report_dict.get("metadata") or {}
        # an unset metadata value (None) is what SmdaReport emits for a field the report never
        # had; handing it back explicitly would make fromDict normalise it into a value
        extras["metadata"] = {key: value for key, value in metadata.items() if key not in cls._METADATA_FIELDS_HELD_ELSEWHERE and value is not None}
        return extras

    def toSmdaReportDict(self, xcfg: Dict[int, Dict[str, Any]], smda_extras: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """The SMDA report this sample came from, as SmdaReport.fromDict() reads it, given the
        functions' xcfg by offset and the extras from smdaExtrasOf(). Without extras (a sample
        stored before they were kept) the defaults of an empty report fill in (#94)."""
        # extras stored before the buffer was left out may still carry one
        extras = {key: value for key, value in (smda_extras or {}).items() if key not in self._REPORT_FIELDS_NOT_KEPT}
        metadata = dict(extras.pop("metadata", None) or {})
        metadata.update(
            {"binweight": self.binweight, "component": self.component, "family": self.family, "filename": self.filename, "is_library": self.is_library, "version": self.version}
        )
        report_dict: Dict[str, Any] = {
            "code_areas": [],
            "confidence_threshold": 0,
            "disassembly_errors": {},
            "execution_time": 0,
            "identified_alignment": 0,
            "message": "",
            "status": "ok",
        }
        report_dict.update(extras)
        report_dict.update(
            {
                "architecture": self.architecture,
                "base_addr": self.base_addr,
                "binary_size": self.binary_size,
                "bitness": self.bitness,
                "metadata": metadata,
                "sha256": self.sha256,
                "smda_version": self.smda_version,
                "statistics": self.statistics,
                "timestamp": self.timestamp.strftime("%Y-%m-%dT%H-%M-%S") if self.timestamp is not None else "",
                "xcfg": {int(offset): function_dict for offset, function_dict in xcfg.items()},
            }
        )
        return report_dict

    def getShortSha256(self, prefix=8, border=0):
        if border > 0:
            return self.sha256[:border] + "..." + self.sha256[-border:]
        elif prefix > 0:
            return self.sha256[:prefix]
        return self.sha256

    def getShortFilename(self, size_visible=20):
        if len(self.filename) > 2 * size_visible:
            return self.filename[:size_visible] + "..." + self.filename[-size_visible:]
        return self.filename

    def toDict(self):
        sample_entry = {
            "architecture": self.architecture,
            "base_addr": encode_two_complement(self.base_addr),
            "binary_size": self.binary_size,
            "binweight": self.binweight,
            "bitness": self.bitness,
            "component": self.component,
            "family_id": self.family_id,
            "family": self.family,
            "filename": self.filename,
            "is_library": self.is_library,
            "sample_id": self.sample_id,
            "sha256": self.sha256,
            "sha256_verified": self.sha256_verified,
            "smda_version": self.smda_version,
            "statistics": self.statistics,
            "tags": list(self.tags),
            "timestamp": self.timestamp.strftime(TIMESTAMP_FORMAT) if self.timestamp is not None else None,
            "version": self.version,
        }
        return sample_entry

    @classmethod
    def fromDict(cls, entry_dict):
        sample_entry = cls(None)
        sample_entry.family_id = entry_dict["family_id"]
        sample_entry.sample_id = entry_dict["sample_id"]
        sample_entry.architecture = entry_dict["architecture"]
        sample_entry.base_addr = decode_two_complement(entry_dict["base_addr"])
        sample_entry.binary_size = entry_dict["binary_size"]
        sample_entry.binweight = entry_dict["binweight"]
        sample_entry.bitness = entry_dict["bitness"]
        sample_entry.component = entry_dict["component"]
        sample_entry.family = entry_dict["family"]
        sample_entry.version = entry_dict["version"]
        sample_entry.is_library = entry_dict["is_library"]
        sample_entry.filename = entry_dict["filename"]
        sample_entry.sha256 = entry_dict["sha256"]
        # a sample stored, or an entry sent by a server, from before this was recorded has none
        sample_entry.sha256_verified = entry_dict.get("sha256_verified")
        sample_entry.smda_version = entry_dict["smda_version"]
        # a dict, empty for a report without statistics; an export is data from elsewhere, and
        # statistics of another type it states are read as none
        statistics = entry_dict.get("statistics")
        sample_entry.statistics = statistics if isinstance(statistics, dict) else {}
        sample_entry.timestamp = cls._parseTimestamp(entry_dict.get("timestamp"))
        # samples stored before #53 carry no tags
        sample_entry.tags = list(entry_dict.get("tags") or [])
        return sample_entry

    @staticmethod
    def _parseTimestamp(timestamp) -> Optional[datetime.datetime]:
        """The timestamp as toDict writes it, or None for a sample that has none.

        smda reads a report whose timestamp is missing or empty as None, which toDict stores as
        null, and strftime before Python 3.14 writes a year below 1000 with fewer digits than
        strptime reads back.
        Every reader of a stored sample - listings, exports, matching, deleteSample, the query
        cleanup - comes through here, so such a value is read as no timestamp instead of failing
        all of them.
        """
        if not timestamp:
            return None
        try:
            return datetime.datetime.strptime(timestamp, TIMESTAMP_FORMAT)
        except (TypeError, ValueError):
            return None

    def __str__(self):
        return "Sample {} ({}, {} bit) - {} ({}): ".format(self.sample_id, self.architecture, self.bitness, self.filename, self.family)

    def __hash__(self):
        """Override the default hash behavior"""
        return hash(f"{self.sample_id}_{self.sha256}")
