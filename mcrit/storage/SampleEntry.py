import datetime
import json
from typing import TYPE_CHECKING, Any, Dict, Optional

from mcrit.libs.utility import decode_two_complement, encode_two_complement

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
    smda_version: str
    summary: Optional[str]
    statistics: Dict[str, int]
    timestamp: Optional[datetime.datetime]
    # everything else the SMDA report carried, so it can be rebuilt from storage (#94)
    smda_extras: Dict[str, Any]

    # TODO -> rename to fromSmdaReport
    def __init__(self, smda_report: Optional["SmdaReport"], sample_id=-1, family_id=0):
        self.sample_id = sample_id
        self.family_id = family_id
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
            self.smda_extras = self._extrasOf(smda_report.toDict())
        else:
            self.smda_extras = {}

    # the report fields SampleEntry represents in fields of its own, or that are the functions
    _REPORT_FIELDS_HELD_ELSEWHERE = ("architecture", "base_addr", "binary_size", "bitness", "metadata", "sha256", "smda_version", "statistics", "timestamp", "xcfg")
    _METADATA_FIELDS_HELD_ELSEWHERE = ("binweight", "component", "family", "filename", "is_library", "version")

    @classmethod
    def _extrasOf(cls, report_dict: Dict[str, Any]) -> Dict[str, Any]:
        # as JSON would carry them: the data references are keyed by integer addresses, which
        # a report file cannot hold and MongoDB refuses; SmdaReport.fromDict reads the string
        # keys back, exactly as it does for a report loaded from disk
        extras = json.loads(json.dumps({key: value for key, value in report_dict.items() if key not in cls._REPORT_FIELDS_HELD_ELSEWHERE}))
        metadata = report_dict.get("metadata") or {}
        # an unset metadata value (None) is what SmdaReport emits for a field the report never
        # had; handing it back explicitly would make fromDict normalise it into a value
        extras["metadata"] = {key: value for key, value in metadata.items() if key not in cls._METADATA_FIELDS_HELD_ELSEWHERE and value is not None}
        return extras

    def toSmdaReportDict(self, xcfg: Dict[int, Dict[str, Any]]) -> Dict[str, Any]:
        """The SMDA report this sample came from, as SmdaReport.fromDict() reads it, given the
        functions' xcfg by offset. Samples stored before the extras were kept get the defaults
        of an empty report for what was not recorded (#94)."""
        extras = dict(self.smda_extras or {})
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
            "smda_version": self.smda_version,
            "statistics": self.statistics,
            "timestamp": self.timestamp.strftime("%Y-%m-%dT%H-%M-%S") if self.timestamp is not None else None,
            "version": self.version,
            "smda_extras": self.smda_extras,
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
        sample_entry.smda_version = entry_dict["smda_version"]
        sample_entry.statistics = entry_dict["statistics"]
        sample_entry.timestamp = datetime.datetime.strptime(entry_dict["timestamp"], "%Y-%m-%dT%H-%M-%S")
        # samples stored before #94 carry no extras
        sample_entry.smda_extras = entry_dict.get("smda_extras") or {}
        return sample_entry

    def __str__(self):
        return "Sample {} ({}, {} bit) - {} ({}): ".format(self.sample_id, self.architecture, self.bitness, self.filename, self.family)

    def __hash__(self):
        """Override the default hash behavior"""
        return hash(f"{self.sample_id}_{self.sha256}")
