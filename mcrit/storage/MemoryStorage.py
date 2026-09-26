import datetime
import functools
import logging
import operator
import re
import uuid
from collections import defaultdict
from copy import copy, deepcopy
from itertools import zip_longest
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Tuple, Union

from packaging import version as packaging_version
from picblocks.blockhasher import BlockHasher

from mcrit.index.SearchCursor import FullSearchCursor
from mcrit.index.SearchQueryTree import AndNode, BaseVisitor, FilterSingleElementLists, NodeType, OrNode, PropagateNot, SearchConditionNode, SearchFieldResolver
from mcrit.minhash.MinHash import MinHash
from mcrit.storage.FamilyEntry import FamilyEntry
from mcrit.storage.FunctionEntry import FunctionEntry
from mcrit.storage.FunctionLabelEntry import FunctionLabelEntry
from mcrit.storage.MatchingCache import MatchingCache, StorageBackedMatchingCache
from mcrit.storage.SampleEntry import SampleEntry
from mcrit.storage.StorageInterface import StorageInterface

if TYPE_CHECKING:  # pragma: no cover
    from smda.common.SmdaFunction import SmdaFunction
    from smda.common.SmdaReport import SmdaReport

    from mcrit.config.McritConfig import McritConfig

LOGGER = logging.getLogger(__name__)


# Helper Methods for search
def _get_field_once(object, field):
    try:
        return getattr(object, field)
    except Exception:
        pass
    try:
        return object[field]
    except Exception:
        pass


def _get_field(object, field):
    dot_index = field.find(".")
    if dot_index >= 0:
        first_field = field[:dot_index]
        remaining_fields = field[dot_index + 1 :]
        return _get_field(_get_field_once(object, first_field), remaining_fields)
    else:
        return _get_field_once(object, field)


class MemorySearchTranspiler(BaseVisitor):
    """
    Converts a tree to a filter function that can be applied to an entry, returning True or False.
    The input tree MUST NOT contain Not or SearchTerm nodes.
    """

    def visitAndNode(self, node: AndNode) -> Callable:
        visited_children: List[Callable] = [self.visit(child) for child in node.children]

        def and_function(entry):
            for function in visited_children:
                if not function(entry):
                    return False
            return True

        return and_function

    def visitOrNode(self, node: OrNode) -> Callable:
        visited_children: List[Callable] = [self.visit(child) for child in node.children]

        def or_function(entry):
            for function in visited_children:
                if function(entry):
                    return True
            return False

        return or_function

    def visitSearchConditionNode(self, node: SearchConditionNode) -> Callable:
        string_to_operator = {
            "<": operator.lt,
            "<=": operator.le,
            ">": operator.gt,
            ">=": operator.ge,
            "=": operator.eq,
            "": operator.eq,
            "!=": operator.ne,
        }
        # NOTE: the substring operators "?" / "!?" are handled by the early return below
        # and deliberately have no entry here.
        value = node.value
        if node.operator.endswith("?"):
            regex = re.compile(re.escape(node.value), re.IGNORECASE)
            inverse = node.operator == "!?"

            def check_regex(entry):
                value = _get_field(entry, node.field)
                if not isinstance(value, str):
                    return False
                return (regex.search(value) is not None) ^ inverse

            return check_regex

        if node.field in ("pichash", "offset") or node.field.endswith("_id") or "num_" in node.field:
            try:
                value = int(value, 0)
            except Exception:
                pass

        chosen_operator = string_to_operator[node.operator]

        def compare(entry):
            return chosen_operator(_get_field(entry, node.field), value)

        return compare


class MemoryStorage(StorageInterface):
    _families: Dict[int, FamilyEntry]
    _samples: Dict[int, "SampleEntry"]
    _functions: Dict[int, "FunctionEntry"]
    _query_samples: Dict[int, "SampleEntry"]
    _query_functions: Dict[int, "FunctionEntry"]
    _pichashes: Dict[int, Set[Tuple[int, int, int]]]
    # Dict[band_number, Dict[?, List[function_id]]]
    _bands: Dict[int, Dict[int, List[int]]]
    _counters: Dict[str, int]
    _sample_by_sha256: Dict[str, int]
    _sample_id_to_function_ids: Dict[int, List[int]]

    def __init__(self, config: "McritConfig") -> None:
        super().__init__(config)  # sets config
        self._setupEmptyStorage()
        self.blockhasher = BlockHasher()

    def clearStorage(self) -> None:
        self._setupEmptyStorage()

    def _setupEmptyStorage(self) -> None:
        self._mcrit_db_id = str(uuid.uuid4())
        self._db_state = 0
        self._db_timestamp = self._getCurrentTimestamp()
        self._families = {}
        self._samples = {}
        self._functions = {}
        self._query_samples = {}
        self._query_functions = {}
        self._pichashes = {}
        self._bands = {band_number: {} for band_number in range(self._storage_config.STORAGE_NUM_BANDS)}
        self._minhash_versions: Dict[int, str] = {}
        self._counters = defaultdict(lambda: 0)
        # initialize query sample/function ids
        if self._counters["query_samples"] == 0:
            self._counters["query_samples"] += 1
        if self._counters["query_functions"] == 0:
            self._counters["query_functions"] += 1
        # caches
        self._sample_by_sha256 = {}
        self._sample_id_to_function_ids = defaultdict(list)
        unknown_family_id = self.addFamily("")
        assert unknown_family_id == 0

    def _getCurrentTimestamp(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.UTC).replace(tzinfo=None)

    def _updateDbState(self):
        self._db_state += 1
        self._db_timestamp = self._getCurrentTimestamp()

    def _useCounter(self, name: str) -> int:
        result = self._counters[name]
        self._counters[name] += 1
        return result

    def _updateFamilyStats(self, family_id, num_samples_inc, num_functions_inc, num_library_samples_inc):
        family_entry = self.getFamily(family_id)
        assert family_entry is not None
        family_entry.num_samples += num_samples_inc
        family_entry.num_functions += num_functions_inc
        family_entry.num_library_samples += num_library_samples_inc

    def dbLogEvent(self, event_msg, username=None, details=None):
        """For MemoryStorage, this is a dummy function and we do not persist logging messages"""
        logging.info(event_msg)

    # TODO check if this works
    def deleteSample(self, sample_id: int) -> bool:
        if not self.isSampleId(sample_id):
            return False
        function_ids = self._sample_id_to_function_ids[sample_id]
        if sample_id < 0:
            for function_id in function_ids:
                del self._functions[function_id]
            del self._sample_id_to_function_ids[sample_id]
            del self._samples[sample_id]
            return True
        for function_id in function_ids:
            function_entry = self._functions[function_id]
            minhash = function_entry.getMinHash(self._minhash_config.MINHASH_SIGNATURE_BITS)
            # remove function
            del self._functions[function_id]
            # remove pichash entries
            if function_entry.pichash:
                self._pichashes[function_entry.pichash].remove((function_entry.family_id, sample_id, function_id))
            # remove minhash entries, if necessary
            if not minhash or not minhash.hasMinHash():
                continue
            band_hashes = self.getBandHashesForMinHash(minhash)
            for band_number, band_hash in sorted(band_hashes.items()):
                if band_hash not in self._bands[band_number]:
                    continue
                # delete function id from bandhash
                self._bands[band_number][band_hash].remove(function_id)
                # delete bandhash if empty
                if not self._bands[band_number][band_hash]:
                    del self._bands[band_number][band_hash]
        # remove references
        del self._sample_id_to_function_ids[sample_id]
        # in case to samples with same sha256 are added and then removed, it could crash, so remove entry safely
        self._sample_by_sha256.pop(self._samples[sample_id].sha256, None)

        sample_entry = self.getSampleById(sample_id)
        assert sample_entry is not None
        # by what was actually removed, and an emptied family goes with its last sample, like
        # the MongoDB backend does (#151)
        self._updateFamilyStats(sample_entry.family_id, -1, -len(function_ids), -int(sample_entry.is_library))
        # remove sample
        del self._samples[sample_id]
        if sample_entry.family_id != 0 and not any(s.family_id == sample_entry.family_id for s in self._samples.values()):
            self._families.pop(sample_entry.family_id, None)
        return True

    def modifySample(self, sample_id: int, update_information: dict) -> bool:
        if not self.isSampleId(sample_id):
            return False
        current_family_id = self._samples[sample_id].family_id
        if "is_library" in update_information:
            is_library_info_changed = self._samples[sample_id].is_library != update_information["is_library"]
            self._samples[sample_id].is_library = update_information["is_library"]
            if is_library_info_changed:
                self._families[current_family_id].num_library_samples += 1 if update_information["is_library"] else -1
        if "family_name" in update_information:
            family_name = update_information["family_name"]
            old_family_id = self._samples[sample_id].family_id
            family_id = self.addFamily(family_name)
            # update sample_entry and function_entries with new family information
            self._samples[sample_id].family = family_name
            self._samples[sample_id].family_id = family_id
            for function_id, function_entry in self._functions.items():
                if function_entry.sample_id == sample_id:
                    function_entry.family_id = family_id
                    self._functions[function_id] = function_entry
                    self._pichashes[function_entry.pichash].remove((old_family_id, sample_id, function_id))
                    self._pichashes[function_entry.pichash].add((family_id, sample_id, function_id))
            # update family information itself
            self._families[old_family_id].num_samples -= 1
            self._families[old_family_id].num_functions -= self._samples[sample_id].statistics["num_functions"]
            self._families[family_id].num_samples += 1
            self._families[family_id].num_functions += self._samples[sample_id].statistics["num_functions"]
            if self._samples[sample_id].is_library:
                self._families[old_family_id].num_library_samples -= 1
                self._families[family_id].num_library_samples += 1
            if self._families[old_family_id].num_samples == 0 and old_family_id != 0:
                self._families.pop(old_family_id)
        if "version" in update_information:
            self._samples[sample_id].version = update_information["version"]
        if "component" in update_information:
            self._samples[sample_id].component = update_information["component"]
        return True

    def modifyFunction(self, function_id: int, update_information: dict, username: Optional[str] = None) -> bool:
        if function_id < 0 or not self.isFunctionId(function_id):
            return False
        function_entry = self._functions[function_id]
        if "function_name" in update_information:
            function_name = update_information["function_name"]
            function_entry.function_name = function_name
            if function_name:
                label_username = username or "anonymous"
                is_known_label = any(label.username == label_username and label.function_label == function_name for label in function_entry.function_labels)
                if not is_known_label:
                    function_entry.function_labels.append(FunctionLabelEntry(function_name, label_username))
        return True

    def modifyFamily(self, family_id: int, update_information: dict) -> bool:
        if not self.isFamilyId(family_id):
            return False
        old_family_info = self.getFamily(family_id)
        assert old_family_info is not None
        if "is_library" in update_information:
            for sample_id, sample_entry in self._samples.items():
                if family_id == sample_entry.family_id:
                    self._samples[sample_id].is_library = update_information["is_library"]
            self._families[family_id].num_library_samples = self._families[family_id].num_samples
        # the family's own name is not a rename: merging a family into itself dropped it and then failed
        # on the lookup, or for family 0 doubled its counters. Compared with the stored name rather than
        # looked up, because another family may carry the same name
        if "family_name" in update_information and update_information["family_name"] != old_family_info.family_name:
            old_family_info = self.getFamily(family_id)
            family_name = update_information["family_name"]
            new_family_id = self.addFamily(family_name)
            new_family_info = self.getFamily(new_family_id)
            assert old_family_info is not None and new_family_info is not None
            new_num_samples = new_family_info.num_samples + old_family_info.num_samples
            new_num_functions = new_family_info.num_functions + old_family_info.num_functions
            new_num_lib_samples = new_family_info.num_library_samples + old_family_info.num_library_samples
            # update family_entry
            if family_id == 0:
                self._families[0].num_samples = 0
                self._families[0].num_functions = 0
                self._families[0].num_library_samples = 0
            else:
                self._families.pop(family_id)
            self._families[new_family_id].num_samples = new_num_samples
            self._families[new_family_id].num_functions = new_num_functions
            self._families[new_family_id].num_library_samples = new_num_lib_samples
            # update sample_entry and function_entries with new family information
            for sample_id, sample_entry in self._samples.items():
                if family_id == sample_entry.family_id:
                    self._samples[sample_id].family_id = new_family_id
                    self._samples[sample_id].family = family_name
            for function_id, function_entry in self._functions.items():
                if family_id == function_entry.family_id:
                    self._functions[function_id].family_id = new_family_id
                    self._pichashes[function_entry.pichash].remove((family_id, function_entry.sample_id, function_id))
                    self._pichashes[function_entry.pichash].add((new_family_id, function_entry.sample_id, function_id))
        self._updateDbState()
        return True

    def recomputeFamilyStats(self, progress_reporter=None) -> Dict[str, Any]:
        report: Dict[str, Any] = {"num_families": 0, "num_families_corrected": 0, "num_families_created": 0, "corrections": {}}
        for sample_entry in self._samples.values():
            if sample_entry.family_id not in self._families:
                self._families[sample_entry.family_id] = FamilyEntry(family_name=sample_entry.family, family_id=sample_entry.family_id)
                report["num_families_created"] += 1
        functions_by_family: Dict[int, int] = {}
        for function_entry in self._functions.values():
            functions_by_family[function_entry.family_id] = functions_by_family.get(function_entry.family_id, 0) + 1
        for family_id, family_entry in self._families.items():
            samples = [sample_entry for sample_entry in self._samples.values() if sample_entry.family_id == family_id]
            actual = {"num_samples": len(samples), "num_functions": functions_by_family.get(family_id, 0), "num_library_samples": len([s for s in samples if s.is_library])}
            stored = {key: getattr(family_entry, key) for key in actual}
            report["num_families"] += 1
            if stored != actual:
                for key, value in actual.items():
                    setattr(family_entry, key, value)
                report["num_families_corrected"] += 1
                report["corrections"][family_id] = {"before": stored, "after": actual}
        return report

    def deleteMinHashesForSample(self, sample_id: int) -> int:
        if not self.isSampleId(sample_id) or sample_id < 0:
            return 0
        num_hashed = 0
        for function_id in self._sample_id_to_function_ids[sample_id]:
            function_entry = self._functions[function_id]
            minhash = function_entry.getMinHash(self._minhash_config.MINHASH_SIGNATURE_BITS)
            if not minhash or not minhash.hasMinHash():
                continue
            num_hashed += 1
            for band_number, band_hash in sorted(self.getBandHashesForMinHash(minhash).items()):
                if band_hash in self._bands[band_number]:
                    self._bands[band_number][band_hash] = [fid for fid in self._bands[band_number][band_hash] if fid != function_id]
                    if not self._bands[band_number][band_hash]:
                        del self._bands[band_number][band_hash]
            function_entry.minhash = b""
            function_entry.shingler_composition = {}
        self._minhash_versions.pop(sample_id, None)
        return num_hashed

    def deleteAllMinHashes(self, progress_reporter=None) -> int:
        num_deleted = 0
        for sample_id in list(self._samples):
            num_deleted += self.deleteMinHashesForSample(sample_id)
        self._bands = {band_number: {} for band_number in self._bands}
        return num_deleted

    def setMinHashVersionForSamples(self, smda_version: str, sample_ids: Optional[List[int]] = None) -> None:
        for sample_id in list(self._samples) if sample_ids is None else sample_ids:
            if sample_id in self._samples:
                self._minhash_versions[sample_id] = smda_version

    def getSamplesWithStaleMinHashes(self, threshold_version: str) -> List[int]:
        threshold = packaging_version.parse(threshold_version)
        return sorted(sample_id for sample_id in self._samples if self._isStaleMinHashVersion(self._minhash_versions.get(sample_id), threshold))

    def deleteFamily(self, family_id: int, keep_samples: bool = False) -> bool:
        if family_id not in self._families:
            return False
        sample_entries = self.getSamplesByFamilyId(family_id) or []
        if family_id == 0:
            self._families[0].num_samples = 0
            self._families[0].num_functions = 0
            self._families[0].num_library_samples = 0
        else:
            self._families.pop(family_id)
        if keep_samples:
            for sample_entry in sample_entries:
                self._samples[sample_entry.sample_id].family_id = 0
                self._samples[sample_entry.sample_id].family = ""
            function_ids_to_modify = set()
            for function_id, function_entry in self._functions.items():
                if function_entry.family_id == family_id:
                    function_ids_to_modify.add(function_id)
            for function_id in function_ids_to_modify:
                self._functions[function_id].family_id = 0
            self._families[0].num_samples += len(sample_entries)
            self._families[0].num_functions += len(function_ids_to_modify)
            self._families[0].num_library_samples += len([s for s in sample_entries if s.is_library])
        else:
            for sample_entry in sample_entries:
                self.deleteSample(sample_entry.sample_id)
        self._updateDbState()
        return True

    def updateFunctionLabels(self, smda_report: "SmdaReport", username: str) -> Optional["SampleEntry"]:
        assert smda_report.sha256 is not None
        sample_entry = self.getSampleBySha256(smda_report.sha256)
        if not sample_entry:
            return None
        # check which functions in the SmdaReport have suitable function_names
        extracted_labels = {}
        for smda_function in smda_report.getFunctions():
            function_name = smda_function.function_name
            if function_name and smda_function.offset is not None and not re.match("sub_[a-fA-F0-9]{1,16}", function_name):
                extracted_labels[smda_function.offset] = function_name
        # get the respective FunctionEntries and check if the label is novel
        sample_function_entries = {entry.offset: entry for entry in self.getFunctionsBySampleId(sample_entry.sample_id) or []}
        for label_offset, extracted_label in extracted_labels.items():
            is_new_label = False
            if label_offset in sample_function_entries:
                is_new_label = True
                existing_labels = sample_function_entries[label_offset].function_labels
                for existing_label in existing_labels:
                    if existing_label.username == username and existing_label.function_label == extracted_label:
                        is_new_label = False
            # match by function_id or offset and add the label if it had not existed before.
            if is_new_label:
                new_function_entry_label = FunctionLabelEntry(extracted_label, username)
                matched_function_id = sample_function_entries[label_offset].function_id
                assert matched_function_id is not None
                self._functions[matched_function_id].function_labels.append(new_function_entry_label)

    def addSmdaReport(self, smda_report: "SmdaReport", isQuery=False) -> Optional["SampleEntry"]:
        assert smda_report.sha256 is not None
        sample_entry = None
        if isQuery:
            sample_entry = SampleEntry(smda_report, sample_id=-1 * self._useCounter("query_samples"), family_id=0)
            self._query_samples[sample_entry.sample_id] = sample_entry
            function_ids = []
            for smda_function in smda_report.getFunctions():
                function_entry = self._addFunction(sample_entry, smda_function, isQuery=True)
                function_ids.append(function_entry.function_id)
            self._sample_id_to_function_ids[sample_entry.sample_id] = function_ids
        else:
            if not self.getSampleBySha256(smda_report.sha256):
                family_id = self.addFamily(smda_report.family or "")
                sample_entry = SampleEntry(smda_report, sample_id=self._useCounter("samples"), family_id=family_id)
                self._samples[sample_entry.sample_id] = sample_entry
                self._sample_by_sha256[sample_entry.sha256] = sample_entry.sample_id
                function_ids = []
                for smda_function in smda_report.getFunctions():
                    function_entry = self._addFunction(sample_entry, smda_function)
                    function_ids.append(function_entry.function_id)
                self._sample_id_to_function_ids[sample_entry.sample_id] = function_ids
                self._updateFamilyStats(family_id, +1, sample_entry.statistics["num_functions"], int(sample_entry.is_library))
            else:
                LOGGER.warning("Sample %s already existed, skipping.", smda_report.sha256)
        return sample_entry

    def importSampleEntry(self, sample_entry: "SampleEntry") -> Optional["SampleEntry"]:
        if not self.getSampleBySha256(sample_entry.sha256):
            sample_id = self._useCounter("samples")
            sample_entry.sample_id = sample_id
            self._samples[sample_id] = sample_entry
            self._sample_by_sha256[sample_entry.sha256] = sample_id
            self._updateFamilyStats(sample_entry.family_id, +1, sample_entry.statistics["num_functions"], int(sample_entry.is_library))
        else:
            LOGGER.warning("Sample %s already existed, skipping.", sample_entry.sha256)
        return sample_entry

    def importFunctionEntry(self, function_entry: "FunctionEntry") -> Optional["FunctionEntry"]:
        function_entry.function_id = self._useCounter("functions")
        if function_entry.function_name and len(function_entry.function_labels) == 0:
            new_function_entry_label = FunctionLabelEntry(function_entry.function_name, "mcrit-import")
            function_entry.function_labels.append(new_function_entry_label)
        # add function to regular storage
        self._functions[function_entry.function_id] = function_entry
        if function_entry.sample_id not in self._sample_id_to_function_ids:
            self._sample_id_to_function_ids[function_entry.sample_id] = []
        self._sample_id_to_function_ids[function_entry.sample_id].append(function_entry.function_id)
        if function_entry.pichash:
            if function_entry.pichash not in self._pichashes:
                self._pichashes[function_entry.pichash] = set()
            self._pichashes[function_entry.pichash].add((function_entry.family_id, function_entry.sample_id, function_entry.function_id))
        return function_entry

    def importFunctionEntries(self, function_entries: List["FunctionEntry"]) -> Optional[List["FunctionEntry"]]:
        for function_entry in function_entries:
            function_entry.function_id = self._useCounter("functions")
            if function_entry.function_name and len(function_entry.function_labels) == 0:
                new_function_entry_label = FunctionLabelEntry(function_entry.function_name, "mcrit-import")
                function_entry.function_labels.append(new_function_entry_label)
            # add function to regular storage
            self._functions[function_entry.function_id] = function_entry
            if function_entry.sample_id not in self._sample_id_to_function_ids:
                self._sample_id_to_function_ids[function_entry.sample_id] = []
            self._sample_id_to_function_ids[function_entry.sample_id].append(function_entry.function_id)
            if function_entry.pichash:
                if function_entry.pichash not in self._pichashes:
                    self._pichashes[function_entry.pichash] = set()
                self._pichashes[function_entry.pichash].add((function_entry.family_id, function_entry.sample_id, function_entry.function_id))
        return function_entries

    # TODO return type?
    def getLibraryInfoForSampleId(self, sample_id: int) -> Optional[Dict[str, str]]:
        if sample_id not in self._samples:
            return None
        sample_entry = self._samples[sample_id]
        return {"family": sample_entry.family, "version": sample_entry.version} if sample_entry.is_library else None

    def _addFunction(self, sample_entry: "SampleEntry", smda_function: "SmdaFunction", minhash: Optional["MinHash"] = None, isQuery=False) -> "FunctionEntry":
        """Add a function (and optionally its MinHash) to storage, using the respective SampleEntry for reference.

        Args:
            sample_entry: The SampleEntry of smda_function
            smda_function: The SmdaFunction to be added.
            minhash: (Optional, defaults to None) The MinHash of the Function

        Returns:
            A FunctionEntry
        """
        if isQuery:
            function_entry = FunctionEntry(sample_entry, smda_function, -1 * self._useCounter("query_functions"), minhash=minhash)
            self._query_functions[function_entry.function_id] = function_entry
            return function_entry
        else:
            function_entry = FunctionEntry(sample_entry, smda_function, self._useCounter("functions"), minhash=minhash)
        image_lower = sample_entry.base_addr
        image_upper = image_lower + sample_entry.binary_size
        picblockhashes = []
        for hash_entry in self.blockhasher.getBlockhashesForFunction(smda_function, image_lower, image_upper, hash_size=8):
            for block_entry in hash_entry["offset_tuples"]:
                block_entry["hash"] = hash_entry["hash"]
                picblockhashes.append(block_entry)
        function_entry.picblockhashes = picblockhashes
        self._functions[function_entry.function_id] = function_entry
        if minhash and minhash.hasMinHash():
            minhash.function_id = function_entry.function_id
            self._addMinHashToBands(minhash)
        if function_entry.pichash:
            if function_entry.pichash not in self._pichashes:
                self._pichashes[function_entry.pichash] = set()
            self._pichashes[function_entry.pichash].add((function_entry.family_id, sample_entry.sample_id, function_entry.function_id))
        return function_entry

    def addMinHash(self, minhash: "MinHash") -> bool:
        if minhash.function_id is None:
            return False
        is_query_function = minhash.function_id < 0
        functions = self._query_functions if is_query_function else self._functions
        if minhash.function_id not in functions:
            LOGGER.warning("addMinHash() - no function with id %d, skipping its minhash.", minhash.function_id)
            return False
        functions[minhash.function_id].minhash = minhash.getMinHash()
        functions[minhash.function_id].minhash_shingle_composition = minhash.getComposition()
        # query functions are not indexed into bands, they are only ever used as query input
        if not is_query_function:
            self._addMinHashToBands(minhash)
        return True

    def addMinHashes(self, minhashes: List["MinHash"]) -> None:
        for minhash in minhashes:
            self.addMinHash(minhash)

    def createMatchingCache(self, function_ids: List[int], allow_self_return: bool = False, previous: Optional[MatchingCache] = None) -> MatchingCache:
        # previous is accepted for interface compatibility; MemoryStorage serves from memory
        # anyway, so there is nothing to retain across batches
        if allow_self_return:
            return StorageBackedMatchingCache(self, function_ids)
        cache_data = self._getCacheDataForFunctionIds(function_ids)
        return MatchingCache(cache_data)

    def _getCacheDataForFunctionIds(self, function_ids: List[int]) -> Dict:
        cache_data = {}
        sample_ids = {}
        sample_to_func_ids = {}
        minhashes = {}
        # one signature object per distinct signature, shared by every function carrying it -
        # the same deduplication MongoDbStorage._fetchCacheSlice does while decoding, so both
        # backends hand the matcher a cache of the same shape
        interned_signatures: Dict[bytes, bytes] = {}
        for function_id in set(function_ids):
            function_entry = self._query_functions[function_id] if function_id < 0 else self._functions[function_id]
            function_id = function_entry.function_id
            sample_id = function_entry.sample_id
            minhash = function_entry.minhash
            minhashes[function_id] = interned_signatures.setdefault(minhash, minhash)
            sample_ids[function_id] = sample_id
            if sample_id not in sample_to_func_ids:
                sample_to_func_ids[sample_id] = set()
            sample_to_func_ids[sample_id].add(function_id)
        cache_data["func_id_to_minhash"] = minhashes
        cache_data["func_id_to_sample_id"] = sample_ids
        cache_data["sample_id_to_func_ids"] = sample_to_func_ids
        return cache_data

    def clearMatchingCache(self) -> None:
        """no dedicated cache - no cleanup"""
        return

    def getSampleIds(self) -> List[int]:
        # TODO is deepcopy necessary?
        return deepcopy(list(self._samples.keys()))

    def getSamplesByFamilyId(self, family_id: int) -> Optional[List[SampleEntry]]:
        if family_id not in self._families:
            return None
        return [self._samples[sample_id] for sample_id in self._samples if self._samples[sample_id].family_id == family_id]

    def getFamilyIds(self) -> List[int]:
        # TODO is deepcopy necessary?
        return deepcopy(list(self._families.keys()))

    def isSampleId(self, sample_id: int) -> bool:
        return sample_id in self._samples or sample_id in self._query_samples

    def isFunctionId(self, function_id: int) -> bool:
        return function_id in self._functions or function_id in self._query_functions

    def isFamilyId(self, family_id: int) -> bool:
        return family_id in self._families

    def isPicHash(self, pichash: int) -> bool:
        return pichash in self._pichashes

    def getMatchesForPicHash(self, pichash: int) -> Set[Tuple[int, int, int]]:
        if pichash not in self._pichashes:
            return set()
        return deepcopy(self._pichashes[pichash])

    def getMatchesForPicBlockHash(self, picblockhash: int) -> Set[Tuple[int, int, int, int]]:
        result = set()
        for function_id, function_entry in self._functions.items():
            for pbh in function_entry.picblockhashes:
                if picblockhash == pbh["hash"]:
                    result.add((function_entry.family_id, function_entry.sample_id, function_id, pbh["offset"]))
        return result

    def getFamily(self, family_id: int) -> Optional[FamilyEntry]:
        if family_id in self._families:
            return self._families[family_id]
        return None

    def getFamilyEntriesByIds(self, family_ids: List[int]) -> Dict[int, "FamilyEntry"]:
        entries = {}
        for family_id in family_ids:
            entry = self.getFamily(family_id)
            if entry is not None:
                # getFamily hands out the stored entry itself, and GET /families/<id> attaches
                # its sample list to that; a batch entry carries none, as on MongoDbStorage
                entries[family_id] = copy(entry)
                entries[family_id].samples = None
        return entries

    def getFamilyId(self, family_name: str) -> Optional[int]:
        for fam_id, fam_entry in self._families.items():
            if fam_entry.family_name == family_name:
                return fam_id
        return None

    def addFamily(self, family_name: str) -> int:
        family_id = self.getFamilyId(family_name)
        if family_id is None:
            family_id = self._useCounter("families")
            self._families[family_id] = FamilyEntry(family_name=family_name, family_id=family_id)
        return family_id

    def getFunctionById(self, function_id: int, with_xcfg=False) -> Optional["FunctionEntry"]:
        function_entry = None
        if function_id < 0:
            if function_id in self._query_functions:
                function_entry = deepcopy(self._query_functions[function_id])
        else:
            if function_id in self._functions:
                function_entry = deepcopy(self._functions[function_id])
        if function_entry:
            if with_xcfg is False:
                function_entry.xcfg = None
            return deepcopy(function_entry)
        return None

    # TODO does this need to be more efficient?
    def getFunctionsBySampleId(self, sample_id: int) -> Optional[List["FunctionEntry"]]:
        if sample_id in self._samples or sample_id in self._query_samples:
            function_ids = self._sample_id_to_function_ids[sample_id]
            function_entries = []
            for function_id in function_ids:
                if sample_id < 0:
                    function_entry = self._query_functions[function_id]
                else:
                    function_entry = self._functions[function_id]
                function_entries.append(function_entry)
            # TODO is deepcopy necessary?
            return deepcopy(function_entries)
        return None

    def getFunctionIdsBySampleId(self, sample_id: int) -> Optional[List["int"]]:
        function_ids = None
        if sample_id in self._samples or sample_id in self._query_samples:
            function_ids = self._sample_id_to_function_ids[sample_id]
        return function_ids

    def getFunctions(self, start_index: int, limit: int) -> List["FunctionEntry"]:
        index = 0
        function_entries = []
        for _, function_entry in self._functions.items():
            if index >= start_index:
                if (limit == 0) or (len(function_entries) < limit):
                    function_entries.append(function_entry)
                else:
                    break
            index += 1
        return deepcopy(function_entries)

    def getSampleById(self, sample_id: int) -> Optional["SampleEntry"]:
        if sample_id in self._samples:
            return deepcopy(self._samples[sample_id])
        if sample_id in self._query_samples:
            return deepcopy(self._query_samples[sample_id])

    def getSampleEntriesByIds(self, sample_ids: List[int]) -> Dict[int, "SampleEntry"]:
        entries = {}
        for sample_id in sample_ids:
            entry = self.getSampleById(sample_id)
            if entry is not None:
                entries[sample_id] = entry
        return entries

    def getSampleBySha256(self, sha256: str, is_query=False) -> Optional["SampleEntry"]:
        sample_entry = None
        if is_query:
            # query_samples are not covered by the _sample_by_sha256 index
            for query_sample_entry in self._query_samples.values():
                if query_sample_entry.sha256 == sha256:
                    return deepcopy(query_sample_entry)
            return None
        if sha256 in self._sample_by_sha256:
            sample_id = self._sample_by_sha256[sha256]
            sample_entry = deepcopy(self._samples[sample_id])
        return sample_entry

    def getSampleIdByFunctionId(self, function_id: int) -> Optional[int]:
        sample_id = None
        if function_id in self._functions:
            sample_id = self._functions[function_id].sample_id
        if function_id in self._query_functions:
            sample_id = self._query_functions[function_id].sample_id
        return sample_id

    def getSampleIdsByFunctionIds(self, function_ids: List[int]) -> Dict[int, int]:
        sample_ids = {}
        for function_id in set(function_ids):
            if function_id in self._functions:
                sample_ids[function_id] = self._functions[function_id].sample_id
            elif function_id in self._query_functions:
                sample_ids[function_id] = self._query_functions[function_id].sample_id
        return sample_ids

    def getSamples(self, start_index: int, limit: int, is_query=False) -> List["SampleEntry"]:
        index = 0
        sample_entries = []
        samples = self._query_samples if is_query else self._samples
        for _, sample_entry in samples.items():
            if index >= start_index:
                if (limit == 0) or (len(sample_entries) < limit):
                    sample_entries.append(sample_entry)
                else:
                    break
            index += 1
        return deepcopy(sample_entries)

    def getPicHashMatchesByFunctionId(self, function_id: int) -> Optional[Dict[int, Set[Tuple[int, int, int]]]]:
        if function_id not in self._functions:
            return None
        pichash = self._functions[function_id].pichash
        if pichash is None:
            return None
        return {pichash: deepcopy(self._pichashes[pichash])}

    def getPicHashMatchesByFunctionIds(self, function_ids: List[int]) -> Dict[int, Set[Tuple[int, int, int]]]:
        pichashes = {}
        for function_id in function_ids:
            if function_id in self._functions:
                pichash = self._functions[function_id].pichash
                if pichash is None:
                    continue
                pichashes[pichash] = deepcopy(self._pichashes[pichash])
        return pichashes

    def getPicHashMatchesBySampleId(self, sample_id: int) -> Optional[Dict[int, Set[Tuple[int, int, int]]]]:
        function_entries = self.getFunctionsBySampleId(sample_id)
        if function_entries is None:
            return None
        function_ids = [fe.function_id for fe in function_entries]
        return self.getPicHashMatchesByFunctionIds(function_ids)

    def getMinHashByFunctionId(self, function_id: int) -> Optional[bytes]:
        if function_id in self._functions:
            function_entry = self._functions[function_id]
            return function_entry.minhash
        if function_id in self._query_functions:
            function_entry = self._query_functions[function_id]
            return function_entry.minhash
        return None

    # -> Dict[function_id, Set[function_id]]
    # TODO optimize or move to interface
    def getCandidatesForMinHashes(self, function_id_to_minhash: Dict[int, "MinHash"], band_matches_required=1) -> Dict[int, Set[int]]:
        candidates = {}
        for function_id, minhash in function_id_to_minhash.items():
            candidates[function_id] = self.getCandidatesForMinHash(minhash, band_matches_required=band_matches_required)
        return candidates

    # -> Set[function_id]
    def getCandidatesForMinHash(self, minhash: "MinHash", band_matches_required=1) -> Set[int]:
        if not minhash.hasMinHash():
            return set()
        candidates = {}
        band_hashes = self.getBandHashesForMinHash(minhash)
        for band_number, band_hash in sorted(band_hashes.items()):
            if band_hash in self._bands[band_number]:
                for function_id in self._bands[band_number][band_hash]:
                    if function_id not in candidates:
                        candidates[function_id] = 0
                    candidates[function_id] += 1
        # reduce candidates based on banding requirements
        valid_candidates = set([])
        for function_id, hit_count in candidates.items():
            if hit_count >= band_matches_required:
                valid_candidates.add(function_id)
        return valid_candidates

    def getUnhashedFunctions(self, function_ids: Optional[List[int]] = None, only_function_ids=False) -> List[Union[int, "FunctionEntry"]]:
        # Same selection as the MongoDB backend: needs a MinHash, and is large enough to receive
        # one. Functions below the size thresholds can never be hashed, so returning them only
        # makes every caller rediscover that after loading their disassembly.
        candidates = [
            function_entry
            for _, function_entry in self._functions.items()
            if not function_entry.minhash and self.isHashableBySize(function_entry.num_instructions, function_entry.num_blocks)
        ]
        if function_ids is None:
            # the None case additionally requires disassembly to be present, since without it there
            # is nothing to hash from
            candidates = [function_entry for function_entry in candidates if function_entry.xcfg]
        else:
            wanted = set(function_ids)
            candidates = [function_entry for function_entry in candidates if function_entry.function_id in wanted]
        if only_function_ids:
            return [function_entry.function_id for function_entry in candidates]
        return list(candidates)

    def deleteXcfgData(self) -> None:
        for function_id, function_entry in self._functions.items():
            self._functions[function_id].xcfg = {}

    def deleteXcfgForSampleId(self, sample_id: int) -> None:
        for function_id, function_entry in self._functions.items():
            if sample_id == function_entry.sample_id:
                self._functions[function_id].xcfg = {}

    # TODO dict of what?
    def getContent(self) -> Dict[str, Any]:
        content = {
            "families": {family_id: family.toDict() for family_id, family in self._families.items()},
            "samples": {sample_id: sample.toDict() for sample_id, sample in self._samples.items()},
            "functions": {function_id: function.toDict() for function_id, function in self._functions.items()},
            "bands": self._bands,
        }
        return content

    # TODO dict of what?
    def setContent(self, content: Dict[str, Any]) -> None:
        self._families = {int(k): FamilyEntry.fromDict(v) for k, v in content["families"].items()}
        self._samples = {int(k): SampleEntry.fromDict(v) for k, v in content["samples"].items()}
        self._bands = {int(k): {int(ik): iv for ik, iv in v.items()} for k, v in content["bands"].items()}
        self._sample_by_sha256 = {sample.sha256: sample_id for sample_id, sample in self._samples.items()}
        self._sample_id_to_function_ids = defaultdict(list)
        self._pichashes = {}
        for function_id, function in content["functions"].items():
            function_id = int(function_id)
            function_entry = FunctionEntry.fromDict(function)
            self._functions[function_id] = function_entry
            self._sample_id_to_function_ids[function_entry.sample_id].append(function_id)
            if function_entry.pichash:
                if function_entry.pichash not in self._pichashes:
                    self._pichashes[function_entry.pichash] = set()
                self._pichashes[function_entry.pichash].add((function_entry.family_id, function_entry.sample_id, function_entry.function_id))

    def getStats(self, with_pichash=True) -> Dict[str, Any]:
        stats = {
            "db_state": self._db_state,
            "db_timestamp": self._db_timestamp,
            "num_families": len(self._families),
            "num_samples": len(self._samples),
            "num_functions": len(self._functions),
            "num_bands": len(self._bands),
            # None signals "not computed", so that consumers can distinguish it from an actual count of 0
            "num_pichashes": len(self._pichashes) if with_pichash else None,
        }
        return stats

    def _addMinHashToBands(self, minhash: "MinHash") -> None:
        assert minhash.function_id is not None
        band_hashes = self.getBandHashesForMinHash(minhash)
        for band_number, band_hash in sorted(band_hashes.items()):
            if band_hash not in self._bands[band_number]:
                self._bands[band_number][band_hash] = []
            self._bands[band_number][band_hash].append(minhash.function_id)

    def getUniqueBlocks(self, sample_ids: List[int], progress_reporter=None) -> Dict:
        # query once to get all blocks from the functions of our samples
        block_statistics: Dict[str, Any] = {
            "by_sample_id": {sample_id: {"sample_id": sample_id, "total_blocks": 0, "characteristic_blocks": 0, "unique_blocks": 0} for sample_id in sample_ids},
            "unique_blocks_overall": 0,
            "num_samples": len(sample_ids),
        }
        candidate_picblockhashes: Dict[int, Dict[str, Any]] = {}
        for function_id, entry in self._functions.items():
            sample_id = entry.sample_id
            for block_entry in entry.picblockhashes:
                block_hash = block_entry["hash"]
                if block_hash not in candidate_picblockhashes:
                    candidate_picblockhashes[block_hash] = {
                        "samples": set(),
                        "length": block_entry["length"],
                        "function_id": entry.function_id,
                        "sample_id": sample_id,
                        "offset": block_entry["offset"],
                        "instructions": [],
                        "escaped_sequence": "",
                        "score": 0,
                    }
                candidate_picblockhashes[block_hash]["samples"].add(sample_id)
        # update statistics based on candidates
        for picblockhash, entry in candidate_picblockhashes.items():
            for sample_id in entry["samples"]:
                block_statistics["by_sample_id"][sample_id]["total_blocks"] += 1
        LOGGER.info(f"Found {len(candidate_picblockhashes)} candidate picblock hashes")
        for functiond_id, entry in self._functions.items():
            sample_id = entry.sample_id
            if sample_id not in sample_ids:
                for block_entry in entry.picblockhashes:
                    candidate_picblockhashes.pop(block_entry["hash"], None)
        # update statistics again after having reduced to results
        for picblockhash, entry in candidate_picblockhashes.items():
            if len(entry["samples"]) == 1:
                single_sample_id = list(entry["samples"])[0]
                block_statistics["by_sample_id"][single_sample_id]["unique_blocks"] += 1
            for sample_id in entry["samples"]:
                block_statistics["by_sample_id"][sample_id]["characteristic_blocks"] += 1
        block_statistics["unique_blocks_overall"] = len(candidate_picblockhashes)
        LOGGER.info(f"Reduced to {len(candidate_picblockhashes)} unique picblock hashes")
        # iterate over candidates by function_id and extract instructions
        function_id_to_block_offsets = {}
        for picblockhash, entry in candidate_picblockhashes.items():
            candidate_picblockhashes[picblockhash]["samples"] = sorted(list(entry["samples"]))
            # we calculate the score for this block as 80% of how well it covers the samples and 20% how far its size is away from an "ideal" signature block
            sample_score = 100.0 * len(entry["samples"]) / len(sample_ids)
            length_score = 100.0
            if entry["length"] < 7:
                length_score = 100.0 - (100.0 * (7 - entry["length"]) / 7)
            elif entry["length"] > 10:
                length_score = 100.0 * (1 / (entry["length"] - 10))
            candidate_picblockhashes[picblockhash]["score"] = 0.8 * sample_score + 0.2 * length_score
            if entry["function_id"] not in function_id_to_block_offsets:
                function_id_to_block_offsets[entry["function_id"]] = []
            function_id_to_block_offsets[entry["function_id"]].append((entry["offset"], picblockhash))
        for function_id, entry in self._functions.items():
            if function_id not in function_id_to_block_offsets.keys():
                continue
            if entry.xcfg is None:
                continue
            blocks = entry.xcfg["blocks"]
            for block_offset, picblockhash in function_id_to_block_offsets[function_id]:
                # a live xcfg (from SmdaFunction.toDict()) keys blocks by int, while one that has
                # been through a JSON round trip keys them by str - accept either
                block_instructions = blocks.get(block_offset, blocks.get(str(block_offset)))
                if block_instructions is None:
                    continue
                candidate_picblockhashes[picblockhash]["instructions"] = block_instructions
        return {"statistics": block_statistics, "unique_blocks": candidate_picblockhashes}

    def rebuildPicBlockHashIndex(self, progress_reporter=None) -> int:
        # MemoryStorage holds every function in a dict already, so getUniqueBlocks' elimination is
        # an in-memory pass with nothing to index. Implemented rather than left to raise, so the
        # backends stay interchangeable for callers that offer the rebuild unconditionally.
        return 0

    def rebuildFunctionRangeIndex(self, progress_reporter=None) -> int:
        # MemoryStorage already holds every FunctionEntry, so a function's sample is one dict
        # lookup away and there is nothing to index. Reported complete so the two-stage path is
        # available here too, rather than silently falling back to whole-corpus matching.
        self._function_range_index_complete = True
        return len({function_entry.sample_id for function_entry in self._functions.values()})

    def isFunctionRangeIndexComplete(self) -> bool:
        return getattr(self, "_function_range_index_complete", True)

    def getSampleIdsForFunctionIdArray(self, function_ids):
        import numpy as np

        return np.fromiter(
            (self._functions[function_id].sample_id if function_id in self._functions else -1 for function_id in function_ids.tolist()),
            dtype=np.int64,
            count=len(function_ids),
        )

    def getSampleFunctionCounts(self, sample_ids=None):
        """sample_id -> function count, optionally restricted to `sample_ids`.

        The restriction exists for the MongoDB backend, where building the whole-corpus map per
        matching job was the last per-query cost that grew with corpus size. Kept in step here so
        both backends answer the same call; this one still walks its own functions either way.
        """
        wanted = None if sample_ids is None else {int(sample_id) for sample_id in sample_ids}
        counts: Dict[int, int] = {}
        for function_entry in self._functions.values():
            if wanted is not None and function_entry.sample_id not in wanted:
                continue
            counts[function_entry.sample_id] = counts.get(function_entry.sample_id, 0) + 1
        return counts

    def rebuildBandDfIndex(self, progress_reporter=None) -> int:
        # band posting lists are plain in-memory lists whose length is free to read, so the
        # cutoff needs no stored df here
        return 0

    def isBandDfIndexComplete(self) -> bool:
        return True

    def _countBandDf(self, band_number: int, thresholds: List[int]) -> Dict[str, Any]:
        # df is the length of the in-memory posting list; an empty list holds no posting, and the
        # MongoDB count skips its empty documents the same way
        lengths = [len(function_ids) for function_ids in self._bands[band_number].values() if function_ids]
        return {
            "band_hashes": len(lengths),
            "postings": sum(lengths),
            "max_df": max(lengths, default=0),
            "over": {threshold: [sum(1 for df in lengths if df > threshold), sum(df for df in lengths if df > threshold)] for threshold in thresholds},
        }

    def rebuildMinhashBandIndex(self, progress_reporter=None):
        # TODO while minhashes are considerably small, there is a still chance that the
        # sum of all minhashes will eventually exceed available memory on a given system.
        # in this case we can start doing batches in our database interation already
        minhashes = []
        # get all minhash objects from all functions
        for function_id, function_entry in self._functions.items():
            if function_entry.minhash:
                minhash_obj = function_entry.getMinHash(self._minhash_config.MINHASH_SIGNATURE_BITS)
                minhashes.append(minhash_obj)
        # drop band collections
        # recreate indices for band collections
        for band_id in range(self._storage_config.STORAGE_NUM_BANDS):
            self._bands[band_id] = {}
        # re-add minhashes in batches
        batch_size = self._minhash_config.MINHASH_BAND_REBUILD_WORK_PACKAGE_SIZE
        if progress_reporter:
            progress_reporter.set_total((len(minhashes) // batch_size) + 1)
        for minhash_batch in zip_longest(*[iter(minhashes)] * batch_size):
            minhash_batch = [mh for mh in minhash_batch if mh is not None]
            for mh in minhash_batch:
                self._addMinHashToBands(mh)
            if progress_reporter:
                progress_reporter.step()
        return len(minhashes)

    ##### helpers for search ######

    @staticmethod
    def _get_sort_key_from_cursor(full_cursor: Optional[FullSearchCursor]):
        assert full_cursor is not None

        # Mapping entry -> tuple to sort by
        def get_tuple_from_entry(entry):
            return tuple(_get_field(entry, field) for field in full_cursor.sort_fields)

        # custom tuple compare function considering sort direction per dimension
        is_backward_search = not full_cursor.is_forward_search
        sort_directions = [1 if direction ^ is_backward_search else -1 for direction in full_cursor.sort_directions]

        def compare_tuple(tuple1, tuple2):
            for i in range(len(full_cursor.sort_fields)):
                direction = sort_directions[i]
                val1 = tuple1[i]
                val2 = tuple2[i]
                if val1 == val2:
                    pass
                if val1 is None:
                    return -direction
                if val2 is None:
                    return +direction
                if val1 < val2:
                    return -direction
                if val1 > val2:
                    return direction
            return 0

        return lambda entry: functools.cmp_to_key(compare_tuple)(get_tuple_from_entry(entry))

    def _get_search_filter(self, search_fields: List[str], search_tree: NodeType, cursor: Optional[FullSearchCursor], conditional_search_fields=None) -> Callable:
        if cursor is not None:
            full_tree = AndNode([search_tree, cursor.toTree()])
        else:
            full_tree = search_tree
        full_tree = SearchFieldResolver(search_fields, conditional_search_fields=conditional_search_fields).visit(full_tree)
        full_tree = FilterSingleElementLists().visit(full_tree)
        full_tree = PropagateNot().visit(full_tree)
        filter = MemorySearchTranspiler().visit(full_tree)
        return filter

    ##### search ######

    # NOTE: Slowest part of search (by far) is the sorting. Maybe this can be precomputed or cached?
    # NOTE: Once sorting is fast, the search cursor could be handled separately from the search tree.
    #       Instead of putting it in the filter function, we could apply binary search on the sorted data.

    def findFamilyByString(self, search_tree: NodeType, cursor: Optional[FullSearchCursor] = None, max_num_results: int = 100) -> Dict[int, "FamilyEntry"]:
        result_dict = {}
        search_fields = ["family_name"]
        filter = self._get_search_filter(search_fields, search_tree, cursor)
        sort_key = self._get_sort_key_from_cursor(cursor)
        for entry in sorted(self._families.values(), key=sort_key):
            if filter(entry):
                result_dict[entry.family_id] = entry
            if len(result_dict) >= max_num_results:
                break
        return result_dict

    def findSampleByString(self, search_tree: NodeType, cursor: Optional[FullSearchCursor] = None, max_num_results: int = 100) -> Dict[int, "SampleEntry"]:
        result_dict = {}
        search_fields = [
            "filename",
            "family",
            "component",
            "version",
        ]
        conditional_field = ("sha256", lambda search_term: len(search_term) >= 3)
        filter = self._get_search_filter(search_fields, search_tree, cursor, conditional_search_fields=[conditional_field])
        sort_key = self._get_sort_key_from_cursor(cursor)
        for entry in sorted(self._samples.values(), key=sort_key):
            if filter(entry):
                result_dict[entry.sample_id] = entry
            if len(result_dict) >= max_num_results:
                break
        return result_dict

    def findFunctionByString(self, search_tree: NodeType, cursor: Optional[FullSearchCursor] = None, max_num_results: int = 100) -> Dict[int, "FunctionEntry"]:
        result_dict = {}
        # TODO also search through function labels once we have implemented them
        search_fields = ["function_name"]
        filter = self._get_search_filter(search_fields, search_tree, cursor)
        sort_key = self._get_sort_key_from_cursor(cursor)
        for entry in sorted(self._functions.values(), key=sort_key):
            if filter(entry):
                result_dict[entry.function_id] = entry
            if len(result_dict) >= max_num_results:
                break
        return result_dict
