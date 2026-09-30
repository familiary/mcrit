import json
import logging
import math
import os
from typing import Dict, List, Tuple

# Only do basicConfig if no handlers have been configured
if not logging.root.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
LOGGER = logging.getLogger(__name__)


class LogBucket:
    """
    LogBuckets are the vehicle we use to allow fuzzy matching among discrete values.
    Using ranges of values yields at least partial matches for values near each other.
    The step size is derived as 2**floor(log2(value) / 2)

    Example ranges:

        0 [-2, -1, 0, 1, 2]
        1 [-1, 0, 1, 2, 3]
        2 [0, 1, 2, 3, 4]
        3 [1, 2, 3, 4, 6]
        4 [2, 3, 4, 6, 8]
        5 [2, 3, 4, 6, 8]
        ...
        89 [72, 80, 88, 96, 104]
        ...
        97 [80, 88, 96, 104, 112]
        ...

    When matching ranges for values 3 and 5 we get:
        3 [1, 2, 3, 4, 6]
        5 [2, 3, 4, 6, 8]
        intersection = [2, 3, 4, 6]
        union = [1, 2, 3, 4, 6, 8]
        jaccard-similarity = 4/6 = 0.66
    When matching ranges for values 89 and 97 we get:
        89 [72, 80, 88, 96, 104]
        97 [80, 88, 96, 104, 112]
        intersection = [80, 88, 96, 104]
        union = [72, 80, 88, 96, 104, 112]
        jaccard-similarity = 4/6 = 0.66
    As a result, with increasing values, buckets become wider and allow for a "scaled" amount of Fuzziness.
    """

    # tables by (max_value, bucket_width), shared by the instances of this class, as ShingleLoader
    # constructs each shingler twice and each MinHasher would otherwise build the table twice; the
    # tables and the ranges getLogBucketRange returns from them must therefore never be modified
    _tables: Dict[Tuple[int, int], Dict[int, List[int]]] = {}

    def __init__(self, max_value=100000, bucket_width=1):
        # checked here rather than left to fail on the first lookup, far from the configuration at fault
        for name, value in (("max_value", max_value), ("bucket_width", bucket_width)):
            if not isinstance(value, int) or isinstance(value, bool):
                raise TypeError(f"LogBucket needs an int {name}, got {value!r}")
        if max_value < 1 or bucket_width < 0:
            raise ValueError(f"LogBucket needs max_value >= 1 and bucket_width >= 0, got {max_value} and {bucket_width}")
        self._max_value = max_value
        self._bucket_width = bucket_width
        self._init_buckets()

    def _init_buckets(self):
        key = (self._max_value, self._bucket_width)
        if key not in LogBucket._tables:
            LogBucket._tables[key] = self._loadTable()
        self._value_to_bucket_range = LogBucket._tables[key]
        self._recordTableBounds()

    def _loadTable(self):
        # The table only depends on max_value and bucket_width, so the file carries both in its name:
        # asking for other parameters must never be answered with a table built for the defaults.
        # Only the defaults' table is shipped. Any other is built here and kept in memory, never
        # written: the package directory may be read-only, and several workers start at once.
        bucket_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache", f"logbuckets_{self._max_value}_{self._bucket_width}.json")
        if os.path.isfile(bucket_path):
            with open(bucket_path) as fjson:
                return {int(value): bucket_range for value, bucket_range in json.load(fjson).items()}
        LOGGER.info(f"No shipped logbucket table for max_value={self._max_value}, bucket_width={self._bucket_width}, calculating it")
        # Every range should hold the centre bucket and bucket_width buckets either side. From
        # bucket_width 5 on, the slice start goes negative for the lowest values past the width,
        # giving them an empty or wrong range; a max_value too small for its width runs the top
        # values past the buckets built, giving a short range or an IndexError.
        try:
            table = self._buildTable()
        except IndexError:
            table = {}
        malformed = [value for value in range(self._max_value) if len(table.get(value, ())) != 2 * self._bucket_width + 1]
        if malformed:
            raise ValueError(f"bucket_width={self._bucket_width} is too wide for max_value={self._max_value}: values {malformed[:5]} get no proper bucket range")
        return table

    def _buildTable(self):
        value_to_bucket_id = {}
        value_to_bucket_range = {}
        buckets = []
        # a set for the membership test: the list alone made building the default table take 1.3 s instead of 0.2 s
        known_buckets = set()
        # first generate a list of logarithmically-scaled buckets and map to their values of origin
        for value in range(self._max_value * 2):
            log_value = math.log(value, 2) if value > 0 else 0
            floored_exponent = math.floor(log_value)
            if floored_exponent < 2:
                middle_bucket = value
            else:
                window_size = 2 ** math.floor(floored_exponent / 2)
                middle_bucket = window_size * math.ceil(value / window_size)
            if middle_bucket not in known_buckets:
                known_buckets.add(middle_bucket)
                buckets.append(middle_bucket)
            value_to_bucket_id[value] = len(buckets) - 1
        # as these would have incremental steps at values that are divisible by 2, we center around best fitting buckets per value.
        for value in range(self._max_value):
            bucket_id = value_to_bucket_id[value]
            # int can be arbitrarily large in Python, "inf" is guaranteed to be bigger. :)
            best_fit = float("inf")
            best_id = bucket_id
            for test_id in [bucket_id - 1, bucket_id, bucket_id + 1]:
                if test_id < 0 or test_id >= len(buckets):
                    continue
                test_value = buckets[test_id]
                if test_value and abs(value - test_value) < best_fit:
                    best_fit = abs(value - test_value)
                    best_id = test_id
                value_to_bucket_id[value] = best_id
        # having fitted the value->middle_bucket mapping, we expand the middle values to bucket ranges
        for value in range(self._max_value):
            bucket_id = value_to_bucket_id[value]
            bucket_range = buckets[bucket_id - self._bucket_width : bucket_id + self._bucket_width + 1]
            if value < self._bucket_width:
                bucket_range = []
                for bucket_value in range(-1 * self._bucket_width + value, 0, 1):
                    bucket_range.append(bucket_value)
                for index in range(0, self._bucket_width + value + 1, 1):
                    bucket_range.append(buckets[index])
            if len(bucket_range):
                value_to_bucket_range[value] = bucket_range
        return value_to_bucket_range

    def _recordTableBounds(self):
        """Remember the range the table actually covers, for clamping.

        That is 0 and max_value - 1 for any table accepted, since a malformed one is refused, but
        reading the bounds from the table keeps the clamp from ever naming a missing key.
        """
        self._lowest_value = min(self._value_to_bucket_range) if self._value_to_bucket_range else 0
        self._highest_value = max(self._value_to_bucket_range) if self._value_to_bucket_range else 0

    def getLogBucketRange(self, value, increased_center=False):
        # The table is precomputed for 0..max_value-1, so anything outside that raised KeyError -
        # which aborts the whole indexing job over a single outsized function. FuzzyStatPairShingler
        # already applies exactly this bound to stack_size, but the other fields it buckets
        # (max_block_size, num_ins_C, num_ins_S, num_calls) are unbounded, and real corpora contain
        # functions that exceed it: a basic block of 108,837 bytes was what surfaced this.
        #
        # Clamping maps such values onto the nearest defined bucket instead. No value inside the
        # table changes bucket, so no existing MinHash changes - this only defines behaviour where
        # there was previously a crash.
        if value > self._highest_value:
            value = self._highest_value
        elif value < self._lowest_value:
            value = self._lowest_value
        return self._value_to_bucket_range[value]


if __name__ == "__main__":
    log_buckets = LogBucket(16, 2)
    for value in range(16):
        bucket_range = log_buckets.getLogBucketRange(value)
        print(value, bucket_range)
