"""Automatic retention applies only to known transient worker methods.

Unknown methods are kept: a future result-producing job must not silently lose its report.
Failed jobs have no ``finished_at`` and remain available for diagnosis, and so do terminated ones,
which can still reach ``finished_at`` when their method completes. The set is injected into the
queue by QueueFactory; the vendored mongoqueue does not import mcrit. Every name must be a
``@Remote`` method of ``Worker`` (checked in tests/testRemoteCalls.py).
"""

TRANSIENT_JOB_METHODS = frozenset(
    {
        "addBinarySample",
        "deleteFamily",
        "deleteOrphanedQueueFiles",
        "deleteSample",
        "doDbCleanup",
        "modifyFamily",
        "modifySample",
        "rebuildBandDfIndex",
        "rebuildFunctionRangeIndex",
        "rebuildIndex",
        "rebuildPicBlockHashIndex",
        "recalculateMinHashes",
        "recalculatePicHashes",
        "recomputeFamilyStats",
        "repairMinHashes",
        "updateMinHashes",
        "updateMinHashesForSample",
    }
)
