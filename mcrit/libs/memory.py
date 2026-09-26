import ctypes
import ctypes.util
import logging

LOGGER = logging.getLogger(__name__)

# resolved on first use: the libc's malloc_trim, or False where there is none (musl, macOS, Windows)
_malloc_trim = None


def return_freed_memory() -> bool:
    """Hand the memory a finished job freed back to the operating system, where the libc allows it.

    A matching job builds and drops large dicts batch after batch; glibc keeps the freed memory in
    its arenas, so a long-lived worker's resident size stays at its largest job's peak and the
    workers beside it cannot have it (#69). malloc_trim(0) releases it. Only glibc has it; the call
    is skipped elsewhere. Returns whether it ran.
    """
    global _malloc_trim
    if _malloc_trim is None:
        try:
            _malloc_trim = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6").malloc_trim
            _malloc_trim.argtypes = [ctypes.c_size_t]
            _malloc_trim.restype = ctypes.c_int
        except (OSError, AttributeError):
            LOGGER.debug("No malloc_trim in this libc, freed memory stays with the process.")
            _malloc_trim = False
    if _malloc_trim is False:
        return False
    _malloc_trim(0)
    return True
