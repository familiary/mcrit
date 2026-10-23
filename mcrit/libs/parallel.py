import os
from multiprocessing import cpu_count, get_context


def create_process_pool(processes=None):
    """Use a spawn-based context to avoid Python 3.12 fork warnings on Linux.

    The pool's processes and its resource tracker start as `python -c ...`, with the working directory
    first on sys.path while they import multiprocessing; PYTHONSAFEPATH keeps it off. Both start while the pool
    is created, so the variable is set only for that long and not left in this process's environment, where
    every Python process started later would inherit it.
    """
    process_count = processes if processes is not None else cpu_count()
    previous = os.environ.get("PYTHONSAFEPATH")
    os.environ["PYTHONSAFEPATH"] = "1"
    try:
        return get_context("spawn").Pool(process_count)
    finally:
        if previous is None:
            del os.environ["PYTHONSAFEPATH"]
        else:
            os.environ["PYTHONSAFEPATH"] = previous
