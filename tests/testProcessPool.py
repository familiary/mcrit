import os
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import mcrit
from mcrit.libs.parallel import create_process_pool

# a multiprocessing/ in the working directory that leaves a marker and hands over to the real package
PLANTED = """import os, sys
open(os.path.join({marker_dir!r}, "planted-%d" % os.getpid()), "w").close()
_saved = sys.path[:]
sys.path[:] = [p for p in sys.path if p not in ("", ".", {planted_dir!r})]
del sys.modules["multiprocessing"]
import multiprocessing as _real
sys.path[:] = _saved
sys.modules["multiprocessing"] = _real
"""

POOL_SCRIPT = """from mcrit.libs.parallel import create_process_pool

if __name__ == "__main__":
    with create_process_pool(2) as pool:
        print(pool.map(abs, [-1, -2]))
"""


class ProcessPoolTest(unittest.TestCase):
    def test_pool_processes_do_not_run_modules_from_the_working_directory(self):
        # started from a script, as the `mcrit` console script starts a worker, in a directory others can write to
        mcrit_root = os.path.dirname(os.path.dirname(os.path.abspath(mcrit.__file__)))
        env = {key: value for key, value in os.environ.items() if key != "PYTHONSAFEPATH"}
        env["PYTHONPATH"] = os.pathsep.join(filter(None, [mcrit_root, os.environ.get("PYTHONPATH")]))
        with tempfile.TemporaryDirectory() as planted_dir, tempfile.TemporaryDirectory() as script_dir, tempfile.TemporaryDirectory() as marker_dir:
            os.makedirs(os.path.join(planted_dir, "multiprocessing"))
            with open(os.path.join(planted_dir, "multiprocessing", "__init__.py"), "w") as fout:
                fout.write(PLANTED.format(marker_dir=marker_dir, planted_dir=planted_dir))
            script = os.path.join(script_dir, "pool.py")
            with open(script, "w") as fout:
                fout.write(POOL_SCRIPT)
            result = subprocess.run([sys.executable, script], cwd=planted_dir, env=env, capture_output=True, timeout=120)
            planted_runs = os.listdir(marker_dir)
        self.assertIn(b"[1, 2]", result.stdout, result.stderr)
        self.assertEqual([], planted_runs)

    def test_creating_a_pool_leaves_the_environment_as_it_was(self):
        for previous in (None, "", "0"):
            with self.subTest(previous=previous), patch.dict(os.environ):
                os.environ.pop("PYTHONSAFEPATH", None)
                if previous is not None:
                    os.environ["PYTHONSAFEPATH"] = previous
                before = dict(os.environ)
                with create_process_pool(1) as pool:
                    self.assertEqual([1], pool.map(abs, [-1]))
                    self.assertEqual(before, dict(os.environ))
                self.assertEqual(before, dict(os.environ))


if __name__ == "__main__":
    unittest.main()
