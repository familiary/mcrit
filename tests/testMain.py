import os
import sys
import unittest
from unittest.mock import patch

import mcrit.__main__ as mcrit_main


class MainEnvironmentTest(unittest.TestCase):
    def _safe_path_after(self, command):
        with patch.dict(os.environ), patch.object(sys, "argv", ["mcrit", command, "--job_id", "0123456789abcdef01234567"]):
            os.environ.pop("PYTHONSAFEPATH", None)
            with (
                patch.object(mcrit_main, "runServer"),
                patch.object(mcrit_main, "runWorker"),
                patch.object(mcrit_main, "runSpawningWorker"),
                patch.object(mcrit_main, "runSingleJobWorker"),
                patch.object(mcrit_main, "runClient"),
            ):
                mcrit_main.main()
            return os.environ.get("PYTHONSAFEPATH")

    def test_long_running_commands_set_pythonsafepath_for_good(self):
        for command in ["server", "worker", "spawningworker", "singlejobworker"]:
            with self.subTest(command=command):
                self.assertEqual("1", self._safe_path_after(command))

    def test_client_commands_leave_the_environment_alone(self):
        self.assertIsNone(self._safe_path_after("client"))


if __name__ == "__main__":
    unittest.main()
