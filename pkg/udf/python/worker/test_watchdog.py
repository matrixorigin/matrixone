#!/usr/bin/env python3

import importlib.util
import pathlib
import unittest
from unittest import mock


WATCHDOG_PATH = pathlib.Path(__file__).with_name("watchdog.py")
spec = importlib.util.spec_from_file_location("matrixone_python_watchdog", WATCHDOG_PATH)
watchdog = importlib.util.module_from_spec(spec)
spec.loader.exec_module(watchdog)


class WatchdogTest(unittest.TestCase):
    @unittest.skipUnless(watchdog.os.name == "posix", "process-group watchdog")
    def test_parent_watch_read_error_kills_process_group(self):
        with (
            mock.patch.object(watchdog.os, "read", side_effect=OSError("closed")),
            mock.patch.object(watchdog.os, "killpg") as killpg,
            mock.patch.object(watchdog.os, "close"),
        ):
            watchdog._watch(17, 23)

        killpg.assert_called_once_with(23, watchdog.signal.SIGKILL)


if __name__ == "__main__":
    unittest.main()
