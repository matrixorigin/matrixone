# Copyright 2026 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import contextlib
import io
from types import SimpleNamespace
import unittest

import sirius_profile_exit as profile


class DebuggerError(Exception):
    pass


class Debugger:
    error = GdbError = DebuggerError

    def __init__(self, requested=0, actual=0, thread_exit=False, unknown=False):
        self.requested, self.actual = requested, actual
        self.thread_exit, self.unknown = thread_exit, unknown
        self.commands, self.handlers = [], []
        self.inferior = SimpleNamespace(num=1, pid=42)
        self.events = SimpleNamespace(exited=SimpleNamespace(
            connect=self.handlers.append, disconnect=self.handlers.remove
        ))
        self.deleted = False

    def selected_inferior(self):
        return self.inferior

    def Breakpoint(self, *_, **__):
        return SimpleNamespace(delete=lambda: setattr(self, "deleted", True))

    def selected_frame(self):
        return SimpleNamespace(architecture=lambda: SimpleNamespace(
            disassemble=lambda *_, **__: [
                {"asm": "mov 0x8(%rsp),%edi"},
                {"asm": "mov $0xe7,%eax"}, {"asm": "syscall"},
            ]
        ))

    def parse_and_eval(self, value):
        if value == "$pc":
            return 1
        if value.startswith("*(int*)"):
            return self.requested
        return SimpleNamespace(dereference=lambda: self.native_exit)

    def finish(self):
        self.inferior.pid = 0
        event = SimpleNamespace(inferior=self.inferior)
        if self.actual is not None:
            event.exit_code = self.actual
        for handler in self.handlers:
            handler(event)

    def native_exit(self, code):
        if self.unknown:
            raise DebuggerError("unexpected inferior call failure")
        if self.thread_exit:
            raise DebuggerError("Command aborted, thread exited.")
        self.finish()
        raise DebuggerError("inferior exited during the call")

    def execute(self, command, **_):
        self.commands.append(command)
        if command == "info address exit":
            return "Symbol exit is at 0x1234"
        if command == "continue":
            self.finish()


class ProfileExitTest(unittest.TestCase):
    def test_actual_process_exit_controls_the_debugger_status(self):
        for requested, actual, thread_exit in (
            (0, 0, False), (0, 0, True), (0, 99, False),
            (0, 99, True), (17, 17, False),
        ):
            with self.subTest(requested=requested, actual=actual, thread_exit=thread_exit):
                debugger = Debugger(requested, actual, thread_exit)
                with contextlib.redirect_stdout(io.StringIO()):
                    profile.run(debugger)
                self.assertEqual("quit " + str(actual), debugger.commands[-1])
                self.assertEqual(thread_exit or requested != 0, "continue" in debugger.commands)
                self.assertTrue(debugger.deleted)
                self.assertEqual([], debugger.handlers)

    def test_missing_exit_status_and_unrelated_call_failures_stay_failures(self):
        for debugger in (Debugger(actual=None), Debugger(unknown=True)):
            with self.subTest(debugger=debugger), contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaises(DebuggerError):
                    profile.run(debugger)
                self.assertFalse(any(command.startswith("quit ") for command in debugger.commands))
                self.assertTrue(debugger.deleted)
                self.assertEqual([], debugger.handlers)

    def test_breakpoint_setup_failure_disconnects_the_exit_observer(self):
        debugger = Debugger()
        def fail(*_, **__):
            raise DebuggerError("breakpoint setup failed")
        debugger.Breakpoint = fail
        with self.assertRaisesRegex(DebuggerError, "breakpoint setup"):
            profile.run(debugger)
        self.assertEqual([], debugger.handlers)
        self.assertNotIn("run", debugger.commands)
