# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Allocation profiling only. Go's direct exit_group skips native atexit.

"""Finalize native resources for allocation profiling and preserve real exits."""
import re


def run(gdb):
    for command in (
        "set pagination off", "set confirm off", "set startup-with-shell off",
        "set auto-load off", "set breakpoint pending on",
        "handle SIGURG nostop noprint pass", "handle SIGPIPE nostop noprint pass",
        "handle SIGTERM nostop noprint pass", "handle SIGINT nostop noprint pass",
    ):
        gdb.execute(command)
    inferior = gdb.selected_inferior().num
    observed = {"exited": False, "code": None}

    def exited(event):
        if event.inferior.num == inferior:
            observed.update(exited=True, code=getattr(event, "exit_code", None))

    gdb.events.exited.connect(exited)
    breakpoint = None
    try:
        breakpoint = gdb.Breakpoint("runtime.exit", internal=True)
        gdb.execute("run")
        if not observed["exited"]:
            gdb.execute("set language c")
            instructions = gdb.selected_frame().architecture().disassemble(
                int(gdb.parse_and_eval("$pc")), count=3
            )
            if (
                len(instructions) != 3
                or "0x8(%rsp),%edi" not in instructions[0]["asm"].replace(" ", "")
                or "$0xe7,%eax" not in instructions[1]["asm"].replace(" ", "")
                or instructions[2]["asm"] != "syscall"
            ):
                raise gdb.GdbError("unsupported runtime exit ABI")
            code = int(gdb.parse_and_eval("*(int*)($rsp+8)"))
            print("verified runtime exit code:", code)
            if code:
                gdb.execute("continue")
            else:
                symbol = gdb.execute("info address exit", to_string=True)
                address = re.search(r"0x[0-9a-f]+", symbol).group(0)
                function = gdb.parse_and_eval("(void (*)(int)) " + address).dereference()
                try:
                    function(code)
                except gdb.error as error:
                    if not observed["exited"]:
                        if "thread exited" not in str(error).lower():
                            raise
                        # An inferior call can lose its calling thread before
                        # the remaining threads finish exit. Observe the process.
                        gdb.execute("continue")
        if not observed["exited"] or type(observed["code"]) is not int:
            raise gdb.GdbError("profile target has no observed process exit status")
        print("observed native exit code:", observed["code"])
        gdb.execute("quit " + str(observed["code"]))
    finally:
        gdb.events.exited.disconnect(exited)
        if breakpoint is not None:
            breakpoint.delete()


if __name__ == "__main__":
    import gdb
    run(gdb)
