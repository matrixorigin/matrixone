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
# Queries and the packaged service remain unchanged. Nonzero exits stay nonzero.
set pagination off
set confirm off
set startup-with-shell off
set auto-load off
set breakpoint pending on
handle SIGURG nostop noprint pass
handle SIGPIPE nostop noprint pass
handle SIGTERM nostop noprint pass
handle SIGINT nostop noprint pass
python
import gdb, re
class NativeExit(gdb.Breakpoint):
    def stop(self):
        # linux/amd64 ABI0 runtime.exit: mov code+0(FP),edi; SYS_exit_group.
        gdb.execute("set language c")
        instructions = gdb.selected_frame().architecture().disassemble(int(gdb.parse_and_eval("$pc")), count=3)
        if len(instructions) != 3 or "0x8(%rsp),%edi" not in instructions[0]["asm"].replace(" ", "") or "$0xe7,%eax" not in instructions[1]["asm"].replace(" ", "") or instructions[2]["asm"] != "syscall":
            raise gdb.GdbError("unsupported runtime exit ABI")
        code = int(gdb.parse_and_eval("*(int*)($rsp+8)"))
        print("verified runtime exit code:", code)
        if code != 0:
            return False
        symbol = gdb.execute("info address exit", to_string=True)
        address = re.search(r"0x[0-9a-f]+", symbol).group(0)
        function = gdb.parse_and_eval("(void (*)(int)) " + address).dereference()
        try:
            function(code)
        except gdb.error:
            if gdb.selected_inferior().pid != 0:
                raise
        gdb.execute("quit " + str(code))
        return False
NativeExit("runtime.exit", internal=True)
end
run
python
gdb.execute("quit " + str(int(gdb.parse_and_eval("$_exitcode"))))
end
