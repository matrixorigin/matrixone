// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import "os"

func main() {

	NewScope().Call(func(
		cmds Commands,
	) {

		printCommands := func() {
			pt("available commands:")
			for name := range cmds {
				pt(" %s", name)
			}
			pt("\n")
		}

		if len(os.Args) < 2 {
			printCommands()
			return
		}

		cmd := os.Args[1]
		fn, ok := cmds[cmd]
		if !ok {
			pt("no such command\n")
			printCommands()
			return
		}
		fn(os.Args[2:])

	})
}
