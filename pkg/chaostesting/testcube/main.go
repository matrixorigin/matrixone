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

import (
	"fmt"
	"net/http"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"time"

	"github.com/felixge/fgprof"
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
)

func main() {

	scope := NewScope()

	var write fz.WriteTestDataFile
	var clear fz.ClearTestDataFile
	scope.Assign(&write, &clear)

	clear("runtime", "trace")
	var enableRuntimeTrace EnableRuntimeTrace
	scope.Assign(&enableRuntimeTrace)
	if enableRuntimeTrace {
		file, err, done := write("runtime", "trace")
		ce(err)
		defer done()
		ce(trace.Start(file))
		defer trace.Stop()
	}

	var enableCPUProfile EnableCPUProfile
	scope.Assign(&enableCPUProfile)
	if enableCPUProfile {
		f, err := os.Create(fmt.Sprintf("cpu-profile-%s", time.Now().Format("2006-01-02_15-04-05")))
		ce(err)
		ce(pprof.StartCPUProfile(f))
		defer func() {
			pprof.StopCPUProfile()
			ce(f.Close())
		}()
	}

	var httpServerAddr HTTPServerAddr
	scope.Assign(&httpServerAddr)
	if httpServerAddr != "" {
		go http.ListenAndServe(string(httpServerAddr), nil)
	}

	var enableFGProfile EnableFGProfile
	scope.Assign(&enableFGProfile)
	if enableFGProfile {
		f, err := os.Create(fmt.Sprintf("fg-profile-%s", time.Now().Format("2006-01-02_15-04-05")))
		ce(err)
		end := fgprof.Start(f, "pprof")
		defer func() {
			ce(end())
			ce(f.Close())
		}()
	}

	var cmds Commands
	scope.Assign(&cmds)
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

	go func() {
		fn(os.Args[2:])
	}()

	var wt fz.RootWaitTree
	scope.Assign(&wt)
	<-wt.Ctx.Done()
	wt.Wait()

	var cleanup fz.Cleanup
	scope.Assign(&cleanup)
	cleanup()

}
