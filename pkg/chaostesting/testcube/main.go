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
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"time"

	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
)

func main() {

	NewScope().Call(func(
		cmds Commands,
		cleanup fz.Cleanup,
		enableCPUProfile EnableCPUProfile,
		httpServerAddr HTTPServerAddr,
	) {

		// cpu profile
		if enableCPUProfile {
			f, err := os.Create(fmt.Sprintf("cpu-profile-%s", time.Now().Format("2006-01-02_15-04-05")))
			ce(err)
			ce(pprof.StartCPUProfile(f))
			defer func() {
				pprof.StopCPUProfile()
				ce(f.Close())
			}()
		}

		// http server
		if httpServerAddr != "" {
			go http.ListenAndServe(string(httpServerAddr), nil)
		}

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

		ctx, cancel := signal.NotifyContext(context.Background(), os.Kill, os.Interrupt)

		go func() {
			fn(os.Args[2:])
			cancel()
		}()

		<-ctx.Done()
		cleanup()

	})
}
