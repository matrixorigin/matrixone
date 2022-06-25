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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/fatih/color"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixcube/config"
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
	"github.com/reusee/e4"
)

type Parallel int

func (_ Def) Parallel() Parallel {
	return Parallel(runtime.NumCPU())
}

func (_ Def) CmdRun(
	read fz.ReadConfig,
	getCases GetTestCases,
	parallel Parallel,
) Commands {

	errPanic := fmt.Errorf("panic")

	_run := func(configPath string) (err error) {

		defer func() {

			if err != nil {
				return
			}

			if p := recover(); p != nil {
				err = we.With(
					e4.Info("%v", p),
				)(errPanic)
				return
			}

		}()
		defer he(&err,
			e4.Info("config file: %s", configPath),
		)

		// read configs
		content, err := os.ReadFile(configPath)
		ce(err)
		defs, err := read(bytes.NewReader(content))
		ce(err)

		// scope
		scope := NewScope().Fork(defs...)

		// load node configs from source
		var src fz.NodeConfigSources
		scope.Assign(&src)
		scope = scope.Fork(func() (confs NodeConfigs) {
			for _, src := range src.Sources {
				var conf config.Config
				ce(json.Unmarshal(
					[]byte(src.String),
					&conf,
				))
				confs = append(confs, &conf)
			}
			return
		})

		// run
		scope.Call(func(
			execute fz.Execute,
			cleanup fz.Cleanup,
		) {
			defer cleanup()
			ce(execute())
		})

		return
	}

	numRetry := 0

	run := func(configPath string, retry int) (err error) {
		retried := false
		for {
			t0 := time.Now()
			if retried {
				pt("RETRY %s\n", configPath)
			} else {
				pt("START %s\n", configPath)
			}
			err := _run(configPath)
			if err == nil {
				if retried {
					color.Green("RETRY OK %s, %v", configPath, time.Since(t0))
				} else {
					color.Green("OK %s, %v", configPath, time.Since(t0))
				}
				return nil
			}
			//if !errors.Is(err, errPanic) || retry == 0 {
			if retry == 0 {
				color.Red("FAILED %s, %v", configPath, time.Since(t0))
				return err
			}
			retry--
			retried = true
		}
	}

	return Commands{
		// run test cases in single process

		"run": func(args []string) {

			if len(args) > 0 {
				for _, path := range args {
					ce(run(path, numRetry))
				}
				return
			}

			sem := make(chan struct{}, parallel)
			for {
				sem <- struct{}{}
				go func() {
					defer func() {
						<-sem
					}()

					var configPath string

					NewScope().Call(func(
						write fz.WriteConfig,
						id uuid.UUID,
						writeFile fz.WriteTestDataFile,
						filePath fz.TestDataFilePath,
					) {
						f, err, done := writeFile("config", "xml")
						ce(err)
						ce(write(f))
						ce(done())
						configPath = filePath(id, "config", "xml")
					})

					ce(run(configPath, numRetry))

				}()
			}

		},

		"runall": func(args []string) {

			sem := make(chan struct{}, parallel)
			for _, kase := range getCases(args) {
				kase := kase
				sem <- struct{}{}
				go func() {
					defer func() {
						<-sem
					}()
					ce(run(kase.ConfigPath, numRetry))
				}()
			}
			for i := 0; i < cap(sem); i++ {
				sem <- struct{}{}
			}

		},
	}
}
