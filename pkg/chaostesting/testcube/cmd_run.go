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
	"os"
	"runtime"

	"github.com/fatih/color"
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

	runOne := func(configPath string) (err error) {
		defer func() {

			if p := recover(); p != nil {
				color.Red("PANIC %s", configPath)
				panic(p)
			}

			if err == nil {
				color.Green("OK %s", configPath)
			} else {
				color.Red("FAILED %s", configPath)
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

	return Commands{
		"run": func(args []string) {

			sem := make(chan struct{}, parallel)
			for _, run := range getCases(args) {
				run := run
				sem <- struct{}{}
				go func() {
					defer func() {
						<-sem
					}()
					ce(runOne(run.ConfigPath))
				}()
			}
			for i := 0; i < cap(sem); i++ {
				sem <- struct{}{}
			}

		},
	}
}
