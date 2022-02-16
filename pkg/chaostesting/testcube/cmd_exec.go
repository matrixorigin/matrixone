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
	"os"
	"os/exec"
	"time"

	"github.com/google/uuid"
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
	"github.com/reusee/e4"
)

func (_ Def) CmdExec(
	getCases GetTestCases,
	parallel Parallel,
	read fz.ReadConfig,
) Commands {

	execOne := func(id uuid.UUID, configPath string) {

		t0 := time.Now()
		pt("RUN %v\n", id)
		defer func() {
			pt("DONE %v in %v\n", id, time.Since(t0))
		}()

		// read configs
		content, err := os.ReadFile(configPath)
		ce(err)
		defs, err := read(bytes.NewReader(content))
		ce(err)

		// run
		NewScope().Fork(defs...).Fork(

			// configs
			func(
				parallel Parallel,
			) fz.ExecuteTimeout {
				return fz.ExecuteTimeout(time.Minute * time.Duration(parallel))
			},
		).Call(func(
			writeFile fz.WriteTestDataFile,
			clearFile fz.ClearTestDataFile,
			filePath fz.TestDataFilePath,
			id uuid.UUID,
		) {

			var err error
			defer he(&err, e4.Do(func() {
				pt("%s error: %s\n", id, err)
			}))

			ce(clearFile("exec", "output"))

			cmd := exec.Command("testcube", "run", configPath)
			output, err := cmd.CombinedOutput()

			if err != nil {
				f, err, done := writeFile("exec", "output")
				ce(err)
				_, err = f.Write(output)
				ce(err)
				ce(done())
				pt("%s\n", filePath(id, "exec", "output"))
				pt("%s\n", filePath(id, "cube", "log"))
			}

		})

		return
	}

	return Commands{
		// run test cases in distinct processes

		"exec": func(args []string) {

			sem := make(chan struct{}, parallel)
			for {
				sem <- struct{}{}
				go func() {
					defer func() {
						<-sem
					}()

					var configPath string
					var id uuid.UUID

					NewScope().Call(func(
						write fz.WriteConfig,
						_id uuid.UUID,
						writeFile fz.WriteTestDataFile,
						filePath fz.TestDataFilePath,
					) {
						f, err, done := writeFile("config", "xml")
						ce(err)
						ce(write(f))
						ce(done())
						configPath = filePath(_id, "config", "xml")
						id = _id
					})

					execOne(id, configPath)

				}()
			}

		},

		"execall": func(args []string) {

			sem := make(chan struct{}, parallel)
			for _, c := range getCases(args) {
				c := c
				sem <- struct{}{}
				go func() {
					defer func() {
						<-sem
					}()
					execOne(c.ID, c.ConfigPath)
				}()
			}
			for i := 0; i < cap(sem); i++ {
				sem <- struct{}{}
			}

		},
	}
}
