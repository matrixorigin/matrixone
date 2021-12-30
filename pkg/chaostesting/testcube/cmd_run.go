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
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/fatih/color"
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
	"github.com/reusee/e4"
)

func (_ Def) CmdRun(
	read fz.ReadConfig,
) Commands {

	runOne := func(configPath string) (err error) {
		defer func() {
			if err == nil {
				color.Green("OK %s", configPath)
			} else {
				color.Red("FAILED %s", configPath)
			}
		}()
		defer he(&err,
			e4.Info("config file: %s", configPath),
		)

		content, err := os.ReadFile(configPath)
		ce(err)
		defs, err := read(bytes.NewReader(content))
		ce(err)
		NewScope().Fork(defs...).Call(func(
			execute fz.Execute,
		) {
			ce(execute())
		})

		return
	}

	return Commands{
		"run": func(args []string) {

			if len(args) == 0 {
				// run all
				sem := make(chan struct{}, runtime.NumCPU())
				ce(filepath.WalkDir(configFilesDir, func(path string, entry fs.DirEntry, err error) error {
					if err != nil {
						return err
					}
					if entry.IsDir() {
						return nil
					}
					if !strings.HasSuffix(path, ".xml") {
						return nil
					}

					sem <- struct{}{}
					go func() {
						defer func() {
							<-sem
						}()
						ce(runOne(path))
					}()

					return nil
				}), e4.Ignore(os.ErrNotExist))
				for i := 0; i < cap(sem); i++ {
					sem <- struct{}{}
				}

			} else {
				// run some
				sem := make(chan struct{}, runtime.NumCPU())
				for _, path := range args {
					path := filepath.Join(configFilesDir, path+".xml")
					sem <- struct{}{}
					go func() {
						defer func() {
							<-sem
						}()

						ce(runOne(path))

					}()
				}
				for i := 0; i < cap(sem); i++ {
					sem <- struct{}{}
				}

			}

		},
	}
}
