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
	"runtime"
	"strconv"

	"github.com/google/uuid"
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
)

func (def Def) CmdGenConfigs() Commands {
	return Commands{

		"gen-configs": func(args []string) {

			num := 128
			if len(args) > 0 {
				var err error
				num, err = strconv.Atoi(args[0])
				ce(err)
			}

			sem := make(chan struct{}, runtime.NumCPU())
			for i := 0; i < num; i++ {
				i := i
				sem <- struct{}{}
				go func() {
					defer func() {
						<-sem
					}()

					NewScope().Call(func(
						write fz.WriteConfig,
						id uuid.UUID,
						writeFile fz.WriteTestDataFile,
					) {

						f, err, done := writeFile("config", "xml")
						ce(err)
						ce(write(f))
						ce(done())

						pt("generated %d / %d %s\n", i+1, num, id.String())

					})
				}()
			}
			for i := 0; i < cap(sem); i++ {
				sem <- struct{}{}
			}

		},
	}
}
