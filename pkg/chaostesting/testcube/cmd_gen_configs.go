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
	"errors"
	"os"
	"path/filepath"
	"strconv"

	"github.com/google/uuid"
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
)

func (def Def) CmdGenConfigs(
	scope Scope,
) Commands {
	return Commands{

		"gen-configs": func(args []string) {

			num := 128
			if len(args) > 0 {
				var err error
				num, err = strconv.Atoi(args[0])
				ce(err)
			}

			// ensure output dir
			outputDir := "configs"
			_, err := os.Stat(outputDir)
			if errors.Is(err, os.ErrNotExist) {
				err = nil
				ce(os.Mkdir(outputDir, 0755))
			}
			ce(err)

			fzDef := new(fz.Def)
			def2 := new(Def2)

			for i := 0; i < num; i++ {
				scope.Fork(
					// resets
					fzDef.UUID,
					def.NodeConfigs,
					def2.MainAction,
				).Call(func(
					write fz.WriteConfig,
					id uuid.UUID,
				) {

					f, err := os.CreateTemp(outputDir, "*.tmp")
					ce(err)
					ce(write(f))
					ce(f.Close())
					ce(os.Rename(
						f.Name(),
						filepath.Join(outputDir, id.String()+".xml"),
					))
					pt("generated %s\n", id.String())

				})
			}

		},
	}
}
