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

	"github.com/reusee/starlarkutil"
	"go.starlark.net/starlark"
)

func loadScript() (
	defs []any,
	err error,
) {
	defer he(&err)

	const scriptFileName = "script.py"

	src, err := os.ReadFile(scriptFileName)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	ce(err)

	_, err = starlark.ExecFile(
		new(starlark.Thread),
		scriptFileName,
		src,
		starlark.StringDict{

			"parallel": starlarkutil.MakeFunc("parallel", func(n int) {
				defs = append(defs, func() Parallel {
					return Parallel(n)
				})
			}),
		},
	)
	ce(err)

	return
}
