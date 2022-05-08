// Copyright 2021 - 2022 Matrix Origin
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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func initBuiltIns() {
	var err error

	for name, fs := range builtins {
		for _, f := range fs {
			err = appendFunction(name, f)
			if err != nil {
				panic(err)
			}
		}
	}
}

var builtins = map[string][]Function{
	"case": {
		{
			Name: "case_when_int64",
			Flag: plan.Function_NONE,
			Args: MakeUnLimitArgList(func(ts []types.T) bool {
				l := len(ts)
				if l < 3 {
					return false
				}

				for i := 0; i < l-1; i += 2 { //case and then should be int64
					if ts[i] != types.T_int64 && isNotNull(ts[i]) {
						return false
					}
				}
				if l%2 == 1 { // has else part
					if ts[l-1] != types.T_int64 && isNotNull(ts[l-1]) {
						return false
					}
				}
				return true
			}),
			ReturnTyp: types.T_int64,
			ID:        builtinCaseWhenInt64,
			Fn: func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
				// not implement now.
				return nil, nil
			},
		},
	},
}
