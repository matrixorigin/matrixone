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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorize/add"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func initOperators() {
	var err error

	for name, fs := range operators {
		for _, f := range fs {
			err = appendFunction(name, f)
			if err != nil {
				panic(err)
			}
		}
	}
}

// operators contains the operator function indexed by name.
var operators = map[string][]Function{
	"=": {
		{
			Index: 0,
			Flag:  plan.Function_STRICT,
			Kind:  COMPARISON_OPERATOR,
			Args: []types.T{
				types.T_int8, // left part of +
				types.T_int8, // right part of +
			},
			ReturnTyp: types.T_uint8,
			Fn: func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
				lv, rv := vs[0], vs[1]
				lvs, rvs := lv.Col.([]uint8), rv.Col.([]uint8)
				vec, err := process.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vector.SetCol(vec, add.Uint8Add(lvs, rvs, rs))
				if lv.Ref == 0 {
					process.Put(proc, lv)
				}
				if rv.Ref == 0 {
					process.Put(proc, rv)
				}
				return vec, nil
			},
			TypeCheckFn: strictTypeCheck,
		},
	},
}
