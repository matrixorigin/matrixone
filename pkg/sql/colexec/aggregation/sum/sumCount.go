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

package sum

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggregation"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func NewSumCount(typ types.Type) *sumCount {
	return &sumCount{typ: typ}
}

func (a *sumCount) Reset() {
	a.cnt = 0
	a.sum = 0
}

func (a *sumCount) Type() types.Type {
	return a.typ
}

func (a *sumCount) Dup() aggregation.Aggregation {
	return &sumCount{typ: a.typ}
}

func (a *sumCount) Fill(sels []int64, vec *vector.Vector) error {
	vs := vec.Col.([][]interface{})
	if n := len(sels); n > 0 {
		for _, sel := range sels {
			if len(vs[sel]) > 0 {
				a.cnt += vs[sel][0].(int64)
				a.sum += vs[sel][1].(float64)
			}
		}
	} else {
		for _, v := range vs {
			if len(v) > 0 {
				a.cnt += v[0].(int64)
				a.sum += v[1].(float64)
			}
		}
	}
	return nil
}

func (a *sumCount) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return []interface{}{a.cnt, a.sum}
}

func (a *sumCount) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	vec := vector.New(a.typ)
	vec.SetCol([][]interface{}{[]interface{}{a.cnt, a.sum}})
	return vec, nil
}
