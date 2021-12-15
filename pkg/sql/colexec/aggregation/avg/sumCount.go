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

package avg

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggregation"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func NewSumCount(typ types.Type) *sumCountAvg {
	return &sumCountAvg{typ: typ}
}

func (a *sumCountAvg) Reset() {
	a.cnt = 0
	a.sum = 0
}

func (a *sumCountAvg) Type() types.Type {
	return a.typ
}

func (a *sumCountAvg) Dup() aggregation.Aggregation {
	return &sumCountAvg{typ: a.typ}
}

func (a *sumCountAvg) Fill(sels []int64, vec *vector.Vector) error {
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

func (a *sumCountAvg) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return a.sum / float64(a.cnt)
}

func (a *sumCountAvg) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	data, err := proc.Alloc(8)
	if err != nil {
		return nil, err
	}
	vec := vector.New(a.typ)
	vs := encoding.DecodeFloat64Slice(data[:8])
	if a.cnt == 0 {
		vs[0] = 0
		vec.Nsp.Add(0)
	} else {
		vs[0] = float64(a.sum) / float64(a.cnt)
	}
	vec.Col = vs
	vec.Data = data
	return vec, nil
}
