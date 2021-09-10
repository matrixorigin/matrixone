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
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
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
			a.cnt += vs[sel][0].(int64)
			a.sum += vs[sel][1].(float64)
		}
	} else {
		for _, v := range vs {
			a.cnt += v[0].(int64)
			a.sum += v[1].(float64)
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
	if a.cnt == 0 {
		vec.Nsp.Add(0)
		vs := []float64{0}
		copy(data[mempool.CountSize:], encoding.EncodeFloat64Slice(vs))
		vec.Col = vs
	} else {
		vs := []float64{float64(a.sum) / float64(a.cnt)}
		copy(data[mempool.CountSize:], encoding.EncodeFloat64Slice(vs))
		vec.Col = vs
	}
	vec.Data = data
	return vec, nil
}
