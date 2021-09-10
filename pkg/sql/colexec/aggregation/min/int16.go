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

package min

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/vectorize/min"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
)

func NewInt16(typ types.Type) *int16Min {
	return &int16Min{typ: typ}
}

func (a *int16Min) Reset() {
	a.v = 0
	a.cnt = 0
}

func (a *int16Min) Type() types.Type {
	return a.typ
}

func (a *int16Min) Dup() aggregation.Aggregation {
	return &int16Min{typ: a.typ}
}

func (a *int16Min) Fill(sels []int64, vec *vector.Vector) error {
	if n := len(sels); n > 0 {
		v := min.Int16MinSels(vec.Col.([]int16), sels)
		if a.cnt == 0 || v < a.v {
			a.v = v
		}
		a.cnt += int64(n - vec.Nsp.FilterCount(sels))
	} else {
		v := min.Int16Min(vec.Col.([]int16))
		a.cnt += int64(vec.Length() - vec.Nsp.Length())
		if a.cnt == 0 || v < a.v {
			a.v = v
		}
	}
	return nil
}

func (a *int16Min) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return a.v
}

func (a *int16Min) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	data, err := proc.Alloc(2)
	if err != nil {
		return nil, err
	}
	vec := vector.New(a.typ)
	if a.cnt == 0 {
		vec.Nsp.Add(0)
		vs := []int16{0}
		copy(data[mempool.CountSize:], encoding.EncodeInt16Slice(vs))
		vec.Col = vs
	} else {
		vs := []int16{a.v}
		copy(data[mempool.CountSize:], encoding.EncodeInt16Slice(vs))
		vec.Col = vs
	}
	vec.Data = data
	return vec, nil
}
