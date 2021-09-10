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

package max

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/vectorize/max"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
)

func NewInt32(typ types.Type) *int32Max {
	return &int32Max{typ: typ}
}

func (a *int32Max) Reset() {
	a.v = 0
	a.cnt = 0
}

func (a *int32Max) Type() types.Type {
	return a.typ
}

func (a *int32Max) Dup() aggregation.Aggregation {
	return &int32Max{typ: a.typ}
}

func (a *int32Max) Fill(sels []int64, vec *vector.Vector) error {
	if n := len(sels); n > 0 {
		v := max.Int32MaxSels(vec.Col.([]int32), sels)
		if a.cnt == 0 || v > a.v {
			a.v = v
		}
		a.cnt += int64(n - vec.Nsp.FilterCount(sels))
	} else {
		v := max.Int32Max(vec.Col.([]int32))
		a.cnt += int64(vec.Length() - vec.Nsp.Length())
		if a.cnt == 0 || v > a.v {
			a.v = v
		}
	}
	return nil
}

func (a *int32Max) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return a.v
}

func (a *int32Max) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	data, err := proc.Alloc(4)
	if err != nil {
		return nil, err
	}
	vec := vector.New(a.typ)
	if a.cnt == 0 {
		vec.Nsp.Add(0)
		vs := []int32{0}
		copy(data[mempool.CountSize:], encoding.EncodeInt32Slice(vs))
		vec.Col = vs
	} else {
		vs := []int32{a.v}
		copy(data[mempool.CountSize:], encoding.EncodeInt32Slice(vs))
		vec.Col = vs
	}
	vec.Data = data
	return vec, nil
}
