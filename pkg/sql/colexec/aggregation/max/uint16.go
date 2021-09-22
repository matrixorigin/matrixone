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
	"matrixone/pkg/vm/process"
)

func NewUint16(typ types.Type) *uint16Max {
	return &uint16Max{typ: typ}
}

func (a *uint16Max) Reset() {
	a.v = 0
	a.cnt = 0
}

func (a *uint16Max) Type() types.Type {
	return a.typ
}

func (a *uint16Max) Dup() aggregation.Aggregation {
	return &uint16Max{typ: a.typ}
}

func (a *uint16Max) Fill(sels []int64, vec *vector.Vector) error {
	if n := len(sels); n > 0 {
		v := max.Uint16MaxSels(vec.Col.([]uint16), sels)
		if a.cnt == 0 || v > a.v {
			a.v = v
		}
		a.cnt += int64(n - vec.Nsp.FilterCount(sels))
	} else {
		v := max.Uint16Max(vec.Col.([]uint16))
		if a.cnt == 0 || v > a.v {
			a.v = v
		}
		a.cnt += int64(vec.Length() - vec.Nsp.Length())
	}
	return nil
}

func (a *uint16Max) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return a.v
}

func (a *uint16Max) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	data, err := proc.Alloc(2)
	if err != nil {
		return nil, err
	}
	vec := vector.New(a.typ)
	vs := encoding.DecodeUint16Slice(data[:2])
	vs[0] = a.v
	if a.cnt == 0 {
		vec.Nsp.Add(0)
	}
	vec.Col = vs
	vec.Data = data
	return vec, nil
}
