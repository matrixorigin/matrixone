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
	"matrixone/pkg/vectorize/sum"
	"matrixone/pkg/vm/process"
)

func NewUint(typ types.Type) *uintAvg {
	return &uintAvg{typ: typ}
}

func (a *uintAvg) Reset() {
	a.cnt = 0
	a.sum = 0
}

func (a *uintAvg) Type() types.Type {
	return a.typ
}

func (a *uintAvg) Dup() aggregation.Aggregation {
	return &uintAvg{typ: a.typ}
}

func (a *uintAvg) Fill(sels []int64, vec *vector.Vector) error {
	if n := len(sels); n > 0 {
		switch vec.Typ.Oid {
		case types.T_uint8:
			a.sum += sum.Uint8SumSels(vec.Col.([]uint8), sels)
		case types.T_uint16:
			a.sum += sum.Uint16SumSels(vec.Col.([]uint16), sels)
		case types.T_uint32:
			a.sum += sum.Uint32SumSels(vec.Col.([]uint32), sels)
		case types.T_uint64:
			a.sum += sum.Uint64SumSels(vec.Col.([]uint64), sels)
		}
		a.cnt += int64(n - vec.Nsp.FilterCount(sels))
	} else {
		switch vec.Typ.Oid {
		case types.T_int8:
			a.sum += sum.Uint8Sum(vec.Col.([]uint8))
		case types.T_int16:
			a.sum += sum.Uint16Sum(vec.Col.([]uint16))
		case types.T_int32:
			a.sum += sum.Uint32Sum(vec.Col.([]uint32))
		case types.T_int64:
			a.sum += sum.Uint64Sum(vec.Col.([]uint64))
		}
		a.cnt += int64(vec.Length() - vec.Nsp.Length())
	}
	return nil
}

func (a *uintAvg) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return float64(a.sum) / float64(a.cnt)
}

func (a *uintAvg) EvalCopy(proc *process.Process) (*vector.Vector, error) {
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
