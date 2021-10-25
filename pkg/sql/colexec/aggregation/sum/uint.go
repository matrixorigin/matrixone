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
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggregation"
	"github.com/matrixorigin/matrixone/pkg/vectorize/sum"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func NewUint(typ types.Type) *uintSum {
	return &uintSum{typ: typ}
}

func (a *uintSum) Reset() {
	a.cnt = 0
	a.sum = 0
}

func (a *uintSum) Type() types.Type {
	return a.typ
}

func (a *uintSum) Dup() aggregation.Aggregation {
	return &uintSum{typ: a.typ}
}

func (a *uintSum) Fill(sels []int64, vec *vector.Vector) error {
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
		case types.T_uint8:
			a.sum += sum.Uint8Sum(vec.Col.([]uint8))
		case types.T_uint16:
			a.sum += sum.Uint16Sum(vec.Col.([]uint16))
		case types.T_uint32:
			a.sum += sum.Uint32Sum(vec.Col.([]uint32))
		case types.T_uint64:
			a.sum += sum.Uint64Sum(vec.Col.([]uint64))
		}
		a.cnt += int64(vec.Length() - vec.Nsp.Length())
	}
	return nil
}

func (a *uintSum) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return a.sum
}

func (a *uintSum) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	data, err := proc.Alloc(8)
	if err != nil {
		return nil, err
	}
	vec := vector.New(a.typ)
	vs := encoding.DecodeUint64Slice(data[:8])
	vs[0] = a.sum
	if a.cnt == 0 {
		vec.Nsp.Add(0)
	}
	vec.Col = vs
	vec.Data = data
	return vec, nil
}
