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
	"github.com/matrixorigin/matrixone/pkg/vectorize/sum"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func NewFloat(typ types.Type) *floatAvg {
	return &floatAvg{typ: typ}
}

func (a *floatAvg) Reset() {
	a.cnt = 0
	a.sum = 0
}

func (a *floatAvg) Type() types.Type {
	return a.typ
}

func (a *floatAvg) Dup() aggregation.Aggregation {
	return &floatAvg{typ: a.typ}
}

func (a *floatAvg) Fill(sels []int64, vec *vector.Vector) error {
	if n := len(sels); n > 0 {
		switch vec.Typ.Oid {
		case types.T_float32:
			a.sum += float64(sum.Float32SumSels(vec.Col.([]float32), sels))
		case types.T_float64:
			a.sum += sum.Float64SumSels(vec.Col.([]float64), sels)
		}
		a.cnt += int64(n - vec.Nsp.FilterCount(sels))
	} else {
		switch vec.Typ.Oid {
		case types.T_float32:
			a.sum += float64(sum.Float32Sum(vec.Col.([]float32)))
		case types.T_float64:
			a.sum += sum.Float64Sum(vec.Col.([]float64))
		}
		a.cnt += int64(vec.Length() - vec.Nsp.Length())
	}
	return nil
}

func (a *floatAvg) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return a.sum / float64(a.cnt)
}

func (a *floatAvg) EvalCopy(proc *process.Process) (*vector.Vector, error) {
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
