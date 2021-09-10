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
	"bytes"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/vectorize/max"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
)

func NewStr(typ types.Type) *strMax {
	return &strMax{typ: typ, v: make([]byte, 0, 8)}
}

func (a *strMax) Reset() {
	a.cnt = 0
	a.v = a.v[:0]
}

func (a *strMax) Type() types.Type {
	return a.typ
}

func (a *strMax) Dup() aggregation.Aggregation {
	return &strMax{typ: a.typ, v: make([]byte, 0, 8)}
}

func (a *strMax) Fill(sels []int64, vec *vector.Vector) error {
	if n := len(sels); n > 0 {
		v := max.StrMaxSels(vec.Col.(*types.Bytes), sels)
		if a.cnt == 0 || bytes.Compare(v, a.v) > 0 {
			a.v = append(a.v[:0], v...)
		}
		a.cnt += int64(n - vec.Nsp.FilterCount(sels))
	} else {
		v := max.StrMax(vec.Col.(*types.Bytes))
		if a.cnt == 0 || bytes.Compare(v, a.v) > 0 {
			a.v = append(a.v[:0], v...)
		}
		a.cnt += int64(vec.Length() - vec.Nsp.Length())
	}
	return nil
}

func (a *strMax) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return []byte(a.v)
}

func (a *strMax) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	length := len(a.v)
	if length == 0 {
		length++
	}
	data, err := proc.Alloc(int64(length))
	if err != nil {
		return nil, err
	}
	vec := vector.New(a.typ)
	col := vec.Col.(*types.Bytes)
	col.Data = data[mempool.CountSize:mempool.CountSize]
	col.Offsets = append(col.Offsets, 0)
	if a.cnt == 0 {
		vec.Nsp.Add(0)
	} else {
		col.Data = append(col.Data, a.v...)
		col.Lengths = append(col.Lengths, uint32(len(a.v)))
	}
	vec.Col = col
	vec.Data = data
	return vec, nil
}
