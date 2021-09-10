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

package count

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
)

func New(typ types.Type) *count {
	return &count{typ: typ}
}

func (a *count) Reset() {
	a.cnt = 0
}

func (a *count) Type() types.Type {
	return a.typ
}

func (a *count) Dup() aggregation.Aggregation {
	return &count{typ: a.typ}
}

func (a *count) Fill(sels []int64, vec *vector.Vector) error {
	if n := len(sels); n > 0 {
		a.cnt += int64(n - vec.Nsp.FilterCount(sels))
	} else {
		a.cnt += int64(vec.Length() - vec.Nsp.Length())
	}
	return nil
}

func (a *count) Eval() interface{} {
	return a.cnt
}

func (a *count) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	data, err := proc.Alloc(8)
	if err != nil {
		return nil, err
	}
	vec := vector.New(a.typ)
	vs := []int64{a.cnt}
	copy(data[mempool.CountSize:], encoding.EncodeInt64Slice(vs))
	vec.Col = vs
	vec.Data = data
	return vec, nil
}
