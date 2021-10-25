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

package starcount

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggregation"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
		a.cnt += int64(n)
	} else {
		a.cnt += int64(vec.Length())
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
	vs := encoding.DecodeInt64Slice(data[:8])
	vs[0] = a.cnt
	vec.Col = vs
	vec.Data = data
	return vec, nil
}
