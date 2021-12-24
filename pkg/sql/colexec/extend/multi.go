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

package extend

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (e *MultiExtend) IsLogical() bool {
	typ := overload.IsLogical(e.Op)
	if typ == overload.MustLogical {
		return true
	} else if typ == overload.MayLogical {
		for _, extend := range e.Args {
			if !extend.IsLogical() {
				return false
			}
		}
		return true
	} else {
		return false
	}
}

func (_ *MultiExtend) IsConstant() bool {
	return false
}

func (e *MultiExtend) Attributes() []string {
	var attrs []string
	for _, arg := range e.Args {
		attrs = append(attrs, arg.Attributes()...)
	}
	return attrs
}

func (e *MultiExtend) ReturnType() types.T {
	if fn, ok := MultiReturnTypes[e.Op]; ok {
		return fn(e.Args)
	}
	return types.T_any
}

func (e *MultiExtend) Eval(bat *batch.Batch, proc *process.Process) (*vector.Vector, types.T, error) {
	var typ types.T

	bs := make([]bool, len(e.Args))
	vecs := make([]*vector.Vector, len(e.Args))
	for i, arg := range e.Args {
		vec, t, err := arg.Eval(bat, proc)
		if err != nil {
			return nil, 0, err
		}
		vecs[i] = vec
		bs[i] = arg.IsConstant()
		if i == 0 {
			typ = t
		}
	}
	vec, err := overload.MultiEval(e.Op, typ, bs, vecs, proc)
	if err != nil {
		return nil, 0, err
	}
	return vec, e.ReturnType(), nil
}

func (a *MultiExtend) Eq(e Extend) bool {
	b := e.(*MultiExtend)
	if a.Op != b.Op {
		return false
	}
	if len(a.Args) != len(b.Args) {
		return false
	}
	for i, arg := range a.Args {
		if !arg.Eq(b.Args[i]) {
			return false
		}
	}
	return true
}

func (e *MultiExtend) String() string {
	if fn, ok := MultiStrings[e.Op]; ok {
		return fn(e.Args)
	}
	return ""
}
