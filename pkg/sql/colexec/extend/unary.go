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
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/sql/colexec/extend/overload"
	"matrixone/pkg/vm/process"
)

func (e *UnaryExtend) IsLogical() bool {
	return overload.IsLogical(e.Op)
}

func (_ *UnaryExtend) IsConstant() bool {
	return false
}

func (e *UnaryExtend) Attributes() []string {
	return e.E.Attributes()
}

func (e *UnaryExtend) ReturnType() types.T {
	if fn, ok := UnaryReturnTypes[e.Op]; ok {
		return fn(e.E)
	}
	return types.T_any
}

func (e *UnaryExtend) Eval(bat *batch.Batch, proc *process.Process) (*vector.Vector, types.T, error) {
	vs, typ, err := e.E.Eval(bat, proc)
	if err != nil {
		return nil, 0, err
	}
	vec, err := overload.UnaryEval(e.Op, typ, e.E.IsConstant(), vs, proc)
	if err != nil {
		return nil, 0, err
	}
	return vec, e.ReturnType(), nil
}

func (a *UnaryExtend) Eq(e Extend) bool {
	if b, ok := e.(*UnaryExtend); ok {
		return a.Op == b.Op && a.E.Eq(b.E)
	}
	return false
}

func (e *UnaryExtend) String() string {
	if fn, ok := UnaryStrings[e.Op]; ok {
		return fn(e.E)
	}
	return ""
}
