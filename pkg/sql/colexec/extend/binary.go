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

func (e *BinaryExtend) IsLogical() bool {
	typ := overload.IsLogical(e.Op)
	if typ == overload.MustLogical {
		return true
	} else if typ == overload.MayLogical {
		return e.Left.IsLogical() && e.Right.IsLogical()
	} else {
		return false
	}
}

func (_ *BinaryExtend) IsConstant() bool {
	return false
}

func (e *BinaryExtend) Attributes() []string {
	return append(e.Left.Attributes(), e.Right.Attributes()...)
}

func (e *BinaryExtend) ExtendAttributes() []*Attribute {
	return append(e.Left.ExtendAttributes(), e.Right.ExtendAttributes()...)
}

func (e *BinaryExtend) ReturnType() types.T {
	if fn, ok := BinaryReturnTypes[e.Op]; ok {
		return fn(e.Left, e.Right)
	}
	return types.T_any
}

func (e *BinaryExtend) Eval(bat *batch.Batch, proc *process.Process) (*vector.Vector, types.T, error) {
	l, lt, err := e.Left.Eval(bat, proc)
	if err != nil {
		return nil, 0, err
	}
	r, rt, err := e.Right.Eval(bat, proc)
	if err != nil {
		return nil, 0, err
	}
	vec, err := overload.BinaryEval(e.Op, lt, rt, e.Left.IsConstant(), e.Right.IsConstant(), l, r, proc)
	if err != nil {
		return nil, 0, err
	}
	return vec, e.ReturnType(), nil
}

func (a *BinaryExtend) Eq(e Extend) bool {
	if b, ok := e.(*BinaryExtend); ok {
		return a.Op == b.Op && a.Left.Eq(b.Left) && a.Right.Eq(b.Right)
	}
	return false
}

func (e *BinaryExtend) String() string {
	if fn, ok := BinaryStrings[e.Op]; ok {
		return fn(e.Left, e.Right)
	}
	return ""
}
