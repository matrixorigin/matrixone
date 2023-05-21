// Copyright 2022 Matrix Origin
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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

type TGenericOfIn interface {
	constraints.Integer | constraints.Float | bool | types.Uuid |
		types.Time | types.Timestamp | types.Date | types.Datetime | types.Decimal64 | types.Decimal128
}

type opOperatorFixedIn[T TGenericOfIn] struct {
	ready bool
	mp    map[T]bool
}

type opOperatorStrIn struct {
	ready bool
	mp    map[string]bool
}

func newOpOperatorFixedIn[T TGenericOfIn]() *opOperatorFixedIn[T] {
	op := new(opOperatorFixedIn[T])
	op.ready = false
	return op
}

func newOpOperatorStrIn() *opOperatorStrIn {
	op := new(opOperatorStrIn)
	op.ready = false
	return op
}

func (op *opOperatorFixedIn[T]) init(tuple *vector.Vector) {
	op.ready = true

	if tuple.IsConstNull() {
		op.mp = make(map[T]bool)
		return
	}
	p := vector.GenerateFunctionFixedTypeParameter[T](tuple)

	if tuple.IsConst() {
		v, _ := p.GetValue(0)
		op.mp = make(map[T]bool, 1)
		op.mp[v] = true
		return
	}

	op.mp = make(map[T]bool, tuple.Length())
	for i := uint64(0); i < uint64(tuple.Length()); i++ {
		v, null := p.GetValue(i)
		if !null {
			op.mp[v] = true
		}
	}
}

func (op *opOperatorStrIn) init(tuple *vector.Vector) {
	op.ready = true

	if tuple.IsConstNull() {
		op.mp = make(map[string]bool)
		return
	}
	p := vector.GenerateFunctionStrParameter(tuple)

	if tuple.IsConst() {
		v, _ := p.GetStrValue(0)
		op.mp = make(map[string]bool, 1)
		op.mp[string(v)] = true
		return
	}

	op.mp = make(map[string]bool)
	for i := uint64(0); i < uint64(tuple.Length()); i++ {
		v, null := p.GetStrValue(i)
		if !null {
			op.mp[string(v)] = true
		}
	}
}

func (op *opOperatorFixedIn[T]) operatorIn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	if !op.ready {
		op.init(parameters[1])
	}

	p := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	rs := vector.MustFunctionResult[bool](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p.GetValue(i)
		if null {
			if err := rs.Append(false, true); err != nil {
				return err
			}
		} else {
			_, ok := op.mp[v]
			if err := rs.Append(ok, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (op *opOperatorFixedIn[T]) operatorNotIn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	if !op.ready {
		op.init(parameters[1])
	}

	p := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	rs := vector.MustFunctionResult[bool](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p.GetValue(i)
		if null {
			if err := rs.Append(false, true); err != nil {
				return err
			}
		} else {
			_, ok := op.mp[v]
			if err := rs.Append(!ok, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (op *opOperatorStrIn) operatorIn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	if !op.ready {
		op.init(parameters[1])
	}

	p := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[bool](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p.GetStrValue(i)
		if null {
			if err := rs.Append(false, true); err != nil {
				return err
			}
		} else {
			_, ok := op.mp[string(v)]
			if err := rs.Append(ok, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (op *opOperatorStrIn) operatorNotIn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	if !op.ready {
		op.init(parameters[1])
	}

	p := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[bool](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p.GetStrValue(i)
		if null {
			if err := rs.Append(false, true); err != nil {
				return err
			}
		} else {
			_, ok := op.mp[string(v)]
			if err := rs.Append(!ok, false); err != nil {
				return err
			}
		}
	}
	return nil
}
