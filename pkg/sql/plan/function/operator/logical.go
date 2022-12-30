// Copyright 2021 - 2022 Matrix Origin
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

package operator

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/logical"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type logicFn func(v1, v2, r *vector.Vector) error

type logicType int8

const (
	AND logicType = 0
	OR  logicType = 1
	XOR logicType = 2
)

func Logic(vectors []*vector.Vector, proc *process.Process, cfn logicFn, op logicType) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	if left.IsConstNull() || right.IsConstNull() {
		if op == AND {
			return HandleAndNullCol(vectors, proc)
		}

		if op == OR {
			return HandleOrNullCol(vectors, proc)
		}

		if op == XOR {
			if left.IsConstNull() {
				return proc.AllocConstNullVector(boolType, right.Length()), nil
			} else {
				return proc.AllocConstNullVector(boolType, left.Length()), nil
			}
		}
	}

	if left.IsConst() && right.IsConst() {
		vec := proc.AllocScalarVector(boolType)
		if err := cfn(left, right, vec); err != nil {
			return nil, err
		}
		return vec, nil
	}

	length := left.Length()
	if left.IsConst() {
		length = right.Length()
	}
	resultVector := allocateBoolVector(length, proc)
	nulls.Or(left.GetNulls(), right.GetNulls(), resultVector.GetNulls())

	if err := cfn(left, right, resultVector); err != nil {
		return nil, err
	}
	return resultVector, nil
}

func LogicAnd(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Logic(args, proc, logical.And, AND)
}

func LogicOr(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Logic(args, proc, logical.Or, OR)
}

func LogicXor(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Logic(args, proc, logical.Xor, XOR)
}

func LogicNot(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	v1 := vs[0]
	if v1.IsConstNull() {
		return proc.AllocScalarNullVector(boolType), nil
	}
	if v1.IsConst() {
		vec := proc.AllocScalarVector(boolType)
		if err := logical.Not(v1, vec); err != nil {
			return nil, err
		}
		return vec, nil
	}
	length := v1.Length()
	vec := allocateBoolVector(length, proc)
	nulls.Or(v1.GetNulls(), nil, vec.GetNulls())
	if err := logical.Not(v1, vec); err != nil {
		return nil, err
	}
	return vec, nil
}
