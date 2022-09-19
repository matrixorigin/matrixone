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
	if left.IsScalarNull() || right.IsScalarNull() {
		if op == AND {
			return HandleAndNullCol(vectors, proc)
		}

		if op == OR {
			return HandleOrNullCol(vectors, proc)
		}

		if op == XOR {
			if left.IsScalarNull() {
				return proc.AllocConstNullVector(boolType, vector.Length(right)), nil
			} else {
				return proc.AllocConstNullVector(boolType, vector.Length(left)), nil
			}
		}
	}

	if left.IsScalar() && right.IsScalar() {
		vec := proc.AllocScalarVector(boolType)
		if err := cfn(left, right, vec); err != nil {
			return nil, err
		}
		return vec, nil
	}

	length := vector.Length(left)
	if left.IsScalar() {
		length = vector.Length(right)
	}
	resultVector := allocateBoolVector(length, proc)
	nulls.Or(left.Nsp, right.Nsp, resultVector.Nsp)

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
	if v1.IsScalarNull() {
		return proc.AllocScalarNullVector(boolType), nil
	}
	if v1.IsScalar() {
		vec := proc.AllocScalarVector(boolType)
		if err := logical.Not(v1, vec); err != nil {
			return nil, err
		}
		return vec, nil
	}
	length := vector.Length(v1)
	vec := allocateBoolVector(length, proc)
	nulls.Or(v1.Nsp, nil, vec.Nsp)
	if err := logical.Not(v1, vec); err != nil {
		return nil, err
	}
	return vec, nil
}
