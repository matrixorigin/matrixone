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

package operator

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/equal"
	"github.com/matrixorigin/matrixone/pkg/vectorize/greatequal"
	"github.com/matrixorigin/matrixone/pkg/vectorize/greatthan"
	"github.com/matrixorigin/matrixone/pkg/vectorize/lessequal"
	"github.com/matrixorigin/matrixone/pkg/vectorize/lessthan"
	"github.com/matrixorigin/matrixone/pkg/vectorize/notequal"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

type compareT interface {
	constraints.Integer | constraints.Float | bool |
		types.Decimal64 | types.Decimal128 |
		types.Date | types.Datetime | types.Timestamp
}

type compareFn func(v1, v2, r *vector.Vector) error

func CompareOrder(vs []*vector.Vector, proc *process.Process, cfn compareFn) (*vector.Vector, error) {
	left, right := vs[0], vs[1]

	if left.IsScalarNull() || right.IsScalarNull() {
		return handleScalarNull(left, right, proc)
	}

	if left.IsScalar() && right.IsScalar() {
		resultVector := proc.AllocScalarVector(boolType)
		if err := cfn(left, right, resultVector); err != nil {
			return nil, err
		}
		return resultVector, nil
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

// Equal compare operator
func EqGeneralX[T constraints.Integer | constraints.Float](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrder(args, proc, equal.NumericEqual[T])
}

func EqDecimal64X(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrder(args, proc, equal.Decimal64VecEq)
}

func EqDecimal128X(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrder(args, proc, equal.Decimal128VecEq)
}

// Not Equal compare operator
func NeGeneralX[T constraints.Integer | constraints.Float](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrder(args, proc, notequal.NumericNotEqual[T])
}

func NeDecimal64X(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrder(args, proc, notequal.Decimal64VecNe)
}

func NeDecimal128X(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrder(args, proc, notequal.Decimal128VecNe)
}

// Great than operator
func GtGeneralX[T constraints.Integer | constraints.Float](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrder(args, proc, greatthan.NumericGreatThan[T])
}

func GtDecimal64X(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrder(args, proc, greatthan.Decimal64VecGt)
}

func GtDecimal128X(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrder(args, proc, greatthan.Decimal128VecGt)
}

// Great equal operator
func GeGeneralX[T constraints.Integer | constraints.Float](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrder(args, proc, greatequal.NumericGreatEqual[T])
}

func GeDecimal64X(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrder(args, proc, greatequal.Decimal64VecGe)
}

func GeDecimal128X(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrder(args, proc, greatequal.Decimal128VecGe)
}

// less than operator
func LtGeneralX[T constraints.Integer | constraints.Float](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrder(args, proc, lessthan.NumericLessThan[T])
}

func LtDecimal64X(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrder(args, proc, lessthan.Decimal64VecLt)
}

func LtDecimal128X(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrder(args, proc, lessthan.Decimal128VecLt)
}

// less equal operator
func LeGeneralX[T constraints.Integer | constraints.Float](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrder(args, proc, lessequal.NumericLessEqual[T])
}

func LeDecimal64X(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrder(args, proc, lessequal.Decimal64VecLe)
}

func LeDecimal128X(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrder(args, proc, lessequal.Decimal128VecLe)
}
