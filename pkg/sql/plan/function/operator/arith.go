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
	"golang.org/x/exp/constraints"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/add"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

//
// This file contains some vectorized arithmatic operators.
//

type arithT interface {
	constraints.Integer | constraints.Float | bool |
		types.Decimal64 | types.Decimal128
}

type arithFn func(v1, v2, r *vector.Vector) error

func Arith[T arithT](vectors []*vector.Vector, proc *process.Process, typ types.Type, afn arithFn) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	leftValues, rightValues := vector.MustTCols[T](left), vector.MustTCols[T](right)

	if left.IsScalarNull() || right.IsScalarNull() {
		return proc.AllocScalarNullVector(typ), nil
	}

	if left.IsScalar() && right.IsScalar() {
		resultVector := proc.AllocScalarVector(typ)
		if err := afn(left, right, resultVector); err != nil {
			return nil, err
		}
		return resultVector, nil
	}

	resultElementSize := typ.Oid.TypeLen()
	nEle := len(leftValues)
	if left.IsScalar() {
		nEle = len(rightValues)
	}

	resultVector, err := proc.AllocVector(typ, int64(resultElementSize*nEle))
	if err != nil {
		return nil, err
	}

	resultValues := encoding.DecodeFixedSlice[T](resultVector.Data, resultElementSize)
	nulls.Or(left.Nsp, right.Nsp, resultVector.Nsp)
	vector.SetCol(resultVector, resultValues)
	if err = afn(left, right, resultVector); err != nil {
		return nil, err
	}
	return resultVector, nil
}

func PlusUint[T constraints.Unsigned](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T](args, proc, args[0].GetType(), add.NumericAddUnsigned[T])
}
func PlusInt[T constraints.Signed](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T](args, proc, args[0].GetType(), add.NumericAddSigned[T])
}
func PlusFloat[T constraints.Float](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T](args, proc, args[0].GetType(), add.NumericAddFloat[T])
}
func PlusDecimal64(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[types.Decimal64](args, proc, args[0].GetType(), add.Decimal64VecAdd)
}
func PlusDecimal128(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[types.Decimal128](args, proc, args[0].GetType(), add.Decimal128VecAdd)
}
