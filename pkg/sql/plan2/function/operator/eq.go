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
	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/eq"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func Equal[T constraints.Integer | constraints.Float](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	leftValues, rightValues := left.Col.([]T), right.Col.([]T)
	// Need to add length for bool type
	resultElementSize := types.T_bool.FixedLength()
	switch {
	case left.IsConst && right.IsConst:
		resultVector := vector.New(SelsType)
		resultVector.Data = make([]byte, resultElementSize)
		// Whether to use 'DecodeInt64Slice' for bool type remains to be discussed
		resultValues := encoding.DecodeInt64Slice(resultVector.Data)
		resultValues = resultValues[:len(leftValues)] // Equivalent to resultValues[0]
		if nulls.Any(left.Nsp) || nulls.Any(right.Nsp) {
			vector.SetCol(resultVector, eq.NumericEqNullable[T](leftValues, rightValues, roaring.Or(left.Nsp.Np, right.Nsp.Np), resultValues))
		} else {
			vector.SetCol(resultVector, eq.NumericEq[T](leftValues, rightValues, resultValues))
		}
		resultVector.Length = left.Length
		resultVector.IsConst = true
	case left.IsConst && !right.IsConst:
		resultVector, err := process.Get2(proc, int64(resultElementSize*len(rightValues)), SelsType)
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeInt64Slice(resultVector.Data)
		resultValues = resultValues[:len(rightValues)]
		switch {
		case nulls.Any(left.Nsp) && !nulls.Any(right.Nsp): // when left is const null and right is Column not null
			// I don't think it's necessary to deal with this situation, just use the initialized slice directly
			vector.SetCol(resultVector, resultValues)
		case nulls.Any(left.Nsp) && nulls.Any(right.Nsp): // when left is const null and right is column has null
			vector.SetCol(resultVector, resultValues)
		case !nulls.Any(left.Nsp) && nulls.Any(right.Nsp): // when left is const not null and right is column has null
			vector.SetCol(resultVector, eq.NumericEqNullableScalar(leftValues[0], rightValues, right.Nsp.Np, resultValues))
		default: // when left is const not null and right is column without null
			vector.SetCol(resultVector, eq.NumericEqScalar(leftValues[0], rightValues, resultValues))
		}
		return resultVector, nil
	case !left.IsConst && right.IsConst:
		resultVector, err := process.Get2(proc, int64(resultElementSize*len(leftValues)), SelsType)
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeInt64Slice(resultVector.Data)
		resultValues = resultValues[:len(leftValues)]
		switch {
		case nulls.Any(left.Nsp) && !nulls.Any(right.Nsp): // when left is Column has null and right is const not null
			vector.SetCol(resultVector, eq.NumericEqNullableScalar(rightValues[0], leftValues, right.Nsp.Np, resultValues))
		case nulls.Any(left.Nsp) && nulls.Any(right.Nsp): // when left is Column has null and right is const null
			vector.SetCol(resultVector, resultValues)
		case !nulls.Any(left.Nsp) && nulls.Any(right.Nsp): // when left is column without null and right is const null
			vector.SetCol(resultVector, resultValues)
		default: // when left is const not null and right is column without null
			vector.SetCol(resultVector, eq.NumericEqScalar(rightValues[0], leftValues, resultValues))
		}
		return resultVector, nil
	}
	resultVector, err := process.Get2(proc, int64(resultElementSize*len(leftValues)), SelsType)
	if err != nil {
		return nil, err
	}
	resultValues := encoding.DecodeInt64Slice(resultVector.Data)
	resultValues = resultValues[:len(leftValues)]
	switch {
	case nulls.Any(left.Nsp) && nulls.Any(right.Nsp):
		vector.SetCol(resultVector, eq.NumericEqNullable(leftValues, rightValues, roaring.Or(left.Nsp.Np, right.Nsp.Np), resultValues))
	case !nulls.Any(left.Nsp) && nulls.Any(right.Nsp):
		vector.SetCol(resultVector, eq.NumericEqNullable(leftValues, rightValues, right.Nsp.Np, resultValues))
	case nulls.Any(left.Nsp) && !nulls.Any(right.Nsp):
		vector.SetCol(resultVector, eq.NumericEqNullable(leftValues, rightValues, left.Nsp.Np, resultValues))
	default:
		vector.SetCol(resultVector, eq.NumericEq(leftValues, rightValues, resultValues))
	}
	return resultVector, nil
}
