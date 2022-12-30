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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/neg"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func UnaryTilde[T constraints.Integer](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	srcVector := vectors[0]
	srcValues := vector.MustTCols[T](srcVector)
	returnType := types.Type{
		Oid:  types.T_uint64,
		Size: types.T_uint64.ToType().Size,
	}

	if srcVector.IsConst() {
		if srcVector.IsConstNull() {
			return proc.AllocScalarNullVector(returnType), nil
		}
		resVector := proc.AllocScalarVector(returnType)
		resValues := make([]uint64, 1)
		nulls.Set(resVector.Nsp, srcVector.GetNulls())
		resValues[0] = funcBitInversion(srcValues[0])
		vector.SetCol(resVector, resValues)
		return resVector, nil
	} else {
		resVector, err := proc.AllocVectorOfRows(returnType, int64(len(srcValues)), srcVector.GetNulls())
		if err != nil {
			return nil, err
		}
		resValues := vector.MustTCols[uint64](resVector)

		var i uint64
		if nulls.Any(resVector.Nsp) {
			for i = 0; i < uint64(len(resValues)); i++ {
				if !nulls.Contains(resVector.Nsp, i) {
					resValues[i] = funcBitInversion(srcValues[i])
				} else {
					resValues[i] = 0
				}
			}
		} else {
			for i = 0; i < uint64(len(resValues)); i++ {
				resValues[i] = funcBitInversion(srcValues[i])
			}
		}
		return resVector, nil
	}
}

func funcBitInversion[T constraints.Integer](x T) uint64 {
	if x > 0 {
		n := uint64(x)
		return ^n
	} else {
		return uint64(^x)
	}
}

func UnaryMinus[T constraints.Signed | constraints.Float](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	srcVector := vectors[0]
	srcValues := vector.MustTCols[T](srcVector)

	if srcVector.IsConst() {
		if srcVector.IsConstNull() {
			return proc.AllocScalarNullVector(srcVector.GetType()), nil
		}
		resVector := proc.AllocScalarVector(srcVector.GetType())
		resValues := make([]T, 1)
		nulls.Set(resVector.Nsp, srcVector.GetNulls())
		vector.SetCol(resVector, neg.NumericNeg(srcValues, resValues))
		return resVector, nil
	} else {
		resVector, err := proc.AllocVectorOfRows(srcVector.GetType(), int64(len(srcValues)), srcVector.GetNulls())
		if err != nil {
			return nil, err
		}
		resValues := vector.MustTCols[T](resVector)
		neg.NumericNeg(srcValues, resValues)
		return resVector, nil
	}
}

func UnaryMinusDecimal64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	srcVector := vectors[0]
	srcValues := vector.MustTCols[types.Decimal64](srcVector)

	if srcVector.IsConst() {
		if srcVector.IsConstNull() {
			return proc.AllocScalarNullVector(srcVector.GetType()), nil
		}
		resVector := proc.AllocScalarVector(srcVector.GetType())
		resValues := make([]types.Decimal64, 1)
		nulls.Set(resVector.Nsp, srcVector.Nsp)
		vector.SetCol(resVector, neg.Decimal64Neg(srcValues, resValues))
		return resVector, nil
	} else {
		resVector, err := proc.AllocVectorOfRows(srcVector.GetType(), int64(len(srcValues)), srcVector.GetNulls())
		if err != nil {
			return nil, err
		}
		resValues := vector.MustTCols[types.Decimal64](resVector)
		neg.Decimal64Neg(srcValues, resValues)
		return resVector, nil
	}
}

func UnaryMinusDecimal128(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	srcVector := vectors[0]
	srcValues := vector.MustTCols[types.Decimal128](srcVector)

	if srcVector.IsConst() {
		if srcVector.IsConstNull() {
			return proc.AllocScalarNullVector(srcVector.GetType()), nil
		}
		resVector := proc.AllocScalarVector(srcVector.GetType())
		resValues := make([]types.Decimal128, 1)
		vector.SetCol(resVector, neg.Decimal128Neg(srcValues, resValues))
		return resVector, nil
	} else {
		resVector, err := proc.AllocVectorOfRows(srcVector.GetType(), int64(len(srcValues)), srcVector.GetNulls())
		if err != nil {
			return nil, err
		}
		resValues := vector.MustTCols[types.Decimal128](resVector)
		// XXX should pass in nulls
		neg.Decimal128Neg(srcValues, resValues)
		return resVector, nil
	}
}
