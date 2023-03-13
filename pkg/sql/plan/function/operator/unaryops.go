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

func UnaryTilde[T constraints.Integer](ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	srcVector := ivecs[0]
	srcValues := vector.MustFixedCol[T](srcVector)
	rtyp := types.T_uint64.ToType()

	if srcVector.IsConst() {
		if srcVector.IsConstNull() {
			return vector.NewConstNull(*srcVector.GetType(), srcVector.Length(), proc.Mp()), nil
		}
		return vector.NewConstFixed(rtyp, funcBitInversion(srcValues[0]), srcVector.Length(), proc.Mp()), nil
	} else {
		resVector, err := proc.AllocVectorOfRows(rtyp, len(srcValues), srcVector.GetNulls())
		if err != nil {
			return nil, err
		}
		resValues := vector.MustFixedCol[uint64](resVector)

		var i uint64
		if nulls.Any(resVector.GetNulls()) {
			for i = 0; i < uint64(len(resValues)); i++ {
				if !nulls.Contains(resVector.GetNulls(), i) {
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

func UnaryMinus[T constraints.Signed | constraints.Float](ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	srcVector := ivecs[0]
	srcValues := vector.MustFixedCol[T](srcVector)

	if srcVector.IsConst() {
		if srcVector.IsConstNull() {
			return vector.NewConstNull(*srcVector.GetType(), srcVector.Length(), proc.Mp()), nil
		}
		var resValues [1]T
		neg.NumericNeg(srcValues, resValues[:])
		return vector.NewConstFixed(*srcVector.GetType(), resValues[0], srcVector.Length(), proc.Mp()), nil
	} else {
		resVector, err := proc.AllocVectorOfRows(*srcVector.GetType(), len(srcValues), srcVector.GetNulls())
		if err != nil {
			return nil, err
		}
		resValues := vector.MustFixedCol[T](resVector)
		neg.NumericNeg(srcValues, resValues)
		return resVector, nil
	}
}

func UnaryMinusDecimal64(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	srcVector := ivecs[0]
	srcValues := vector.MustFixedCol[types.Decimal64](srcVector)

	if srcVector.IsConst() {
		if srcVector.IsConstNull() {
			return vector.NewConstNull(*srcVector.GetType(), srcVector.Length(), proc.Mp()), nil
		}
		var resValues [1]types.Decimal64
		neg.Decimal64Neg(srcValues, resValues[:])
		return vector.NewConstFixed(*srcVector.GetType(), resValues[0], srcVector.Length(), proc.Mp()), nil
	} else {
		resVector, err := proc.AllocVectorOfRows(*srcVector.GetType(), len(srcValues), srcVector.GetNulls())
		if err != nil {
			return nil, err
		}
		resValues := vector.MustFixedCol[types.Decimal64](resVector)
		neg.Decimal64Neg(srcValues, resValues)
		return resVector, nil
	}
}

func UnaryMinusDecimal128(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	srcVector := ivecs[0]
	srcValues := vector.MustFixedCol[types.Decimal128](srcVector)

	if srcVector.IsConst() {
		if srcVector.IsConstNull() {
			return vector.NewConstNull(*srcVector.GetType(), srcVector.Length(), proc.Mp()), nil
		}
		var resValues [1]types.Decimal128
		neg.Decimal128Neg(srcValues, resValues[:])
		return vector.NewConstFixed(*srcVector.GetType(), resValues[0], srcVector.Length(), proc.Mp()), nil
	} else {
		resVector, err := proc.AllocVectorOfRows(*srcVector.GetType(), len(srcValues), srcVector.GetNulls())
		if err != nil {
			return nil, err
		}
		resValues := vector.MustFixedCol[types.Decimal128](resVector)
		// XXX should pass in nulls
		neg.Decimal128Neg(srcValues, resValues)
		return resVector, nil
	}
}
