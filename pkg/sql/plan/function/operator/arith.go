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
	"github.com/matrixorigin/matrixone/pkg/vectorize/div"
	"github.com/matrixorigin/matrixone/pkg/vectorize/mod"
	"github.com/matrixorigin/matrixone/pkg/vectorize/mult"
	"github.com/matrixorigin/matrixone/pkg/vectorize/sub"
	"golang.org/x/exp/constraints"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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

// Generic T1 is the operand type and generic T2 is the return value type
func Arith[T1 arithT, T2 arithT](vectors []*vector.Vector, proc *process.Process, typ types.Type, afn arithFn) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	leftValues, rightValues := vector.MustTCols[T1](left), vector.MustTCols[T1](right)

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

	nEle := len(leftValues)
	if left.IsScalar() {
		nEle = len(rightValues)
	}

	resultVector, err := proc.AllocVectorOfRows(typ, int64(nEle), nil)
	if err != nil {
		return nil, err
	}
	nulls.Or(left.Nsp, right.Nsp, resultVector.Nsp)
	if err = afn(left, right, resultVector); err != nil {
		return nil, err
	}
	return resultVector, nil
}

// Addition operation
func PlusUint[T constraints.Unsigned](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, args[0].GetType(), add.NumericAddUnsigned[T])
}
func PlusInt[T constraints.Signed](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, args[0].GetType(), add.NumericAddSigned[T])
}
func PlusFloat[T constraints.Float](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, args[0].GetType(), add.NumericAddFloat[T])
}
func PlusDecimal64(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := args[0], args[1]
	lvScale, rvScale := lv.Typ.Scale, rv.Typ.Scale
	resultScale := lvScale
	if lvScale < rvScale {
		resultScale = rvScale
	}
	resultTyp := types.Type{Oid: types.T_decimal64, Size: types.DECIMAL64_NBYTES, Width: types.DECIMAL128_WIDTH, Scale: resultScale}
	return Arith[types.Decimal64, types.Decimal64](args, proc, resultTyp, add.Decimal64VecAdd)
}
func PlusDecimal128(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := args[0], args[1]
	lvScale, rvScale := lv.Typ.Scale, rv.Typ.Scale
	resultScale := lvScale
	if lvScale < rvScale {
		resultScale = rvScale
	}
	resultTyp := types.Type{Oid: types.T_decimal128, Size: types.DECIMAL128_NBYTES, Width: types.DECIMAL128_WIDTH, Scale: resultScale}
	return Arith[types.Decimal128, types.Decimal128](args, proc, resultTyp, add.Decimal128VecAdd)
}

// Subtraction operation
func MinusUint[T constraints.Unsigned](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, args[0].GetType(), sub.NumericSubUnsigned[T])
}
func MinusInt[T constraints.Signed](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, args[0].GetType(), sub.NumericSubSigned[T])
}
func MinusFloat[T constraints.Float](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, args[0].GetType(), sub.NumericSubFloat[T])
}
func MinusDecimal64(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := args[0], args[1]
	lvScale, rvScale := lv.Typ.Scale, rv.Typ.Scale
	resultScale := lvScale
	if lvScale < rvScale {
		resultScale = rvScale
	}
	resultTyp := types.Type{Oid: types.T_decimal64, Size: types.DECIMAL64_NBYTES, Width: types.DECIMAL64_WIDTH, Scale: resultScale}
	return Arith[types.Decimal64, types.Decimal64](args, proc, resultTyp, sub.Decimal64VecSub)
}
func MinusDecimal128(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := args[0], args[1]
	lvScale := lv.Typ.Scale
	rvScale := rv.Typ.Scale
	resultScale := lvScale
	if lvScale < rvScale {
		resultScale = rvScale
	}
	resultTyp := types.Type{Oid: types.T_decimal128, Size: types.DECIMAL128_NBYTES, Width: types.DECIMAL128_WIDTH, Scale: resultScale}
	return Arith[types.Decimal128, types.Decimal128](args, proc, resultTyp, sub.Decimal128VecSub)
}

func MinusDatetime(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	resultType := types.T_int64.ToType()
	return Arith[types.Datetime, types.Datetime](args, proc, resultType, sub.DatetimeSub)
}

// Multiplication operation
func MultUint[T constraints.Unsigned](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, args[0].GetType(), mult.NumericMultUnsigned[T])
}
func MultInt[T constraints.Signed](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, args[0].GetType(), mult.NumericMultSigned[T])
}
func MultFloat[T constraints.Float](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, args[0].GetType(), mult.NumericMultFloat[T])
}
func MultDecimal64(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := args[0], args[1]
	resultScale := lv.Typ.Scale + rv.Typ.Scale
	resultTyp := types.Type{Oid: types.T_decimal128, Size: types.DECIMAL128_NBYTES, Width: types.DECIMAL128_WIDTH, Scale: resultScale}
	return Arith[types.Decimal64, types.Decimal128](args, proc, resultTyp, mult.Decimal64VecMult)
}
func MultDecimal128(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := args[0], args[1]
	resultScale := lv.Typ.Scale + rv.Typ.Scale
	resultTyp := types.Type{Oid: types.T_decimal128, Size: types.DECIMAL128_NBYTES, Width: types.DECIMAL128_WIDTH, Scale: resultScale}
	return Arith[types.Decimal128, types.Decimal128](args, proc, resultTyp, mult.Decimal128VecMult)
}

// Division operation
func DivFloat[T constraints.Float](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, args[0].GetType(), div.NumericDivFloat[T])
}
func DivDecimal64(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var scale int32
	if args[0].Typ.Scale == 0 {
		scale = types.MYSQL_DEFAULT_SCALE
	} else {
		scale = types.MYSQL_DEFAULT_SCALE + args[0].Typ.Scale
	}
	resultTyp := types.Type{Oid: types.T_decimal128, Size: types.DECIMAL128_NBYTES, Width: types.DECIMAL128_WIDTH, Scale: scale}
	return Arith[types.Decimal64, types.Decimal128](args, proc, resultTyp, div.Decimal64VecDiv)
}
func DivDecimal128(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var scale int32
	if args[0].Typ.Scale == 0 {
		scale = types.MYSQL_DEFAULT_SCALE
	} else {
		scale = types.MYSQL_DEFAULT_SCALE + args[0].Typ.Scale
	}
	resultTyp := types.Type{Oid: types.T_decimal128, Size: types.DECIMAL128_NBYTES, Width: types.DECIMAL128_WIDTH, Scale: scale}
	return Arith[types.Decimal128, types.Decimal128](args, proc, resultTyp, div.Decimal128VecDiv)
}

// Integer division operation
func IntegerDivFloat[T constraints.Float](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	resultTyp := types.T_int64.ToType()
	return Arith[T, int64](args, proc, resultTyp, div.NumericIntegerDivFloat[T])
}

// mod operation
func ModUint[T constraints.Unsigned](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, args[0].GetType(), mod.NumericModUnsigned[T])
}
func ModInt[T constraints.Signed](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, args[0].GetType(), mod.NumericModSigned[T])
}
func ModFloat[T constraints.Float](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, args[0].GetType(), mod.NumericModFloat[T])
}
