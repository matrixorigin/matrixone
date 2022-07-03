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

package operator

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/mul"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

var (
	MultUint8 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Mult[uint8](vs, proc, types.Type{Oid: types.T_uint8})
	}

	MultUint16 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Mult[uint16](vs, proc, types.Type{Oid: types.T_uint16})
	}

	MultUint32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Mult[uint32](vs, proc, types.Type{Oid: types.T_uint32})
	}

	MultUint64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Mult[uint64](vs, proc, types.Type{Oid: types.T_uint64})
	}

	MultInt8 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Mult[int8](vs, proc, types.Type{Oid: types.T_int8})
	}

	MultInt16 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Mult[int16](vs, proc, types.Type{Oid: types.T_int16})
	}

	MultInt32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Mult[int32](vs, proc, types.Type{Oid: types.T_int32})
	}

	MultInt64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Mult[int64](vs, proc, types.Type{Oid: types.T_int64})
	}

	MultFloat32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Mult[float32](vs, proc, types.Type{Oid: types.T_float32})
	}

	MultFloat64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Mult[float64](vs, proc, types.Type{Oid: types.T_float64})
	}
)

func Mult[T constraints.Integer | constraints.Float](vectors []*vector.Vector, proc *process.Process, typ types.Type) (*vector.Vector, error) {
	lv, rv := vectors[0], vectors[1]
	lvs, rvs := vector.MustTCols[T](lv), vector.MustTCols[T](rv)

	if lv.IsScalarNull() || rv.IsScalarNull() {
		return proc.AllocScalarNullVector(lv.Typ), nil
	}
	rtl := typ.Oid.TypeLen()
	switch {
	case lv.IsScalar() && rv.IsScalar():
		vec := proc.AllocScalarVector(typ)
		rs := make([]T, 1)
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, mul.NumericMul(lvs, rvs, rs))
		return vec, nil
	case lv.IsScalar() && !rv.IsScalar():
		vec, err := proc.AllocVector(typ, int64(rtl)*int64(len(rvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeFixedSlice[T](vec.Data, rtl)
		nulls.Set(vec.Nsp, rv.Nsp)
		vector.SetCol(vec, mul.NumericMulScalar(lvs[0], rvs, rs))
		return vec, nil
	case !lv.IsScalar() && rv.IsScalar():
		vec, err := proc.AllocVector(typ, int64(rtl)*int64(len(lvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeFixedSlice[T](vec.Data, rtl)
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, mul.NumericMulScalar(rvs[0], lvs, rs))
		return vec, nil
	default:
		vec, err := proc.AllocVector(typ, int64(rtl)*int64(len(lvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeFixedSlice[T](vec.Data, rtl)
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, mul.NumericMul(lvs, rvs, rs))
		return vec, nil
	}
}

//LeftType:   types.T_decimal64,
//RightType:  types.T_decimal64,
//ReturnType: types.T_decimal128,
func MultDecimal64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := vectors[0], vectors[1]
	lvs, rvs := vector.MustTCols[types.Decimal64](lv), vector.MustTCols[types.Decimal64](rv)
	resultScale := lv.Typ.Scale + rv.Typ.Scale
	resultTyp := types.Type{Oid: types.T_decimal128, Size: 16, Width: 38, Scale: resultScale}
	if lv.IsScalarNull() || rv.IsScalarNull() {
		return proc.AllocScalarNullVector(resultTyp), nil
	}
	switch {
	case lv.IsScalar() && rv.IsScalar():
		vec := proc.AllocScalarVector(resultTyp)
		rs := make([]types.Decimal128, 1)
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, mul.Decimal64Mul(lvs, rvs, rs))
		return vec, nil
	case lv.IsScalar() && !rv.IsScalar():
		vec, err := proc.AllocVector(resultTyp, int64(resultTyp.Size)*int64(len(rvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeDecimal128Slice(vec.Data)
		rs = rs[:len(rvs)]
		nulls.Set(vec.Nsp, rv.Nsp)
		vector.SetCol(vec, mul.Decimal64MulScalar(lvs[0], rvs, rs))
		vec.Typ = resultTyp
		return vec, nil
	case !lv.IsScalar() && rv.IsScalar():
		vec, err := proc.AllocVector(lv.Typ, int64(resultTyp.Size)*int64(len(lvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeDecimal128Slice(vec.Data)
		rs = rs[:len(lvs)]
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, mul.Decimal64MulScalar(rvs[0], lvs, rs))
		vec.Typ = resultTyp
		return vec, nil
	default:
		vec, err := proc.AllocVector(lv.Typ, int64(resultTyp.Size)*int64(len(lvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeDecimal128Slice(vec.Data)
		rs = rs[:len(rvs)]
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, mul.Decimal64Mul(lvs, rvs, rs))
		vec.Typ = resultTyp
		return vec, nil
	}
}

//LeftType:   types.T_decimal128,
//RightType:  types.T_decimal128,
//ReturnType: types.T_decimal128,
func MultDecimal128(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := vectors[0], vectors[1]
	lvs, rvs := vector.MustTCols[types.Decimal128](lv), vector.MustTCols[types.Decimal128](rv)
	resultScale := lv.Typ.Scale + rv.Typ.Scale
	resultTyp := types.Type{Oid: types.T_decimal128, Size: 16, Width: 38, Scale: resultScale}
	if lv.IsScalarNull() || rv.IsScalarNull() {
		return proc.AllocScalarNullVector(resultTyp), nil
	}
	switch {
	case lv.IsScalar() && rv.IsScalar():
		vec := proc.AllocScalarVector(resultTyp)
		rs := make([]types.Decimal128, 1)
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, mul.Decimal128Mul(lvs, rvs, rs))
		vec.Typ = resultTyp
		return vec, nil
	case lv.IsScalar() && !rv.IsScalar():
		vec, err := proc.AllocVector(resultTyp, int64(resultTyp.Size)*int64(len(rvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeDecimal128Slice(vec.Data)
		rs = rs[:len(rvs)]
		nulls.Set(vec.Nsp, rv.Nsp)
		vector.SetCol(vec, mul.Decimal128MulScalar(lvs[0], rvs, rs))
		vec.Typ = resultTyp
		return vec, nil
	case !lv.IsScalar() && rv.IsScalar():
		vec, err := proc.AllocVector(lv.Typ, int64(resultTyp.Size)*int64(len(lvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeDecimal128Slice(vec.Data)
		rs = rs[:len(lvs)]
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, mul.Decimal128MulScalar(rvs[0], lvs, rs))
		vec.Typ = resultTyp
		return vec, nil
	default:
		vec, err := proc.AllocVector(lv.Typ, int64(resultTyp.Size)*int64(len(lvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeDecimal128Slice(vec.Data)
		rs = rs[:len(rvs)]
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, mul.Decimal128Mul(lvs, rvs, rs))
		vec.Typ = resultTyp
		return vec, nil
	}
}
