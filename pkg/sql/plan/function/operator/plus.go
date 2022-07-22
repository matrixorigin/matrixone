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
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/add"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

var (
	PlusUint8 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Plus[uint8](vs, proc, types.Type{Oid: types.T_uint8})
	}

	PlusUint16 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Plus[uint16](vs, proc, types.Type{Oid: types.T_uint16})
	}

	PlusUint32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Plus[uint32](vs, proc, types.Type{Oid: types.T_uint32})
	}

	PlusUint64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Plus[uint64](vs, proc, types.Type{Oid: types.T_uint64})
	}

	PlusInt8 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Plus[int8](vs, proc, types.Type{Oid: types.T_int8})
	}

	PlusInt16 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Plus[int16](vs, proc, types.Type{Oid: types.T_int16})
	}

	PlusInt32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Plus[int32](vs, proc, types.Type{Oid: types.T_int32})
	}

	PlusInt64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Plus[int64](vs, proc, types.Type{Oid: types.T_int64})
	}

	PlusFloat32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Plus[float32](vs, proc, types.Type{Oid: types.T_float32})
	}

	PlusFloat64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Plus[float64](vs, proc, types.Type{Oid: types.T_float64})
	}
)

func Plus[T constraints.Integer | constraints.Float](vectors []*vector.Vector, proc *process.Process, typ types.Type) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	leftValues, rightValues := vector.MustTCols[T](left), vector.MustTCols[T](right)
	if left.IsScalarNull() || right.IsScalarNull() {
		return proc.AllocScalarNullVector(left.Typ), nil
	}
	resultElementSize := typ.Oid.TypeLen()
	switch {
	case left.IsScalar() && right.IsScalar():
		resultVector := proc.AllocScalarVector(typ)
		resultValues := make([]T, 1)
		nulls.Or(left.Nsp, right.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, add.NumericAdd(leftValues, rightValues, resultValues))
		return resultVector, nil
	case left.IsScalar() && !right.IsScalar():
		resultVector, err := proc.AllocVector(typ, int64(resultElementSize*len(rightValues)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFixedSlice[T](resultVector.Data, resultElementSize)
		nulls.Or(left.Nsp, right.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, add.NumericAddScalar(leftValues[0], rightValues, resultValues))
		return resultVector, nil
	case !left.IsScalar() && right.IsScalar():
		resultVector, err := proc.AllocVector(typ, int64(resultElementSize*len(leftValues)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFixedSlice[T](resultVector.Data, resultElementSize)
		nulls.Or(left.Nsp, right.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, add.NumericAddScalar(rightValues[0], leftValues, resultValues))
		return resultVector, nil
	}
	resultVector, err := proc.AllocVector(typ, int64(resultElementSize*len(leftValues)))
	if err != nil {
		return nil, err
	}
	resultValues := encoding.DecodeFixedSlice[T](resultVector.Data, resultElementSize)
	nulls.Or(left.Nsp, right.Nsp, resultVector.Nsp)
	vector.SetCol(resultVector, add.NumericAdd(leftValues, rightValues, resultValues))
	return resultVector, nil
}

//LeftType:   types.T_decimal64,
//RightType:  types.T_decimal64,
//ReturnType: types.T_decimal64,
func PlusDecimal64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := vectors[0], vectors[1]
	lvs, rvs := vector.MustTCols[types.Decimal64](lv), vector.MustTCols[types.Decimal64](rv)
	lvScale, rvScale := lv.Typ.Scale, rv.Typ.Scale
	resultScale := lvScale
	if lvScale < rvScale {
		resultScale = rvScale
	}
	resultTyp := types.Type{Oid: types.T_decimal64, Size: types.DECIMAL64_NBYTES, Width: types.DECIMAL128_WIDTH, Scale: resultScale}
	if lv.IsScalarNull() || rv.IsScalarNull() {
		return proc.AllocScalarNullVector(resultTyp), nil
	}
	switch {
	case lv.IsScalar() && rv.IsScalar():
		vec := proc.AllocScalarVector(resultTyp)
		rs := make([]types.Decimal64, 1)
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, add.Decimal64Add(lvs, rvs, lv.Typ.Scale, rv.Typ.Scale, rs))
		return vec, nil
	case lv.IsScalar() && !rv.IsScalar():
		vec, err := proc.AllocVector(resultTyp, int64(resultTyp.Size)*int64(len(rvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeDecimal64Slice(vec.Data)
		rs = rs[:len(rvs)]
		nulls.Set(vec.Nsp, rv.Nsp)
		vector.SetCol(vec, add.Decimal64AddScalar(lvs[0], rvs, lvScale, rvScale, rs))
		return vec, nil
	case !lv.IsScalar() && rv.IsScalar():
		vec, err := proc.AllocVector(resultTyp, int64(resultTyp.Size)*int64(len(lvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeDecimal64Slice(vec.Data)
		rs = rs[:len(lvs)]
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, add.Decimal64AddScalar(rvs[0], lvs, rvScale, lvScale, rs))
		return vec, nil
	}
	vec, err := proc.AllocVector(lv.Typ, int64(resultTyp.Size)*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeDecimal64Slice(vec.Data)
	rs = rs[:len(rvs)]
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	vector.SetCol(vec, add.Decimal64Add(lvs, rvs, lv.Typ.Scale, rv.Typ.Scale, rs))
	return vec, nil
}

//LeftType:   types.T_decimal128,
//RightType:  types.T_decimal128,
//ReturnType: types.T_decimal128,
func PlusDecimal128(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := vectors[0], vectors[1]
	lvs, rvs := vector.MustTCols[types.Decimal128](lv), vector.MustTCols[types.Decimal128](rv)
	lvScale, rvScale := lv.Typ.Scale, rv.Typ.Scale
	resultScale := lvScale
	if lvScale < rvScale {
		resultScale = rvScale
	}
	resultTyp := types.Type{Oid: types.T_decimal128, Size: types.DECIMAL128_NBYTES, Width: types.DECIMAL128_WIDTH, Scale: resultScale}
	if lv.IsScalarNull() || rv.IsScalarNull() {
		return proc.AllocScalarNullVector(resultTyp), nil
	}
	switch {
	case lv.IsScalar() && rv.IsScalar():
		vec := proc.AllocScalarVector(resultTyp)
		rs := make([]types.Decimal128, 1)
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, add.Decimal128Add(lvs, rvs, lv.Typ.Scale, rv.Typ.Scale, rs))
		return vec, nil
	case lv.IsScalar() && !rv.IsScalar():
		vec, err := proc.AllocVector(resultTyp, int64(resultTyp.Size)*int64(len(rvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeDecimal128Slice(vec.Data)
		rs = rs[:len(rvs)]
		nulls.Set(vec.Nsp, rv.Nsp)
		vector.SetCol(vec, add.Decimal128AddScalar(lvs[0], rvs, lvScale, rvScale, rs))
		return vec, nil
	case !lv.IsScalar() && rv.IsScalar():
		vec, err := proc.AllocVector(resultTyp, int64(resultTyp.Size)*int64(len(lvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeDecimal128Slice(vec.Data)
		rs = rs[:len(lvs)]
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, add.Decimal128AddScalar(rvs[0], lvs, rvScale, lvScale, rs))
		return vec, nil
	}
	vec, err := proc.AllocVector(lv.Typ, int64(resultTyp.Size)*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeDecimal128Slice(vec.Data)
	rs = rs[:len(rvs)]
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	vector.SetCol(vec, add.Decimal128Add(lvs, rvs, lv.Typ.Scale, rv.Typ.Scale, rs))
	return vec, nil
}
