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
	"github.com/matrixorigin/matrixone/pkg/vectorize/sub"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

var (
	MinusUint8 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Minus[uint8](vs, proc, types.Type{Oid: types.T_uint8})
	}

	MinusUint16 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Minus[uint16](vs, proc, types.Type{Oid: types.T_uint16})
	}

	MinusUint32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Minus[uint32](vs, proc, types.Type{Oid: types.T_uint32})
	}

	MinusUint64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Minus[uint64](vs, proc, types.Type{Oid: types.T_uint64})
	}

	MinusInt8 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Minus[int8](vs, proc, types.Type{Oid: types.T_int8})
	}

	MinusInt16 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Minus[int16](vs, proc, types.Type{Oid: types.T_int16})
	}

	MinusInt32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Minus[int32](vs, proc, types.Type{Oid: types.T_int32})
	}

	MinusInt64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Minus[int64](vs, proc, types.Type{Oid: types.T_int64})
	}

	MinusFloat32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Minus[float32](vs, proc, types.Type{Oid: types.T_float32})
	}

	MinusFloat64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return Minus[float64](vs, proc, types.Type{Oid: types.T_float64})
	}
)

func Minus[T constraints.Integer | constraints.Float](vectors []*vector.Vector, proc *process.Process, typ types.Type) (*vector.Vector, error) {
	lv, rv := vectors[0], vectors[1]
	lvs, rvs := vector.MustTCols[T](lv), vector.MustTCols[T](rv)
	resultElementSize := typ.Oid.TypeLen()
	if lv.IsScalarNull() || rv.IsScalarNull() {
		return proc.AllocScalarNullVector(typ), nil
	}
	switch {
	case lv.IsScalar() && rv.IsScalar():
		resultVector := proc.AllocScalarVector(typ)
		resultValues := make([]T, 1)
		//nulls.Reset(resultVector.Nsp) ; i think this is good
		nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, sub.Numeric(lvs, rvs, resultValues))
		return resultVector, nil
	case lv.IsScalar() && !rv.IsScalar():
		resultVector, err := proc.AllocVector(typ, int64(resultElementSize*len(rvs)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFixedSlice[T](resultVector.Data, resultElementSize)
		nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, sub.NumericScalar(lvs[0], rvs, resultValues))
		return resultVector, nil
	case !lv.IsScalar() && rv.IsScalar():
		resultVector, err := proc.AllocVector(typ, int64(resultElementSize*len(lvs)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFixedSlice[T](resultVector.Data, resultElementSize)
		nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, sub.NumericByScalar(rvs[0], lvs, resultValues))
		return resultVector, nil
	default:
		resultVector, err := proc.AllocVector(typ, int64(resultElementSize*len(lvs)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFixedSlice[T](resultVector.Data, resultElementSize)
		nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, sub.Numeric(lvs, rvs, resultValues))
		return resultVector, nil
	}
}

// Since the underlying operator does not generically process decimal64 and decimal128, sub of decimal64 and decimal128 are not generalized
func MinusDecimal64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := vectors[0], vectors[1]
	lvs, rvs := vector.MustTCols[types.Decimal64](lv), vector.MustTCols[types.Decimal64](rv)
	lvScale, rvScale := lv.Typ.Scale, rv.Typ.Scale
	resultScale := lvScale
	if lvScale < rvScale {
		resultScale = rvScale
	}
	resultTyp := types.Type{Oid: types.T_decimal64, Size: 8, Width: 18, Scale: resultScale}
	if lv.IsScalarNull() || rv.IsScalarNull() {
		return proc.AllocScalarNullVector(resultTyp), nil
	}

	switch {
	case lv.IsScalar() && rv.IsScalar():
		resultVector := proc.AllocScalarVector(resultTyp)
		resultValues := make([]types.Decimal64, 1)
		nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, sub.Decimal64Sub(lvs, rvs, lvScale, rvScale, resultValues))
		return resultVector, nil
	case lv.IsScalar() && !rv.IsScalar():
		resultVector, err := proc.AllocVector(resultTyp, int64(resultTyp.Size)*int64(len(rvs)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeDecimal64Slice(resultVector.Data)
		resultValues = resultValues[:len(rvs)]
		nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, sub.Decimal64SubScalar(lvs[0], rvs, lvScale, rvScale, resultValues))
		return resultVector, nil
	case !lv.IsScalar() && rv.IsScalar():
		resultVector, err := proc.AllocVector(resultTyp, int64(resultTyp.Size)*int64(len(lvs)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeDecimal64Slice(resultVector.Data)
		resultValues = resultValues[:len(lvs)]
		nulls.Set(resultVector.Nsp, lv.Nsp)
		nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, sub.Decimal64SubByScalar(rvs[0], lvs, rvScale, lvScale, resultValues))
		return resultVector, nil
	default:
		resultVector, err := proc.AllocVector(lv.Typ, int64(resultTyp.Size)*int64(len(lvs)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeDecimal64Slice(resultVector.Data)
		resultValues = resultValues[:len(rvs)]
		nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, sub.Decimal64Sub(lvs, rvs, lv.Typ.Scale, rv.Typ.Scale, resultValues))
		return resultVector, nil
	}
}

func MinusDecimal128(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := vectors[0], vectors[1]
	lvs, rvs := vector.MustTCols[types.Decimal128](lv), vector.MustTCols[types.Decimal128](rv)
	lvScale := lv.Typ.Scale
	rvScale := rv.Typ.Scale
	resultScale := lvScale
	if lvScale < rvScale {
		resultScale = rvScale
	}
	resultTyp := types.Type{Oid: types.T_decimal128, Size: 16, Width: 38, Scale: resultScale}
	if lv.IsScalarNull() || rv.IsScalarNull() {
		return proc.AllocScalarNullVector(resultTyp), nil
	}

	switch {
	case lv.IsScalar() && rv.IsScalar():
		vec := proc.AllocScalarVector(resultTyp)
		rs := make([]types.Decimal128, 1)
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, sub.Decimal128Sub(lvs, rvs, lvScale, rvScale, rs))
		return vec, nil
	case lv.IsScalar() && !rv.IsScalar():
		vec, err := proc.AllocVector(resultTyp, int64(resultTyp.Size)*int64(len(rvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeDecimal128Slice(vec.Data)
		rs = rs[:len(rvs)]
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, sub.Decimal128SubScalar(lvs[0], rvs, lvScale, rvScale, rs))
		return vec, nil
	case !lv.IsScalar() && rv.IsScalar():
		vec, err := proc.AllocVector(resultTyp, int64(resultTyp.Size)*int64(len(lvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeDecimal128Slice(vec.Data)
		rs = rs[:len(lvs)]
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, sub.Decimal128SubByScalar(rvs[0], lvs, rvScale, lvScale, rs))
		return vec, nil
	default:
		vec, err := proc.AllocVector(resultTyp, int64(resultTyp.Size)*int64(len(lvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeDecimal128Slice(vec.Data)
		rs = rs[:len(rvs)]
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, sub.Decimal128Sub(lvs, rvs, lv.Typ.Scale, rv.Typ.Scale, rs))
		return vec, nil
	}
}
