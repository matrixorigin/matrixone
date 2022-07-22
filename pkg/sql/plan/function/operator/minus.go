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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
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
		vector.SetCol(resultVector, Numeric(lvs, rvs, resultValues))
		return resultVector, nil
	case lv.IsScalar() && !rv.IsScalar():
		resultVector, err := proc.AllocVector(typ, int64(resultElementSize*len(rvs)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFixedSlice[T](resultVector.Data, resultElementSize)
		nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, NumericScalar(lvs[0], rvs, resultValues))
		return resultVector, nil
	case !lv.IsScalar() && rv.IsScalar():
		resultVector, err := proc.AllocVector(typ, int64(resultElementSize*len(lvs)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFixedSlice[T](resultVector.Data, resultElementSize)
		nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, NumericByScalar(rvs[0], lvs, resultValues))
		return resultVector, nil
	default:
		resultVector, err := proc.AllocVector(typ, int64(resultElementSize*len(lvs)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFixedSlice[T](resultVector.Data, resultElementSize)
		nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, Numeric(lvs, rvs, resultValues))
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
	resultTyp := types.Type{Oid: types.T_decimal64, Size: types.DECIMAL64_NBYTES, Width: types.DECIMAL64_WIDTH, Scale: resultScale}
	if lv.IsScalarNull() || rv.IsScalarNull() {
		return proc.AllocScalarNullVector(resultTyp), nil
	}

	switch {
	case lv.IsScalar() && rv.IsScalar():
		resultVector := proc.AllocScalarVector(resultTyp)
		resultValues := make([]types.Decimal64, 1)
		nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, Decimal64Sub(lvs, rvs, lvScale, rvScale, resultValues))
		return resultVector, nil
	case lv.IsScalar() && !rv.IsScalar():
		resultVector, err := proc.AllocVector(resultTyp, int64(resultTyp.Size)*int64(len(rvs)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeDecimal64Slice(resultVector.Data)
		resultValues = resultValues[:len(rvs)]
		nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, Decimal64SubScalar(lvs[0], rvs, lvScale, rvScale, resultValues))
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
		vector.SetCol(resultVector, Decimal64SubByScalar(rvs[0], lvs, rvScale, lvScale, resultValues))
		return resultVector, nil
	default:
		resultVector, err := proc.AllocVector(lv.Typ, int64(resultTyp.Size)*int64(len(lvs)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeDecimal64Slice(resultVector.Data)
		resultValues = resultValues[:len(rvs)]
		nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, Decimal64Sub(lvs, rvs, lv.Typ.Scale, rv.Typ.Scale, resultValues))
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
	resultTyp := types.Type{Oid: types.T_decimal128, Size: types.DECIMAL128_NBYTES, Width: types.DECIMAL128_WIDTH, Scale: resultScale}
	if lv.IsScalarNull() || rv.IsScalarNull() {
		return proc.AllocScalarNullVector(resultTyp), nil
	}

	switch {
	case lv.IsScalar() && rv.IsScalar():
		vec := proc.AllocScalarVector(resultTyp)
		rs := make([]types.Decimal128, 1)
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, Decimal128Sub(lvs, rvs, lvScale, rvScale, rs))
		return vec, nil
	case lv.IsScalar() && !rv.IsScalar():
		vec, err := proc.AllocVector(resultTyp, int64(resultTyp.Size)*int64(len(rvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeDecimal128Slice(vec.Data)
		rs = rs[:len(rvs)]
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, Decimal128SubScalar(lvs[0], rvs, lvScale, rvScale, rs))
		return vec, nil
	case !lv.IsScalar() && rv.IsScalar():
		vec, err := proc.AllocVector(resultTyp, int64(resultTyp.Size)*int64(len(lvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeDecimal128Slice(vec.Data)
		rs = rs[:len(lvs)]
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, Decimal128SubByScalar(rvs[0], lvs, rvScale, lvScale, rs))
		return vec, nil
	default:
		vec, err := proc.AllocVector(resultTyp, int64(resultTyp.Size)*int64(len(lvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeDecimal128Slice(vec.Data)
		rs = rs[:len(rvs)]
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, Decimal128Sub(lvs, rvs, lv.Typ.Scale, rv.Typ.Scale, rs))
		return vec, nil
	}
}

var (
	Int8Sub                = NumericSigned[int8]
	Int8SubScalar          = NumericScalarSigned[int8]
	Int8SubByScalar        = NumericByScalarSigned[int8]
	Int16Sub               = NumericSigned[int16]
	Int16SubScalar         = NumericScalarSigned[int16]
	Int16SubByScalar       = NumericByScalarSigned[int16]
	Int32Sub               = NumericSigned[int32]
	Int32SubScalar         = NumericScalarSigned[int32]
	Int32SubByScalar       = NumericByScalarSigned[int32]
	Int64Sub               = NumericSigned[int64]
	Int64SubScalar         = NumericScalarSigned[int64]
	Int64SubByScalar       = NumericByScalarSigned[int64]
	Uint8Sub               = NumericUnsigned[uint8]
	Uint8SubScalar         = NumericScalarUnsigned[uint8]
	Uint8SubByScalar       = NumericByScalarUnsigned[uint8]
	Uint16Sub              = NumericUnsigned[uint16]
	Uint16SubScalar        = NumericScalarUnsigned[uint16]
	Uint16SubByScalar      = NumericByScalarUnsigned[uint16]
	Uint32Sub              = NumericUnsigned[uint32]
	Uint32SubScalar        = NumericScalarUnsigned[uint32]
	Uint32SubByScalar      = NumericByScalarUnsigned[uint32]
	Uint64Sub              = NumericUnsigned[uint64]
	Uint64SubScalar        = NumericScalarUnsigned[uint64]
	Uint64SubByScalar      = NumericByScalarUnsigned[uint64]
	Float32Sub             = Numeric[float32]
	Float32SubScalar       = NumericScalar[float32]
	Float32SubByScalar     = NumericByScalar[float32]
	Float64Sub             = Numeric[float64]
	Float64SubScalar       = NumericScalar[float64]
	Float64SubByScalar     = NumericByScalar[float64]
	Int8SubSels            = NumericSelsSigned[int8]
	Int8SubScalarSels      = NumericScalarSelsSigned[int8]
	Int8SubByScalarSels    = NumericByScalarSelsSigned[int8]
	Int16SubSels           = NumericSelsSigned[int16]
	Int16SubScalarSels     = NumericScalarSelsSigned[int16]
	Int16SubByScalarSels   = NumericByScalarSelsSigned[int16]
	Int32SubSels           = NumericSelsSigned[int32]
	Int32SubScalarSels     = NumericScalarSelsSigned[int32]
	Int32SubByScalarSels   = NumericByScalarSelsSigned[int32]
	Int64SubSels           = NumericSelsSigned[int64]
	Int64SubScalarSels     = NumericScalarSelsSigned[int64]
	Int64SubByScalarSels   = NumericByScalarSelsSigned[int64]
	Uint8SubSels           = NumericSelsUnsigned[uint8]
	Uint8SubScalarSels     = NumericScalarSelsUnsigned[uint8]
	Uint8SubByScalarSels   = NumericByScalarSelsUnsigned[uint8]
	Uint16SubSels          = NumericSelsUnsigned[uint16]
	Uint16SubScalarSels    = NumericScalarSelsUnsigned[uint16]
	Uint16SubByScalarSels  = NumericByScalarSelsUnsigned[uint16]
	Uint32SubSels          = NumericSelsUnsigned[uint32]
	Uint32SubScalarSels    = NumericScalarSelsUnsigned[uint32]
	Uint32SubByScalarSels  = NumericByScalarSelsUnsigned[uint32]
	Uint64SubSels          = NumericSelsUnsigned[uint64]
	Uint64SubScalarSels    = NumericScalarSelsUnsigned[uint64]
	Uint64SubByScalarSels  = NumericByScalarSelsUnsigned[uint64]
	Float32SubSels         = NumericSels[float32]
	Float32SubScalarSels   = NumericScalarSels[float32]
	Float32SubByScalarSels = NumericByScalarSels[float32]
	Float64SubSels         = NumericSels[float64]
	Float64SubScalarSels   = NumericScalarSels[float64]
	Float64SubByScalarSels = NumericByScalarSels[float64]

	Decimal64Sub              = decimal64Sub
	Decimal64SubSels          = decimal64SubSels
	Decimal64SubScalar        = decimal64SubScalar
	Decimal64SubScalarSels    = decimal64SubScalarSels
	Decimal64SubByScalar      = decimal64SubByScalar
	Decimal64SubByScalarSels  = decimal64SubByScalarSels
	Decimal128Sub             = decimal128Sub
	Decimal128SubSels         = decimal128SubSels
	Decimal128SubScalar       = decimal128SubScalar
	Decimal128SubScalarSels   = decimal128SubScalarSels
	Decimal128SubByScalar     = decimal128SubByScalar
	Decimal128SubByScalarSels = decimal128SubByScalarSels

	Int32Int64Sub         = NumericBigSmall[int64, int32]
	Int32Int64SubSels     = NumericSelsBigSmall[int64, int32]
	Int16Int64Sub         = NumericBigSmall[int64, int16]
	Int16Int64SubSels     = NumericSelsBigSmall[int64, int16]
	Int8Int64Sub          = NumericBigSmall[int64, int8]
	Int8Int64SubSels      = NumericSelsBigSmall[int64, int8]
	Int16Int32Sub         = NumericBigSmall[int32, int16]
	Int16Int32SubSels     = NumericSelsBigSmall[int32, int16]
	Int8Int32Sub          = NumericBigSmall[int32, int8]
	Int8Int32SubSels      = NumericSelsBigSmall[int32, int8]
	Int8Int16Sub          = NumericBigSmall[int16, int8]
	Int8Int16SubSels      = NumericSelsBigSmall[int16, int8]
	Uint32Uint64Sub       = NumericBigSmall[uint64, uint32]
	Uint32Uint64SubSels   = NumericSelsBigSmall[uint64, uint32]
	Uint16Uint64Sub       = NumericBigSmall[uint64, uint16]
	Uint16Uint64SubSels   = NumericSelsBigSmall[uint64, uint16]
	Uint8Uint64Sub        = NumericBigSmall[uint64, uint8]
	Uint8Uint64SubSels    = NumericSelsBigSmall[uint64, uint8]
	Uint16Uint32Sub       = NumericBigSmall[uint32, uint16]
	Uint16Uint32SubSels   = NumericSelsBigSmall[uint32, uint16]
	Uint8Uint32Sub        = NumericBigSmall[uint32, uint8]
	Uint8Uint32SubSels    = NumericSelsBigSmall[uint32, uint8]
	Uint8Uint16Sub        = NumericBigSmall[uint16, uint8]
	Uint8Uint16SubSels    = NumericSelsBigSmall[uint16, uint8]
	Float32Float64Sub     = NumericBigSmall[float64, float32]
	Float32Float64SubSels = NumericSelsBigSmall[float64, float32]
)

func NumericSigned[T constraints.Signed](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = x - ys[i]
		if x < 0 && ys[i] > 0 && rs[i] >= 0 {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "int subtraction overflow"))
		}
		if x > 0 && ys[i] < 0 && rs[i] <= 0 {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "int subtraction overflow"))
		}
	}
	return rs
}

func NumericSelsSigned[T constraints.Signed](xs, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
		if xs[sel] < 0 && ys[i] > 0 && rs[i] >= 0 {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "int subtraction overflow"))
		}
		if xs[sel] > 0 && ys[i] < 0 && rs[i] <= 0 {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "int subtraction overflow"))
		}
	}
	return rs
}

func NumericScalarSigned[T constraints.Signed](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x - y
		if x < 0 && ys[i] > 0 && rs[i] >= 0 {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "int subtraction overflow"))
		}
		if x > 0 && ys[i] < 0 && rs[i] <= 0 {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "int subtraction overflow"))
		}
	}
	return rs
}

func NumericScalarSelsSigned[T constraints.Signed](x T, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
		if x < 0 && ys[i] > 0 && rs[i] >= 0 {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "int subtraction overflow"))
		}
		if x > 0 && ys[i] < 0 && rs[i] <= 0 {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "int subtraction overflow"))
		}
	}
	return rs
}

func NumericByScalarSigned[T constraints.Signed](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = y - x
		if y < 0 && x > 0 && rs[i] >= 0 {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "int subtraction overflow"))
		}
		if y > 0 && x < 0 && rs[i] <= 0 {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "int subtraction overflow"))
		}
	}
	return rs
}

func NumericByScalarSelsSigned[T constraints.Signed](x T, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
		if ys[sel] < 0 && x > 0 && rs[i] >= 0 {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "int subtraction underflow"))
		}
		if ys[sel] > 0 && ys[i] < 0 && rs[i] <= 0 {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "int subtraction overflow"))
		}
	}
	return rs
}

func NumericUnsigned[T constraints.Unsigned](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = x - ys[i]
		if rs[i] > x {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "subtraction underflow"))
		}
	}
	return rs
}

func NumericSelsUnsigned[T constraints.Unsigned](xs, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
		if rs[i] > xs[sel] {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "subtraction underflow"))
		}
	}
	return rs
}

func NumericScalarUnsigned[T constraints.Unsigned](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x - y
		if rs[i] > x {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "subtraction underflow"))
		}
	}
	return rs
}

func NumericScalarSelsUnsigned[T constraints.Unsigned](x T, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
		if rs[i] > x {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "subtraction underflow"))
		}
	}
	return rs
}

func NumericByScalarUnsigned[T constraints.Unsigned](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = y - x
		if rs[i] > y {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "subtraction underflow"))
		}
	}
	return rs
}

func NumericByScalarSelsUnsigned[T constraints.Unsigned](x T, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
		if rs[i] > ys[sel] {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "subtraction underflow"))
		}
	}
	return rs
}

func Numeric[T constraints.Integer | constraints.Float](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func NumericSels[T constraints.Integer | constraints.Float](xs, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func NumericScalar[T constraints.Integer | constraints.Float](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func NumericScalarSels[T constraints.Integer | constraints.Float](x T, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func NumericByScalar[T constraints.Integer | constraints.Float](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func NumericByScalarSels[T constraints.Integer | constraints.Float](x T, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func NumericBigSmall[TBig, TSmall constraints.Integer | constraints.Float](xs []TBig, ys []TSmall, rs []TBig) []TBig {
	for i, x := range xs {
		rs[i] = x - TBig(ys[i])
	}
	return rs
}

func NumericSelsBigSmall[TBig, TSmall constraints.Integer | constraints.Float](xs []TBig, ys []TSmall, rs []TBig, sels []int64) []TBig {
	for i, sel := range sels {
		rs[i] = xs[sel] - TBig(ys[sel])
	}
	return rs
}

/*
func numericSubScalarBigSmall[TBig, TSmall constraints.Integer | constraints.Float](x TBig, ys []TSmall, rs []TBig) []TBig {
	for i, y := range ys {
		rs[i] = x - TBig(y)
	}
	return rs
}

func numericSubScalarSelsBigSmall[TBig, TSmall constraints.Integer | constraints.Float](x TBig, ys []TSmall, rs []TBig, sels []int64) []TBig {
	for i, sel := range sels {
		rs[i] = x - TBig(ys[sel])
	}
	return rs
}

func numericSubByScalarBigSmall[TBig, TSmall constraints.Integer | constraints.Float](x TSmall, ys, rs []TBig) []TBig {
	for i, y := range ys {
		rs[i] = y - TBig(x)
	}
	return rs
}

func numericSubByScalarSelsBigSmall[TBig, TSmall constraints.Integer | constraints.Float](x TSmall, ys, rs []TBig, sels []int64) []TBig {
	for i, sel := range sels {
		rs[i] = ys[sel] - TBig(x)
	}
	return rs
}

func numericSubSmallBig[TSmall, TBig constraints.Integer | constraints.Float](xs []TSmall, ys, rs []TBig) []TBig {
	for i, x := range xs {
		rs[i] = TBig(x) - ys[i]
	}
	return rs
}

func numericSubSelsSmallBig[TSmall, TBig constraints.Integer | constraints.Float](xs []TSmall, ys, rs []TBig, sels []int64) []TBig {
	for i, sel := range sels {
		rs[i] = TBig(xs[sel]) - ys[sel]
	}
	return rs
}

func numericSubScalarSmallBig[TSmall, TBig constraints.Integer | constraints.Float](x TSmall, ys, rs []TBig) []TBig {
	for i, y := range ys {
		rs[i] = TBig(x) - y
	}
	return rs
}

func numericSubScalarSelsSmallBig[TSmall, TBig constraints.Integer | constraints.Float](x TSmall, ys, rs []TBig, sels []int64) []TBig {
	for i, sel := range sels {
		rs[i] = TBig(x) - ys[sel]
	}
	return rs
}

func numericSubByScalarSmallBig[TSmall, TBig constraints.Integer | constraints.Float](x TBig, ys []TSmall, rs []TBig) []TBig {
	for i, y := range ys {
		rs[i] = TBig(y) - x
	}
	return rs
}

func numericSubByScalarSelsSmallBig[TSmall, TBig constraints.Integer | constraints.Float](x TBig, ys []TSmall, rs []TBig, sels []int64) []TBig {
	for i, sel := range sels {
		rs[i] = TBig(ys[sel]) - x
	}
	return rs
}
*/

func decimal64Sub(xs []types.Decimal64, ys []types.Decimal64, _ int32, _ int32, rs []types.Decimal64) []types.Decimal64 {
	for i, x := range xs {
		rs[i] = types.Decimal64SubAligned(x, ys[i])
	}
	return rs
}

func decimal64SubSels(xs, ys []types.Decimal64, xsScale, ysScale int32, rs []types.Decimal64, sels []int64) []types.Decimal64 {
	for i, sel := range sels {
		rs[i] = types.Decimal64Sub(xs[sel], ys[sel], xsScale, ysScale)
	}
	return rs
}

func decimal64SubScalar(x types.Decimal64, ys []types.Decimal64, _, _ int32, rs []types.Decimal64) []types.Decimal64 {
	for i, y := range ys {
		rs[i] = types.Decimal64SubAligned(x, y)
	}
	return rs
}

func decimal64SubScalarSels(x types.Decimal64, ys []types.Decimal64, xScale, ysScale int32, rs []types.Decimal64, sels []int64) []types.Decimal64 {
	for i, sel := range sels {
		rs[i] = types.Decimal64Sub(x, ys[sel], xScale, ysScale)
	}
	return rs
}

func decimal64SubByScalar(x types.Decimal64, ys []types.Decimal64, _, _ int32, rs []types.Decimal64) []types.Decimal64 {
	for i, y := range ys {
		rs[i] = types.Decimal64SubAligned(y, x)
	}
	return rs
}

func decimal64SubByScalarSels(x types.Decimal64, ys []types.Decimal64, _, _ int32, rs []types.Decimal64, sels []int64) []types.Decimal64 {
	for i, sel := range sels {
		rs[i] = ys[sel].Sub(x)
	}
	return rs
}

func decimal128Sub(xs []types.Decimal128, ys []types.Decimal128, _ int32, _ int32, rs []types.Decimal128) []types.Decimal128 {
	for i, x := range xs {
		rs[i] = x.Sub(ys[i])
	}
	return rs
}

func decimal128SubSels(xs, ys []types.Decimal128, _, _ int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for i, sel := range sels {
		rs[i] = xs[sel].Sub(ys[sel])
	}
	return rs
}

func decimal128SubScalar(x types.Decimal128, ys []types.Decimal128, _, _ int32, rs []types.Decimal128) []types.Decimal128 {
	for i, y := range ys {
		rs[i] = x.Sub(y)
	}
	return rs
}

func decimal128SubScalarSels(x types.Decimal128, ys []types.Decimal128, xScale, ysScale int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for i, sel := range sels {
		rs[i] = types.Decimal128Sub(x, ys[sel], xScale, ysScale)
	}
	return rs
}

func decimal128SubByScalar(x types.Decimal128, ys []types.Decimal128, _, _ int32, rs []types.Decimal128) []types.Decimal128 {
	for i, y := range ys {
		rs[i] = types.Decimal128SubAligned(y, x)
	}
	return rs
}

func decimal128SubByScalarSels(x types.Decimal128, ys []types.Decimal128, xScale, ysScale int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for i, sel := range sels {
		rs[i] = types.Decimal128Sub(ys[sel], x, ysScale, xScale)
	}
	return rs
}
