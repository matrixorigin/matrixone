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

func Minus[T constraints.Integer | constraints.Float](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := vectors[0], vectors[1]
	lvs, rvs := lv.Col.([]T), rv.Col.([]T)
	resultElementSize := lv.Typ.Oid.FixedLength()

	switch {
	case lv.IsConst && rv.IsConst:
		resultVector, err := process.Get2(proc, int64(resultElementSize), lv.Typ)
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFixedSlice[T](resultVector.Data, resultElementSize)
		nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, sub.Numeric(lvs, rvs, resultValues))
		resultVector.IsConst = true
		resultVector.Length = lv.Length
		return resultVector, nil
	case lv.IsConst && !rv.IsConst:
		resultVector, err := process.Get2(proc, int64(resultElementSize*len(rvs)), lv.Typ)
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFixedSlice[T](resultVector.Data, resultElementSize)
		nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, sub.NumericScalar[T](lvs[0], rvs, resultValues))
		return resultVector, nil
	case !lv.IsConst && rv.IsConst:
		resultVector, err := process.Get2(proc, int64(resultElementSize*len(lvs)), lv.Typ)
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFixedSlice[T](resultVector.Data, resultElementSize)
		nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, sub.NumericScalar[T](rvs[0], lvs, resultValues))
		return resultVector, nil
	}
	resultVector, err := process.Get2(proc, int64(resultElementSize*len(lvs)), lv.Typ)
	if err != nil {
		return nil, err
	}
	resultValues := encoding.DecodeFixedSlice[T](resultVector.Data, resultElementSize)
	nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)
	vector.SetCol(resultVector, sub.Numeric[T](lvs, rvs, resultValues))
	return resultVector, nil
}

// Since the underlying operator does not generically process decimal64 and decimal128, sub of decimal64 and decimal128 are not generalized
func MinusDecimal64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv := vectors[0]
	rv := vectors[1]
	lvs, rvs := lv.Col.([]types.Decimal64), rv.Col.([]types.Decimal64)
	lvScale, rvScale := lv.Typ.Scale, rv.Typ.Scale
	resultScale := lvScale
	if lvScale < rvScale {
		resultScale = rvScale
	}
	resultTyp := types.Type{Oid: types.T_decimal64, Size: 8, Width: 18, Scale: resultScale}
	switch {
	case lv.IsConst && rv.IsConst:
		vec, err := process.Get2(proc, int64(resultTyp.Size), resultTyp)
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeDecimal64Slice(vec.Data)
		rs = rs[:len(rvs)]
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, sub.Decimal64Sub(lvs, rvs, lvScale, rvScale, rs))
		vec.IsConst = true
		vec.Length = lv.Length
		return vec, nil
	case lv.IsConst && !rv.IsConst:
		vec, err := process.Get2(proc, int64(resultTyp.Size)*int64(len(rvs)), resultTyp)
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeDecimal64Slice(vec.Data)
		rs = rs[:len(rvs)]
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, sub.Decimal64SubScalar(lvs[0], rvs, lvScale, rvScale, rs))
		return vec, nil
	case !lv.IsConst && rv.IsConst:
		vec, err := process.Get2(proc, int64(resultTyp.Size)*int64(len(lvs)), resultTyp)
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeDecimal64Slice(vec.Data)
		rs = rs[:len(lvs)]
		nulls.Set(vec.Nsp, lv.Nsp)
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, sub.Decimal64SubScalar(rvs[0], lvs, rvScale, lvScale, rs))
		return vec, nil
	}
	vec, err := process.Get2(proc, int64(resultTyp.Size)*int64(len(lvs)), lv.Typ)
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeDecimal64Slice(vec.Data)
	rs = rs[:len(rvs)]
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	vector.SetCol(vec, sub.Decimal64Sub(lvs, rvs, lv.Typ.Scale, rv.Typ.Scale, rs))
	return vec, nil
}

func MinusDecimal128(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv := vectors[0]
	rv := vectors[1]
	lvs, rvs := lv.Col.([]types.Decimal128), rv.Col.([]types.Decimal128)
	lvScale := lv.Typ.Scale
	rvScale := rv.Typ.Scale
	resultScale := lvScale
	if lvScale < rvScale {
		resultScale = rvScale
	}
	resultTyp := types.Type{Oid: types.T_decimal128, Size: 16, Width: 38, Scale: resultScale}
	switch {
	case lv.IsConst && rv.IsConst:
		vec, err := process.Get2(proc, int64(resultTyp.Size), resultTyp)
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeDecimal128Slice(vec.Data)
		rs = rs[:len(rvs)]
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, sub.Decimal128Sub(lvs, rvs, lvScale, rvScale, rs))
		vec.IsConst = true
		vec.Length = lv.Length
		return vec, nil
	case lv.IsConst && !rv.IsConst:
		vec, err := process.Get(proc, int64(resultTyp.Size)*int64(len(rvs)), resultTyp)
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeDecimal128Slice(vec.Data)
		rs = rs[:len(rvs)]
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, sub.Decimal128SubScalar(lvs[0], rvs, lvScale, rvScale, rs))
		return vec, nil
	case !lv.IsConst && rv.IsConst:
		vec, err := process.Get(proc, int64(resultTyp.Size)*int64(len(lvs)), resultTyp)
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeDecimal128Slice(vec.Data)
		rs = rs[:len(lvs)]
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, sub.Decimal128SubScalar(rvs[0], lvs, rvScale, lvScale, rs))
		return vec, nil
	}
	vec, err := process.Get(proc, int64(resultTyp.Size)*int64(len(lvs)), resultTyp)
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeDecimal128Slice(vec.Data)
	rs = rs[:len(rvs)]
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	vector.SetCol(vec, sub.Decimal128Sub(lvs, rvs, lv.Typ.Scale, rv.Typ.Scale, rs))
	return vec, nil
}
