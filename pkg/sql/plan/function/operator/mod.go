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
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

// ErrModByZero is reported when computing the rest of a division by zero.
var ErrModByZero = errors.New(errno.SyntaxErrororAccessRuleViolation, "zero modulus")

func ModInt[T constraints.Integer](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := vectors[0], vectors[1]
	lvs, rvs := vector.MustTCols[T](lv), vector.MustTCols[T](rv)
	rtl := lv.Typ.Oid.FixedLength()

	if lv.IsScalarNull() || rv.IsScalarNull() {
		return proc.AllocScalarNullVector(lv.Typ), nil
	}

	switch {
	case lv.IsScalar() && rv.IsScalar():
		vec := proc.AllocScalarVector(lv.Typ)
		rs := make([]T, 1)
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		if !nulls.Any(rv.Nsp) {
			for _, v := range rvs {
				if v == 0 {
					return nil, ErrModByZero
				}
			}
			vector.SetCol(vec, IntMod(lvs, rvs, rs))
			return vec, nil
		}
		sels := process.GetSels(proc)
		defer process.PutSels(sels, proc)
		for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
			if nulls.Contains(rv.Nsp, i) {
				continue
			}
			if rvs[i] == 0 {
				return nil, ErrModByZero
			}
			sels = append(sels, int64(i))
		}
		vector.SetCol(vec, IntModSels(lvs, rvs, rs, sels))
		return vec, nil
	case lv.IsScalar() && !rv.IsScalar():
		if !nulls.Any(rv.Nsp) {
			for _, v := range rvs {
				if v == 0 {
					return nil, ErrModByZero
				}
			}
			vec, err := proc.AllocVector(lv.Typ, int64(rtl)*int64(len(rvs)))
			if err != nil {
				return nil, err
			}
			rs := encoding.DecodeFixedSlice[T](vec.Data, rtl)
			nulls.Set(vec.Nsp, rv.Nsp)
			vector.SetCol(vec, IntModScalar(lvs[0], rvs, rs))
			return vec, nil
		}
		sels := process.GetSels(proc)
		defer process.PutSels(sels, proc)
		for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
			if nulls.Contains(rv.Nsp, i) {
				continue
			}
			if rvs[i] == 0 {
				return nil, ErrModByZero
			}
			sels = append(sels, int64(i))
		}
		vec, err := proc.AllocVector(lv.Typ, int64(rtl)*int64(len(rvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeFixedSlice[T](vec.Data, rtl)
		nulls.Set(vec.Nsp, rv.Nsp)
		vector.SetCol(vec, IntModByScalarSels(lvs[0], rvs, rs, sels))
		return vec, nil
	case !lv.IsScalar() && rv.IsScalar():
		if rvs[0] == 0 {
			return nil, ErrModByZero
		}
		vec, err := proc.AllocVector(lv.Typ, int64(rtl)*int64(len(lvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeFixedSlice[T](vec.Data, rtl)
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, IntModByScalar(rvs[0], lvs, rs))
		return vec, nil
	}
	vec, err := proc.AllocVector(lv.Typ, int64(rtl)*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeFixedSlice[T](vec.Data, rtl)
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	if !nulls.Any(rv.Nsp) {
		for _, v := range rvs {
			if v == 0 {
				return nil, ErrModByZero
			}
		}
		vector.SetCol(vec, IntMod(lvs, rvs, rs))
		return vec, nil
	}
	sels := process.GetSels(proc)
	defer process.PutSels(sels, proc)
	for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
		if nulls.Contains(rv.Nsp, i) {
			continue
		}
		if rvs[i] == 0 {
			return nil, ErrModByZero
		}
		sels = append(sels, int64(i))
	}
	vector.SetCol(vec, IntModSels(lvs, rvs, rs, sels))
	return vec, nil
}

func ModFloat[T constraints.Float](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := vectors[0], vectors[1]
	lvs, rvs := vector.MustTCols[T](lv), vector.MustTCols[T](rv)
	rtl := lv.Typ.Oid.FixedLength()

	if lv.IsScalarNull() || rv.IsScalarNull() {
		return proc.AllocScalarNullVector(lv.Typ), nil
	}

	switch {
	case lv.IsScalar() && rv.IsScalar():
		vec := proc.AllocScalarVector(lv.Typ)
		rs := make([]T, 1)
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		if !nulls.Any(rv.Nsp) {
			for _, v := range rvs {
				if v == 0 {
					return nil, ErrModByZero
				}
			}
			vector.SetCol(vec, FloatMod(lvs, rvs, rs))
			return vec, nil
		}
		sels := process.GetSels(proc)
		defer process.PutSels(sels, proc)
		for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
			if nulls.Contains(rv.Nsp, i) {
				continue
			}
			if rvs[i] == 0 {
				return nil, ErrModByZero
			}
			sels = append(sels, int64(i))
		}
		vector.SetCol(vec, FloatModSels(lvs, rvs, rs, sels))
		return vec, nil
	case lv.IsScalar() && !rv.IsScalar():
		if !nulls.Any(rv.Nsp) {
			for _, v := range rvs {
				if v == 0 {
					return nil, ErrModByZero
				}
			}
			vec, err := proc.AllocVector(lv.Typ, int64(rtl)*int64(len(rvs)))
			if err != nil {
				return nil, err
			}
			rs := encoding.DecodeFixedSlice[T](vec.Data, rtl)
			nulls.Set(vec.Nsp, rv.Nsp)
			vector.SetCol(vec, FloatModScalar(lvs[0], rvs, rs))
			return vec, nil
		}
		sels := process.GetSels(proc)
		defer process.PutSels(sels, proc)
		for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
			if nulls.Contains(rv.Nsp, i) {
				continue
			}
			if rvs[i] == 0 {
				return nil, ErrModByZero
			}
			sels = append(sels, int64(i))
		}
		vec, err := proc.AllocVector(lv.Typ, int64(rtl)*int64(len(rvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeFixedSlice[T](vec.Data, rtl)
		nulls.Set(vec.Nsp, rv.Nsp)
		vector.SetCol(vec, FloatModByScalarSels(lvs[0], rvs, rs, sels))
		return vec, nil
	case !lv.IsScalar() && rv.IsScalar():
		if rvs[0] == 0 {
			return nil, ErrModByZero
		}
		vec, err := proc.AllocVector(lv.Typ, int64(rtl)*int64(len(lvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeFixedSlice[T](vec.Data, rtl)
		nulls.Set(vec.Nsp, lv.Nsp)
		vector.SetCol(vec, FloatModByScalar(rvs[0], lvs, rs))
		return vec, nil
	}
	vec, err := proc.AllocVector(lv.Typ, int64(rtl)*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeFixedSlice[T](vec.Data, rtl)
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	if !nulls.Any(rv.Nsp) {
		for _, v := range rvs {
			if v == 0 {
				return nil, ErrModByZero
			}
		}
		vector.SetCol(vec, FloatMod(lvs, rvs, rs))
		return vec, nil
	}
	sels := process.GetSels(proc)
	defer process.PutSels(sels, proc)
	for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
		if nulls.Contains(rv.Nsp, i) {
			continue
		}
		if rvs[i] == 0 {
			return nil, ErrModByZero
		}
		sels = append(sels, int64(i))
	}
	vector.SetCol(vec, FloatModSels(lvs, rvs, rs, sels))
	return vec, nil
}

var (
	Int8Mod                = IntMod[int8]
	Int8ModSels            = IntModSels[int8]
	Int8ModScalar          = IntModScalar[int8]
	Int8ModScalarSels      = IntModScalarSels[int8]
	Int8ModByScalar        = IntModByScalar[int8]
	Int8ModByScalarSels    = IntModByScalarSels[int8]
	Int16Mod               = IntMod[int16]
	Int16ModSels           = IntModSels[int16]
	Int16ModScalar         = IntModScalar[int16]
	Int16ModScalarSels     = IntModScalarSels[int16]
	Int16ModByScalar       = IntModByScalar[int16]
	Int16ModByScalarSels   = IntModByScalarSels[int16]
	Int32Mod               = IntMod[int32]
	Int32ModSels           = IntModSels[int32]
	Int32ModScalar         = IntModScalar[int32]
	Int32ModScalarSels     = IntModScalarSels[int32]
	Int32ModByScalar       = IntModByScalar[int32]
	Int32ModByScalarSels   = IntModByScalarSels[int32]
	Int64Mod               = IntMod[int64]
	Int64ModSels           = IntModSels[int64]
	Int64ModScalar         = IntModScalar[int64]
	Int64ModScalarSels     = IntModScalarSels[int64]
	Int64ModByScalar       = IntModByScalar[int64]
	Int64ModByScalarSels   = IntModByScalarSels[int64]
	Uint8Mod               = IntMod[uint8]
	Uint8ModSels           = IntModSels[uint8]
	Uint8ModScalar         = IntModScalar[uint8]
	Uint8ModScalarSels     = IntModScalarSels[uint8]
	Uint8ModByScalar       = IntModByScalar[uint8]
	Uint8ModByScalarSels   = IntModByScalarSels[uint8]
	Uint16Mod              = IntMod[uint16]
	Uint16ModSels          = IntModSels[uint16]
	Uint16ModScalar        = IntModScalar[uint16]
	Uint16ModScalarSels    = IntModScalarSels[uint16]
	Uint16ModByScalar      = IntModByScalar[uint16]
	Uint16ModByScalarSels  = IntModByScalarSels[uint16]
	Uint32Mod              = IntMod[uint32]
	Uint32ModSels          = IntModSels[uint32]
	Uint32ModScalar        = IntModScalar[uint32]
	Uint32ModScalarSels    = IntModScalarSels[uint32]
	Uint32ModByScalar      = IntModByScalar[uint32]
	Uint32ModByScalarSels  = IntModByScalarSels[uint32]
	Uint64Mod              = IntMod[uint64]
	Uint64ModSels          = IntModSels[uint64]
	Uint64ModScalar        = IntModScalar[uint64]
	Uint64ModScalarSels    = IntModScalarSels[uint64]
	Uint64ModByScalar      = IntModByScalar[uint64]
	Uint64ModByScalarSels  = IntModByScalarSels[uint64]
	Float32Mod             = FloatMod[float32]
	Float32ModSels         = FloatModSels[float32]
	Float32ModScalar       = FloatModScalar[float32]
	Float32ModScalarSels   = FloatModScalarSels[float32]
	Float32ModByScalar     = FloatModByScalar[float32]
	Float32ModByScalarSels = FloatModByScalarSels[float32]
	Float64Mod             = FloatMod[float64]
	Float64ModSels         = FloatModSels[float64]
	Float64ModScalar       = FloatModScalar[float64]
	Float64ModScalarSels   = FloatModScalarSels[float64]
	Float64ModByScalar     = FloatModByScalar[float64]
	Float64ModByScalarSels = FloatModByScalarSels[float64]
)

func IntMod[T constraints.Integer](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func IntModSels[T constraints.Integer](xs, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = xs[sel] % ys[sel]
	}
	return rs
}

func IntModScalar[T constraints.Integer](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func IntModScalarSels[T constraints.Integer](x T, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = x % ys[sel]
	}
	return rs
}

func IntModByScalar[T constraints.Integer](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = y % x
	}
	return rs
}

func IntModByScalarSels[T constraints.Integer](x T, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = ys[sel] % x
	}
	return rs
}

func FloatMod[T constraints.Float](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = T(math.Mod(float64(x), float64(ys[i])))
	}
	return rs
}

func FloatModSels[T constraints.Float](xs, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = T(math.Mod(float64(xs[sel]), float64(ys[sel])))
	}
	return rs
}

func FloatModScalar[T constraints.Float](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = T(math.Mod(float64(x), float64(y)))
	}
	return rs
}

func FloatModScalarSels[T constraints.Float](x T, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = T(math.Mod(float64(x), float64(ys[sel])))
	}
	return rs
}

func FloatModByScalar[T constraints.Float](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = T(math.Mod(float64(y), float64(x)))
	}
	return rs
}

func FloatModByScalarSels[T constraints.Float](x T, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = T(math.Mod(float64(ys[sel]), float64(x)))
	}
	return rs
}
