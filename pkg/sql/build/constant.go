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

package build

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sqlerror"
	"math"
)

func Neg(x *extend.ValueExtend) (extend.Extend, error) {
	switch x.V.Typ.Oid {
	case types.T_int64:
		x.V.Col = []int64{-1 * x.V.Col.([]int64)[0]}
		return x, nil
	case types.T_float64:
		x.V.Col = []float64{-1 * x.V.Col.([]float64)[0]}
		return x, nil
	}
	return nil, sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf(" %s cannot neg", x.V.Typ))
}

func Eq(x, y *extend.ValueExtend) (extend.Extend, error) {
	vec := vector.New(types.Type{Oid: types.T_int64, Size: 8})
	vec.Ref = 1
	switch {
	case x.V.Typ.Oid == types.T_int64 && y.V.Typ.Oid == types.T_int64:
		if x.V.Col.([]int64)[0] == y.V.Col.([]int64)[0] {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_int64 && y.V.Typ.Oid == types.T_float64:
		if float64(x.V.Col.([]int64)[0]) == y.V.Col.([]float64)[0] {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_float64 && y.V.Typ.Oid == types.T_int64:
		if x.V.Col.([]float64)[0] == float64(y.V.Col.([]int64)[0]) {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_float64 && y.V.Typ.Oid == types.T_float64:
		if x.V.Col.([]float64)[0] == y.V.Col.([]float64)[0] {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_varchar && y.V.Typ.Oid == types.T_varchar:
		if bytes.Compare(x.V.Col.(*types.Bytes).Data, y.V.Col.(*types.Bytes).Data) == 0 {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	default:
		return nil, sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf(" %s cannot eq %s", y.V.Typ, x.V.Typ))
	}
	return &extend.ValueExtend{V: vec}, nil
}

func Ne(x, y *extend.ValueExtend) (extend.Extend, error) {
	vec := vector.New(types.Type{Oid: types.T_int64, Size: 8})
	vec.Ref = 1
	switch {
	case x.V.Typ.Oid == types.T_int64 && y.V.Typ.Oid == types.T_int64:
		if x.V.Col.([]int64)[0] != y.V.Col.([]int64)[0] {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_int64 && y.V.Typ.Oid == types.T_float64:
		if float64(x.V.Col.([]int64)[0]) != y.V.Col.([]float64)[0] {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_float64 && y.V.Typ.Oid == types.T_int64:
		if x.V.Col.([]float64)[0] != float64(y.V.Col.([]int64)[0]) {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_float64 && y.V.Typ.Oid == types.T_float64:
		if x.V.Col.([]float64)[0] != y.V.Col.([]float64)[0] {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_varchar && y.V.Typ.Oid == types.T_varchar:
		if bytes.Compare(x.V.Col.(*types.Bytes).Data, y.V.Col.(*types.Bytes).Data) != 0 {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	default:
		return nil, sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf(" %s cannot eq %s", y.V.Typ, x.V.Typ))
	}
	return &extend.ValueExtend{V: vec}, nil
}

func Lt(x, y *extend.ValueExtend) (extend.Extend, error) {
	vec := vector.New(types.Type{Oid: types.T_int64, Size: 8})
	vec.Ref = 1
	switch {
	case x.V.Typ.Oid == types.T_int64 && y.V.Typ.Oid == types.T_int64:
		if x.V.Col.([]int64)[0] < y.V.Col.([]int64)[0] {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_int64 && y.V.Typ.Oid == types.T_float64:
		if float64(x.V.Col.([]int64)[0]) < y.V.Col.([]float64)[0] {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_float64 && y.V.Typ.Oid == types.T_int64:
		if x.V.Col.([]float64)[0] < float64(y.V.Col.([]int64)[0]) {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_float64 && y.V.Typ.Oid == types.T_float64:
		if x.V.Col.([]float64)[0] < y.V.Col.([]float64)[0] {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_varchar && y.V.Typ.Oid == types.T_varchar:
		if bytes.Compare(x.V.Col.(*types.Bytes).Data, y.V.Col.(*types.Bytes).Data) < 0 {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	default:
		return nil, sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf(" %s cannot eq %s", y.V.Typ, x.V.Typ))
	}
	return &extend.ValueExtend{V: vec}, nil
}

func Le(x, y *extend.ValueExtend) (extend.Extend, error) {
	vec := vector.New(types.Type{Oid: types.T_int64, Size: 8})
	vec.Ref = 1
	switch {
	case x.V.Typ.Oid == types.T_int64 && y.V.Typ.Oid == types.T_int64:
		if x.V.Col.([]int64)[0] <= y.V.Col.([]int64)[0] {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_int64 && y.V.Typ.Oid == types.T_float64:
		if float64(x.V.Col.([]int64)[0]) <= y.V.Col.([]float64)[0] {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_float64 && y.V.Typ.Oid == types.T_int64:
		if x.V.Col.([]float64)[0] <= float64(y.V.Col.([]int64)[0]) {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_float64 && y.V.Typ.Oid == types.T_float64:
		if x.V.Col.([]float64)[0] <= y.V.Col.([]float64)[0] {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_varchar && y.V.Typ.Oid == types.T_varchar:
		if bytes.Compare(x.V.Col.(*types.Bytes).Data, y.V.Col.(*types.Bytes).Data) <= 0 {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	default:
		return nil, sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf(" %s cannot eq %s", y.V.Typ, x.V.Typ))
	}
	return &extend.ValueExtend{V: vec}, nil
}

func Gt(x, y *extend.ValueExtend) (extend.Extend, error) {
	vec := vector.New(types.Type{Oid: types.T_int64, Size: 8})
	vec.Ref = 1
	switch {
	case x.V.Typ.Oid == types.T_int64 && y.V.Typ.Oid == types.T_int64:
		if x.V.Col.([]int64)[0] > y.V.Col.([]int64)[0] {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_int64 && y.V.Typ.Oid == types.T_float64:
		if float64(x.V.Col.([]int64)[0]) > y.V.Col.([]float64)[0] {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_float64 && y.V.Typ.Oid == types.T_int64:
		if x.V.Col.([]float64)[0] > float64(y.V.Col.([]int64)[0]) {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_float64 && y.V.Typ.Oid == types.T_float64:
		if x.V.Col.([]float64)[0] > y.V.Col.([]float64)[0] {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_varchar && y.V.Typ.Oid == types.T_varchar:
		if bytes.Compare(x.V.Col.(*types.Bytes).Data, y.V.Col.(*types.Bytes).Data) > 0 {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	default:
		return nil, sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf(" %s cannot eq %s", y.V.Typ, x.V.Typ))
	}
	return &extend.ValueExtend{V: vec}, nil
}

func Ge(x, y *extend.ValueExtend) (extend.Extend, error) {
	vec := vector.New(types.Type{Oid: types.T_int64, Size: 8})
	vec.Ref = 1
	switch {
	case x.V.Typ.Oid == types.T_int64 && y.V.Typ.Oid == types.T_int64:
		if x.V.Col.([]int64)[0] >= y.V.Col.([]int64)[0] {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_int64 && y.V.Typ.Oid == types.T_float64:
		if float64(x.V.Col.([]int64)[0]) >= y.V.Col.([]float64)[0] {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_float64 && y.V.Typ.Oid == types.T_int64:
		if x.V.Col.([]float64)[0] >= float64(y.V.Col.([]int64)[0]) {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_float64 && y.V.Typ.Oid == types.T_float64:
		if x.V.Col.([]float64)[0] >= y.V.Col.([]float64)[0] {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	case x.V.Typ.Oid == types.T_varchar && y.V.Typ.Oid == types.T_varchar:
		if bytes.Compare(x.V.Col.(*types.Bytes).Data, y.V.Col.(*types.Bytes).Data) >= 0 {
			vec.SetCol([]int64{1})
		} else {
			vec.SetCol([]int64{0})
		}
	default:
		return nil, sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf(" %s cannot eq %s", y.V.Typ, x.V.Typ))
	}
	return &extend.ValueExtend{V: vec}, nil
}

func div(x, y *extend.ValueExtend) (extend.Extend, error) {
	var xv, yv float64

	vec := vector.New(types.Type{Oid: types.T_float64, Size: 8})
	vec.Ref = 1
	switch {
	case x.V.Typ.Oid == types.T_int64 && y.V.Typ.Oid == types.T_int64:
		xv, yv = float64(x.V.Col.([]int64)[0]), float64(y.V.Col.([]int64)[0])
	case x.V.Typ.Oid == types.T_int64 && y.V.Typ.Oid == types.T_float64:
		xv, yv = float64(x.V.Col.([]int64)[0]), y.V.Col.([]float64)[0]
	case x.V.Typ.Oid == types.T_float64 && y.V.Typ.Oid == types.T_int64:
		xv, yv = x.V.Col.([]float64)[0], float64(y.V.Col.([]int64)[0])
	case x.V.Typ.Oid == types.T_float64 && y.V.Typ.Oid == types.T_float64:
		xv, yv = x.V.Col.([]float64)[0], y.V.Col.([]float64)[0]
	default:
		return nil, sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf(" %s cannot div %s", y.V.Typ, x.V.Typ))
	}
	vec.Col = []float64{xv / yv}
	return &extend.ValueExtend{V: vec}, nil
}

func mod(x, y *extend.ValueExtend) (extend.Extend, error) {
	if x.V.Typ.Oid == types.T_int64 && y.V.Typ.Oid == types.T_int64 {
		vec := vector.New(types.Type{Oid: types.T_int64, Size: 8})
		vec.Ref = 1
		vec.Col = []int64{x.V.Col.([]int64)[0] % y.V.Col.([]int64)[0]}
		return &extend.ValueExtend{V: vec}, nil
	}
	if x.V.Typ.Oid == types.T_int64 && y.V.Typ.Oid == types.T_float64 {
		vec := vector.New(types.Type{Oid: types.T_float64, Size: 8})
		vec.Ref = 1
		vec.Col = []float64{math.Mod(float64(x.V.Col.([]int64)[0]), y.V.Col.([]float64)[0])}
		return &extend.ValueExtend{V: vec}, nil
	}
	if x.V.Typ.Oid == types.T_float64 && y.V.Typ.Oid == types.T_int64 {
		vec := vector.New(types.Type{Oid: types.T_float64, Size: 8})
		vec.Ref = 1
		vec.Col = []float64{math.Mod(x.V.Col.([]float64)[0], float64(y.V.Col.([]int64)[0]))}
		return &extend.ValueExtend{V: vec}, nil
	}
	if x.V.Typ.Oid == types.T_float64 && y.V.Typ.Oid == types.T_float64 {
		vec := vector.New(types.Type{Oid: types.T_float64, Size: 8})
		vec.Ref = 1
		vec.Col = []float64{math.Mod(x.V.Col.([]float64)[0], y.V.Col.([]float64)[0])}
		return &extend.ValueExtend{V: vec}, nil
	}
	return nil, sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf(" %s cannot mod %s", y.V.Typ, x.V.Typ))
}

func mul(x, y *extend.ValueExtend) (extend.Extend, error) {
	if x.V.Typ.Oid == types.T_int64 && y.V.Typ.Oid == types.T_int64 {
		vec := vector.New(types.Type{Oid: types.T_int64, Size: 8})
		vec.Ref = 1
		vec.Col = []int64{x.V.Col.([]int64)[0] * y.V.Col.([]int64)[0]}
		return &extend.ValueExtend{V: vec}, nil
	}
	if x.V.Typ.Oid == types.T_int64 && y.V.Typ.Oid == types.T_float64 {
		vec := vector.New(types.Type{Oid: types.T_float64, Size: 8})
		vec.Ref = 1
		vec.Col = []float64{float64(x.V.Col.([]int64)[0]) * y.V.Col.([]float64)[0]}
		return &extend.ValueExtend{V: vec}, nil
	}
	if x.V.Typ.Oid == types.T_float64 && y.V.Typ.Oid == types.T_int64 {
		vec := vector.New(types.Type{Oid: types.T_float64, Size: 8})
		vec.Ref = 1
		vec.Col = []float64{x.V.Col.([]float64)[0] * float64(y.V.Col.([]int64)[0])}
		return &extend.ValueExtend{V: vec}, nil
	}
	if x.V.Typ.Oid == types.T_float64 && y.V.Typ.Oid == types.T_float64 {
		vec := vector.New(types.Type{Oid: types.T_float64, Size: 8})
		vec.Ref = 1
		vec.Col = []float64{x.V.Col.([]float64)[0] * y.V.Col.([]float64)[0]}
		return &extend.ValueExtend{V: vec}, nil
	}
	return nil, sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf(" %s cannot mul %s", y.V.Typ, x.V.Typ))
}

func plus(x, y *extend.ValueExtend) (extend.Extend, error) {
	if x.V.Typ.Oid == types.T_int64 && y.V.Typ.Oid == types.T_int64 {
		vec := vector.New(types.Type{Oid: types.T_int64, Size: 8})
		vec.Ref = 1
		vec.Col = []int64{x.V.Col.([]int64)[0] + y.V.Col.([]int64)[0]}
		return &extend.ValueExtend{V: vec}, nil
	}
	if x.V.Typ.Oid == types.T_int64 && y.V.Typ.Oid == types.T_float64 {
		vec := vector.New(types.Type{Oid: types.T_float64, Size: 8})
		vec.Ref = 1
		vec.Col = []float64{float64(x.V.Col.([]int64)[0]) + y.V.Col.([]float64)[0]}
		return &extend.ValueExtend{V: vec}, nil
	}
	if x.V.Typ.Oid == types.T_float64 && y.V.Typ.Oid == types.T_int64 {
		vec := vector.New(types.Type{Oid: types.T_float64, Size: 8})
		vec.Ref = 1
		vec.Col = []float64{x.V.Col.([]float64)[0] + float64(y.V.Col.([]int64)[0])}
		return &extend.ValueExtend{V: vec}, nil
	}
	if x.V.Typ.Oid == types.T_float64 && y.V.Typ.Oid == types.T_float64 {
		vec := vector.New(types.Type{Oid: types.T_float64, Size: 8})
		vec.Ref = 1
		vec.Col = []float64{x.V.Col.([]float64)[0] + y.V.Col.([]float64)[0]}
		return &extend.ValueExtend{V: vec}, nil
	}
	return nil, sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf(" %s cannot plus %s", y.V.Typ, x.V.Typ))
}

func minus(x, y *extend.ValueExtend) (extend.Extend, error) {
	if x.V.Typ.Oid == types.T_int64 && y.V.Typ.Oid == types.T_int64 {
		vec := vector.New(types.Type{Oid: types.T_int64, Size: 8})
		vec.Ref = 1
		vec.Col = []int64{x.V.Col.([]int64)[0] - y.V.Col.([]int64)[0]}
		return &extend.ValueExtend{V: vec}, nil
	}
	if x.V.Typ.Oid == types.T_int64 && y.V.Typ.Oid == types.T_float64 {
		vec := vector.New(types.Type{Oid: types.T_float64, Size: 8})
		vec.Ref = 1
		vec.Col = []float64{float64(x.V.Col.([]int64)[0]) - y.V.Col.([]float64)[0]}
		return &extend.ValueExtend{V: vec}, nil
	}
	if x.V.Typ.Oid == types.T_float64 && y.V.Typ.Oid == types.T_int64 {
		vec := vector.New(types.Type{Oid: types.T_float64, Size: 8})
		vec.Ref = 1
		vec.Col = []float64{x.V.Col.([]float64)[0] - float64(y.V.Col.([]int64)[0])}
		return &extend.ValueExtend{V: vec}, nil
	}
	if x.V.Typ.Oid == types.T_float64 && y.V.Typ.Oid == types.T_float64 {
		vec := vector.New(types.Type{Oid: types.T_float64, Size: 8})
		vec.Ref = 1
		vec.Col = []float64{x.V.Col.([]float64)[0] - y.V.Col.([]float64)[0]}
		return &extend.ValueExtend{V: vec}, nil
	}
	return nil, sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf(" %s cannot minus %s", y.V.Typ, x.V.Typ))
}

func isZero(e *extend.ValueExtend) bool {
	if e.V.Typ.Oid == types.T_int64 && e.V.Col.([]int64)[0] == 0 {
		return true
	}
	if e.V.Typ.Oid == types.T_float64 && e.V.Col.([]float64)[0] == 0 {
		return true
	}
	return false
}

func toInt8(e *extend.ValueExtend) error {
	vec := vector.New(types.Type{Oid: types.T_int8, Size: 1})
	vec.Ref = 1
	switch e.V.Typ.Oid {
	case types.T_int64:
		vec.Col = []int8{int8(e.V.Col.([]int64)[0])}
	case types.T_float64:
		vec.Col = []int8{int8(e.V.Col.([]float64)[0])}
	default:
		return sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf("cannot convert %s to int8", e.V.Typ))
	}
	e.V = vec
	return nil
}

func toInt16(e *extend.ValueExtend) error {
	vec := vector.New(types.Type{Oid: types.T_int16, Size: 2})
	vec.Ref = 1
	switch e.V.Typ.Oid {
	case types.T_int64:
		vec.Col = []int16{int16(e.V.Col.([]int64)[0])}
	case types.T_float64:
		vec.Col = []int16{int16(e.V.Col.([]float64)[0])}
	default:
		return sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf("cannot convert %s to int16", e.V.Typ))
	}
	e.V = vec
	return nil
}

func toInt32(e *extend.ValueExtend) error {
	vec := vector.New(types.Type{Oid: types.T_int32, Size: 4})
	vec.Ref = 1
	switch e.V.Typ.Oid {
	case types.T_int64:
		vec.Col = []int32{int32(e.V.Col.([]int64)[0])}
	case types.T_float64:
		vec.Col = []int32{int32(e.V.Col.([]float64)[0])}
	default:
		return sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf("cannot convert %s to int32", e.V.Typ))
	}
	e.V = vec
	return nil
}

func toInt64(e *extend.ValueExtend) error {
	vec := vector.New(types.Type{Oid: types.T_int64, Size: 8})
	vec.Ref = 1
	switch e.V.Typ.Oid {
	case types.T_int64:
		vec.Col = []int64{int64(e.V.Col.([]int64)[0])}
	case types.T_float64:
		vec.Col = []int64{int64(e.V.Col.([]float64)[0])}
	default:
		return sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf("cannot convert %s to int64", e.V.Typ))
	}
	e.V = vec
	return nil
}

func toUint8(e *extend.ValueExtend) error {
	vec := vector.New(types.Type{Oid: types.T_uint8, Size: 1})
	vec.Ref = 1
	switch e.V.Typ.Oid {
	case types.T_int64:
		vec.Col = []uint8{uint8(e.V.Col.([]int64)[0])}
	case types.T_float64:
		vec.Col = []uint8{uint8(e.V.Col.([]float64)[0])}
	default:
		return sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf("cannot convert %s to uint8", e.V.Typ))
	}
	e.V = vec
	return nil
}

func toUint16(e *extend.ValueExtend) error {
	vec := vector.New(types.Type{Oid: types.T_uint16, Size: 2})
	vec.Ref = 1
	switch e.V.Typ.Oid {
	case types.T_int64:
		vec.Col = []uint16{uint16(e.V.Col.([]int64)[0])}
	case types.T_float64:
		vec.Col = []uint16{uint16(e.V.Col.([]float64)[0])}
	default:
		return sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf("cannot convert %s to uint16", e.V.Typ))
	}
	e.V = vec
	return nil
}

func toUint32(e *extend.ValueExtend) error {
	vec := vector.New(types.Type{Oid: types.T_uint32, Size: 4})
	vec.Ref = 1
	switch e.V.Typ.Oid {
	case types.T_int64:
		vec.Col = []uint32{uint32(e.V.Col.([]int64)[0])}
	case types.T_float64:
		vec.Col = []uint32{uint32(e.V.Col.([]float64)[0])}
	default:
		return sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf("cannot convert %s to uint32", e.V.Typ))
	}
	e.V = vec
	return nil
}

func toUint64(e *extend.ValueExtend) error {
	vec := vector.New(types.Type{Oid: types.T_uint64, Size: 8})
	vec.Ref = 1
	switch e.V.Typ.Oid {
	case types.T_int64:
		vec.Col = []uint64{uint64(e.V.Col.([]int64)[0])}
	case types.T_float64:
		vec.Col = []uint64{uint64(e.V.Col.([]float64)[0])}
	default:
		return sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf("cannot convert %s to uint64", e.V.Typ))
	}
	e.V = vec
	return nil
}

func toFloat32(e *extend.ValueExtend) error {
	vec := vector.New(types.Type{Oid: types.T_float32, Size: 4})
	vec.Ref = 1
	switch e.V.Typ.Oid {
	case types.T_int64:
		vec.Col = []float32{float32(e.V.Col.([]int64)[0])}
	case types.T_float64:
		vec.Col = []float32{float32(e.V.Col.([]float64)[0])}
	default:
		return sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf("cannot convert %s to float32", e.V.Typ))
	}
	e.V = vec
	return nil
}

func toFloat64(e *extend.ValueExtend) error {
	vec := vector.New(types.Type{Oid: types.T_float64, Size: 8})
	vec.Ref = 1
	switch e.V.Typ.Oid {
	case types.T_int64:
		vec.Col = []float64{float64(e.V.Col.([]int64)[0])}
	case types.T_float64:
		vec.Col = []float64{float64(e.V.Col.([]float64)[0])}
	default:
		return sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf("cannot convert %s to float64", e.V.Typ))
	}
	e.V = vec
	return nil
}

func toChar(e *extend.ValueExtend) error {
	switch e.V.Typ.Oid {
	case types.T_varchar:
		e.V.Typ.Oid = types.T_char
		e.V.Ref = 1
	case types.T_int64, types.T_float64: // just ignore at this stage because binary operator supports it now.
		return nil
	default:
		return sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf("cannot convert %s to char", e.V.Typ))
	}
	return nil
}
