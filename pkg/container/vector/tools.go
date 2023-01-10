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

package vector

import (
	"context"
	"reflect"
	"strings"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"golang.org/x/exp/constraints"
)

func MustTCols[T any](v *Vector) []T {
	// XXX hack.   Sometimes we generate an t_any, for untyped const null.
	// This should be handled more carefully and gracefully.
	if v.GetType().Oid == types.T_any {
		return nil
	}
	if v.class == CONSTANT {
		vs := (*(*[]T)(unsafe.Pointer(v.col)))[:v.length]
		rs := make([]T, v.length)
		for i := range rs {
			rs[i] = vs[0]
		}
		return rs
	}
	return (*(*[]T)(unsafe.Pointer(v.col)))[:v.length]
}

func MustBytesCols(v *Vector) [][]byte {
	ret := make([][]byte, v.length)
	varcol := MustTCols[types.Varlena](v)
	if v.class == CONSTANT {
		for i := range varcol {
			ret[i] = (&varcol[0]).GetByteSlice(v.area)
		}
	} else {
		for i := range varcol {
			ret[i] = (&varcol[i]).GetByteSlice(v.area)
		}
	}
	return ret
}

func MustStrCols(v *Vector) []string {
	ret := make([]string, v.length)
	varcol := MustTCols[types.Varlena](v)
	if v.class == CONSTANT {
		for i := range varcol {
			ret[i] = (&varcol[0]).GetString(v.area)
		}
	} else {
		for i := range varcol {
			ret[i] = (&varcol[i]).GetString(v.area)
		}
	}
	return ret
}

func MustVarlenaRawData(v *Vector) (data []types.Varlena, area []byte) {
	data = MustTCols[types.Varlena](v)
	area = v.area
	return
}

// XXX extend will extend the vector's Data to accommodate rows more entry.
func extend[T any](v *Vector, rows int, m *mpool.MPool) error {
	sz := v.typ.TypeSize()
	if length := v.length + rows; length >= v.col.Cap/sz {
		data := (*(*[]byte)(unsafe.Pointer(v.col)))[:v.length*sz]
		ndata, err := m.Grow(data, length*sz)
		if err != nil {
			return err
		}
		*(*[]byte)(unsafe.Pointer(v.col)) = ndata
	}
	return nil
}

func VectorToProtoVector(vec *Vector) (*api.Vector, error) {
	nsp, err := vec.nsp.Show()
	if err != nil {
		return nil, err
	}
	sz := vec.typ.TypeSize()
	return &api.Vector{
		Nsp:      nsp,
		Nullable: true,
		Area:     vec.area,
		IsConst:  vec.IsConst(),
		Len:      uint32(vec.length),
		Type:     TypeToProtoType(vec.typ),
		Data:     (*(*[]byte)(unsafe.Pointer(vec.col)))[:vec.length*sz],
	}, nil
}

func ProtoVectorToVector(vec *api.Vector) (*Vector, error) {
	rvec := &Vector{
		col:    new(reflect.SliceHeader),
		area:   vec.Area,
		length: int(vec.Len),
		typ:    ProtoTypeToType(vec.Type),
	}
	if vec.IsConst {
		rvec.class = CONSTANT
	} else {
		rvec.class = FLAT
	}
	rvec.nsp = &nulls.Nulls{}
	if err := rvec.nsp.Read(vec.Nsp); err != nil {
		return nil, err
	}
	*(*[]byte)(unsafe.Pointer(rvec.col)) = vec.Data
	return rvec, nil
}

func TypeToProtoType(typ types.Type) *plan.Type {
	return &plan.Type{
		Id:        int32(typ.Oid),
		Width:     typ.Width,
		Precision: typ.Precision,
		Size:      typ.Size,
		Scale:     typ.Scale,
	}
}

func ProtoTypeToType(typ *plan.Type) types.Type {
	return types.Type{
		Oid:       types.T(typ.Id),
		Size:      typ.Size,
		Width:     typ.Width,
		Scale:     typ.Scale,
		Precision: typ.Precision,
	}
}

// CompareAndCheckIntersect  we use this method for eval expr by zonemap
func (v *Vector) CompareAndCheckIntersect(vec *Vector) (bool, error) {
	switch v.typ.Oid {
	case types.T_int8:
		return checkNumberIntersect[int8](v, vec)
	case types.T_int16:
		return checkNumberIntersect[int16](v, vec)
	case types.T_int32:
		return checkNumberIntersect[int32](v, vec)
	case types.T_int64:
		return checkNumberIntersect[int64](v, vec)
	case types.T_uint8:
		return checkNumberIntersect[uint8](v, vec)
	case types.T_uint16:
		return checkNumberIntersect[uint16](v, vec)
	case types.T_uint32:
		return checkNumberIntersect[uint32](v, vec)
	case types.T_uint64:
		return checkNumberIntersect[uint64](v, vec)
	case types.T_float32:
		return checkNumberIntersect[float32](v, vec)
	case types.T_float64:
		return checkNumberIntersect[float64](v, vec)
	case types.T_date:
		return checkNumberIntersect[types.Date](v, vec)
	case types.T_time:
		return checkNumberIntersect[types.Time](v, vec)
	case types.T_datetime:
		return checkNumberIntersect[types.Datetime](v, vec)
	case types.T_timestamp:
		return checkNumberIntersect[types.Timestamp](v, vec)
	case types.T_decimal64:
		return checkGeneralIntersect(v, vec, func(t1, t2 types.Decimal64) bool {
			return t1.Ge(t2)
		}, func(t1, t2 types.Decimal64) bool {
			return t1.Le(t2)
		})
	case types.T_decimal128:
		return checkGeneralIntersect(v, vec, func(t1, t2 types.Decimal128) bool {
			return t1.Ge(t2)
		}, func(t1, t2 types.Decimal128) bool {
			return t1.Le(t2)
		})
	case types.T_uuid:
		return checkGeneralIntersect(v, vec, func(t1, t2 types.Uuid) bool {
			return t1.Ge(t2)
		}, func(t1, t2 types.Uuid) bool {
			return t1.Le(t2)
		})
	case types.T_varchar, types.T_char, types.T_text:
		return checkStrIntersect(v, vec, func(t1, t2 string) bool {
			return strings.Compare(t1, t2) >= 0
		}, func(t1, t2 string) bool {
			return strings.Compare(t1, t2) <= 0
		})
	}
	return false, moerr.NewInternalErrorNoCtx("unsupport type to check intersect")
}

func checkNumberIntersect[T constraints.Integer | constraints.Float | types.Date | types.Datetime | types.Timestamp](v1, v2 *Vector) (bool, error) {
	cols1 := MustTCols[T](v1)
	cols2 := MustTCols[T](v2)
	return checkIntersect(cols1, cols2, func(i1, i2 T) bool {
		return i1 >= i2
	}, func(i1, i2 T) bool {
		return i1 <= i2
	})
}

func checkStrIntersect(v1, v2 *Vector, gtFun compFn[string], ltFun compFn[string]) (bool, error) {
	cols1 := MustStrCols(v1)
	cols2 := MustStrCols(v2)
	return checkIntersect(cols1, cols2, gtFun, ltFun)
}

func checkGeneralIntersect[T compT](v1, v2 *Vector, gtFun compFn[T], ltFun compFn[T]) (bool, error) {
	cols1 := MustTCols[T](v1)
	cols2 := MustTCols[T](v2)
	return checkIntersect(cols1, cols2, gtFun, ltFun)
}

func checkIntersect[T compT](cols1, cols2 []T, gtFun compFn[T], ltFun compFn[T]) (bool, error) {
	// get v1's min/max
	colLength := len(cols1)
	min := cols1[0]
	max := cols1[0]
	for i := 1; i < colLength; i++ {
		// cols1[i] <= min
		if ltFun(cols1[i], min) {
			min = cols1[i]
		} else if gtFun(cols1[i], max) {
			// cols1[i] >= max
			max = cols1[i]
		}
	}

	// check v2 if some item >= min && <= max
	for i := 0; i < len(cols2); i++ {
		// cols2[i] >= min && cols2[i] <= max
		if gtFun(cols2[i], min) && ltFun(cols2[i], max) {
			return true, nil
		}
	}
	return false, nil
}

// CompareAndCheckAnyResultIsTrue  we use this method for eval expr by zonemap
// funName must be ">,<,>=,<="
func (v *Vector) CompareAndCheckAnyResultIsTrue(ctx context.Context, vec *Vector, funName string) (bool, error) {
	if v.typ.Oid != vec.typ.Oid {
		return false, moerr.NewInternalErrorNoCtx("can not compare two vector because their type is not match")
	}
	if v.Length() != vec.Length() {
		return false, moerr.NewInternalErrorNoCtx("can not compare two vector because their length is not match")
	}
	if v.Length() == 0 {
		return false, moerr.NewInternalErrorNoCtx("can not compare two vector because their length is zero")
	}

	switch funName {
	case ">", "<", ">=", "<=":
	default:
		return false, moerr.NewInternalErrorNoCtx("unsupport compare function")
	}

	switch v.typ.Oid {
	case types.T_int8:
		return compareNumber[int8](ctx, v, vec, funName)
	case types.T_int16:
		return compareNumber[int16](ctx, v, vec, funName)
	case types.T_int32:
		return compareNumber[int32](ctx, v, vec, funName)
	case types.T_int64:
		return compareNumber[int64](ctx, v, vec, funName)
	case types.T_uint8:
		return compareNumber[uint8](ctx, v, vec, funName)
	case types.T_uint16:
		return compareNumber[uint16](ctx, v, vec, funName)
	case types.T_uint32:
		return compareNumber[uint32](ctx, v, vec, funName)
	case types.T_uint64:
		return compareNumber[uint64](ctx, v, vec, funName)
	case types.T_float32:
		return compareNumber[float32](ctx, v, vec, funName)
	case types.T_float64:
		return compareNumber[float64](ctx, v, vec, funName)
	case types.T_date:
		return compareNumber[types.Date](ctx, v, vec, funName)
	case types.T_time:
		return compareNumber[types.Time](ctx, v, vec, funName)
	case types.T_datetime:
		return compareNumber[types.Datetime](ctx, v, vec, funName)
	case types.T_timestamp:
		return compareNumber[types.Timestamp](ctx, v, vec, funName)
	case types.T_decimal64:
		switch funName {
		case ">":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal64) bool {
				return t1.Gt(t2)
			}), nil
		case "<":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal64) bool {
				return t1.Lt(t2)
			}), nil
		case ">=":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal64) bool {
				return t1.Ge(t2)
			}), nil
		case "<=":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal64) bool {
				return t1.Le(t2)
			}), nil
		}
	case types.T_decimal128:
		switch funName {
		case ">":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal128) bool {
				return t1.Gt(t2)
			}), nil
		case "<":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal128) bool {
				return t1.Lt(t2)
			}), nil
		case ">=":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal128) bool {
				return t1.Ge(t2)
			}), nil
		case "<=":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal128) bool {
				return t1.Le(t2)
			}), nil
		}
	case types.T_uuid:
		switch funName {
		case ">":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Uuid) bool {
				return t1.Gt(t2)
			}), nil
		case "<":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Uuid) bool {
				return t1.Lt(t2)
			}), nil
		case ">=":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Uuid) bool {
				return t1.Ge(t2)
			}), nil
		case "<=":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Uuid) bool {
				return t1.Le(t2)
			}), nil
		}
	case types.T_varchar, types.T_char:
		switch funName {
		case ">":
			return runStrCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 string) bool {
				return strings.Compare(t1, t2) == 1
			}), nil
		case "<":
			return runStrCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 string) bool {
				return strings.Compare(t1, t2) == -1
			}), nil
		case ">=":
			return runStrCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 string) bool {
				return strings.Compare(t1, t2) >= 0
			}), nil
		case "<=":
			return runStrCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 string) bool {
				return strings.Compare(t1, t2) <= 0
			}), nil
		}
	default:
		return false, moerr.NewInternalErrorNoCtx("unsupport compare type")
	}

	return false, moerr.NewInternalErrorNoCtx("unsupport compare function")
}

type compT interface {
	constraints.Integer | constraints.Float | types.Decimal64 | types.Decimal128 |
		types.Date | types.Time | types.Datetime | types.Timestamp | types.Uuid | string
}

type compFn[T compT] func(T, T) bool
type numberType interface {
	constraints.Integer | constraints.Float | types.Date | types.Time | types.Datetime | types.Timestamp
}

func compareNumber[T numberType](ctx context.Context, v1, v2 *Vector, fnName string) (bool, error) {
	switch fnName {
	case ">":
		return runCompareCheckAnyResultIsTrue(v1, v2, func(t1, t2 T) bool {
			return t1 > t2
		}), nil
	case "<":
		return runCompareCheckAnyResultIsTrue(v1, v2, func(t1, t2 T) bool {
			return t1 < t2
		}), nil
	case ">=":
		return runCompareCheckAnyResultIsTrue(v1, v2, func(t1, t2 T) bool {
			return t1 >= t2
		}), nil
	case "<=":
		return runCompareCheckAnyResultIsTrue(v1, v2, func(t1, t2 T) bool {
			return t1 <= t2
		}), nil
	default:
		return false, moerr.NewInternalErrorNoCtx("unsupport compare function")
	}
}

func runCompareCheckAnyResultIsTrue[T compT](vec1, vec2 *Vector, fn compFn[T]) bool {
	// column_a operator column_b  -> return true
	// that means we don't known the return, just readBlock
	if vec1.IsConstNull() || vec2.IsConstNull() {
		return true
	}
	if nulls.Any(vec1.nsp) || nulls.Any(vec2.nsp) {
		return true
	}
	cols1 := MustTCols[T](vec1)
	cols2 := MustTCols[T](vec2)
	return compareCheckAnyResultIsTrue(cols1, cols2, fn)
}

func runStrCompareCheckAnyResultIsTrue(vec1, vec2 *Vector, fn compFn[string]) bool {
	// column_a operator column_b  -> return true
	// that means we don't known the return, just readBlock
	if vec1.IsConstNull() || vec2.IsConstNull() {
		return true
	}
	if nulls.Any(vec1.nsp) || nulls.Any(vec2.nsp) {
		return true
	}

	cols1 := MustStrCols(vec1)
	cols2 := MustStrCols(vec2)
	return compareCheckAnyResultIsTrue(cols1, cols2, fn)
}

func compareCheckAnyResultIsTrue[T compT](cols1, cols2 []T, fn compFn[T]) bool {
	for i := 0; i < len(cols1); i++ {
		for j := 0; j < len(cols2); j++ {
			if fn(cols1[i], cols2[j]) {
				return true
			}
		}
	}
	return false
}
