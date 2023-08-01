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
	"bytes"
	"context"
	"fmt"
	"sort"
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

func MustFixedCol[T any](v *Vector) []T {
	// XXX hack.   Sometimes we generate an t_any, for untyped const null.
	// This should be handled more carefully and gracefully.
	if v.GetType().Oid == types.T_any || len(v.data) == 0 {
		return nil
	}
	if v.class == CONSTANT {
		return v.col.([]T)[:1]
	}
	return v.col.([]T)[:v.length]
}

func MustBytesCol(v *Vector) [][]byte {
	if v.GetType().Oid == types.T_any || len(v.data) == 0 {
		return nil
	}
	varcol := MustFixedCol[types.Varlena](v)
	if v.class == CONSTANT {
		return [][]byte{(&varcol[0]).GetByteSlice(v.area)}
	} else {
		ret := make([][]byte, v.length)
		for i := range varcol {
			ret[i] = (&varcol[i]).GetByteSlice(v.area)
		}
		return ret
	}
}

func MustStrCol(v *Vector) []string {
	if v.GetType().Oid == types.T_any || len(v.data) == 0 {
		return nil
	}
	varcol := MustFixedCol[types.Varlena](v)
	if v.class == CONSTANT {
		return []string{(&varcol[0]).GetString(v.area)}
	} else {
		ret := make([]string, v.length)
		for i := range varcol {
			ret[i] = (&varcol[i]).GetString(v.area)
		}
		return ret
	}
}

func MustArrayCol[T types.BuiltinNumber](v *Vector) [][]T {
	if v.GetType().Oid == types.T_any || len(v.data) == 0 {
		return nil
	}
	varcol := MustFixedCol[types.Varlena](v)
	if v.class == CONSTANT {
		return [][]T{types.GetArray[T](&varcol[0], v.area)}
	} else {
		ret := make([][]T, v.length)
		for i := range varcol {
			ret[i] = types.GetArray[T](&varcol[i], v.area)
		}
		return ret
	}
}

// ExpandFixedCol decode data and return decoded []T.
// For const/scalar vector we expand and return newly allocated slice.
func ExpandFixedCol[T any](v *Vector) []T {
	if v.IsConst() {
		vs := make([]T, v.Length())
		if len(v.data) > 0 {
			cols := v.col.([]T)
			for i := range vs {
				vs[i] = cols[0]
			}
		}
		return vs
	}
	return MustFixedCol[T](v)
}

func ExpandStrCol(v *Vector) []string {
	if v.IsConst() {
		vs := make([]string, v.Length())
		if len(v.data) > 0 {
			cols := v.col.([]types.Varlena)
			ss := cols[0].GetString(v.area)
			for i := range vs {
				vs[i] = ss
			}
		}
		return vs
	}
	return MustStrCol(v)
}

func ExpandArrayCol[T types.BuiltinNumber](v *Vector) [][]T {
	if v.IsConst() {
		vs := make([][]T, v.Length())
		if len(v.data) > 0 {
			cols := v.col.([]types.Varlena)
			ss := types.GetArray[T](&cols[0], v.area)
			for i := range vs {
				vs[i] = ss
			}
		}
		return vs
	}
	return MustArrayCol[T](v)
}

func ExpandBytesCol(v *Vector) [][]byte {
	if v.IsConst() {
		vs := make([][]byte, v.Length())
		if len(v.data) > 0 {
			cols := v.col.([]types.Varlena)
			ss := cols[0].GetByteSlice(v.area)
			for i := range vs {
				vs[i] = ss
			}
		}
		return vs
	}
	return MustBytesCol(v)
}

func MustVarlenaToInt64Slice(v *Vector) [][3]int64 {
	data := MustFixedCol[types.Varlena](v)
	pointer := (*[3]int64)(data[0].UnsafePtr())
	return unsafe.Slice(pointer, len(data))
}

func MustVarlenaRawData(v *Vector) (data []types.Varlena, area []byte) {
	data = MustFixedCol[types.Varlena](v)
	area = v.area
	return
}

// XXX extend will extend the vector's Data to accommodate rows more entry.
func extend(v *Vector, rows int, m *mpool.MPool) error {
	if tgtCap := v.length + rows; tgtCap > v.capacity {
		sz := v.typ.TypeSize()
		ndata, err := m.Grow(v.data, tgtCap*sz)
		if err != nil {
			return err
		}
		v.data = ndata[:cap(ndata)]
		v.setupColFromData()
	}
	return nil
}

func (v *Vector) setupColFromData() {
	if v.GetType().IsVarlen() {
		v.col = DecodeFixedCol[types.Varlena](v)
	} else {
		// The followng switch attach the correct type to v.col
		// even though v.col is only an interface.
		switch v.typ.Oid {
		case types.T_bool:
			v.col = DecodeFixedCol[bool](v)
		case types.T_int8:
			v.col = DecodeFixedCol[int8](v)
		case types.T_int16:
			v.col = DecodeFixedCol[int16](v)
		case types.T_int32:
			v.col = DecodeFixedCol[int32](v)
		case types.T_int64:
			v.col = DecodeFixedCol[int64](v)
		case types.T_uint8:
			v.col = DecodeFixedCol[uint8](v)
		case types.T_uint16:
			v.col = DecodeFixedCol[uint16](v)
		case types.T_uint32:
			v.col = DecodeFixedCol[uint32](v)
		case types.T_uint64:
			v.col = DecodeFixedCol[uint64](v)
		case types.T_float32:
			v.col = DecodeFixedCol[float32](v)
		case types.T_float64:
			v.col = DecodeFixedCol[float64](v)
		case types.T_decimal64:
			v.col = DecodeFixedCol[types.Decimal64](v)
		case types.T_decimal128:
			v.col = DecodeFixedCol[types.Decimal128](v)
		case types.T_uuid:
			v.col = DecodeFixedCol[types.Uuid](v)
		case types.T_date:
			v.col = DecodeFixedCol[types.Date](v)
		case types.T_time:
			v.col = DecodeFixedCol[types.Time](v)
		case types.T_datetime:
			v.col = DecodeFixedCol[types.Datetime](v)
		case types.T_timestamp:
			v.col = DecodeFixedCol[types.Timestamp](v)
		case types.T_TS:
			v.col = DecodeFixedCol[types.TS](v)
		case types.T_Rowid:
			v.col = DecodeFixedCol[types.Rowid](v)
		case types.T_Blockid:
			v.col = DecodeFixedCol[types.Blockid](v)
		default:
			panic(fmt.Sprintf("unknown type %s", v.typ.Oid))
		}
	}
	tlen := v.GetType().TypeSize()
	v.capacity = cap(v.data) / tlen
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
		Data:     vec.data[:vec.length*sz],
	}, nil
}

func ProtoVectorToVector(vec *api.Vector) (*Vector, error) {
	rvec := &Vector{
		area:         vec.Area,
		length:       int(vec.Len),
		typ:          ProtoTypeToType(vec.Type),
		cantFreeData: true,
		cantFreeArea: true,
	}
	if vec.IsConst {
		rvec.class = CONSTANT
	} else {
		rvec.class = FLAT
	}
	if err := rvec.nsp.Read(vec.Nsp); err != nil {
		return nil, err
	}
	if rvec.IsConst() && rvec.nsp.Contains(0) {
		return rvec, nil
	}
	rvec.data = vec.Data
	rvec.setupColFromData()
	return rvec, nil
}

func TypeToProtoType(typ types.Type) *plan.Type {
	return &plan.Type{
		Id:    int32(typ.Oid),
		Width: typ.Width,
		Scale: typ.Scale,
	}
}

func ProtoTypeToType(typ *plan.Type) types.Type {
	return types.New(types.T(typ.Id), typ.Width, typ.Scale)
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
			return (t1.Compare(t2) >= 0)
		}, func(t1, t2 types.Decimal64) bool {
			return (t1.Compare(t2) <= 0)
		})
	case types.T_decimal128:
		return checkGeneralIntersect(v, vec, func(t1, t2 types.Decimal128) bool {
			return (t1.Compare(t2) >= 0)
		}, func(t1, t2 types.Decimal128) bool {
			return (t1.Compare(t2) <= 0)
		})
	case types.T_uuid:
		return checkGeneralIntersect(v, vec, func(t1, t2 types.Uuid) bool {
			return t1.Ge(t2)
		}, func(t1, t2 types.Uuid) bool {
			return t1.Le(t2)
		})
	case types.T_varchar, types.T_binary, types.T_varbinary, types.T_char, types.T_text:
		return checkStrIntersect(v, vec, func(t1, t2 string) bool {
			return strings.Compare(t1, t2) >= 0
		}, func(t1, t2 string) bool {
			return strings.Compare(t1, t2) <= 0
		})
	case types.T_array_float32:
		return checkArrayIntersect[float32](v, vec, func(t1, t2 []float32) bool {
			return types.CompareArray[float32](t1, t2) >= 0
		}, func(t1, t2 []float32) bool {
			return types.CompareArray[float32](t1, t2) <= 0
		})
	case types.T_array_float64:
		return checkArrayIntersect[float64](v, vec, func(t1, t2 []float64) bool {
			return types.CompareArray[float64](t1, t2) >= 0
		}, func(t1, t2 []float64) bool {
			return types.CompareArray[float64](t1, t2) <= 0
		})
	}
	//TODO: would T_array be used in Zonemap?
	return false, moerr.NewInternalErrorNoCtx("unsupport type to check intersect")
}

func checkNumberIntersect[T constraints.Integer | constraints.Float | types.Date | types.Datetime | types.Timestamp](v1, v2 *Vector) (bool, error) {
	cols1 := MustFixedCol[T](v1)
	cols2 := MustFixedCol[T](v2)
	return checkIntersect(cols1, cols2, func(i1, i2 T) bool {
		return i1 >= i2
	}, func(i1, i2 T) bool {
		return i1 <= i2
	})
}

func checkStrIntersect(v1, v2 *Vector, gtFun compFn[string], ltFun compFn[string]) (bool, error) {
	cols1 := MustStrCol(v1)
	cols2 := MustStrCol(v2)
	return checkIntersect(cols1, cols2, gtFun, ltFun)
}

func checkArrayIntersect[T types.BuiltinNumber](v1, v2 *Vector, gtFun compFn[[]T], ltFun compFn[[]T]) (bool, error) {
	cols1 := MustArrayCol[T](v1)
	cols2 := MustArrayCol[T](v2)
	return checkIntersect(cols1, cols2, gtFun, ltFun)
}

func checkGeneralIntersect[T any](v1, v2 *Vector, gtFun compFn[T], ltFun compFn[T]) (bool, error) {
	cols1 := MustFixedCol[T](v1)
	cols2 := MustFixedCol[T](v2)
	return checkIntersect(cols1, cols2, gtFun, ltFun)
}

func checkIntersect[T any](cols1, cols2 []T, gtFun compFn[T], ltFun compFn[T]) (bool, error) {
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
				return t1.Compare(t2) > 0
			}), nil
		case "<":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal64) bool {
				return t1.Compare(t2) < 0
			}), nil
		case ">=":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal64) bool {
				return t1.Compare(t2) >= 0
			}), nil
		case "<=":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal64) bool {
				return t1.Compare(t2) <= 0
			}), nil
		}
	case types.T_decimal128:
		switch funName {
		case ">":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal128) bool {
				return t1.Compare(t2) > 0
			}), nil
		case "<":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal128) bool {
				return t1.Compare(t2) < 0
			}), nil
		case ">=":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal128) bool {
				return t1.Compare(t2) >= 0
			}), nil
		case "<=":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal128) bool {
				return t1.Compare(t2) <= 0
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
	case types.T_varchar, types.T_binary, types.T_varbinary, types.T_char:
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
	case types.T_array_float32:
		return compareArray[float32](ctx, v, vec, funName)
	case types.T_array_float64:
		return compareArray[float64](ctx, v, vec, funName)
	default:
		return false, moerr.NewInternalErrorNoCtx("unsupport compare type")
	}
	return false, moerr.NewInternalErrorNoCtx("unsupport compare function")
}

type compFn[T any] func(T, T) bool

func compareNumber[T types.OrderedT](ctx context.Context, v1, v2 *Vector, fnName string) (bool, error) {
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

func compareArray[T types.BuiltinNumber](ctx context.Context, v1, v2 *Vector, fnName string) (bool, error) {
	switch fnName {
	case ">":
		return runArrayCompareCheckAnyResultIsTrue(v1, v2, func(t1, t2 []T) bool {
			return types.CompareArray[T](t1, t2) == 1
		}), nil
	case "<":
		return runArrayCompareCheckAnyResultIsTrue(v1, v2, func(t1, t2 []T) bool {
			return types.CompareArray[T](t1, t2) == -1
		}), nil
	case ">=":
		return runArrayCompareCheckAnyResultIsTrue(v1, v2, func(t1, t2 []T) bool {
			return types.CompareArray[T](t1, t2) >= 0
		}), nil
	case "<=":
		return runArrayCompareCheckAnyResultIsTrue(v1, v2, func(t1, t2 []T) bool {
			return types.CompareArray[T](t1, t2) <= 0
		}), nil
	default:
		return false, moerr.NewInternalErrorNoCtx("unsupport compare function")
	}
}

func runCompareCheckAnyResultIsTrue[T any](vec1, vec2 *Vector, fn compFn[T]) bool {
	// column_a operator column_b  -> return true
	// that means we don't known the return, just readBlock
	if vec1.IsConstNull() || vec2.IsConstNull() {
		return true
	}
	if nulls.Any(vec1.GetNulls()) || nulls.Any(vec2.GetNulls()) {
		return true
	}
	cols1 := MustFixedCol[T](vec1)
	cols2 := MustFixedCol[T](vec2)
	return compareCheckAnyResultIsTrue(cols1, cols2, fn)
}

func runStrCompareCheckAnyResultIsTrue(vec1, vec2 *Vector, fn compFn[string]) bool {
	// column_a operator column_b  -> return true
	// that means we don't known the return, just readBlock
	if vec1.IsConstNull() || vec2.IsConstNull() {
		return true
	}
	if nulls.Any(vec1.GetNulls()) || nulls.Any(vec2.GetNulls()) {
		return true
	}

	cols1 := MustStrCol(vec1)
	cols2 := MustStrCol(vec2)
	return compareCheckAnyResultIsTrue(cols1, cols2, fn)
}

func runArrayCompareCheckAnyResultIsTrue[T types.BuiltinNumber](vec1, vec2 *Vector, fn compFn[[]T]) bool {
	// column_a operator column_b  -> return true
	// that means we don't known the return, just readBlock
	if vec1.IsConstNull() || vec2.IsConstNull() {
		return true
	}
	if nulls.Any(vec1.GetNulls()) || nulls.Any(vec2.GetNulls()) {
		return true
	}

	cols1 := MustArrayCol[T](vec1)
	cols2 := MustArrayCol[T](vec2)
	return compareCheckAnyResultIsTrue(cols1, cols2, fn)
}

func compareCheckAnyResultIsTrue[T any](cols1, cols2 []T, fn compFn[T]) bool {
	for i := 0; i < len(cols1); i++ {
		for j := 0; j < len(cols2); j++ {
			if fn(cols1[i], cols2[j]) {
				return true
			}
		}
	}
	return false
}

func appendBytesToFixSized[T types.FixedSizeT](vec *Vector) func([]byte, bool, *mpool.MPool) error {
	return func(buf []byte, isNull bool, mp *mpool.MPool) (err error) {
		v := types.DecodeFixed[T](buf)
		return AppendFixed(vec, v, isNull, mp)
	}
}

func MakeAppendBytesFunc(vec *Vector) func([]byte, bool, *mpool.MPool) error {
	t := vec.GetType()
	if t.IsVarlen() {
		return func(v []byte, isNull bool, mp *mpool.MPool) (err error) {
			return AppendBytes(vec, v, isNull, mp)
		}
	}
	switch t.Oid {
	case types.T_bool:
		return appendBytesToFixSized[bool](vec)
	case types.T_int8:
		return appendBytesToFixSized[int8](vec)
	case types.T_int16:
		return appendBytesToFixSized[int16](vec)
	case types.T_int32:
		return appendBytesToFixSized[int32](vec)
	case types.T_int64:
		return appendBytesToFixSized[int64](vec)
	case types.T_uint8:
		return appendBytesToFixSized[uint8](vec)
	case types.T_uint16:
		return appendBytesToFixSized[uint16](vec)
	case types.T_uint32:
		return appendBytesToFixSized[uint32](vec)
	case types.T_uint64:
		return appendBytesToFixSized[uint64](vec)
	case types.T_float32:
		return appendBytesToFixSized[float32](vec)
	case types.T_float64:
		return appendBytesToFixSized[float64](vec)
	case types.T_date:
		return appendBytesToFixSized[types.Date](vec)
	case types.T_datetime:
		return appendBytesToFixSized[types.Datetime](vec)
	case types.T_time:
		return appendBytesToFixSized[types.Time](vec)
	case types.T_timestamp:
		return appendBytesToFixSized[types.Timestamp](vec)
	case types.T_decimal64:
		return appendBytesToFixSized[types.Decimal64](vec)
	case types.T_decimal128:
		return appendBytesToFixSized[types.Decimal128](vec)
	case types.T_uuid:
		return appendBytesToFixSized[types.Uuid](vec)
	case types.T_TS:
		return appendBytesToFixSized[types.TS](vec)
	case types.T_Rowid:
		return appendBytesToFixSized[types.Rowid](vec)
	case types.T_Blockid:
		return appendBytesToFixSized[types.Blockid](vec)
	}
	panic(fmt.Sprintf("unexpected type: %s", vec.GetType().String()))
}

func OrderedBinarySearchOffsetByValFactory[T types.OrderedT](v T) func(*Vector) int {
	return func(vec *Vector) int {
		rows := MustFixedCol[T](vec)
		offset := sort.Search(vec.Length(), func(idx int) bool {
			return rows[idx] >= v
		})
		if offset < vec.Length() && rows[offset] == v {
			return offset
		}
		return -1
	}
}

func VarlenBinarySearchOffsetByValFactory(val []byte) func(*Vector) int {
	return func(data *Vector) int {
		offset := -1
		start, end := 0, data.Length()-1
		var mid int
		for start <= end {
			mid = (start + end) / 2
			res := bytes.Compare(data.GetBytesAt(mid), val)
			if res > 0 {
				end = mid - 1
			} else if res < 0 {
				start = mid + 1
			} else {
				offset = mid
				break
			}
		}
		return offset
	}
}

func BinarySearchOffsetByValFactory(t types.T, v any) func(*Vector) int {
	if !t.IsFixedLen() {
		val := v.([]byte)
		return VarlenBinarySearchOffsetByValFactory(val)
	}
	switch t {
	case types.T_int8:
		return OrderedBinarySearchOffsetByValFactory[int8](v.(int8))
	case types.T_int16:
		return OrderedBinarySearchOffsetByValFactory[int16](v.(int16))
	case types.T_int32:
		return OrderedBinarySearchOffsetByValFactory[int32](v.(int32))
	case types.T_int64:
		return OrderedBinarySearchOffsetByValFactory[int64](v.(int64))
	case types.T_uint8:
		return OrderedBinarySearchOffsetByValFactory[uint8](v.(uint8))
	case types.T_uint16:
		return OrderedBinarySearchOffsetByValFactory[uint16](v.(uint16))
	case types.T_uint32:
		return OrderedBinarySearchOffsetByValFactory[uint32](v.(uint32))
	case types.T_uint64:
		return OrderedBinarySearchOffsetByValFactory[uint64](v.(uint64))
	case types.T_float32:
		return OrderedBinarySearchOffsetByValFactory[float32](v.(float32))
	case types.T_float64:
		return OrderedBinarySearchOffsetByValFactory[float64](v.(float64))
	case types.T_date:
		return OrderedBinarySearchOffsetByValFactory[types.Date](v.(types.Date))
	case types.T_datetime:
		return OrderedBinarySearchOffsetByValFactory[types.Datetime](v.(types.Datetime))
	case types.T_time:
		return OrderedBinarySearchOffsetByValFactory[types.Time](v.(types.Time))
	case types.T_timestamp:
		return OrderedBinarySearchOffsetByValFactory[types.Timestamp](v.(types.Timestamp))
	case types.T_decimal64:
		return FixSizedBinarySearchOffsetByValFactory[types.Decimal64](v.(types.Decimal64), types.CompareDecimal64)
	case types.T_decimal128:
		return FixSizedBinarySearchOffsetByValFactory[types.Decimal128](v.(types.Decimal128), types.CompareDecimal128)
	case types.T_decimal256:
		return FixSizedBinarySearchOffsetByValFactory[types.Decimal256](v.(types.Decimal256), types.CompareDecimal256)
	case types.T_Rowid:
		return FixSizedBinarySearchOffsetByValFactory[types.Rowid](v.(types.Rowid), types.CompareRowidRowidAligned)
	case types.T_Blockid:
		return FixSizedBinarySearchOffsetByValFactory[types.Blockid](v.(types.Blockid), types.CompareBlockidBlockidAligned)
	case types.T_TS:
		return FixSizedBinarySearchOffsetByValFactory[types.TS](v.(types.TS), types.CompareTSTSAligned)
	case types.T_uuid:
		return FixSizedBinarySearchOffsetByValFactory[types.Uuid](v.(types.Uuid), types.CompareUuid)
	default:
		return nil
	}
}

func FixSizedBinarySearchOffsetByValFactory[T any](v T, comp func(T, T) int64) func(*Vector) int {
	return func(data *Vector) int {
		offset := -1
		vals := MustFixedCol[T](data)
		start, end := 0, len(vals)-1
		var mid int
		for start <= end {
			mid = (start + end) / 2
			res := comp(vals[mid], v)
			if res > 0 {
				end = mid - 1
			} else if res < 0 {
				start = mid + 1
			} else {
				offset = mid
				break
			}
		}
		return offset
	}
}
