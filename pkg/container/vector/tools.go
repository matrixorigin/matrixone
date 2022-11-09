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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"golang.org/x/exp/constraints"
)

// CheckInsertVector  vector.data will not be nil when insert rows
func CheckInsertVector(v *Vector, m *mpool.MPool) *Vector {
	if v.data == nil && v.Typ.Oid != types.T_any && v.Length() > 0 {
		newVec := New(v.Typ)
		newVec.isConst = v.isConst
		val := GetInitConstVal(v.Typ)
		for i := 0; i < v.Length(); i++ {
			newVec.Append(val, true, m)
		}
		newVec.length = v.length
		return newVec
	}
	return v
}

func MustTCols[T types.FixedSizeT](v *Vector) []T {
	// XXX hack.   Sometimes we generate an t_any, for untyped const null.
	// This should be handled more carefully and gracefully.
	if v.GetType().Oid == types.T_any {
		return nil
	}

	if t, ok := v.Col.([]T); ok {
		return t
	}
	panic("unexpected parameter types were received")
}

func MustBytesCols(v *Vector) [][]byte {
	varcol := MustTCols[types.Varlena](v)
	ret := make([][]byte, len(varcol))
	for i := range varcol {
		ret[i] = (&varcol[i]).GetByteSlice(v.area)
	}
	return ret
}

func MustStrCols(v *Vector) []string {
	varcol := MustTCols[types.Varlena](v)
	ret := make([]string, len(varcol))
	for i := range varcol {
		ret[i] = (&varcol[i]).GetString(v.area)
	}
	return ret
}

func MustVarlenaRawData(v *Vector) (data []types.Varlena, area []byte) {
	data = MustTCols[types.Varlena](v)
	area = v.area
	return
}

func BuildVarlenaVector(typ types.Type, data []types.Varlena, area []byte) (vec *Vector, err error) {
	vec = NewOriginal(typ)
	vec.data = types.EncodeSlice(data)
	vec.area = area
	vec.colFromData()
	return
}

func VectorToProtoVector(vec *Vector) (*api.Vector, error) {
	nsp, err := vec.Nsp.Show()
	if err != nil {
		return nil, err
	}
	return &api.Vector{
		Nsp:      nsp,
		Nullable: true,
		Area:     vec.area,
		IsConst:  vec.isConst,
		Len:      uint32(vec.length),
		Type:     TypeToProtoType(vec.Typ),
		Data:     vec.encodeColToByteSlice(),
	}, nil
}

func ProtoVectorToVector(vec *api.Vector) (*Vector, error) {
	rvec := &Vector{
		original: true,
		data:     vec.Data,
		area:     vec.Area,
		isConst:  vec.IsConst,
		length:   int(vec.Len),
		Typ:      ProtoTypeToType(vec.Type),
	}
	rvec.Nsp = &nulls.Nulls{}
	if err := rvec.Nsp.Read(vec.Nsp); err != nil {
		return nil, err
	}
	rvec.colFromData()
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

func (v *Vector) colFromData() {
	if v.data == nil {
		v.Col = nil
	}

	if v.Typ.Oid == types.T_tuple || v.Typ.Oid == types.T_any {
		// No op
	} else if v.GetType().IsVarlen() {
		v.Col = DecodeFixedCol[types.Varlena](v, types.VarlenaSize)
	} else {
		// The followng switch attach the correct type to v.Col
		// even though v.Col is only an interface.
		tlen := v.GetType().TypeSize()
		switch v.Typ.Oid {
		case types.T_bool:
			v.Col = DecodeFixedCol[bool](v, tlen)
		case types.T_int8:
			v.Col = DecodeFixedCol[int8](v, tlen)
		case types.T_int16:
			v.Col = DecodeFixedCol[int16](v, tlen)
		case types.T_int32:
			v.Col = DecodeFixedCol[int32](v, tlen)
		case types.T_int64:
			v.Col = DecodeFixedCol[int64](v, tlen)
		case types.T_uint8:
			v.Col = DecodeFixedCol[uint8](v, tlen)
		case types.T_uint16:
			v.Col = DecodeFixedCol[uint16](v, tlen)
		case types.T_uint32:
			v.Col = DecodeFixedCol[uint32](v, tlen)
		case types.T_uint64:
			v.Col = DecodeFixedCol[uint64](v, tlen)
		case types.T_float32:
			v.Col = DecodeFixedCol[float32](v, tlen)
		case types.T_float64:
			v.Col = DecodeFixedCol[float64](v, tlen)
		case types.T_decimal64:
			v.Col = DecodeFixedCol[types.Decimal64](v, tlen)
		case types.T_decimal128:
			v.Col = DecodeFixedCol[types.Decimal128](v, tlen)
		case types.T_uuid:
			v.Col = DecodeFixedCol[types.Uuid](v, tlen)
		case types.T_date:
			v.Col = DecodeFixedCol[types.Date](v, tlen)
		case types.T_time:
			v.Col = DecodeFixedCol[types.Time](v, tlen)
		case types.T_datetime:
			v.Col = DecodeFixedCol[types.Datetime](v, tlen)
		case types.T_timestamp:
			v.Col = DecodeFixedCol[types.Timestamp](v, tlen)
		case types.T_TS:
			v.Col = DecodeFixedCol[types.TS](v, tlen)
		case types.T_Rowid:
			v.Col = DecodeFixedCol[types.Rowid](v, tlen)
		default:
			panic("unknown type")
		}
	}
}

func (v *Vector) setupColFromData(start, end int) {
	if v.Typ.Oid == types.T_tuple {
		vec := v.Col.([][]interface{})
		v.Col = vec[start:end]
	} else if v.GetType().IsVarlen() {
		v.Col = DecodeFixedCol[types.Varlena](v, types.VarlenaSize)[start:end]
	} else {
		// The followng switch attach the correct type to v.Col
		// even though v.Col is only an interface.
		tlen := v.GetType().TypeSize()
		switch v.Typ.Oid {
		case types.T_bool:
			v.Col = DecodeFixedCol[bool](v, tlen)[start:end]
		case types.T_int8:
			v.Col = DecodeFixedCol[int8](v, tlen)[start:end]
		case types.T_int16:
			v.Col = DecodeFixedCol[int16](v, tlen)[start:end]
		case types.T_int32:
			v.Col = DecodeFixedCol[int32](v, tlen)[start:end]
		case types.T_int64:
			v.Col = DecodeFixedCol[int64](v, tlen)[start:end]
		case types.T_uint8:
			v.Col = DecodeFixedCol[uint8](v, tlen)[start:end]
		case types.T_uint16:
			v.Col = DecodeFixedCol[uint16](v, tlen)[start:end]
		case types.T_uint32:
			v.Col = DecodeFixedCol[uint32](v, tlen)[start:end]
		case types.T_uint64:
			v.Col = DecodeFixedCol[uint64](v, tlen)[start:end]
		case types.T_float32:
			v.Col = DecodeFixedCol[float32](v, tlen)[start:end]
		case types.T_float64:
			v.Col = DecodeFixedCol[float64](v, tlen)[start:end]
		case types.T_decimal64:
			v.Col = DecodeFixedCol[types.Decimal64](v, tlen)[start:end]
		case types.T_decimal128:
			v.Col = DecodeFixedCol[types.Decimal128](v, tlen)[start:end]
		case types.T_uuid:
			v.Col = DecodeFixedCol[types.Uuid](v, tlen)[start:end]
		case types.T_date:
			v.Col = DecodeFixedCol[types.Date](v, tlen)[start:end]
		case types.T_time:
			v.Col = DecodeFixedCol[types.Time](v, tlen)[start:end]
		case types.T_datetime:
			v.Col = DecodeFixedCol[types.Datetime](v, tlen)[start:end]
		case types.T_timestamp:
			v.Col = DecodeFixedCol[types.Timestamp](v, tlen)[start:end]
		case types.T_TS:
			v.Col = DecodeFixedCol[types.TS](v, tlen)[start:end]
		case types.T_Rowid:
			v.Col = DecodeFixedCol[types.Rowid](v, tlen)[start:end]
		default:
			panic("unknown type")
		}
	}
}

func (v *Vector) encodeColToByteSlice() []byte {
	if v.Col == nil {
		return nil
	}
	switch v.GetType().Oid {
	case types.T_bool:
		return types.EncodeSlice(v.Col.([]bool))
	case types.T_int8:
		return types.EncodeSlice(v.Col.([]int8))
	case types.T_int16:
		return types.EncodeSlice(v.Col.([]int16))
	case types.T_int32:
		return types.EncodeSlice(v.Col.([]int32))
	case types.T_int64:
		return types.EncodeSlice(v.Col.([]int64))
	case types.T_uint8:
		return types.EncodeSlice(v.Col.([]uint8))
	case types.T_uint16:
		return types.EncodeSlice(v.Col.([]uint16))
	case types.T_uint32:
		return types.EncodeSlice(v.Col.([]uint32))
	case types.T_uint64:
		return types.EncodeSlice(v.Col.([]uint64))
	case types.T_float32:
		return types.EncodeSlice(v.Col.([]float32))
	case types.T_float64:
		return types.EncodeSlice(v.Col.([]float64))
	case types.T_decimal64:
		return types.EncodeSlice(v.Col.([]types.Decimal64))
	case types.T_decimal128:
		return types.EncodeSlice(v.Col.([]types.Decimal128))
	case types.T_uuid:
		return types.EncodeSlice(v.Col.([]types.Uuid))
	case types.T_date:
		return types.EncodeSlice(v.Col.([]types.Date))
	case types.T_time:
		return types.EncodeSlice(v.Col.([]types.Time))
	case types.T_datetime:
		return types.EncodeSlice(v.Col.([]types.Datetime))
	case types.T_timestamp:
		return types.EncodeSlice(v.Col.([]types.Timestamp))
	case types.T_TS:
		return types.EncodeSlice(v.Col.([]types.TS))
	case types.T_Rowid:
		return types.EncodeSlice(v.Col.([]types.Rowid))
	case types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text:
		return types.EncodeSlice(v.Col.([]types.Varlena))
	case types.T_tuple:
		bs, _ := types.Encode(v.Col.([][]interface{}))
		return bs
	default:
		panic("unknow type when encode vector column")
	}
}

// XXX extend will extend the vector's Data to accommodate rows more entry.
func (v *Vector) extend(rows int, m *mpool.MPool) error {
	origSz := len(v.data)
	growSz := rows * v.GetType().TypeSize()
	tgtSz := origSz + growSz
	if tgtSz <= cap(v.data) {
		// XXX v.data can hold data, just grow.
		// XXX do not use Grow, because this case it will still malloc and copy
		v.data = v.data[:tgtSz]
	} else if v.data == nil {
		// XXX mheap Relloc is broken, cannot handle nil, so we Alloc here.
		// XXX The interface on size, int/int64 u, FUBAR.
		data, err := m.Alloc(tgtSz)
		if err != nil {
			return err
		}
		v.data = data[:tgtSz]
	} else {
		data, err := m.Grow(v.data, tgtSz)
		if err != nil {
			return err
		}
		v.data = data[:tgtSz]
	}

	newRows := int(tgtSz / v.GetType().TypeSize())
	// Setup v.Col
	v.setupColFromData(0, newRows)
	return nil
}

// CompareAndCheckIntersect  we use this method for eval expr by zonemap
func (v *Vector) CompareAndCheckIntersect(vec *Vector) (bool, error) {
	switch v.Typ.Oid {
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
	case types.T_varchar, types.T_char:
		return checkStrIntersect(v, vec, func(t1, t2 string) bool {
			return strings.Compare(t1, t2) >= 0
		}, func(t1, t2 string) bool {
			return strings.Compare(t1, t2) <= 0
		})
	}
	return false, moerr.NewInternalError("unsupport type to check intersect")
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
		if gtFun(cols1[i], min) && ltFun(cols1[i], max) {
			return true, nil
		}
	}
	return false, nil
}

// CompareAndCheckAnyResultIsTrue  we use this method for eval expr by zonemap
// funName must be ">,<,>=,<="
func (v *Vector) CompareAndCheckAnyResultIsTrue(vec *Vector, funName string) (bool, error) {
	if v.Typ.Oid != vec.Typ.Oid {
		return false, moerr.NewInternalError("can not compare two vector because their type is not match")
	}
	if v.Length() != vec.Length() {
		return false, moerr.NewInternalError("can not compare two vector because their length is not match")
	}
	if v.Length() == 0 {
		return false, moerr.NewInternalError("can not compare two vector because their length is zero")
	}

	switch funName {
	case ">", "<", ">=", "<=":
	default:
		return false, moerr.NewInternalError("unsupport compare function")
	}

	switch v.Typ.Oid {
	case types.T_int8:
		return compareNumber[int8](v, vec, funName)
	case types.T_int16:
		return compareNumber[int16](v, vec, funName)
	case types.T_int32:
		return compareNumber[int32](v, vec, funName)
	case types.T_int64:
		return compareNumber[int64](v, vec, funName)
	case types.T_uint8:
		return compareNumber[uint8](v, vec, funName)
	case types.T_uint16:
		return compareNumber[uint16](v, vec, funName)
	case types.T_uint32:
		return compareNumber[uint32](v, vec, funName)
	case types.T_uint64:
		return compareNumber[uint64](v, vec, funName)
	case types.T_float32:
		return compareNumber[float32](v, vec, funName)
	case types.T_float64:
		return compareNumber[float64](v, vec, funName)
	case types.T_date:
		return compareNumber[types.Date](v, vec, funName)
	case types.T_time:
		return compareNumber[types.Time](v, vec, funName)
	case types.T_datetime:
		return compareNumber[types.Datetime](v, vec, funName)
	case types.T_timestamp:
		return compareNumber[types.Timestamp](v, vec, funName)
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
		return false, moerr.NewInternalError("unsupport compare type")
	}

	return false, moerr.NewInternalError("unsupport compare function")
}

type compT interface {
	constraints.Integer | constraints.Float | types.Decimal64 | types.Decimal128 |
		types.Date | types.Time | types.Datetime | types.Timestamp | types.Uuid | string
}

type compFn[T compT] func(T, T) bool

func compareNumber[T constraints.Integer | constraints.Float | types.Date | types.Time | types.Datetime | types.Timestamp](v1, v2 *Vector, fnName string) (bool, error) {
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
			return t1 >= t2
		}), nil
	default:
		return false, moerr.NewInternalError("unsupport compare function")
	}
}

func runCompareCheckAnyResultIsTrue[T compT](vec1, vec2 *Vector, fn compFn[T]) bool {
	// column_a operator column_b  -> return true
	// that means we don't known the return, just readBlock
	if vec1.IsScalarNull() || vec2.IsScalarNull() {
		return true
	}
	if nulls.Any(vec1.Nsp) || nulls.Any(vec2.Nsp) {
		return true
	}
	cols1 := MustTCols[T](vec1)
	cols2 := MustTCols[T](vec2)
	return compareCheckAnyResultIsTrue(cols1, cols2, fn)
}

func runStrCompareCheckAnyResultIsTrue(vec1, vec2 *Vector, fn compFn[string]) bool {
	// column_a operator column_b  -> return true
	// that means we don't known the return, just readBlock
	if vec1.IsScalarNull() || vec2.IsScalarNull() {
		return true
	}
	if nulls.Any(vec1.Nsp) || nulls.Any(vec2.Nsp) {
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
