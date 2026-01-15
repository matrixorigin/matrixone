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
	"fmt"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func ToFixedColNoTypeCheck[T any](v *Vector, ret *[]T) {
	if v.class == CONSTANT {
		*ret = toSliceOfLengthNoTypeCheck[T](v, 1)
	} else {
		ToSliceNoTypeCheck(v, ret)
		*ret = (*ret)[:v.length]
	}
}

func ToFixedCol[T any](v *Vector, ret *[]T) {
	// XXX hack.   Sometimes we generate an t_any, for untyped const null.
	// This should be handled more carefully and gracefully.
	if v.GetType().Oid == types.T_any || len(v.data) == 0 {
		return
	}
	if v.class == CONSTANT {
		*ret = toSliceOfLengthNoTypeCheck[T](v, 1)
	} else {
		ToSlice(v, ret)
		*ret = (*ret)[:v.length]
	}
}

func MustFixedColNoTypeCheck[T any](v *Vector) (ret []T) {
	ToFixedColNoTypeCheck(v, &ret)
	return
}

func MustFixedColWithTypeCheck[T any](v *Vector) (ret []T) {
	ToFixedCol(v, &ret)
	return
}

// InefficientMustBytesCol
// It should only be used for debugging purposes or in cases where performance is not a critical factor.
// The function performs a potentially slow and memory-intensive operation to extract the byte representation of the column.
// Avoid using this function in scenarios where speed is important.
//
//	vs, area := vector.MustVarlenaRawData(vec)
//	for i := range vs {
//		vs[i].GetByteSlice(area)
//	}
func InefficientMustBytesCol(v *Vector) [][]byte {
	if v.GetType().Oid == types.T_any || len(v.data) == 0 {
		return nil
	}
	varcol := MustFixedColWithTypeCheck[types.Varlena](v)
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

// InefficientMustStrCol
// It should only be used for debugging purposes or in cases where performance is not a critical factor.
// The function performs a potentially slow and memory-intensive operation to extract the byte representation of the column.
// Avoid using this function in scenarios where speed is important.
//
//	vs, area := vector.MustVarlenaRawData(vec)
//	for i := range vs {
//		vs[i].UnsafeGetString(area)
//	}
//
// todo:
// There is a bug here.
// If the vector is reused, that is, the initial value of Varlena is not 0,
// it will cause the UnsafeGetString method to panic. This is because what is stored here is the offset of the last value.
// and InefficientMustBytesCol has a same bug.
func InefficientMustStrCol(v *Vector) []string {
	if v.GetType().Oid == types.T_any || len(v.data) == 0 {
		return nil
	}
	varcol := MustFixedColNoTypeCheck[types.Varlena](v)
	if v.class == CONSTANT {
		return []string{(&varcol[0]).UnsafeGetString(v.area)}
	} else {
		ret := make([]string, v.length)
		for i := range varcol {
			ret[i] = (&varcol[i]).UnsafeGetString(v.area)
		}
		return ret
	}
}

// MustArrayCol  Converts Vector<[]T> to [][]T
func MustArrayCol[T types.RealNumbers](v *Vector) [][]T {
	if v.GetType().Oid == types.T_any || len(v.data) == 0 {
		return nil
	}
	varcol := MustFixedColWithTypeCheck[types.Varlena](v)
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
			var cols []T
			ToSlice(v, &cols)
			for i := range vs {
				vs[i] = cols[0]
			}
		}
		return vs
	}
	return MustFixedColWithTypeCheck[T](v)
}

func ExpandStrCol(v *Vector) []string {
	if v.IsConst() {
		vs := make([]string, v.Length())
		if len(v.data) > 0 {
			var cols []types.Varlena
			ToSliceNoTypeCheck(v, &cols)
			ss := cols[0].UnsafeGetString(v.area)
			for i := range vs {
				vs[i] = ss
			}
		}
		return vs
	}
	return InefficientMustStrCol(v)
}

func ExpandBytesCol(v *Vector) [][]byte {
	if v.IsConst() {
		vs := make([][]byte, v.Length())
		if len(v.data) > 0 {
			var cols []types.Varlena
			ToSliceNoTypeCheck(v, &cols)
			ss := cols[0].GetByteSlice(v.area)
			for i := range vs {
				vs[i] = ss
			}
		}
		return vs
	}
	return InefficientMustBytesCol(v)
}

func MustVarlenaToInt64Slice(v *Vector) [][3]int64 {
	data := MustFixedColNoTypeCheck[types.Varlena](v)
	pointer := (*[3]int64)(data[0].UnsafePtr())
	return unsafe.Slice(pointer, len(data))
}

func MustVarlenaRawData(v *Vector) (data []types.Varlena, area []byte) {
	data = MustFixedColNoTypeCheck[types.Varlena](v)
	area = v.area
	return
}

// XXX extend will extend the vector's Data to accommodate rows more entry.
func extend(v *Vector, rows int, m *mpool.MPool) error {
	if rows <= 0 {
		// we will at least extent by 1.
		// This is a pure hack to
		rows = 1
	}

	tgtLen := v.length + rows
	tgtDataCap := tgtLen * v.typ.TypeSize()
	if tgtDataCap > cap(v.data) {
		ndata, err := m.Grow(v.data, tgtDataCap, v.offHeap)
		if err != nil {
			return err
		}
		v.data = ndata
	}
	v.data = v.data[:cap(v.data)]
	return nil
}

func VectorToProtoVector(vec *Vector) (ret api.Vector, err error) {
	nsp, err := vec.nsp.Show()
	if err != nil {
		return
	}
	sz := vec.typ.TypeSize()
	return api.Vector{
		Nsp:      nsp,
		Nullable: true,
		Area:     vec.area,
		IsConst:  vec.IsConst(),
		Len:      uint32(vec.length),
		Type:     TypeToProtoType(vec.typ),
		// Data:     vec.data,
		Data: vec.data[:vec.length*sz],
	}, nil
}

func ProtoVectorToVector(vec api.Vector) (*Vector, error) {
	rvec := NewVecFromReuse()
	rvec.area = vec.Area
	rvec.length = int(vec.Len)
	rvec.typ = ProtoTypeToType(vec.Type)
	rvec.cantFreeData = true
	rvec.cantFreeArea = true

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
	return rvec, nil
}

func TypeToProtoType(typ types.Type) plan.Type {
	return plan.Type{
		Id:    int32(typ.Oid),
		Width: typ.Width,
		Scale: typ.Scale,
	}
}

func ProtoTypeToType(typ plan.Type) types.Type {
	return types.New(types.T(typ.Id), typ.Width, typ.Scale)
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
	case types.T_bit:
		return appendBytesToFixSized[uint64](vec)
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
	case types.T_enum:
		return appendBytesToFixSized[types.Enum](vec)
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
	case types.T_year:
		return appendBytesToFixSized[types.MoYear](vec)
	}
	panic(fmt.Sprintf("unexpected type: %s", vec.GetType().String()))
}
