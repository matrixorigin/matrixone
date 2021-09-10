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

package hash

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"reflect"
	"unsafe"
)

func Rehash(count int, hs []uint64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_int8:
		vs := vec.Col.([]int8)
		for i := 0; i < count; i++ {
			hs[i] = hs[i]*31 + uint64(vs[i])
		}
	case types.T_int16:
		vs := vec.Col.([]int16)
		for i := 0; i < count; i++ {
			hs[i] = uint64(Memhash16(noescape(unsafe.Pointer(&vs[i])), uintptr(hs[i])))
		}
	case types.T_int32:
		vs := vec.Col.([]int32)
		for i := 0; i < count; i++ {
			hs[i] = uint64(Memhash32(noescape(unsafe.Pointer(&vs[i])), uintptr(hs[i])))
		}
	case types.T_int64:
		vs := vec.Col.([]int64)
		for i := 0; i < count; i++ {
			hs[i] = uint64(Memhash64(noescape(unsafe.Pointer(&vs[i])), uintptr(hs[i])))
		}
	case types.T_uint8:
		vs := vec.Col.([]uint8)
		for i := 0; i < count; i++ {
			hs[i] = hs[i]*31 + uint64(vs[i])
		}
	case types.T_uint16:
		vs := vec.Col.([]uint16)
		for i := 0; i < count; i++ {
			hs[i] = uint64(Memhash16(noescape(unsafe.Pointer(&vs[i])), uintptr(hs[i])))
		}
	case types.T_uint32:
		vs := vec.Col.([]uint32)
		for i := 0; i < count; i++ {
			hs[i] = uint64(Memhash32(noescape(unsafe.Pointer(&vs[i])), uintptr(hs[i])))
		}
	case types.T_uint64:
		vs := vec.Col.([]uint64)
		for i := 0; i < count; i++ {
			hs[i] = uint64(Memhash64(noescape(unsafe.Pointer(&vs[i])), uintptr(hs[i])))
		}
	case types.T_decimal:
		vs := vec.Col.([]types.Decimal)
		for i := 0; i < count; i++ {
			hs[i] = uint64(Memhash(unsafe.Pointer(&vs[i]), uintptr(hs[i]), uintptr(encoding.DecimalSize)))
		}
	case types.T_float32:
		vs := vec.Col.([]float32)
		for i := 0; i < count; i++ {
			hs[i] = uint64(F32hash(noescape(unsafe.Pointer(&vs[i])), uintptr(hs[i])))
		}
	case types.T_float64:
		vs := vec.Col.([]float64)
		for i := 0; i < count; i++ {
			hs[i] = uint64(F64hash(noescape(unsafe.Pointer(&vs[i])), uintptr(hs[i])))
		}
	case types.T_date:
		vs := vec.Col.([]types.Date)
		for i := 0; i < count; i++ {
			hs[i] = uint64(Memhash(unsafe.Pointer(&vs[i]), uintptr(hs[i]), uintptr(encoding.DateSize)))
		}
	case types.T_datetime:
		vs := vec.Col.([]types.Datetime)
		for i := 0; i < count; i++ {
			hs[i] = uint64(Memhash(unsafe.Pointer(&vs[i]), uintptr(hs[i]), uintptr(encoding.DatetimeSize)))
		}
	case types.T_char, types.T_varchar, types.T_json:
		vs := vec.Col.(*types.Bytes)
		hp := *(*reflect.SliceHeader)(unsafe.Pointer(&vs.Data))
		for i := 0; i < count; i++ {
			hs[i] = uint64(Memhash(noescape(unsafe.Pointer(hp.Data+uintptr(vs.Offsets[i]))), uintptr(hs[i]), uintptr(vs.Lengths[i])))
		}
	}
}

func RehashSels(sels []int64, hs []uint64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_int8:
		vs := vec.Col.([]int8)
		for _, sel := range sels {
			hs[sel] = hs[sel]*31 + uint64(vs[sel])
		}
	case types.T_int16:
		vs := vec.Col.([]int16)
		for _, sel := range sels {
			hs[sel] = uint64(Memhash16(noescape(unsafe.Pointer(&vs[sel])), uintptr(hs[sel])))
		}
	case types.T_int32:
		vs := vec.Col.([]int32)
		for _, sel := range sels {
			hs[sel] = uint64(Memhash32(noescape(unsafe.Pointer(&vs[sel])), uintptr(hs[sel])))
		}
	case types.T_int64:
		vs := vec.Col.([]int64)
		for _, sel := range sels {
			hs[sel] = uint64(Memhash64(noescape(unsafe.Pointer(&vs[sel])), uintptr(hs[sel])))
		}
	case types.T_uint8:
		vs := vec.Col.([]uint8)
		for _, sel := range sels {
			hs[sel] = hs[sel]*31 + uint64(vs[sel])
		}
	case types.T_uint16:
		vs := vec.Col.([]uint16)
		for _, sel := range sels {
			hs[sel] = uint64(Memhash16(noescape(unsafe.Pointer(&vs[sel])), uintptr(hs[sel])))
		}
	case types.T_uint32:
		vs := vec.Col.([]uint32)
		for _, sel := range sels {
			hs[sel] = uint64(Memhash32(noescape(unsafe.Pointer(&vs[sel])), uintptr(hs[sel])))
		}
	case types.T_uint64:
		vs := vec.Col.([]uint64)
		for _, sel := range sels {
			hs[sel] = uint64(Memhash64(noescape(unsafe.Pointer(&vs[sel])), uintptr(hs[sel])))
		}
	case types.T_decimal:
		vs := vec.Col.([]types.Decimal)
		for _, sel := range sels {
			hs[sel] = uint64(Memhash(unsafe.Pointer(&vs[sel]), uintptr(hs[sel]), uintptr(encoding.DecimalSize)))
		}
	case types.T_float32:
		vs := vec.Col.([]float32)
		for _, sel := range sels {
			hs[sel] = uint64(F32hash(noescape(unsafe.Pointer(&vs[sel])), uintptr(hs[sel])))
		}
	case types.T_float64:
		vs := vec.Col.([]float64)
		for _, sel := range sels {
			hs[sel] = uint64(F64hash(noescape(unsafe.Pointer(&vs[sel])), uintptr(hs[sel])))
		}
	case types.T_date:
		vs := vec.Col.([]types.Date)
		for _, sel := range sels {
			hs[sel] = uint64(Memhash(unsafe.Pointer(&vs[sel]), uintptr(hs[sel]), uintptr(encoding.DateSize)))
		}
	case types.T_datetime:
		vs := vec.Col.([]types.Datetime)
		for _, sel := range sels {
			hs[sel] = uint64(Memhash(unsafe.Pointer(&vs[sel]), uintptr(hs[sel]), uintptr(encoding.DatetimeSize)))
		}
	case types.T_char, types.T_varchar, types.T_json:
		vs := vec.Col.(*types.Bytes)
		hp := *(*reflect.SliceHeader)(unsafe.Pointer(&vs.Data))
		for _, sel := range sels {
			hs[sel] = uint64(Memhash(noescape(unsafe.Pointer(hp.Data+uintptr(vs.Offsets[sel]))), uintptr(hs[sel]), uintptr(vs.Lengths[sel])))
		}
	}
}
