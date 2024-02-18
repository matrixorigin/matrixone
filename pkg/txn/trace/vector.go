// Copyright 2024 Matrix Origin
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

package trace

import (
	"fmt"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func writeValue(
	vec *vector.Vector,
	row int,
	buf *buffer) {
	dst := buf.alloc(20)
	t := vec.GetType()
	switch t.Oid {
	case types.T_bool:
		v := vector.MustFixedCol[bool](vec)[row]
		if v {
			buf.buf.WriteString("true")
		} else {
			buf.buf.WriteString("false")
		}
	case types.T_bit:
		v := vector.MustFixedCol[uint64](vec)[row]
		buf.buf.MustWrite(uintToString(dst, uint64(v)))
	case types.T_int8:
		v := vector.MustFixedCol[int8](vec)[row]
		buf.buf.MustWrite(intToString(dst, int64(v)))
	case types.T_int16:
		v := vector.MustFixedCol[int16](vec)[row]
		buf.buf.MustWrite(intToString(dst, int64(v)))
	case types.T_int32:
		v := vector.MustFixedCol[int32](vec)[row]
		buf.buf.MustWrite(intToString(dst, int64(v)))
	case types.T_int64:
		v := vector.MustFixedCol[int64](vec)[row]
		buf.buf.MustWrite(intToString(dst, int64(v)))
	case types.T_uint8:
		v := vector.MustFixedCol[uint8](vec)[row]
		buf.buf.MustWrite(uintToString(dst, uint64(v)))
	case types.T_uint16:
		v := vector.MustFixedCol[uint16](vec)[row]
		buf.buf.MustWrite(uintToString(dst, uint64(v)))
	case types.T_uint32:
		v := vector.MustFixedCol[uint32](vec)[row]
		buf.buf.MustWrite(uintToString(dst, uint64(v)))
	case types.T_uint64:
		v := vector.MustFixedCol[uint64](vec)[row]
		buf.buf.MustWrite(uintToString(dst, uint64(v)))
	case types.T_float32:
		v := vector.MustFixedCol[float32](vec)[row]
		buf.buf.MustWrite(floatToString(dst, float64(v)))
	case types.T_float64:
		v := vector.MustFixedCol[float64](vec)[row]
		buf.buf.MustWrite(floatToString(dst, float64(v)))
	case types.T_date:
		v := vector.MustFixedCol[types.Date](vec)[row]
		buf.buf.MustWrite(intToString(dst, int64(v)))
	case types.T_time:
		v := vector.MustFixedCol[types.Time](vec)[row]
		buf.buf.MustWrite(intToString(dst, int64(v)))
	case types.T_datetime:
		v := vector.MustFixedCol[types.Datetime](vec)[row]
		buf.buf.MustWrite(intToString(dst, int64(v)))
	case types.T_timestamp:
		v := vector.MustFixedCol[types.Timestamp](vec)[row]
		buf.buf.MustWrite(intToString(dst, int64(v)))
	case types.T_decimal64:
		v := vector.MustFixedCol[types.Decimal64](vec)[row]
		buf.buf.MustWrite(uintToString(dst, uint64(v)))
	case types.T_uuid:
		v := vector.MustFixedCol[types.Uuid](vec)[row]
		buf.buf.MustWrite(v[:])
	case types.T_char, types.T_varchar, types.T_binary:
		buf.buf.MustWrite(vec.GetBytesAt(row))
	case types.T_enum:
		v := vector.MustFixedCol[types.Enum](vec)[row]
		buf.buf.MustWrite(uintToString(dst, uint64(v)))
	default:
		panic(fmt.Sprintf("not support for %s", t.String()))
	}
}

func uintToString(dst []byte, v uint64) []byte {
	return strconv.AppendUint(nil, v, 10)
}

func intToString(dst []byte, v int64) []byte {
	return strconv.AppendInt(nil, v, 10)
}

func floatToString(dst []byte, v float64) []byte {
	return strconv.AppendFloat(nil, v, 'f', -1, 64)
}
