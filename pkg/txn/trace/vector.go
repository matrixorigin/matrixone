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
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func writeCompletedValue(
	row []byte,
	buf *buffer,
	dst []byte) {
	tuples, _, _, err := types.DecodeTuple(row)
	if err != nil {
		panic(err)
	}
	buf.buf.WriteString("[")
	for i, t := range tuples {
		switch v := t.(type) {
		case bool:
			if v {
				buf.buf.WriteString("true")
			} else {
				buf.buf.WriteString("false")
			}
		case int8:
			buf.buf.MustWrite(intToString(dst, int64(v)))
		case int16:
			buf.buf.MustWrite(intToString(dst, int64(v)))
		case int32:
			buf.buf.MustWrite(intToString(dst, int64(v)))
		case int64:
			buf.buf.MustWrite(intToString(dst, int64(v)))
		case uint8:
			buf.buf.MustWrite(uintToString(dst, uint64(v)))
		case uint16:
			buf.buf.MustWrite(uintToString(dst, uint64(v)))
		case uint32:
			buf.buf.MustWrite(uintToString(dst, uint64(v)))
		case uint64:
			buf.buf.MustWrite(uintToString(dst, uint64(v)))
		case float32:
			buf.buf.MustWrite(floatToString(dst, float64(v)))
		case float64:
			buf.buf.MustWrite(floatToString(dst, float64(v)))
		case []byte:
			buf.buf.MustWrite(v)
		case types.Date:
			buf.buf.MustWrite(intToString(dst, int64(v)))
		case types.Time:
			buf.buf.MustWrite(intToString(dst, int64(v)))
		case types.Datetime:
			buf.buf.MustWrite(intToString(dst, int64(v)))
		case types.Timestamp:
			buf.buf.MustWrite(intToString(dst, int64(v)))
		case types.Decimal64:
			buf.buf.MustWrite(uintToString(dst, uint64(v)))
		default:
			buf.buf.WriteString(fmt.Sprintf("%v", t))
		}
		if i < len(tuples)-1 {
			buf.buf.WriteString(",")
		}
	}
	buf.buf.WriteString("]")
}

func writeValue(
	vec *vector.Vector,
	row int,
	buf *buffer,
	dst []byte) {
	t := vec.GetType()
	switch t.Oid {
	case types.T_bool:
		v := vector.MustFixedColNoTypeCheck[bool](vec)[row]
		if v {
			buf.buf.WriteString("true")
		} else {
			buf.buf.WriteString("false")
		}
	case types.T_bit:
		v := vector.MustFixedColNoTypeCheck[uint64](vec)[row]
		buf.buf.MustWrite(uintToString(dst, uint64(v)))
	case types.T_int8:
		v := vector.MustFixedColNoTypeCheck[int8](vec)[row]
		buf.buf.MustWrite(intToString(dst, int64(v)))
	case types.T_int16:
		v := vector.MustFixedColNoTypeCheck[int16](vec)[row]
		buf.buf.MustWrite(intToString(dst, int64(v)))
	case types.T_int32:
		v := vector.MustFixedColNoTypeCheck[int32](vec)[row]
		buf.buf.MustWrite(intToString(dst, int64(v)))
	case types.T_int64:
		v := vector.MustFixedColNoTypeCheck[int64](vec)[row]
		buf.buf.MustWrite(intToString(dst, int64(v)))
	case types.T_uint8:
		v := vector.MustFixedColNoTypeCheck[uint8](vec)[row]
		buf.buf.MustWrite(uintToString(dst, uint64(v)))
	case types.T_uint16:
		v := vector.MustFixedColNoTypeCheck[uint16](vec)[row]
		buf.buf.MustWrite(uintToString(dst, uint64(v)))
	case types.T_uint32:
		v := vector.MustFixedColNoTypeCheck[uint32](vec)[row]
		buf.buf.MustWrite(uintToString(dst, uint64(v)))
	case types.T_uint64:
		v := vector.MustFixedColNoTypeCheck[uint64](vec)[row]
		buf.buf.MustWrite(uintToString(dst, uint64(v)))
	case types.T_float32:
		v := vector.MustFixedColNoTypeCheck[float32](vec)[row]
		buf.buf.MustWrite(floatToString(dst, float64(v)))
	case types.T_float64:
		v := vector.MustFixedColNoTypeCheck[float64](vec)[row]
		buf.buf.MustWrite(floatToString(dst, float64(v)))
	case types.T_date:
		v := vector.MustFixedColNoTypeCheck[types.Date](vec)[row]
		buf.buf.MustWrite(intToString(dst, int64(v)))
	case types.T_time:
		v := vector.MustFixedColNoTypeCheck[types.Time](vec)[row]
		buf.buf.MustWrite(intToString(dst, int64(v)))
	case types.T_datetime:
		v := vector.MustFixedColNoTypeCheck[types.Datetime](vec)[row]
		buf.buf.MustWrite(intToString(dst, int64(v)))
	case types.T_timestamp:
		v := vector.MustFixedColNoTypeCheck[types.Timestamp](vec)[row]
		buf.buf.MustWrite(intToString(dst, int64(v)))
	case types.T_decimal64:
		v := vector.MustFixedColNoTypeCheck[types.Decimal64](vec)[row]
		buf.buf.MustWrite(uintToString(dst, uint64(v)))
	case types.T_uuid:
		v := vector.MustFixedColNoTypeCheck[types.Uuid](vec)[row]
		buf.buf.MustWrite(v[:])
	case types.T_char, types.T_varchar, types.T_binary:
		data := vec.GetBytesAt(row)
		buf.buf.MustWrite(data)
	case types.T_enum:
		v := vector.MustFixedColNoTypeCheck[types.Enum](vec)[row]
		buf.buf.MustWrite(uintToString(dst, uint64(v)))
	case types.T_Rowid:
		v := vector.MustFixedColNoTypeCheck[types.Rowid](vec)[row]
		buf.buf.WriteString(v.String())
	case types.T_TS:
		v := vector.MustFixedColNoTypeCheck[types.TS](vec)[row]
		buf.buf.MustWrite(intToString(dst, int64(v.Physical())))
		buf.buf.WriteString("-")
		buf.buf.MustWrite(intToString(dst, int64(v.Logical())))
	case types.T_Blockid:
		v := vector.MustFixedColNoTypeCheck[types.Blockid](vec)[row]
		n := hex.EncodedLen(len(v[:]))
		hex.Encode(dst[:n], v[:])
		buf.buf.MustWrite(dst[:n])
	default:
		buf.buf.WriteString("not support")
	}
}

func uintToString(dst []byte, v uint64) []byte {
	return AppendUint(dst[:0], v, 10)
}

func intToString(dst []byte, v int64) []byte {
	return AppendInt(dst[:0], v, 10)
}

func floatToString(dst []byte, v float64) []byte {
	return strconv.AppendFloat(dst[:0], v, 'f', -1, 64)
}
