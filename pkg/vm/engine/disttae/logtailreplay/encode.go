// Copyright 2023 Matrix Origin
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

package logtailreplay

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func EncodePrimaryKeyVector(vec *vector.Vector, packer *types.Packer) (ret [][]byte) {
	packer.Reset()

	if vec.IsConstNull() {
		return make([][]byte, vec.Length())
	}

	switch vec.GetType().Oid {

	case types.T_bool:
		s := vector.MustFixedCol[bool](vec)
		for _, v := range s {
			packer.EncodeBool(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_bit:
		s := vector.MustFixedCol[uint64](vec)
		for _, v := range s {
			packer.EncodeUint64(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_int8:
		s := vector.MustFixedCol[int8](vec)
		for _, v := range s {
			packer.EncodeInt8(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_int16:
		s := vector.MustFixedCol[int16](vec)
		for _, v := range s {
			packer.EncodeInt16(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_int32:
		s := vector.MustFixedCol[int32](vec)
		for _, v := range s {
			packer.EncodeInt32(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_int64:
		s := vector.MustFixedCol[int64](vec)
		for _, v := range s {
			packer.EncodeInt64(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_uint8:
		s := vector.MustFixedCol[uint8](vec)
		for _, v := range s {
			packer.EncodeUint8(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_uint16:
		s := vector.MustFixedCol[uint16](vec)
		for _, v := range s {
			packer.EncodeUint16(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_uint32:
		s := vector.MustFixedCol[uint32](vec)
		for _, v := range s {
			packer.EncodeUint32(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_uint64:
		s := vector.MustFixedCol[uint64](vec)
		for _, v := range s {
			packer.EncodeUint64(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_float32:
		s := vector.MustFixedCol[float32](vec)
		for _, v := range s {
			packer.EncodeFloat32(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_float64:
		s := vector.MustFixedCol[float64](vec)
		for _, v := range s {
			packer.EncodeFloat64(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_date:
		s := vector.MustFixedCol[types.Date](vec)
		for _, v := range s {
			packer.EncodeDate(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_time:
		s := vector.MustFixedCol[types.Time](vec)
		for _, v := range s {
			packer.EncodeTime(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_datetime:
		s := vector.MustFixedCol[types.Datetime](vec)
		for _, v := range s {
			packer.EncodeDatetime(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_timestamp:
		s := vector.MustFixedCol[types.Timestamp](vec)
		for _, v := range s {
			packer.EncodeTimestamp(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_enum:
		s := vector.MustFixedCol[types.Enum](vec)
		for _, v := range s {
			packer.EncodeEnum(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_decimal64:
		s := vector.MustFixedCol[types.Decimal64](vec)
		for _, v := range s {
			packer.EncodeDecimal64(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_decimal128:
		s := vector.MustFixedCol[types.Decimal128](vec)
		for _, v := range s {
			packer.EncodeDecimal128(v)
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_uuid:
		s := vector.MustFixedCol[types.Uuid](vec)
		for _, v := range s {
			packer.EncodeStringType(v[:])
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}

	case types.T_json, types.T_char, types.T_varchar,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_Rowid,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		for i := 0; i < vec.Length(); i++ {
			packer.EncodeStringType(vec.GetBytesAt(i))
			ret = append(ret, packer.Bytes())
			packer.Reset()
		}
	default:
		panic(fmt.Sprintf("unknown type: %v", vec.GetType().String()))

	}

	l := vec.Length()

	if vec.IsConst() {
		for len(ret) < l {
			ret = append(ret, ret[0])
		}
	}

	if len(ret) != l {
		panic(fmt.Sprintf("bad result, expecting %v, got %v", l, len(ret)))
	}

	return
}

func EncodePrimaryKey(v any, packer *types.Packer) []byte {
	packer.Reset()

	switch v := v.(type) {

	case bool:
		packer.EncodeBool(v)

	case int8:
		packer.EncodeInt8(v)

	case int16:
		packer.EncodeInt16(v)

	case int32:
		packer.EncodeInt32(v)

	case int64:
		packer.EncodeInt64(v)

	case uint8:
		packer.EncodeUint8(v)

	case uint16:
		packer.EncodeUint16(v)

	case uint32:
		packer.EncodeUint32(v)

	case uint64:
		packer.EncodeUint64(v)

	case float32:
		packer.EncodeFloat32(v)

	case float64:
		packer.EncodeFloat64(v)

	case types.Date:
		packer.EncodeDate(v)

	case types.Time:
		packer.EncodeTime(v)

	case types.Datetime:
		packer.EncodeDatetime(v)

	case types.Timestamp:
		packer.EncodeTimestamp(v)

	case types.Decimal64:
		packer.EncodeDecimal64(v)

	case types.Decimal128:
		packer.EncodeDecimal128(v)

	case types.Uuid:
		packer.EncodeStringType(v[:])

	case types.Enum:
		packer.EncodeEnum(v)

	case string:
		packer.EncodeStringType([]byte(v))

	case []byte:
		packer.EncodeStringType(v)

	default:
		panic(fmt.Sprintf("unknown type: %T", v))

	}

	return packer.Bytes()
}
