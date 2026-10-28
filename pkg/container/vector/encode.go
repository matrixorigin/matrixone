// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vector

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// EncodeRowToPacker encodes one non-null vector cell with the same
// type-directed Packer encoding that data-branch hash maps use for key
// materialization, so bytes produced here can be compared byte-wise against
// such keys (for example by mo_data_branch_update_link readers and writers).
func EncodeRowToPacker(p *types.Packer, vec *Vector, row int) error {
	switch vec.GetType().Oid {
	case types.T_bool:
		v := GetFixedAtNoTypeCheck[bool](vec, row)
		p.EncodeBool(v)
	case types.T_int8:
		v := GetFixedAtNoTypeCheck[int8](vec, row)
		p.EncodeInt8(v)
	case types.T_int16:
		v := GetFixedAtNoTypeCheck[int16](vec, row)
		p.EncodeInt16(v)
	case types.T_int32:
		v := GetFixedAtNoTypeCheck[int32](vec, row)
		p.EncodeInt32(v)
	case types.T_int64:
		v := GetFixedAtNoTypeCheck[int64](vec, row)
		p.EncodeInt64(v)
	case types.T_uint8:
		v := GetFixedAtNoTypeCheck[uint8](vec, row)
		p.EncodeUint8(v)
	case types.T_uint16:
		v := GetFixedAtNoTypeCheck[uint16](vec, row)
		p.EncodeUint16(v)
	case types.T_uint32:
		v := GetFixedAtNoTypeCheck[uint32](vec, row)
		p.EncodeUint32(v)
	case types.T_uint64:
		v := GetFixedAtNoTypeCheck[uint64](vec, row)
		p.EncodeUint64(v)
	case types.T_float32:
		v := GetFixedAtNoTypeCheck[float32](vec, row)
		p.EncodeFloat32(v)
	case types.T_float64:
		v := GetFixedAtNoTypeCheck[float64](vec, row)
		p.EncodeFloat64(v)
	case types.T_date:
		v := GetFixedAtNoTypeCheck[types.Date](vec, row)
		p.EncodeDate(v)
	case types.T_time:
		v := GetFixedAtNoTypeCheck[types.Time](vec, row)
		p.EncodeTime(v)
	case types.T_datetime:
		v := GetFixedAtNoTypeCheck[types.Datetime](vec, row)
		p.EncodeDatetime(v)
	case types.T_timestamp:
		v := GetFixedAtNoTypeCheck[types.Timestamp](vec, row)
		p.EncodeTimestamp(v)
	case types.T_year:
		v := GetFixedAtNoTypeCheck[types.MoYear](vec, row)
		p.EncodeMoYear(v)
	case types.T_decimal64:
		v := GetFixedAtNoTypeCheck[types.Decimal64](vec, row)
		p.EncodeDecimal64(v)
	case types.T_decimal128:
		v := GetFixedAtNoTypeCheck[types.Decimal128](vec, row)
		p.EncodeDecimal128(v)
	case types.T_decimal256:
		raw := vec.GetRawBytesAt(row)
		tmp := make([]byte, len(raw))
		copy(tmp, raw)
		p.EncodeStringType(tmp)
	case types.T_uuid:
		v := GetFixedAtNoTypeCheck[types.Uuid](vec, row)
		p.EncodeUuid(v)
	case types.T_bit:
		v := GetFixedAtNoTypeCheck[uint64](vec, row)
		p.EncodeBit(v)
	case types.T_enum:
		v := GetFixedAtNoTypeCheck[types.Enum](vec, row)
		p.EncodeEnum(v)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_json,
		types.T_binary, types.T_varbinary, types.T_datalink,
		types.T_array_float32, types.T_array_float64:
		p.EncodeStringType(vec.GetBytesAt(row))
	case types.T_TS:
		v := GetFixedAtNoTypeCheck[types.TS](vec, row)
		p.EncodeStringType(v[:])
	default:
		raw := vec.GetRawBytesAt(row)
		tmp := make([]byte, len(raw))
		copy(tmp, raw)
		p.EncodeStringType(tmp)
	}
	return nil
}

// EncodeDecodedValueToPacker is the Go-value counterpart of
// EncodeRowToPacker and must stay byte-compatible with it.
func EncodeDecodedValueToPacker(p *types.Packer, typ types.Type, v any) error {
	if v == nil {
		p.EncodeNull()
		return nil
	}
	switch typ.Oid {
	case types.T_bool:
		val, ok := v.(bool)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected bool value")
		}
		p.EncodeBool(val)
	case types.T_int8:
		val, ok := v.(int8)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected int8 value")
		}
		p.EncodeInt8(val)
	case types.T_int16:
		val, ok := v.(int16)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected int16 value")
		}
		p.EncodeInt16(val)
	case types.T_int32:
		val, ok := v.(int32)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected int32 value")
		}
		p.EncodeInt32(val)
	case types.T_int64:
		val, ok := v.(int64)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected int64 value")
		}
		p.EncodeInt64(val)
	case types.T_uint8:
		val, ok := v.(uint8)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected uint8 value")
		}
		p.EncodeUint8(val)
	case types.T_uint16:
		val, ok := v.(uint16)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected uint16 value")
		}
		p.EncodeUint16(val)
	case types.T_uint32:
		val, ok := v.(uint32)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected uint32 value")
		}
		p.EncodeUint32(val)
	case types.T_uint64:
		val, ok := v.(uint64)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected uint64 value")
		}
		p.EncodeUint64(val)
	case types.T_float32:
		val, ok := v.(float32)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected float32 value")
		}
		p.EncodeFloat32(val)
	case types.T_float64:
		val, ok := v.(float64)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected float64 value")
		}
		p.EncodeFloat64(val)
	case types.T_date:
		val, ok := v.(types.Date)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected date value")
		}
		p.EncodeDate(val)
	case types.T_time:
		val, ok := v.(types.Time)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected time value")
		}
		p.EncodeTime(val)
	case types.T_datetime:
		val, ok := v.(types.Datetime)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected datetime value")
		}
		p.EncodeDatetime(val)
	case types.T_timestamp:
		val, ok := v.(types.Timestamp)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected timestamp value")
		}
		p.EncodeTimestamp(val)
	case types.T_year:
		val, ok := v.(types.MoYear)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected year value")
		}
		p.EncodeMoYear(val)
	case types.T_decimal64:
		val, ok := v.(types.Decimal64)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected decimal64 value")
		}
		p.EncodeDecimal64(val)
	case types.T_decimal128:
		val, ok := v.(types.Decimal128)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected decimal128 value")
		}
		p.EncodeDecimal128(val)
	case types.T_decimal256:
		return encodeDecodedDecimal256ToPacker(p, v)
	case types.T_uuid:
		val, ok := v.(types.Uuid)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected uuid value")
		}
		p.EncodeUuid(val)
	case types.T_bit:
		switch val := v.(type) {
		case uint64:
			p.EncodeBit(val)
		default:
			return moerr.NewInvalidInputNoCtx("expected bit value")
		}
	case types.T_enum:
		switch val := v.(type) {
		case types.Enum:
			p.EncodeEnum(val)
		case uint16:
			p.EncodeEnum(types.Enum(val))
		default:
			return moerr.NewInvalidInputNoCtx("expected enum value")
		}
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_json,
		types.T_binary, types.T_varbinary, types.T_datalink,
		types.T_array_float32, types.T_array_float64, types.T_TS:
		bytesVal, ok := v.([]byte)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected byte slice value")
		}
		p.EncodeStringType(bytesVal)
	default:
		bytesVal, ok := v.([]byte)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected byte slice value")
		}
		p.EncodeStringType(bytesVal)
	}
	return nil
}

func encodeDecodedDecimal256ToPacker(p *types.Packer, v any) error {
	switch val := v.(type) {
	case types.Decimal256:
		p.EncodeStringType(types.EncodeDecimal256(&val))
	case []byte:
		if len(val) != types.Decimal256Size {
			return moerr.NewInvalidInputNoCtxf("expected decimal256 raw bytes length %d, got %d", types.Decimal256Size, len(val))
		}
		p.EncodeStringType(val)
	default:
		return moerr.NewInvalidInputNoCtx("expected decimal256 value")
	}
	return nil
}
