// Copyright 2026 Matrix Origin
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

package types

// RawToFloat64 maps the encoded value raw of type t (a zone map bound, for
// example) onto a float64 that preserves its order, for statistics and range
// estimates. Integers, floats and temporal values are their numeric
// encoding, decimals their value at t.Scale, and strings their first 8 bytes
// read big-endian. ok is false for a type with no such mapping. This is the
// one such mapping; add a new ordered type here.
func RawToFloat64(t Type, raw []byte) (value float64, ok bool) {
	switch t.Oid {
	case T_bit, T_uint64:
		return float64(DecodeUint64(raw)), true
	case T_int8:
		return float64(DecodeInt8(raw)), true
	case T_int16:
		return float64(DecodeInt16(raw)), true
	case T_int32:
		return float64(DecodeInt32(raw)), true
	case T_int64:
		return float64(DecodeInt64(raw)), true
	case T_uint8:
		return float64(DecodeUint8(raw)), true
	case T_uint16:
		return float64(DecodeUint16(raw)), true
	case T_uint32:
		return float64(DecodeUint32(raw)), true
	case T_float32:
		return float64(DecodeFloat32(raw)), true
	case T_float64:
		return DecodeFloat64(raw), true
	case T_bf16:
		return float64(DecodeFixed[BF16](raw).ToFloat32()), true
	case T_float16:
		return float64(DecodeFixed[Float16](raw).ToFloat32()), true
	case T_float8:
		return float64(DecodeFixed[Float8](raw).ToFloat32()), true
	case T_float4:
		return float64(DecodeFixed[Float4](raw).ToFloat32()), true
	case T_date:
		return float64(DecodeDate(raw)), true
	case T_time:
		return float64(DecodeTime(raw)), true
	case T_timestamp:
		return float64(DecodeTimestamp(raw)), true
	case T_datetime:
		return float64(DecodeDatetime(raw)), true
	case T_year:
		return float64(DecodeMoYear(raw)), true
	case T_char, T_varchar, T_text, T_datalink:
		return float64(prefixToUint64(raw)), true
	case T_decimal64:
		return Decimal64ToFloat64(DecodeDecimal64(raw), t.Scale), true
	case T_decimal128:
		return Decimal128ToFloat64(DecodeDecimal128(raw), t.Scale), true
	case T_decimal256:
		return Decimal256ToFloat64(DecodeDecimal256(raw), t.Scale), true
	default:
		return 0, false
	}
}

// prefixToUint64 reads the first 8 bytes of b big-endian, zero-padded.
func prefixToUint64(b []byte) uint64 {
	var result uint64
	for i := 0; i < 8; i++ {
		result <<= 8
		if i < len(b) {
			result |= uint64(b[i])
		}
	}
	return result
}
