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

// RowEncoder encodes one non-null row of its bound vector into a types.Packer
// with the same type-directed Packer encoding that data-branch hash maps use
// for key materialization, so bytes produced here can be compared byte-wise
// against such keys (for example by mo_data_branch_update_link readers and
// writers).
//
// A RowEncoder borrows the vector's type resolution and backing storage:
// create it once per vector with NewRowEncoder, keep the vector alive and
// unmodified while the encoder is in use, and skip (or reject) null rows
// before calling — both rules match EncodeRowToPacker.
type RowEncoder func(p *types.Packer, row int)

// NewRowEncoder binds a RowEncoder to vec, resolving vec's type and column
// layout once so a row loop pays one type dispatch per vector instead of one
// per row. The produced bytes are identical to EncodeRowToPacker on the same
// vector.
func NewRowEncoder(vec *Vector) RowEncoder {
	enc := newRowEncoder(vec)
	if vec.IsConst() {
		// A constant vector repeats row 0's payload at every logical row.
		return func(p *types.Packer, _ int) { enc(p, 0) }
	}
	return enc
}

// EncodeRowToPacker encodes one non-null vector cell with the same encoding
// as NewRowEncoder. Single-row uses keep this entry point; loops over rows
// should bind a RowEncoder once instead of re-dispatching per row.
func EncodeRowToPacker(p *types.Packer, vec *Vector, row int) error {
	NewRowEncoder(vec)(p, row)
	return nil
}

// newRowEncoder is NewRowEncoder's dispatch core. Each arm captures the typed
// column (a length-1 slice for a constant vector, whose only reachable index
// is 0 through the NewRowEncoder wrapper).
func newRowEncoder(vec *Vector) RowEncoder {
	switch vec.GetType().Oid {
	case types.T_bool:
		col := MustFixedColNoTypeCheck[bool](vec)
		return func(p *types.Packer, row int) { p.EncodeBool(col[row]) }
	case types.T_int8:
		col := MustFixedColNoTypeCheck[int8](vec)
		return func(p *types.Packer, row int) { p.EncodeInt8(col[row]) }
	case types.T_int16:
		col := MustFixedColNoTypeCheck[int16](vec)
		return func(p *types.Packer, row int) { p.EncodeInt16(col[row]) }
	case types.T_int32:
		col := MustFixedColNoTypeCheck[int32](vec)
		return func(p *types.Packer, row int) { p.EncodeInt32(col[row]) }
	case types.T_int64:
		col := MustFixedColNoTypeCheck[int64](vec)
		return func(p *types.Packer, row int) { p.EncodeInt64(col[row]) }
	case types.T_uint8:
		col := MustFixedColNoTypeCheck[uint8](vec)
		return func(p *types.Packer, row int) { p.EncodeUint8(col[row]) }
	case types.T_uint16:
		col := MustFixedColNoTypeCheck[uint16](vec)
		return func(p *types.Packer, row int) { p.EncodeUint16(col[row]) }
	case types.T_uint32:
		col := MustFixedColNoTypeCheck[uint32](vec)
		return func(p *types.Packer, row int) { p.EncodeUint32(col[row]) }
	case types.T_uint64:
		col := MustFixedColNoTypeCheck[uint64](vec)
		return func(p *types.Packer, row int) { p.EncodeUint64(col[row]) }
	case types.T_float32:
		col := MustFixedColNoTypeCheck[float32](vec)
		return func(p *types.Packer, row int) { p.EncodeFloat32(col[row]) }
	case types.T_float64:
		col := MustFixedColNoTypeCheck[float64](vec)
		return func(p *types.Packer, row int) { p.EncodeFloat64(col[row]) }
	case types.T_date:
		col := MustFixedColNoTypeCheck[types.Date](vec)
		return func(p *types.Packer, row int) { p.EncodeDate(col[row]) }
	case types.T_time:
		col := MustFixedColNoTypeCheck[types.Time](vec)
		return func(p *types.Packer, row int) { p.EncodeTime(col[row]) }
	case types.T_datetime:
		col := MustFixedColNoTypeCheck[types.Datetime](vec)
		return func(p *types.Packer, row int) { p.EncodeDatetime(col[row]) }
	case types.T_timestamp:
		col := MustFixedColNoTypeCheck[types.Timestamp](vec)
		return func(p *types.Packer, row int) { p.EncodeTimestamp(col[row]) }
	case types.T_year:
		col := MustFixedColNoTypeCheck[types.MoYear](vec)
		return func(p *types.Packer, row int) { p.EncodeMoYear(col[row]) }
	case types.T_decimal64:
		col := MustFixedColNoTypeCheck[types.Decimal64](vec)
		return func(p *types.Packer, row int) { p.EncodeDecimal64(col[row]) }
	case types.T_decimal128:
		col := MustFixedColNoTypeCheck[types.Decimal128](vec)
		return func(p *types.Packer, row int) { p.EncodeDecimal128(col[row]) }
	case types.T_decimal256:
		return encodeRawRowAsStringType(vec)
	case types.T_uuid:
		col := MustFixedColNoTypeCheck[types.Uuid](vec)
		return func(p *types.Packer, row int) { p.EncodeUuid(col[row]) }
	case types.T_bit:
		col := MustFixedColNoTypeCheck[uint64](vec)
		return func(p *types.Packer, row int) { p.EncodeBit(col[row]) }
	case types.T_enum:
		col := MustFixedColNoTypeCheck[types.Enum](vec)
		return func(p *types.Packer, row int) { p.EncodeEnum(col[row]) }
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_json,
		types.T_binary, types.T_varbinary, types.T_datalink,
		types.T_array_float32, types.T_array_float64:
		data, area := MustVarlenaRawData(vec)
		return func(p *types.Packer, row int) {
			p.EncodeStringType(data[row].GetByteSlice(area))
		}
	case types.T_TS:
		col := MustFixedColNoTypeCheck[types.TS](vec)
		return func(p *types.Packer, row int) { p.EncodeStringType(col[row][:]) }
	default:
		return encodeRawRowAsStringType(vec)
	}
}

// encodeRawRowAsStringType binds the raw-bytes fallback for decimal256 and
// the types without a dedicated Packer primitive. The copy keeps the encoded
// bytes stable if vec's backing storage is reused after the call.
func encodeRawRowAsStringType(vec *Vector) RowEncoder {
	return func(p *types.Packer, row int) {
		raw := vec.GetRawBytesAt(row)
		tmp := make([]byte, len(raw))
		copy(tmp, raw)
		p.EncodeStringType(tmp)
	}
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
