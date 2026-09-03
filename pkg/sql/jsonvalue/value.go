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

package jsonvalue

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const (
	mysqlTypeVarchar = 15
	mysqlTypeBlob    = 252
	mysqlTypeString  = 254
)

type GeometryConverter func([]byte) (bytejson.ByteJson, error)

// FromVector converts one resolved SQL value to the scalar representation used
// inside MySQL-compatible JSON constructors. Prepared TEXT provenance is
// resolved by the caller before entering this type-driven conversion.
func FromVector(
	ctx context.Context,
	v *vector.Vector,
	row int,
	loc *time.Location,
	geometry GeometryConverter,
) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if v.IsNull(uint64(row)) {
		return nil, nil
	}
	typ := v.GetType()
	switch typ.Oid {
	case types.T_bool:
		return vector.GetFixedAtNoTypeCheck[bool](v, row), nil
	case types.T_int8:
		return int64(vector.GetFixedAtNoTypeCheck[int8](v, row)), nil
	case types.T_int16:
		return int64(vector.GetFixedAtNoTypeCheck[int16](v, row)), nil
	case types.T_int32:
		return int64(vector.GetFixedAtNoTypeCheck[int32](v, row)), nil
	case types.T_int64:
		return vector.GetFixedAtNoTypeCheck[int64](v, row), nil
	case types.T_uint8:
		return uint64(vector.GetFixedAtNoTypeCheck[uint8](v, row)), nil
	case types.T_uint16:
		return uint64(vector.GetFixedAtNoTypeCheck[uint16](v, row)), nil
	case types.T_uint32:
		return uint64(vector.GetFixedAtNoTypeCheck[uint32](v, row)), nil
	case types.T_uint64:
		return vector.GetFixedAtNoTypeCheck[uint64](v, row), nil
	case types.T_float32:
		return float64(vector.GetFixedAtNoTypeCheck[float32](v, row)), nil
	case types.T_float64:
		return vector.GetFixedAtNoTypeCheck[float64](v, row), nil
	case types.T_char, types.T_varchar, types.T_text:
		return string(v.GetBytesAt(row)), nil
	case types.T_json:
		data := v.GetBytesAt(row)
		if len(data) == 0 {
			return nil, nil
		}
		return types.DecodeJson(data), nil
	case types.T_date:
		return typed(bytejson.TpCodeDate, vector.GetFixedAtNoTypeCheck[types.Date](v, row).String()), nil
	case types.T_time:
		return typed(bytejson.TpCodeTime, vector.GetFixedAtNoTypeCheck[types.Time](v, row).String2(6)), nil
	case types.T_datetime:
		return typed(bytejson.TpCodeDatetime, vector.GetFixedAtNoTypeCheck[types.Datetime](v, row).String2(6)), nil
	case types.T_timestamp:
		if loc == nil {
			loc = time.Local
		}
		return typed(bytejson.TpCodeDatetime, vector.GetFixedAtNoTypeCheck[types.Timestamp](v, row).String2(loc, 6)), nil
	case types.T_decimal64:
		value := vector.GetFixedAtNoTypeCheck[types.Decimal64](v, row)
		return typed(bytejson.TpCodeDecimal, value.Format(typ.Scale)), nil
	case types.T_decimal128:
		value := vector.GetFixedAtNoTypeCheck[types.Decimal128](v, row)
		return typed(bytejson.TpCodeDecimal, value.Format(typ.Scale)), nil
	case types.T_decimal256:
		value := vector.GetFixedAtNoTypeCheck[types.Decimal256](v, row)
		return typed(bytejson.TpCodeDecimal, value.Format(typ.Scale)), nil
	case types.T_binary:
		return bytejson.NewMySQLOpaque(mysqlTypeString, v.GetBytesAt(row)), nil
	case types.T_varbinary:
		return bytejson.NewMySQLOpaque(mysqlTypeVarchar, v.GetBytesAt(row)), nil
	case types.T_blob:
		return bytejson.NewMySQLOpaque(mysqlTypeBlob, v.GetBytesAt(row)), nil
	case types.T_year:
		return uint64(vector.GetFixedAtNoTypeCheck[types.MoYear](v, row)), nil
	case types.T_bit:
		return bit(vector.GetFixedAtNoTypeCheck[uint64](v, row), typ.Width, ctx)
	case types.T_enum:
		return vector.GetFixedAtNoTypeCheck[types.Enum](v, row).String(), nil
	case types.T_geometry, types.T_geometry32:
		if geometry == nil {
			return nil, moerr.NewInvalidInputf(ctx, "geometry JSON conversion is unavailable")
		}
		return geometry(v.GetBytesAt(row))
	case types.T_uuid:
		return vector.GetFixedAtNoTypeCheck[types.Uuid](v, row).String(), nil
	case types.T_array_float32:
		values := types.BytesToArray[float32](v.GetBytesAt(row))
		out := make([]any, len(values))
		for i, value := range values {
			out[i] = float64(value)
		}
		return out, nil
	case types.T_array_float64:
		values := types.BytesToArray[float64](v.GetBytesAt(row))
		out := make([]any, len(values))
		for i, value := range values {
			out[i] = value
		}
		return out, nil
	case types.T_array_bf16:
		values := types.BytesToArray[types.BF16](v.GetBytesAt(row))
		out := make([]any, len(values))
		for i, value := range values {
			out[i] = float64(value.ToFloat32())
		}
		return out, nil
	case types.T_array_float16:
		values := types.BytesToArray[types.Float16](v.GetBytesAt(row))
		out := make([]any, len(values))
		for i, value := range values {
			out[i] = float64(value.ToFloat32())
		}
		return out, nil
	case types.T_array_int8:
		values := types.BytesToArray[int8](v.GetBytesAt(row))
		out := make([]any, len(values))
		for i, value := range values {
			out[i] = float64(value)
		}
		return out, nil
	case types.T_array_uint8:
		values := types.BytesToArray[uint8](v.GetBytesAt(row))
		out := make([]any, len(values))
		for i, value := range values {
			out[i] = float64(value)
		}
		return out, nil
	default:
		return nil, moerr.NewInvalidInputf(ctx, "unsupported type for JSON constructor: %v", typ.String())
	}
}

func typed(tp bytejson.TpCode, value string) bytejson.ByteJson {
	data := binary.AppendUvarint(nil, uint64(len(value)))
	data = append(data, value...)
	return bytejson.ByteJson{Type: tp, Data: data}
}

func bit(value uint64, width int32, ctx context.Context) (bytejson.ByteJson, error) {
	if width <= 0 {
		width = 1
	}
	if width > 64 {
		return bytejson.ByteJson{}, moerr.NewInvalidInputf(ctx, "cannot cast BIT(%d) to json", width)
	}
	if width < 64 {
		value &= uint64(1)<<width - 1
	}
	byteLen := int((width + 7) / 8)
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], value)
	return bytejson.NewMySQLOpaque(16, raw[8-byteLen:]), nil
}
