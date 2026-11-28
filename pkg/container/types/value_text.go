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

import (
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
)

// The text of a value. ValueText is the one formatting of a single value as
// text: logs, debug and dump output, duplicate-key and other error messages
// all use it, and AppendSQLLiteral builds on it. A value is what
// vector.GetAny returns for its type: the Go element type for a fixed-size
// type, []byte for a varlena one. The forms vector.ExportValue returns are
// accepted too: a bytejson.ByteJson for json, a []T for a numeric vector. Adding a type means adding its case here;
// TestValueTextCoversTypeOps (in package vector) fails until it is added. See
// docs/design/20261126-type-vft.md.

// ValueText returns the text of v, a non-null value of type t. Timestamps
// are shown in time.Local.
func ValueText(t Type, v any) string {
	return string(AppendValueText(nil, t, v, nil))
}

// AppendValueText appends the text of v, a non-null value of type t, to buf.
// Timestamps are shown in loc, or in time.Local when loc is nil.
func AppendValueText(buf []byte, t Type, v any, loc *time.Location) []byte {
	switch t.Oid {
	case T_bool:
		return strconv.AppendBool(buf, v.(bool))
	case T_bit, T_uint64:
		return strconv.AppendUint(buf, v.(uint64), 10)
	case T_int8:
		return strconv.AppendInt(buf, int64(v.(int8)), 10)
	case T_int16:
		return strconv.AppendInt(buf, int64(v.(int16)), 10)
	case T_int32:
		return strconv.AppendInt(buf, int64(v.(int32)), 10)
	case T_int64:
		return strconv.AppendInt(buf, v.(int64), 10)
	case T_uint8:
		return strconv.AppendUint(buf, uint64(v.(uint8)), 10)
	case T_uint16:
		return strconv.AppendUint(buf, uint64(v.(uint16)), 10)
	case T_uint32:
		return strconv.AppendUint(buf, uint64(v.(uint32)), 10)
	case T_float32:
		return strconv.AppendFloat(buf, float64(v.(float32)), 'g', -1, 32)
	case T_float64:
		return strconv.AppendFloat(buf, v.(float64), 'g', -1, 64)
	case T_bf16:
		return strconv.AppendFloat(buf, float64(v.(BF16).ToFloat32()), 'g', -1, 32)
	case T_float16:
		return strconv.AppendFloat(buf, float64(v.(Float16).ToFloat32()), 'g', -1, 32)
	case T_float8:
		return strconv.AppendFloat(buf, float64(v.(Float8).ToFloat32()), 'g', -1, 32)
	case T_float4:
		return strconv.AppendFloat(buf, float64(v.(Float4).ToFloat32()), 'g', -1, 32)
	case T_decimal64:
		return append(buf, v.(Decimal64).Format(t.Scale)...)
	case T_decimal128:
		return append(buf, v.(Decimal128).Format(t.Scale)...)
	case T_decimal256:
		return append(buf, v.(Decimal256).Format(t.Scale)...)
	case T_date:
		return append(buf, v.(Date).String()...)
	case T_time:
		return append(buf, v.(Time).String2(t.Scale)...)
	case T_datetime:
		return append(buf, v.(Datetime).String2(t.Scale)...)
	case T_timestamp:
		if loc == nil {
			loc = time.Local
		}
		return append(buf, v.(Timestamp).String2(loc, t.Scale)...)
	case T_year:
		return append(buf, v.(MoYear).String()...)
	case T_enum:
		return append(buf, v.(Enum).String()...)
	case T_uuid:
		return append(buf, v.(Uuid).String()...)
	case T_TS:
		return append(buf, v.(TS).ToString()...)
	case T_Rowid:
		return append(buf, v.(Rowid).String()...)
	case T_Blockid:
		b := v.(Blockid)
		return append(buf, b.String()...)
	case T_json:
		if j, ok := v.(bytejson.ByteJson); ok {
			return append(buf, j.String()...)
		}
		return append(buf, DecodeJson(v.([]byte)).String()...)
	case T_array_float32:
		return appendArrayText[float32](buf, v)
	case T_array_float64:
		return appendArrayText[float64](buf, v)
	case T_array_bf16:
		return appendArrayText[BF16](buf, v)
	case T_array_float16:
		return appendArrayText[Float16](buf, v)
	case T_array_int8:
		return appendArrayText[int8](buf, v)
	case T_array_uint8:
		return appendArrayText[uint8](buf, v)
	case T_array_float8, T_array_float4:
		text, err := BlockScaledToString(v.([]byte))
		if err != nil {
			return append(buf, err.Error()...)
		}
		return append(buf, text...)
	case T_char, T_varchar, T_text, T_binary, T_varbinary, T_blob, T_datalink,
		T_geometry, T_geometry32:
		return append(buf, v.([]byte)...)
	default:
		// a type with no case: TestValueTextCoversTypeOps fails on it
		return fmt.Appendf(buf, "<unsupported %s value %v>", t.Oid.String(), v)
	}
}

// AppendSQLLiteral appends v, a value of type t, as a portable SQL literal
// (one a MySQL server reads too) that reads back as the same value; a nil v
// is NULL. Numbers, booleans and ENUM ordinals are bare, bit and binary
// values are hex literals, block-scaled vectors are their exact JSON cell,
// and every other value is its ValueText in single quotes with backslashes
// and quotes escaped. Timestamps are written in loc, or in time.Local when
// loc is nil.
func AppendSQLLiteral(buf []byte, t Type, v any, loc *time.Location) ([]byte, error) {
	return appendSQLLiteral(buf, t, v, loc, false)
}

// AppendMOSQLLiteral is AppendSQLLiteral for SQL that MatrixOne runs: a
// timestamp, and a NaN or infinite float, is written as the BIT_CAST of its
// encoding, which is exact and independent of the session time zone.
func AppendMOSQLLiteral(buf []byte, t Type, v any) ([]byte, error) {
	return appendSQLLiteral(buf, t, v, time.UTC, true)
}

func appendSQLLiteral(buf []byte, t Type, v any, loc *time.Location, mo bool) ([]byte, error) {
	if v == nil {
		return append(buf, "NULL"...), nil
	}
	if mo {
		if out, ok := appendMOExactLiteral(buf, t, v); ok {
			return out, nil
		}
	}
	oid := t.Oid
	switch {
	case oid == T_bool, oid.IsInteger(), oid.IsFloat(), oid == T_enum:
		return AppendValueText(buf, t, v, loc), nil
	case oid == T_bit:
		value := v.(uint64)
		byteLength := min(max((t.Width+7)/8, 1), 8)
		b := slices.Clone(EncodeUint64(&value)[:byteLength])
		slices.Reverse(b)
		return appendHexLiteral(buf, b), nil
	case oid == T_binary, oid == T_varbinary, oid == T_blob,
		oid == T_geometry, oid == T_geometry32:
		return appendHexLiteral(buf, v.([]byte)), nil
	case oid.IsBlockScaledArray():
		text, err := BlockScaledToJSON(v.([]byte))
		if err != nil {
			return buf, err
		}
		return appendQuoted(buf, text), nil
	default:
		return appendQuoted(buf, string(AppendValueText(nil, t, v, loc))), nil
	}
}

// appendMOExactLiteral appends the BIT_CAST literal AppendMOSQLLiteral uses
// for a timestamp and for a NaN or infinite float; ok is false for any other
// value.
func appendMOExactLiteral(buf []byte, t Type, v any) (_ []byte, ok bool) {
	if t.Oid == T_timestamp {
		ts := v.(Timestamp)
		return appendBitCast(buf, EncodeTimestamp(&ts), "TIMESTAMP("+strconv.Itoa(int(t.Scale))+")"), true
	}
	if !t.Oid.IsFloat() {
		return buf, false
	}
	if f, isDouble := v.(float64); isDouble {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return appendBitCast(buf, EncodeFloat64(&f), "DOUBLE"), true
		}
		return buf, false
	}
	if f := floatValue32(t, v); math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
		return appendBitCast(buf, EncodeFloat32(&f), "FLOAT"), true
	}
	return buf, false
}

// floatValue32 is a float32 or low-precision float value as a float32.
func floatValue32(t Type, v any) float32 {
	switch t.Oid {
	case T_bf16:
		return v.(BF16).ToFloat32()
	case T_float16:
		return v.(Float16).ToFloat32()
	case T_float8:
		return v.(Float8).ToFloat32()
	case T_float4:
		return v.(Float4).ToFloat32()
	default:
		return v.(float32)
	}
}

// appendBitCast appends BIT_CAST(UNHEX('<raw>') AS <typ>).
func appendBitCast(buf, raw []byte, typ string) []byte {
	buf = append(buf, "bit_cast(unhex('"...)
	buf = hex.AppendEncode(buf, raw)
	buf = append(buf, "') AS "...)
	buf = append(buf, typ...)
	return append(buf, ')')
}

func appendArrayText[T ArrayElement](buf []byte, v any) []byte {
	if a, ok := v.([]T); ok {
		return append(buf, ArrayToString(a)...)
	}
	return append(buf, BytesToArrayToString[T](v.([]byte))...)
}

func appendHexLiteral(buf, b []byte) []byte {
	buf = append(buf, "x'"...)
	buf = hex.AppendEncode(buf, b)
	return append(buf, '\'')
}

var sqlQuoteEscaper = strings.NewReplacer(`\`, `\\`, `'`, `\'`)

func appendQuoted(buf []byte, s string) []byte {
	buf = append(buf, '\'')
	buf = append(buf, sqlQuoteEscaper.Replace(s)...)
	return append(buf, '\'')
}
