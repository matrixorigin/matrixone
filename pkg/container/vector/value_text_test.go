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

package vector

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// zeroRowVector returns a one-row vector of oid holding the zero value (an
// empty value for a varlena type, a small object for json).
func zeroRowVector(t *testing.T, oid types.T, mp *mpool.MPool) *Vector {
	typ := oid.ToType()
	vec := NewVec(typ, mp)
	raw := make([]byte, typ.TypeSize())
	if typ.IsVarlen() {
		raw = nil
	}
	if oid == types.T_json {
		j, err := types.ParseStringToByteJson(`{"a":1}`)
		require.NoError(t, err)
		raw, err = types.EncodeJson(j)
		require.NoError(t, err)
	}
	require.NoError(t, AppendRaw(vec, raw, false, mp))
	return vec
}

// Every type id the vector table supports has a text form, a SQL literal
// and a row key, and its raw bytes round-trip.
func TestValueTextCoversTypeOps(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, row := range typeOpsRows {
		oid := row.oid
		vec := zeroRowVector(t, oid, mp)
		typ := *vec.GetType()
		val := GetAny(vec, 0, false)

		text := types.ValueText(typ, val)
		require.NotContains(t, text, "<unsupported", "%s has no ValueText case", oid)
		require.Equal(t, text, vec.RowToString(0), oid.String())
		require.Equal(t, text, types.ValueText(typ, ExportValue(vec, 0)), "%s: the exported form has the same text", oid)

		if !oid.IsBlockScaledArray() { // an empty cell is not a valid block-scaled vector
			lit, err := types.AppendSQLLiteral(nil, typ, val, time.UTC)
			require.NoError(t, err, oid.String())
			require.NotEmpty(t, lit, oid.String())
		}

		// Equal values give equal keys, from another vector and from a view;
		// a value whose raw bytes differ gives a different key.
		key := RowKey(vec, 0)
		same := NewVec(typ, mp)
		require.NoError(t, AppendRaw(same, vec.GetRawBytesAt(0), false, mp))
		require.Equal(t, key, RowKey(same, 0), "%s: equal values, equal keys", oid)
		require.Equal(t, key, RowKeyView(same, 0), "%s: a view key equals the stored key", oid)
		other := append([]byte(nil), vec.GetRawBytesAt(0)...)
		if typ.IsVarlen() {
			other = append(other, 'x')
		} else {
			other[0] ^= 1
		}
		diff := NewVec(typ, mp)
		require.NoError(t, AppendRaw(diff, other, false, mp))
		require.NotEqual(t, key, RowKey(diff, 0), "%s: different values, different keys", oid)
		same.Free(mp)
		diff.Free(mp)

		raw := RawFromValue(oid, val)
		require.Equal(t, vec.GetRawBytesAt(0), raw, oid.String())
		require.Equal(t, val, ValueFromRaw(oid, raw), oid.String())
		vec.Free(mp)
	}
}

func TestValueTextAndSQLLiteral(t *testing.T) {
	dec := types.New(types.T_decimal256, 50, 2)
	d256, err := types.ParseDecimal256("-12345678901234567890123456789012345678901.25", 50, 2)
	require.NoError(t, err)
	require.Equal(t, "-12345678901234567890123456789012345678901.25", types.ValueText(dec, d256))
	lit, err := types.AppendSQLLiteral(nil, dec, d256, nil)
	require.NoError(t, err)
	require.Equal(t, "'-12345678901234567890123456789012345678901.25'", string(lit))

	str := types.T_varchar.ToType()
	lit, err = types.AppendSQLLiteral(nil, str, []byte(`it's a \ test`), nil)
	require.NoError(t, err)
	require.Equal(t, `'it\'s a \\ test'`, string(lit))

	lit, err = types.AppendSQLLiteral(nil, types.T_varbinary.ToType(), []byte{0xde, 0xad}, nil)
	require.NoError(t, err)
	require.Equal(t, "x'dead'", string(lit))

	bit := types.New(types.T_bit, 12, 0)
	lit, err = types.AppendSQLLiteral(nil, bit, uint64(0xabc), nil)
	require.NoError(t, err)
	require.Equal(t, "x'0abc'", string(lit))

	lit, err = types.AppendSQLLiteral(nil, str, nil, nil)
	require.NoError(t, err)
	require.Equal(t, "NULL", string(lit))

	require.Equal(t, "1.5", types.ValueText(types.T_float32.ToType(), float32(1.5)))
	require.Equal(t, "true", types.ValueText(types.T_bool.ToType(), true))

	ts := types.New(types.T_timestamp, 0, 3)
	tsv, err := types.ParseTimestamp(time.UTC, "2026-11-27 10:11:12.345", 3)
	require.NoError(t, err)
	lit, err = types.AppendSQLLiteral(nil, ts, tsv, time.UTC)
	require.NoError(t, err)
	require.Equal(t, "'2026-11-27 10:11:12.345'", string(lit))
}

func TestRawToFloat64(t *testing.T) {
	dec := types.New(types.T_decimal256, 50, 2)
	d, err := types.ParseDecimal256("-3.25", 50, 2)
	require.NoError(t, err)
	f, ok := types.RawToFloat64(dec, types.EncodeDecimal256(&d))
	require.True(t, ok)
	require.Equal(t, -3.25, f)

	f, ok = types.RawToFloat64(types.T_varchar.ToType(), []byte("ab"))
	require.True(t, ok)
	require.Equal(t, float64(uint64('a')<<56|uint64('b')<<48), f)

	_, ok = types.RawToFloat64(types.T_json.ToType(), nil)
	require.False(t, ok)
}

// TestTextAppenderCoversTypeOps: every type id on the table either has no
// text form (and says so) or parses a sample, and the parse of the value's
// ValueText reads back the same value. A new type must be added to one of
// the two lists.
func TestTextAppenderCoversTypeOps(t *testing.T) {
	mp := mpool.MustNewZero()
	noText := map[types.T]bool{
		types.T_enum: true, types.T_TS: true, types.T_Rowid: true, types.T_Blockid: true,
		types.T_geometry: true, types.T_geometry32: true,
	}
	samples := map[types.T]string{
		types.T_bool: "true", types.T_bit: "5",
		types.T_int8: "-5", types.T_int16: "-300", types.T_int32: "-70000", types.T_int64: "-5000000000",
		types.T_uint8: "200", types.T_uint16: "60000", types.T_uint32: "4000000000", types.T_uint64: "18000000000000000000",
		types.T_float32: "1.5", types.T_float64: "2.25",
		types.T_bf16: "1.5", types.T_float16: "1.5", types.T_float8: "1.5", types.T_float4: "1.5",
		types.T_decimal64: "12.34", types.T_decimal128: "-123456789012345678.12", types.T_decimal256: "-1234567890123456789012345678901234567890.12",
		types.T_date: "2024-02-29", types.T_time: "12:34:56", types.T_datetime: "2024-02-29 12:34:56",
		types.T_timestamp: "2024-02-29 12:34:56", types.T_year: "2024", types.T_uuid: "123e4567-e89b-12d3-a456-426614174000",
		types.T_json: `{"a": 1}`,
		types.T_char: "abc", types.T_varchar: "abc", types.T_text: "abc", types.T_binary: "abc",
		types.T_varbinary: "abc", types.T_blob: "abc", types.T_datalink: "abc",
		types.T_array_float32: "[1, 2, 3]", types.T_array_float64: "[1, 2, 3]", types.T_array_bf16: "[1, 2, 3]",
		types.T_array_float16: "[1, 2, 3]", types.T_array_int8: "[1, 2, 3]", types.T_array_uint8: "[1, 2, 3]",
		types.T_array_float8: "[1, 2, 3]", types.T_array_float4: "[1, 2, 3]",
	}
	opts := types.TextParseOptions{Loc: time.UTC}
	for _, row := range typeOpsRows {
		oid := row.oid
		typ := oid.ToType()
		switch {
		case oid.IsDecimal():
			typ = types.New(oid, 50, 2)
			if oid == types.T_decimal64 {
				typ = types.New(oid, 18, 2)
			} else if oid == types.T_decimal128 {
				typ = types.New(oid, 38, 2)
			}
		case oid.IsArray():
			typ = types.New(oid, 3, 0)
		case oid == types.T_bit:
			typ = types.New(oid, 8, 0)
		}
		appendText, err := TextAppender(typ, opts)
		if noText[oid] {
			require.Error(t, err, "%s is listed as having no text form", oid)
			continue
		}
		require.NoError(t, err, "%s: add a TextAppender sample or list it as having no text form", oid)
		sample, ok := samples[oid]
		require.True(t, ok, "%s has no sample", oid)

		vec := NewVec(typ, mp)
		require.NoError(t, appendText(vec, sample, mp), oid.String())
		text := string(types.AppendValueText(nil, typ, GetAny(vec, 0, false), time.UTC))
		require.NoError(t, appendText(vec, text, mp), "%s: its ValueText %q parses", oid, text)
		require.Equal(t, vec.GetRawBytesAt(0), vec.GetRawBytesAt(1), "%s: %q reads back the same value", oid, text)
		if samples[oid] != "abc" { // a string accepts any text
			require.Error(t, appendText(vec, "not a value", mp), "%s rejects bad text", oid)
			require.Equal(t, 2, vec.Length(), "%s: a failed parse appends nothing", oid)
		}
		vec.Free(mp)
	}
}
