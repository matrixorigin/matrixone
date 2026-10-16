// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package external

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/parquetvariant"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func shreddedField(typed parquet.Node) parquet.Group {
	g := parquet.Group{"value": parquet.Optional(parquet.Leaf(parquet.ByteArrayType))}
	if typed != nil {
		g["typed_value"] = parquet.Optional(typed)
	}
	return g
}
func shreddedSchema(typed parquet.Node, optional bool) *parquet.Schema {
	g := shreddedField(typed)
	g["metadata"] = parquet.Leaf(parquet.ByteArrayType)
	var node parquet.Node = parquetVariantTestNode{g}
	if optional {
		node = parquet.Optional(node)
	}
	return parquet.NewSchema("shredded", parquet.Group{"v": node})
}

// Independent one-byte metadata tables, not the production encoder.
func shreddedMetadata(keys ...string) []byte {
	b := []byte{1, byte(len(keys)), 0}
	offset := 0
	for _, key := range keys {
		offset += len(key)
		b = append(b, byte(offset))
	}
	for _, key := range keys {
		b = append(b, key...)
	}
	return b
}
func shreddedRow(s *parquet.Schema, metadata, raw []byte, typed any) parquet.Row {
	v := map[string]any{"metadata": metadata, "typed_value": typed}
	if raw != nil {
		v["value"] = raw
	}
	return s.Deconstruct(nil, map[string]any{"v": v})
}
func shreddedRead(t *testing.T, s *parquet.Schema, row parquet.Row) bytejson.ByteJson {
	t.Helper()
	col := parquetFileColumns(t, s).Columns()[0]
	got, err := reconstructParquetVariant(context.Background(), col, row)
	require.NoError(t, err)
	bj := got.(bytejson.ByteJson)
	require.True(t, bytejson.IsValidByteJson(bj))
	return bj
}

func TestParquetVariantShreddedPresenceAndMerge(t *testing.T) {
	s := shreddedSchema(parquet.Group{"a": shreddedField(parquet.Leaf(parquet.Int64Type)), "b": shreddedField(parquet.String())}, false)
	meta := shreddedMetadata("a", "b", "cold")
	for _, tc := range []struct {
		raw   []byte
		typed any
		want  string
	}{
		{nil, nil, "null"},
		{[]byte{0}, nil, "null"},
		{[]byte{0x0d, 'a', 'b', 'c'}, nil, `"abc"`},
		{nil, map[string]any{}, `{}`},
		{nil, map[string]any{"a": map[string]any{"value": []byte{0}}}, `{"a": null}`},
		{nil, map[string]any{"a": map[string]any{"typed_value": int64(math.MaxInt64)}, "b": map[string]any{"typed_value": "agent"}}, `{"a": 9223372036854775807, "b": "agent"}`},
		{[]byte{2, 1, 2, 0, 2, 12, 7}, map[string]any{"a": map[string]any{"typed_value": int64(-8)}}, `{"a": -8, "cold": 7}`},
	} {
		require.Equal(t, tc.want, shreddedRead(t, s, shreddedRow(s, meta, tc.raw, tc.typed)).String())
	}
	col := parquetFileColumns(t, s).Columns()[0]
	for _, tc := range []struct {
		meta, raw []byte
		typed     any
	}{
		{meta, []byte{2, 0, 0}, nil},                       // An object must have a defined typed object.
		{meta, []byte{0}, map[string]any{}},                // Conflicting representations.
		{meta, []byte{2, 1, 0, 0, 1, 0}, map[string]any{}}, // Collision even with a missing typed field.
		{shreddedMetadata("a"), nil, map[string]any{}},     // All typed schema names must be in metadata.
		{[]byte{1, 1, 0, 1, 0xff}, nil, map[string]any{}},
	} {
		_, err := reconstructParquetVariant(context.Background(), col, shreddedRow(s, tc.meta, tc.raw, tc.typed))
		require.Error(t, err)
	}
}

func TestParquetVariantShreddedArrays(t *testing.T) {
	scalar := parquet.Leaf(parquet.Int64Type)
	inner := parquet.List(shreddedField(scalar))
	s := shreddedSchema(parquet.List(shreddedField(inner)), false)
	element := func(v any) any { return map[string]any{"typed_value": v} }
	raw := func(b []byte) any { return map[string]any{"value": b} }
	for _, tc := range []struct {
		v    any
		want string
	}{
		{[]any{}, `[]`},
		{[]any{raw([]byte{0})}, `[null]`},
		{[]any{element([]any{})}, `[[]]`},
		{[]any{element([]any{element(int64(1)), raw([]byte{0})}), raw([]byte{0}), element([]any{element(int64(-2))})}, `[[1, null], null, [-2]]`},
	} {
		require.Equal(t, tc.want, shreddedRead(t, s, shreddedRow(s, shreddedMetadata(), nil, tc.v)).String())
	}
	col := parquetFileColumns(t, s).Columns()[0]
	for _, row := range []parquet.Row{
		shreddedRow(s, shreddedMetadata(), nil, []any{map[string]any{}}),
		shreddedRow(s, shreddedMetadata(), []byte{3, 0, 0}, []any{}),
		shreddedRow(s, shreddedMetadata(), nil, []any{element([]any{map[string]any{}})}),
	} {
		_, err := reconstructParquetVariant(context.Background(), col, row)
		require.Error(t, err)
	}
	valid := shreddedRow(s, shreddedMetadata(), nil, []any{element([]any{element(int64(1)), element(int64(2))}), element([]any{})})
	// Drop a single outer-element boundary from one of the leaves.
	for i, v := range valid {
		if strings.Contains(strings.Join(s.Columns()[v.Column()], "."), "element.value") && v.RepetitionLevel() == 1 {
			bad := append(parquet.Row(nil), valid[:i]...)
			bad = append(bad, valid[i+1:]...)
			_, err := reconstructParquetVariant(context.Background(), col, bad)
			require.Error(t, err)
			break
		}
	}
}

func TestParquetVariantShreddedTypedScalars(t *testing.T) {
	unhex := func(s string) []byte { b, err := hex.DecodeString(s); require.NoError(t, err); return b }

	for _, tc := range []struct {
		node parquet.Node
		v    any
		want string
		tp   bytejson.TpCode
	}{
		{parquet.Int(8), int32(-128), "-128", bytejson.TpCodeInt64},
		{parquet.Int(16), int32(32767), "32767", bytejson.TpCodeInt64},
		{parquet.Leaf(parquet.FloatType), float32(1.25), "1.25", bytejson.TpCodeFloat64},
		{parquet.Leaf(parquet.DoubleType), float64(-0.125), "-0.125", bytejson.TpCodeFloat64},
		{parquet.Leaf(parquet.BooleanType), true, "true", bytejson.TpCodeLiteral},
		{parquet.String(), "你好🌏", `"你好🌏"`, bytejson.TpCodeString},
		{parquet.Leaf(parquet.ByteArrayType), []byte{0, 255, 1}, `"AP8B"`, bytejson.TpCodeOpaque},
		{parquet.Decimal(2, 9, parquet.Int32Type), int32(-1), "-0.01", bytejson.TpCodeDecimal},
		{parquet.Decimal(3, 18, parquet.Int64Type), int64(900719925474099300), "900719925474099.300", bytejson.TpCodeDecimal},
		{parquet.Decimal(2, 38, parquet.FixedLenByteArrayType(16)), bytes.Repeat([]byte{0xff}, 16), "-0.01", bytejson.TpCodeDecimal},
		{parquet.Decimal(0, 38, parquet.FixedLenByteArrayType(16)), unhex("4b3b4ca85a86c47a098a223fffffffff"), strings.Repeat("9", 38), bytejson.TpCodeDecimal},
		{parquet.Decimal(38, 38, parquet.FixedLenByteArrayType(16)), unhex("b4c4b357a5793b85f675ddc000000001"), "-0." + strings.Repeat("9", 38), bytejson.TpCodeDecimal},

		{parquet.Date(), int32(0), `"1970-01-01"`, bytejson.TpCodeDate},
		{parquet.TimeAdjusted(parquet.Microsecond, false), int64(1), `"00:00:00.000001"`, bytejson.TpCodeTime},
		{parquet.TimestampAdjusted(parquet.Microsecond, false), int64(-1), `"1969-12-31 23:59:59.999999"`, bytejson.TpCodeDatetime},
	} {
		t.Run(tc.node.String(), func(t *testing.T) {
			s := shreddedSchema(tc.node, false)
			bj := shreddedRead(t, s, shreddedRow(s, shreddedMetadata(), nil, tc.v))
			require.Equal(t, tc.tp, bj.Type)
			require.Equal(t, tc.want, bj.String())
		})
	}
	for _, tc := range []struct {
		node parquet.Node
		v    any
	}{
		{parquet.Int(8), int32(128)},
		{parquet.Int(16), int32(-32769)},
		{parquet.Decimal(0, 2, parquet.Int32Type), int32(100)},
		{parquet.Decimal(0, 38, parquet.FixedLenByteArrayType(16)), bytes.Repeat([]byte{0}, 17)},
		{parquet.Decimal(0, 38, parquet.FixedLenByteArrayType(16)), unhex("4b3b4ca85a86c47a098a224000000000")}, // 10^38 exceeds precision.
		{parquet.Decimal(0, 38, parquet.FixedLenByteArrayType(16)), unhex("80000000000000000000000000000000")}, // minimum int128 also exceeds precision.

		{parquet.Decimal(0, 38, parquet.FixedLenByteArrayType(16)), []byte{}},
		{parquet.Leaf(parquet.DoubleType), math.Inf(1)},
		{parquet.Leaf(parquet.FloatType), float32(math.NaN())},
		{parquet.String(), string([]byte{255})},
		{parquet.Date(), int32(math.MaxInt32)},
		{parquet.TimeAdjusted(parquet.Microsecond, false), int64(-1)},
	} {
		s := shreddedSchema(tc.node, false)
		col := parquetFileColumns(t, s).Columns()[0]
		_, err := reconstructParquetVariant(context.Background(), col, shreddedRow(s, shreddedMetadata(), nil, tc.v))
		require.Error(t, err)
	}
	for _, node := range []parquet.Node{parquet.Uint(64), parquet.Timestamp(parquet.Microsecond), parquet.TimestampAdjusted(parquet.Nanosecond, false), parquet.UUID(), parquet.List(parquet.Optional(shreddedField(nil)))} {
		require.Error(t, validateParquetVariantSchema(context.Background(), parquetFileColumns(t, shreddedSchema(node, false))))
	}
}

// Ten thousand distinct source rows and independently computed JSON values.
// Read real pages/row groups through MO's batch import, for JSON and text.
func TestParquetVariantShreddedTenThousandFileRows(t *testing.T) {
	g := shreddedField(parquet.Group{"a": shreddedField(parquet.Leaf(parquet.Int64Type))})
	g["metadata"] = parquet.Leaf(parquet.ByteArrayType)
	s := parquet.NewSchema("test", parquet.Group{"a_unused_000": parquet.Leaf(parquet.Int32Type), "z_nested": parquet.Optional(parquetVariantTestNode{g})})
	const n = 10000
	input := make([]any, n)
	expected := make([]string, n)
	for i := 0; i < n; i++ {
		row := map[string]any{"a_unused_000": int32(i)}
		input[i] = row
		if i%17 == 0 {
			continue
		}
		v := map[string]any{"metadata": shreddedMetadata("a", "cold")}
		row["z_nested"] = v
		if i%19 == 0 {
			v["value"] = []byte{0}
			expected[i] = "null"
			continue
		}
		if i%23 == 0 {
			v["value"] = []byte{5, 'x'}
			expected[i] = `"x"`
			continue
		}
		// cold is in the residual object; a is a typed field with type drift.
		wire := binary.LittleEndian.AppendUint64([]byte{6 << 2}, uint64(i))
		v["value"] = append([]byte{2, 1, 1, 0, 9}, wire...)
		a := map[string]any{}
		v["typed_value"] = map[string]any{"a": a}
		want := map[string]any{"cold": i}
		switch i % 5 {
		case 0: // missing
		case 1:
			a["value"] = []byte{0}
			want["a"] = nil
		case 2:
			a["value"] = []byte{5, 'x'}
			want["a"] = "x"
		case 3:
			a["typed_value"] = int64(i) * 900719925474
			want["a"] = int64(i) * 900719925474
		case 4:
			a["value"] = []byte{3, 0, 0}
			want["a"] = []any{}
		}
		b, err := json.Marshal(want)
		require.NoError(t, err)
		bj, err := bytejson.ParseFromByteSliceExact(b)
		require.NoError(t, err)
		expected[i] = bj.String()
	}
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, s, parquet.MaxRowsPerRowGroup(127), parquet.PageBufferSize(256))
	physical := make([]parquet.Row, len(input))
	for i, row := range input {
		physical[i] = s.Deconstruct(nil, row)
	}
	_, err := w.WriteRows(physical)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	require.Equal(t, int64(n), file.NumRows())
	require.Equal(t, 79, len(file.RowGroups()))
	for i, group := range file.RowGroups() {
		want := int64(127)
		if i == 78 {
			want = 94
		}
		require.Equal(t, want, group.NumRows())
	}
	for _, target := range []types.T{types.T_json, types.T_text} {
		t.Run(target.String(), func(t *testing.T) {
			param := nestedAndScalarProjectionParam(buf.Bytes())
			param.Cols[1].Typ.Id = int32(target)
			proc := testutil.NewProc(t)
			r := NewParquetReader(param, proc)
			empty, err := r.Open(param, proc)
			require.NoError(t, err)
			require.False(t, empty)
			defer r.Close()
			seen := 0
			done := false
			for !done {
				bat := vectorBatch([]types.Type{types.T_int32.ToType(), target.ToType()})
				done, err = r.ReadBatch(context.Background(), bat, proc, nil)
				require.NoError(t, err)
				for j := 0; j < bat.RowCount(); j++ {
					require.Equal(t, int32(seen), vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])[j])
					v := bat.Vecs[1]
					require.Equal(t, expected[seen] == "", v.IsNull(uint64(j)), fmt.Sprintf("row %d", seen))
					if expected[seen] != "" {
						got := v.GetStringAt(j)
						if target == types.T_json {
							bj := types.DecodeJson(v.GetBytesAt(j))
							require.True(t, bytejson.IsValidByteJson(bj))
							got = bj.String()
						}
						require.Equal(t, expected[seen], got, fmt.Sprintf("row %d", seen))
					}
					seen++
				}
				bat.Clean(proc.Mp())
			}
			require.Equal(t, n, seen)
		})
	}
}

func TestParquetVariantShreddedTenThousandInvalidRows(t *testing.T) {
	s := shreddedSchema(parquet.Int(8), false)
	col := parquetFileColumns(t, s).Columns()[0]
	meta, raw, typed := variantChildren(col)
	for i := 0; i < 10000; i++ {
		good := shreddedRow(s, shreddedMetadata(), nil, int32(i%127))
		bad := append(parquet.Row(nil), good...)
		for j, v := range bad {
			switch i % 10 {
			case 0:
				if v.Column() == meta.Index() {
					bad = append(bad[:j], bad[j+1:]...)
				}
			case 1:
				if v.Column() == meta.Index() {
					bad = append(bad, v)
				}
			case 2:
				if v.Column() == meta.Index() {
					bad[j] = parquet.ByteArrayValue([]byte{2, 0, 0}).Level(0, 0, meta.Index())
				}
			case 3:
				if v.Column() == typed.Index() {
					bad[j] = parquet.Int32Value(int32(128+i)).Level(0, 1, typed.Index())
				}
			case 4:
				if v.Column() == typed.Index() {
					bad[j] = parquet.DoubleValue(1).Level(0, 1, typed.Index())
				}
			case 5:
				if v.Column() == typed.Index() {
					bad[j] = parquet.NullValue().Level(0, 1, typed.Index())
				}
			case 6:
				if v.Column() == raw.Index() {
					bad[j] = parquet.ByteArrayValue([]byte{0}).Level(0, 1, raw.Index())
				}
			case 7:
				if v.Column() == typed.Index() {
					bad[j] = v.Level(1, 1, typed.Index())
				}
			case 8:
				if v.Column() == typed.Index() {
					bad[j] = v.Level(0, 2, typed.Index())
				}
			case 9:
				if v.Column() == typed.Index() {
					bad[j] = v.Level(0, 1, 99)
				}
			}
			if i%10 == 0 && v.Column() == meta.Index() {
				break
			}
		}
		_, err := reconstructParquetVariant(context.Background(), col, bad)
		require.Error(t, err, "case %d must reject", i)
	}
}

func TestParquetVariantShreddedCombinedDepthAndCancellation(t *testing.T) {
	for _, depth := range []int{60, 63} {
		var node parquet.Node = parquet.Leaf(parquet.Int64Type)
		var value any = int64(1)
		for i := 0; i < depth; i++ {
			node = parquet.Group{"a": shreddedField(node)}
			value = map[string]any{"a": map[string]any{"typed_value": value}}
		}
		s := shreddedSchema(node, false)
		var buf bytes.Buffer
		w := parquet.NewWriter(&buf, s)
		require.NoError(t, w.Close())
		file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if depth == 63 {
			require.ErrorContains(t, err, "127 nested levels")
			continue
		}
		require.NoError(t, err)
		col := file.Root().Columns()[0]
		err = validateParquetVariantSchema(context.Background(), col)
		require.NoError(t, err)
		bj := shreddedRead(t, s, shreddedRow(s, shreddedMetadata("a"), nil, value))
		require.Equal(t, bytejson.TpCodeObject, bj.Type)
	}
	s := shreddedSchema(parquet.Group{"a": shreddedField(nil)}, false)
	col := parquetFileColumns(t, s).Columns()[0]
	for _, n := range []int{99, 100} {
		bj, err := bytejson.ParseFromStringExact(strings.Repeat("[", n) + "1" + strings.Repeat("]", n))
		require.NoError(t, err)
		meta, payload, err := parquetvariant.Encode(context.Background(), bj)
		require.NoError(t, err)
		_ = meta
		row := shreddedRow(s, shreddedMetadata("a"), nil, map[string]any{"a": map[string]any{"value": payload}})
		_, err = reconstructParquetVariant(context.Background(), col, row)
		if n == 99 {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, "100")
		}
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := fmt.Errorf("caller stopped import")
	cancel(cause)
	_, err := reconstructParquetVariant(ctx, col, nil)
	require.ErrorIs(t, err, cause)
}

func FuzzParquetVariantShreddedRows(f *testing.F) {
	s := shreddedSchema(parquet.Group{"a": shreddedField(parquet.Int(8))}, false)
	// Open schema once without a testing.T dependency.
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, s)
	if err := w.Close(); err != nil {
		f.Fatal(err)
	}
	file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		f.Fatal(err)
	}
	col := file.Root().Columns()[0]
	arraySchema := shreddedSchema(parquet.List(shreddedField(parquet.Int(8))), false)
	var arrayBuf bytes.Buffer
	arrayWriter := parquet.NewWriter(&arrayBuf, arraySchema)
	if err := arrayWriter.Close(); err != nil {
		f.Fatal(err)
	}
	arrayFile, err := parquet.OpenFile(bytes.NewReader(arrayBuf.Bytes()), int64(arrayBuf.Len()))
	if err != nil {
		f.Fatal(err)
	}
	arrayCol := arrayFile.Root().Columns()[0]
	f.Add([]byte{2, 4, 0, 1, 2, 3, 4})
	f.Add([]byte{0, 1, 2, 3})
	f.Add([]byte{1, 0, 0})
	f.Add([]byte{255, 255, 255, 255})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4096 {
			return
		}
		meta := shreddedMetadata("a")
		raw := data
		var typed any
		if len(data) > 0 && data[0]&1 == 0 {
			typed = map[string]any{"a": map[string]any{"typed_value": int32(int8(data[0]))}}
			raw = nil
		}
		activeSchema, activeCol := s, col
		var arrayExpected []any
		isArray := len(data) > 0 && data[0]%4 == 2
		if isArray {
			activeSchema, activeCol = arraySchema, arrayCol
			meta = shreddedMetadata()
			raw = nil
			elements := make([]any, 0, min(len(data), 32))
			arrayExpected = make([]any, 0, min(len(data), 32))
			for _, b := range data[:min(len(data), 32)] {
				switch b % 3 {
				case 0:
					elements = append(elements, map[string]any{"value": []byte{0}})
					arrayExpected = append(arrayExpected, nil)
				case 1:
					elements = append(elements, map[string]any{"typed_value": int32(int8(b))})
					arrayExpected = append(arrayExpected, int64(int8(b)))
				case 2:
					elements = append(elements, map[string]any{"value": []byte{5, b % 128}})
					arrayExpected = append(arrayExpected, string([]byte{b % 128}))
				}
			}
			typed = elements
		}
		row := shreddedRow(activeSchema, meta, raw, typed)
		mutated := len(data) > 1 && data[1]%5 != 4
		if len(data) > 1 {
			j := int(data[1]) % len(row)
			v := row[j]
			switch data[1] % 5 {
			case 0:
				row[j] = v.Level(int(data[1]%3), int(data[1]%4), v.Column())
			case 1:
				row = append(row, v)
			case 2:
				row = append(row[:j], row[j+1:]...)
			case 3:
				row[j] = parquet.ByteArrayValue(data[2:]).Level(0, int(v.DefinitionLevel()), v.Column())
			}
		}
		got, err := reconstructParquetVariant(context.Background(), activeCol, row)
		if !mutated && (isArray || typed != nil) {
			require.NoError(t, err, "valid generated typed rows must be accepted")
		}
		if err != nil {
			return
		}
		bj := got.(bytejson.ByteJson)
		require.True(t, bytejson.IsValidByteJson(bj))
		require.True(t, json.Valid([]byte(bj.String())))
		if isArray && !mutated {
			expected, e := json.Marshal(arrayExpected)
			require.NoError(t, e)
			want, e := bytejson.ParseFromByteSliceExact(expected)
			require.NoError(t, e)
			require.Zero(t, bytejson.CompareByteJson(want, bj))
		}
		metadata, value, err := parquetvariant.Encode(context.Background(), bj)
		require.NoError(t, err)
		decoded, err := parquetvariant.Decode(context.Background(), metadata, value)
		require.NoError(t, err)
		require.Zero(t, bytejson.CompareByteJson(bj, decoded))
	})
}

func TestParquetVariantSQLNullCannotHideMalformedRow(t *testing.T) {
	s := shreddedSchema(parquet.Leaf(parquet.Int64Type), true)
	col := parquetFileColumns(t, s).Columns()[0]
	valid := s.Deconstruct(nil, map[string]any{"v": nil})
	for _, fn := range []func(context.Context, *parquet.Column, []parquet.Value) (any, error){reconstructNestedValue, reconstructNestedByType} {
		got, err := fn(context.Background(), col, valid)
		require.NoError(t, err)
		require.Nil(t, got)
	}
	_, _, typed := variantChildren(col)
	proc := testutil.NewProc(t)
	handler := &ParquetHandler{}
	for _, row := range []parquet.Row{
		{valid[0], valid[1], parquet.Int64Value(1).Level(0, 2, typed.Index())},
		{valid[0], valid[1]},
		{valid[0], valid[1], valid[2], valid[2]},
	} {
		for _, fn := range []func(context.Context, *parquet.Column, []parquet.Value) (any, error){reconstructNestedValue, reconstructNestedByType} {
			_, err := fn(context.Background(), col, row)
			require.Error(t, err)
		}
		vec := vector.NewVec(types.T_json.ToType())
		err := handler.processNestedValue(row, col, nil, vec, &plan.ColDef{Name: "v", Typ: plan.Type{Id: int32(types.T_json)}}, proc)
		require.Error(t, err)
		require.Zero(t, vec.Length())
		vec.Free(proc.Mp())
	}
}

func TestParquetVariantShreddedObjectsInArraysFile(t *testing.T) {
	object := parquet.Group{"A": shreddedField(parquet.String()), "a": shreddedField(parquet.Leaf(parquet.Int64Type))}
	group := shreddedField(parquet.List(shreddedField(object)))
	group["metadata"] = parquet.Leaf(parquet.ByteArrayType)
	s := parquet.NewSchema("test", parquet.Group{"a_unused_000": parquet.Leaf(parquet.Int32Type), "z_nested": parquetVariantTestNode{group}})
	const n = 1000
	rows := make([]parquet.Row, n)
	expected := make([]string, n)
	for i := 0; i < n; i++ {
		elements := make([]any, 0, i%9)
		want := make([]any, 0, i%9)
		for j := 0; j < i%9; j++ {
			switch j % 4 {
			case 0:
				elements = append(elements, map[string]any{"value": []byte{0}})
				want = append(want, nil)
			case 1:
				elements = append(elements, map[string]any{"typed_value": map[string]any{}})
				want = append(want, map[string]any{})
			case 2:
				elements = append(elements, map[string]any{"typed_value": map[string]any{"A": map[string]any{"typed_value": "Agent🌏"}, "a": map[string]any{"typed_value": int64(i)}}})
				want = append(want, map[string]any{"A": "Agent🌏", "a": i})
			case 3:
				elements = append(elements, map[string]any{"value": []byte{2, 1, 2, 0, 1, 4}, "typed_value": map[string]any{"a": map[string]any{"value": []byte{0}}}})
				want = append(want, map[string]any{"cold": true, "a": nil})
			}
		}
		rows[i] = s.Deconstruct(nil, map[string]any{"a_unused_000": int32(i), "z_nested": map[string]any{"metadata": shreddedMetadata("A", "a", "cold"), "typed_value": elements}})
		data, err := json.Marshal(want)
		require.NoError(t, err)
		bj, err := bytejson.ParseFromByteSliceExact(data)
		require.NoError(t, err)
		expected[i] = bj.String()
	}
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, s, parquet.MaxRowsPerRowGroup(71), parquet.PageBufferSize(128))
	_, err := w.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	for _, target := range []types.T{types.T_json, types.T_text} {
		param := nestedAndScalarProjectionParam(buf.Bytes())
		param.Cols[1].Typ.Id = int32(target)
		proc := testutil.NewProc(t)
		r := NewParquetReader(param, proc)
		_, err := r.Open(param, proc)
		require.NoError(t, err)
		seen := 0
		done := false
		for !done {
			bat := vectorBatch([]types.Type{types.T_int32.ToType(), target.ToType()})
			done, err = r.ReadBatch(context.Background(), bat, proc, nil)
			require.NoError(t, err)
			for j := 0; j < bat.RowCount(); j++ {
				require.Equal(t, int32(seen), vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])[j])
				v := bat.Vecs[1]
				require.False(t, v.IsNull(uint64(j)))
				got := v.GetStringAt(j)
				if target == types.T_json {
					got = types.DecodeJson(v.GetBytesAt(j)).String()
				}
				require.Equal(t, expected[seen], got, "row %d", seen)
				seen++
			}
			bat.Clean(proc.Mp())
		}
		require.Equal(t, n, seen)
		r.Close()
	}
}

func TestParquetVariantShreddedSchemaContracts(t *testing.T) {
	invalid := []parquet.Group{
		{"metadata": parquet.Leaf(parquet.ByteArrayType), "typed_value": parquet.Optional(parquet.Decimal(0, 38, parquet.FixedLenByteArrayType(1)))},
		{"metadata": parquet.Leaf(parquet.ByteArrayType), "value": parquet.Optional(parquet.Leaf(parquet.ByteArrayType))},
		{"metadata": parquet.Leaf(parquet.ByteArrayType), "typed_value": parquet.Optional(parquet.Group{"a": parquet.Optional(shreddedField(nil))})},
		{"metadata": parquet.Leaf(parquet.ByteArrayType), "typed_value": parquet.Optional(parquet.Group{"a": parquet.Leaf(parquet.Int64Type)})},
		{"metadata": parquet.Leaf(parquet.ByteArrayType), "typed_value": parquet.Optional(parquet.Group{"a": parquet.Group{"metadata": parquet.Leaf(parquet.ByteArrayType), "value": parquet.Optional(parquet.Leaf(parquet.ByteArrayType))}})},
		{"metadata": parquet.Leaf(parquet.ByteArrayType), "typed_value": parquet.Optional(parquet.Group{"a": parquet.Group{"other": parquet.Optional(parquet.String())}})},
		{"metadata": parquet.Leaf(parquet.ByteArrayType), "value": parquet.Repeated(parquet.Leaf(parquet.ByteArrayType))},
		{"metadata": parquet.Leaf(parquet.ByteArrayType), "value": parquet.String()},
	}
	for _, g := range invalid {
		s := parquet.NewSchema("invalid", parquet.Group{"v": parquetVariantTestNode{g}})
		require.Error(t, validateParquetVariantSchema(context.Background(), parquetFileColumns(t, s)))
	}
	// Omitting residual value is valid when a field is fully typed. Missing
	// fields remain absent and a missing root follows the VARIANT null rule.
	typedOnly := parquet.Group{"metadata": parquet.Leaf(parquet.ByteArrayType), "typed_value": parquet.Optional(parquet.Group{"a": parquet.Group{"typed_value": parquet.Optional(parquet.String())}})}
	s := parquet.NewSchema("valid", parquet.Group{"v": parquetVariantTestNode{typedOnly}})
	for _, tc := range []struct {
		v    any
		want string
	}{{nil, "null"}, {map[string]any{}, "{}"}, {map[string]any{"a": map[string]any{"typed_value": "x"}}, `{"a": "x"}`}} {
		row := s.Deconstruct(nil, map[string]any{"v": map[string]any{"metadata": shreddedMetadata("a"), "typed_value": tc.v}})
		require.Equal(t, tc.want, shreddedRead(t, s, row).String())
	}
}
