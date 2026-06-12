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

package externalwrite

import (
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// fixedCol builds a one-row vector of fixed type t holding v.
func fixedCol[T any](t *testing.T, mp *mpool.MPool, typ types.Type, v T) *vector.Vector {
	vec := vector.NewVec(typ)
	require.NoError(t, vector.AppendFixed[T](vec, v, false, mp))
	return vec
}

// bytesCol builds a one-row vector of bytes type typ holding v.
func bytesCol(t *testing.T, mp *mpool.MPool, typ types.Type, v []byte) *vector.Vector {
	vec := vector.NewVec(typ)
	require.NoError(t, vector.AppendBytes(vec, v, false, mp))
	return vec
}

func decType(oid types.T, width, scale int32) types.Type {
	typ := oid.ToType()
	typ.Width = width
	typ.Scale = scale
	return typ
}

// allTypesBatch builds a single-row batch with one column per supported type.
// Returns the batch and the parallel list of column names.
func allTypesBatch(t *testing.T, mp *mpool.MPool) (*batch.Batch, []string) {
	jsonBytes, err := types.EncodeJson(must(types.ParseStringToByteJson(`{"a":1}`)))
	require.NoError(t, err)

	cols := []struct {
		name string
		vec  *vector.Vector
	}{
		{"c_bool", fixedCol(t, mp, types.T_bool.ToType(), true)},
		{"c_bit", fixedCol(t, mp, decType(types.T_bit, 8, 0), uint64(5))},
		{"c_i8", fixedCol(t, mp, types.T_int8.ToType(), int8(-1))},
		{"c_i16", fixedCol(t, mp, types.T_int16.ToType(), int16(-2))},
		{"c_i32", fixedCol(t, mp, types.T_int32.ToType(), int32(-3))},
		{"c_i64", fixedCol(t, mp, types.T_int64.ToType(), int64(-4))},
		{"c_u8", fixedCol(t, mp, types.T_uint8.ToType(), uint8(1))},
		{"c_u16", fixedCol(t, mp, types.T_uint16.ToType(), uint16(2))},
		{"c_u32", fixedCol(t, mp, types.T_uint32.ToType(), uint32(3))},
		{"c_u64", fixedCol(t, mp, types.T_uint64.ToType(), uint64(4))},
		{"c_f32", fixedCol(t, mp, types.T_float32.ToType(), float32(1.5))},
		{"c_f32s", fixedCol(t, mp, decType(types.T_float32, 10, 2), float32(2.5))},
		{"c_f64", fixedCol(t, mp, types.T_float64.ToType(), float64(3.5))},
		{"c_f64s", fixedCol(t, mp, decType(types.T_float64, 10, 2), float64(4.5))},
		{"c_varchar", bytesCol(t, mp, types.T_varchar.ToType(), []byte("hi"))},
		{"c_char", bytesCol(t, mp, types.T_char.ToType(), []byte("ab"))},
		{"c_text", bytesCol(t, mp, types.T_text.ToType(), []byte("txt"))},
		{"c_datalink", bytesCol(t, mp, types.T_datalink.ToType(), []byte("file://x"))},
		{"c_binary", bytesCol(t, mp, types.T_binary.ToType(), []byte{0x01, 0x02})},
		{"c_varbinary", bytesCol(t, mp, types.T_varbinary.ToType(), []byte{0x03})},
		{"c_blob", bytesCol(t, mp, types.T_blob.ToType(), []byte{0x04, 0x05})},
		{"c_json", bytesCol(t, mp, types.T_json.ToType(), jsonBytes)},
		{"c_arrf32", bytesCol(t, mp, types.T_array_float32.ToType(), types.ArrayToBytes[float32]([]float32{1, 2}))},
		{"c_arrf64", bytesCol(t, mp, types.T_array_float64.ToType(), types.ArrayToBytes[float64]([]float64{3, 4}))},
		{"c_date", fixedCol(t, mp, types.T_date.ToType(), must(types.ParseDateCast("2026-06-08")))},
		{"c_datetime", fixedCol(t, mp, decType(types.T_datetime, 0, 0), must(types.ParseDatetime("2026-06-08 09:05:07", 0)))},
		{"c_time", fixedCol(t, mp, decType(types.T_time, 0, 0), must(types.ParseTime("09:05:07", 0)))},
		{"c_timestamp", fixedCol(t, mp, decType(types.T_timestamp, 0, 0), must(types.ParseTimestamp(time.UTC, "2026-06-08 09:05:07", 0)))},
		{"c_year", fixedCol(t, mp, types.T_year.ToType(), must(types.ParseMoYearFromInt(2024)))},
		{"c_dec64", fixedCol(t, mp, decType(types.T_decimal64, 10, 2), must(types.ParseDecimal64("123.45", 10, 2)))},
		{"c_dec128", fixedCol(t, mp, decType(types.T_decimal128, 20, 2), must(types.ParseDecimal128("678.90", 20, 2)))},
		{"c_dec256", fixedCol(t, mp, decType(types.T_decimal256, 40, 2), must(types.ParseDecimal256("11.22", 40, 2)))},
		{"c_uuid", fixedCol(t, mp, types.T_uuid.ToType(), must(types.ParseUuid("00000000-0000-0000-0000-000000000001")))},
		{"c_enum", fixedCol(t, mp, types.T_enum.ToType(), types.Enum(1))},
	}

	names := make([]string, len(cols))
	bat := batch.New(func() []string {
		n := make([]string, len(cols))
		for i, c := range cols {
			n[i] = c.name
		}
		return n
	}())
	for i, c := range cols {
		bat.Vecs[i] = c.vec
		names[i] = c.name
	}
	bat.SetRowCount(1)
	return bat, names
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// TestEncodeCSVAllTypes exercises every csvValue branch over a wide batch.
func TestEncodeCSVAllTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	bat, names := allTypesBatch(t, mp)
	defer bat.Clean(mp)

	w := NewExternalWriter(nil, WriterConfig{
		Format:     FormatCSV,
		Attrs:      names,
		EnclosedBy: '"',
		Stmt:       time.Now(),
	}).(*externalWriter)

	out, err := w.encodeCSV(bat)
	require.NoError(t, err)
	s := string(out)
	require.True(t, strings.HasSuffix(s, "\n"))
	// spot-check a few representative renderings
	require.Contains(t, s, "true")   // bool
	require.Contains(t, s, "-4")     // int64
	require.Contains(t, s, `"hi"`)   // enclosed varchar
	require.Contains(t, s, "123.45") // decimal64
	require.Contains(t, s, "2026")   // date/year
}

// TestEncodeJSONLineAllTypes exercises every jsonValue branch over a wide batch.
func TestEncodeJSONLineAllTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	bat, names := allTypesBatch(t, mp)
	defer bat.Clean(mp)

	w := NewExternalWriter(nil, WriterConfig{
		Format: FormatJSONLine,
		Attrs:  names,
		Stmt:   time.Now(),
	}).(*externalWriter)

	// bit cannot round-trip through JSON strings (DDL rejects it for writable
	// jsonline tables); the encoder guards the unreachable path with an error.
	_, err := w.encodeJSONLine(bat)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bit")

	// drop the binary-payload columns (bit/binary/varbinary/blob, which DDL
	// rejects for writable jsonline tables): every other branch must encode.
	excluded := map[string]bool{"c_bit": true, "c_binary": true, "c_varbinary": true, "c_blob": true}
	jnames := make([]string, 0, len(names))
	jvecs := make([]*vector.Vector, 0, len(names))
	for k, name := range names {
		if excluded[name] {
			continue
		}
		jnames = append(jnames, name)
		jvecs = append(jvecs, bat.Vecs[k])
	}
	require.Len(t, jnames, len(names)-len(excluded))
	jbat := batch.New(jnames)
	jbat.Vecs = jvecs
	jbat.SetRowCount(bat.RowCount())

	w2 := NewExternalWriter(nil, WriterConfig{
		Format: FormatJSONLine,
		Attrs:  jnames,
		Stmt:   time.Now(),
	}).(*externalWriter)
	out, err := w2.encodeJSONLine(jbat)
	require.NoError(t, err)
	s := string(out)
	require.True(t, strings.HasSuffix(s, "\n"))
	require.Contains(t, s, `"c_bool":true`)
	require.Contains(t, s, `"c_i64":-4`)
	require.Contains(t, s, `"c_varchar":"hi"`)
	require.Contains(t, s, `"c_json":{"a":1}`)
	require.Contains(t, s, `"c_f32":1.5`)
}

// TestCSVValueUnsupportedType ensures an unsupported column type errors.
func TestCSVValueUnsupportedType(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixed[types.Rowid](vec, types.Rowid{}, false, mp))
	defer vec.Free(mp)

	bat := batch.New([]string{"r"})
	bat.Vecs[0] = vec
	bat.SetRowCount(1)

	w := NewExternalWriter(nil, WriterConfig{Format: FormatCSV, Attrs: []string{"r"}}).(*externalWriter)
	_, err := w.encodeCSV(bat)
	require.Error(t, err)

	w2 := NewExternalWriter(nil, WriterConfig{Format: FormatJSONLine, Attrs: []string{"r"}}).(*externalWriter)
	_, err = w2.encodeJSONLine(bat)
	require.Error(t, err)
}

// TestAddEscape covers escape-char and enclosure-char doubling.
func TestAddEscape(t *testing.T) {
	// default escape '\\' with '"' enclosure
	require.Equal(t, `a\\b`, string(addEscape([]byte(`a\b`), '"', '\\')))
	require.Equal(t, `a""b`, string(addEscape([]byte(`a"b`), '"', '\\')))
	// enclosure == escape: doubled once, not twice
	require.Equal(t, `x\\y`, string(addEscape([]byte(`x\y`), '\\', '\\')))
	// nothing to escape
	require.Equal(t, `plain`, string(addEscape([]byte(`plain`), 0, '\\')))

	// custom escape char: it is doubled, backslash is left alone
	require.Equal(t, `a!!b`, string(addEscape([]byte(`a!b`), '"', '!')))
	require.Equal(t, `a\b`, string(addEscape([]byte(`a\b`), '"', '!')))
	require.Equal(t, `a""!!`, string(addEscape([]byte(`a"!`), '"', '!')))

	// escaping disabled (ESCAPED BY ''): only the enclosure is doubled
	require.Equal(t, `a\b`, string(addEscape([]byte(`a\b`), '"', 0)))
	require.Equal(t, `a""b`, string(addEscape([]byte(`a"b`), '"', 0)))
}

// TestColCount validates the leading-column accounting.
func TestColCount(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := testBatch(t, mp)
	defer bat.Clean(mp)

	// Attrs shorter than vecs -> only Attrs columns are written.
	w := NewExternalWriter(nil, WriterConfig{Attrs: []string{"id"}}).(*externalWriter)
	require.Equal(t, 1, w.colCount(bat))

	// Attrs longer than vecs -> clamp to len(vecs).
	w = NewExternalWriter(nil, WriterConfig{Attrs: []string{"a", "b", "c"}}).(*externalWriter)
	require.Equal(t, 2, w.colCount(bat))

	// No Attrs -> all vecs.
	w = NewExternalWriter(nil, WriterConfig{}).(*externalWriter)
	require.Equal(t, 2, w.colCount(bat))
}

// TestConstNullVector covers constant and constant-null vectors through the
// vec.IsNull check the encoders rely on.
func TestConstNullVector(t *testing.T) {
	mp := mpool.MustNewZero()

	cn := vector.NewConstNull(types.T_int64.ToType(), 3, mp)
	defer cn.Free(mp)
	require.True(t, cn.IsNull(0))
	require.True(t, cn.IsNull(2))

	cf, err := vector.NewConstFixed[int64](types.T_int64.ToType(), 7, 3, mp)
	require.NoError(t, err)
	defer cf.Free(mp)
	require.False(t, cf.IsNull(0))
	require.False(t, cf.IsNull(2))
}

// TestEncodeCSVConstVector confirms const vectors expand to every row.
func TestEncodeCSVConstVector(t *testing.T) {
	mp := mpool.MustNewZero()
	cf, err := vector.NewConstFixed[int64](types.T_int64.ToType(), 42, 3, mp)
	require.NoError(t, err)

	bat := batch.New([]string{"v"})
	bat.Vecs[0] = cf
	bat.SetRowCount(3)
	defer bat.Clean(mp)

	w := NewExternalWriter(nil, WriterConfig{Format: FormatCSV, Attrs: []string{"v"}}).(*externalWriter)
	out, err := w.encodeCSV(bat)
	require.NoError(t, err)
	require.Equal(t, "42\n42\n42\n", string(out))
}
