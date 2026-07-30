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
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/stretchr/testify/require"
)

func testBatch(t *testing.T, mp *mpool.MPool) *batch.Batch {
	bat := batch.New([]string{"id", "name"})

	idVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](idVec, 1, false, mp))
	require.NoError(t, vector.AppendFixed[int64](idVec, 2, true, mp)) // null
	bat.Vecs[0] = idVec

	nameVec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(nameVec, []byte("alice"), false, mp))
	require.NoError(t, vector.AppendBytes(nameVec, []byte("bob"), false, mp))
	bat.Vecs[1] = nameVec

	bat.SetRowCount(2)
	return bat
}

func TestEncodeCSV(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := testBatch(t, mp)
	defer bat.Clean(mp)

	w := NewExternalWriter(nil, WriterConfig{
		Format: FormatCSV,
		Attrs:  []string{"id", "name"},
		Stmt:   time.Now(),
	}).(*externalWriter)

	out, err := w.encodeCSV(bat)
	require.NoError(t, err)
	// Strings are quoted with the default '"' enclosure (the reader's default).
	require.Equal(t, "1,\"alice\"\n\\N,\"bob\"\n", string(out))
}

func TestEncodeJSONLine(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := testBatch(t, mp)
	defer bat.Clean(mp)

	w := NewExternalWriter(nil, WriterConfig{
		Format: FormatJSONLine,
		Attrs:  []string{"id", "name"},
		Stmt:   time.Now(),
	}).(*externalWriter)

	out, err := w.encodeJSONLine(bat)
	require.NoError(t, err)
	require.Equal(t, "{\"id\":1,\"name\":\"alice\"}\n{\"id\":null,\"name\":\"bob\"}\n", string(out))
}

// TestEncodeJSONLineLineTerminator: the configured LINES TERMINATED BY value
// separates records (the reader parses jsonline with the same terminator).
func TestEncodeJSONLineLineTerminator(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := testBatch(t, mp)
	defer bat.Clean(mp)

	w := NewExternalWriter(nil, WriterConfig{
		Format:         FormatJSONLine,
		Attrs:          []string{"id", "name"},
		LineTerminator: []byte("\r\n"),
		Stmt:           time.Now(),
	}).(*externalWriter)

	out, err := w.encodeJSONLine(bat)
	require.NoError(t, err)
	require.Equal(t, "{\"id\":1,\"name\":\"alice\"}\r\n{\"id\":null,\"name\":\"bob\"}\r\n", string(out))
}

// TestEncodeLineStartingBy: LINES STARTING BY prefixes every record in both
// formats (the reader strips the prefix when parsing).
func TestEncodeLineStartingBy(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := testBatch(t, mp)
	defer bat.Clean(mp)

	w := NewExternalWriter(nil, WriterConfig{
		Format:         FormatCSV,
		Attrs:          []string{"id", "name"},
		LineStartingBy: []byte("row:"),
		Stmt:           time.Now(),
	}).(*externalWriter)
	out, err := w.encodeCSV(bat)
	require.NoError(t, err)
	require.Equal(t, "row:1,\"alice\"\nrow:\\N,\"bob\"\n", string(out))

	w2 := NewExternalWriter(nil, WriterConfig{
		Format:         FormatJSONLine,
		Attrs:          []string{"id", "name"},
		LineStartingBy: []byte("row:"),
		Stmt:           time.Now(),
	}).(*externalWriter)
	out, err = w2.encodeJSONLine(bat)
	require.NoError(t, err)
	require.Equal(t, "row:{\"id\":1,\"name\":\"alice\"}\nrow:{\"id\":null,\"name\":\"bob\"}\n", string(out))
}

// TestEncodeCSVCommentGuard: when a COMMENT marker is configured, the writer
// encloses the first field of any row whose unenclosed line prefix would match
// the marker (so the reader does not skip it as a comment) — and ONLY then.
// The collision matters for UNENCLOSED types (string-like columns are always
// enclosed, so they can never begin a line with a bare marker), matching the
// reviewer's comment='1' / first-field-serializes-as-1 example. The encoded
// output must round-trip through the reader with all rows preserved.
func TestEncodeCSVCommentGuard(t *testing.T) {
	mp := mpool.MustNewZero()

	// first column is an integer (written unenclosed); 13 and 135 begin with the
	// marker '13', while 1 and 99 do not.
	bat := batch.New([]string{"a", "b"})
	aVec := vector.NewVec(types.T_int64.ToType())
	for _, v := range []int64{13, 1, 135, 99} {
		require.NoError(t, vector.AppendFixed[int64](aVec, v, false, mp))
	}
	bat.Vecs[0] = aVec
	bVec := vector.NewVec(types.T_int64.ToType())
	for _, v := range []int64{1, 2, 3, 4} {
		require.NoError(t, vector.AppendFixed[int64](bVec, v, false, mp))
	}
	bat.Vecs[1] = bVec
	bat.SetRowCount(4)
	defer bat.Clean(mp)

	w := NewExternalWriter(nil, WriterConfig{
		Format:  FormatCSV,
		Attrs:   []string{"a", "b"},
		Comment: []byte("13"),
		Stmt:    time.Now(),
	}).(*externalWriter)
	out, err := w.encodeCSV(bat)
	require.NoError(t, err)
	// 13 and 135 start with the marker '13' and are enclosed; 1 and 99 are left
	// bare — only the real collisions are quoted.
	require.Equal(t, "\"13\",1\n1,2\n\"135\",3\n99,4\n", string(out))

	// round-trip: every written row must read back through the parser
	requireRoundTrip(t, "13", out, []string{"13", "1", "135", "99"})

	// no marker => never quote the integer column (regression guard)
	w2 := NewExternalWriter(nil, WriterConfig{
		Format: FormatCSV,
		Attrs:  []string{"a", "b"},
		Stmt:   time.Now(),
	}).(*externalWriter)
	out, err = w2.encodeCSV(bat)
	require.NoError(t, err)
	require.Equal(t, "13,1\n1,2\n135,3\n99,4\n", string(out))
}

// requireRoundTrip parses encoded CSV with the given comment marker and asserts
// the first-column values match want in order (no row dropped as a comment).
func requireRoundTrip(t *testing.T, comment string, encoded []byte, want []string) {
	t.Helper()
	cfg := csvparser.CSVConfig{FieldsTerminatedBy: ",", FieldsEnclosedBy: `"`, FieldsEscapedBy: `\`, Comment: comment}
	p, err := csvparser.NewCSVParser(&cfg, strings.NewReader(string(encoded)), csvparser.ReadBlockSize, false)
	require.NoError(t, err)
	var got []string
	for {
		row, rerr := p.Read(nil)
		if rerr != nil {
			break
		}
		got = append(got, string(append([]byte(nil), row[0].Val...)))
	}
	require.Equal(t, want, got)
}

// TestEncodeJSONLineNoAttrs: jsonline cannot emit objects without key names.
func TestEncodeJSONLineNoAttrs(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := testBatch(t, mp)
	defer bat.Clean(mp)

	w := NewExternalWriter(nil, WriterConfig{Format: FormatJSONLine}).(*externalWriter)
	_, err := w.encodeJSONLine(bat)
	require.Error(t, err)
	require.Contains(t, err.Error(), "column names")
}

// TestEncodeCSVCustomEscape: a custom FIELDS ESCAPED BY char is doubled in
// every (non-null) field — including unquoted ones, since the reader
// unescapes those too — and backslash is no longer special.
func TestEncodeCSVCustomEscape(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"id", "name"})
	bat.Vecs[0] = func() *vector.Vector {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed[int64](vec, 7, false, mp))
		return vec
	}()
	bat.Vecs[1] = func() *vector.Vector {
		vec := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(vec, []byte(`bang!and\slash`), false, mp))
		return vec
	}()
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	w := NewExternalWriter(nil, WriterConfig{
		Format:    FormatCSV,
		Attrs:     []string{"id", "name"},
		EscapedBy: '!',
		Stmt:      time.Now(),
	}).(*externalWriter)
	out, err := w.encodeCSV(bat)
	require.NoError(t, err)
	require.Equal(t, "7,\"bang!!and\\slash\"\n", string(out))

	// ESCAPED BY '': nothing escaped beyond enclosure doubling.
	w2 := NewExternalWriter(nil, WriterConfig{
		Format:   FormatCSV,
		Attrs:    []string{"id", "name"},
		NoEscape: true,
		Stmt:     time.Now(),
	}).(*externalWriter)
	out, err = w2.encodeCSV(bat)
	require.NoError(t, err)
	require.Equal(t, "7,\"bang!and\\slash\"\n", string(out))
}

// TestAppendJSONString covers the escaping rules of the direct JSON encoder.
func TestAppendJSONString(t *testing.T) {
	enc := func(s string) string {
		var b bytes.Buffer
		appendJSONString(&b, []byte(s))
		return b.String()
	}
	require.Equal(t, `"plain"`, enc("plain"))
	require.Equal(t, `"a\"b"`, enc(`a"b`))
	require.Equal(t, `"a\\b"`, enc(`a\b`))
	require.Equal(t, `"x\ny\r\t"`, enc("x\ny\r\t"))
	require.Equal(t, `"c\u0001d"`, enc("c\x01d"))
	require.Equal(t, `"héllo"`, enc("héllo")) // multi-byte UTF-8 passes through
}

// TestAddEscapeNoCopy: the no-escape fast path returns the input slice itself.
func TestAddEscapeNoCopy(t *testing.T) {
	s := []byte("nothing-to-escape")
	out := addEscape(s, '"', '\\')
	require.Equal(t, &s[0], &out[0])
}

// TestAppendJSONFloat pins json-compat float formatting: shortest 'f' form,
// 'e' outside [1e-6, 1e21) with the exponent trimmed like encoding/json, the
// float32 bits path, and the NaN/Inf error.
func TestAppendJSONFloat(t *testing.T) {
	w := NewExternalWriter(nil, WriterConfig{}).(*externalWriter)
	enc := func(f float64, bits int) string {
		var b bytes.Buffer
		require.NoError(t, w.appendJSONFloat(&b, f, bits))
		return b.String()
	}

	// property check: byte-identical with encoding/json across the regimes
	for _, f := range []float64{0, 1.5, -2.25, 1e-9, -1e-9, 1e-6, 9.99e-7, 1e21, -3e21, 123456789.125, 1e20} {
		ref, err := json.Marshal(f)
		require.NoError(t, err)
		require.Equal(t, string(ref), enc(f, 64), "float64 %v", f)
	}
	for _, f := range []float32{0, 1.5, -2.25, 1e-9, 1e21, 3.4e38} {
		ref, err := json.Marshal(f)
		require.NoError(t, err)
		require.Equal(t, string(ref), enc(float64(f), 32), "float32 %v", f)
	}

	// the e-09 -> e-9 trim fires
	require.Equal(t, "1e-9", enc(1e-9, 64))

	// NaN / Inf are rejected like encoding/json
	var b bytes.Buffer
	require.Error(t, w.appendJSONFloat(&b, math.NaN(), 64))
	require.Error(t, w.appendJSONFloat(&b, math.Inf(1), 64))
}

// TestEncodeRoundTripGuards: values no encoding can round-trip are rejected,
// not silently corrupted.
func TestEncodeRoundTripGuards(t *testing.T) {
	mp := mpool.MustNewZero()
	mkBat := func(s string) *batch.Batch {
		bat := batch.New([]string{"b"})
		vec := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(vec, []byte(s), false, mp))
		bat.Vecs[0] = vec
		bat.SetRowCount(1)
		return bat
	}

	// CSV: literal \N is fine under the default escape (written \\N) ...
	bat := mkBat(`\N`)
	defer bat.Clean(mp)
	w := NewExternalWriter(nil, WriterConfig{Format: FormatCSV, Attrs: []string{"b"}}).(*externalWriter)
	out, err := w.encodeCSV(bat)
	require.NoError(t, err)
	require.Equal(t, "\"\\\\N\"\n", string(out))

	// ... but errors under a custom or disabled escape (the reader null-matches
	// even enclosed fields outside the backslash flavor)
	w2 := NewExternalWriter(nil, WriterConfig{Format: FormatCSV, Attrs: []string{"b"}, EscapedBy: '!'}).(*externalWriter)
	_, err = w2.encodeCSV(bat)
	require.Error(t, err)
	w3 := NewExternalWriter(nil, WriterConfig{Format: FormatCSV, Attrs: []string{"b"}, NoEscape: true}).(*externalWriter)
	_, err = w3.encodeCSV(bat)
	require.Error(t, err)

	// jsonline: \N never round-trips (the reader compares decoded strings
	// against the null token), and invalid UTF-8 would be rewritten to U+FFFD
	wj := NewExternalWriter(nil, WriterConfig{Format: FormatJSONLine, Attrs: []string{"b"}}).(*externalWriter)
	_, err = wj.encodeJSONLine(bat)
	require.Error(t, err)
	batBad := mkBat("x\xffy")
	defer batBad.Clean(mp)
	_, err = wj.encodeJSONLine(batBad)
	require.Error(t, err)
	require.Contains(t, err.Error(), "UTF-8")

	// with ESCAPED BY '', a trailing CR in the last column cannot survive the
	// reader's end-of-record CR strip
	batCR := mkBat("abc\r")
	defer batCR.Clean(mp)
	_, err = w3.encodeCSV(batCR)
	require.Error(t, err)
	// with escaping enabled it is rewritten to E+'r' and round-trips
	out, err = w.encodeCSV(batCR)
	require.NoError(t, err)
	require.Equal(t, "\"abc\\r\"\n", string(out))
}

// TestNeedsEnclosureBoundary: a value whose suffix is a proper prefix of a
// multi-char field terminator must be enclosed (the reader matches the
// terminator across the value/terminator boundary).
func TestNeedsEnclosureBoundary(t *testing.T) {
	w := NewExternalWriter(nil, WriterConfig{FieldTerminator: []byte("00")}).(*externalWriter)
	require.True(t, w.needsEnclosure([]byte("10")))  // suffix '0' is a prefix of '00'
	require.True(t, w.needsEnclosure([]byte("100"))) // contains '00'
	require.False(t, w.needsEnclosure([]byte("12"))) // no overlap
	require.False(t, w.needsEnclosure([]byte("01"))) // prefix at start is harmless

	w2 := NewExternalWriter(nil, WriterConfig{FieldTerminator: []byte("||")}).(*externalWriter)
	require.True(t, w2.needsEnclosure([]byte("pipe|")))
	require.False(t, w2.needsEnclosure([]byte("plain")))
}
