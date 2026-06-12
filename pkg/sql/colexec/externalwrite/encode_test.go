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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
