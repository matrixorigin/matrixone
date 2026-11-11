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
	"bytes"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/stretchr/testify/require"
)

func jsonTextSelectionFields(value JSONTextSelection) []any {
	return []any{value.Text, value.Type, value.Exists, value.InputNull, value.ValueNull, value.Multiple, value.InvalidPath, value.TextError}
}

func TestJSONReaderSynchronousTextSelection(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	var live uint64
	calls := 0
	admission := func(capacity uint64) (func(), error) {
		calls++
		live += capacity
		return func() { live -= capacity }, nil
	}
	builder, err := NewJSONReaderBuilder(mp, nil, admission)
	require.NoError(t, err)
	defer builder.Close()
	raw := jsonAdmissionValue(t, `{"keep":"owned text","items":[1,2],"null":null}`)
	require.NoError(t, builder.Append(raw, false))
	require.NoError(t, builder.Append(nil, true))
	reader, err := builder.Finish()
	require.NoError(t, err)
	cursor, err := reader.OpenCursor()
	require.NoError(t, err)
	before := calls
	for _, pathText := range []string{"$.keep", "$.missing", "$.null", "$.items[*]"} {
		path := *jsonReaderPath(t, pathText)
		want, err := cursor.QueryText(0, []byte(pathText), false)
		require.NoError(t, err)
		got, err := reader.QueryText(0, []byte(pathText), false)
		require.NoError(t, err)
		require.Equal(t, jsonTextSelectionFields(want), jsonTextSelectionFields(got))
		got.Close()
		got, err = reader.QueryTextAtPath(0, path, false)
		require.NoError(t, err)
		require.Equal(t, jsonTextSelectionFields(want), jsonTextSelectionFields(got))
		got.Close()
		want.Close()
	}
	owned, err := reader.QueryText(0, []byte("$.keep"), false)
	require.NoError(t, err)
	require.Equal(t, byte(bytejson.TpCodeString), owned.Type)
	private := reader.(*jsonReader).column.GetBytesAt(0)
	index := bytes.Index(private, []byte("owned text"))
	require.GreaterOrEqual(t, index, 0)
	original := private[index]
	private[index] = 'X'
	text := owned.Text
	private[index] = original
	require.Equal(t, "owned text", text, "returned text must not alias the private backing")
	null, err := reader.QueryText(1, []byte("invalid path"), false)
	require.NoError(t, err)
	require.True(t, null.InputNull)
	bad, err := reader.QueryText(0, []byte("invalid path"), false)
	require.Error(t, err)
	require.True(t, bad.InvalidPath)
	_, err = reader.QueryTextAtPath(2, *jsonReaderPath(t, "$.keep"), false)
	require.Error(t, err)
	require.Greater(t, calls, before, "owned text and query backing require admission")
	cursor.Close()
	reader.Close()
	clear(raw)
	require.Equal(t, "owned text", owned.Text)
	require.Positive(t, live, "the independent text retains its charge")
	owned.Close()
	require.Zero(t, live)
	_, err = reader.QueryText(0, []byte("$.keep"), false)
	require.ErrorIs(t, err, io.ErrClosedPipe)
}

func TestJSONReaderStringTextMatchesUnquote(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	builder, err := NewJSONReaderBuilder(mp, nil)
	require.NoError(t, err)
	defer builder.Close()
	literals := []string{`""`, `"plain"`, `"\"quoted\""`, `"slash\\line\n"`, `"猫🌍"`, `"\u0000"`}
	want := make([]string, len(literals))
	for i, literal := range literals {
		value, err := bytejson.ParseFromString(literal)
		require.NoError(t, err)
		want[i], err = value.Unquote()
		require.NoError(t, err)
		require.NoError(t, builder.Append(jsonAdmissionValue(t, `{"items":[`+literal+`]}`), false))
	}
	reader, err := builder.Finish()
	require.NoError(t, err)
	closed := false
	defer func() {
		if !closed {
			reader.Close()
		}
	}()
	path := *jsonReaderPath(t, "$.items[*]")
	owned := make([]JSONTextSelection, len(literals))
	for row := range literals {
		for _, legacyFloat := range []bool{false, true} {
			got, err := reader.QueryTextAtPath(row, path, legacyFloat)
			require.NoError(t, err)
			require.True(t, got.Exists)
			require.Equal(t, byte(bytejson.TpCodeString), got.Type)
			require.Equal(t, want[row], got.Text)
			if legacyFloat {
				got.Close()
			} else {
				owned[row] = got
			}
		}
	}
	reader.Close()
	closed = true
	for row := range owned {
		require.Equal(t, want[row], owned[row].Text, "selected strings remain owned after reader close")
		owned[row].Close()
	}
}
