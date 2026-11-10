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

package objectio

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/require"
)

func TestOpenJSONReaderCacheLifetime(t *testing.T) {
	for _, sealed := range []bool{false, true} {
		mp := mpool.MustNewZero()
		func() {
			defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
			source := vector.NewVec(types.T_json.ToType())
			defer source.Free(mp)
			doc, err := types.ParseStringToByteJson(`{"keep":1,"large":[0,0]}`)
			require.NoError(t, err)
			require.NoError(t, vector.AppendByteJson(source, doc, false, mp))
			var data fscache.Data = fileservice.NewBytes(marshalLegacyTestColumn(t, source))
			if sealed {
				validated, err := validateVectorCacheData(data)
				require.NoError(t, err)
				data = validated
			}
			before := mp.CurrNB()
			reader, err := OpenJSONReader(data, mp, nil)
			data.Release()
			require.NoError(t, err)
			if sealed {
				require.Equal(t, before, mp.CurrNB(), "sealed cache input must not copy the column")
			}
			defer reader.Close()
			path, err := types.ParseStringToPath("$.keep")
			require.NoError(t, err)
			result, err := reader.Query(0, &path)
			require.NoError(t, err)
			defer result.Close()
			text, err := result.Text()
			require.NoError(t, err)
			require.Equal(t, "1", text)
		}()
	}
}

func TestCacheJSONReaderMatchesPrivateReader(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	source := vector.NewVec(types.T_json.ToType())
	defer source.Free(mp)
	doc, err := types.ParseStringToByteJson(`{"keep":1,"arr":[1,2],"nil":null,"one":[3],"flt":0.000031}`)
	require.NoError(t, err)
	require.NoError(t, vector.AppendByteJson(source, doc, false, mp))
	require.NoError(t, vector.AppendBytes(source, nil, true, mp))
	private, err := vector.NewJSONReaderSnapshot(source, mp)
	require.NoError(t, err)
	defer private.Close()
	data, err := validateVectorCacheData(fileservice.NewBytes(marshalLegacyTestColumn(t, source)))
	require.NoError(t, err)
	cache, err := OpenJSONReader(data, mp, nil)
	data.Release()
	require.NoError(t, err)
	defer cache.Close()
	for _, text := range []string{"$", "$.keep", "$.arr", "$.nil", "$.missing", "$.arr[*]", "$.one[*]", "$.flt"} {
		path, err := types.ParseStringToPath(text)
		require.NoError(t, err)
		for row := range 2 {
			a, err := private.Query(row, &path)
			require.NoError(t, err)
			b, err := cache.Query(row, &path)
			require.NoError(t, err)
			require.Equal(t, a.Exists(), b.Exists(), text)
			require.Equal(t, a.SQLNull(), b.SQLNull(), text)
			require.Equal(t, a.Type(), b.Type(), text)
			require.Equal(t, a.IsJSONNull(), b.IsJSONNull(), text)
			if a.Exists() && !a.SQLNull() {
				at, ae := a.Text()
				bt, be := b.Text()
				require.NoError(t, ae)
				require.NoError(t, be)
				require.Equal(t, at, bt, text)
				av, aok := a.Int64()
				bv, bok := b.Int64()
				require.Equal(t, av, bv)
				require.Equal(t, aok, bok)
			}

			ac, err := private.OpenCursor()
			require.NoError(t, err)
			bc, err := cache.OpenCursor()
			require.NoError(t, err)
			require.NoError(t, ac.ResetQuery(row, path))
			require.NoError(t, bc.ResetQuery(row, path))
			for _, legacyFloat := range []bool{false, true} {
				left, err := ac.QueryText(row, []byte(text), legacyFloat)
				require.NoError(t, err)
				right, err := bc.QueryText(row, []byte(text), legacyFloat)
				require.NoError(t, err)
				require.Equal(t, left, right, text)
				again, err := bc.QueryText(row, []byte(text), legacyFloat)
				require.NoError(t, err)
				require.Equal(t, right, again, "selection must leave the query result unchanged")
				switch {
				case row == 1:
					require.True(t, right.InputNull)
				case text == "$.missing":
					require.False(t, right.Exists)
				case text == "$.nil":
					require.True(t, right.ValueNull)
				case text == "$.arr[*]":
					require.True(t, right.Multiple)
				case text == "$.one[*]":
					require.Equal(t, "3", right.Text)
				default:
					require.True(t, right.Exists)
					require.False(t, right.Multiple)
				}
			}
			ac.Close()
			bc.Close()
			_, err = bc.QueryText(row, []byte(text), false)
			require.Error(t, err)
			a.Close()
			b.Close()
		}
	}
	cursor, err := cache.OpenCursor()
	require.NoError(t, err)
	cursorPath, err := types.ParseStringToPath("$.keep")
	require.NoError(t, err)
	require.NoError(t, cursor.ResetQuery(0, cursorPath))
	cursorText, err := cursor.Text()
	require.NoError(t, err)
	require.Equal(t, "1", cursorText)
	require.NoError(t, cursor.ResetQuery(1, cursorPath))
	require.True(t, cursor.SQLNull())
	require.False(t, cursor.Exists())
	require.Error(t, cursor.ResetQuery(2, cursorPath))
	require.False(t, cursor.SQLNull())
	require.False(t, cursor.Exists())
	cursor.Close()
	selected, err := cache.Select([]int64{1, 0}, mp)
	require.NoError(t, err)
	defer selected.Close()
	window, err := selected.Window(1, 2)
	require.NoError(t, err)
	defer window.Close()
	cache.Close()
	selected.Close()
	path, err := types.ParseStringToPath("$.keep")
	require.NoError(t, err)
	result, err := window.Query(0, &path)
	require.NoError(t, err)
	window.Close()
	text, err := result.Text()
	result.Close()
	require.NoError(t, err)
	require.Equal(t, "1", text)
}
