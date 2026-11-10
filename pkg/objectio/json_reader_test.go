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
	"bytes"
	"context"
	"io"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/require"
)

func privateJSONColumnForTest(t *testing.T, source *vector.Vector) fscache.Data {
	t.Helper()
	raw := marshalLegacyTestColumn(t, source)
	constructor := privateJSONColumnCacheConstructorFactory(int64(len(raw)), compress.None)
	data, err := constructor(context.Background(), nil, raw, fileservice.DefaultCacheDataAllocator())
	require.NoError(t, err)
	_, ok := data.(*privateJSONVectorCacheData)
	require.True(t, ok)
	return data
}

// A cache validator can receive bytes from an arbitrary caller. Admission must
// not grant immutable-reader authority over that caller's writable storage.
func TestJSONCacheAdmissionDoesNotSealCallerBacking(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	source := vector.NewVec(types.T_json.ToType())
	defer source.Free(mp)
	doc, err := types.ParseStringToByteJson(`{"keep":"ORIGINAL"}`)
	require.NoError(t, err)
	require.NoError(t, vector.AppendByteJson(source, doc, false, mp))
	raw := marshalLegacyTestColumn(t, source)
	data := fileservice.NewBytes(raw)
	sealed, err := validateVectorCacheData(data)
	require.NoError(t, err)
	defer sealed.Release()
	reader, err := OpenJSONReader(sealed, mp, nil)
	require.NoError(t, err)
	defer reader.Close()
	pos := bytes.Index(raw, []byte("ORIGINAL"))
	require.GreaterOrEqual(t, pos, 0)
	copy(raw[pos:pos+len("ORIGINAL")], "MUTATED!")
	cursor, err := reader.OpenCursor()
	require.NoError(t, err)
	defer cursor.Close()
	result, err := cursor.QueryText(0, []byte("$.keep"), false)
	require.NoError(t, err)
	require.Equal(t, "ORIGINAL", result.Text)
	fresh, err := OpenJSONReader(sealed, mp, nil)
	require.NoError(t, err)
	defer fresh.Close()
	freshCursor, err := fresh.OpenCursor()
	require.NoError(t, err)
	defer freshCursor.Close()
	updated, err := freshCursor.QueryText(0, []byte("$.keep"), false)
	require.NoError(t, err)
	require.Equal(t, "MUTATED!", updated.Text)
	raw[pos] = 0xff
	invalid, err := OpenJSONReader(sealed, mp, nil)
	require.Error(t, err)
	require.Nil(t, invalid)
	again, err := cursor.QueryText(0, []byte("$.keep"), false)
	require.NoError(t, err)
	require.Equal(t, "ORIGINAL", again.Text)
}

func TestJSONCacheReaderRejectsMalformedColumnHeader(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	source := vector.NewVec(types.T_json.ToType())
	defer source.Free(mp)
	doc, err := types.ParseStringToByteJson(`{"keep":1}`)
	require.NoError(t, err)
	require.NoError(t, vector.AppendByteJson(source, doc, false, mp))
	valid := marshalLegacyTestColumn(t, source)
	for _, tc := range []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{name: "short", mutate: func(b []byte) []byte { return b[:1] }},
		{name: "type", mutate: func(b []byte) []byte { DecodeIOEntryHeader(b).Type = 0xffff; return b }},
		{name: "version", mutate: func(b []byte) []byte { DecodeIOEntryHeader(b).Version = 0xffff; return b }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := fileservice.NewBytes(tc.mutate(bytes.Clone(valid)))
			defer data.Release()
			require.NotPanics(t, func() {
				reader, err := OpenJSONReader(data, mp, nil)
				require.Error(t, err)
				require.Nil(t, reader)
			})
		})
	}
}

// Rehoming is a public callback boundary. Neither the callback's input nor an
// alias to its returned data may modify an already admitted immutable version.
func TestJSONCacheRehomeDoesNotExposeAdmittedBacking(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	source := vector.NewVec(types.T_json.ToType())
	defer source.Free(mp)
	doc, err := types.ParseStringToByteJson(`{"keep":"ORIGINAL"}`)
	require.NoError(t, err)
	require.NoError(t, vector.AppendByteJson(source, doc, false, mp))
	data := privateJSONColumnForTest(t, source)
	defer data.Release()
	reader, err := OpenJSONReader(data, mp, nil)
	require.NoError(t, err)
	defer reader.Close()
	// A full-range Slice must not reveal the embedded general marker.
	require.Same(t, data, data.Slice(int(data.Size())))
	var callbackInput, callbackOutput []byte
	rehomed := data.(fscache.DataOwnership).RehomeCacheData(func(value []byte) fscache.Data {
		callbackInput = value
		callbackOutput = bytes.Clone(value)
		return fileservice.NewBytes(callbackOutput)
	})
	defer rehomed.Release()
	rehomedReader, err := OpenJSONReader(rehomed, mp, nil)
	require.NoError(t, err)
	defer rehomedReader.Close()
	for _, alias := range [][]byte{callbackInput, callbackOutput} {
		pos := bytes.Index(alias, []byte("ORIGINAL"))
		require.GreaterOrEqual(t, pos, 0)
		copy(alias[pos:pos+len("ORIGINAL")], "MUTATED!")
	}
	for _, current := range []vector.JSONReader{reader, rehomedReader} {
		cursor, err := current.OpenCursor()
		require.NoError(t, err)
		result, err := cursor.QueryText(0, []byte("$.keep"), false)
		cursor.Close()
		require.NoError(t, err)
		require.Equal(t, "ORIGINAL", result.Text)
	}
}

type aliasingCacheAllocator struct{ calls int }

func (a *aliasingCacheAllocator) BackingSize(size int) int { a.calls++; return size }
func (a *aliasingCacheAllocator) AllocateCacheData(context.Context, int) fscache.Data {
	a.calls++
	return fileservice.NewBytes(nil)
}
func (a *aliasingCacheAllocator) AllocateCacheDataWithHint(context.Context, int, malloc.Hints) fscache.Data {
	a.calls++
	return fileservice.NewBytes(nil)
}
func (a *aliasingCacheAllocator) CopyToCacheData(_ context.Context, raw []byte) fscache.Data {
	a.calls++
	return fileservice.NewBytes(raw)
}

func TestJSONCachePrivateConstructorRejectsAliasingAllocator(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	source := vector.NewVec(types.T_json.ToType())
	defer source.Free(mp)
	doc, err := types.ParseStringToByteJson(`{"keep":"ORIGINAL"}`)
	require.NoError(t, err)
	require.NoError(t, vector.AppendByteJson(source, doc, false, mp))
	raw := marshalLegacyTestColumn(t, source)
	allocator := new(aliasingCacheAllocator)
	constructor := privateJSONColumnCacheConstructorFactory(int64(len(raw)), compress.None)
	data, err := constructor(context.Background(), nil, raw, allocator)
	require.NoError(t, err)
	defer data.Release()
	require.Zero(t, allocator.calls)
	reader, err := OpenJSONReader(data, mp, nil)
	require.NoError(t, err)
	defer reader.Close()
	pos := bytes.Index(raw, []byte("ORIGINAL"))
	require.GreaterOrEqual(t, pos, 0)
	copy(raw[pos:pos+len("ORIGINAL")], "MUTATED!")
	view := data.Bytes()
	pos = bytes.Index(view, []byte("ORIGINAL"))
	require.GreaterOrEqual(t, pos, 0)
	copy(view[pos:pos+len("ORIGINAL")], "MUTATED!")
	cursor, err := reader.OpenCursor()
	require.NoError(t, err)
	defer cursor.Close()
	value, err := cursor.QueryText(0, []byte("$.keep"), false)
	require.NoError(t, err)
	require.Equal(t, "ORIGINAL", value.Text)
}

func TestCacheJSONReaderIndependentConcurrentReaders(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	source := vector.NewVec(types.T_json.ToType())
	defer source.Free(mp)
	doc, err := types.ParseStringToByteJson(`{"keep":"ORIGINAL"}`)
	require.NoError(t, err)
	require.NoError(t, vector.AppendByteJson(source, doc, false, mp))
	data := privateJSONColumnForTest(t, source)
	reader, err := OpenJSONReader(data, mp, nil)
	require.NoError(t, err)
	data.Release()
	start := make(chan struct{})
	results := make(chan error, 2)
	var done sync.WaitGroup
	for range 2 {
		view, err := reader.Window(0, 1)
		require.NoError(t, err)
		done.Add(1)
		go func() {
			defer done.Done()
			defer view.Close()
			<-start
			cursor, err := view.OpenCursor()
			if err == nil {
				var value vector.JSONTextSelection
				value, err = cursor.QueryText(0, []byte("$.keep"), false)
				if err == nil && value.Text != "ORIGINAL" {
					err = io.ErrUnexpectedEOF
				}
				cursor.Close()
			}
			results <- err
		}()
	}
	reader.Close()
	close(start)
	done.Wait()
	for range 2 {
		require.NoError(t, <-results)
	}
}

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
				data.Release()
				data = privateJSONColumnForTest(t, source)
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
	data := privateJSONColumnForTest(t, source)
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
