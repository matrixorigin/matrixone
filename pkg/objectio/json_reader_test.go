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
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
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

func testJSONCachePinAdmission(t *testing.T) JSONCachePinAdmission {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, 256)
	require.NoError(t, err)
	account, err := registry.Open(1 << 30)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, account.Seal().Used)
		require.Zero(t, registry.LiveAllocationMetadata())
		_, err := registry.Finalize(account)
		require.NoError(t, err)
	})
	return func(capacity uint64) (func(), error) {
		reservation, err := account.ReserveCapacity(capacity, 1, 1)
		if err != nil {
			return nil, err
		}
		lease, err := reservation.Commit(capacity)
		if err != nil {
			reservation.Abort()
			return nil, err
		}
		return lease.Release, nil
	}
}

func cacheJSONTextFields(value vector.JSONTextSelection) []any {
	return []any{value.Text, value.Type, value.Exists, value.InputNull, value.ValueNull, value.Multiple, value.InvalidPath, value.TextError}
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
	result.Close()
	fresh, err := OpenJSONReader(sealed, mp, nil)
	require.NoError(t, err)
	defer fresh.Close()
	freshCursor, err := fresh.OpenCursor()
	require.NoError(t, err)
	defer freshCursor.Close()
	updated, err := freshCursor.QueryText(0, []byte("$.keep"), false)
	require.NoError(t, err)
	require.Equal(t, "MUTATED!", updated.Text)
	updated.Close()
	raw[pos] = 0xff
	invalid, err := OpenJSONReader(sealed, mp, nil)
	require.Error(t, err)
	require.Nil(t, invalid)
	again, err := cursor.QueryText(0, []byte("$.keep"), false)
	require.NoError(t, err)
	require.Equal(t, "ORIGINAL", again.Text)
	again.Close()
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
	reader, err := OpenJSONReader(data, mp, nil, testJSONCachePinAdmission(t))
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
		result.Close()
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
	reader, err := OpenJSONReader(data, mp, nil, testJSONCachePinAdmission(t))
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
	value.Close()
}

func TestCacheJSONReaderIndependentConcurrentReaders(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	source := vector.NewVec(types.T_json.ToType())
	defer source.Free(mp)
	doc, err := types.ParseStringToByteJson(`{"keep":"ORIGINAL","number":123}`)
	require.NoError(t, err)
	require.NoError(t, vector.AppendByteJson(source, doc, false, mp))
	data := privateJSONColumnForTest(t, source)
	reader, err := OpenJSONReader(data, mp, nil, testJSONCachePinAdmission(t))
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
				value.Close()
				cursor.Close()
			}
			if err == nil {
				var value vector.JSONTextSelection
				value, err = view.QueryText(0, []byte("$.keep"), false)
				if err == nil && value.Text != "ORIGINAL" {
					err = io.ErrUnexpectedEOF
				}
				value.Close()
			}
			if err == nil {
				localMP := mpool.MustNewZero()
				output := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), localMP)
				err = output.PreExtendAndReset(1)
				if err == nil {
					var value vector.JSONTextSelection
					var appended bool
					value, appended, err = view.(vector.JSONDirectTextReader).QueryTextTo(0, []byte("$.number"), false, 512, vector.MustFunctionResult[types.Varlena](output))
					value.Close()
					if err == nil && (!appended || string(output.GetResultVector().GetBytesAt(0)) != "123") {
						err = io.ErrUnexpectedEOF
					}
				}
				output.Free()
				mpool.DeleteMPool(localMP)
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

func TestCacheJSONReaderSynchronousTextSelection(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	source := vector.NewVec(types.T_json.ToType())
	defer source.Free(mp)
	doc, err := types.ParseStringToByteJson(`{"keep":"owned text","items":[1,2],"null":null}`)
	require.NoError(t, err)
	require.NoError(t, vector.AppendByteJson(source, doc, false, mp))
	require.NoError(t, vector.AppendBytes(source, nil, true, mp))
	data := privateJSONColumnForTest(t, source)
	defer data.Release()
	base := testJSONCachePinAdmission(t)
	calls := 0
	admission := func(capacity uint64) (func(), error) {
		calls++
		return base(capacity)
	}
	reader, err := OpenJSONReader(data, mp, nil, admission)
	require.NoError(t, err)
	cursor, err := reader.OpenCursor()
	require.NoError(t, err)
	before := calls
	for _, pathText := range []string{"$.keep", "$.missing", "$.null", "$.items[*]"} {
		path, e := types.ParseStringToPath(pathText)
		require.NoError(t, e)
		want, e := cursor.QueryText(0, []byte(pathText), false)
		require.NoError(t, e)
		got, e := reader.QueryText(0, []byte(pathText), false)
		require.NoError(t, e)
		require.Equal(t, cacheJSONTextFields(want), cacheJSONTextFields(got))
		got.Close()
		got, e = reader.QueryTextAtPath(0, path, false)
		require.NoError(t, e)
		require.Equal(t, cacheJSONTextFields(want), cacheJSONTextFields(got))
		got.Close()
		want.Close()
	}
	owned, err := reader.QueryText(0, []byte("$.keep"), false)
	require.NoError(t, err)
	require.Equal(t, byte(bytejson.TpCodeString), owned.Type)
	private := reader.(*cacheJSONReader).column.GetBytesAt(0)
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
	require.Greater(t, calls, before)
	cursor.Close()
	reader.Close()
	clear(doc.Data)
	require.Equal(t, "owned text", owned.Text)
	owned.Close()
	_, err = reader.QueryText(0, []byte("$.keep"), false)
	require.ErrorIs(t, err, io.ErrClosedPipe)
}

func TestCacheJSONReaderStringTextMatchesUnquote(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	source := vector.NewVec(types.T_json.ToType())
	defer source.Free(mp)
	literals := []string{`""`, `"plain"`, `"\"quoted\""`, `"slash\\line\n"`, `"猫🌍"`, `"\u0000"`}
	want := make([]string, len(literals))
	for i, literal := range literals {
		value, err := types.ParseStringToByteJson(literal)
		require.NoError(t, err)
		want[i], err = value.Unquote()
		require.NoError(t, err)
		doc, err := types.ParseStringToByteJson(`{"items":[` + literal + `]}`)
		require.NoError(t, err)
		require.NoError(t, vector.AppendByteJson(source, doc, false, mp))
	}
	data := privateJSONColumnForTest(t, source)
	dataReleased := false
	defer func() {
		if !dataReleased {
			data.Release()
		}
	}()
	reader, err := OpenJSONReader(data, mp, nil, testJSONCachePinAdmission(t))
	require.NoError(t, err)
	closed := false
	defer func() {
		if !closed {
			reader.Close()
		}
	}()
	path, err := types.ParseStringToPath("$.items[*]")
	require.NoError(t, err)
	owned := make([]vector.JSONTextSelection, len(literals))
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
	data.Release()
	dataReleased = true
	for row := range owned {
		require.Equal(t, want[row], owned[row].Text, "selected strings remain owned after reader close")
		owned[row].Close()
	}
}

func TestCacheJSONReaderAccountsPinnedBacking(t *testing.T) {
	for _, insufficient := range []bool{false, true} {
		t.Run(fmt.Sprintf("insufficient=%v", insufficient), func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
			source := vector.NewVec(types.T_json.ToType())
			defer source.Free(mp)
			doc, err := types.ParseStringToByteJson(`{"keep":"ORIGINAL"}`)
			require.NoError(t, err)
			require.NoError(t, vector.AppendByteJson(source, doc, false, mp))
			data := privateJSONColumnForTest(t, source)
			defer func() {
				if data != nil {
					data.Release()
				}
			}()
			capacity := uint64(data.Capacity())
			require.Positive(t, capacity)
			ownerMeta := vector.JSONReaderMetadataCapacity(reflect.TypeFor[vector.Vector]().Size() + reflect.TypeFor[vector.RefCountedBufferLease]().Size())
			handleMeta := vector.JSONReaderMetadataCapacity(reflect.TypeFor[cacheJSONReader]().Size())
			cursorMeta := vector.JSONReaderMetadataCapacity(reflect.TypeFor[cacheJSONCursor]().Size())
			registry, err := mpool.NewAllocationAccountRegistry(1, 256)
			require.NoError(t, err)
			limit := capacity + ownerMeta + 2*handleMeta + cursorMeta
			if insufficient {
				limit = capacity - 1
			}
			account, err := registry.Open(limit)
			require.NoError(t, err)
			defer func() {
				require.Zero(t, account.Seal().Used)
				require.Zero(t, registry.LiveAllocationMetadata())
				_, err := registry.Finalize(account)
				require.NoError(t, err)
			}()
			selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
			require.NoError(t, err)
			fallbackCalled := false
			reader, err := OpenJSONReader(data, mp, selection, func(uint64) (func(), error) {
				fallbackCalled = true
				return func() {}, nil
			})
			require.False(t, fallbackCalled, "an explicit account must not be charged twice")
			if reader != nil {
				defer reader.Close()
			}
			if insufficient {
				require.Error(t, err)
				require.Nil(t, reader)
				require.Zero(t, account.Snapshot().Used)
				require.Positive(t, data.Size(), "failed admission must retain the original owner")
				data.Release()
				data = nil
				return
			}
			require.NoError(t, err)
			require.Equal(t, capacity+ownerMeta+handleMeta, account.Snapshot().Used)
			view, err := reader.Window(0, 1)
			require.NoError(t, err)
			defer view.Close()
			require.Equal(t, capacity+ownerMeta+2*handleMeta, account.Snapshot().Used)
			cursor, err := view.OpenCursor()
			require.NoError(t, err)
			defer cursor.Close()
			require.Equal(t, capacity+ownerMeta+2*handleMeta+cursorMeta, account.Snapshot().Used)
			data.Release()
			data = nil
			reader.Close()
			view.Close()
			require.Equal(t, capacity+ownerMeta+cursorMeta, account.Snapshot().Used, "a surviving cursor retains its metadata and the one pin charge")
			selected, err := cursor.QueryText(0, []byte("$.keep"), false)
			require.NoError(t, err)
			require.Equal(t, "ORIGINAL", selected.Text)
			selected.Close()
			cursor.Close()
			require.Zero(t, account.Snapshot().Used)
		})
	}
}

func TestCacheJSONReaderPinAdmissionFailureAndSnapshotFallback(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	source := vector.NewVec(types.T_json.ToType())
	defer source.Free(mp)
	doc, err := types.ParseStringToByteJson(`{"keep":1}`)
	require.NoError(t, err)
	require.NoError(t, vector.AppendByteJson(source, doc, false, mp))
	data := privateJSONColumnForTest(t, source)
	defer data.Release()
	releases := 0
	reader, err := OpenJSONReader(data, mp, nil, func(uint64) (func(), error) {
		return func() { releases++ }, context.Canceled
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, reader)
	require.Equal(t, 1, releases, "failed pin admission must release partial capacity")
	before := mp.CurrNB()
	reader, err = OpenJSONReader(data, mp, nil)
	require.NoError(t, err)
	require.Greater(t, mp.CurrNB(), before, "an unaccounted pin must use an owned snapshot")
	reader.Close()
	require.Equal(t, before, mp.CurrNB())
}

func TestCacheJSONReaderMetadataAdmissionFailureIsAtomic(t *testing.T) {
	for _, failAt := range []int{2, 3} {
		t.Run(fmt.Sprintf("admission=%d", failAt), func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
			source := vector.NewVec(types.T_json.ToType())
			defer source.Free(mp)
			doc, err := types.ParseStringToByteJson(`{"keep":1}`)
			require.NoError(t, err)
			require.NoError(t, vector.AppendByteJson(source, doc, false, mp))
			data := privateJSONColumnForTest(t, source)
			defer data.Release()
			want := errors.New("metadata admission rejected")
			var live uint64
			calls, partialReleases := 0, 0
			admission := func(capacity uint64) (func(), error) {
				calls++
				if calls == failAt {
					return func() { partialReleases++ }, want
				}
				live += capacity
				return func() { live -= capacity }, nil
			}
			reader, err := OpenJSONReader(data, mp, nil, admission)
			require.ErrorIs(t, err, want)
			require.Nil(t, reader)
			require.Zero(t, live)
			require.Equal(t, 1, partialReleases)
			require.Positive(t, data.Size(), "failed metadata admission keeps the cache owner usable")
		})
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
			reader, err := OpenJSONReader(data, mp, nil, testJSONCachePinAdmission(t))
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
			require.Equal(t, "1", text.Text)
			text.Close()
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
	cache, err := OpenJSONReader(data, mp, nil, testJSONCachePinAdmission(t))
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
				require.Equal(t, at.Text, bt.Text, text)
				at.Close()
				bt.Close()
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
				require.Equal(t, cacheJSONTextFields(left), cacheJSONTextFields(right), text)
				again, err := bc.QueryText(row, []byte(text), legacyFloat)
				require.NoError(t, err)
				require.Equal(t, cacheJSONTextFields(right), cacheJSONTextFields(again), "selection must leave the query result unchanged")
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
				left.Close()
				right.Close()
				again.Close()
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
	require.Equal(t, "1", cursorText.Text)
	cursorText.Close()
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
	require.Equal(t, "1", text.Text)
	text.Close()
}

type wrappedCacheJSONReader struct{ vector.JSONReader }

func TestCacheJSONReaderRepeatView(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	source := vector.NewVec(types.T_json.ToType())
	defer source.Free(mp)
	for _, text := range []string{`{"keep":1}`, `{"keep":2}`} {
		doc, err := types.ParseStringToByteJson(text)
		require.NoError(t, err)
		require.NoError(t, vector.AppendByteJson(source, doc, false, mp))
	}
	require.NoError(t, vector.AppendBytes(source, nil, true, mp))
	data := privateJSONColumnForTest(t, source)
	reader, err := OpenJSONReader(data, mp, nil, testJSONCachePinAdmission(t))
	data.Release()
	require.NoError(t, err)
	defer reader.Close()
	require.True(t, IsPrivateJSONCacheReader(reader))
	require.False(t, IsPrivateJSONCacheReader(&wrappedCacheJSONReader{JSONReader: reader}))
	require.False(t, IsPrivateJSONCacheReader(nil))
	_, err = reader.Repeat(3, 1)
	require.Error(t, err)
	_, err = reader.Repeat(0, -1)
	require.Error(t, err)

	before := mp.CurrNB()
	repeated, err := reader.Repeat(1, 100_000)
	require.NoError(t, err)
	defer repeated.Close()
	require.True(t, IsPrivateJSONCacheReader(repeated))
	require.Equal(t, before, mp.CurrNB())
	window, err := repeated.Window(40_000, 40_003)
	require.NoError(t, err)
	defer window.Close()
	selected, err := window.Select([]int64{2, 0}, mp)
	require.NoError(t, err)
	defer selected.Close()
	again, err := selected.Repeat(0, 3)
	require.NoError(t, err)
	defer again.Close()
	nullView, err := reader.Repeat(2, 2)
	require.NoError(t, err)
	null, err := nullView.IsNull(1)
	require.NoError(t, err)
	require.True(t, null)
	nullView.Close()
	materialized, err := again.Materialize(mp)
	require.NoError(t, err)
	defer materialized.Free(mp)
	cursor, err := again.OpenCursor()
	require.NoError(t, err)
	reader.Close()
	repeated.Close()
	window.Close()
	selected.Close()
	again.Close()
	require.False(t, IsPrivateJSONCacheReader(again))
	result, err := cursor.QueryText(2, []byte("$.keep"), false)
	require.NoError(t, err)
	require.Equal(t, "2", result.Text)
	result.Close()
	cursor.Close()
	_, err = again.Repeat(0, 1)
	require.ErrorIs(t, err, io.ErrClosedPipe)
	require.Equal(t, 3, materialized.Length())
	for row := 0; row < 3; row++ {
		value := types.DecodeJson(materialized.GetBytesAt(row))
		path, err := types.ParseStringToPath("$.keep")
		require.NoError(t, err)
		selectedValue, exists := value.QueryWithExists([]*bytejson.Path{&path})
		require.True(t, exists)
		require.Equal(t, int64(2), selectedValue.GetInt64())
	}
}
