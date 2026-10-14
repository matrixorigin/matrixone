// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectio

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/require"
)

// Poison at the allocator's final release, including cache-bypassed reads.
// Borrowed vector slices remain addressable but must no longer be consumed.
type jsonPoisonData struct {
	fscache.Data
	refs  atomic.Int32
	freed *atomic.Int64
}

func (d *jsonPoisonData) Slice(n int) fscache.Data {
	d.Data = d.Data.Slice(n)
	return d
}

func (d *jsonPoisonData) Retain() {
	d.Data.Retain()
	d.refs.Add(1)
}

func (d *jsonPoisonData) Release() {
	if d.refs.Add(-1) == 0 {
		for i := range d.Data.Bytes() {
			d.Data.Bytes()[i] = 0xff
		}
		d.freed.Add(1)
	}
	d.Data.Release()
}

type jsonPoisonAllocator struct {
	fileservice.CacheDataAllocator
	allocated, freed *atomic.Int64
}

func (a *jsonPoisonAllocator) wrap(data fscache.Data) fscache.Data {
	d := &jsonPoisonData{Data: data, freed: a.freed}
	d.refs.Store(1)
	a.allocated.Add(1)
	return d
}

func (a *jsonPoisonAllocator) AllocateCacheData(ctx context.Context, n int) fscache.Data {
	return a.wrap(a.CacheDataAllocator.AllocateCacheData(ctx, n))
}

func (a *jsonPoisonAllocator) AllocateCacheDataWithHint(ctx context.Context, n int, hints malloc.Hints) fscache.Data {
	return a.wrap(a.CacheDataAllocator.AllocateCacheDataWithHint(ctx, n, hints))
}

func (a *jsonPoisonAllocator) CopyToCacheData(ctx context.Context, data []byte) fscache.Data {
	return a.wrap(a.CacheDataAllocator.CopyToCacheData(ctx, data))
}

type jsonPoisonFS struct {
	fileservice.FileService
	allocated, freed atomic.Int64
	afterRead        func(*fileservice.IOVector)
}

func (f *jsonPoisonFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	for i := range v.Entries {
		convert := v.Entries[i].ToCacheData
		if convert == nil {
			continue
		}
		v.Entries[i].ToCacheData = func(ctx context.Context, reader io.Reader, data []byte, allocator fileservice.CacheDataAllocator) (fscache.Data, error) {
			return convert(ctx, reader, data, &jsonPoisonAllocator{allocator, &f.allocated, &f.freed})
		}
	}
	if err := f.FileService.Read(ctx, v); err != nil {
		return err
	}
	if f.afterRead != nil {
		f.afterRead(v)
	}
	return nil
}

func jsonReviewObject(t *testing.T, ctx context.Context, fs fileservice.FileService, mp *mpool.MPool, name string, b *batch.Batch) ObjectMeta {
	t.Helper()
	w, err := NewObjectWriterSpecial(WriterNormal, name, fs)
	require.NoError(t, err)
	_, err = w.Write(b)
	require.NoError(t, err)
	blocks, err := w.WriteEnd(ctx)
	require.NoError(t, err)
	r, err := NewObjectReaderWithStr(name, fs)
	require.NoError(t, err)
	extent := blocks[0].GetExtent()
	r.CacheMetaExtent(&extent)
	meta, err := r.ReadMeta(ctx, mp)
	require.NoError(t, err)
	return meta
}

func TestJSONSubcolumnsPoisonOnRelease(t *testing.T) {
	ctx := t.Context()
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	base, err := fileservice.NewMemoryFS("json-poison", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer base.Close(ctx)
	b := jsonStorageBatch(t, mp, 0, 100)
	defer b.Clean(mp)
	meta := jsonReviewObject(t, ctx, base, mp, "poison", b)
	fs := &jsonPoisonFS{FileService: base}
	projection := WithJSONPathProjection(ctx, JSONPathProjection{1: {"$.cost", "$.trace.id"}})
	got := jsonReadVector(t, projection, meta, "poison", 0, fs, mp)
	defer got.Free(mp)
	require.Positive(t, fs.allocated.Load())
	require.Equal(t, fs.allocated.Load(), fs.freed.Load(), "all borrowed input storage must be released")
	for row := 0; row < 100; row++ {
		require.Equal(t, b.Vecs[1].IsNull(uint64(row)), got.IsNull(uint64(row)), "row %d root SQL NULL", row)
		if got.IsNull(uint64(row)) {
			continue
		}
		before, after := types.DecodeJson(b.Vecs[1].GetBytesAt(row)), types.DecodeJson(got.GetBytesAt(row))
		for _, path := range []string{"$.cost", "$.trace.id"} {
			p, err := bytejson.ParseJsonPath(path)
			require.NoError(t, err)
			want, wantPresent := before.QuerySimpleExist(&p)
			actual, actualPresent := after.QuerySimpleExist(&p)
			require.Equal(t, wantPresent, actualPresent)
			require.Equal(t, want.Type, actual.Type)
			require.Equal(t, want.Data, actual.Data)
		}
	}
}

func TestJSONSubcolumnsControlCharacterKeys(t *testing.T) {
	keys := []string{"", "a.b", "\"", "\\", "<>&", "\u2028", "\u2029", "中文🙂"}
	for c := rune(0); c < 32; c++ {
		keys = append(keys, string(c))
	}
	for c := rune(0x7f); c <= 0x9f; c++ {
		keys = append(keys, string(c))
	}
	for _, key := range keys {
		t.Run(fmt.Sprintf("key_%x", []byte(key)), func(t *testing.T) {
			ctx := t.Context()
			mp := mpool.MustNewZero()
			defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
			base, err := fileservice.NewMemoryFS("json-key", fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			defer base.Close(ctx)
			b := batch.NewWithSize(2)
			b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			b.Vecs[1] = vector.NewVec(types.T_json.ToType())
			defer b.Clean(mp)
			quoted, err := json.Marshal(key)
			require.NoError(t, err)
			documents := []string{
				"{" + string(quoted) + ":9007199254740993.001}",
				"{" + string(quoted) + ":null}", "{}", "null", "{}",
				"{" + string(quoted) + ":{" + string(quoted) + ":9007199254740993.002}}",
				"{" + string(quoted) + ":{" + string(quoted) + ":null}}",
				"{" + string(quoted) + ":[1,null,true]}",
			}
			for row, text := range documents {
				bj, err := bytejson.ParseFromStringExact(text)
				require.NoError(t, err)
				require.NoError(t, vector.AppendFixed(b.Vecs[0], int64(row), false, mp))
				require.NoError(t, vector.AppendByteJson(b.Vecs[1], bj, row == 4, mp))
			}
			b.SetRowCount(len(documents))
			meta := jsonReviewObject(t, ctx, base, mp, "key", b)
			path := "$." + string(quoted)
			paths := []string{path, path + "." + string(quoted)}
			fs := &jsonRecordingFS{FileService: base}
			projection := WithJSONPathProjection(ctx, JSONPathProjection{1: paths})
			got := jsonReadVector(t, projection, meta, "key", 0, fs, mp)
			defer got.Free(mp)
			require.False(t, fs.touches(meta.MustDataMeta().GetBlockMeta(0).ColumnMeta(1).Location()), "selected key must use its subcolumn")
			for row := range documents {
				require.Equal(t, b.Vecs[1].IsNull(uint64(row)), got.IsNull(uint64(row)))
				if got.IsNull(uint64(row)) {
					continue
				}
				for _, path := range paths {
					parsed, err := bytejson.ParseJsonPath(path)
					require.NoError(t, err)
					want, present := types.DecodeJson(b.Vecs[1].GetBytesAt(row)).QuerySimpleExist(&parsed)
					actual, actualPresent := types.DecodeJson(got.GetBytesAt(row)).QuerySimpleExist(&parsed)
					require.Equal(t, present, actualPresent)
					require.Equal(t, want.Type, actual.Type)
					require.Equal(t, want.Data, actual.Data)
				}
			}
		})
	}
}

func TestJSONSubcolumnsReleaseOnError(t *testing.T) {
	for _, mode := range []string{"cancel_materialization", "decode_error"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			mp := mpool.MustNewZero()
			defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
			base, err := fileservice.NewMemoryFS("json-error", fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			defer base.Close(t.Context())
			b := jsonStorageBatch(t, mp, 0, 100)
			defer b.Clean(mp)
			meta := jsonReviewObject(t, ctx, base, mp, "error", b)
			fs := &jsonPoisonFS{FileService: base}
			injected := false
			fs.afterRead = func(v *fileservice.IOVector) {
				if len(v.Entries) < 2 {
					return
				}
				root, err := DecodeCached(v.Entries[0].CachedData)
				require.NoError(t, err)
				flags := root.(*vector.Vector)
				isRoot := flags.GetType().Oid == types.T_bool
				flags.Free(nil)
				if !isRoot {
					return
				}
				injected = true
				if mode == "cancel_materialization" {
					cancel()
				} else {
					last := &v.Entries[len(v.Entries)-1]
					last.CachedData.Release()
					last.CachedData = nil
				}
			}
			projection := WithJSONPathProjection(ctx, JSONPathProjection{1: {"$.cost", "$.trace.id"}})
			result, err := ReadProjectedJSONColumns(projection, meta, "error", 0, []uint16{1}, []types.Type{types.T_json.ToType()}, mp, fs, fileservice.SkipAllCache)
			require.True(t, injected)
			require.Error(t, err)
			if mode == "cancel_materialization" {
				require.ErrorIs(t, err, context.Canceled)
			}
			for _, entry := range result.Entries {
				require.Nil(t, entry.CachedData)
			}
			require.Positive(t, fs.allocated.Load())
			require.Equal(t, fs.allocated.Load(), fs.freed.Load(), "failed reads must release all input allocations")
		})
	}
}
