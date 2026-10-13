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

package objectio

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

type jsonRecordingFS struct {
	fileservice.FileService
	reads []fileservice.IOEntry
}

func (f *jsonRecordingFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	f.reads = append(f.reads, v.Entries...)
	return f.FileService.Read(ctx, v)
}
func (f *jsonRecordingFS) touches(ext Extent) bool {
	for _, e := range f.reads {
		if e.Offset < int64(ext.Offset()+ext.Length()) && e.Offset+e.Size > int64(ext.Offset()) {
			return true
		}
	}
	return false
}
func jsonStorageBatch(t *testing.T, mp *mpool.MPool, start, rows int) *batch.Batch {
	b := batch.NewWithSize(2)
	b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	b.Vecs[1] = vector.NewVec(types.T_json.ToType())
	for row := 0; row < rows; row++ {
		i := start + row
		a := fmt.Sprintf(`{"trace":{"id":"t%d","latency":%d},"cost":9007199254740993.001,"a.b":{"\"":"quoted"},"Case":"upper","case":"lower","tail":"%s"}`, i, i, strings.Repeat("unused", 128))
		switch i % 17 {
		case 0:
			a = `{}`
		case 1:
			a = `{"trace":null,"cost":"changed"}`
		case 2:
			a = `{"trace":{"id":null},"cost":{"nested":true}}`
		case 3:
			a = `{"trace":{"id":[1,"two",null]},"cost":false}`
		case 4:
			a = `null`
		case 5:
			a = `[1,2,3]`
		case 6:
			a = `{"trace":{"id":42,"latency":"slow"},"cost":1e-30}`
		}
		require.NoError(t, vector.AppendFixed(b.Vecs[0], int64(i), false, mp))
		bj, e := bytejson.ParseFromStringExact(a)
		require.NoError(t, e)
		require.NoError(t, vector.AppendByteJson(b.Vecs[1], bj, i%17 == 7, mp))
	}
	b.SetRowCount(rows)
	return b
}
func jsonReadVector(t *testing.T, ctx context.Context, meta ObjectMeta, name string, block uint16, fs fileservice.FileService, mp *mpool.MPool) *vector.Vector {
	v, e := ReadProjectedJSONColumns(ctx, meta, name, block, []uint16{1}, []types.Type{types.T_json.ToType()}, mp, fs, fileservice.SkipAllCache)
	require.NoError(t, e)
	obj, e := DecodeCached(v.Entries[0].CachedData)
	require.NoError(t, e)
	decoded := obj.(*vector.Vector)
	result, e := decoded.Dup(mp)
	require.NoError(t, e)
	decoded.Free(nil)
	v.Release()
	return result
}
func TestJSONSubcolumnsTenThousandRows(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	base, e := fileservice.NewMemoryFS("json-test", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, e)
	defer base.Close(ctx)
	fs := &jsonRecordingFS{FileService: base}
	name := "json-subcolumns"
	w, e := NewObjectWriterSpecial(WriterNormal, name, fs)
	require.NoError(t, e)
	original := []*batch.Batch{jsonStorageBatch(t, mp, 0, 5000), jsonStorageBatch(t, mp, 5000, 5000)}
	defer func() {
		for _, b := range original {
			b.Clean(mp)
		}
	}()
	for _, b := range original {
		_, e = w.Write(b)
		require.NoError(t, e)
	}
	blocks, e := w.WriteEnd(ctx)
	require.NoError(t, e)
	require.Len(t, blocks, 2)
	r, e := NewObjectReaderWithStr(name, fs)
	require.NoError(t, e)
	extent := blocks[0].GetExtent()
	r.CacheMetaExtent(&extent)
	meta, e := r.ReadMeta(ctx, mp)
	require.NoError(t, e)
	require.Greater(t, meta.SubMetaCount(), uint16(SchemaJSONSubcolumns-2))
	require.Equal(t, uint32(2), meta.MustDataMeta().BlockCount())
	paths := []string{`$.trace.id`, `$.trace.latency`, `$.cost`, `$."a.b"."\""`, `$.Case`, `$.case`}
	projected := WithJSONPathProjection(ctx, JSONPathProjection{1: paths})
	for block, b := range original {
		rawExt := meta.MustDataMeta().GetBlockMeta(uint32(block)).ColumnMeta(1).Location()
		fs.reads = nil
		got := jsonReadVector(t, projected, meta, name, uint16(block), fs, mp)
		require.False(t, fs.touches(rawExt), "projection must not read source JSON extent")
		for row := 0; row < 5000; row++ {
			require.Equal(t, b.Vecs[1].IsNull(uint64(row)), got.IsNull(uint64(row)))
			if got.IsNull(uint64(row)) {
				continue
			}
			before := types.DecodeJson(b.Vecs[1].GetBytesAt(row))
			after := types.DecodeJson(got.GetBytesAt(row))
			for _, p := range paths {
				path, e := bytejson.ParseJsonPath(p)
				require.NoError(t, e)
				x, xok := before.QuerySimpleExist(&path)
				y, yok := after.QuerySimpleExist(&path)
				require.Equal(t, xok, yok, "row=%d path=%s", row, p)
				require.Equal(t, x.Type, y.Type)
				require.Equal(t, x.Data, y.Data, "row=%d path=%s", row, p)
			}
		}
		got.Free(mp)
		for _, readCtx := range []context.Context{ctx, WithJSONPathProjection(ctx, JSONPathProjection{1: {"$.unknown"}})} {
			fs.reads = nil
			full := jsonReadVector(t, readCtx, meta, name, uint16(block), fs, mp)
			require.True(t, fs.touches(rawExt))
			for row := 0; row < 5000; row++ {
				require.Equal(t, b.Vecs[1].IsNull(uint64(row)), full.IsNull(uint64(row)))
				if !full.IsNull(uint64(row)) {
					require.Equal(t, b.Vecs[1].GetBytesAt(row), full.GetBytesAt(row))
				}
			}
			full.Free(mp)
		}
	}
	cancelled, cancel := context.WithCancel(projected)
	cancel()
	v, e := ReadProjectedJSONColumns(cancelled, meta, name, 0, []uint16{1}, []types.Type{types.T_json.ToType()}, mp, fs, fileservice.SkipAllCache)
	v.Release()
	require.Error(t, e)
}
func TestJSONSubcolumnsLegacyAndContract(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	fs, e := fileservice.NewMemoryFS("json-legacy", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, e)
	defer fs.Close(ctx)
	b := jsonStorageBatch(t, mp, 0, 100)
	defer b.Clean(mp)
	w, e := NewObjectWriterSpecial(WriterNormal, "legacy", fs)
	require.NoError(t, e)
	w.SetJSONSubcolumns(false)
	_, e = w.Write(b)
	require.NoError(t, e)
	blocks, e := w.WriteEnd(ctx)
	require.NoError(t, e)
	r, e := NewObjectReaderWithStr("legacy", fs)
	require.NoError(t, e)
	extent := blocks[0].GetExtent()
	r.CacheMetaExtent(&extent)
	meta, e := r.ReadMeta(ctx, mp)
	require.NoError(t, e)
	require.Zero(t, meta.SubMetaCount())
	got := jsonReadVector(t, WithJSONPathProjection(ctx, JSONPathProjection{1: {"$.trace.id"}}), meta, "legacy", 0, fs, mp)
	defer got.Free(mp)
	for i := 0; i < 100; i++ {
		if !got.IsNull(uint64(i)) {
			require.Equal(t, b.Vecs[1].GetBytesAt(i), got.GetBytesAt(i))
		}
	}
	for _, s := range []string{"bad", JSONProjectionPrefix + `{"1":["$"]}`, JSONProjectionPrefix + `{"1":["$.*"]}`, JSONProjectionPrefix + `{"1":["$.x[0]"]}`, JSONProjectionPrefix + `{"1":[]}`} {
		_, e := DecodeJSONPathProjection(s)
		require.Error(t, e)
	}
	p := JSONPathProjection{1: {`$."a.b"."\""`}}
	decoded, e := DecodeJSONPathProjection(EncodeJSONPathProjection(p))
	require.NoError(t, e)
	require.Equal(t, p, decoded)
}

func TestJSONSubcolumnsDiscoveryBudgetAndCorruption(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	fs, e := fileservice.NewMemoryFS("json-budget", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, e)
	defer fs.Close(ctx)
	b := batch.NewWithSize(1)
	b.Vecs[0] = vector.NewVec(types.T_json.ToType())
	defer b.Clean(mp)
	fields := make(map[string]any)
	for i := 0; i < 300; i++ {
		fields[fmt.Sprintf("field%03d", i)] = strings.Repeat("x", 100)
	}
	fields["a"] = int64(1)
	fields["deep"] = map[string]any{"b": int64(2)}
	for i := 0; i < 100; i++ {
		if i == 1 {
			fields["a"] = map[string]any{"b": int64(9)}
		}
		j, e := bytejson.CreateByteJSON(fields)
		require.NoError(t, e)
		require.NoError(t, vector.AppendByteJson(b.Vecs[0], j, false, mp))
	}
	b.SetRowCount(100)
	aux, e := buildJSONSubcolumns(b, NewSeqnums([]uint16{0}), 0)
	require.NoError(t, e)
	require.LessOrEqual(t, len(aux.Vecs), 34)
	serialized, e := aux.MarshalBinary()
	require.NoError(t, e)
	require.LessOrEqual(t, len(serialized), jsonSubcolumnMaxBytes)
	aux.Clean(nil)
	w, e := NewObjectWriterSpecial(WriterNormal, "budget", fs)
	require.NoError(t, e)
	_, e = w.Write(b)
	require.NoError(t, e)
	blocks, e := w.WriteEnd(ctx)
	require.NoError(t, e)
	r, e := NewObjectReaderWithStr("budget", fs)
	require.NoError(t, e)
	ext := blocks[0].GetExtent()
	r.CacheMetaExtent(&ext)
	meta, e := r.ReadMeta(ctx, mp)
	require.NoError(t, e)
	// Corruption of the new optional directory must return an error, not panic.
	corrupt := append(ObjectMeta(nil), meta...)
	directory := int(IOEntryHeaderSize+metaHeaderLen+schemaCountLen) + int(SchemaJSONSubcolumns-2)*int(typePosLen)
	for _, field := range []int{directory, directory + int(schemaType+schemaBlockCount)} {
		copy := append(ObjectMeta(nil), corrupt...)
		for i := 0; i < 4; i++ {
			copy[field+i] = 255
		}
		_, _, e = readJSONManifest(ctx, copy, "budget", 0, 100, fs, mp, fileservice.SkipAllCache)
		require.Error(t, e)
	}
	empty := append(ObjectMeta(nil), meta...)
	section, _ := empty.SubMeta(uint16(SchemaJSONSubcolumns - 2))
	emptyBlock := section.GetBlockMeta(uint32(section.BlockHeader().StartID()))
	emptyBlock.BlockHeader().SetColumnCount(0)
	emptyBlock.BlockHeader().SetMetaColumnCount(0)
	_, _, e = readJSONManifest(ctx, empty, "budget", 0, 100, fs, mp, fileservice.SkipAllCache)
	require.Error(t, e)
	// Parent scalar/container drift and requesting both ancestor and descendant.
	result, e := ReadProjectedJSONColumns(WithJSONPathProjection(ctx, JSONPathProjection{0: {"$.a", "$.a.b"}}), meta, "budget", 0, []uint16{0}, []types.Type{types.T_json.ToType()}, mp, fs, fileservice.SkipAllCache)
	require.NoError(t, e)
	obj, e := DecodeCached(result.Entries[0].CachedData)
	require.NoError(t, e)
	got := obj.(*vector.Vector)
	for row := 0; row < 100; row++ {
		before := types.DecodeJson(b.Vecs[0].GetBytesAt(row))
		after := types.DecodeJson(got.GetBytesAt(row))
		for _, s := range []string{"$.a", "$.a.b"} {
			p, e := bytejson.ParseJsonPath(s)
			require.NoError(t, e)
			x, xok := before.QuerySimpleExist(&p)
			y, yok := after.QuerySimpleExist(&p)
			require.Equal(t, xok, yok)
			require.Equal(t, x.Type, y.Type)
			require.Equal(t, x.Data, y.Data)
		}
	}
	got.Free(nil)
	result.Release()
}

func FuzzJSONSubcolumnProjection(f *testing.F) {
	for _, s := range []string{`{"a":{"b":1},"x":null}`, `{"a":1}`, `null`, `[]`, `{"a":{"b":[1,2]}}`} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > 16384 {
			return
		}
		j, e := bytejson.ParseFromStringExact(s)
		if e != nil {
			return
		}
		mp := mpool.MustNewZero()
		defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
		b := batch.NewWithSize(1)
		b.Vecs[0] = vector.NewVec(types.T_json.ToType())
		defer b.Clean(mp)
		for _, value := range []bytejson.ByteJson{j, j} {
			require.NoError(t, vector.AppendByteJson(b.Vecs[0], value, false, mp))
		}
		b.SetRowCount(2)
		aux, e := buildJSONSubcolumns(b, NewSeqnums([]uint16{0}), 0)
		require.NoError(t, e)
		defer aux.Clean(nil)
		var manifest jsonSubcolumnManifest
		require.NoError(t, json.Unmarshal(aux.Vecs[0].GetBytesAt(0), &manifest))
		require.LessOrEqual(t, len(manifest.Sources), 64)
		for _, source := range manifest.Sources {
			for _, path := range source.Paths {
				p, e := bytejson.ParseJsonPath(jsonObjectKeyPath(path.Keys))
				require.NoError(t, e)
				want, present := j.QuerySimpleExist(&p)
				v := aux.Vecs[path.Column]
				require.Equal(t, !present, v.IsNull(0))
				if present {
					got := types.DecodeJson(v.GetBytesAt(0))
					require.Equal(t, want.Type, got.Type)
					require.Equal(t, want.Data, got.Data)
				}
			}
		}
	})
}
