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
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

func TestCachedInt32BlocksRequiresCompleteMetadata(t *testing.T) {
	ctx := t.Context()
	fs, err := fileservice.NewMemoryFS("cached-index-metadata", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	t.Cleanup(func() { bat.Clean(mp) })
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(12), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(12), false, mp))
	bat.SetRowCount(1)
	id := NewObjectid()
	name := BuildObjectNameWithObjectID(&id)
	writer, err := NewObjectWriter(name, fs, 0, []uint16{0, 1}, nil)
	require.NoError(t, err)
	for range 2 {
		_, err = writer.Write(bat)
		require.NoError(t, err)
	}
	blocks, err := writer.WriteEnd(ctx)
	require.NoError(t, err)
	require.Len(t, blocks, 2)
	loc := BuildLocation(name, blocks[0].GetExtent(), 1, 0)

	var dst [2]CachedInt32Block
	require.False(t, CachedInt32Blocks(ctx, loc, []uint16{0}, dst[:]), "cold metadata must stay a miss")
	_, err = FastLoadObjectMeta(ctx, &loc, false, fs)
	require.NoError(t, err)
	require.True(t, CachedInt32Blocks(ctx, loc, []uint16{0}, dst[:]))
	for i, block := range dst {
		require.Equal(t, uint32(1), block.Rows)
		require.Equal(t, blocks[i].MustGetColumn(0).Location(), Extent(block.Extents[0][:]))
	}
	require.False(t, CachedInt32Blocks(ctx, loc, nil, dst[:]))
	require.False(t, CachedInt32Blocks(ctx, loc, []uint16{0}, dst[:1]))
	require.False(t, CachedInt32Blocks(ctx, loc, []uint16{0}, make([]CachedInt32Block, 3)))
	require.False(t, CachedInt32Blocks(ctx, loc, []uint16{2}, dst[:]), "missing column")
	require.False(t, CachedInt32Blocks(ctx, loc, []uint16{1}, dst[:]), "unsupported column type")

	key := encodeCacheKey(*loc.ShortName(), cacheKeyTypeMeta)
	raw, ok := metaCache.Get(ctx, key)
	require.True(t, ok)
	// Publish a truncated copy under its own key so neither the fixture nor
	// immutable metadata borrowed by other readers is modified.
	invalidID := NewObjectid()
	invalid := BuildLocation(BuildObjectNameWithObjectID(&invalidID), loc.Extent(), loc.Rows(), 0)
	metaCache.Set(ctx, encodeCacheKey(*invalid.ShortName(), cacheKeyTypeMeta), bytes.Clone(raw[:IOEntryHeaderSize]), IOEntryHeaderSize)
	require.False(t, CachedInt32Blocks(ctx, invalid, []uint16{0}, dst[:]), "truncated metadata")
}

func TestReadCachedInt32RowsRequiresAllColumnsAndReleases(t *testing.T) {
	ctx := t.Context()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	capacity := toml.ByteSize(1 << 20)
	fs, err := fileservice.NewLocalFS2(ctx, "test", t.TempDir(), fileservice.CacheConfig{MemoryCapacity: &capacity}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	fs.SetAsyncUpdate(false)

	vec := vector.NewVec(types.T_int32.ToType())
	defer vec.Free(mp)
	require.NoError(t, vector.AppendFixed(vec, int32(12), false, mp))
	require.NoError(t, vector.AppendFixed(vec, int32(15), true, mp))
	payload, err := vec.MarshalBinary()
	require.NoError(t, err)
	header := IOEntryHeader{Type: IOET_ColData, Version: IOET_ColumnData_V2}
	encoded := append(append([]byte(nil), EncodeIOEntryHeader(&header)...), payload...)
	size := uint32(len(encoded))
	const name = "column-sample"
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{FilePath: name, Entries: []fileservice.IOEntry{
		{Offset: 0, Size: int64(size), Data: encoded},
		{Offset: int64(size), Size: int64(size), Data: encoded},
	}}))
	var block CachedInt32Block
	block.Rows = 2
	for i := range 2 {
		copy(block.Extents[i][:], NewExtent(compress.None, uint32(i)*size, size, size))
	}
	warm := func(i int) {
		t.Helper()
		read := fileservice.IOVector{FilePath: name, Entries: []fileservice.IOEntry{
			newColumnIOEntry(Extent(block.Extents[i][:]), columnCacheConstructorFactory),
		}}
		defer read.ReleaseReadResultOnError()
		require.NoError(t, fs.Read(ctx, &read))
	}
	dst := make([]CachedInt32Row, 3)
	picks := []int64{1, 0, 0}
	counters := new(perfcounter.CounterSet)
	sampled := perfcounter.WithCounterSet(ctx, counters)
	before := mp.CurrNB()
	require.False(t, ReadCachedInt32Rows(sampled, fs, name, block, 2, picks, dst, mp))
	warm(0)
	require.False(t, ReadCachedInt32Rows(sampled, fs, name, block, 2, picks, dst, mp))
	warm(1)
	require.True(t, ReadCachedInt32Rows(sampled, fs, name, block, 2, picks, dst, mp))
	require.Zero(t, dst[0].Present)
	require.Equal(t, CachedInt32Row{Values: [4]int32{12, 12}, Present: 0b11}, dst[1])
	require.Equal(t, dst[1], dst[2])
	require.Equal(t, before, mp.CurrNB())
	require.Zero(t, counters.FileService.DiskReadSize.Load())
	cancelled, cancel := context.WithCancel(sampled)
	cancel()
	require.False(t, ReadCachedInt32Rows(cancelled, fs, name, block, 2, picks, dst, mp))
}
