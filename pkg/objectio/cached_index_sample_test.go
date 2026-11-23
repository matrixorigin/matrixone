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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

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
