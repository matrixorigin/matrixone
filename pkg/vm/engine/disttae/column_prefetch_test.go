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

package disttae

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
)

type columnPrefetchFS struct {
	fileservice.FileService
	reqs []fileservice.ObjectPrefetch
}

func (fs *columnPrefetchFS) PrefetchObject(_ context.Context, req fileservice.ObjectPrefetch) error {
	fs.reqs = append(fs.reqs, req)
	return nil
}

// newColumnPrefetchSource writes one object of 3 blocks, a small int32 column 0 and a large
// vector column 1, recording their ranges in the object or not, and returns a data source
// over its blocks.
func newColumnPrefetchSource(t *testing.T, recordColumns bool) (*LocalDisttaeDataSource, *columnPrefetchFS, objectio.ObjectStats) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	ctx := context.Background()
	mem, err := fileservice.NewMemoryFS("mem", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	fs := &columnPrefetchFS{FileService: mem}

	id := objectio.NewObjectid()
	name := objectio.BuildObjectNameWithObjectID(&id)
	writer, err := objectio.NewObjectWriter(name, mem, 0, []uint16{0, 1}, nil)
	require.NoError(t, err)
	const rows = 200
	for blk := 0; blk < 3; blk++ {
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_array_float32.ToType())
		for row := 0; row < rows; row++ {
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(blk*rows+row), false, mp))
			emb := make([]float32, 64)
			for i := range emb {
				emb[i] = float32(blk*rows + row + i)
			}
			require.NoError(t, vector.AppendArray(bat.Vecs[1], emb, false, mp))
		}
		bat.SetRowCount(rows)
		_, err := writer.Write(bat)
		require.NoError(t, err)
		bat.Clean(mp)
	}
	if recordColumns {
		writer.WriteObjectMeta(ctx, 3*rows, []objectio.ColumnMeta{objectio.BuildObjectColumnMeta(), objectio.BuildObjectColumnMeta()})
	}
	blocks, err := writer.WriteEnd(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()

	pState := logtailreplay.NewPartitionState("", true, 0, false)
	pState.UpdateDuration(types.BuildTS(0, 0), types.MaxTs())
	require.NoError(t, pState.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{ObjectStats: stats, CreateTime: types.BuildTS(1, 0)}, false))

	data := readutil.NewBlockListRelationData(0)
	for i, b := range blocks {
		loc := objectio.BuildLocation(name, b.GetExtent(), rows, uint16(i))
		block := objectio.BlockInfo{BlockID: *objectio.BuildObjectBlockid(name, uint16(i)), MetaLoc: objectio.ObjectLocation(loc)}
		data.AppendBlockInfo(&block)
	}
	ls := &LocalDisttaeDataSource{fs: fs, pState: pState, rangeSlice: data.GetBlockInfoSlice(), iteratePhase: engine.Persisted}
	return ls, fs, stats
}

// A column-prefetch scan downloads the ranges of the columns it reads, however much of the
// object they are, and never the whole object; other scans keep whole objects.
func TestColumnPrefetchRangesOrWholeObject(t *testing.T) {
	for _, tc := range []struct {
		name     string
		policy   fileservice.Policy
		seqNums  []uint16
		ranges   int // ranges of the one request; -1 means the whole object; -2 no request
		bare     bool
		overHalf bool // the columns read are at least half of the object
	}{
		{name: "small column", policy: fileservice.SkipFullFilePreloads | fileservice.ColumnPrefetch, seqNums: []uint16{0}, ranges: 1},
		{name: "column over half of the object", policy: fileservice.SkipFullFilePreloads | fileservice.ColumnPrefetch, seqNums: []uint16{1}, ranges: 1, overHalf: true},
		{name: "all columns", policy: fileservice.SkipFullFilePreloads | fileservice.ColumnPrefetch, seqNums: []uint16{0, 1}, ranges: 2},
		{name: "columns not in the object", policy: fileservice.SkipFullFilePreloads | fileservice.ColumnPrefetch, seqNums: []uint16{7}, ranges: -2},
		{name: "column ranges not recorded", policy: fileservice.SkipFullFilePreloads | fileservice.ColumnPrefetch, seqNums: []uint16{0}, ranges: -2, bare: true},
		{name: "scan", seqNums: []uint16{0}, ranges: -1},
		{name: "whole preloads skipped", policy: fileservice.SkipFullFilePreloads, seqNums: []uint16{0}, ranges: -2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ls, fs, stats := newColumnPrefetchSource(t, !tc.bare)
			ctx := fileservice.WithFileServicePolicy(context.Background(), tc.policy)
			ls.batchPrefetch(ctx, tc.seqNums)
			if tc.ranges == -2 {
				require.Empty(t, fs.reqs)
				return
			}
			require.Len(t, fs.reqs, 1, "an object is asked for once")
			req := fs.reqs[0]
			require.Equal(t, stats.ObjectName().String(), req.FilePath)
			if tc.ranges == -1 {
				require.Empty(t, req.Ranges)
				require.Equal(t, int64(stats.Size()), req.Size)
				require.Equal(t, 3, req.Reads)
				require.Equal(t, 3, req.Blocks)
				return
			}
			require.Len(t, req.Ranges, tc.ranges)
			var size int64
			for _, r := range req.Ranges {
				size += r.Size
			}
			require.Equal(t, size, req.Size)
			require.Less(t, req.Size, int64(stats.Size()), "the columns not read are not downloaded")
			if tc.overHalf {
				require.GreaterOrEqual(t, req.Size*2, int64(stats.Size()))
			}
		})
	}
}
