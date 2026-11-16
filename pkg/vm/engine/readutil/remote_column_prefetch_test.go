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

package readutil

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
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type objectPrefetchRecordingFS struct {
	fileservice.FileService
	reqs []fileservice.ObjectPrefetch
}

func (fs *objectPrefetchRecordingFS) PrefetchObject(_ context.Context, req fileservice.ObjectPrefetch) error {
	fs.reqs = append(fs.reqs, req)
	return nil
}

// writeColumnObjects writes objects of 3 blocks each, with an int32 column 0 and a vector
// column 1 whose ranges the objects record, and returns relation data over their blocks.
func writeColumnObjects(t *testing.T, fs fileservice.FileService, objects int) (engine.RelData, []objectio.ObjectStats) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	ctx := context.Background()
	const rows = 100
	data := NewBlockListRelationData(0)
	var stats []objectio.ObjectStats
	for o := 0; o < objects; o++ {
		id := objectio.NewObjectid()
		name := objectio.BuildObjectNameWithObjectID(&id)
		writer, err := objectio.NewObjectWriter(name, fs, 0, []uint16{0, 1}, nil)
		require.NoError(t, err)
		for blk := 0; blk < 3; blk++ {
			bat := batch.NewWithSize(2)
			bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_array_float32.ToType())
			for row := 0; row < rows; row++ {
				require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(row), false, mp))
				require.NoError(t, vector.AppendArray(bat.Vecs[1], make([]float32, 32), false, mp))
			}
			bat.SetRowCount(rows)
			_, err := writer.Write(bat)
			require.NoError(t, err)
			bat.Clean(mp)
		}
		writer.WriteObjectMeta(ctx, 3*rows, []objectio.ColumnMeta{objectio.BuildObjectColumnMeta(), objectio.BuildObjectColumnMeta()})
		blocks, err := writer.WriteEnd(ctx)
		require.NoError(t, err)
		stats = append(stats, writer.GetObjectStats())
		for i, b := range blocks {
			loc := objectio.BuildLocation(name, b.GetExtent(), rows, uint16(i))
			block := objectio.BlockInfo{BlockID: *objectio.BuildObjectBlockid(name, uint16(i)), MetaLoc: objectio.ObjectLocation(loc)}
			data.AppendBlockInfo(&block)
		}
	}
	return data, stats
}

// A remote scan of some columns prefetches the ranges of those columns, once per object,
// for the scan it closes; other scans do not.
func TestRemoteDataSourcePrefetchesColumns(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy fileservice.Policy
		want   bool
	}{
		{name: "column ranges", policy: fileservice.SkipFullFilePreloads | fileservice.ColumnPrefetch, want: true},
		{name: "whole objects", policy: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem, err := fileservice.NewMemoryFS("mem", fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			fs := &objectPrefetchRecordingFS{FileService: mem}
			data, stats := writeColumnObjects(t, mem, 2)
			ctx := fileservice.WithFileServicePolicy(context.Background(), tc.policy)
			ds := NewRemoteDataSource(ctx, fs, timestamp.Timestamp{}, data)
			for i := 0; i < data.DataCnt(); i++ {
				_, state, err := ds.Next(ctx, nil, nil, []uint16{0}, 0, nil, nil, nil)
				require.NoError(t, err)
				require.Equal(t, engine.Persisted, state)
			}
			_, state, err := ds.Next(ctx, nil, nil, []uint16{0}, 0, nil, nil, nil)
			require.NoError(t, err)
			require.Equal(t, engine.End, state)
			if !tc.want {
				require.Empty(t, fs.reqs)
				ds.Close()
				return
			}
			require.Len(t, fs.reqs, len(stats), "each object is asked for once")
			for i, req := range fs.reqs {
				require.Equal(t, stats[i].ObjectName().String(), req.FilePath)
				require.Len(t, req.Ranges, 1)
				require.Equal(t, req.Ranges[0].Size, req.Size)
				require.Less(t, req.Size, int64(stats[i].Size()), "the columns not read are not downloaded")
				require.NotNil(t, req.Scan)
				require.Same(t, fs.reqs[0].Scan, req.Scan)
			}
			ds.Close()
			require.Nil(t, ds.prefetch.scan, "the scan's prefetches end with it")
		})
	}
}
