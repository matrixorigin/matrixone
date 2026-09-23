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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

func TestSharedDecodedColumnTopNIsolation(t *testing.T) {
	ctx := context.Background()
	capacity := toml.ByteSize(128 << 10)
	fs, err := fileservice.NewS3FS(ctx,
		fileservice.ObjectStorageArguments{Name: "shared-column", Endpoint: "disk", Bucket: t.TempDir()},
		fileservice.CacheConfig{MemoryCapacity: &capacity}, nil, false, false)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_array_float32.ToType())
	t.Cleanup(func() { bat.Clean(mp) })
	for _, value := range []float32{1, 2, 3, 4} {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], types.ArrayToBytes([]float32{value}), false, mp))
	}
	bat.SetRowCount(4)
	id := NewObjectid()
	name := BuildObjectNameWithObjectID(&id)
	writer, err := NewObjectWriter(name, fs, 0, []uint16{0}, nil)
	require.NoError(t, err)
	_, err = writer.Write(bat)
	require.NoError(t, err)
	blocks, err := writer.WriteEnd(ctx)
	require.NoError(t, err)
	location := BuildLocation(name, blocks[0].GetExtent(), 4, 0)
	meta, err := FastLoadObjectMeta(ctx, &location, false, fs)
	require.NoError(t, err)
	dataMeta := meta.MustGetMeta(SchemaData)
	fs.FlushCache(ctx)
	pinned := fs.AllocateCacheData(ctx, int(capacity))
	t.Cleanup(pinned.Release)
	read := func(share bool) fileservice.IOVector {
		var options []ReadOneBlockOption
		if share {
			options = append(options, ShareScopedDecodedColumn)
		}
		v, err := ReadOneBlock(ctx, &dataMeta, name.String(), 0, []uint16{0},
			[]types.Type{types.T_array_float32.ToType()}, mp, fs, fileservice.SkipFullFilePreloads, options...)
		require.NoError(t, err)
		t.Cleanup(v.Release)
		return v
	}
	first, second := read(true), read(true)
	owner := first.Entries[0].CachedData
	require.Same(t, owner, second.Entries[0].CachedData)
	require.True(t, isValidatedVectorCacheData(owner), "sharing must preserve the concrete validation marker")
	backing := owner.(validatedVectorCacheDataMarker).validatedVectorBackingForScope()
	snapshot := bytes.Clone(backing)
	ordinary := read(false)
	require.NotSame(t, owner, ordinary.Entries[0].CachedData, "ordinary reads must not acquire scoped sharing")
	for _, tc := range []struct {
		name          string
		query         float32
		rows          []int64
		bound         plan.BoundType
		upper         float64
		wantRows      []int64
		wantDistances []float64
	}{
		{name: "all rows", query: 0, wantRows: []int64{0}, wantDistances: []float64{1}},
		{name: "different query and selection", query: 5, rows: []int64{1, 2}, wantRows: []int64{2}, wantDistances: []float64{4}},
		{name: "threshold excludes winner", query: 5, rows: []int64{1, 2}, bound: plan.BoundType_EXCLUSIVE, upper: 4, wantRows: []int64{}, wantDistances: []float64{}},
		{name: "empty selection", query: 0, rows: []int64{}, wantRows: []int64{}, wantDistances: []float64{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			top := &IndexReaderTopOp{Typ: types.T_array_float32, MetricType: metric.Metric_L2Distance,
				NumVec: types.ArrayToBytes([]float32{tc.query}), Limit: 1, UpperBoundType: tc.bound, UpperBound: tc.upper}
			rows, distances, err := SearchCachedVectorTopN(ctx, second.Entries[0], tc.rows, top)
			require.NoError(t, err)
			require.EqualValues(t, tc.wantRows, rows)
			require.EqualValues(t, tc.wantDistances, distances)
			require.Equal(t, snapshot, backing)
		})
	}
}

func TestReadOneBlockScopedDecodePosition(t *testing.T) {
	// Metadata is sufficient to prove opt-in before physical compaction; the
	// read seam supplies tiny owned data and leaves synthetic-column expansion real.
	meta := BuildMetaData(1, 3)
	block := meta.GetBlockMeta(0)
	block.BlockHeader().SetRows(1)
	block.BlockHeader().SetMaxSeqnum(2)
	block.BlockHeader().SetMetaColumnCount(3)
	for i, typ := range []types.T{types.T_array_float32, types.T_int64, types.T_array_float64} {
		block.ColumnMeta(uint16(i)).setDataType(uint8(typ))
		block.ColumnMeta(uint16(i)).setLocation(NewExtent(1, uint32(i+1), 1, 1))
	}
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	for _, tc := range []struct {
		name     string
		columns  []uint16
		typs     []types.Type
		selected int
		marked   int
	}{
		{"first", []uint16{0, 1, 2}, []types.Type{types.T_array_float32.ToType(), types.T_int64.ToType(), types.T_array_float64.ToType()}, 0, 0},
		{"last", []uint16{0, 1, 2}, []types.Type{types.T_array_float32.ToType(), types.T_int64.ToType(), types.T_array_float64.ToType()}, 2, 2},
		{"scalar", []uint16{0, 1, 2}, []types.Type{types.T_array_float32.ToType(), types.T_int64.ToType(), types.T_array_float64.ToType()}, 1, -1},
		{"synthetic predecessor", []uint16{3, 0, 2}, []types.Type{types.T_int64.ToType(), types.T_array_float32.ToType(), types.T_array_float64.ToType()}, 1, 1},
		{"selected missing", []uint16{3, 0}, []types.Type{types.T_array_float32.ToType(), types.T_array_float32.ToType()}, 0, -1},
		{"selected special", []uint16{SEQNUM_COMMITTS, 0}, []types.Type{TSType, types.T_array_float32.ToType()}, 0, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := &scopedDescriptorFS{}
			v, err := ReadOneBlockWithScopedDecode(t.Context(), &meta, "columns", 0, tc.columns, tc.typs, mp, fs, 0, tc.selected)
			require.NoError(t, err)
			defer v.Release()
			require.Len(t, v.Entries, len(tc.columns))
			for i := range v.Entries {
				if i == tc.marked {
					require.Equal(t, "objectio-validated-column-v1", v.Entries[i].DecodeSharing.Codec)
					require.Equal(t, int64(tc.columns[i]+1), v.Entries[i].Offset)
				} else {
					require.Empty(t, v.Entries[i].DecodeSharing.Codec)
				}
			}
			if tc.marked >= 0 {
				require.Equal(t, 1, fs.marked)
			} else {
				require.Zero(t, fs.marked)
			}
		})
	}
	for _, pos := range []int{-1, 1, 2} {
		t.Run(fmt.Sprintf("invalid %d", pos), func(t *testing.T) {
			v, err := ReadOneBlockWithScopedDecode(t.Context(), nil, "", 0, []uint16{0, 1}, []types.Type{types.T_array_float32.ToType()}, mp, nil, 0, pos)
			require.ErrorContains(t, err, "outside column/type lists")
			require.Empty(t, v.Entries)
		})
	}
	fs := &scopedDescriptorFS{}
	v, err := ReadOneBlockWithMeta(t.Context(), &meta, "columns", 0, []uint16{0}, []types.Type{types.T_array_float32.ToType()}, mp, fs, constructorFactory, 0)
	require.NoError(t, err)
	v.Release()
	require.Zero(t, fs.marked, "custom factories never opt in")
}

type scopedDescriptorFS struct {
	fileservice.FileService
	marked int
}

func (f *scopedDescriptorFS) Read(_ context.Context, v *fileservice.IOVector) error {
	for i := range v.Entries {
		if v.Entries[i].DecodeSharing.Codec != "" {
			f.marked++
		}
		v.Entries[i].CachedData = fileservice.NewBytes([]byte{1})
	}
	return nil
}
