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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

func TestBuildSnapshotBlockFilter(t *testing.T) {
	mp := mpool.MustNew(t.Name())
	tableDef := &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"a"},
			PkeyColName: "a",
		},
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}, Seqnum: 7},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}, Seqnum: 9},
		},
		Name2ColIndex: map[string]int32{
			"a": 0,
			"b": 1,
		},
	}

	t.Run("nil expr", func(t *testing.T) {
		filter, seqnum, typ, ok, err := buildSnapshotBlockFilter(tableDef, nil, mp)
		require.NoError(t, err)
		require.False(t, ok)
		require.False(t, filter.Valid)
		require.Zero(t, seqnum)
		require.Equal(t, types.Type{}, typ)
	})

	t.Run("non pk expr", func(t *testing.T) {
		vec := vector.NewVec(types.T_int64.ToType())
		defer vec.Free(mp)
		for _, v := range []int64{1, 3, 5} {
			require.NoError(t, vector.AppendFixed(vec, v, false, mp))
		}
		expr := readutil.ConstructInExpr(context.Background(), "b", vec)

		filter, seqnum, typ, ok, err := buildSnapshotBlockFilter(tableDef, expr, mp)
		require.NoError(t, err)
		require.False(t, ok)
		require.False(t, filter.Valid)
		require.Zero(t, seqnum)
		require.Equal(t, types.Type{}, typ)
	})

	t.Run("pk in expr", func(t *testing.T) {
		vec := vector.NewVec(types.T_int64.ToType())
		defer vec.Free(mp)
		for _, v := range []int64{1, 3, 5} {
			require.NoError(t, vector.AppendFixed(vec, v, false, mp))
		}
		vec.InplaceSort()
		expr := readutil.ConstructInExpr(context.Background(), "a", vec)

		filter, seqnum, typ, ok, err := buildSnapshotBlockFilter(tableDef, expr, mp)
		if filter.Cleanup != nil {
			defer filter.Cleanup()
		}

		require.NoError(t, err)
		require.True(t, ok)
		require.True(t, filter.Valid)
		require.NotNil(t, filter.DecideSearchFunc(true))
		require.NotNil(t, filter.DecideSearchFunc(false))
		require.Equal(t, uint16(7), seqnum)
		require.Equal(t, types.T_int64, typ.Oid)
	})
}

func TestNormalizeSnapshotScanParallelism(t *testing.T) {
	t.Run("serial on small scan", func(t *testing.T) {
		relData := newTestSnapshotRelData(snapshotScanMinBlocksPerReader*2 - 1)
		require.Equal(t, 1, normalizeSnapshotScanParallelism(relData, 0))
		require.Equal(t, 1, normalizeSnapshotScanParallelism(relData, 1))
	})

	t.Run("auto parallelism on large scan", func(t *testing.T) {
		relData := newTestSnapshotRelData(snapshotScanMinBlocksPerReader * 4)
		actual := normalizeSnapshotScanParallelism(relData, 0)
		require.GreaterOrEqual(t, actual, 2)
		require.LessOrEqual(t, actual, snapshotScanMaxParallelism)
	})

	t.Run("respect requested cap", func(t *testing.T) {
		relData := newTestSnapshotRelData(snapshotScanMinBlocksPerReader * 4)
		require.Equal(t, 2, normalizeSnapshotScanParallelism(relData, 2))
	})
}

func TestSplitSnapshotScanShards(t *testing.T) {
	relData := newTestSnapshotRelData(snapshotScanMinBlocksPerReader*4 + 3)
	shards := splitSnapshotScanShards(relData, 4)
	require.Len(t, shards, 4)

	totalBlocks := 0
	for _, shard := range shards {
		require.NotNil(t, shard)
		require.Greater(t, shard.DataCnt(), 0)
		totalBlocks += shard.DataCnt()
	}
	require.Equal(t, relData.DataCnt(), totalBlocks)
}

func newTestSnapshotRelData(blockCnt int) *readutil.BlockListRelData {
	relData := readutil.NewBlockListRelationData(0)
	for i := 0; i < blockCnt; i++ {
		blk := objectio.BlockInfo{}
		relData.AppendBlockInfo(&blk)
	}
	return relData
}
