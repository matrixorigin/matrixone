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
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
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

func TestAttrsToSeqnums(t *testing.T) {
	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "id", Seqnum: 3},
		},
		Name2ColIndex: map[string]int32{
			"id": 0,
		},
	}

	seqnums := attrsToSeqnums([]string{"id", objectio.DefaultCommitTS_Attr, objectio.PhysicalAddr_Attr}, tableDef)
	require.Equal(t, []uint16{3, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ROWID}, seqnums)
}

func TestReadSnapshotWithSource_SkipsEmptyBatchAndEmitsRows(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	source := &stubSnapshotDataSource{
		steps: []snapshotDataSourceStep{
			{state: engine.InMem, values: nil},
			{state: engine.InMem, values: []int64{7, 9}},
			{state: engine.End},
		},
	}

	var got []int64
	err := readSnapshotWithSource(
		context.Background(),
		nil,
		source,
		types.BuildTS(20, 0),
		snapshotScanReaderConfig{
			attrs:    []string{"id"},
			seqnums:  []uint16{1},
			colTypes: []types.Type{types.T_int64.ToType()},
		},
		mp,
		func(bat *batch.Batch) error {
			got = append(got, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0])...)
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []int64{7, 9}, got)
	require.True(t, source.closed)
}

func TestReadSnapshotWithSource_PropagatesOnBatchError(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	source := &stubSnapshotDataSource{
		steps: []snapshotDataSourceStep{
			{state: engine.InMem, values: []int64{1}},
		},
	}
	wantErr := errors.New("emit failed")

	err := readSnapshotWithSource(
		context.Background(),
		nil,
		source,
		types.BuildTS(20, 0),
		snapshotScanReaderConfig{
			attrs:    []string{"id"},
			seqnums:  []uint16{1},
			colTypes: []types.Type{types.T_int64.ToType()},
		},
		mp,
		func(*batch.Batch) error {
			return wantErr
		},
	)
	require.ErrorIs(t, err, wantErr)
	require.True(t, source.closed)
}

func TestBuildSnapshotScanReaderConfig(t *testing.T) {
	mp := mpool.MustNew(t.Name())
	tableDef := &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"id"},
			PkeyColName: "id",
		},
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}, Seqnum: 5},
			{Name: "payload", Typ: plan.Type{Id: int32(types.T_varchar)}, Seqnum: 6},
		},
		Name2ColIndex: map[string]int32{
			"id":      0,
			"payload": 1,
		},
	}

	filterVec := vector.NewVec(types.T_int64.ToType())
	defer filterVec.Free(mp)
	require.NoError(t, vector.AppendFixed(filterVec, int64(3), false, mp))
	filterExpr := readutil.ConstructInExpr(context.Background(), "id", filterVec)

	cfg, err := buildSnapshotScanReaderConfig(
		tableDef,
		[]string{"ID", "payload", objectio.DefaultCommitTS_Attr},
		[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType(), types.T_TS.ToType()},
		filterExpr,
		mp,
	)
	if cfg.blockFilter.Cleanup != nil {
		defer cfg.blockFilter.Cleanup()
	}

	require.NoError(t, err)
	require.Equal(t, []uint16{5, 6, objectio.SEQNUM_COMMITTS}, cfg.seqnums)
	require.True(t, cfg.hasBlockFilter)
	require.Equal(t, uint16(5), cfg.filterSeqnum)
	require.Equal(t, types.T_int64, cfg.filterType.Oid)
}

func TestUnwrapTxnTable(t *testing.T) {
	tbl := &txnTable{}

	got, err := unwrapTxnTable(tbl)
	require.NoError(t, err)
	require.Same(t, tbl, got)

	got, err = unwrapTxnTable(&txnTableDelegate{origin: tbl})
	require.NoError(t, err)
	require.Same(t, tbl, got)

	_, err = unwrapTxnTable(&mockRelation{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot scan requires disttae relation")
}

type snapshotDataSourceStep struct {
	state  engine.DataState
	values []int64
	err    error
}

type stubSnapshotDataSource struct {
	steps   []snapshotDataSourceStep
	idx     int
	closed  bool
	orderBy []*plan.OrderBySpec
}

func (s *stubSnapshotDataSource) Next(
	_ context.Context,
	_ []string,
	_ []types.Type,
	_ []uint16,
	_ int32,
	_ any,
	mp *mpool.MPool,
	bat *batch.Batch,
) (*objectio.BlockInfo, engine.DataState, error) {
	if s.idx >= len(s.steps) {
		return nil, engine.End, nil
	}
	step := s.steps[s.idx]
	s.idx++
	if step.err != nil {
		return nil, step.state, step.err
	}
	if step.state != engine.InMem {
		return nil, step.state, nil
	}
	for _, value := range step.values {
		if err := vector.AppendFixed(bat.Vecs[0], value, false, mp); err != nil {
			return nil, engine.End, err
		}
	}
	bat.SetRowCount(len(step.values))
	return nil, engine.InMem, nil
}

func (s *stubSnapshotDataSource) ApplyTombstones(
	context.Context,
	*objectio.Blockid,
	[]int64,
	engine.TombstoneApplyPolicy,
) ([]int64, error) {
	return nil, nil
}

func (s *stubSnapshotDataSource) GetTombstones(context.Context, *objectio.Blockid) (objectio.Bitmap, error) {
	return objectio.Bitmap{}, nil
}

func (s *stubSnapshotDataSource) SetOrderBy(orderBy []*plan.OrderBySpec) {
	s.orderBy = orderBy
}

func (s *stubSnapshotDataSource) GetOrderBy() []*plan.OrderBySpec {
	return s.orderBy
}

func (s *stubSnapshotDataSource) SetFilterZM(objectio.ZoneMap) {}

func (s *stubSnapshotDataSource) Close() {
	s.closed = true
}

func (s *stubSnapshotDataSource) String() string {
	return "stubSnapshotDataSource"
}

func newTestSnapshotRelData(blockCnt int) *readutil.BlockListRelData {
	relData := readutil.NewBlockListRelationData(0)
	for i := 0; i < blockCnt; i++ {
		blk := objectio.BlockInfo{}
		relData.AppendBlockInfo(&blk)
	}
	return relData
}
