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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestPartitionChangesHandleCloseWithTypedNil(t *testing.T) {
	var handle engine.ChangesHandle = (*PartitionChangesHandle)(nil)
	require.NoError(t, handle.Close())
}

func TestUseBoundedVisibleStateRangeForEmptyWatermark(t *testing.T) {
	base := context.Background()
	require.False(t, useBoundedVisibleStateRange(base))
	require.False(t, useBoundedVisibleStateRange(
		engine.WithSnapshotReadPolicy(base, engine.SnapshotReadPolicyVisibleState),
	))

	bounded := engine.WithChangeRangeLimit(base, engine.ChangeRangeLimit{
		MaxInMemoryBytes: 64 << 20,
	})
	require.False(t, useBoundedVisibleStateRange(bounded))
	require.True(t, useBoundedVisibleStateRange(
		engine.WithSnapshotReadPolicy(bounded, engine.SnapshotReadPolicyVisibleState),
	))
}

func TestCollectChangesRoutesBoundedEmptyWatermarkToVisibleStateRange(t *testing.T) {
	original := newPartitionChangesHandle
	t.Cleanup(func() { newPartitionChangesHandle = original })
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	_, err := original(
		context.Background(), &txnTable{}, types.BuildTS(1, 0), types.TS{}, false,
		engine.SnapshotReadPolicyVisibleState, mp,
	)
	require.Error(t, err)

	want := &stubChangesHandle{}
	called := false
	newPartitionChangesHandle = func(
		_ context.Context,
		_ *txnTable,
		from, to types.TS,
		skipDeletes bool,
		policy engine.SnapshotReadPolicy,
		_ *mpool.MPool,
	) (engine.ChangesHandle, error) {
		called = true
		require.True(t, from.IsEmpty())
		require.Equal(t, types.BuildTS(20, 0), to)
		require.False(t, skipDeletes)
		require.Equal(t, engine.SnapshotReadPolicyVisibleState, policy)
		return want, nil
	}
	ctx := engine.WithSnapshotReadPolicy(context.Background(), engine.SnapshotReadPolicyVisibleState)
	ctx = engine.WithChangeRangeLimit(ctx, engine.ChangeRangeLimit{MaxInMemoryBytes: 64 << 20})

	got, err := (&txnTable{}).CollectChanges(
		ctx, types.TS{}, types.BuildTS(20, 0), false, mp,
	)
	require.NoError(t, err)
	require.True(t, called)
	require.Same(t, want, got)
}

func TestCollectChangesInitializesLogicalSeqnumsBeforeBuildingRange(t *testing.T) {
	original := newPartitionChangesHandle
	t.Cleanup(func() { newPartitionChangesHandle = original })
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	var got []uint16
	newPartitionChangesHandle = func(
		_ context.Context, tbl *txnTable, _, _ types.TS, _ bool,
		_ engine.SnapshotReadPolicy, _ *mpool.MPool,
	) (engine.ChangesHandle, error) {
		got = append([]uint16(nil), tbl.seqnums...)
		return &stubChangesHandle{}, nil
	}
	tbl := &txnTable{tableDef: &plan2.TableDef{Cols: []*plan2.ColDef{
		{Name: "added", Typ: plan2.Type{Id: int32(types.T_int32)}, Seqnum: 2},
		{Name: "id", Typ: plan2.Type{Id: int32(types.T_int32)}, Seqnum: 0},
		{Name: "payload", Typ: plan2.Type{Id: int32(types.T_varchar)}, Seqnum: 1},
		{Name: catalog.Row_ID, Typ: plan2.Type{Id: int32(types.T_Rowid)}, Seqnum: uint32(objectio.SEQNUM_ROWID)},
	}}}
	ctx := engine.WithSnapshotReadPolicy(context.Background(), engine.SnapshotReadPolicyVisibleState)
	ctx = engine.WithChangeRangeLimit(ctx, engine.ChangeRangeLimit{MaxInMemoryBytes: 64 << 20})
	_, err := tbl.CollectChanges(ctx, types.TS{}, types.BuildTS(20, 0), false, mp)
	require.NoError(t, err)
	require.Equal(t, []uint16{2, 0, 1}, got)
}

func TestPartitionChangesHandleCloseClosesCurrentHandle(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	stub := &stubChangesHandle{}
	handle := &PartitionChangesHandle{
		mp:                  mp,
		currentChangeHandle: stub,
	}

	require.NoError(t, handle.Close())
	require.True(t, stub.closed)
	require.Nil(t, handle.currentChangeHandle)
}

func TestPartitionChangesHandleCloseCleansBufferedBatches(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	baseline := mp.CurrNB()

	data := batch.NewWithSize(1)
	data.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(data.Vecs[0], int64(1), false, mp))
	data.SetRowCount(1)

	resources := newTestVisibleStateResources()
	resources.reserved = 123
	stub := &stubChangesHandle{}
	handle := &PartitionChangesHandle{
		mp: mp, visibleResources: resources, currentChangeHandle: stub,
		bufferedBatches: []queuedChangeBatch{{data: data, reservedBytes: 123}},
	}

	require.NoError(t, handle.Close())
	require.True(t, stub.closed)
	require.Empty(t, handle.bufferedBatches)
	require.Zero(t, resources.reserved)
	require.Equal(t, baseline, mp.CurrNB())
}

func TestPartitionChangesHandleDelegatesOneBatchPerNext(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(mp)

	const upstreamBatches = 1_000_000
	stub := &stubChangesHandle{remaining: upstreamBatches}
	handle := &PartitionChangesHandle{
		snapshotReadPolicy:  engine.SnapshotReadPolicyVisibleState,
		currentChangeHandle: stub,
	}

	gotData, gotTombstone, hint, err := handle.Next(context.Background(), mp)
	require.NoError(t, err)
	require.NotNil(t, gotData)
	require.Nil(t, gotTombstone)
	require.Equal(t, engine.ChangesHandle_Tail_done, hint)
	require.Equal(t, 1, stub.calls, "first Next must not drain the requested range")
	require.Equal(t, upstreamBatches-1, stub.remaining)
	require.LessOrEqual(t, mp.CurrNB(), int64(1<<20), "retained mpool memory must stay batch-bounded")
	gotData.Clean(mp)
	require.Zero(t, mp.CurrNB(), "the partition handle must not retain prior batches")
}

func TestPartitionChangesHandleVisibleStateResourcesUseBufferedReplay(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	data := batch.NewWithSize(1)
	data.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(data.Vecs[0], int64(7), false, mp))
	data.SetRowCount(1)
	defer data.Clean(mp)

	resources := newTestVisibleStateResources()
	resources.reserved = 77
	handle := &PartitionChangesHandle{
		snapshotReadPolicy: engine.SnapshotReadPolicyVisibleState,
		visibleResources:   resources,
		bufferedBatches: []queuedChangeBatch{{
			data: data, hint: engine.ChangesHandle_Snapshot, reservedBytes: 77,
		}},
	}

	gotData, gotTombstone, hint, err := handle.Next(context.Background(), mp)
	require.NoError(t, err)
	require.Same(t, data, gotData)
	require.Nil(t, gotTombstone)
	require.Equal(t, engine.ChangesHandle_Snapshot, hint)
	require.Empty(t, handle.bufferedBatches)
	require.Zero(t, resources.reserved)
}

func TestPartitionChangesHandlePreservesHistoryContext(t *testing.T) {
	base := context.Background()
	require.False(t, engine.CollectChangesPreserveAllVersionsFromContext(
		(&PartitionChangesHandle{}).collectChangesContext(base),
	))
	require.True(t, engine.CollectChangesPreserveAllVersionsFromContext(
		(&PartitionChangesHandle{preserveAllVersions: true}).collectChangesContext(base),
	))
}

func TestInitializeVisibleStateRangeRecovery(t *testing.T) {
	missing := moerr.NewFileNotFoundNoCtx("gc-ed-object")
	recoveryCalls := 0
	used, err := initializeVisibleStateRange(
		func() error { return missing },
		func(got error) error {
			recoveryCalls++
			require.Equal(t, missing, got)
			return nil
		},
	)
	require.NoError(t, err)
	require.True(t, used)
	require.Equal(t, 1, recoveryCalls)

	internal := moerr.NewInternalErrorNoCtx("not recoverable")
	used, err = initializeVisibleStateRange(
		func() error { return internal },
		func(error) error {
			t.Fatal("unexpected visible-state recovery")
			return nil
		},
	)
	require.Equal(t, internal, err)
	require.False(t, used)
}

func TestDeferredChangesHandleBuildsOnFirstNext(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(mp)
	builds := 0
	handle := &deferredChangesHandle{
		build: func(context.Context) (engine.ChangesHandle, error) {
			builds++
			return &stubChangesHandle{remaining: 1}, nil
		},
	}

	require.Zero(t, builds, "CollectChanges must not materialize the range")
	data, tombstone, _, err := handle.Next(context.Background(), mp)
	require.NoError(t, err)
	require.Equal(t, 1, builds)
	require.NotNil(t, data)
	require.Nil(t, tombstone)
	data.Clean(mp)
	require.NoError(t, handle.Close())
	require.Zero(t, mp.CurrNB())
}

type stubChangesHandle struct {
	closed    bool
	calls     int
	remaining int
}

func (s *stubChangesHandle) Next(_ context.Context, mp *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	s.calls++
	if s.remaining == 0 {
		return nil, nil, engine.ChangesHandle_Tail_done, nil
	}
	s.remaining--
	data := batch.NewWithSize(1)
	data.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	const rowsPerBatch = 4096
	if err := data.Vecs[0].PreExtend(rowsPerBatch, mp); err != nil {
		return nil, nil, engine.ChangesHandle_Tail_done, err
	}
	for row := 0; row < rowsPerBatch; row++ {
		if err := vector.AppendFixed(data.Vecs[0], int64(s.calls), false, mp); err != nil {
			return nil, nil, engine.ChangesHandle_Tail_done, err
		}
	}
	data.SetRowCount(rowsPerBatch)
	return data, nil, engine.ChangesHandle_Tail_done, nil
}

func (s *stubChangesHandle) Close() error {
	s.closed = true
	return nil
}
