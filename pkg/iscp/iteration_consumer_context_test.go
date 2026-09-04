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

package iscp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type iterationChangesHandle struct {
	next func(context.Context, *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error)
}

func (h *iterationChangesHandle) Next(ctx context.Context, mp *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	return h.next(ctx, mp)
}

func (h *iterationChangesHandle) Close() error { return nil }

type waitingIterationConsumer struct {
	entered chan struct{}
	once    chan struct{}
}

func newWaitingIterationConsumer() *waitingIterationConsumer {
	return &waitingIterationConsumer{
		entered: make(chan struct{}),
		once:    make(chan struct{}),
	}
}

func (c *waitingIterationConsumer) Consume(ctx context.Context, data DataRetriever) error {
	d := data.Next()
	defer d.Done()
	close(c.entered)
	<-c.once
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Second):
		return errors.New("consumer context was not canceled")
	}
}

type failingIterationConsumer struct {
	err error
}

func (c failingIterationConsumer) Consume(context.Context, DataRetriever) error {
	return c.err
}

type drainingIterationConsumer struct{}

func (c drainingIterationConsumer) Consume(ctx context.Context, data DataRetriever) error {
	for {
		d := data.Next()
		noMoreData := d.noMoreData
		err := d.err
		d.Done()
		if err != nil {
			return err
		}
		if noMoreData {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

type boundaryOnlyIterationConsumer struct {
	gotBoundary bool
}

type batchAttrsIterationConsumer struct {
	attrs []string
}

type sourceIDsIterationConsumer struct {
	sourceIDs chan uint64
}

func (c *sourceIDsIterationConsumer) Consume(_ context.Context, data DataRetriever) error {
	for {
		d := data.Next()
		if d.SourceTableID != 0 {
			c.sourceIDs <- d.SourceTableID
		}
		done := d.noMoreData
		err := d.err
		d.Done()
		if err != nil || done {
			return err
		}
	}
}

func (c *batchAttrsIterationConsumer) Consume(_ context.Context, data DataRetriever) error {
	for {
		d := data.Next()
		if d.insertBatch != nil && len(d.insertBatch.Batches) > 0 {
			c.attrs = append([]string(nil), d.insertBatch.Batches[0].Attrs...)
		}
		done := d.noMoreData
		err := d.err
		d.Done()
		if err != nil || done {
			return err
		}
	}
}

func (c *boundaryOnlyIterationConsumer) NeedsChangePayload(int8) bool { return false }

func (c *boundaryOnlyIterationConsumer) Consume(_ context.Context, data DataRetriever) error {
	d := data.Next()
	c.gotBoundary = d.noMoreData && d.insertBatch == nil && d.deleteBatch == nil && d.err == nil
	d.Done()
	return nil
}

func testIterationContext(jobNames ...string) *IterationContext {
	if len(jobNames) == 0 {
		jobNames = []string{"job"}
	}
	jobIDs := make([]uint64, len(jobNames))
	lsns := make([]uint64, len(jobNames))
	for i := range jobNames {
		jobIDs[i] = uint64(i + 1)
		lsns[i] = uint64(i + 10)
	}
	return &IterationContext{
		accountID: 1,
		tableID:   2,
		jobNames:  jobNames,
		jobIDs:    jobIDs,
		lsn:       lsns,
	}
}

func TestSharedIterationRoutesEachSourceOnlyToOwningJob(t *testing.T) {
	jobSpecs := []*JobSpec{
		{ConsumerInfo: ConsumerInfo{SrcTables: []TableInfo{{TableID: 10}, {TableID: 20}}}},
		{ConsumerInfo: ConsumerInfo{SrcTables: []TableInfo{{TableID: 10}, {TableID: 30}}}},
	}
	require.True(t, consumerAcceptsSource(jobSpecs, 0, 10))
	require.True(t, consumerAcceptsSource(jobSpecs, 0, 20))
	require.False(t, consumerAcceptsSource(jobSpecs, 0, 30))
	require.True(t, consumerAcceptsSource(jobSpecs, 1, 10))
	require.False(t, consumerAcceptsSource(jobSpecs, 1, 20))
	require.True(t, consumerAcceptsSource(jobSpecs, 1, 30))
}

func TestSharedIterationFanoutRoutesOverlappingSourceSets(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	makeChanges := func(sourceID uint64) *iterationChangesHandle {
		sent := false
		return &iterationChangesHandle{next: func(context.Context, *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
			if sent {
				return nil, nil, engine.ChangesHandle_Tail_done, nil
			}
			sent = true
			bat := batch.NewWithSize(2)
			bat.Vecs[0] = vector.NewVec(types.T_TS.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_Rowid.ToType())
			var block types.Blockid
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.BuildTS(sourceID, 0), false, mp))
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], types.NewRowid(&block, uint32(sourceID)), false, mp))
			bat.SetRowCount(1)
			return bat, nil, engine.ChangesHandle_Tail_done, nil
		}}
	}

	iterCtx := &IterationContext{
		accountID: 1,
		tableID:   2,
		jobNames:  []string{"mv_ab", "mv_ac"},
		jobIDs:    []uint64{1, 2},
		lsn:       []uint64{1, 2},
	}
	jobSpecs := []*JobSpec{
		{ConsumerInfo: ConsumerInfo{SrcTables: []TableInfo{{TableID: 10}, {TableID: 20}}}},
		{ConsumerInfo: ConsumerInfo{SrcTables: []TableInfo{{TableID: 10}, {TableID: 30}}}},
	}
	consumers := []*sourceIDsIterationConsumer{
		{sourceIDs: make(chan uint64, 4)},
		{sourceIDs: make(chan uint64, 4)},
	}
	streams := []iterationSourceChanges{
		{tableID: 10, changes: makeChanges(10)},
		{tableID: 20, changes: makeChanges(20)},
		{tableID: 30, changes: makeChanges(30)},
	}
	packer := types.NewPacker()
	defer packer.Close()

	runISCPTaskIterationConsumers(
		context.Background(), nil, iterCtx, streams,
		[]Consumer{consumers[0], consumers[1]},
		[]*JobStatus{{}, {}}, nil, ISCPDataType_Tail,
		packer, mp, 0, 1, 0, 1, false, jobSpecs,
	)

	var got0, got1 []uint64
	for len(consumers[0].sourceIDs) > 0 {
		got0 = append(got0, <-consumers[0].sourceIDs)
	}
	for len(consumers[1].sourceIDs) > 0 {
		got1 = append(got1, <-consumers[1].sourceIDs)
	}
	require.ElementsMatch(t, []uint64{10, 20}, got0)
	require.ElementsMatch(t, []uint64{10, 30}, got1)
}

func runIterationConsumersForTest(
	ctx context.Context,
	iterCtx *IterationContext,
	changes engine.ChangesHandle,
	consumers []Consumer,
	typ int8,
	mp *mpool.MPool,
) <-chan struct{} {
	done, _ := runIterationConsumersWithStatusesForTest(ctx, iterCtx, changes, consumers, typ, mp)
	return done
}

func runIterationConsumersWithStatusesForTest(
	ctx context.Context,
	iterCtx *IterationContext,
	changes engine.ChangesHandle,
	consumers []Consumer,
	typ int8,
	mp *mpool.MPool,
) (<-chan struct{}, []*JobStatus) {
	done := make(chan struct{})
	statuses := make([]*JobStatus, len(consumers))
	for i := range statuses {
		statuses[i] = &JobStatus{}
	}
	packer := types.NewPacker()
	go func() {
		defer close(done)
		defer packer.Close()
		runISCPTaskIterationConsumers(
			ctx,
			nil,
			iterCtx,
			changes,
			consumers,
			statuses,
			nil,
			typ,
			packer,
			mp,
			1,
			0,
			1,
			0,
			false,
			nil,
		)
	}()
	return done, statuses
}

func TestRunIterationSkipsChangesForBoundaryOnlyConsumers(t *testing.T) {
	nextCalled := false
	changes := &iterationChangesHandle{next: func(
		context.Context,
		*mpool.MPool,
	) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
		nextCalled = true
		return nil, nil, engine.ChangesHandle_Snapshot, nil
	}}
	consumer := &boundaryOnlyIterationConsumer{}
	mp := mpool.MustNewZero()
	done := runIterationConsumersForTest(
		context.Background(),
		testIterationContext(),
		changes,
		[]Consumer{consumer},
		ISCPDataType_Snapshot,
		mp,
	)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("boundary-only iteration did not finish")
	}
	require.False(t, nextCalled)
	require.True(t, consumer.gotBoundary)
}

func TestRunIterationKeepsChangesForMixedConsumers(t *testing.T) {
	nextCalled := false
	changes := &iterationChangesHandle{next: func(
		context.Context,
		*mpool.MPool,
	) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
		nextCalled = true
		return nil, nil, engine.ChangesHandle_Snapshot, nil
	}}
	boundaryConsumer := &boundaryOnlyIterationConsumer{}
	done := runIterationConsumersForTest(
		context.Background(),
		testIterationContext("boundary", "legacy"),
		changes,
		[]Consumer{boundaryConsumer, drainingIterationConsumer{}},
		ISCPDataType_Snapshot,
		mpool.MustNewZero(),
	)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("mixed-consumer iteration did not finish")
	}
	require.True(t, nextCalled)
	require.True(t, boundaryConsumer.gotBoundary)
}

func TestRunIterationRestoresAttrsWithRetainedRowID(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	bat := batch.NewWithSize(4)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[3] = vector.NewVec(types.T_TS.ToType())
	var block types.Blockid
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.NewRowid(&block, 0), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[2], int64(2), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[3], types.BuildTS(1, 0), false, mp))
	bat.SetRowCount(1)

	sent := false
	changes := &iterationChangesHandle{next: func(context.Context, *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
		if sent {
			return nil, nil, engine.ChangesHandle_Tail_done, nil
		}
		sent = true
		return bat, nil, engine.ChangesHandle_Tail_done, nil
	}}
	consumer := &batchAttrsIterationConsumer{}
	packer := types.NewPacker()
	defer packer.Close()
	runISCPTaskIterationConsumers(
		context.Background(), nil, testIterationContext(), changes,
		[]Consumer{consumer}, []*JobStatus{{}},
		&planpb.TableDef{Cols: []*planpb.ColDef{
			{Name: "event_id"}, {Name: "bytes_sent"}, {Name: objectio.DefaultCommitTS_Attr},
		}},
		ISCPDataType_Tail, packer, mp, 3, 0, 2, 1, true,
		nil,
	)
	require.Equal(t, []string{
		catalog.Row_ID, "event_id", "bytes_sent", objectio.DefaultCommitTS_Attr,
	}, consumer.attrs)
	require.Zero(t, mp.CurrNB())
}

func TestRunInitSQLWithRuntimeCancelInFlightInitSQL(t *testing.T) {
	exec := newRuntimeTestExecutor()
	iterCtx := testIterationContext("index_idx01")
	key := NewJobRuntimeKey(iterCtx.accountID, iterCtx.tableID, iterCtx.jobNames[0], iterCtx.jobIDs[0])
	entered := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		done <- runInitSQLWithRuntime(context.Background(), exec, iterCtx, func(ctx context.Context) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		})
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("init sql did not start")
	}
	require.NoError(t, exec.CancelAndDrainJobConsumer(context.Background(), key.AccountID, key.TableID, key.JobName, key.JobID))
	require.True(t, exec.IsJobFenced(key))

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("init sql was not drained")
	}
}

func TestRunInitSQLWithRuntimeSkipsFencedInitSQL(t *testing.T) {
	exec := newRuntimeTestExecutor()
	iterCtx := testIterationContext("index_idx01")
	key := NewJobRuntimeKey(iterCtx.accountID, iterCtx.tableID, iterCtx.jobNames[0], iterCtx.jobIDs[0])
	exec.fencedJobs[key] = JobFence{ExpireAt: time.Now().Add(time.Minute)}

	called := false
	err := runInitSQLWithRuntime(context.Background(), exec, iterCtx, func(context.Context) error {
		called = true
		return nil
	})

	require.ErrorIs(t, err, errInitSQLJobFenced)
	require.False(t, called)
}

type iscpTxnForTest struct {
	client.TxnOperator
	commitErr         error
	rollbackErr       error
	committed         bool
	rolledBack        bool
	commitCtx         context.Context
	rollbackCtx       context.Context
	rollbackErrAtCall error
}

func (t *iscpTxnForTest) Commit(ctx context.Context) error {
	t.committed = true
	t.commitCtx = ctx
	return t.commitErr
}

func (t *iscpTxnForTest) Rollback(ctx context.Context) error {
	t.rolledBack = true
	t.rollbackCtx = ctx
	t.rollbackErrAtCall = ctx.Err()
	return t.rollbackErr
}

func TestFinishISCPTransactionReturnsCommitError(t *testing.T) {
	commitErr := errors.New("commit failed")
	txn := &iscpTxnForTest{commitErr: commitErr}

	err := finishISCPTransaction(context.Background(), txn, nil)

	require.ErrorIs(t, err, commitErr)
	require.True(t, txn.committed)
	require.False(t, txn.rolledBack)
}

func TestFinishISCPTransactionRollsBackWithIndependentContext(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	txn := &iscpTxnForTest{}

	err := finishISCPTransaction(parent, txn, context.Canceled)

	require.ErrorIs(t, err, context.Canceled)
	require.True(t, txn.rolledBack)
	require.False(t, txn.committed)
	require.NotNil(t, txn.rollbackCtx)
	require.NoError(t, txn.rollbackErrAtCall)
}

func TestRunISCPTaskIterationConsumersCancelSnapshotInFlightConsumer(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bat := testutil.NewBatchWithVectors(
		[]*vector.Vector{
			testutil.NewVector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{1}),
			testutil.NewVector(1, types.T_TS.ToType(), proc.Mp(), false, []types.TS{types.BuildTS(1, 0)}),
		}, nil)

	sent := false
	changes := &iterationChangesHandle{next: func(ctx context.Context, _ *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
		if !sent {
			sent = true
			return bat, nil, engine.ChangesHandle_Snapshot, nil
		}
		<-ctx.Done()
		return nil, nil, engine.ChangesHandle_Snapshot, ctx.Err()
	}}
	consumer := newWaitingIterationConsumer()
	done := runIterationConsumersForTest(ctx, testIterationContext(), changes, []Consumer{consumer}, ISCPDataType_Snapshot, proc.Mp())

	<-consumer.entered
	close(consumer.once)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("iteration did not cancel in-flight snapshot consumer")
	}
}

func TestRunISCPTaskIterationConsumersCancelTailFinalizationConsumer(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changes := &iterationChangesHandle{next: func(context.Context, *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
		return nil, nil, engine.ChangesHandle_Tail_done, nil
	}}
	consumer := newWaitingIterationConsumer()
	done := runIterationConsumersForTest(ctx, testIterationContext(), changes, []Consumer{consumer}, ISCPDataType_Tail, proc.Mp())

	<-consumer.entered
	close(consumer.once)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("iteration did not cancel tail finalization consumer")
	}
}

func TestRunISCPTaskIterationConsumersSiblingFailureDoesNotFailHealthyConsumer(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := context.Background()

	changes := &iterationChangesHandle{next: func(context.Context, *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
		return nil, nil, engine.ChangesHandle_Tail_done, nil
	}}
	done, statuses := runIterationConsumersWithStatusesForTest(
		ctx,
		testIterationContext("healthy", "failing"),
		changes,
		[]Consumer{
			drainingIterationConsumer{},
			failingIterationConsumer{err: errors.New("sibling failed")},
		},
		ISCPDataType_Tail,
		proc.Mp(),
	)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("shared iteration did not finish after one sibling failed")
	}
	require.Zero(t, statuses[0].ErrorCode)
	require.Empty(t, statuses[0].ErrorMsg)
	require.NotZero(t, statuses[1].ErrorCode)
	require.Contains(t, statuses[1].ErrorMsg, "sibling failed")
}
