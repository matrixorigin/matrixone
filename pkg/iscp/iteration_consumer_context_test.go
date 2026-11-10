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
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
			typ,
			packer,
			mp,
			1,
			0,
			1,
			0,
		)
	}()
	return done, statuses
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
		require.ErrorIs(t, err, errIterationNeedsRecovery)
	case <-time.After(time.Second):
		t.Fatal("init sql was not drained")
	}
}

func TestInitSQLRetainsFenceCauseAfterFenceRemoval(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	exec := newRuntimeTestExecutor()
	iter := testIterationContext("index_idx01")
	key := NewJobRuntimeKey(iter.accountID, iter.tableID, iter.jobNames[0], iter.jobIDs[0])
	entered, stopped, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	done, drained := make(chan error, 1), make(chan error, 1)
	go func() {
		done <- runInitSQLWithRuntime(ctx, exec, iter, func(ctx context.Context) error {
			close(entered)
			<-ctx.Done()
			close(stopped)
			<-release
			return ctx.Err()
		})
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("initializer did not start")
	}
	go func() {
		drained <- exec.CancelAndDrainJobConsumer(ctx, key.AccountID, key.TableID, key.JobName, key.JobID)
	}()
	select {
	case <-stopped:
	case <-ctx.Done():
		t.Fatal("initializer did not observe cancellation")
	}
	exec.RemoveJobFence(key)
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-drained:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("drain did not finish")
	}
	select {
	case err := <-done:
		require.ErrorIs(t, err, errIterationNeedsRecovery)
	case <-ctx.Done():
		t.Fatal("initializer did not finish")
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

// Hold the producer after consumers join so rollback removes the fence before
// the iteration can inspect the final live map. Both stop paths must retain
// their observed outcome, while a completed sibling remains unaffected.
func TestConsumerFenceRollbackRetainsRecoveryOutcome(t *testing.T) {
	for _, registered := range []bool{false, true} {
		t.Run(map[bool]string{false: "registration rejected", true: "registered consumer canceled"}[registered], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			exec := newRuntimeTestExecutor()
			iter := testIterationContext("index_fenced", "index_completed")
			key := NewJobRuntimeKey(iter.accountID, iter.tableID, iter.jobNames[0], iter.jobIDs[0])
			entered := make(chan struct{})
			readerEntered := make(chan struct{})
			producerStopped, releaseProducer := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseProducer) }) }
			changes := &iterationChangesHandle{next: func(ctx context.Context, _ *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
				close(readerEntered)
				<-ctx.Done()
				close(producerStopped)
				<-releaseProducer
				return nil, nil, engine.ChangesHandle_Tail_done, nil
			}}
			if !registered {
				exec.InstallJobFence(key, time.Minute)
			}
			stopped := fenceRollbackIterationConsumer{entered: entered}
			statuses := []*JobStatus{{LSN: 10}, {LSN: 11, Stage: JobStage_Running}}
			before := *statuses[1]
			mp := mpool.MustNewZero()
			done := make(chan []bool, 1)
			joined := false
			t.Cleanup(func() {
				if joined {
					require.Zero(t, mp.CurrNB())
					mpool.DeleteMPool(mp)
				}
			})
			t.Cleanup(func() {
				cancel()
				release()
				if !joined {
					select {
					case <-done:
						joined = true
					case <-time.After(3 * time.Second):
						t.Error("iteration did not stop during cleanup")
					}
				}
			})
			go func() {
				packer := types.NewPacker()
				result := runISCPTaskIterationConsumers(ctx, exec, iter, changes, []Consumer{stopped, readerEntryConsumer{entered: readerEntered}}, statuses, ISCPDataType_Tail, packer, mp, 1, 0, 1, 0)
				packer.Close()
				done <- result
			}()
			if registered {
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("consumer did not register")
				}
				require.NoError(t, exec.CancelAndDrainJobConsumer(ctx, key.AccountID, key.TableID, key.JobName, key.JobID))
			}
			select {
			case <-producerStopped:
			case <-ctx.Done():
				t.Fatal("consumers did not join")
			}
			exec.RemoveJobFence(key)
			require.NoError(t, exec.checkIterationFences(iter), "the final map no longer contains the rolled-back fence")
			release()
			select {
			case recovery := <-done:
				joined = true
				require.Equal(t, []bool{true, false}, recovery)
				require.Equal(t, before, *statuses[1], "completed sibling retains its outcome")
				require.Zero(t, statuses[0].ErrorCode, "an observed fence does not authorize failure publication")
			case <-ctx.Done():
				t.Fatal("iteration did not finish")
			}
			if !registered {
				select {
				case <-entered:
					t.Fatal("fenced consumer was admitted")
				default:
				}
			}
		})
	}
}

type fenceRollbackIterationConsumer struct{ entered chan struct{} }

type readerEntryConsumer struct{ entered <-chan struct{} }

func (c readerEntryConsumer) Consume(ctx context.Context, _ DataRetriever) error {
	select {
	case <-c.entered:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c fenceRollbackIterationConsumer) Consume(ctx context.Context, _ DataRetriever) error {
	close(c.entered)
	<-ctx.Done()
	return ctx.Err()
}
