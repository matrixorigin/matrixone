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
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type tailGroupResult struct {
	inserts, deletes             int
	insertBatches, deleteBatches int
}

type recordingTailConsumer struct {
	groups    []tailGroupResult
	sourceIDs []uint64
}

func (c *recordingTailConsumer) Consume(ctx context.Context, retriever DataRetriever) error {
	for {
		data := retriever.Next()
		if data == nil {
			return ctx.Err()
		}
		if data.err != nil {
			err := data.err
			data.Done()
			return err
		}
		if data.noMoreData {
			data.Done()
			return nil
		}
		group := tailGroupResult{
			inserts:       data.insertBatch.Rows.Len(),
			deletes:       data.deleteBatch.Rows.Len(),
			insertBatches: len(data.insertBatch.Batches),
			deleteBatches: len(data.deleteBatch.Batches),
		}
		c.groups = append(c.groups, group)
		c.sourceIDs = append(c.sourceIDs, data.SourceTableID)
		data.Done()
	}
}

func TestISCPMultiSourceTailGroupsStaySeparate(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	makeSource := func(first int64) engine.ChangesHandle {
		step := 0
		return &iterationChangesHandle{next: func(context.Context, *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
			step++
			switch step {
			case 1:
				return tailGroupBatch(mp, first, first), nil, engine.ChangesHandle_Tail_wip, nil
			case 2:
				return tailGroupBatch(mp, first+1, first+1), nil, engine.ChangesHandle_Tail_done, nil
			default:
				return nil, nil, engine.ChangesHandle_Tail_done, nil
			}
		}}
	}
	streams := []iterationSourceChanges{
		{tableID: 11, changes: makeSource(1)},
		{tableID: 22, changes: makeSource(3)},
	}
	consumer := new(recordingTailConsumer)
	statuses := []*JobStatus{{}}
	packer := types.NewPacker()
	defer packer.Close()
	runISCPTaskIterationConsumers(t.Context(), nil, testIterationContext(), streams,
		[]Consumer{consumer}, statuses, nil, ISCPDataType_Tail, packer, mp,
		1, 0, 1, 0, false, nil)
	require.Zero(t, statuses[0].ErrorCode)
	require.Equal(t, []uint64{11, 22}, consumer.sourceIDs)
	require.Equal(t, []tailGroupResult{{inserts: 2, insertBatches: 2}, {inserts: 2, insertBatches: 2}}, consumer.groups)
	require.Zero(t, mp.CurrNB())
}

func tailGroupBatch(mp *mpool.MPool, pk int64, ts int64) *batch.Batch {
	return testutil.NewBatchWithVectors([]*vector.Vector{
		testutil.NewVector(1, types.T_int64.ToType(), mp, false, []int64{pk}),
		testutil.NewVector(1, types.T_TS.ToType(), mp, false, []types.TS{types.BuildTS(ts, 0)}),
	}, nil)
}

func TestISCPConsumesTailWIPAsOneCompleteGroup(t *testing.T) {
	for _, kind := range []string{"insert", "delete"} {
		t.Run(kind, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			step := 0
			changes := &iterationChangesHandle{next: func(context.Context, *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
				step++
				switch step {
				case 1, 2:
					bat := tailGroupBatch(mp, int64(step), int64(step))
					if kind == "insert" {
						if step == 1 {
							return bat, nil, engine.ChangesHandle_Tail_wip, nil
						}
						return bat, nil, engine.ChangesHandle_Tail_done, nil
					}
					if step == 1 {
						return nil, bat, engine.ChangesHandle_Tail_wip, nil
					}
					return nil, bat, engine.ChangesHandle_Tail_done, nil
				default:
					return nil, nil, engine.ChangesHandle_Tail_done, nil
				}
			}}
			consumer := new(recordingTailConsumer)
			done, statuses := runIterationConsumersWithStatusesForTest(context.Background(), testIterationContext(), changes, []Consumer{consumer}, ISCPDataType_Tail, mp)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("tail group did not finish")
			}
			require.Zero(t, statuses[0].ErrorCode)
			require.Len(t, consumer.groups, 1)
			if kind == "insert" {
				require.Equal(t, tailGroupResult{inserts: 2, insertBatches: 2}, consumer.groups[0])
			} else {
				require.Equal(t, tailGroupResult{deletes: 2, deleteBatches: 2}, consumer.groups[0])
			}
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestISCPRejectsIncompleteTailWIPWithoutPublishing(t *testing.T) {
	for _, ending := range []string{"eof", "error", "error-with-batch", "unknown-hint", "snapshot-mix"} {
		t.Run(ending, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			step := 0
			changes := &iterationChangesHandle{next: func(context.Context, *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
				step++
				if step == 1 {
					return tailGroupBatch(mp, 1, 1), nil, engine.ChangesHandle_Tail_wip, nil
				}
				if ending == "error" {
					return nil, nil, engine.ChangesHandle_Tail_done, errors.New("source read failed")
				}
				if ending == "error-with-batch" {
					return tailGroupBatch(mp, 2, 2), nil, engine.ChangesHandle_Tail_done, errors.New("source read failed")
				}
				if ending == "unknown-hint" {
					return tailGroupBatch(mp, 2, 2), nil, engine.ChangesHandle_Hint(99), nil
				}
				if ending == "snapshot-mix" {
					return tailGroupBatch(mp, 2, 2), nil, engine.ChangesHandle_Snapshot, nil
				}
				return nil, nil, engine.ChangesHandle_Tail_done, nil
			}}
			consumer := new(recordingTailConsumer)
			done, statuses := runIterationConsumersWithStatusesForTest(context.Background(), testIterationContext(), changes, []Consumer{consumer}, ISCPDataType_Tail, mp)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("incomplete tail did not stop")
			}
			require.Empty(t, consumer.groups)
			require.NotZero(t, statuses[0].ErrorCode)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestISCPCancelCleansPendingTailWIP(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	readingAgain := make(chan struct{})
	step := 0
	changes := &iterationChangesHandle{next: func(ctx context.Context, _ *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
		step++
		if step == 1 {
			return tailGroupBatch(mp, 1, 1), nil, engine.ChangesHandle_Tail_wip, nil
		}
		close(readingAgain)
		<-ctx.Done()
		return nil, nil, engine.ChangesHandle_Tail_done, ctx.Err()
	}}
	consumer := new(recordingTailConsumer)
	done := runIterationConsumersForTest(ctx, testIterationContext(), changes, []Consumer{consumer}, ISCPDataType_Tail, mp)
	select {
	case <-readingAgain:
	case <-time.After(5 * time.Second):
		t.Fatal("tail reader did not retain the pending group")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancel did not stop the pending tail")
	}
	require.Empty(t, consumer.groups)
	require.Zero(t, mp.CurrNB())
}

func TestISCPIterationWaitsForPendingTailCleanup(t *testing.T) {
	for _, ending := range []string{"cancel", "incomplete-eof"} {
		t.Run(ending, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			// Closing the pending tail batch is the observable cleanup step:
			// it blocks until the test allows it.
			releaseStarted := make(chan struct{})
			allowRelease := make(chan struct{})
			released := make(chan struct{})
			var startOnce, allowOnce sync.Once
			t.Cleanup(func() { allowOnce.Do(func() { close(allowRelease) }) })
			original := closePendingTailBatch
			t.Cleanup(func() { closePendingTailBatch = original })
			closePendingTailBatch = func(atomicBatch *AtomicBatch) {
				startOnce.Do(func() { close(releaseStarted) })
				<-allowRelease
				original(atomicBatch)
				select {
				case <-released:
				default:
					close(released)
				}
			}
			firstBatch := testutil.NewBatchWithVectors([]*vector.Vector{
				testutil.NewVector(1, types.T_int64.ToType(), mp, false, []int64{1}),
				testutil.NewVector(1, types.T_TS.ToType(), mp, false, []types.TS{types.BuildTS(1, 0)}),
			}, nil)
			require.Positive(t, mp.CurrNB())

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			readingAgain := make(chan struct{})
			step := 0
			changes := &iterationChangesHandle{next: func(ctx context.Context, _ *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
				step++
				if step == 1 {
					return firstBatch, nil, engine.ChangesHandle_Tail_wip, nil
				}
				close(readingAgain)
				if ending == "cancel" {
					<-ctx.Done()
					return nil, nil, engine.ChangesHandle_Tail_done, ctx.Err()
				}
				return nil, nil, engine.ChangesHandle_Tail_done, nil
			}}
			done := runIterationConsumersForTest(ctx, testIterationContext(), changes,
				[]Consumer{new(recordingTailConsumer)}, ISCPDataType_Tail, mp)
			select {
			case <-readingAgain:
			case <-time.After(5 * time.Second):
				t.Fatal("tail reader did not reach the second source read")
			}
			if ending == "cancel" {
				cancel()
			}
			select {
			case <-releaseStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("pending tail cleanup did not start")
			}
			select {
			case <-done:
				t.Fatal("iteration returned before pending tail cleanup completed")
			case <-time.After(time.Second):
			}
			allowOnce.Do(func() { close(allowRelease) })
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("iteration did not finish after pending tail cleanup")
			}
			select {
			case <-released:
			default:
				t.Fatal("pending tail cleanup was still running when iteration returned")
			}
			require.Zero(t, mp.CurrNB())
		})
	}
}
