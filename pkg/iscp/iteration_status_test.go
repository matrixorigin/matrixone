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

package iscp

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestUnfinishedIterationPreservesCompletedSiblings(t *testing.T) {
	iter := NewIterationContext(0, 10, []string{"done", "pending"}, []uint64{1, 2}, []uint64{7, 9}, types.BuildTS(10, 1), types.BuildTS(20, 1))
	specs := []*JobSpec{{ConsumerInfo: ConsumerInfo{IndexName: "done"}}, {ConsumerInfo: ConsumerInfo{IndexName: "pending"}}}
	statuses := []*JobStatus{{LSN: 7}, {LSN: 8}}
	out, kept, _, err := unfinishedIteration(iter, specs, statuses, []int8{ISCPJobState_Completed, ISCPJobState_Completed})
	require.NoError(t, err)
	require.Equal(t, []string{"pending"}, out.jobNames)
	require.Equal(t, []uint64{2}, out.jobIDs)
	require.Equal(t, []uint64{9}, out.lsn)
	require.Same(t, specs[1], kept[0])
	require.Equal(t, iter.fromTS, out.fromTS)
	// A failed/repaired completed iteration is terminal too; the scheduler
	// retries its safe watermark in a new LSN, rather than replaying this one.
	statuses[0].ErrorCode = 1
	out, _, _, err = unfinishedIteration(iter, specs, statuses, []int8{ISCPJobState_Completed, ISCPJobState_Completed})
	require.NoError(t, err)
	require.Equal(t, []string{"pending"}, out.jobNames)
	statuses[1].LSN = 9
	_, _, _, err = unfinishedIteration(iter, specs, statuses, []int8{ISCPJobState_Completed, ISCPJobState_Running})
	require.ErrorIs(t, err, errIterationNeedsRecovery, "same LSN is insufficient to claim another runner's work")
	statuses[1].LSN = 10
	out, _, _, err = unfinishedIteration(iter, specs, statuses, []int8{ISCPJobState_Completed, ISCPJobState_Running})
	require.NoError(t, err)
	require.Nil(t, out, "newer durable work supersedes this iteration")
	require.Equal(t, types.TS{}, previousWatermark(types.TS{}))
	require.Equal(t, types.BuildTS(10, 0), previousWatermark(iter.fromTS))
}

type statusTestEngine struct{ engine.Engine }

func (statusTestEngine) LatestLogtailAppliedTime() timestamp.Timestamp { return timestamp.Timestamp{} }
func (statusTestEngine) New(context.Context, client.TxnOperator) error { return nil }

type statusTestClient struct {
	client.TxnClient
	ops  []*statusTestTxn
	next int
}

func (c *statusTestClient) New(context.Context, timestamp.Timestamp, ...client.TxnOption) (client.TxnOperator, error) {
	op := c.ops[c.next]
	c.next++
	return op, nil
}

type statusTestTxn struct {
	client.TxnOperator
	commits, rollbacks int
	commitErr          error
}

func (t *statusTestTxn) Commit(context.Context) error   { t.commits++; return t.commitErr }
func (t *statusTestTxn) Rollback(context.Context) error { t.rollbacks++; return nil }

func durableStatusResult(t *testing.T, mp *mpool.MPool, status *JobStatus, state int8, watermark types.TS) executor.Result {
	t.Helper()
	mem := executor.NewMemResult([]types.Type{types.T_json.ToType(), types.T_int8.ToType(), types.T_varchar.ToType()}, mp)
	if status != nil {
		text, err := MarshalJobStatus(status)
		require.NoError(t, err)
		mem.NewBatchWithRowCount(1)
		require.NoError(t, executor.AppendBytesRows(mem, 0, encodeJSONRows(t, []string{text})))
		require.NoError(t, executor.AppendFixedRows(mem, 1, []int8{state}))
		require.NoError(t, executor.AppendStringRows(mem, 2, []string{watermark.ToString()}))
	}
	return mem.GetResult()
}

func TestFinalPublicationReconcilesDurableOutcome(t *testing.T) {
	for _, tc := range []struct {
		name             string
		status           *JobStatus
		state            int8
		ackLost, wantErr bool
	}{
		{name: "committed output acknowledgement lost", status: &JobStatus{LSN: 7}, state: ISCPJobState_Completed, ackLost: true},
		{name: "completed output rejects error overwrite", status: &JobStatus{LSN: 7}, state: ISCPJobState_Completed},
		{name: "successor", status: &JobStatus{LSN: 8}, state: ISCPJobState_Running},
		{name: "retired"},
		{name: "running needs retry", status: &JobStatus{LSN: 7}, state: ISCPJobState_Running, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
			write, read := &statusTestTxn{}, &statusTestTxn{}
			if tc.ackLost {
				write.commitErr = moerr.NewInternalErrorNoCtx("lost acknowledgement")
			}
			tcClient := &statusTestClient{ops: []*statusTestTxn{write, read}}
			old := ExecWithResult
			t.Cleanup(func() { ExecWithResult = old })
			updates := 0
			ExecWithResult = func(_ context.Context, sql, _ string, _ client.TxnOperator) (executor.Result, error) {
				if updates == 0 {
					updates++
					require.Contains(t, sql, "AND job_state = 2")
					if tc.ackLost {
						return executor.Result{AffectedRows: 1}, nil
					}
					return executor.Result{}, nil
				}
				require.Contains(t, sql, "SELECT job_status, job_state, watermark")
				return durableStatusResult(t, mp, tc.status, tc.state, types.BuildTS(20, 0)), nil
			}
			err := FlushJobStatusOnIterationState(t.Context(), "", statusTestEngine{}, tcClient, 0, 10, []string{"idx"}, []uint64{3}, []uint64{7}, []*JobStatus{{LSN: 7, ErrorCode: 1}}, types.BuildTS(10, 0), ISCPJobState_Completed, []uint64{7})
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, updates, "reconciliation must not rewrite the completed output")
			require.Equal(t, 1, read.commits)
			if tc.ackLost {
				require.Equal(t, 1, write.commits)
			} else {
				require.Equal(t, 1, write.rollbacks)
			}
		})
	}
}

func TestIdleWatermarkDoesNotReplaceInitializationState(t *testing.T) {
	old := ExecWithResult
	t.Cleanup(func() { ExecWithResult = old })
	table := NewTableEntry(&ISCPTaskExecutor{}, 0, 1, 2, "db", "t")
	job := NewJobEntry(table, "idx", &JobSpec{}, 3, types.BuildTS(10, 0), ISCPJobState_Completed, 0)
	job.watermark = types.BuildTS(20, 0)
	ExecWithResult = func(_ context.Context, sql, _ string, _ client.TxnOperator) (executor.Result, error) {
		require.Contains(t, sql, "JSON_SET(job_status, '$.LSN'")
		require.NotContains(t, sql, "$.Stage")
		return executor.Result{AffectedRows: 1}, nil
	}
	flush, err := job.tryFlushWatermark(t.Context(), nil, 0)
	require.NoError(t, err)
	require.True(t, flush)
	require.Equal(t, ISCPJobState_Completed, job.state)
	require.Equal(t, types.BuildTS(10, 0), job.persistedWatermark, "in-memory progress must wait for commit")
	ExecWithResult = func(context.Context, string, string, client.TxnOperator) (executor.Result, error) {
		return executor.Result{}, nil
	}
	_, err = job.tryFlushWatermark(t.Context(), nil, 0)
	require.Error(t, err)
	require.False(t, errors.Is(err, context.Canceled))
}

func TestDataRetrieverCancelAfterClose(t *testing.T) {
	r := NewDataRetriever(t.Context(), 0, 1, "idx", 1, &JobStatus{}, 1, ISCPDataType_Snapshot)
	r.Close()
	cause := moerr.NewInternalErrorNoCtx("retired generation")
	r.Cancel(cause)
	require.True(t, r.IsCanceled())
	require.ErrorIs(t, r.terminalError(), cause)
}

func TestWorkerDoesNotCompleteUnownedRunningGeneration(t *testing.T) {
	// This durable row can result either from another runner admitting queued
	// work during lease handoff, or from this runner losing its admission ACK.
	for _, why := range []string{"queued runner handoff", "admission acknowledgement lost"} {
		t.Run(why, func(t *testing.T) {
			res, mp := newISCPLogResult(t, []iscpLogBatch{{jobNames: []string{"index_idx"}, jobIDs: []uint64{3}, jobSpecs: []string{mustMarshalJobSpec(t, "idx")}, jobStatuss: []string{mustMarshalJobStatus(t, 7, JobStage_Init)}}})
			vector.MustFixedColWithTypeCheck[int8](res.Batches[0].Vecs[4])[0] = ISCPJobState_Running
			old := ExecWithResult
			t.Cleanup(func() { ExecWithResult = old; require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
			reads := 0
			ExecWithResult = func(_ context.Context, sql, _ string, _ client.TxnOperator) (executor.Result, error) {
				require.Contains(t, sql, "SELECT job_name, job_id, job_spec", "unknown admission never authorizes a status write")
				reads++
				return res, nil
			}
			txn := &statusTestTxn{}
			w := &worker{ctx: t.Context(), cnEngine: statusTestEngine{}, cnTxnClient: &statusTestClient{ops: []*statusTestTxn{txn}}}
			w.onItem(NewIterationContext(0, 10, []string{"index_idx"}, []uint64{3}, []uint64{7}, types.TS{}, types.TS{}))
			require.Equal(t, 1, reads, "uncertain ownership stops before retry or failure publication")
			require.Equal(t, 1, txn.rollbacks)
		})
	}
}
