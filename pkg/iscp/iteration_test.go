// Copyright 2024 Matrix Origin
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
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestResolveSingleSourceInsertIndexesWithRetainedRowID(t *testing.T) {
	def := &planpb.TableDef{
		Cols:          []*planpb.ColDef{{Name: "id"}, {Name: "value"}, {Name: "__mo_cpkey"}, {Name: "__mo_commit_ts"}},
		Name2ColIndex: map[string]int32{"id": 0, "value": 1},
		Pkey:          &planpb.PrimaryKeyDef{Names: []string{"id"}},
	}

	tsIdx, pkIdx := resolveSingleSourceInsertIndexes(def, false)
	require.Equal(t, 3, tsIdx)
	require.Equal(t, 0, pkIdx)

	tsIdx, pkIdx = resolveSingleSourceInsertIndexes(def, true)
	require.Equal(t, 4, tsIdx)
	require.Equal(t, 1, pkIdx)
}

func TestResolveMVBatchIndexesUsesRetainedBatchLayout(t *testing.T) {
	def := &planpb.TableDef{
		Pkey: &planpb.PrimaryKeyDef{Names: []string{"id"}},
	}
	bat := batch.NewWithSize(4)
	bat.Attrs = []string{"__mo_rowid", "id", "service", "__mo_commit_ts"}
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[3] = vector.NewVec(types.T_TS.ToType())
	defer bat.Clean(nil)

	tsIdx, pkIdx := resolveMVBatchIndexes(bat, def, true, true)
	require.Equal(t, 3, tsIdx)
	require.Equal(t, 0, pkIdx)
}

func TestAtomicBatchRetainsRowsWithDistinctRowIDs(t *testing.T) {
	mp := mpool.MustNew(t.Name())
	defer mpool.DeleteMPool(mp)
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	var block types.Blockid
	ts := types.BuildTS(1, 0)
	for i := 0; i < 3; i++ {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.NewRowid(&block, uint32(i)), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(i+1), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], ts, false, mp))
	}
	bat.SetRowCount(3)
	atomicBat := NewAtomicBatch(mp)
	packer := types.NewPacker()
	defer packer.Close()
	atomicBat.Append(packer, bat, 2, 0)
	require.Equal(t, 3, atomicBat.Rows.Len())
	atomicBat.Close()
	require.Zero(t, mp.CurrNB())
}

type iscpLogBatch struct {
	jobNames   []string
	jobIDs     []uint64
	jobSpecs   []string
	jobStatuss []string
}

func TestGetJobSpecsReadsAcrossBatchesAndMatchesJobsByKey(t *testing.T) {
	oldExecWithResult := ExecWithResult
	defer func() {
		ExecWithResult = oldExecWithResult
	}()

	result, mp := newISCPLogResult(t, []iscpLogBatch{
		{
			jobNames:   []string{"index_idx02"},
			jobIDs:     []uint64{2},
			jobSpecs:   []string{mustMarshalJobSpec(t, "idx02")},
			jobStatuss: []string{mustMarshalJobStatus(t, 22, JobStage_Running)},
		},
		{
			jobNames:   []string{"index_idx01"},
			jobIDs:     []uint64{3},
			jobSpecs:   []string{mustMarshalJobSpec(t, "idx01")},
			jobStatuss: []string{mustMarshalJobStatus(t, 33, JobStage_Init)},
		},
	})
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
		mpool.DeleteMPool(mp)
	}()

	ExecWithResult = func(context.Context, string, string, client.TxnOperator) (executor.Result, error) {
		return result, nil
	}

	jobSpecs, prevStatuses, err := GetJobSpecs(
		context.Background(),
		"",
		nil,
		nil,
		nil,
		0,
		42,
		[]string{"index_idx01", "index_idx02"},
		[]uint64{101, 202},
		types.TS{},
		[]*JobStatus{{}, {}},
		[]uint64{3, 2},
	)
	require.NoError(t, err)
	require.Equal(t, "idx01", jobSpecs[0].IndexName)
	require.Equal(t, "idx02", jobSpecs[1].IndexName)
	require.Equal(t, uint64(33), prevStatuses[0].LSN)
	require.Equal(t, uint64(22), prevStatuses[1].LSN)
}

func TestGetJobSpecsMissingJobFlushesPermanentErrorWithoutNilStatuses(t *testing.T) {
	oldExecWithResult := ExecWithResult
	oldFlushJobStatusOnIterationState := FlushJobStatusOnIterationState
	defer func() {
		ExecWithResult = oldExecWithResult
		FlushJobStatusOnIterationState = oldFlushJobStatusOnIterationState
	}()

	result, mp := newISCPLogResult(t, []iscpLogBatch{
		{
			jobNames:   []string{"index_idx02"},
			jobIDs:     []uint64{2},
			jobSpecs:   []string{mustMarshalJobSpec(t, "idx02")},
			jobStatuss: []string{mustMarshalJobStatus(t, 22, JobStage_Running)},
		},
	})
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
		mpool.DeleteMPool(mp)
	}()

	ExecWithResult = func(context.Context, string, string, client.TxnOperator) (executor.Result, error) {
		return result, nil
	}

	var capturedStatuses []*JobStatus
	FlushJobStatusOnIterationState = func(
		_ context.Context,
		_ string,
		_ engine.Engine,
		_ client.TxnClient,
		_ uint32,
		_ uint64,
		_ []string,
		_ []uint64,
		_ []uint64,
		jobStatuses []*JobStatus,
		_ types.TS,
		_ int8,
		_ []uint64,
	) error {
		capturedStatuses = jobStatuses
		return nil
	}

	_, _, err := GetJobSpecs(
		context.Background(),
		"",
		nil,
		nil,
		nil,
		0,
		42,
		[]string{"index_idx01", "index_idx02"},
		[]uint64{101, 202},
		types.TS{},
		make([]*JobStatus, 2),
		[]uint64{3, 2},
	)
	require.Error(t, err)
	require.True(t, isPermanentError(err))
	require.Len(t, capturedStatuses, 2)
	require.NotNil(t, capturedStatuses[0])
	require.NotNil(t, capturedStatuses[1])
	require.Equal(t, uint64(101), capturedStatuses[0].LSN)
	require.Equal(t, uint64(202), capturedStatuses[1].LSN)
}

func TestGetJobSpecsFlushesPermanentErrorForInvalidRows(t *testing.T) {
	oldExecWithResult := ExecWithResult
	oldFlushJobStatusOnIterationState := FlushJobStatusOnIterationState
	defer func() {
		ExecWithResult = oldExecWithResult
		FlushJobStatusOnIterationState = oldFlushJobStatusOnIterationState
	}()

	testCases := []struct {
		name    string
		batches []iscpLogBatch
	}{
		{
			name: "unexpected job",
			batches: []iscpLogBatch{
				{
					jobNames:   []string{"index_idx02"},
					jobIDs:     []uint64{2},
					jobSpecs:   []string{mustMarshalJobSpec(t, "idx02")},
					jobStatuss: []string{mustMarshalJobStatus(t, 22, JobStage_Running)},
				},
			},
		},
		{
			name: "duplicate job",
			batches: []iscpLogBatch{
				{
					jobNames:   []string{"index_idx01"},
					jobIDs:     []uint64{1},
					jobSpecs:   []string{mustMarshalJobSpec(t, "idx01")},
					jobStatuss: []string{mustMarshalJobStatus(t, 11, JobStage_Running)},
				},
				{
					jobNames:   []string{"index_idx01"},
					jobIDs:     []uint64{1},
					jobSpecs:   []string{mustMarshalJobSpec(t, "idx01")},
					jobStatuss: []string{mustMarshalJobStatus(t, 12, JobStage_Running)},
				},
			},
		},
		{
			name: "invalid job spec",
			batches: []iscpLogBatch{
				{
					jobNames:   []string{"index_idx01"},
					jobIDs:     []uint64{1},
					jobSpecs:   []string{`"invalid-job-spec"`},
					jobStatuss: []string{mustMarshalJobStatus(t, 11, JobStage_Running)},
				},
			},
		},
		{
			name: "invalid job status",
			batches: []iscpLogBatch{
				{
					jobNames:   []string{"index_idx01"},
					jobIDs:     []uint64{1},
					jobSpecs:   []string{mustMarshalJobSpec(t, "idx01")},
					jobStatuss: []string{`"invalid-job-status"`},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, mp := newISCPLogResult(t, tc.batches)
			defer func() {
				require.Equal(t, int64(0), mp.CurrNB())
				mpool.DeleteMPool(mp)
			}()

			ExecWithResult = func(context.Context, string, string, client.TxnOperator) (executor.Result, error) {
				return result, nil
			}

			var capturedStatuses []*JobStatus
			FlushJobStatusOnIterationState = func(
				_ context.Context,
				_ string,
				_ engine.Engine,
				_ client.TxnClient,
				_ uint32,
				_ uint64,
				_ []string,
				_ []uint64,
				_ []uint64,
				jobStatuses []*JobStatus,
				_ types.TS,
				_ int8,
				_ []uint64,
			) error {
				capturedStatuses = jobStatuses
				return nil
			}

			jobSpecs, prevStatuses, err := GetJobSpecs(
				context.Background(),
				"",
				nil,
				nil,
				nil,
				0,
				42,
				[]string{"index_idx01"},
				[]uint64{101},
				types.TS{},
				[]*JobStatus{nil},
				[]uint64{1},
			)
			require.Error(t, err)
			require.True(t, isPermanentError(err))
			require.Nil(t, jobSpecs)
			require.Nil(t, prevStatuses)
			require.Len(t, capturedStatuses, 1)
			require.NotNil(t, capturedStatuses[0])
			require.Equal(t, uint64(101), capturedStatuses[0].LSN)
		})
	}
}

func TestNormalizeJobStatusesResizesAndFillsNilEntries(t *testing.T) {
	statuses := normalizeJobStatuses(
		[]*JobStatus{{Stage: JobStage_Running}},
		[]uint64{10, 20},
	)

	require.Len(t, statuses, 2)
	require.Equal(t, JobStage_Running, statuses[0].Stage)
	require.Equal(t, uint64(10), statuses[0].LSN)
	require.NotNil(t, statuses[1])
	require.Equal(t, uint64(20), statuses[1].LSN)
}

func TestFlushFinalJobStatusUsesCurrentJobLSNAsPrevLSN(t *testing.T) {
	oldFlushJobStatusOnIterationState := FlushJobStatusOnIterationState
	defer func() {
		FlushJobStatusOnIterationState = oldFlushJobStatusOnIterationState
	}()

	iterCtx := &IterationContext{
		accountID: 7,
		tableID:   42,
		jobNames:  []string{"job_first", "job_second"},
		jobIDs:    []uint64{101, 202},
		lsn:       []uint64{11, 22},
	}
	status := &JobStatus{From: types.BuildTS(100, 0), To: types.BuildTS(200, 0)}
	status.SetError(moerr.NewInternalErrorNoCtx("sink failed"))

	var capturedJobNames []string
	var capturedJobIDs []uint64
	var capturedLSNs []uint64
	var capturedPrevLSN []uint64
	var capturedStatuses []*JobStatus
	var capturedState int8
	FlushJobStatusOnIterationState = func(
		_ context.Context,
		_ string,
		_ engine.Engine,
		_ client.TxnClient,
		_ uint32,
		_ uint64,
		jobNames []string,
		jobIDs []uint64,
		lsns []uint64,
		jobStatuses []*JobStatus,
		_ types.TS,
		state int8,
		prevLSN []uint64,
	) error {
		capturedJobNames = append([]string(nil), jobNames...)
		capturedJobIDs = append([]uint64(nil), jobIDs...)
		capturedLSNs = append([]uint64(nil), lsns...)
		capturedPrevLSN = append([]uint64(nil), prevLSN...)
		capturedStatuses = append([]*JobStatus(nil), jobStatuses...)
		capturedState = state
		return nil
	}

	err := flushFinalJobStatusOnIterationState(
		context.Background(),
		"",
		nil,
		nil,
		iterCtx,
		1,
		status,
		status.From,
		ISCPJobState_Error,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"job_second"}, capturedJobNames)
	require.Equal(t, []uint64{202}, capturedJobIDs)
	require.Equal(t, []uint64{22}, capturedLSNs)
	require.Equal(t, []uint64{22}, capturedPrevLSN)
	require.Same(t, status, capturedStatuses[0])
	require.Equal(t, ISCPJobState_Error, capturedState)
}

func newISCPLogResult(t *testing.T, batches []iscpLogBatch) (executor.Result, *mpool.MPool) {
	t.Helper()

	mp := mpool.MustNewZero()
	memRes := executor.NewMemResult(
		[]types.Type{
			types.T_varchar.ToType(),
			types.T_uint64.ToType(),
			types.T_json.ToType(),
			types.T_json.ToType(),
		},
		mp,
	)
	for _, batch := range batches {
		memRes.NewBatchWithRowCount(len(batch.jobNames))
		require.NoError(t, executor.AppendStringRows(memRes, 0, batch.jobNames))
		require.NoError(t, executor.AppendFixedRows(memRes, 1, batch.jobIDs))
		require.NoError(t, executor.AppendBytesRows(memRes, 2, encodeJSONRows(t, batch.jobSpecs)))
		require.NoError(t, executor.AppendBytesRows(memRes, 3, encodeJSONRows(t, batch.jobStatuss)))
	}
	return memRes.GetResult(), mp
}

func mustMarshalJobSpec(t *testing.T, indexName string) string {
	t.Helper()
	jobSpec, err := MarshalJobSpec(&JobSpec{
		ConsumerInfo: ConsumerInfo{
			IndexName: indexName,
		},
	})
	require.NoError(t, err)
	return jobSpec
}

func mustMarshalJobStatus(t *testing.T, lsn uint64, stage int8) string {
	t.Helper()
	jobStatus, err := json.Marshal(&JobStatus{
		LSN:   lsn,
		Stage: stage,
	})
	require.NoError(t, err)
	return string(jobStatus)
}

func encodeJSONRows(t *testing.T, rows []string) [][]byte {
	t.Helper()
	encodedRows := make([][]byte, len(rows))
	for i, row := range rows {
		byteJSON, err := types.ParseStringToByteJson(row)
		require.NoError(t, err)
		encodedRows[i], err = types.EncodeJson(byteJSON)
		require.NoError(t, err)
	}
	return encodedRows
}

func TestFlushStatusSucceedsOnOneAffectedRow(t *testing.T) {
	oldExecWithResult := ExecWithResult
	defer func() {
		ExecWithResult = oldExecWithResult
	}()

	ExecWithResult = func(_ context.Context, _ string, _ string, _ client.TxnOperator) (executor.Result, error) {
		return executor.Result{AffectedRows: 1}, nil
	}

	err := FlushStatus(
		context.Background(),
		"",
		nil,
		0,
		0,
		"test_job",
		1,
		&JobStatus{Stage: JobStage_Running},
		types.TS{},
		ISCPJobState_Error,
		0,
	)
	require.NoError(t, err)
}

func TestFlushStatusErrorsOnZeroAffectedRows(t *testing.T) {
	oldExecWithResult := ExecWithResult
	defer func() {
		ExecWithResult = oldExecWithResult
	}()

	ExecWithResult = func(_ context.Context, _ string, _ string, _ client.TxnOperator) (executor.Result, error) {
		return executor.Result{AffectedRows: 0}, nil
	}

	err := FlushStatus(
		context.Background(),
		"",
		nil,
		0,
		0,
		"test_job",
		1,
		&JobStatus{Stage: JobStage_Running},
		types.TS{},
		ISCPJobState_Error,
		0,
	)
	require.Error(t, err)
	require.False(t, isPermanentError(err))
	require.Contains(t, err.Error(), "affected 0 rows")
}

// TestGetJobSpecsPermanentErrorFailsWhenFlushReturnsError verifies that when
// FlushPermanentErrorMessage fails (e.g. because the conditional UPDATE affects
// 0 rows), GetJobSpecs returns a non-permanent error so that ExecuteIteration
// does NOT swallow it as handled.
func TestGetJobSpecsPermanentErrorFailsWhenFlushReturnsError(t *testing.T) {
	oldExecWithResult := ExecWithResult
	oldFlushJobStatusOnIterationState := FlushJobStatusOnIterationState
	defer func() {
		ExecWithResult = oldExecWithResult
		FlushJobStatusOnIterationState = oldFlushJobStatusOnIterationState
	}()

	result, mp := newISCPLogResult(t, []iscpLogBatch{
		{
			jobNames:   []string{"index_idx02"},
			jobIDs:     []uint64{2},
			jobSpecs:   []string{mustMarshalJobSpec(t, "idx02")},
			jobStatuss: []string{mustMarshalJobStatus(t, 22, JobStage_Running)},
		},
	})
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
		mpool.DeleteMPool(mp)
	}()

	ExecWithResult = func(_ context.Context, _ string, _ string, _ client.TxnOperator) (executor.Result, error) {
		return result, nil
	}

	FlushJobStatusOnIterationState = func(
		_ context.Context,
		_ string,
		_ engine.Engine,
		_ client.TxnClient,
		_ uint32,
		_ uint64,
		_ []string,
		_ []uint64,
		_ []uint64,
		_ []*JobStatus,
		_ types.TS,
		_ int8,
		_ []uint64,
	) error {
		return moerr.NewInternalErrorNoCtx("update affected 0 rows for job")
	}

	_, _, err := GetJobSpecs(
		context.Background(),
		"",
		nil,
		nil,
		nil,
		0,
		42,
		[]string{"index_idx01", "index_idx02"},
		[]uint64{101, 202},
		types.TS{},
		make([]*JobStatus, 2),
		[]uint64{3, 2},
	)
	require.Error(t, err)
	require.False(t, isPermanentError(err))
}
