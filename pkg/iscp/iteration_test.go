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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

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
