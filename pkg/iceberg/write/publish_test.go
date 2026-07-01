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

package write

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func TestExecuteExistingMappingCTASUsesUnifiedAppendExecutor(t *testing.T) {
	executor := &capturingAppendExecutor{result: &api.CommitResult{SnapshotID: 300, CommitID: "commit-300"}}
	result, err := ExecuteExistingMappingCTAS(context.Background(), executor, appendEntrypointSpec())
	require.NoError(t, err)
	require.Equal(t, int64(300), result.Commit.SnapshotID)
	require.Equal(t, string(AppendSourceCTAS), result.Request.Summary["source-kind"])
	require.Equal(t, string(AppendSourceCTAS), executor.request.Summary["source-kind"])
	require.Equal(t, "idem-ctas", executor.request.IdempotencyKey)
	require.Equal(t, "job-ctas", executor.request.PublishAuditHint.JobID)
}

func TestExecuteSinkPublishUsesUnifiedAppendExecutor(t *testing.T) {
	executor := &capturingAppendExecutor{result: &api.CommitResult{SnapshotID: 301, CommitID: "commit-301"}}
	result, err := ExecuteSinkPublish(context.Background(), executor, appendEntrypointSpec())
	require.NoError(t, err)
	require.Equal(t, int64(301), result.Commit.SnapshotID)
	require.Equal(t, string(AppendSourceSink), result.Request.Summary["source-kind"])
	require.Equal(t, string(AppendSourceSink), executor.request.Summary["source-kind"])
	require.Equal(t, "batch-sink", executor.request.SourceBatch)
	require.Equal(t, "job-ctas", executor.request.PublishAuditHint.JobID)
}

func TestExecuteAppendEntrypointRequiresExecutor(t *testing.T) {
	_, err := ExecuteSinkPublish(context.Background(), nil, appendEntrypointSpec())
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrConfigInvalid))
}

type capturingAppendExecutor struct {
	request api.AppendRequest
	result  *api.CommitResult
}

func (e *capturingAppendExecutor) CommitAppend(ctx context.Context, req api.AppendRequest) (*api.CommitResult, error) {
	e.request = req
	if e.result == nil {
		return &api.CommitResult{SnapshotID: 1}, nil
	}
	return e.result, nil
}

func appendEntrypointSpec() AppendRequestSpec {
	return AppendRequestSpec{
		Namespace:            api.Namespace{"gold"},
		Table:                "orders",
		TableLocation:        "s3://warehouse/gold/orders",
		TargetRef:            "main",
		TableUUID:            "table-uuid",
		BaseSnapshotID:       200,
		BaseSchemaID:         3,
		BaseSpecID:           7,
		BaseSchema:           baseSchema(3),
		BaseSpec:             baseSpec(7),
		WriterOwnerAccountID: 7,
		IdempotencyKey:       "idem-ctas",
		SourceBatch:          "batch-sink",
		SourceQueryID:        "query-ctas",
		DataFiles: []api.DataFile{{
			FilePath:        "s3://warehouse/gold/orders/data/part-1.parquet",
			RecordCount:     10,
			FileSizeInBytes: 100,
			FileFormat:      "parquet",
			SpecID:          7,
		}},
		PublishAuditHint: api.PublishAuditHint{
			JobID:       "job-ctas",
			SourceDB:    "src",
			SourceTable: "orders",
		},
	}
}
