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

package iceberg

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/icebergwrite"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestDMLOverwriteCoordinatorCollectsReplacementRowsAndAffectedFiles(t *testing.T) {
	bat, cleanup := newReplacementExecutorBatch(t)
	defer cleanup()

	committer := &recordingDMLOverwriteCommitter{}
	coord := NewDMLOverwriteCoordinator(DMLOverwriteCoordinatorSpec{
		Committer: committer,
		Base: dml.CommitBase{
			BaseSnapshotID: 30,
			IdempotencyKey: "stmt-1",
			StatementID:    "stmt-1",
		},
		Schema:       api.Schema{SchemaID: 9},
		ObjectWriter: &recordingDMLDeleteObjectWriter{},
		Scope:        dml.OverwritePartition,
		Partition:    map[string]any{"region": "ksa"},
		AffectedDataFiles: []api.DataFile{{
			FilePath:    "s3://warehouse/gold/orders/data/a.parquet",
			RecordCount: 10,
			Partition:   map[string]any{"region": "ksa"},
		}},
	})
	req := icebergwrite.AppendRequest{
		Operation:  icebergwrite.OperationOverwrite,
		Namespace:  "sales",
		Table:      "orders",
		DefaultRef: "main",
		Attrs:      []string{"id", "name"},
	}
	require.NoError(t, coord.Begin(context.Background(), req))

	mp := mpool.MustNewZero()
	proc := process.NewTopProcess(context.Background(), mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, coord.AppendWithProcess(proc, bat))
	require.NoError(t, coord.Commit(context.Background()))

	require.Len(t, committer.requests, 1)
	require.Equal(t, "orders", committer.requests[0].Base.Table)
	require.Equal(t, api.Namespace{"sales"}, committer.requests[0].Base.Namespace)
	require.Equal(t, dml.OverwritePartition, committer.requests[0].Scope)
	require.Equal(t, "ksa", committer.requests[0].Partition["region"])
	require.Len(t, committer.requests[0].AffectedDataFiles, 1)
	require.Equal(t, "s3://warehouse/gold/orders/data/a.parquet", committer.requests[0].AffectedDataFiles[0].FilePath)
	require.Equal(t, []string{"id", "name"}, committer.replacementAttrs)
	require.Equal(t, 1, committer.replacementRows)
}

func TestDMLOverwriteCoordinatorCommitsDeleteOnlyOverwrite(t *testing.T) {
	committer := &recordingDMLOverwriteCommitter{}
	coord := NewDMLOverwriteCoordinator(DMLOverwriteCoordinatorSpec{
		Committer:    committer,
		Base:         dml.CommitBase{BaseSnapshotID: 30, IdempotencyKey: "stmt-1"},
		Schema:       api.Schema{SchemaID: 9},
		ObjectWriter: &recordingDMLDeleteObjectWriter{},
		AffectedDataFiles: []api.DataFile{{
			FilePath:    "s3://warehouse/gold/orders/data/a.parquet",
			RecordCount: 10,
		}},
	})
	require.NoError(t, coord.Begin(context.Background(), icebergwrite.AppendRequest{
		Operation: icebergwrite.OperationOverwrite,
		Table:     "orders",
		Attrs:     []string{"id"},
	}))
	require.NoError(t, coord.Commit(context.Background()))
	require.Len(t, committer.requests, 1)
	require.Len(t, committer.requests[0].AffectedDataFiles, 1)
	require.Empty(t, committer.requests[0].ReplacementBatches)
}

func TestDMLOverwriteCoordinatorSharesCommitAcrossScopes(t *testing.T) {
	bat, cleanup := newReplacementExecutorBatch(t)
	defer cleanup()

	committer := &recordingDMLOverwriteCommitter{}
	coord := NewDMLOverwriteCoordinator(DMLOverwriteCoordinatorSpec{
		Committer:    committer,
		Base:         dml.CommitBase{BaseSnapshotID: 30, IdempotencyKey: "stmt-1"},
		Schema:       api.Schema{SchemaID: 9},
		ObjectWriter: &recordingDMLDeleteObjectWriter{},
		Scope:        dml.OverwritePartition,
		Partition:    map[string]any{"region": "ksa"},
		AffectedDataFiles: []api.DataFile{{
			FilePath:    "s3://warehouse/gold/orders/data/a.parquet",
			RecordCount: 10,
			Partition:   map[string]any{"region": "ksa"},
		}},
	})
	req := icebergwrite.AppendRequest{
		Operation: icebergwrite.OperationOverwrite,
		Table:     "orders",
		Attrs:     []string{"id", "name"},
	}
	require.NoError(t, coord.Begin(context.Background(), req))
	require.NoError(t, coord.Begin(context.Background(), req))

	mp := mpool.MustNewZero()
	proc := process.NewTopProcess(context.Background(), mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, coord.AppendWithProcess(proc, bat))
	require.NoError(t, coord.AppendWithProcess(proc, bat))

	require.NoError(t, coord.Commit(context.Background()))
	require.Empty(t, committer.requests)

	require.NoError(t, coord.Commit(context.Background()))
	require.Len(t, committer.requests, 1)
	require.Len(t, committer.requests[0].ReplacementBatches, 2)
	require.Equal(t, 2, committer.replacementRows)

	require.NoError(t, coord.Commit(context.Background()))
	require.Len(t, committer.requests, 1)
}

type recordingDMLOverwriteCommitter struct {
	requests         []DMLOverwriteActionStreamRequest
	replacementAttrs []string
	replacementRows  int
	err              error
}

func (c *recordingDMLOverwriteCommitter) CommitOverwrite(ctx context.Context, req DMLOverwriteActionStreamRequest) (DMLCommitActionStreamResult, error) {
	c.requests = append(c.requests, req)
	if len(req.ReplacementBatches) > 0 {
		c.replacementAttrs = append([]string(nil), req.ReplacementBatches[0].Attrs...)
		for _, replacement := range req.ReplacementBatches {
			if replacement.Batch != nil {
				c.replacementRows += replacement.Batch.RowCount()
			}
		}
	}
	return DMLCommitActionStreamResult{}, c.err
}
