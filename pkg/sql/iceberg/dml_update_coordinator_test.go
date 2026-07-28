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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/icebergwrite"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestDMLUpdateCoordinatorCollectsTargetsAndReplacementRows(t *testing.T) {
	bat, cleanup := newMatchedScanBatch(t)
	defer cleanup()

	committer := &recordingDMLUpdateCommitter{}
	coord := NewDMLUpdateCoordinator(DMLUpdateCoordinatorSpec{
		Committer: committer,
		Base: dml.CommitBase{
			BaseSnapshotID: 30,
			IdempotencyKey: "stmt-1",
			StatementID:    "stmt-1",
		},
		Schema:         api.Schema{SchemaID: 9},
		DeleteSchemaID: 9,
		ObjectWriter:   &recordingDMLDeleteObjectWriter{},
		DataFiles:      dmlDeleteCoordinatorDataFiles(),
	})
	req := icebergwrite.AppendRequest{
		Operation:               icebergwrite.OperationUpdate,
		Namespace:               "sales",
		Table:                   "orders",
		DefaultRef:              "main",
		Attrs:                   []string{"id", "name", api.DMLDataFilePathColumnName, api.DMLRowOrdinalColumnName},
		DataFilePathColumnIndex: 2,
		RowOrdinalColumnIndex:   3,
	}
	require.NoError(t, coord.Begin(context.Background(), req))

	mp := mpool.MustNewZero()
	proc := process.NewTopProcess(context.Background(), mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, coord.AppendWithProcess(proc, bat))
	require.NoError(t, coord.Commit(context.Background()))

	require.Len(t, committer.requests, 1)
	require.Len(t, committer.requests[0].Targets, 2)
	require.Equal(t, "orders", committer.requests[0].Base.Table)
	require.Equal(t, api.Namespace{"sales"}, committer.requests[0].Base.Namespace)
	require.Equal(t, []string{"id", "name"}, committer.replacementAttrs)
	require.Equal(t, 3, committer.replacementRows)
}

func TestDMLUpdateCoordinatorResolvesReorderedBatchAttrsByName(t *testing.T) {
	bat, cleanup := newReorderedMatchedScanBatch(t)
	defer cleanup()

	committer := &recordingDMLUpdateCommitter{}
	coord := NewDMLUpdateCoordinator(DMLUpdateCoordinatorSpec{
		Committer:      committer,
		Base:           dml.CommitBase{BaseSnapshotID: 30, IdempotencyKey: "stmt-1", StatementID: "stmt-1"},
		Schema:         api.Schema{SchemaID: 9},
		DeleteSchemaID: 9,
		ObjectWriter:   &recordingDMLDeleteObjectWriter{},
		DataFiles:      dmlDeleteCoordinatorDataFiles(),
	})
	req := icebergwrite.AppendRequest{
		Operation:               icebergwrite.OperationUpdate,
		Namespace:               "sales",
		Table:                   "orders",
		DefaultRef:              "main",
		Attrs:                   []string{"id", "name", api.DMLDataFilePathColumnName, api.DMLRowOrdinalColumnName},
		DataFilePathColumnIndex: 2,
		RowOrdinalColumnIndex:   3,
	}
	require.NoError(t, coord.Begin(context.Background(), req))

	mp := mpool.MustNewZero()
	proc := process.NewTopProcess(context.Background(), mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, coord.AppendWithProcess(proc, bat))
	require.NoError(t, coord.Commit(context.Background()))

	require.Len(t, committer.requests, 1)
	require.Len(t, committer.requests[0].Targets, 2)
	require.Equal(t, int32(7), committer.replacementID)
	require.Equal(t, "alice", committer.replacementName)
	require.Equal(t, int64(10), committer.requests[0].Targets[0].PositionRows[0].Pos)
}

func newReorderedMatchedScanBatch(t *testing.T) (*batch.Batch, func()) {
	t.Helper()
	mp := mpool.MustNewZero()
	bat := batch.New([]string{
		api.DMLDataFilePathColumnName,
		"name",
		api.DMLRowOrdinalColumnName,
		"id",
	})
	pathVec := vector.NewVec(types.T_varchar.ToType())
	nameVec := vector.NewVec(types.T_varchar.ToType())
	ordinalVec := vector.NewVec(types.T_int64.ToType())
	idVec := vector.NewVec(types.T_int32.ToType())
	for _, row := range []struct {
		id      int32
		name    string
		path    string
		ordinal int64
	}{
		{7, "alice", "s3://warehouse/gold/orders/data/a.parquet", 10},
		{8, "bob", "s3://warehouse/gold/orders/data/a.parquet", 11},
		{9, "cyd", "s3://warehouse/gold/orders/data/b.parquet", 40},
	} {
		require.NoError(t, vector.AppendBytes(pathVec, []byte(row.path), false, mp))
		require.NoError(t, vector.AppendBytes(nameVec, []byte(row.name), false, mp))
		require.NoError(t, vector.AppendFixed[int64](ordinalVec, row.ordinal, false, mp))
		require.NoError(t, vector.AppendFixed[int32](idVec, row.id, false, mp))
	}
	bat.Vecs[0] = pathVec
	bat.Vecs[1] = nameVec
	bat.Vecs[2] = ordinalVec
	bat.Vecs[3] = idVec
	bat.SetRowCount(3)
	return bat, func() { bat.Clean(mp) }
}

type recordingDMLUpdateCommitter struct {
	requests         []DMLUpdateActionStreamRequest
	replacementAttrs []string
	replacementRows  int
	replacementID    int32
	replacementName  string
	err              error
}

func (c *recordingDMLUpdateCommitter) CommitUpdate(ctx context.Context, req DMLUpdateActionStreamRequest) (DMLCommitActionStreamResult, error) {
	c.requests = append(c.requests, req)
	if len(req.ReplacementBatches) > 0 {
		c.replacementAttrs = append([]string(nil), req.ReplacementBatches[0].Attrs...)
		if req.ReplacementBatches[0].Batch != nil {
			bat := req.ReplacementBatches[0].Batch
			c.replacementRows = bat.RowCount()
			if bat.RowCount() > 0 && len(bat.Vecs) >= 2 {
				c.replacementID = vector.GetFixedAtWithTypeCheck[int32](bat.Vecs[0], 0)
				c.replacementName = bat.Vecs[1].GetStringAt(0)
			}
		}
	}
	return DMLCommitActionStreamResult{}, c.err
}
