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

func TestDMLMergeCoordinatorCollectsActionsAndCommits(t *testing.T) {
	bat, cleanup := newMergeActionScanBatch(t)
	defer cleanup()

	committer := &recordingDMLMergeCommitter{}
	coord := NewDMLMergeCoordinator(DMLMergeCoordinatorSpec{
		Committer: committer,
		Base: dml.CommitBase{
			BaseSnapshotID: 30,
			IdempotencyKey: "stmt-merge",
			StatementID:    "stmt-merge",
		},
		Schema:         api.Schema{SchemaID: 9},
		DeleteSchemaID: 9,
		ObjectWriter:   &recordingDMLDeleteObjectWriter{},
		DataFiles:      dmlDeleteCoordinatorDataFiles(),
	})
	req := icebergwrite.AppendRequest{
		Operation:               icebergwrite.OperationMerge,
		Namespace:               "sales",
		Table:                   "orders",
		DefaultRef:              "main",
		Attrs:                   []string{"id", "name", api.DMLDataFilePathColumnName, api.DMLRowOrdinalColumnName, api.DMLMergeActionColumnName},
		DataFilePathColumnIndex: 2,
		RowOrdinalColumnIndex:   3,
		MergeActionColumnIndex:  4,
	}
	require.NoError(t, coord.Begin(context.Background(), req))

	mp := mpool.MustNewZero()
	proc := process.NewTopProcess(context.Background(), mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, coord.AppendWithProcess(proc, bat))
	require.NoError(t, coord.Commit(context.Background()))

	require.Len(t, committer.requests, 1)
	commitReq := committer.requests[0]
	require.Equal(t, "orders", commitReq.Base.Table)
	require.Equal(t, api.Namespace{"sales"}, commitReq.Base.Namespace)
	require.Equal(t, "main", commitReq.Base.TargetRef)
	require.Len(t, commitReq.MatchedDeletes, 1)
	require.Equal(t, "s3://warehouse/gold/orders/data/b.parquet", commitReq.MatchedDeletes[0].DataFile.FilePath)
	require.Len(t, commitReq.MatchedDeletes[0].PositionRows, 1)
	require.Equal(t, int64(40), commitReq.MatchedDeletes[0].PositionRows[0].Pos)
	require.Len(t, commitReq.MatchedUpdates, 1)
	require.Equal(t, "s3://warehouse/gold/orders/data/a.parquet", commitReq.MatchedUpdates[0].DeleteTarget.DataFile.FilePath)
	require.Len(t, commitReq.MatchedUpdates[0].DeleteTarget.PositionRows, 1)
	require.Equal(t, int64(10), commitReq.MatchedUpdates[0].DeleteTarget.PositionRows[0].Pos)
	require.Equal(t, []string{"id", "name"}, committer.updateAttrs)
	require.Equal(t, 1, committer.updateRows)
	require.Equal(t, int32(7), committer.updateID)
	require.Equal(t, "alice-new", committer.updateName)
	require.Equal(t, []string{"id", "name"}, committer.insertAttrs)
	require.Equal(t, 1, committer.insertRows)
	require.Equal(t, int32(12), committer.insertID)
	require.Equal(t, "dee", committer.insertName)
}

func TestDMLMergeCoordinatorResolvesReorderedBatchAttrsByName(t *testing.T) {
	bat, cleanup := newReorderedMergeActionScanBatch(t)
	defer cleanup()

	committer := &recordingDMLMergeCommitter{}
	coord := NewDMLMergeCoordinator(DMLMergeCoordinatorSpec{
		Committer:      committer,
		Base:           dml.CommitBase{BaseSnapshotID: 30, IdempotencyKey: "stmt-merge", StatementID: "stmt-merge"},
		Schema:         api.Schema{SchemaID: 9},
		DeleteSchemaID: 9,
		ObjectWriter:   &recordingDMLDeleteObjectWriter{},
		DataFiles:      dmlDeleteCoordinatorDataFiles(),
	})
	req := icebergwrite.AppendRequest{
		Operation:               icebergwrite.OperationMerge,
		Namespace:               "sales",
		Table:                   "orders",
		DefaultRef:              "main",
		Attrs:                   []string{"id", "name", api.DMLDataFilePathColumnName, api.DMLRowOrdinalColumnName, api.DMLMergeActionColumnName},
		DataFilePathColumnIndex: 2,
		RowOrdinalColumnIndex:   3,
		MergeActionColumnIndex:  4,
	}
	require.NoError(t, coord.Begin(context.Background(), req))

	mp := mpool.MustNewZero()
	proc := process.NewTopProcess(context.Background(), mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, coord.AppendWithProcess(proc, bat))
	require.NoError(t, coord.Commit(context.Background()))

	require.Len(t, committer.requests, 1)
	commitReq := committer.requests[0]
	require.Len(t, commitReq.MatchedDeletes, 1)
	require.Equal(t, int64(40), commitReq.MatchedDeletes[0].PositionRows[0].Pos)
	require.Len(t, commitReq.MatchedUpdates, 1)
	require.Equal(t, int64(10), commitReq.MatchedUpdates[0].DeleteTarget.PositionRows[0].Pos)
	require.Equal(t, int32(7), committer.updateID)
	require.Equal(t, "alice-new", committer.updateName)
	require.Equal(t, int32(12), committer.insertID)
	require.Equal(t, "dee", committer.insertName)
}

func TestDMLMergeCoordinatorSplitsExecutionBatchWithoutAttrs(t *testing.T) {
	bat, cleanup := newMergeActionScanBatch(t)
	defer cleanup()
	bat.Attrs = nil

	committer := &recordingDMLMergeCommitter{}
	coord := NewDMLMergeCoordinator(DMLMergeCoordinatorSpec{
		Committer:      committer,
		Base:           dml.CommitBase{BaseSnapshotID: 30, IdempotencyKey: "stmt-merge", StatementID: "stmt-merge"},
		Schema:         api.Schema{SchemaID: 9},
		DeleteSchemaID: 9,
		ObjectWriter:   &recordingDMLDeleteObjectWriter{},
		DataFiles:      dmlDeleteCoordinatorDataFiles(),
	})
	req := icebergwrite.AppendRequest{
		Operation:               icebergwrite.OperationMerge,
		Namespace:               "sales",
		Table:                   "orders",
		DefaultRef:              "main",
		Attrs:                   []string{"id", "name", api.DMLDataFilePathColumnName, api.DMLRowOrdinalColumnName, api.DMLMergeActionColumnName},
		DataFilePathColumnIndex: 2,
		RowOrdinalColumnIndex:   3,
		MergeActionColumnIndex:  4,
	}
	require.NoError(t, coord.Begin(context.Background(), req))

	mp := mpool.MustNewZero()
	proc := process.NewTopProcess(context.Background(), mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, coord.AppendWithProcess(proc, bat))
	require.NoError(t, coord.Commit(context.Background()))

	require.Len(t, committer.requests, 1)
	require.Len(t, committer.requests[0].MatchedDeletes, 1)
	require.Len(t, committer.requests[0].MatchedUpdates, 1)
	require.Equal(t, []string{"id", "name"}, committer.updateAttrs)
	require.Equal(t, int32(7), committer.updateID)
	require.Equal(t, "alice-new", committer.updateName)
	require.Equal(t, []string{"id", "name"}, committer.insertAttrs)
	require.Equal(t, int32(12), committer.insertID)
	require.Equal(t, "dee", committer.insertName)
}

func newMergeActionScanBatch(t *testing.T) (*batch.Batch, func()) {
	t.Helper()
	mp := mpool.MustNewZero()
	bat := batch.New([]string{
		"id",
		"name",
		api.DMLDataFilePathColumnName,
		api.DMLRowOrdinalColumnName,
		api.DMLMergeActionColumnName,
	})
	idVec := vector.NewVec(types.T_int32.ToType())
	nameVec := vector.NewVec(types.T_varchar.ToType())
	pathVec := vector.NewVec(types.T_varchar.ToType())
	ordinalVec := vector.NewVec(types.T_int64.ToType())
	actionVec := vector.NewVec(types.T_varchar.ToType())
	for _, row := range []struct {
		id      int32
		name    string
		path    string
		ordinal int64
		action  string
	}{
		{7, "alice-new", "s3://warehouse/gold/orders/data/a.parquet", 10, api.DMLMergeActionUpdate},
		{9, "cyd", "s3://warehouse/gold/orders/data/b.parquet", 40, api.DMLMergeActionDelete},
		{12, "dee", "", 0, api.DMLMergeActionInsert},
		{14, "erin", "s3://warehouse/gold/orders/data/a.parquet", 60, api.DMLMergeActionNoop},
	} {
		require.NoError(t, vector.AppendFixed[int32](idVec, row.id, false, mp))
		require.NoError(t, vector.AppendBytes(nameVec, []byte(row.name), false, mp))
		require.NoError(t, vector.AppendBytes(pathVec, []byte(row.path), false, mp))
		require.NoError(t, vector.AppendFixed[int64](ordinalVec, row.ordinal, false, mp))
		require.NoError(t, vector.AppendBytes(actionVec, []byte(row.action), false, mp))
	}
	bat.Vecs[0] = idVec
	bat.Vecs[1] = nameVec
	bat.Vecs[2] = pathVec
	bat.Vecs[3] = ordinalVec
	bat.Vecs[4] = actionVec
	bat.SetRowCount(4)
	return bat, func() { bat.Clean(mp) }
}

func newReorderedMergeActionScanBatch(t *testing.T) (*batch.Batch, func()) {
	t.Helper()
	mp := mpool.MustNewZero()
	bat := batch.New([]string{
		api.DMLMergeActionColumnName,
		api.DMLDataFilePathColumnName,
		"name",
		api.DMLRowOrdinalColumnName,
		"id",
	})
	actionVec := vector.NewVec(types.T_varchar.ToType())
	pathVec := vector.NewVec(types.T_varchar.ToType())
	nameVec := vector.NewVec(types.T_varchar.ToType())
	ordinalVec := vector.NewVec(types.T_int64.ToType())
	idVec := vector.NewVec(types.T_int32.ToType())
	for _, row := range []struct {
		id      int32
		name    string
		path    string
		ordinal int64
		action  string
	}{
		{7, "alice-new", "s3://warehouse/gold/orders/data/a.parquet", 10, api.DMLMergeActionUpdate},
		{9, "cyd", "s3://warehouse/gold/orders/data/b.parquet", 40, api.DMLMergeActionDelete},
		{12, "dee", "", 0, api.DMLMergeActionInsert},
		{14, "erin", "s3://warehouse/gold/orders/data/a.parquet", 60, api.DMLMergeActionNoop},
	} {
		require.NoError(t, vector.AppendBytes(actionVec, []byte(row.action), false, mp))
		require.NoError(t, vector.AppendBytes(pathVec, []byte(row.path), false, mp))
		require.NoError(t, vector.AppendBytes(nameVec, []byte(row.name), false, mp))
		require.NoError(t, vector.AppendFixed[int64](ordinalVec, row.ordinal, false, mp))
		require.NoError(t, vector.AppendFixed[int32](idVec, row.id, false, mp))
	}
	bat.Vecs[0] = actionVec
	bat.Vecs[1] = pathVec
	bat.Vecs[2] = nameVec
	bat.Vecs[3] = ordinalVec
	bat.Vecs[4] = idVec
	bat.SetRowCount(4)
	return bat, func() { bat.Clean(mp) }
}

type recordingDMLMergeCommitter struct {
	requests    []DMLMergeActionStreamRequest
	updateAttrs []string
	updateRows  int
	updateID    int32
	updateName  string
	insertAttrs []string
	insertRows  int
	insertID    int32
	insertName  string
	err         error
}

func (c *recordingDMLMergeCommitter) CommitMerge(ctx context.Context, req DMLMergeActionStreamRequest) (DMLCommitActionStreamResult, error) {
	c.requests = append(c.requests, req)
	if len(req.MatchedUpdateReplacementBatches) > 0 {
		c.updateAttrs = append([]string(nil), req.MatchedUpdateReplacementBatches[0].Attrs...)
		bat := req.MatchedUpdateReplacementBatches[0].Batch
		if bat != nil && bat.RowCount() > 0 {
			c.updateRows = bat.RowCount()
			c.updateID = vector.GetFixedAtWithTypeCheck[int32](bat.Vecs[0], 0)
			c.updateName = bat.Vecs[1].GetStringAt(0)
		}
	}
	if len(req.UnmatchedAppendBatches) > 0 {
		c.insertAttrs = append([]string(nil), req.UnmatchedAppendBatches[0].Attrs...)
		bat := req.UnmatchedAppendBatches[0].Batch
		if bat != nil && bat.RowCount() > 0 {
			c.insertRows = bat.RowCount()
			c.insertID = vector.GetFixedAtWithTypeCheck[int32](bat.Vecs[0], 0)
			c.insertName = bat.Vecs[1].GetStringAt(0)
		}
	}
	return DMLCommitActionStreamResult{}, c.err
}
