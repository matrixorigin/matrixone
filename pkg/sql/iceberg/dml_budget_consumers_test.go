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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	icebergwritepkg "github.com/matrixorigin/matrixone/pkg/iceberg/write"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/icebergwrite"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func assertConsumerBudgetError(t *testing.T, err error, used, requested, limit int64) {
	t.Helper()
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState), "%v", err)
	for _, field := range []string{fmt.Sprintf("used_bytes=%d", used), fmt.Sprintf("requested_bytes=%d", requested), fmt.Sprintf("limit_bytes=%d", limit), "iceberg.write.dml_max_memory"} {
		require.Contains(t, err.Error(), field)
	}
}

func TestDMLCoordinatorsRejectBudgetBeforeCommit(t *testing.T) {
	ctx := context.Background()
	matched, cleanMatched := newMatchedScanBatch(t)
	defer cleanMatched()
	merged, cleanMerged := newMergeActionScanBatch(t)
	defer cleanMerged()
	replacement, cleanReplacement := newReplacementExecutorBatch(t)
	defer cleanReplacement()
	proc := process.NewTopProcess(ctx, mpool.MustNewZero(), nil, nil, nil, nil, nil, nil, nil, nil, nil)
	attrs := []string{"id", "name", api.DMLDataFilePathColumnName, api.DMLRowOrdinalColumnName}
	t.Run("delete append", func(t *testing.T) {
		committer := &recordingDMLDeleteCommitter{}
		c := NewDMLDeleteCoordinator(DMLDeleteCoordinatorSpec{Committer: committer, DataFiles: dmlDeleteCoordinatorDataFiles(), IncludePositionRows: true, MemoryLimitBytes: 1})
		require.NoError(t, c.Begin(ctx, icebergwrite.AppendRequest{Operation: icebergwrite.OperationDelete, DataFilePathColumnIndex: 2, RowOrdinalColumnIndex: 3}))
		err := c.Append(ctx, matched)
		assertConsumerBudgetError(t, err, 0, retainedDMLCollectorBatchBytes(matched, 0), 1)
		require.Zero(t, c.budget.usedBytes())
		require.Empty(t, committer.requests)
		require.NoError(t, c.Abort(ctx, err))
	})
	t.Run("delete commit request", func(t *testing.T) {
		n := retainedDMLCollectorBatchBytes(matched, 0)
		committer := &recordingDMLDeleteCommitter{}
		c := NewDMLDeleteCoordinator(DMLDeleteCoordinatorSpec{Committer: committer, DataFiles: dmlDeleteCoordinatorDataFiles(), IncludePositionRows: true, MemoryLimitBytes: n})
		require.NoError(t, c.Begin(ctx, icebergwrite.AppendRequest{Operation: icebergwrite.OperationDelete, DataFilePathColumnIndex: 2, RowOrdinalColumnIndex: 3}))
		require.NoError(t, c.Append(ctx, matched))
		used := c.budget.usedBytes()
		request := c.collector.RetainedBytes()
		require.Positive(t, request)
		assertConsumerBudgetError(t, c.Commit(ctx), used, request, n)
		require.Empty(t, committer.requests)
	})
	t.Run("update append", func(t *testing.T) {
		committer := &recordingDMLUpdateCommitter{}
		c := NewDMLUpdateCoordinator(DMLUpdateCoordinatorSpec{Committer: committer, ObjectWriter: &recordingDMLDeleteObjectWriter{}, DataFiles: dmlDeleteCoordinatorDataFiles(), MemoryLimitBytes: 1})
		require.NoError(t, c.Begin(ctx, icebergwrite.AppendRequest{Operation: icebergwrite.OperationUpdate, Attrs: attrs, DataFilePathColumnIndex: 2, RowOrdinalColumnIndex: 3}))
		err := c.AppendWithProcess(proc, matched)
		assertConsumerBudgetError(t, err, 0, retainedDMLCollectorBatchBytes(matched, 0), 1)
		require.Zero(t, c.budget.usedBytes())
		require.Empty(t, committer.requests)
		require.NoError(t, c.Abort(ctx, err))
	})
	t.Run("update commit request", func(t *testing.T) {
		committer := &recordingDMLUpdateCommitter{}
		c := NewDMLUpdateCoordinator(DMLUpdateCoordinatorSpec{Committer: committer, ObjectWriter: &recordingDMLDeleteObjectWriter{}, DataFiles: dmlDeleteCoordinatorDataFiles()})
		require.NoError(t, c.Begin(ctx, icebergwrite.AppendRequest{Operation: icebergwrite.OperationUpdate, Attrs: attrs, DataFilePathColumnIndex: 2, RowOrdinalColumnIndex: 3}))
		require.NoError(t, c.AppendWithProcess(proc, matched))
		used, requested := c.budget.usedBytes(), c.collector.RetainedBytes()
		require.Positive(t, requested)
		c.budget.limit = used
		assertConsumerBudgetError(t, c.Commit(ctx), used, requested, used)
		require.Empty(t, committer.requests)
	})
	t.Run("merge append", func(t *testing.T) {
		committer := &recordingDMLMergeCommitter{}
		c := NewDMLMergeCoordinator(DMLMergeCoordinatorSpec{Committer: committer, ObjectWriter: &recordingDMLDeleteObjectWriter{}, DataFiles: dmlDeleteCoordinatorDataFiles(), MemoryLimitBytes: 1})
		mergeAttrs := append(append([]string(nil), attrs...), api.DMLMergeActionColumnName)
		require.NoError(t, c.Begin(ctx, icebergwrite.AppendRequest{Operation: icebergwrite.OperationMerge, Attrs: mergeAttrs, DataFilePathColumnIndex: 2, RowOrdinalColumnIndex: 3, MergeActionColumnIndex: 4}))
		err := c.AppendWithProcess(proc, merged)
		assertConsumerBudgetError(t, err, 0, retainedDMLBatchBytes(merged), 1)
		require.Zero(t, c.budget.usedBytes())
		require.Empty(t, committer.requests)
		require.NoError(t, c.Abort(ctx, err))
	})
	t.Run("merge commit request", func(t *testing.T) {
		committer := &recordingDMLMergeCommitter{}
		c := NewDMLMergeCoordinator(DMLMergeCoordinatorSpec{Committer: committer, ObjectWriter: &recordingDMLDeleteObjectWriter{}, DataFiles: dmlDeleteCoordinatorDataFiles()})
		mergeAttrs := append(append([]string(nil), attrs...), api.DMLMergeActionColumnName)
		require.NoError(t, c.Begin(ctx, icebergwrite.AppendRequest{Operation: icebergwrite.OperationMerge, Attrs: mergeAttrs, DataFilePathColumnIndex: 2, RowOrdinalColumnIndex: 3, MergeActionColumnIndex: 4}))
		require.NoError(t, c.AppendWithProcess(proc, merged))
		used := c.budget.usedBytes()
		requested := saturatingDMLAdd(c.deleteCollector.RetainedBytes(), c.updateCollector.RetainedBytes())
		require.Positive(t, requested)
		c.budget.limit = used
		assertConsumerBudgetError(t, c.Commit(ctx), used, requested, used)
		require.Empty(t, committer.requests)
	})
	t.Run("overwrite append", func(t *testing.T) {
		committer := &recordingDMLOverwriteCommitter{}
		c := NewDMLOverwriteCoordinator(DMLOverwriteCoordinatorSpec{Committer: committer, ObjectWriter: &recordingDMLDeleteObjectWriter{}, MemoryLimitBytes: 1})
		require.NoError(t, c.Begin(ctx, icebergwrite.AppendRequest{Operation: icebergwrite.OperationOverwrite, Attrs: []string{"id", "name"}}))
		err := c.AppendWithProcess(proc, replacement)
		assertConsumerBudgetError(t, err, 0, retainedDMLBatchBytes(replacement), 1)
		require.Zero(t, c.budget.usedBytes())
		require.Empty(t, committer.requests)
		require.NoError(t, c.Abort(ctx, err))
	})
}

type budgetAuditCommitter struct{ calls int }

func (c *budgetAuditCommitter) CommitTable(context.Context, api.CommitRequest) (*api.CommitResult, error) {
	c.calls++
	return nil, nil
}

func TestManifestBudgetFailureReachesPublishDAO(t *testing.T) {
	store := &recordingPublishStore{}
	committer := &budgetAuditCommitter{}
	workflow := icebergwritepkg.AppendWorkflow{
		Builder:   &icebergwritepkg.ManifestAttemptBuilder{SnapshotID: 200, SequenceNumber: 9, ManifestPath: "s3://warehouse/metadata/m.avro", ManifestListPath: "s3://warehouse/metadata/list.avro", MaxMemoryBytes: 1024, InitialMemoryBytes: 1024},
		Committer: committer, AuditRecorder: PublishAuditRecorder{DAO: store},
	}
	req := api.AppendRequest{
		CatalogRequest: api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}}, Namespace: api.Namespace{"sales"}, Table: "orders", TableLocation: "s3://warehouse/sales/orders", TargetRef: "main", TableUUID: "table-uuid", FormatVersion: 2, BaseSnapshotID: 100, BaseSchemaID: 3, BaseSpecID: 7, WriterOwnerAccountID: 7, IdempotencyKey: "idem-1",
		BaseSchema:       api.Schema{SchemaID: 3, Fields: []api.SchemaField{{ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeLong}, Required: true}}, IdentifierFieldIDs: []int{1}},
		BaseSpec:         api.PartitionSpec{SpecID: 7, Fields: []api.PartitionField{{SourceID: 1, FieldID: 1000, Name: "id", Transform: "identity"}}},
		DataFiles:        []api.DataFile{{FilePath: "s3://warehouse/sales/orders/data/part.parquet", FileFormat: "parquet", Partition: map[string]any{"id": int64(1)}, RecordCount: 2, FileSizeInBytes: 10, ValueCounts: map[int]int64{1: 2}, NullValueCounts: map[int]int64{1: 0}, SpecID: 7}},
		PublishAuditHint: api.PublishAuditHint{JobID: "job-1", SourceDB: "gold", SourceTable: "orders"},
	}
	_, err := workflow.CommitAppend(context.Background(), req)
	require.True(t, api.IsMemoryLimit(err), "%v", err)
	require.Zero(t, committer.calls)
	require.Len(t, store.jobs, 1)
	require.Equal(t, "failed", store.jobs[0].Status)
	require.Equal(t, string(api.ErrPlanningLimitExceeded), store.jobs[0].ErrorCategory)
}

func TestParquetBufferedSinkBudgetError(t *testing.T) {
	type row struct{ ID int64 }
	for _, tc := range []struct {
		name      string
		limit     int64
		failWrite bool
	}{{"write", 1, true}, {"close", 65536, false}} {
		t.Run(tc.name, func(t *testing.T) {
			budget := newDMLMemoryBudget(tc.limit, 0)
			objects := &recordingDMLDeleteObjectWriter{}
			sink := &bufferedDataFileObject{ctx: context.Background(), writer: objects, budget: budget, location: "s3://secret-token/object"}
			writer := parquet.NewGenericWriter[row](sink)
			_, err := writer.Write([]row{{ID: 1}})
			require.NoError(t, err)
			err = writer.Close()
			if tc.failWrite {
				require.Error(t, err)
				assertConsumerBudgetError(t, api.ToMOErr(context.Background(), errors.Join(err, moerr.NewInternalError(context.Background(), "secret-token"))), 0, 65536, tc.limit)
				require.Zero(t, budget.usedBytes())
				require.NoError(t, sink.Abort())
			} else {
				require.NoError(t, err)
				size := int64(len(sink.data))
				require.Positive(t, size)
				err = sink.Close()
				assertConsumerBudgetError(t, api.ToMOErr(context.Background(), errors.Join(err, moerr.NewInternalError(context.Background(), "secret-token"))), 65536, size, tc.limit)
				require.Zero(t, budget.usedBytes())
			}
			require.NotContains(t, api.ToMOErr(context.Background(), errors.Join(err, moerr.NewInternalError(context.Background(), "secret-token"))).Error(), "secret-token")
			require.Empty(t, objects.objects)
		})
	}
}

func TestReplacementWriterRejectsMetadataBudget(t *testing.T) {
	bat, cleanup := newReplacementExecutorBatch(t)
	defer cleanup()
	schema := api.Schema{SchemaID: 9, Fields: []api.SchemaField{
		{ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeInt}, Required: true},
		{ID: 2, Name: "name", Type: api.IcebergType{Kind: api.TypeString}},
	}}
	budget := newDMLMemoryBudget(1, 0)
	objects := &recordingDMLDeleteObjectWriter{}
	files, err := WriteDMLReplacementDataFiles(context.Background(), DMLReplacementDataFilesRequest{
		TableLocation: "s3://warehouse/orders", Base: dml.CommitBase{Table: "orders", StatementID: "stmt"}, SnapshotID: 10,
		Schema: schema, Attrs: bat.Attrs, Batch: bat, ObjectWriter: objects, MemoryBudget: budget,
	})
	assertConsumerBudgetError(t, err, 0, retainedDMLDataFileMetadataBytesForSchema(schema, api.PartitionSpec{}), 1)
	require.Zero(t, budget.usedBytes())
	require.Empty(t, files)
	require.Empty(t, objects.objects)
}

func TestDeleteEncodersPreserveBudgetError(t *testing.T) {
	ctx := context.Background()
	t.Run("position", func(t *testing.T) {
		b := newDMLMemoryBudget(1, 0)
		var output bytes.Buffer
		_, err := dml.WritePositionDeleteFile(ctx, &output, dml.PositionDeleteWriteRequest{FilePath: "s3://warehouse/delete", Rows: []dml.PositionDeleteRow{{FilePath: "s3://warehouse/data", Pos: 1}}, MemoryBudget: b})
		assertConsumerBudgetError(t, err, 0, 64, 1)
		require.Zero(t, b.usedBytes())
		require.Zero(t, output.Len())
	})
	t.Run("equality", func(t *testing.T) {
		b := newDMLMemoryBudget(1, 0)
		var output bytes.Buffer
		_, err := dml.WriteEqualityDeleteFile(ctx, &output, dml.EqualityDeleteWriteRequest{FilePath: "s3://warehouse/delete", Schema: api.Schema{Fields: []api.SchemaField{{ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeLong}}}}, EqualityIDs: []int{1}, Rows: []dml.EqualityDeleteRow{{Values: map[int]any{1: int64(44)}}}, MemoryBudget: b})
		assertConsumerBudgetError(t, err, 0, 3<<20, 1)
		require.Zero(t, b.usedBytes())
		require.Zero(t, output.Len())
	})
}

func TestDMLCoordinatorPartialAdmissionBudgetErrors(t *testing.T) {
	ctx := context.Background()
	proc := process.NewTopProcess(ctx, mpool.MustNewZero(), nil, nil, nil, nil, nil, nil, nil, nil, nil)
	attrs := []string{"id", "name", api.DMLDataFilePathColumnName, api.DMLRowOrdinalColumnName}
	matched, cleanMatched := newMatchedScanBatch(t)
	defer cleanMatched()
	t.Run("update replacement", func(t *testing.T) {
		collector := retainedDMLCollectorBatchBytes(matched, 0)
		requested := retainedDMLBatchBytes(matched)
		committer := &recordingDMLUpdateCommitter{}
		c := NewDMLUpdateCoordinator(DMLUpdateCoordinatorSpec{Committer: committer, ObjectWriter: &recordingDMLDeleteObjectWriter{}, DataFiles: dmlDeleteCoordinatorDataFiles(), MemoryLimitBytes: collector})
		require.NoError(t, c.Begin(ctx, icebergwrite.AppendRequest{Operation: icebergwrite.OperationUpdate, Attrs: attrs, DataFilePathColumnIndex: 2, RowOrdinalColumnIndex: 3}))
		budget := c.budget
		err := c.AppendWithProcess(proc, matched)
		assertConsumerBudgetError(t, err, collector, requested, collector)
		require.Equal(t, collector, budget.usedBytes())
		require.NotEmpty(t, c.collector.Targets())
		require.Empty(t, c.replacementBats)
		require.Empty(t, committer.requests)
		require.NoError(t, c.Abort(ctx, err))
		require.Zero(t, budget.usedBytes())
	})

	merged, cleanMerged := newMergeActionScanBatch(t)
	defer cleanMerged()
	mergeAttrs := append(append([]string(nil), attrs...), api.DMLMergeActionColumnName)
	newCoordinator := func(limit int64, committer *recordingDMLMergeCommitter) *DMLMergeCoordinator {
		c := NewDMLMergeCoordinator(DMLMergeCoordinatorSpec{Committer: committer, ObjectWriter: &recordingDMLDeleteObjectWriter{}, DataFiles: dmlDeleteCoordinatorDataFiles(), MemoryLimitBytes: limit})
		require.NoError(t, c.Begin(ctx, icebergwrite.AppendRequest{Operation: icebergwrite.OperationMerge, Attrs: mergeAttrs, DataFilePathColumnIndex: 2, RowOrdinalColumnIndex: 3, MergeActionColumnIndex: 4}))
		return c
	}
	probe := newCoordinator(0, &recordingDMLMergeCommitter{})
	del, upd, ins, err := probe.splitMergeActionBatch(ctx, merged, proc.Mp())
	require.NoError(t, err)
	defer cleanMergeSplitBatch(del, proc.Mp())
	defer cleanMergeSplitBatch(upd, proc.Mp())
	defer cleanMergeSplitBatch(ins, proc.Mp())
	transient := retainedDMLBatchBytes(merged)
	delKey := int64(len(api.LengthPrefixedKey("s3://warehouse/gold/orders/data/b.parquet", "40")) + 32)
	updKey := int64(len(api.LengthPrefixedKey("s3://warehouse/gold/orders/data/a.parquet", "10")) + 32)
	delCollector := retainedDMLCollectorBatchBytes(del, 0)
	updCollector := retainedDMLCollectorBatchBytes(upd, 0)
	updBatch, insBatch := retainedDMLBatchBytes(upd), retainedDMLBatchBytes(ins)
	for _, tc := range []struct {
		name                                  string
		used, requested, retained             int64
		keys, deletes, updates, updateBatches int
	}{
		{"matched key", transient, delKey, 0, 0, 0, 0, 0},
		{"delete collector", transient + delKey, delCollector, delKey, 1, 0, 0, 0},
		{"update collector", transient + delKey + delCollector + updKey, updCollector, delKey + delCollector + updKey, 2, 1, 0, 0},
		{"update replacement", transient + delKey + delCollector + updKey + updCollector, updBatch, delKey + delCollector + updKey + updCollector, 2, 1, 1, 0},
		{"insert replacement", transient + delKey + delCollector + updKey + updCollector + updBatch, insBatch, delKey + delCollector + updKey + updCollector + updBatch, 2, 1, 1, 1},
	} {
		t.Run("merge "+tc.name, func(t *testing.T) {
			committer := &recordingDMLMergeCommitter{}
			c := newCoordinator(tc.used, committer)
			budget := c.budget
			err := c.AppendWithProcess(proc, merged)
			assertConsumerBudgetError(t, err, tc.used, tc.requested, tc.used)
			require.Equal(t, tc.retained, budget.usedBytes())
			require.Len(t, c.matchedRows, tc.keys)
			require.Len(t, c.deleteCollector.Targets(), tc.deletes)
			require.Len(t, c.updateCollector.Targets(), tc.updates)
			require.Len(t, c.updateBats, tc.updateBatches)
			require.Empty(t, c.insertBats)
			require.Empty(t, committer.requests)
			require.NoError(t, c.Abort(ctx, err))
			require.Zero(t, budget.usedBytes())
		})
	}
}

func TestReplacementFanoutSinkCloseBudgetError(t *testing.T) {
	ctx := context.Background()
	bat, cleanup := newReplacementExecutorBatch(t)
	defer cleanup()
	schema := api.Schema{SchemaID: 9, Fields: []api.SchemaField{
		{ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeInt}, Required: true},
		{ID: 2, Name: "name", Type: api.IcebergType{Kind: api.TypeString}},
	}}
	metadata := retainedDMLDataFileMetadataBytesForSchema(schema, api.PartitionSpec{})
	req := DMLReplacementDataFilesRequest{
		TableLocation: "s3://secret-token/orders", Base: dml.CommitBase{Table: "orders", StatementID: "stmt"}, SnapshotID: 10,
		Schema: schema, Attrs: bat.Attrs, Batch: bat,
	}
	baseline := &recordingDMLDeleteObjectWriter{}
	req.ObjectWriter = baseline
	_, err := WriteDMLReplacementDataFiles(ctx, req)
	require.NoError(t, err)
	require.Len(t, baseline.objects, 1)
	var size int64
	for _, payload := range baseline.objects {
		size = int64(len(payload))
	}
	require.Positive(t, size)
	budget := newDMLMemoryBudget(metadata+(64<<10), 0)
	objects := &recordingDMLDeleteObjectWriter{}
	req.MemoryBudget, req.ObjectWriter = budget, objects
	files, err := WriteDMLReplacementDataFiles(ctx, req)
	assertConsumerBudgetError(t, err, metadata+(64<<10), size, metadata+(64<<10))
	require.NotContains(t, err.Error(), "secret-token")
	require.Empty(t, files)
	require.Empty(t, objects.objects)
	// The orphan's metadata reservation survives until the owning statement ends.
	require.Equal(t, metadata, budget.usedBytes())
}

func TestReplacementFanoutSinkWriteBudgetError(t *testing.T) {
	ctx := context.Background()
	bat, cleanup := newReplacementExecutorBatch(t)
	defer cleanup()
	schema := api.Schema{SchemaID: 9, Fields: []api.SchemaField{
		{ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeInt}, Required: true},
		{ID: 2, Name: "name", Type: api.IcebergType{Kind: api.TypeString}},
	}}
	metadata := retainedDMLDataFileMetadataBytesForSchema(schema, api.PartitionSpec{})
	budget := newDMLMemoryBudget(metadata, 0)
	objects := &recordingDMLDeleteObjectWriter{}
	files, err := WriteDMLReplacementDataFiles(ctx, DMLReplacementDataFilesRequest{
		TableLocation: "s3://secret-token/orders", Base: dml.CommitBase{Table: "orders", StatementID: "stmt"}, SnapshotID: 10,
		Schema: schema, Attrs: bat.Attrs, Batch: bat, MemoryBudget: budget, ObjectWriter: objects,
	})
	assertConsumerBudgetError(t, err, metadata, 64<<10, metadata)
	require.NotContains(t, err.Error(), "secret-token")
	require.Empty(t, files)
	require.Empty(t, objects.objects)
	require.Zero(t, budget.usedBytes(), "successful abort releases buffer and metadata")
}

func TestReplacementFanoutUploadCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bat, cleanup := newReplacementExecutorBatch(t)
	defer cleanup()
	schema := api.Schema{Fields: []api.SchemaField{
		{ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeInt}, Required: true},
		{ID: 2, Name: "name", Type: api.IcebergType{Kind: api.TypeString}},
	}}
	metadata := retainedDMLDataFileMetadataBytesForSchema(schema, api.PartitionSpec{})
	budget := newDMLMemoryBudget(metadata+(1<<20), 0)
	started := make(chan struct{})
	done := make(chan error, 1)
	writer := testDeleteObjectWriterFunc(func(ctx context.Context, _ string, _ []byte) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	go func() {
		_, err := WriteDMLReplacementDataFiles(ctx, DMLReplacementDataFilesRequest{
			TableLocation: "s3://warehouse/orders", Base: dml.CommitBase{Table: "orders", StatementID: "stmt"}, SnapshotID: 10,
			Schema: schema, Attrs: bat.Attrs, Batch: bat, MemoryBudget: budget, ObjectWriter: writer,
		})
		done <- err
	}()
	<-started
	// A concurrent snapshot must complete while object I/O is blocked.
	_, limited := budget.availableMemory()
	require.True(t, limited)
	cancel()
	require.Error(t, <-done)
	require.Equal(t, metadata, budget.usedBytes(), "uncertain upload retains orphan metadata")
}

func TestReplacementFanoutSharedBudgetRotates(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"name"})
	vec := vector.NewVec(types.T_varchar.ToType())
	for i := 0; i < 8; i++ {
		require.NoError(t, vector.AppendBytes(vec, bytes.Repeat([]byte{byte('a' + i)}, 64<<10), false, mp))
	}
	bat.Vecs[0] = vec
	bat.SetRowCount(8)
	defer bat.Clean(mp)
	schema := api.Schema{Fields: []api.SchemaField{{ID: 1, Name: "name", Type: api.IcebergType{Kind: api.TypeString}}}}
	metadata := retainedDMLDataFileMetadataBytesForSchema(schema, api.PartitionSpec{})
	budget := newDMLMemoryBudget(1<<20, 0)
	objects := &recordingDMLDeleteObjectWriter{}
	files, err := WriteDMLReplacementDataFiles(ctx, DMLReplacementDataFilesRequest{
		TableLocation: "s3://warehouse/orders", Base: dml.CommitBase{Table: "orders", StatementID: "stmt"}, SnapshotID: 10,
		Schema: schema, Attrs: bat.Attrs, Batch: bat, MemoryBudget: budget, ObjectWriter: objects,
	})
	require.NoError(t, err)
	require.Greater(t, len(files), 1)
	require.Len(t, objects.objects, len(files))
	var rows int64
	for _, file := range files {
		rows += file.RecordCount
		_, err := parquet.OpenFile(bytes.NewReader(objects.objects[file.FilePath]), file.FileSizeInBytes)
		require.NoError(t, err)
	}
	require.Equal(t, int64(8), rows)
	require.Equal(t, metadata*int64(len(files)), budget.usedBytes())
}

func TestFanoutSharedBudgetPartitionedOutput(t *testing.T) {
	ctx := context.Background()
	schema := api.Schema{Fields: []api.SchemaField{
		{ID: 1, Name: "id", Required: true, Type: api.IcebergType{Kind: api.TypeLong}},
		{ID: 2, Name: "region", Type: api.IcebergType{Kind: api.TypeString}},
		{ID: 3, Name: "payload", Type: api.IcebergType{Kind: api.TypeString}},
	}}
	spec := api.PartitionSpec{SpecID: 7, Fields: []api.PartitionField{{SourceID: 2, FieldID: 1000, Name: "region", Transform: "identity"}}}
	for _, partitions := range []int{1, 2, 8, 9} {
		t.Run(fmt.Sprintf("partitions_%d", partitions), func(t *testing.T) {
			const limit = int64(4 << 20)
			metadataBytes := retainedDMLDataFileMetadataBytesForSchema(schema, spec)
			budget := newDMLMemoryBudget(limit, 0)
			objects := &recordingDMLDeleteObjectWriter{}
			writer, err := icebergwritepkg.NewFanoutParquetDataWriter(ctx, icebergwritepkg.FanoutWriterConfig{
				Schema: schema, PartitionSpec: spec, TableLocation: "s3://warehouse/table",
			}, bufferedDataFileOutputFactory{Writer: objects, MemoryBudget: budget, RetainedMetadataBytes: metadataBytes})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, writer.Abort(ctx)) })
			want := make(map[int64]string)
			for round := 0; round < 3; round++ {
				for part := 0; part < partitions; part++ {
					id := int64(round*partitions + part)
					region := fmt.Sprintf("p%d", part)
					if part == 0 {
						region = ""
					}
					want[id] = region
					func() {
						mp := mpool.MustNewZero()
						bat := batch.New([]string{"id", "region", "payload"})
						bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
						bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
						bat.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
						defer bat.Clean(mp)
						require.NoError(t, vector.AppendFixed[int64](bat.Vecs[0], id, false, mp))
						require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(region), part == 0, mp))
						require.NoError(t, vector.AppendBytes(bat.Vecs[2], bytes.Repeat([]byte{byte('a' + id)}, 64<<10), false, mp))
						bat.SetRowCount(1)
						require.NoError(t, writer.WriteBatch(ctx, bat.Attrs, bat))
						require.LessOrEqual(t, budget.usedBytes(), limit)
						require.GreaterOrEqual(t, budget.usedBytes(), metadataBytes*int64(len(objects.objects)))
					}()
				}
			}
			if partitions >= 8 {
				require.NotEmpty(t, objects.objects, "pressure must close a file before final Close")
			}
			files, err := writer.Close(ctx)
			require.NoError(t, err)
			require.Len(t, objects.objects, len(files))
			require.Equal(t, metadataBytes*int64(len(files)), budget.usedBytes())
			seen := make(map[int64]string)
			fileCounts := make(map[string]int)
			for _, file := range files {
				func() {
					reader := parquet.NewReader(bytes.NewReader(objects.objects[file.FilePath]))
					defer reader.Close()
					rows := make([]parquet.Row, file.RecordCount)
					n, err := reader.ReadRows(rows)
					require.True(t, err == nil || errors.Is(err, io.EOF), "%v", err)
					require.Equal(t, int(file.RecordCount), n)
					columns := reader.Schema().Columns()
					indexes := make(map[string]int)
					for i, path := range columns {
						indexes[path[0]] = i
					}
					var nulls int64
					var minID, maxID int64
					for i, row := range rows {
						id := row[indexes["id"]].Int64()
						if i == 0 || id < minID {
							minID = id
						}
						if i == 0 || id > maxID {
							maxID = id
						}
						region := ""
						if row[indexes["region"]].IsNull() {
							nulls++
						} else {
							region = string(row[indexes["region"]].ByteArray())
						}
						require.NotContains(t, seen, id)
						seen[id] = region
						require.True(t, bytes.Equal(bytes.Repeat([]byte{byte('a' + id)}, 64<<10), row[indexes["payload"]].ByteArray()), "payload mismatch for id %d", id)
						if region == "" {
							require.Nil(t, file.Partition["region"])
						} else {
							require.Equal(t, region, file.Partition["region"])
						}
					}
					fileCounts[seen[rows[0][indexes["id"]].Int64()]]++
					require.Equal(t, minID, int64(binary.LittleEndian.Uint64(file.LowerBounds[1])))
					require.Equal(t, maxID, int64(binary.LittleEndian.Uint64(file.UpperBounds[1])))
					require.Equal(t, file.RecordCount, file.ValueCounts[1])
					require.Equal(t, file.RecordCount, file.ValueCounts[2])
					require.Equal(t, nulls, file.NullValueCounts[2])
					require.Equal(t, int64(0), file.NullValueCounts[1])
				}()
			}
			require.Equal(t, want, seen)
			if partitions >= 8 {
				reopened := false
				for _, n := range fileCounts {
					reopened = reopened || n > 1
				}
				require.True(t, reopened, "an evicted partition must reopen")
			}
		})
	}
}
