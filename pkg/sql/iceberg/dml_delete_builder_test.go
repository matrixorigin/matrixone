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
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
)

func TestBuildDMLDeleteActionStreamWritesEqualityDeleteObject(t *testing.T) {
	writer := &recordingDMLDeleteObjectWriter{}
	base := dml.CommitBase{
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		StatementID:    "delete-sensitive-email",
		IdempotencyKey: "idem-delete",
		BaseSnapshotID: 10,
	}
	stream, err := BuildDMLDeleteActionStream(context.Background(), DMLDeleteActionStreamRequest{
		TableLocation: "s3://warehouse/gold/orders",
		Schema: api.Schema{SchemaID: 9, Fields: []api.SchemaField{
			{ID: 1, Name: "id", Required: true, Type: api.IcebergType{Kind: api.TypeLong}},
			{ID: 2, Name: "region", Type: api.IcebergType{Kind: api.TypeString}},
		}},
		Base:         base,
		SnapshotID:   11,
		ObjectWriter: writer,
		Targets: []DMLMatchedDeleteTarget{{
			DataFile: api.DataFile{
				FilePath:  "s3://warehouse/gold/orders/data/part-1.parquet",
				Partition: map[string]any{"created_day": int32(19895)},
				SpecID:    7,
			},
			EqualityIDs:     []int{1, 2},
			PredicateStable: true,
			EqualityRows: []dml.EqualityDeleteRow{
				{Values: map[int]any{1: int64(44), 2: "ksa"}},
			},
		}},
	})
	if err != nil {
		t.Fatalf("build DML delete action stream: %v", err)
	}
	if stream.Operation != dml.OperationDelete || len(stream.Actions) != 1 {
		t.Fatalf("unexpected stream: %+v", stream)
	}
	action := stream.Actions[0]
	if action.Kind != dml.ActionAddEqualityDelete {
		t.Fatalf("expected equality delete action, got %s", action.Kind)
	}
	deleteFile := action.DeleteFile
	if deleteFile.Content != api.DataFileContentEqualityDelete || deleteFile.RecordCount != 1 {
		t.Fatalf("unexpected equality delete file: %+v", deleteFile)
	}
	if deleteFile.SpecID != 7 || deleteFile.DeleteSchemaID != 9 {
		t.Fatalf("unexpected spec/schema on delete file: %+v", deleteFile)
	}
	if deleteFile.Partition["created_day"] != int32(19895) {
		t.Fatalf("unexpected partition: %+v", deleteFile.Partition)
	}
	if !strings.Contains(deleteFile.FilePath, "/delete/equality/") {
		t.Fatalf("expected equality delete path, got %s", deleteFile.FilePath)
	}
	if strings.Contains(deleteFile.FilePath, "delete-sensitive-email") || strings.Contains(deleteFile.FilePath, "part-1.parquet") {
		t.Fatalf("delete file path leaked raw statement/data path: %s", deleteFile.FilePath)
	}
	payload := writer.objects[deleteFile.FilePath]
	if len(payload) == 0 {
		t.Fatalf("delete object was not written: %s", deleteFile.FilePath)
	}
	pf, err := parquet.OpenFile(bytes.NewReader(payload), int64(len(payload)))
	if err != nil {
		t.Fatalf("open equality delete parquet: %v", err)
	}
	if pf.Root().Column("id").ID() != 1 || pf.Root().Column("region").ID() != 2 {
		t.Fatalf("equality delete parquet field ids mismatch")
	}
}

func TestBuildDMLDeleteActionStreamWritesPositionDeleteObject(t *testing.T) {
	writer := &recordingDMLDeleteObjectWriter{}
	base := dml.CommitBase{
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		StatementID:    "delete-position",
		IdempotencyKey: "idem-delete",
		BaseSnapshotID: 10,
	}
	stream, err := BuildDMLDeleteActionStream(context.Background(), DMLDeleteActionStreamRequest{
		TableLocation:  "s3://warehouse/gold/orders",
		Schema:         api.Schema{SchemaID: 9},
		Base:           base,
		SnapshotID:     11,
		DeleteSchemaID: 9,
		ObjectWriter:   writer,
		Targets: []DMLMatchedDeleteTarget{{
			DataFile: api.DataFile{
				FilePath:  "s3://warehouse/gold/orders/data/part-1.parquet",
				Partition: map[string]any{"created_day": int32(19895)},
				SpecID:    7,
			},
			HasRowOrdinal: true,
			PositionRows: []dml.PositionDeleteRow{
				{Pos: 8},
				{Pos: 2},
			},
		}},
	})
	if err != nil {
		t.Fatalf("build DML position delete stream: %v", err)
	}
	if len(stream.Actions) != 1 || stream.Actions[0].Kind != dml.ActionAddPositionDelete {
		t.Fatalf("expected one position delete action, got %+v", stream.Actions)
	}
	deleteFile := stream.Actions[0].DeleteFile
	if deleteFile.Content != api.DataFileContentPositionDelete || deleteFile.ReferencedDataFile != "s3://warehouse/gold/orders/data/part-1.parquet" {
		t.Fatalf("unexpected position delete file: %+v", deleteFile)
	}
	if !strings.Contains(deleteFile.FilePath, "/delete/position/") {
		t.Fatalf("expected position delete path, got %s", deleteFile.FilePath)
	}
	payload := writer.objects[deleteFile.FilePath]
	pf, err := parquet.OpenFile(bytes.NewReader(payload), int64(len(payload)))
	if err != nil {
		t.Fatalf("open position delete parquet: %v", err)
	}
	if pf.Root().Column("file_path").ID() != 2147483546 || pf.Root().Column("pos").ID() != 2147483545 {
		t.Fatalf("position delete parquet reserved field ids mismatch")
	}
}

func TestBuildDMLDeleteActionStreamConsumesMatchedBatch(t *testing.T) {
	writer := &recordingDMLDeleteObjectWriter{}
	bat, cleanup := newMatchedDeleteBatch(t)
	defer cleanup()

	stream, err := BuildDMLDeleteActionStream(context.Background(), DMLDeleteActionStreamRequest{
		TableLocation:  "s3://warehouse/gold/orders",
		Schema:         api.Schema{SchemaID: 9},
		Base:           dml.CommitBase{Namespace: api.Namespace{"sales"}, Table: "orders", StatementID: "delete-batch", IdempotencyKey: "idem-delete-batch", BaseSnapshotID: 10},
		SnapshotID:     11,
		DeleteSchemaID: 9,
		ObjectWriter:   writer,
		MatchedBatches: []DMLMatchedRowsBatchRequest{{
			DataFile:            api.DataFile{FilePath: "s3://warehouse/gold/orders/data/part-batch.parquet", SpecID: 7},
			Batch:               bat,
			IncludePositionRows: true,
			StartRowOrdinal:     20,
		}},
	})
	if err != nil {
		t.Fatalf("build DML delete stream from matched batch: %v", err)
	}
	if len(stream.Actions) != 1 || stream.Actions[0].Kind != dml.ActionAddPositionDelete {
		t.Fatalf("expected one position delete action, got %+v", stream.Actions)
	}
	if stream.Profile.MatchedRows != 2 {
		t.Fatalf("expected matched row count from batch, got %+v", stream.Profile)
	}
	deleteFile := stream.Actions[0].DeleteFile
	if deleteFile.ReferencedDataFile != "s3://warehouse/gold/orders/data/part-batch.parquet" || deleteFile.RecordCount != 2 {
		t.Fatalf("unexpected delete file from matched batch: %+v", deleteFile)
	}
	if len(writer.objects) != 1 {
		t.Fatalf("expected one delete object write, got %d", len(writer.objects))
	}
}

func TestBuildDMLUpdateActionStreamAddsReplacementDataFiles(t *testing.T) {
	writer := &recordingDMLDeleteObjectWriter{}
	base := dml.CommitBase{
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		StatementID:    "update-position",
		IdempotencyKey: "idem-update",
		BaseSnapshotID: 10,
	}
	stream, err := BuildDMLUpdateActionStream(context.Background(), DMLUpdateActionStreamRequest{
		DMLDeleteActionStreamRequest: DMLDeleteActionStreamRequest{
			TableLocation:  "s3://warehouse/gold/orders",
			Schema:         api.Schema{SchemaID: 9},
			Base:           base,
			SnapshotID:     11,
			DeleteSchemaID: 9,
			ObjectWriter:   writer,
			Targets: []DMLMatchedDeleteTarget{{
				DataFile:      api.DataFile{FilePath: "s3://warehouse/gold/orders/data/part-1.parquet", SpecID: 7},
				HasRowOrdinal: true,
				PositionRows:  []dml.PositionDeleteRow{{Pos: 2}},
			}},
		},
		AppendedDataFiles: []api.DataFile{{
			Content:         api.DataFileContentData,
			FilePath:        "s3://warehouse/gold/orders/data/replacement-1.parquet",
			FileFormat:      "parquet",
			RecordCount:     1,
			FileSizeInBytes: 128,
			SpecID:          7,
		}},
	})
	if err != nil {
		t.Fatalf("build DML update stream: %v", err)
	}
	if stream.Operation != dml.OperationUpdate || len(stream.Actions) != 2 {
		t.Fatalf("unexpected update stream: %+v", stream)
	}
	if stream.Actions[0].Kind != dml.ActionAddPositionDelete || stream.Actions[1].Kind != dml.ActionAppendData {
		t.Fatalf("unexpected update actions: %+v", stream.Actions)
	}
	if len(writer.objects) != 1 {
		t.Fatalf("expected one delete object write, got %d", len(writer.objects))
	}
	for path := range writer.objects {
		if !strings.Contains(path, "/mo-dml/update/") {
			t.Fatalf("update delete object must use update-scoped path, got %s", path)
		}
	}
	if stream.Actions[1].File.FilePath != "s3://warehouse/gold/orders/data/replacement-1.parquet" {
		t.Fatalf("unexpected replacement file: %+v", stream.Actions[1].File)
	}
}

func TestBuildDMLMergeActionStreamCombinesMatchedAndUnmatchedActions(t *testing.T) {
	writer := &recordingDMLDeleteObjectWriter{}
	base := dml.CommitBase{
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		StatementID:    "merge-statement",
		IdempotencyKey: "idem-merge",
		BaseSnapshotID: 10,
	}
	stream, err := BuildDMLMergeActionStream(context.Background(), DMLMergeActionStreamRequest{
		TableLocation: "s3://warehouse/gold/orders",
		Schema: api.Schema{SchemaID: 9, Fields: []api.SchemaField{
			{ID: 1, Name: "id", Required: true, Type: api.IcebergType{Kind: api.TypeInt}},
			{ID: 2, Name: "name", Type: api.IcebergType{Kind: api.TypeString}},
		}},
		Base:           base,
		SnapshotID:     11,
		DeleteSchemaID: 9,
		ObjectWriter:   writer,
		MatchedDeletes: []DMLMatchedDeleteTarget{{
			DataFile:      api.DataFile{FilePath: "s3://warehouse/gold/orders/data/delete-target.parquet", SpecID: 7},
			HasRowOrdinal: true,
			PositionRows:  []dml.PositionDeleteRow{{Pos: 4}},
		}},
		MatchedUpdates: []DMLMatchedUpdateTarget{{
			DeleteTarget: DMLMatchedDeleteTarget{
				DataFile:      api.DataFile{FilePath: "s3://warehouse/gold/orders/data/update-target.parquet", SpecID: 7},
				HasRowOrdinal: true,
				PositionRows:  []dml.PositionDeleteRow{{Pos: 9}},
			},
			ReplacementFiles: []api.DataFile{{
				Content:         api.DataFileContentData,
				FilePath:        "s3://warehouse/gold/orders/data/updated-row.parquet",
				FileFormat:      "parquet",
				RecordCount:     1,
				FileSizeInBytes: 128,
				SpecID:          7,
			}},
		}},
		UnmatchedAppends: []api.DataFile{{
			Content:         api.DataFileContentData,
			FilePath:        "s3://warehouse/gold/orders/data/inserted-row.parquet",
			FileFormat:      "parquet",
			RecordCount:     1,
			FileSizeInBytes: 128,
			SpecID:          7,
		}},
	})
	if err != nil {
		t.Fatalf("build DML merge stream: %v", err)
	}
	if stream.Operation != dml.OperationMerge || len(stream.Actions) != 4 {
		t.Fatalf("unexpected merge stream: %+v", stream)
	}
	gotKinds := []dml.ActionKind{
		stream.Actions[0].Kind,
		stream.Actions[1].Kind,
		stream.Actions[2].Kind,
		stream.Actions[3].Kind,
	}
	wantKinds := []dml.ActionKind{
		dml.ActionAddPositionDelete,
		dml.ActionAddPositionDelete,
		dml.ActionAppendData,
		dml.ActionAppendData,
	}
	for idx := range wantKinds {
		if gotKinds[idx] != wantKinds[idx] {
			t.Fatalf("unexpected action kinds at %d: got %v want %v", idx, gotKinds, wantKinds)
		}
	}
	if len(writer.objects) != 2 {
		t.Fatalf("expected two delete object writes, got %d", len(writer.objects))
	}
	for path := range writer.objects {
		if !strings.Contains(path, "/mo-dml/merge/") {
			t.Fatalf("merge delete object must use merge-scoped path, got %s", path)
		}
	}
	if stream.Profile.PositionDeleteFiles != 2 || stream.Profile.AddedDataFiles != 2 {
		t.Fatalf("unexpected merge profile: %+v", stream.Profile)
	}
}

func TestBuildDMLMergeActionStreamMaterializesReplacementBatches(t *testing.T) {
	writer := &recordingDMLDeleteObjectWriter{}
	updateBat, updateCleanup := newReplacementExecutorBatch(t)
	defer updateCleanup()
	insertBat, insertCleanup := newReplacementExecutorBatch(t)
	defer insertCleanup()
	base := dml.CommitBase{
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		StatementID:    "merge-batch",
		IdempotencyKey: "idem-merge-batch",
		BaseSnapshotID: 10,
	}
	stream, err := BuildDMLMergeActionStream(context.Background(), DMLMergeActionStreamRequest{
		TableLocation: "s3://warehouse/gold/orders",
		Schema: api.Schema{SchemaID: 9, Fields: []api.SchemaField{
			{ID: 1, Name: "id", Required: true, Type: api.IcebergType{Kind: api.TypeInt}},
			{ID: 2, Name: "name", Type: api.IcebergType{Kind: api.TypeString}},
		}},
		Base:           base,
		SnapshotID:     11,
		DeleteSchemaID: 9,
		ObjectWriter:   writer,
		MatchedUpdates: []DMLMatchedUpdateTarget{{
			DeleteTarget: DMLMatchedDeleteTarget{
				DataFile:      api.DataFile{FilePath: "s3://warehouse/gold/orders/data/update-target.parquet", SpecID: 7},
				HasRowOrdinal: true,
				PositionRows:  []dml.PositionDeleteRow{{Pos: 9}},
			},
		}},
		MatchedUpdateReplacementBatches: []DMLReplacementDataBatch{{
			Attrs:        []string{"id", "name"},
			Batch:        updateBat,
			ObjectWriter: writer,
		}},
		UnmatchedAppendBatches: []DMLReplacementDataBatch{{
			Attrs:        []string{"id", "name"},
			Batch:        insertBat,
			ObjectWriter: writer,
		}},
	})
	if err != nil {
		t.Fatalf("build DML merge stream from replacement batches: %v", err)
	}
	if stream.Operation != dml.OperationMerge || len(stream.Actions) != 3 {
		t.Fatalf("unexpected merge stream: %+v", stream)
	}
	if stream.Profile.PositionDeleteFiles != 1 || stream.Profile.AddedDataFiles != 2 {
		t.Fatalf("unexpected merge profile: %+v", stream.Profile)
	}
	var replacementObjects, deleteObjects int
	for path := range writer.objects {
		switch {
		case strings.Contains(path, "/replacement/"):
			replacementObjects++
			if !strings.Contains(path, "/mo-dml/merge/") || strings.Contains(path, "merge-batch") {
				t.Fatalf("unexpected merge replacement object path: %s", path)
			}
		case strings.Contains(path, "/delete/position/"):
			deleteObjects++
		}
	}
	if replacementObjects != 2 || deleteObjects != 1 {
		t.Fatalf("expected two replacement objects and one delete object, replacement=%d delete=%d paths=%#v", replacementObjects, deleteObjects, writer.objects)
	}
}

func TestBuildDMLOverwriteActionStreamCombinesDeletesAndReplacements(t *testing.T) {
	base := dml.CommitBase{
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		StatementID:    "overwrite-statement",
		IdempotencyKey: "idem-overwrite",
		BaseSnapshotID: 10,
	}
	stream, err := BuildDMLOverwriteActionStream(context.Background(), DMLOverwriteActionStreamRequest{
		Base:  base,
		Scope: dml.OverwriteTable,
		AffectedDataFiles: []api.DataFile{{
			Content:    api.DataFileContentData,
			FilePath:   "s3://warehouse/gold/orders/data/old.parquet",
			FileFormat: "parquet",
			SpecID:     7,
		}},
		ReplacementFiles: []api.DataFile{{
			Content:         api.DataFileContentData,
			FilePath:        "s3://warehouse/gold/orders/data/new.parquet",
			FileFormat:      "parquet",
			RecordCount:     10,
			FileSizeInBytes: 512,
			SpecID:          7,
		}},
	})
	if err != nil {
		t.Fatalf("build DML overwrite stream: %v", err)
	}
	if stream.Operation != dml.OperationOverwrite || len(stream.Actions) != 2 {
		t.Fatalf("unexpected overwrite stream: %+v", stream)
	}
	if stream.Actions[0].Kind != dml.ActionDeleteDataFile || stream.Actions[1].Kind != dml.ActionAppendData {
		t.Fatalf("unexpected overwrite actions: %+v", stream.Actions)
	}
	if stream.Profile.DeletedDataFiles != 1 || stream.Profile.AddedDataFiles != 1 {
		t.Fatalf("unexpected overwrite profile: %+v", stream.Profile)
	}
	paths, err := BuildDMLManifestPaths(context.Background(), DMLManifestPathRequest{
		TableLocation: "s3://warehouse/gold/orders",
		Stream:        *stream,
		SnapshotID:    12,
	})
	if err != nil {
		t.Fatalf("build overwrite manifest paths: %v", err)
	}
	if paths.DataManifestPath == "" || paths.DeleteManifestPath != "" || paths.ManifestListPath == "" {
		t.Fatalf("overwrite should need only data manifest and list paths: %+v", paths)
	}
}

func TestBuildDMLOverwriteActionStreamUsesAffectedScanPlan(t *testing.T) {
	stream, err := BuildDMLOverwriteActionStream(context.Background(), DMLOverwriteActionStreamRequest{
		Base: dml.CommitBase{
			Namespace:      api.Namespace{"sales"},
			Table:          "orders",
			StatementID:    "overwrite-scan",
			IdempotencyKey: "idem-overwrite-scan",
			BaseSnapshotID: 10,
		},
		Scope: dml.OverwriteTable,
		AffectedScanPlan: &api.IcebergScanPlan{
			DataTasks: []api.DataFileTask{
				{DataFile: api.DataFile{FilePath: "s3://warehouse/gold/orders/data/old-1.parquet", FileFormat: "parquet", RecordCount: 1, FileSizeInBytes: 10}},
				{DataFile: api.DataFile{FilePath: "s3://warehouse/gold/orders/data/old-1.parquet", FileFormat: "parquet", RecordCount: 1, FileSizeInBytes: 10}},
				{DataFile: api.DataFile{FilePath: "s3://warehouse/gold/orders/data/old-2.parquet", FileFormat: "parquet", RecordCount: 2, FileSizeInBytes: 20}},
			},
		},
		ReplacementFiles: []api.DataFile{{
			Content:         api.DataFileContentData,
			FilePath:        "s3://warehouse/gold/orders/data/new.parquet",
			FileFormat:      "parquet",
			RecordCount:     3,
			FileSizeInBytes: 128,
		}},
	})
	if err != nil {
		t.Fatalf("build overwrite from scan plan: %v", err)
	}
	if len(stream.Actions) != 3 {
		t.Fatalf("expected two unique affected files plus replacement, got %+v", stream.Actions)
	}
	if stream.Profile.DeletedDataFiles != 2 || stream.Profile.AddedDataFiles != 1 {
		t.Fatalf("unexpected overwrite profile: %+v", stream.Profile)
	}
}

func TestBuildDMLOverwriteActionStreamFiltersAffectedScanPlanByPartition(t *testing.T) {
	stream, err := BuildDMLOverwriteActionStream(context.Background(), DMLOverwriteActionStreamRequest{
		Base: dml.CommitBase{
			Namespace:      api.Namespace{"sales"},
			Table:          "orders",
			StatementID:    "overwrite-partition",
			IdempotencyKey: "idem-overwrite-partition",
			BaseSnapshotID: 10,
		},
		Scope:     dml.OverwritePartition,
		Partition: map[string]any{"region": "ksa"},
		AffectedScanPlan: &api.IcebergScanPlan{
			DataTasks: []api.DataFileTask{
				{DataFile: api.DataFile{FilePath: "s3://warehouse/gold/orders/data/ksa-a.parquet", Partition: map[string]any{"region": "ksa"}, SpecID: 7}},
				{DataFile: api.DataFile{FilePath: "s3://warehouse/gold/orders/data/ksa-b.parquet", Partition: map[string]any{"region": "ksa"}, SpecID: 7}},
				{DataFile: api.DataFile{FilePath: "s3://warehouse/gold/orders/data/uae-a.parquet", Partition: map[string]any{"region": "uae"}, SpecID: 7}},
			},
		},
	})
	if err != nil {
		t.Fatalf("build partition overwrite stream: %v", err)
	}
	if stream.Profile.DeletedDataFiles != 2 || len(stream.Actions) != 2 {
		t.Fatalf("expected two partition-scoped deletes, got actions=%+v profile=%+v", stream.Actions, stream.Profile)
	}
	for _, action := range stream.Actions {
		if action.Kind != dml.ActionDeleteDataFile || action.ReplacedFile.Partition["region"] != "ksa" {
			t.Fatalf("unexpected partition overwrite action: %+v", action)
		}
	}
}

func TestBuildDMLOverwriteActionStreamRejectsPartitionScopeWithoutTuple(t *testing.T) {
	_, err := BuildDMLOverwriteActionStream(context.Background(), DMLOverwriteActionStreamRequest{
		Base: dml.CommitBase{
			Namespace:      api.Namespace{"sales"},
			Table:          "orders",
			StatementID:    "overwrite-partition",
			IdempotencyKey: "idem-overwrite-partition",
			BaseSnapshotID: 10,
		},
		Scope: dml.OverwritePartition,
		AffectedScanPlan: &api.IcebergScanPlan{
			DataTasks: []api.DataFileTask{{DataFile: api.DataFile{FilePath: "s3://warehouse/gold/orders/data/ksa-a.parquet", Partition: map[string]any{"region": "ksa"}}}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "explicit partition tuple") {
		t.Fatalf("expected missing partition tuple error, got %v", err)
	}
}

func TestBuildDMLOverwriteActionStreamRejectsDeleteTasksInAffectedScanPlan(t *testing.T) {
	_, err := BuildDMLOverwriteActionStream(context.Background(), DMLOverwriteActionStreamRequest{
		Base: dml.CommitBase{
			Namespace:      api.Namespace{"sales"},
			Table:          "orders",
			StatementID:    "overwrite-delete-tasks",
			IdempotencyKey: "idem-overwrite-delete-tasks",
			BaseSnapshotID: 10,
		},
		AffectedScanPlan: &api.IcebergScanPlan{
			DataTasks:   []api.DataFileTask{{DataFile: api.DataFile{FilePath: "s3://warehouse/gold/orders/data/old.parquet"}}},
			DeleteTasks: []api.DeleteFileTask{{DataFile: api.DataFile{FilePath: "s3://warehouse/gold/orders/delete/pos.parquet"}}},
		},
		ReplacementFiles: []api.DataFile{{FilePath: "s3://warehouse/gold/orders/data/new.parquet", FileFormat: "parquet", RecordCount: 1, FileSizeInBytes: 10}},
	})
	if err == nil || !strings.Contains(err.Error(), "delete tasks is not supported") {
		t.Fatalf("expected delete task fail-fast, got %v", err)
	}
}

func TestBuildDMLDeleteActionStreamRejectsUnmaterializableTarget(t *testing.T) {
	_, err := BuildDMLDeleteActionStream(context.Background(), DMLDeleteActionStreamRequest{
		TableLocation:  "s3://warehouse/gold/orders",
		Schema:         api.Schema{SchemaID: 9},
		Base:           dml.CommitBase{Namespace: api.Namespace{"sales"}, Table: "orders", IdempotencyKey: "idem-delete"},
		SnapshotID:     11,
		ObjectWriter:   &recordingDMLDeleteObjectWriter{},
		Targets:        []DMLMatchedDeleteTarget{{DataFile: api.DataFile{FilePath: "s3://warehouse/gold/orders/data/part-1.parquet"}}},
		DeleteSchemaID: 9,
	})
	if err == nil {
		t.Fatal("expected unmaterializable target to be rejected")
	}
	if !strings.Contains(err.Error(), "cannot be materialized") {
		t.Fatalf("unexpected error: %v", err)
	}
}

type recordingDMLDeleteObjectWriter struct {
	objects map[string][]byte
}

func (w *recordingDMLDeleteObjectWriter) WriteObject(ctx context.Context, location string, payload []byte) error {
	if w.objects == nil {
		w.objects = make(map[string][]byte)
	}
	w.objects[location] = append([]byte(nil), payload...)
	return nil
}
