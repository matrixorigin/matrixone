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
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

func TestDMLActionExecutorCommitDeleteWritesDeleteObjectAndCommits(t *testing.T) {
	deleteWriter := &recordingDMLDeleteObjectWriter{}
	manifestWriter := &fakeSQLManifestWriter{}
	committer := &fakeDMLWorkflowCommitter{
		result: &api.CommitResult{
			SnapshotID:           31,
			CommitID:             "commit-delete-31",
			MetadataLocationHash: "metadata-delete-31",
			Verified:             true,
		},
	}
	executor := DMLActionExecutor{
		Workflow: dml.CommitWorkflow{
			ManifestWriter: manifestWriter,
			Committer:      committer,
		},
		TableLocation:  "s3://warehouse/gold/orders/",
		SnapshotID:     31,
		SequenceNumber: 7,
	}

	executor = withSQLTestDMLExecutorMetadata(executor, 1)
	result, err := executor.CommitDelete(context.Background(), DMLDeleteActionStreamRequest{
		Schema: api.Schema{SchemaID: 9},
		Base: dml.CommitBase{
			Namespace:      api.Namespace{"gold"},
			Table:          "orders",
			TargetRef:      "main",
			BaseSnapshotID: 30,
			IdempotencyKey: "idem-delete",
			StatementID:    "stmt-delete",
		},
		DeleteSchemaID: 9,
		ObjectWriter:   deleteWriter,
		Targets: []DMLMatchedDeleteTarget{{
			DataFile:      api.DataFile{FilePath: "s3://warehouse/gold/orders/data/part-1.parquet", SpecID: 1},
			HasRowOrdinal: true,
			PositionRows:  []dml.PositionDeleteRow{{Pos: 5}},
		}},
	})
	if err != nil {
		t.Fatalf("commit DML delete: %v", err)
	}
	if result.CommitResult == nil || result.CommitResult.CommitID != "commit-delete-31" {
		t.Fatalf("unexpected commit result: %+v", result.CommitResult)
	}
	if len(deleteWriter.objects) != 1 {
		t.Fatalf("expected one delete object write, got %d", len(deleteWriter.objects))
	}
	for path := range deleteWriter.objects {
		if !strings.Contains(path, "/mo-dml/delete/") || !strings.Contains(path, "/delete/position/") {
			t.Fatalf("unexpected delete object path: %s", path)
		}
	}
	if len(manifestWriter.paths) != 2 || result.Request.DeleteManifestPath == "" || result.Request.ManifestListPath == "" || result.Request.DataManifestPath != "" {
		t.Fatalf("expected delete manifest and manifest list writes, paths=%#v request=%+v", manifestWriter.paths, result.Request)
	}
	if len(committer.requests) != 1 || committer.requests[0].IdempotencyKey != "idem-delete" || committer.requests[0].TargetRef != "main" {
		t.Fatalf("unexpected catalog commit request: %+v", committer.requests)
	}
	if result.Profile["operation"] != "delete" || result.Profile["matched_rows"] != "1" || result.Profile["added_delete_files"] != "1" {
		t.Fatalf("unexpected profile: %#v", result.Profile)
	}
	if result.Profile["delete_manifest"] == "" || strings.Contains(result.Profile["delete_manifest"], "s3://warehouse") {
		t.Fatalf("delete manifest profile path must be redacted: %#v", result.Profile)
	}
}

func TestDMLActionExecutorRejectsUnconfiguredWorkflowBeforeObjectWrite(t *testing.T) {
	deleteWriter := &recordingDMLDeleteObjectWriter{}
	executor := DMLActionExecutor{
		TableLocation:  "s3://warehouse/gold/orders",
		SnapshotID:     31,
		SequenceNumber: 7,
	}
	_, err := executor.CommitDelete(context.Background(), DMLDeleteActionStreamRequest{
		Schema: api.Schema{SchemaID: 9},
		Base: dml.CommitBase{
			Namespace:      api.Namespace{"gold"},
			Table:          "orders",
			TargetRef:      "main",
			BaseSnapshotID: 30,
			IdempotencyKey: "idem-delete",
			StatementID:    "stmt-delete",
		},
		DeleteSchemaID: 9,
		ObjectWriter:   deleteWriter,
		Targets: []DMLMatchedDeleteTarget{{
			DataFile:      api.DataFile{FilePath: "s3://warehouse/gold/orders/data/part-1.parquet", SpecID: 1},
			HasRowOrdinal: true,
			PositionRows:  []dml.PositionDeleteRow{{Pos: 5}},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "requires a manifest writer") {
		t.Fatalf("expected manifest writer preflight error, got %v", err)
	}
	if len(deleteWriter.objects) != 0 {
		t.Fatalf("preflight failure must not write delete objects: %#v", deleteWriter.objects)
	}
}

func TestDMLActionExecutorValidateCommitPrerequisitesRejectsMissingInputs(t *testing.T) {
	for _, tc := range []struct {
		name           string
		executor       DMLActionExecutor
		tableLocation  string
		snapshotID     int64
		wantErrMessage string
	}{
		{
			name: "missing manifest writer",
			executor: DMLActionExecutor{
				Workflow:       dml.CommitWorkflow{Committer: &fakeDMLWorkflowCommitter{}},
				SequenceNumber: 7,
			},
			tableLocation:  "s3://warehouse/gold/orders",
			snapshotID:     31,
			wantErrMessage: "requires a manifest writer",
		},
		{
			name: "missing committer",
			executor: DMLActionExecutor{
				Workflow:       dml.CommitWorkflow{ManifestWriter: &fakeSQLManifestWriter{}},
				SequenceNumber: 7,
			},
			tableLocation:  "s3://warehouse/gold/orders",
			snapshotID:     31,
			wantErrMessage: "requires a committer",
		},
		{
			name: "missing table location",
			executor: DMLActionExecutor{
				Workflow: dml.CommitWorkflow{
					ManifestWriter: &fakeSQLManifestWriter{},
					Committer:      &fakeDMLWorkflowCommitter{},
				},
				SequenceNumber: 7,
			},
			tableLocation:  "   ",
			snapshotID:     31,
			wantErrMessage: "requires table location",
		},
		{
			name: "missing snapshot",
			executor: DMLActionExecutor{
				Workflow: dml.CommitWorkflow{
					ManifestWriter: &fakeSQLManifestWriter{},
					Committer:      &fakeDMLWorkflowCommitter{},
				},
				SequenceNumber: 7,
			},
			tableLocation:  "s3://warehouse/gold/orders",
			wantErrMessage: "requires positive snapshot and sequence numbers",
		},
		{
			name: "missing sequence",
			executor: DMLActionExecutor{
				Workflow: dml.CommitWorkflow{
					ManifestWriter: &fakeSQLManifestWriter{},
					Committer:      &fakeDMLWorkflowCommitter{},
				},
			},
			tableLocation:  "s3://warehouse/gold/orders",
			snapshotID:     31,
			wantErrMessage: "requires positive snapshot and sequence numbers",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.executor.validateCommitPrerequisites(context.Background(), tc.tableLocation, tc.snapshotID)
			if err == nil || !strings.Contains(err.Error(), tc.wantErrMessage) {
				t.Fatalf("expected %q error, got %v", tc.wantErrMessage, err)
			}
		})
	}

	executor := DMLActionExecutor{
		Workflow: dml.CommitWorkflow{
			ManifestWriter: &fakeSQLManifestWriter{},
			Committer:      &fakeDMLWorkflowCommitter{},
		},
		SequenceNumber: 8,
	}
	if err := executor.validateCommitPrerequisites(context.Background(), "s3://warehouse/gold/orders", 32); err != nil {
		t.Fatalf("valid commit prerequisites should pass: %v", err)
	}
}

func TestDMLActionExecutorCommitUpdateWritesDeleteAndDataManifests(t *testing.T) {
	deleteWriter := &recordingDMLDeleteObjectWriter{}
	manifestWriter := &fakeSQLManifestWriter{}
	committer := &fakeDMLWorkflowCommitter{
		result: &api.CommitResult{
			SnapshotID:           35,
			CommitID:             "commit-update-35",
			MetadataLocationHash: "metadata-update-35",
			Verified:             true,
		},
	}
	executor := DMLActionExecutor{
		Workflow: dml.CommitWorkflow{
			ManifestWriter: manifestWriter,
			Committer:      committer,
		},
		TableLocation:  "s3://warehouse/gold/orders",
		SnapshotID:     35,
		SequenceNumber: 9,
	}

	executor = withSQLTestDMLExecutorMetadata(executor, 1)
	result, err := executor.CommitUpdate(context.Background(), DMLUpdateActionStreamRequest{
		DMLDeleteActionStreamRequest: DMLDeleteActionStreamRequest{
			Schema: api.Schema{SchemaID: 9},
			Base: dml.CommitBase{
				Namespace:      api.Namespace{"gold"},
				Table:          "orders",
				TargetRef:      "main",
				BaseSnapshotID: 34,
				IdempotencyKey: "idem-update",
				StatementID:    "stmt-update",
			},
			DeleteSchemaID: 9,
			ObjectWriter:   deleteWriter,
			Targets: []DMLMatchedDeleteTarget{{
				DataFile:      api.DataFile{FilePath: "s3://warehouse/gold/orders/data/update-target.parquet", SpecID: 1},
				HasRowOrdinal: true,
				PositionRows:  []dml.PositionDeleteRow{{Pos: 6}},
			}},
		},
		AppendedDataFiles: []api.DataFile{{
			FilePath:        "s3://warehouse/gold/orders/data/update-replacement.parquet",
			FileFormat:      "parquet",
			RecordCount:     1,
			FileSizeInBytes: 32,
			SpecID:          1,
		}},
	})
	if err != nil {
		t.Fatalf("commit DML update: %v", err)
	}
	if result.Request.DataManifestPath == "" || result.Request.DeleteManifestPath == "" || result.Request.ManifestListPath == "" {
		t.Fatalf("update should write data/delete manifests and manifest list: %+v", result.Request)
	}
	if len(deleteWriter.objects) != 1 {
		t.Fatalf("expected one delete object write, got %d", len(deleteWriter.objects))
	}
	for path := range deleteWriter.objects {
		if !strings.Contains(path, "/mo-dml/update/") {
			t.Fatalf("update delete object must use update-scoped path: %s", path)
		}
	}
	if len(manifestWriter.paths) != 3 {
		t.Fatalf("expected data manifest, delete manifest, and manifest list writes, got %#v", manifestWriter.paths)
	}
	if result.Profile["operation"] != "update" || result.Profile["added_data_files"] != "1" || result.Profile["added_delete_files"] != "1" {
		t.Fatalf("unexpected update profile: %#v", result.Profile)
	}
}

func TestDMLActionExecutorRejectsTagUpdateBeforeObjectWrite(t *testing.T) {
	deleteWriter := &recordingDMLDeleteObjectWriter{}
	manifestWriter := &fakeSQLManifestWriter{}
	committer := &fakeDMLWorkflowCommitter{}
	executor := DMLActionExecutor{
		Workflow: dml.CommitWorkflow{
			ManifestWriter: manifestWriter,
			Committer:      committer,
		},
		TableLocation:  "s3://warehouse/gold/orders",
		SnapshotID:     35,
		SequenceNumber: 9,
	}

	_, err := executor.CommitUpdate(context.Background(), DMLUpdateActionStreamRequest{
		DMLDeleteActionStreamRequest: DMLDeleteActionStreamRequest{
			Schema: api.Schema{SchemaID: 9},
			Base: dml.CommitBase{
				Namespace:      api.Namespace{"gold"},
				Table:          "orders",
				TargetRef:      "tag:release",
				BaseSnapshotID: 34,
				IdempotencyKey: "idem-update-tag",
				StatementID:    "stmt-update-tag",
			},
			DeleteSchemaID: 9,
			ObjectWriter:   deleteWriter,
			Targets: []DMLMatchedDeleteTarget{{
				DataFile:      api.DataFile{FilePath: "s3://warehouse/gold/orders/data/update-target.parquet", SpecID: 1},
				HasRowOrdinal: true,
				PositionRows:  []dml.PositionDeleteRow{{Pos: 6}},
			}},
		},
		AppendedDataFiles: []api.DataFile{{
			FilePath:        "s3://warehouse/gold/orders/data/update-replacement.parquet",
			FileFormat:      "parquet",
			RecordCount:     1,
			FileSizeInBytes: 32,
			SpecID:          1,
		}},
	})
	if err == nil || !strings.Contains(err.Error(), string(api.ErrUnsupportedFeature)) {
		t.Fatalf("expected tag write rejection, got %v", err)
	}
	if len(deleteWriter.objects) != 0 {
		t.Fatalf("tag write gate must run before delete object writes: %#v", deleteWriter.objects)
	}
	if len(manifestWriter.paths) != 0 || len(committer.requests) != 0 {
		t.Fatalf("tag write gate must run before manifest/catalog commit writes: paths=%#v requests=%#v", manifestWriter.paths, committer.requests)
	}
}

func TestDMLActionExecutorCommitUpdateMaterializesReplacementBatch(t *testing.T) {
	deleteWriter := &recordingDMLDeleteObjectWriter{}
	manifestWriter := &fakeSQLManifestWriter{}
	committer := &fakeDMLWorkflowCommitter{
		result: &api.CommitResult{
			SnapshotID:           36,
			CommitID:             "commit-update-36",
			MetadataLocationHash: "metadata-update-36",
			Verified:             true,
		},
	}
	executor := DMLActionExecutor{
		Workflow: dml.CommitWorkflow{
			ManifestWriter: manifestWriter,
			Committer:      committer,
		},
		TableLocation:  "s3://warehouse/gold/orders",
		SnapshotID:     36,
		SequenceNumber: 10,
	}
	bat, cleanup := newReplacementExecutorBatch(t)
	defer cleanup()

	executor = withSQLTestDMLExecutorMetadata(executor, 0, 1)
	result, err := executor.CommitUpdate(context.Background(), DMLUpdateActionStreamRequest{
		DMLDeleteActionStreamRequest: DMLDeleteActionStreamRequest{
			Schema: api.Schema{SchemaID: 9, Fields: []api.SchemaField{
				{ID: 1, Name: "id", Required: true, Type: api.IcebergType{Kind: api.TypeInt}},
				{ID: 2, Name: "name", Type: api.IcebergType{Kind: api.TypeString}},
			}},
			Base: dml.CommitBase{
				Namespace:      api.Namespace{"gold"},
				Table:          "orders",
				TargetRef:      "main",
				BaseSnapshotID: 35,
				IdempotencyKey: "idem-update-batch",
				StatementID:    "stmt-update-batch",
			},
			DeleteSchemaID: 9,
			ObjectWriter:   deleteWriter,
			Targets: []DMLMatchedDeleteTarget{{
				DataFile:      api.DataFile{FilePath: "s3://warehouse/gold/orders/data/update-target-batch.parquet", SpecID: 1},
				HasRowOrdinal: true,
				PositionRows:  []dml.PositionDeleteRow{{Pos: 9}},
			}},
		},
		ReplacementBatch: DMLReplacementDataBatch{
			Batch:               bat,
			TargetFileSizeBytes: 1,
		},
	})
	if err != nil {
		t.Fatalf("commit DML update with replacement batch: %v", err)
	}
	if result.Request.DataManifestPath == "" || result.Request.DeleteManifestPath == "" || result.Request.ManifestListPath == "" {
		t.Fatalf("update should write data/delete manifests and manifest list: %+v", result.Request)
	}
	var replacementObjects, deleteObjects int
	for path := range deleteWriter.objects {
		switch {
		case strings.Contains(path, "/replacement/"):
			replacementObjects++
			if !strings.Contains(path, "/mo-dml/update/") || strings.Contains(path, "stmt-update-batch") {
				t.Fatalf("unexpected replacement object path: %s", path)
			}
		case strings.Contains(path, "/delete/position/"):
			deleteObjects++
		}
	}
	if replacementObjects != 1 || deleteObjects != 1 {
		t.Fatalf("expected one replacement data object and one delete object, replacement=%d delete=%d paths=%#v", replacementObjects, deleteObjects, deleteWriter.objects)
	}
	if result.Profile["operation"] != "update" || result.Profile["added_data_files"] != "1" || result.Profile["added_delete_files"] != "1" {
		t.Fatalf("unexpected update profile: %#v", result.Profile)
	}
}

func TestDMLActionExecutorRecordsMaterializedObjectsWhenActionBuildFails(t *testing.T) {
	deleteWriter := &recordingDMLDeleteObjectWriter{}
	orphanRecorder := &recordingSQLOrphanRecorder{}
	executor := DMLActionExecutor{
		Workflow: dml.CommitWorkflow{
			ManifestWriter: &fakeSQLManifestWriter{},
			Committer:      &fakeDMLWorkflowCommitter{},
			OrphanRecorder: orphanRecorder,
		},
		Catalog: api.CatalogRequest{Catalog: model.Catalog{
			AccountID: 9,
			CatalogID: 8,
		}},
		TableLocation:  "s3://warehouse/gold/orders",
		SnapshotID:     36,
		SequenceNumber: 10,
	}
	bat, cleanup := newReplacementExecutorBatch(t)
	defer cleanup()

	_, err := executor.CommitUpdate(context.Background(), DMLUpdateActionStreamRequest{
		DMLDeleteActionStreamRequest: DMLDeleteActionStreamRequest{
			Schema: api.Schema{SchemaID: 9, Fields: []api.SchemaField{
				{ID: 1, Name: "id", Required: true, Type: api.IcebergType{Kind: api.TypeInt}},
				{ID: 2, Name: "name", Type: api.IcebergType{Kind: api.TypeString}},
			}},
			Base: dml.CommitBase{
				Namespace:      api.Namespace{"gold"},
				Table:          "orders",
				TargetRef:      "main",
				BaseSnapshotID: 35,
				IdempotencyKey: "idem-update-build-fail",
				StatementID:    "stmt-update-build-fail",
			},
			DeleteSchemaID: 9,
			ObjectWriter:   deleteWriter,
			Targets: []DMLMatchedDeleteTarget{{
				DataFile: api.DataFile{FilePath: "s3://warehouse/gold/orders/data/update-target-batch.parquet", SpecID: 1},
			}},
		},
		ReplacementBatch: DMLReplacementDataBatch{
			Batch:               bat,
			TargetFileSizeBytes: 1,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "cannot be materialized") {
		t.Fatalf("expected action build failure after replacement write, got %v", err)
	}
	if len(deleteWriter.objects) != 1 {
		t.Fatalf("expected replacement object write before build failure, got %#v", deleteWriter.objects)
	}
	if len(orphanRecorder.candidates) != 1 {
		t.Fatalf("expected materialized replacement to be recorded as orphan, got %#v", orphanRecorder.candidates)
	}
	if !strings.Contains(orphanRecorder.candidates[0].FilePath, "/replacement/") ||
		orphanRecorder.candidates[0].AccountID != 9 ||
		orphanRecorder.candidates[0].CatalogID != 8 {
		t.Fatalf("unexpected orphan candidate: %+v", orphanRecorder.candidates[0])
	}
}

func TestDMLMaterializedObjectTrackerWrapsDataFileOutputFactory(t *testing.T) {
	tracker := newDMLMaterializedObjectTracker()
	if wrapDMLDataFileOutputFactory(nil, tracker) != nil {
		t.Fatalf("nil output factory should stay nil")
	}
	outputFactory := &recordingDMLDataFileOutputFactory{}
	wrapped := wrapDMLDataFileOutputFactory(outputFactory, tracker)
	wc, err := wrapped.CreateDataFile(context.Background(), "s3://warehouse/gold/orders/data/replacement.parquet")
	if err != nil {
		t.Fatalf("create data file: %v", err)
	}
	if _, err := wc.Write([]byte("payload")); err != nil {
		t.Fatalf("write data file: %v", err)
	}
	if paths := tracker.paths(); len(paths) != 1 || paths[0] != "s3://warehouse/gold/orders/data/replacement.parquet" {
		t.Fatalf("created path should be tracked before write or close can fail, got %#v", paths)
	}
	if err := wc.Close(); err != nil {
		t.Fatalf("close data file: %v", err)
	}
	if paths := tracker.paths(); len(paths) != 1 || paths[0] != "s3://warehouse/gold/orders/data/replacement.parquet" {
		t.Fatalf("unexpected tracked paths: %#v", paths)
	}
	if got := outputFactory.writes["s3://warehouse/gold/orders/data/replacement.parquet"]; string(got) != "payload" {
		t.Fatalf("unexpected data file payload: %q", got)
	}

	_, err = (trackingDMLDataFileOutputFactory{}).CreateDataFile(context.Background(), "s3://warehouse/gold/orders/data/missing.parquet")
	if err == nil || !strings.Contains(err.Error(), "requires output factory") {
		t.Fatalf("expected missing output factory error, got %v", err)
	}
	err = (trackingDMLDeleteObjectWriter{}).WriteObject(context.Background(), "s3://warehouse/gold/orders/delete/pos.parquet", []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "requires object writer") {
		t.Fatalf("expected missing object writer error, got %v", err)
	}
}

func TestDMLMaterializedObjectTrackerRetainsAttemptedPathsOnWriteFailure(t *testing.T) {
	tracker := newDMLMaterializedObjectTracker()
	sentinel := moerr.NewInternalErrorNoCtx("remote write result unknown")
	writer := wrapDMLDeleteObjectWriter(testDeleteObjectWriterFunc(func(context.Context, string, []byte) error {
		return sentinel
	}), tracker)
	location := "s3://warehouse/gold/orders/delete/pos.parquet"
	if err := writer.WriteObject(context.Background(), location, []byte("payload")); !errors.Is(err, sentinel) {
		t.Fatalf("expected write failure, got %v", err)
	}
	if paths := tracker.paths(); len(paths) != 1 || paths[0] != location {
		t.Fatalf("attempted object path was not retained: %#v", paths)
	}

	factory := wrapDMLDataFileOutputFactory(testDataFileOutputFactoryFunc(func(context.Context, string) (io.WriteCloser, error) {
		return closeErrorWriteCloser{closeErr: sentinel}, nil
	}), tracker)
	dataLocation := "s3://warehouse/gold/orders/data/replacement.parquet"
	wc, err := factory.CreateDataFile(context.Background(), dataLocation)
	if err != nil {
		t.Fatalf("create tracked data file: %v", err)
	}
	if err := wc.Close(); !errors.Is(err, sentinel) {
		t.Fatalf("expected close failure, got %v", err)
	}
	if paths := tracker.paths(); len(paths) != 2 || paths[0] != location || paths[1] != dataLocation {
		t.Fatalf("failed-close data path was not retained: %#v", paths)
	}
}

type testDeleteObjectWriterFunc func(context.Context, string, []byte) error

func (f testDeleteObjectWriterFunc) WriteObject(ctx context.Context, location string, payload []byte) error {
	return f(ctx, location, payload)
}

type testDataFileOutputFactoryFunc func(context.Context, string) (io.WriteCloser, error)

func (f testDataFileOutputFactoryFunc) CreateDataFile(ctx context.Context, location string) (io.WriteCloser, error) {
	return f(ctx, location)
}

type closeErrorWriteCloser struct {
	closeErr error
}

func (closeErrorWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (w closeErrorWriteCloser) Close() error              { return w.closeErr }

func TestDMLActionExecutorCommitMergeCombinesMatchedAndUnmatchedActions(t *testing.T) {
	deleteWriter := &recordingDMLDeleteObjectWriter{}
	manifestWriter := &fakeSQLManifestWriter{}
	committer := &fakeDMLWorkflowCommitter{
		result: &api.CommitResult{
			SnapshotID:           38,
			CommitID:             "commit-merge-38",
			MetadataLocationHash: "metadata-merge-38",
			Verified:             true,
		},
	}
	executor := DMLActionExecutor{
		Workflow: dml.CommitWorkflow{
			ManifestWriter: manifestWriter,
			Committer:      committer,
		},
		TableLocation:  "s3://warehouse/gold/orders",
		SnapshotID:     38,
		SequenceNumber: 10,
	}

	executor = withSQLTestDMLExecutorMetadata(executor, 1)
	result, err := executor.CommitMerge(context.Background(), DMLMergeActionStreamRequest{
		Schema:         api.Schema{SchemaID: 9},
		DeleteSchemaID: 9,
		Base: dml.CommitBase{
			Namespace:      api.Namespace{"gold"},
			Table:          "orders",
			TargetRef:      "main",
			BaseSnapshotID: 37,
			IdempotencyKey: "idem-merge",
			StatementID:    "stmt-merge",
		},
		ObjectWriter: deleteWriter,
		MatchedDeletes: []DMLMatchedDeleteTarget{{
			DataFile:      api.DataFile{FilePath: "s3://warehouse/gold/orders/data/delete-target.parquet", SpecID: 1},
			HasRowOrdinal: true,
			PositionRows:  []dml.PositionDeleteRow{{Pos: 7}},
		}},
		MatchedUpdates: []DMLMatchedUpdateTarget{{
			DeleteTarget: DMLMatchedDeleteTarget{
				DataFile:      api.DataFile{FilePath: "s3://warehouse/gold/orders/data/update-target.parquet", SpecID: 1},
				HasRowOrdinal: true,
				PositionRows:  []dml.PositionDeleteRow{{Pos: 8}},
			},
			ReplacementFiles: []api.DataFile{{
				FilePath:        "s3://warehouse/gold/orders/data/merge-replacement.parquet",
				FileFormat:      "parquet",
				RecordCount:     1,
				FileSizeInBytes: 64,
				SpecID:          1,
			}},
		}},
		UnmatchedAppends: []api.DataFile{{
			FilePath:        "s3://warehouse/gold/orders/data/merge-insert.parquet",
			FileFormat:      "parquet",
			RecordCount:     1,
			FileSizeInBytes: 64,
			SpecID:          1,
		}},
	})
	if err != nil {
		t.Fatalf("commit DML merge: %v", err)
	}
	if result.Request.DataManifestPath == "" || result.Request.DeleteManifestPath == "" || result.Request.ManifestListPath == "" {
		t.Fatalf("merge should write data/delete manifests and manifest list: %+v", result.Request)
	}
	if len(deleteWriter.objects) != 2 {
		t.Fatalf("expected two delete object writes, got %d", len(deleteWriter.objects))
	}
	for path := range deleteWriter.objects {
		if !strings.Contains(path, "/mo-dml/merge/") {
			t.Fatalf("merge delete object must use merge-scoped path: %s", path)
		}
	}
	if result.Profile["operation"] != "merge" || result.Profile["added_data_files"] != "2" || result.Profile["added_delete_files"] != "2" {
		t.Fatalf("unexpected merge profile: %#v", result.Profile)
	}
}

func newReplacementExecutorBatch(t *testing.T) (*batch.Batch, func()) {
	t.Helper()
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"id", "name"})
	idVec := vector.NewVec(types.T_int32.ToType())
	nameVec := vector.NewVec(types.T_varchar.ToType())
	if err := vector.AppendFixed[int32](idVec, 42, false, mp); err != nil {
		t.Fatalf("append replacement id: %v", err)
	}
	if err := vector.AppendBytes(nameVec, []byte("replacement"), false, mp); err != nil {
		t.Fatalf("append replacement name: %v", err)
	}
	bat.Vecs[0] = idVec
	bat.Vecs[1] = nameVec
	bat.SetRowCount(1)
	return bat, func() { bat.Clean(mp) }
}

type recordingDMLDataFileOutputFactory struct {
	writes map[string][]byte
}

func (f *recordingDMLDataFileOutputFactory) CreateDataFile(ctx context.Context, location string) (io.WriteCloser, error) {
	if f.writes == nil {
		f.writes = make(map[string][]byte)
	}
	return &recordingDMLDataFile{location: location, onClose: func(location string, payload []byte) {
		f.writes[location] = append([]byte(nil), payload...)
	}}, nil
}

type recordingDMLDataFile struct {
	bytes.Buffer
	location string
	onClose  func(string, []byte)
}

func (w *recordingDMLDataFile) Close() error {
	if w.onClose != nil {
		w.onClose(w.location, w.Bytes())
	}
	return nil
}

func TestDMLActionExecutorCommitOverwriteBuildsDataManifestCommit(t *testing.T) {
	manifestWriter := &fakeSQLManifestWriter{}
	committer := &fakeDMLWorkflowCommitter{
		result: &api.CommitResult{
			SnapshotID:           41,
			CommitID:             "commit-overwrite-41",
			MetadataLocationHash: "metadata-overwrite-41",
			Verified:             true,
		},
	}
	oldFile := api.DataFile{
		FilePath:        "s3://warehouse/gold/orders/data/old.parquet",
		FileFormat:      "parquet",
		RecordCount:     3,
		FileSizeInBytes: 90,
		SpecID:          1,
	}
	baseManifest := api.ManifestFile{
		Path:            "s3://warehouse/gold/orders/metadata/base-manifest.avro",
		Content:         api.ManifestContentData,
		AddedSnapshotID: 40,
		AddedFilesCount: 1,
	}
	executor := DMLActionExecutor{
		Workflow: dml.CommitWorkflow{
			ManifestWriter: manifestWriter,
			Committer:      committer,
		},
		TableLocation:      "s3://warehouse/gold/orders",
		SnapshotID:         41,
		SequenceNumber:     8,
		PreservedManifests: []api.ManifestFile{baseManifest},
		PreservedSources: []dml.PreservedManifestSource{{
			Manifest: baseManifest,
			Entries: []api.ManifestEntry{{
				Status:     api.ManifestEntryExisting,
				SnapshotID: 40,
				DataFile:   oldFile,
			}},
		}},
	}

	executor = withSQLTestDMLExecutorMetadata(executor, 1)
	result, err := executor.CommitOverwrite(context.Background(), DMLOverwriteActionStreamRequest{
		Base: dml.CommitBase{
			Namespace:      api.Namespace{"gold"},
			Table:          "orders",
			TargetRef:      "main",
			BaseSnapshotID: 40,
			IdempotencyKey: "idem-overwrite",
			StatementID:    "stmt-overwrite",
		},
		Scope:             dml.OverwriteTable,
		AffectedDataFiles: []api.DataFile{oldFile},
		ReplacementFiles: []api.DataFile{{
			FilePath:        "s3://warehouse/gold/orders/data/new.parquet",
			FileFormat:      "parquet",
			RecordCount:     3,
			FileSizeInBytes: 96,
			SpecID:          1,
		}},
	})
	if err != nil {
		t.Fatalf("commit DML overwrite: %v", err)
	}
	if result.Request.DataManifestPath == "" || result.Request.DeleteManifestPath != "" || result.Request.ManifestListPath == "" {
		t.Fatalf("overwrite should write data manifest and manifest list only: %+v", result.Request)
	}
	if len(manifestWriter.paths) != 2 || manifestWriter.paths[0] != result.Request.DataManifestPath || manifestWriter.paths[1] != result.Request.ManifestListPath {
		t.Fatalf("unexpected manifest writes: %#v request=%+v", manifestWriter.paths, result.Request)
	}
	if len(committer.requests) != 1 || len(committer.requests[0].Requirements) == 0 {
		t.Fatalf("expected one commit request with requirements: %+v", committer.requests)
	}
	if got := committer.requests[0].Requirements[0]; got.Type != "assert-ref-snapshot-id" || got.SnapshotID != 40 {
		t.Fatalf("unexpected snapshot requirement: %+v", got)
	}
	if result.Profile["operation"] != "overwrite" || result.Profile["deleted_data_files"] != "1" || result.Profile["added_data_files"] != "1" {
		t.Fatalf("unexpected profile: %#v", result.Profile)
	}
}

func TestDMLActionExecutorRejectsTagOverwriteBeforeObjectWrite(t *testing.T) {
	bat, cleanup := newReplacementExecutorBatch(t)
	defer cleanup()

	deleteWriter := &recordingDMLDeleteObjectWriter{}
	manifestWriter := &fakeSQLManifestWriter{}
	committer := &fakeDMLWorkflowCommitter{}
	executor := DMLActionExecutor{
		Workflow: dml.CommitWorkflow{
			ManifestWriter: manifestWriter,
			Committer:      committer,
		},
		TableLocation:  "s3://warehouse/gold/orders",
		SnapshotID:     41,
		SequenceNumber: 8,
	}

	_, err := executor.CommitOverwrite(context.Background(), DMLOverwriteActionStreamRequest{
		Schema: api.Schema{SchemaID: 9},
		Base: dml.CommitBase{
			Namespace:      api.Namespace{"gold"},
			Table:          "orders",
			TargetRef:      "tag:release",
			BaseSnapshotID: 40,
			IdempotencyKey: "idem-overwrite-tag",
			StatementID:    "stmt-overwrite-tag",
		},
		ObjectWriter: deleteWriter,
		AffectedDataFiles: []api.DataFile{{
			FilePath:        "s3://warehouse/gold/orders/data/old.parquet",
			FileFormat:      "parquet",
			RecordCount:     3,
			FileSizeInBytes: 90,
			SpecID:          1,
		}},
		ReplacementBatches: []DMLReplacementDataBatch{{
			Attrs:        []string{"id", "name"},
			Batch:        bat,
			ObjectWriter: deleteWriter,
		}},
	})
	if err == nil || !strings.Contains(err.Error(), string(api.ErrUnsupportedFeature)) {
		t.Fatalf("expected tag write rejection, got %v", err)
	}
	if len(deleteWriter.objects) != 0 {
		t.Fatalf("tag write gate must run before overwrite object writes: %#v", deleteWriter.objects)
	}
	if len(manifestWriter.paths) != 0 || len(committer.requests) != 0 {
		t.Fatalf("tag write gate must run before manifest/catalog commit writes: paths=%#v requests=%#v", manifestWriter.paths, committer.requests)
	}
}
