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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
	"github.com/matrixorigin/matrixone/pkg/iceberg/testutil"
)

func TestBuildDMLManifestPathsUsesStatementScopedHash(t *testing.T) {
	stream := dml.ActionStream{
		Operation: dml.OperationUpdate,
		Base: dml.CommitBase{
			Table:          "orders",
			StatementID:    "stmt-with-sensitive-text",
			IdempotencyKey: "idem-fallback",
		},
		Actions: []dml.Action{
			{Kind: dml.ActionAppendData},
			{Kind: dml.ActionAddEqualityDelete},
		},
	}
	paths, err := BuildDMLManifestPaths(context.Background(), DMLManifestPathRequest{
		TableLocation: "s3://warehouse/gold/orders/",
		Stream:        stream,
		SnapshotID:    202,
	})
	if err != nil {
		t.Fatalf("build DML manifest paths: %v", err)
	}
	wantPrefix := "s3://warehouse/gold/orders/metadata/mo-dml/update/stmt-" + api.PathHash("stmt-with-sensitive-text")
	if paths.DataManifestPath != wantPrefix+"/data-manifest.avro" {
		t.Fatalf("unexpected data manifest path: %s", paths.DataManifestPath)
	}
	if paths.DeleteManifestPath != wantPrefix+"/delete-manifest.avro" {
		t.Fatalf("unexpected delete manifest path: %s", paths.DeleteManifestPath)
	}
	if paths.ManifestListPath != wantPrefix+"/manifest-list-snap-202.avro" {
		t.Fatalf("unexpected manifest list path: %s", paths.ManifestListPath)
	}
	for _, path := range []string{paths.DataManifestPath, paths.DeleteManifestPath, paths.ManifestListPath} {
		if strings.Contains(path, "stmt-with-sensitive-text") || strings.Contains(path, "idem-fallback") {
			t.Fatalf("DML manifest path leaked raw statement/idempotency: %s", path)
		}
	}
}

func TestBuildDMLManifestPathsOnlyAllocatesNeededManifestKinds(t *testing.T) {
	paths, err := BuildDMLManifestPaths(context.Background(), DMLManifestPathRequest{
		TableLocation: "s3://warehouse/gold/orders",
		Stream: dml.ActionStream{
			Operation: dml.OperationDelete,
			Base: dml.CommitBase{
				Table:          "orders",
				IdempotencyKey: "idem-delete",
			},
			Actions: []dml.Action{{Kind: dml.ActionAddPositionDelete}},
		},
		SnapshotID: 203,
	})
	if err != nil {
		t.Fatalf("build delete DML manifest paths: %v", err)
	}
	if paths.DataManifestPath != "" {
		t.Fatalf("delete-only action should not allocate data manifest path: %+v", paths)
	}
	if paths.DeleteManifestPath == "" || paths.ManifestListPath == "" {
		t.Fatalf("delete-only action must allocate delete manifest and manifest list paths: %+v", paths)
	}
}

func TestBuildDMLManifestPathsRejectsUnsupportedOperation(t *testing.T) {
	_, err := BuildDMLManifestPaths(context.Background(), DMLManifestPathRequest{
		TableLocation: "s3://warehouse/gold/orders",
		Stream: dml.ActionStream{
			Operation: dml.Operation("../escape"),
			Base: dml.CommitBase{
				Table:          "orders",
				IdempotencyKey: "idem",
			},
			Actions: []dml.Action{{Kind: dml.ActionAppendData}},
		},
		SnapshotID: 204,
	})
	if err == nil {
		t.Fatal("expected unsupported DML operation to be rejected")
	}
	if strings.Contains(err.Error(), "../escape/stmt-") {
		t.Fatalf("unsupported operation leaked into object path: %v", err)
	}
}

func TestBuildDMLDeleteFilePathUsesStatementAndDataFileHashes(t *testing.T) {
	stream := dml.ActionStream{
		Operation: dml.OperationDelete,
		Base: dml.CommitBase{
			Table:          "orders",
			StatementID:    "delete from orders where email = 'secret@example.com'",
			IdempotencyKey: "idem-fallback",
		},
	}
	targetPath := "s3://warehouse/gold/orders/data/customer_email=secret@example.com/part-1.parquet"
	path, err := BuildDMLDeleteFilePath(context.Background(), DMLDeleteFilePathRequest{
		TableLocation:      "s3://warehouse/gold/orders/",
		Stream:             stream,
		SnapshotID:         206,
		DeleteKind:         dml.ActionAddEqualityDelete,
		TargetDataFilePath: targetPath,
		FileSequence:       3,
	})
	if err != nil {
		t.Fatalf("build DML delete file path: %v", err)
	}
	wantPrefix := "s3://warehouse/gold/orders/data/mo-dml/delete/stmt-" + api.PathHash(stream.Base.StatementID)
	wantSuffix := "/delete/equality/delete-snap-206-seq-3-df-" + api.PathHash(targetPath) + ".parquet"
	if path != wantPrefix+wantSuffix {
		t.Fatalf("unexpected delete file path: %s", path)
	}
	if strings.Contains(path, "secret@example.com") || strings.Contains(path, "idem-fallback") || strings.Contains(path, "part-1.parquet") {
		t.Fatalf("DML delete file path leaked raw statement/idempotency/data path: %s", path)
	}
}

func TestBuildDMLDeleteFilePathRejectsNonDeleteKind(t *testing.T) {
	_, err := BuildDMLDeleteFilePath(context.Background(), DMLDeleteFilePathRequest{
		TableLocation:      "s3://warehouse/gold/orders",
		Stream:             dml.ActionStream{Operation: dml.OperationDelete, Base: dml.CommitBase{Table: "orders", IdempotencyKey: "idem"}},
		SnapshotID:         207,
		DeleteKind:         dml.ActionAppendData,
		TargetDataFilePath: "s3://warehouse/gold/orders/data/a.parquet",
		FileSequence:       1,
	})
	if err == nil {
		t.Fatal("expected non-delete action kind to be rejected")
	}
	if strings.Contains(err.Error(), "data/mo-dml") {
		t.Fatalf("rejected delete kind should not leak into object path: %v", err)
	}
}

func TestBuildDMLDeleteFilePathRejectsMissingTargetDataFile(t *testing.T) {
	_, err := BuildDMLDeleteFilePath(context.Background(), DMLDeleteFilePathRequest{
		TableLocation: "s3://warehouse/gold/orders",
		Stream:        dml.ActionStream{Operation: dml.OperationDelete, Base: dml.CommitBase{Table: "orders", IdempotencyKey: "idem"}},
		SnapshotID:    208,
		DeleteKind:    dml.ActionAddPositionDelete,
		FileSequence:  1,
	})
	if err == nil {
		t.Fatal("expected missing target data file to be rejected")
	}
}

func TestBuildDMLCommitWorkflowRequestAssemblesManifestPaths(t *testing.T) {
	stream := dml.ActionStream{
		Operation: dml.OperationMerge,
		Base: dml.CommitBase{
			Namespace:      api.Namespace{"sales"},
			Table:          "orders",
			StatementID:    "stmt-merge",
			IdempotencyKey: "idem-merge",
		},
		Actions: []dml.Action{
			{Kind: dml.ActionAppendData},
			{Kind: dml.ActionAddPositionDelete},
		},
	}
	req, err := BuildDMLCommitWorkflowRequest(context.Background(), DMLCommitWorkflowRequestSpec{
		Stream:         stream,
		TableLocation:  "s3://warehouse/gold/orders/",
		SnapshotID:     205,
		SequenceNumber: 33,
	})
	if err != nil {
		t.Fatalf("build DML commit workflow request: %v", err)
	}
	wantPrefix := "s3://warehouse/gold/orders/metadata/mo-dml/merge/stmt-" + api.PathHash("stmt-merge")
	if req.TableLocation != "s3://warehouse/gold/orders" {
		t.Fatalf("unexpected table location: %s", req.TableLocation)
	}
	if req.SnapshotID != 205 || req.SequenceNumber != 33 {
		t.Fatalf("unexpected snapshot/sequence: %+v", req)
	}
	if req.DataManifestPath != wantPrefix+"/data-manifest.avro" {
		t.Fatalf("unexpected data manifest path: %s", req.DataManifestPath)
	}
	if req.DeleteManifestPath != wantPrefix+"/delete-manifest.avro" {
		t.Fatalf("unexpected delete manifest path: %s", req.DeleteManifestPath)
	}
	if req.ManifestListPath != wantPrefix+"/manifest-list-snap-205.avro" {
		t.Fatalf("unexpected manifest list path: %s", req.ManifestListPath)
	}
}

func TestBuildDMLCommitWorkflowRequestRejectsInvalidRequiredFields(t *testing.T) {
	stream := dml.ActionStream{
		Operation: dml.OperationMerge,
		Base: dml.CommitBase{
			Namespace:      api.Namespace{"sales"},
			Table:          "orders",
			StatementID:    "stmt-merge",
			IdempotencyKey: "idem-merge",
		},
		Actions: []dml.Action{{Kind: dml.ActionAppendData}},
	}

	for _, tc := range []struct {
		name           string
		spec           DMLCommitWorkflowRequestSpec
		wantErrMessage string
	}{
		{
			name: "missing table location",
			spec: DMLCommitWorkflowRequestSpec{
				Stream:         stream,
				TableLocation:  "   ",
				SnapshotID:     205,
				SequenceNumber: 33,
			},
			wantErrMessage: "requires table location",
		},
		{
			name: "missing snapshot",
			spec: DMLCommitWorkflowRequestSpec{
				Stream:         stream,
				TableLocation:  "s3://warehouse/gold/orders",
				SequenceNumber: 33,
			},
			wantErrMessage: "requires positive snapshot and sequence numbers",
		},
		{
			name: "missing sequence",
			spec: DMLCommitWorkflowRequestSpec{
				Stream:        stream,
				TableLocation: "s3://warehouse/gold/orders",
				SnapshotID:    205,
			},
			wantErrMessage: "requires positive snapshot and sequence numbers",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := BuildDMLCommitWorkflowRequest(context.Background(), tc.spec)
			if err == nil || !strings.Contains(err.Error(), tc.wantErrMessage) {
				t.Fatalf("expected %q error, got %v", tc.wantErrMessage, err)
			}
		})
	}
}

func TestCommitDMLActionStreamBuildsRequestAndProfile(t *testing.T) {
	writer := &fakeSQLManifestWriter{}
	committer := &fakeDMLWorkflowCommitter{
		result: &api.CommitResult{
			SnapshotID:           909,
			CommitID:             "commit-909",
			MetadataLocationHash: "metadata-hash-909",
			Verified:             true,
		},
	}
	stream := dml.ActionStream{
		Operation: dml.OperationOverwrite,
		Base: dml.CommitBase{
			Namespace:      api.Namespace{"gold"},
			Table:          "orders",
			TargetRef:      "main",
			BaseSnapshotID: 808,
			IdempotencyKey: "idem-overwrite",
			StatementID:    "stmt-overwrite",
		},
		Actions: []dml.Action{{
			Kind: dml.ActionAppendData,
			File: api.DataFile{
				FilePath:        "s3://warehouse/gold/orders/data/replacement.parquet",
				FileFormat:      "parquet",
				RecordCount:     4,
				FileSizeInBytes: 64,
			},
		}},
		Profile: dml.Profile{
			Operation:      dml.OperationOverwrite,
			MatchedRows:    4,
			AddedDataFiles: 1,
		},
	}

	out, err := CommitDMLActionStream(context.Background(), DMLCommitActionStreamSpec{
		Workflow: dml.CommitWorkflow{
			ManifestWriter: writer,
			Committer:      committer,
		},
		Stream:         stream,
		TableLocation:  "s3://warehouse/gold/orders/",
		FormatVersion:  2,
		Schema:         api.Schema{SchemaID: 9, Fields: []api.SchemaField{{ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeLong}}}},
		PartitionSpecs: []api.PartitionSpec{{SpecID: 0}},
		SnapshotID:     909,
		SequenceNumber: 44,
	})
	if err != nil {
		t.Fatalf("commit DML action stream: %v", err)
	}
	if out.CommitResult == nil || out.CommitResult.SnapshotID != 909 || out.CommitResult.CommitID != "commit-909" {
		t.Fatalf("unexpected commit result: %+v", out.CommitResult)
	}
	if out.Request.TableLocation != "s3://warehouse/gold/orders" || out.Request.DataManifestPath == "" || out.Request.ManifestListPath == "" {
		t.Fatalf("unexpected workflow request: %+v", out.Request)
	}
	if len(writer.paths) != 2 || writer.paths[0] != out.Request.DataManifestPath || writer.paths[1] != out.Request.ManifestListPath {
		t.Fatalf("unexpected manifest writes: %#v, request=%+v", writer.paths, out.Request)
	}
	if len(committer.requests) != 1 || committer.requests[0].IdempotencyKey != "idem-overwrite" || committer.requests[0].TargetRef != "main" {
		t.Fatalf("unexpected commit request: %+v", committer.requests)
	}
	if out.Profile["matched_rows"] != "4" || out.Profile["added_data_files"] != "1" || out.Profile["snapshot_id"] != "909" || out.Profile["commit_id"] != "commit-909" {
		t.Fatalf("unexpected commit profile: %#v", out.Profile)
	}
	for _, key := range []string{"data_manifest", "manifest_list"} {
		if out.Profile[key] == "" || strings.Contains(out.Profile[key], "s3://warehouse") {
			t.Fatalf("profile %s should contain a redacted path, got %q", key, out.Profile[key])
		}
	}
	testutil.AssertNoIcebergSensitiveLeak(t, "DML commit profile", profileText(out.Profile),
		"s3://warehouse/gold/orders",
		"replacement.parquet",
		"stmt-overwrite",
	)
}

func TestCommitDMLActionStreamPropagatesBuildIntentError(t *testing.T) {
	_, err := CommitDMLActionStream(context.Background(), DMLCommitActionStreamSpec{
		Workflow: dml.CommitWorkflow{
			ManifestWriter: &fakeSQLManifestWriter{},
			Committer:      &fakeDMLWorkflowCommitter{},
		},
		Stream: dml.ActionStream{
			Operation: dml.OperationDelete,
			Base: dml.CommitBase{
				Namespace:      api.Namespace{"gold"},
				Table:          "orders",
				StatementID:    "stmt-empty-delete",
				IdempotencyKey: "idem-empty-delete",
			},
		},
		TableLocation:  "s3://warehouse/gold/orders",
		SnapshotID:     910,
		SequenceNumber: 45,
	})
	if err == nil || !strings.Contains(err.Error(), "action stream is empty") {
		t.Fatalf("expected empty action stream error, got %v", err)
	}
}

func TestCommitDMLActionStreamPropagatesWorkflowError(t *testing.T) {
	_, err := CommitDMLActionStream(context.Background(), DMLCommitActionStreamSpec{
		Workflow: dml.CommitWorkflow{
			Committer: &fakeDMLWorkflowCommitter{},
		},
		Stream: dml.ActionStream{
			Operation: dml.OperationDelete,
			Base: dml.CommitBase{
				Namespace:      api.Namespace{"gold"},
				Table:          "orders",
				StatementID:    "stmt-delete",
				IdempotencyKey: "idem-delete",
			},
			Actions: []dml.Action{{Kind: dml.ActionAddPositionDelete}},
		},
		TableLocation:  "s3://warehouse/gold/orders",
		SnapshotID:     911,
		SequenceNumber: 46,
	})
	if err == nil || !strings.Contains(err.Error(), "requires a manifest writer") {
		t.Fatalf("expected workflow manifest writer error, got %v", err)
	}
}

func profileText(profile map[string]string) string {
	var b strings.Builder
	for key, value := range profile {
		b.WriteString(key)
		b.WriteByte('=')
		b.WriteString(value)
		b.WriteByte('\n')
	}
	return b.String()
}
