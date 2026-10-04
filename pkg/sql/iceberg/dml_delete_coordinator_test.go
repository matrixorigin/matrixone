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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/icebergwrite"
)

func TestDMLDeleteCoordinatorCollectsScanBatchesAndCommits(t *testing.T) {
	bat, cleanup := newMatchedScanBatch(t)
	defer cleanup()
	committer := &recordingDMLDeleteCommitter{}
	coord := NewDMLDeleteCoordinator(DMLDeleteCoordinatorSpec{
		Committer: committer,
		Base: dml.CommitBase{
			BaseSnapshotID: 30,
			IdempotencyKey: "stmt-1",
			StatementID:    "stmt-1",
		},
		Schema:              api.Schema{SchemaID: 9},
		DeleteSchemaID:      9,
		DataFiles:           dmlDeleteCoordinatorDataFiles(),
		IncludePositionRows: true,
	})
	req := icebergwrite.AppendRequest{
		Operation:               icebergwrite.OperationDelete,
		Namespace:               "sales",
		Table:                   "orders",
		DefaultRef:              "main",
		DataFilePathColumnIndex: 2,
		RowOrdinalColumnIndex:   3,
	}
	if err := coord.Begin(context.Background(), req); err != nil {
		t.Fatalf("begin coordinator: %v", err)
	}
	if err := coord.Append(context.Background(), bat); err != nil {
		t.Fatalf("append batch: %v", err)
	}
	if err := coord.Commit(context.Background()); err != nil {
		t.Fatalf("commit coordinator: %v", err)
	}
	if len(committer.requests) != 1 {
		t.Fatalf("expected one commit request, got %d", len(committer.requests))
	}
	commitReq := committer.requests[0]
	if commitReq.Base.Table != "orders" || strings.Join(commitReq.Base.Namespace, ".") != "sales" || commitReq.Base.TargetRef != "main" {
		t.Fatalf("unexpected commit base: %+v", commitReq.Base)
	}
	if len(commitReq.Targets) != 2 {
		t.Fatalf("expected two matched targets, got %+v", commitReq.Targets)
	}
	if commitReq.Targets[0].DataFile.SpecID != 3 || len(commitReq.Targets[0].PositionRows) != 2 || commitReq.Targets[0].PositionRows[1].Pos != 11 {
		t.Fatalf("unexpected first target: %+v", commitReq.Targets[0])
	}
	if commitReq.Targets[1].DataFile.FilePath != "s3://warehouse/gold/orders/data/b.parquet" || len(commitReq.Targets[1].PositionRows) != 1 {
		t.Fatalf("unexpected second target: %+v", commitReq.Targets[1])
	}
}

func TestDMLDeleteCoordinatorSkipsCommitWhenNoRowsMatched(t *testing.T) {
	committer := &recordingDMLDeleteCommitter{}
	coord := NewDMLDeleteCoordinator(DMLDeleteCoordinatorSpec{
		Committer:           committer,
		DataFiles:           dmlDeleteCoordinatorDataFiles(),
		IncludePositionRows: true,
	})
	if err := coord.Begin(context.Background(), icebergwrite.AppendRequest{
		Operation:               icebergwrite.OperationDelete,
		DataFilePathColumnIndex: 2,
		RowOrdinalColumnIndex:   3,
	}); err != nil {
		t.Fatalf("begin coordinator: %v", err)
	}
	if err := coord.Commit(context.Background()); err != nil {
		t.Fatalf("commit coordinator: %v", err)
	}
	if len(committer.requests) != 0 {
		t.Fatalf("empty DELETE should not create a commit request: %+v", committer.requests)
	}
}

func TestDMLDeleteCoordinatorFactoryRejectsAppendRequests(t *testing.T) {
	_, err := (DMLDeleteCoordinatorFactory{}).NewCoordinator(context.Background(), icebergwrite.AppendRequest{Operation: icebergwrite.OperationAppend})
	if err == nil || !strings.Contains(err.Error(), "only accepts DELETE") {
		t.Fatalf("expected append request rejection, got %v", err)
	}
}

func TestDMLDeleteCoordinatorFactoryUsesRequestScanMetadata(t *testing.T) {
	coord, err := (DMLDeleteCoordinatorFactory{Spec: DMLDeleteCoordinatorSpec{
		Committer:           &recordingDMLDeleteCommitter{},
		ObjectWriter:        &recordingDMLDeleteObjectWriter{},
		IncludePositionRows: true,
	}}).NewCoordinator(context.Background(), icebergwrite.AppendRequest{
		Operation:  icebergwrite.OperationDelete,
		DefaultRef: "main",
		DMLScan: icebergwrite.DMLScanMetadata{
			BaseSnapshotID: 30,
			BaseSchemaID:   9,
			Ref:            "audit",
			DataFiles: []api.DataFile{
				{FilePath: "s3://warehouse/gold/orders/data/a.parquet", SpecID: 3},
			},
		},
	})
	if err != nil {
		t.Fatalf("create coordinator: %v", err)
	}
	dmlCoord, ok := coord.(*DMLDeleteCoordinator)
	if !ok {
		t.Fatalf("expected DML delete coordinator, got %T", coord)
	}
	if dmlCoord.spec.Base.BaseSnapshotID != 30 || dmlCoord.spec.Base.BaseSchemaID != 9 || dmlCoord.spec.Base.TargetRef != "audit" {
		t.Fatalf("expected request scan metadata to populate base, got %+v", dmlCoord.spec.Base)
	}
	if dmlCoord.spec.Schema.SchemaID != 9 || dmlCoord.spec.DeleteSchemaID != 9 {
		t.Fatalf("expected request scan metadata to populate schema ids, got schema=%+v delete_schema=%d", dmlCoord.spec.Schema, dmlCoord.spec.DeleteSchemaID)
	}
	if len(dmlCoord.spec.DataFiles) != 1 || dmlCoord.spec.DataFiles[0].SpecID != 3 {
		t.Fatalf("expected request scan data files, got %+v", dmlCoord.spec.DataFiles)
	}
}

func TestBuildDMLDeleteCoordinatorSpecFromScanPlanInheritsBaseAndFiles(t *testing.T) {
	committer := &recordingDMLDeleteCommitter{}
	writer := &recordingDMLDeleteObjectWriter{}
	plan := &api.IcebergScanPlan{
		Snapshot: api.SnapshotPlan{SnapshotID: 30, SchemaID: 9, RefName: "audit"},
		DataTasks: []api.DataFileTask{
			{DataFile: api.DataFile{FilePath: "s3://warehouse/gold/orders/data/a.parquet", SpecID: 3}},
			{DataFile: api.DataFile{FilePath: "s3://warehouse/gold/orders/data/a.parquet", SpecID: 3}},
			{DataFile: api.DataFile{FilePath: "s3://warehouse/gold/orders/data/b.parquet", SpecID: 4}},
		},
	}
	spec, err := BuildDMLDeleteCoordinatorSpecFromScanPlan(context.Background(), DMLDeleteCoordinatorFromScanPlanRequest{
		Committer:             committer,
		Base:                  dml.CommitBase{Table: "orders", IdempotencyKey: "stmt-1"},
		Schema:                api.Schema{SchemaID: 9, Fields: []api.SchemaField{{ID: 1, Name: "order_id", Type: api.IcebergType{Kind: api.TypeLong}}}},
		ObjectWriter:          writer,
		ScanPlan:              plan,
		EqualityFieldIDs:      []int{1},
		EqualityColumnIndexes: []int32{0},
		PredicateStable:       true,
		IncludePositionRows:   true,
	})
	if err != nil {
		t.Fatalf("build coordinator spec: %v", err)
	}
	if spec.Committer != committer || spec.ObjectWriter != writer {
		t.Fatalf("expected helper to preserve production collaborators")
	}
	if spec.Base.BaseSnapshotID != 30 || spec.Base.BaseSchemaID != 9 || spec.Base.TargetRef != "audit" {
		t.Fatalf("unexpected inherited base: %+v", spec.Base)
	}
	if spec.DeleteSchemaID != 9 || !spec.PredicateStable || !spec.IncludePositionRows {
		t.Fatalf("unexpected delete settings: %+v", spec)
	}
	if len(spec.DataFiles) != 2 || spec.DataFiles[0].FilePath != "s3://warehouse/gold/orders/data/a.parquet" || spec.DataFiles[1].SpecID != 4 {
		t.Fatalf("expected deduplicated scan data files, got %+v", spec.DataFiles)
	}
	if len(spec.EqualityFieldIDs) != 1 || spec.EqualityFieldIDs[0] != 1 || len(spec.EqualityColumnIndexes) != 1 || spec.EqualityColumnIndexes[0] != 0 {
		t.Fatalf("unexpected equality metadata: field=%v columns=%v", spec.EqualityFieldIDs, spec.EqualityColumnIndexes)
	}
}

func TestBuildDMLDeleteCoordinatorSpecFromScanPlanFailsFast(t *testing.T) {
	_, err := BuildDMLDeleteCoordinatorSpecFromScanPlan(context.Background(), DMLDeleteCoordinatorFromScanPlanRequest{})
	if err == nil || !strings.Contains(err.Error(), "requires a scan plan") {
		t.Fatalf("expected missing scan plan error, got %v", err)
	}
	_, err = BuildDMLDeleteCoordinatorSpecFromScanPlan(context.Background(), DMLDeleteCoordinatorFromScanPlanRequest{
		Committer:    &recordingDMLDeleteCommitter{},
		Schema:       api.Schema{SchemaID: -1},
		ObjectWriter: &recordingDMLDeleteObjectWriter{},
		ScanPlan:     &api.IcebergScanPlan{Snapshot: api.SnapshotPlan{SnapshotID: 30}},
	})
	if err == nil || !strings.Contains(err.Error(), "requires a schema id") {
		t.Fatalf("expected missing schema error, got %v", err)
	}
	_, err = BuildDMLDeleteCoordinatorSpecFromScanPlan(context.Background(), DMLDeleteCoordinatorFromScanPlanRequest{
		Committer:    &recordingDMLDeleteCommitter{},
		Schema:       api.Schema{SchemaID: 9},
		ObjectWriter: &recordingDMLDeleteObjectWriter{},
		ScanPlan:     &api.IcebergScanPlan{Snapshot: api.SnapshotPlan{SnapshotID: 0}},
	})
	if err == nil || !strings.Contains(err.Error(), "requires a resolved base snapshot") {
		t.Fatalf("expected missing base snapshot error, got %v", err)
	}
}

func TestNewDMLDeleteCoordinatorFactoryFromScanPlanCreatesDeleteCoordinator(t *testing.T) {
	factory, err := NewDMLDeleteCoordinatorFactoryFromScanPlan(context.Background(), DMLDeleteCoordinatorFromScanPlanRequest{
		Committer:    &recordingDMLDeleteCommitter{},
		Schema:       api.Schema{SchemaID: 9},
		ObjectWriter: &recordingDMLDeleteObjectWriter{},
		ScanPlan: &api.IcebergScanPlan{
			Snapshot:  api.SnapshotPlan{SnapshotID: 30, SchemaID: 9},
			DataTasks: []api.DataFileTask{{DataFile: api.DataFile{FilePath: "s3://warehouse/gold/orders/data/a.parquet"}}},
		},
		IncludePositionRows: true,
	})
	if err != nil {
		t.Fatalf("build factory: %v", err)
	}
	coord, err := factory.NewCoordinator(context.Background(), icebergwrite.AppendRequest{
		Operation:               icebergwrite.OperationDelete,
		DataFilePathColumnIndex: 0,
		RowOrdinalColumnIndex:   1,
	})
	if err != nil {
		t.Fatalf("create coordinator: %v", err)
	}
	if _, ok := coord.(*DMLDeleteCoordinator); !ok {
		t.Fatalf("expected DML delete coordinator, got %T", coord)
	}
}

func dmlDeleteCoordinatorDataFiles() []api.DataFile {
	return []api.DataFile{
		{FilePath: "s3://warehouse/gold/orders/data/a.parquet", Partition: map[string]any{"region": "ksa"}, SpecID: 3},
		{FilePath: "s3://warehouse/gold/orders/data/b.parquet", Partition: map[string]any{"region": "ksa"}, SpecID: 3},
	}
}

type recordingDMLDeleteCommitter struct {
	requests []DMLDeleteActionStreamRequest
	err      error
}

func (c *recordingDMLDeleteCommitter) CommitDelete(ctx context.Context, req DMLDeleteActionStreamRequest) (DMLCommitActionStreamResult, error) {
	c.requests = append(c.requests, req)
	return DMLCommitActionStreamResult{}, c.err
}
