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

package metadata

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func TestNativeFacadeImplementsMetadataFacade(t *testing.T) {
	facade := NativeFacade{}
	if facade.AdapterName() != AdapterNativeMetadata {
		t.Fatalf("unexpected adapter name %q", facade.AdapterName())
	}
	meta, err := facade.ParseTableMetadata(context.Background(), []byte(sampleMetadataJSON), "s3://warehouse/sales/orders/metadata/v2.metadata.json")
	if err != nil {
		t.Fatalf("parse through facade: %v", err)
	}
	snapshot, err := facade.ResolveSnapshot(context.Background(), meta, api.SnapshotSelector{RefName: "main"})
	if err != nil || snapshot.SnapshotID != 22 {
		t.Fatalf("resolve through facade snapshot=%+v err=%v", snapshot, err)
	}
	features, err := facade.DetectUnsupportedP0(context.Background(), meta, nil, nil)
	if err != nil {
		t.Fatalf("detect features through facade: %v", err)
	}
	if len(features) != 0 {
		t.Fatalf("sample table should have no unsupported features: %+v", features)
	}
}

func TestFakeFacadeContracts(t *testing.T) {
	var _ api.MetadataFacade = fakeMetadataFacade{}
	var _ api.ScanPlanner = fakeScanPlanner{}
	var _ api.WriteBuilder = fakeWriteBuilder{}
	var _ api.Committer = fakeCommitter{}
	var _ api.FeatureDetector = fakeMetadataFacade{}

	plan, err := fakeScanPlanner{}.PlanScan(context.Background(), api.ScanPlanRequest{})
	if err != nil || plan.Snapshot.SnapshotID != 7 {
		t.Fatalf("fake scan plan=%+v err=%v", plan, err)
	}
	attempt, err := fakeWriteBuilder{}.BuildAppend(context.Background(), api.AppendRequest{DataFiles: []api.DataFile{{FilePath: "s3://warehouse/t/data.parquet"}}})
	if err != nil || len(attempt.DataFiles) != 1 {
		t.Fatalf("fake append attempt=%+v err=%v", attempt, err)
	}
	result, err := fakeCommitter{}.CommitTable(context.Background(), api.CommitRequest{IdempotencyKey: "k"})
	if err != nil || result.SnapshotID != 8 {
		t.Fatalf("fake commit result=%+v err=%v", result, err)
	}
}

type fakeMetadataFacade struct{}

func (fakeMetadataFacade) AdapterName() string {
	return "fake"
}

func (fakeMetadataFacade) ParseTableMetadata(ctx context.Context, data []byte, metadataLocation string) (*api.TableMetadata, error) {
	return &api.TableMetadata{FormatVersion: 2, Location: "s3://warehouse/t"}, nil
}

func (fakeMetadataFacade) ReadManifestList(ctx context.Context, data []byte) ([]api.ManifestFile, error) {
	return []api.ManifestFile{{Path: "s3://warehouse/t/metadata/m0.avro"}}, nil
}

func (fakeMetadataFacade) ReadManifest(ctx context.Context, data []byte) ([]api.ManifestEntry, error) {
	return []api.ManifestEntry{{SnapshotID: 7}}, nil
}

func (fakeMetadataFacade) ResolveSnapshot(ctx context.Context, meta *api.TableMetadata, selector api.SnapshotSelector) (api.Snapshot, error) {
	return api.Snapshot{SnapshotID: 7}, nil
}

func (fakeMetadataFacade) DetectUnsupportedP0(ctx context.Context, meta *api.TableMetadata, manifests []api.ManifestFile, files []api.DataFile) ([]api.UnsupportedFeature, error) {
	return nil, nil
}

type fakeScanPlanner struct{}

func (fakeScanPlanner) PlanScan(ctx context.Context, req api.ScanPlanRequest) (*api.IcebergScanPlan, error) {
	return &api.IcebergScanPlan{Snapshot: api.SnapshotPlan{SnapshotID: 7}}, nil
}

type fakeWriteBuilder struct{}

func (fakeWriteBuilder) BuildAppend(ctx context.Context, req api.AppendRequest) (*api.CommitAttempt, error) {
	return &api.CommitAttempt{DataFiles: req.DataFiles, Summary: req.Summary}, nil
}

type fakeCommitter struct{}

func (fakeCommitter) CommitTable(ctx context.Context, req api.CommitRequest) (*api.CommitResult, error) {
	return &api.CommitResult{SnapshotID: 8, CommitID: req.IdempotencyKey}, nil
}
