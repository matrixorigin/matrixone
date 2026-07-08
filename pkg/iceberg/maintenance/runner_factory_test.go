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

package maintenance

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

func TestCatalogExpireSnapshotsRunnerLoadsCommitsAndRecordsOrphans(t *testing.T) {
	now := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	var loadReq api.LoadTableRequest
	var commitReq api.CommitRequest
	client := &catalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			loadReq = req
			return &api.LoadTableResponse{
				Namespace:        req.Namespace,
				TableName:        req.Table,
				MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
				MetadataJSON:     expireMetadataJSON(),
			}, nil
		},
		CommitTableFunc: func(ctx context.Context, req api.CommitRequest) (*api.CommitResult, error) {
			commitReq = req
			return &api.CommitResult{SnapshotID: 4, CommitID: "commit-expire", Verified: true}, nil
		},
	}
	recorder := &fakeMaintenanceOrphanRecorder{}
	runner := NewCatalogExpireSnapshotsRunner(
		client,
		api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
		CatalogRunnerOptions{
			OrphanRecorder:    recorder,
			Now:               func() time.Time { return now },
			OrphanTTL:         time.Hour,
			DefaultRetainLast: 3,
		},
	)
	result, err := runner.RunMaintenance(context.Background(), Request{
		AccountID:      7,
		CatalogID:      42,
		Namespace:      "sales",
		Table:          "orders",
		TargetRef:      "main",
		JobID:          "job-1",
		IdempotencyKey: "idem-1",
		Operation:      OperationExpireSnapshots,
		Options:        map[string]string{expireOptionOlderThan: "2026-01-04 00:00:00"},
	})
	require.NoError(t, err)
	require.Equal(t, Result{SnapshotAfter: "4", RemovedFileCount: 1, CommitID: "commit-expire", Verified: true}, result)
	require.Equal(t, api.Namespace{"sales"}, loadReq.Namespace)
	require.Equal(t, "orders", loadReq.Table)
	require.Equal(t, "all", loadReq.Snapshots)
	require.Equal(t, "main", commitReq.TargetRef)
	require.Equal(t, "idem-1", commitReq.IdempotencyKey)
	require.Len(t, commitReq.Updates, 2)
	require.Equal(t, "remove-snapshot", commitReq.Updates[0].Type)
	require.Equal(t, "1", commitReq.Updates[0].Payload["snapshot_id"])
	require.Len(t, recorder.candidates, 1)
	require.Equal(t, "s3://warehouse/orders/metadata/snap-1.avro", recorder.candidates[0].FilePath)
	require.Equal(t, now.Add(time.Hour), recorder.candidates[0].ExpireAt)
}

func TestCatalogRewriteManifestsRunnerLoadsAndCommitsCatalogAction(t *testing.T) {
	var loadReq api.LoadTableRequest
	var commitReq api.CommitRequest
	var verifiedPlan *CommitPlan
	client := &catalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			loadReq = req
			return &api.LoadTableResponse{
				Namespace:        req.Namespace,
				TableName:        req.Table,
				MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
				MetadataJSON:     expireMetadataJSON(),
			}, nil
		},
		CommitTableFunc: func(ctx context.Context, req api.CommitRequest) (*api.CommitResult, error) {
			commitReq = req
			return &api.CommitResult{SnapshotID: 5, CommitID: "commit-rewrite-manifests"}, nil
		},
	}
	runner := NewCatalogRewriteManifestsRunner(
		client,
		api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
		CatalogRunnerOptions{
			CommitVerifier: CommitResultVerifierFunc(func(ctx context.Context, req Request, plan *CommitPlan, result api.CommitResult) (api.CommitResult, bool, error) {
				verifiedPlan = plan
				result.SnapshotID = 55
				result.CommitID = "verified-rewrite-manifests"
				return result, true, nil
			}),
		},
	)
	result, err := runner.RunMaintenance(context.Background(), Request{
		AccountID:      7,
		CatalogID:      42,
		Namespace:      "sales",
		Table:          "orders",
		TargetRef:      "main",
		JobID:          "job-1",
		IdempotencyKey: "idem-1",
		Operation:      OperationRewriteManifests,
	})
	require.NoError(t, err)
	require.Equal(t, Result{SnapshotAfter: "55", CommitID: "verified-rewrite-manifests", Verified: true}, result)
	require.NotNil(t, verifiedPlan)
	require.Equal(t, api.Namespace{"sales"}, loadReq.Namespace)
	require.Equal(t, "orders", loadReq.Table)
	require.Equal(t, "main", loadReq.Snapshots)
	require.Equal(t, "main", commitReq.TargetRef)
	require.Equal(t, "idem-1", commitReq.IdempotencyKey)
	require.Len(t, commitReq.Updates, 2)
	require.Equal(t, "rewrite-manifests", commitReq.Updates[0].Type)
	require.Equal(t, "s3://warehouse/orders/metadata/snap-4.avro", commitReq.Updates[0].FilePath)
	require.Equal(t, "4", commitReq.Updates[0].Payload["snapshot_id"])
}

func TestCatalogRewriteManifestsRunnerResolvesConfigPrefix(t *testing.T) {
	var configReq api.GetConfigRequest
	var loadReq api.LoadTableRequest
	var commitReq api.CommitRequest
	client := &catalog.MockClient{
		GetConfigFunc: func(ctx context.Context, req api.GetConfigRequest) (*api.ConfigResponse, error) {
			configReq = req
			return &api.ConfigResponse{Prefix: "main"}, nil
		},
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			loadReq = req
			return &api.LoadTableResponse{
				Namespace:        req.Namespace,
				TableName:        req.Table,
				MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
				MetadataJSON:     expireMetadataJSON(),
			}, nil
		},
		CommitTableFunc: func(ctx context.Context, req api.CommitRequest) (*api.CommitResult, error) {
			commitReq = req
			return &api.CommitResult{SnapshotID: 5, CommitID: "commit-rewrite-manifests"}, nil
		},
	}
	runner := CatalogRewriteManifestsRunner{
		CatalogFactory: maintenanceCatalogFactoryFunc(func(ctx context.Context, catalog model.Catalog) (api.CatalogClient, error) {
			return client, nil
		}),
	}
	_, err := runner.RunMaintenance(context.Background(), Request{
		AccountID:      0,
		CatalogID:      42,
		Catalog:        model.Catalog{AccountID: 0, CatalogID: 42, Warehouse: "s3://mo-iceberg/warehouse"},
		Namespace:      "maintenance",
		Table:          "orders_small",
		TargetRef:      "main",
		JobID:          "job-1",
		IdempotencyKey: "idem-1",
		Operation:      OperationRewriteManifests,
	})
	require.NoError(t, err)
	require.Equal(t, "s3://mo-iceberg/warehouse", configReq.Warehouse)
	require.Equal(t, "main", loadReq.Prefix)
	require.Equal(t, "main", commitReq.Prefix)
}

func TestCatalogRunnerFactoriesRequireFactoryAndResolvedCatalog(t *testing.T) {
	ctx := context.Background()
	req := Request{
		Operation: OperationRewriteManifests,
		Catalog:   model.Catalog{},
		Namespace: "sales",
		Table:     "orders",
	}
	runners := []struct {
		name string
		run  func(context.Context, Request) (Result, error)
	}{
		{"expire", CatalogExpireSnapshotsRunner{}.RunMaintenance},
		{"rewrite-data-files", CatalogRewriteDataFilesRunner{}.RunMaintenance},
		{"rewrite-manifests", CatalogRewriteManifestsRunner{}.RunMaintenance},
	}
	for _, tc := range runners {
		_, err := tc.run(ctx, req)
		require.Error(t, err, tc.name)
		require.Contains(t, err.Error(), string(api.ErrConfigInvalid))
	}

	factory := maintenanceCatalogFactoryFunc(func(ctx context.Context, catalog model.Catalog) (api.CatalogClient, error) {
		return nil, api.NewError(api.ErrCatalogUnavailable, "catalog down", nil)
	})
	_, err := (CatalogRewriteDataFilesRunner{CatalogFactory: factory}).RunMaintenance(ctx, Request{
		Operation: OperationRewriteDataFiles,
		Catalog:   model.Catalog{AccountID: 7, CatalogID: 42, Name: "ksa_gold"},
		Namespace: "sales",
		Table:     "orders",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrCatalogUnavailable))
}

func TestResolveMaintenanceCatalogRequestPrefixSkipsWhenAlreadySetOrClientMissing(t *testing.T) {
	ctx := context.Background()
	req := api.CatalogRequest{Prefix: "explicit", Catalog: model.Catalog{Warehouse: "warehouse"}}
	got, err := resolveMaintenanceCatalogRequestPrefix(ctx, nil, req)
	require.NoError(t, err)
	require.Equal(t, "explicit", got.Prefix)

	client := &catalog.MockClient{GetConfigFunc: func(ctx context.Context, req api.GetConfigRequest) (*api.ConfigResponse, error) {
		t.Fatalf("GetConfig should not be called when prefix is already set")
		return nil, nil
	}}
	got, err = resolveMaintenanceCatalogRequestPrefix(ctx, client, req)
	require.NoError(t, err)
	require.Equal(t, "explicit", got.Prefix)

	errClient := &catalog.MockClient{GetConfigFunc: func(ctx context.Context, req api.GetConfigRequest) (*api.ConfigResponse, error) {
		return nil, api.NewError(api.ErrCatalogUnavailable, "config unavailable", nil)
	}}
	_, err = resolveMaintenanceCatalogRequestPrefix(ctx, errClient, api.CatalogRequest{Catalog: model.Catalog{Warehouse: "warehouse"}})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrCatalogUnavailable))
}

func TestNativeRewriteManifestsRunnerMaterializesWritesAndCommits(t *testing.T) {
	oldManifestPath := "s3://warehouse/orders/metadata/old-manifest.avro"
	oldManifestListPath := "s3://warehouse/orders/metadata/snap-4.avro"
	manifestBytes, err := metadata.EncodeManifest([]api.ManifestEntry{{
		Status:         api.ManifestEntryExisting,
		SnapshotID:     4,
		SequenceNumber: 4,
		FileSequence:   4,
		DataFile: api.DataFile{
			Content:         api.DataFileContentData,
			FilePath:        "s3://warehouse/orders/data/part-1.parquet",
			FileFormat:      "parquet",
			RecordCount:     10,
			FileSizeInBytes: 100,
		},
	}})
	require.NoError(t, err)
	manifestListBytes, err := metadata.EncodeManifestList([]api.ManifestFile{{
		Path:               oldManifestPath,
		Length:             int64(len(manifestBytes)),
		Content:            api.ManifestContentData,
		ExistingFilesCount: 1,
		ExistingRowsCount:  10,
	}})
	require.NoError(t, err)

	now := time.Date(2026, 6, 22, 10, 0, 0, 0, time.UTC)
	var commitReq api.CommitRequest
	client := &catalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			return &api.LoadTableResponse{
				Namespace:        req.Namespace,
				TableName:        req.Table,
				MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
				MetadataJSON:     expireMetadataJSON(),
			}, nil
		},
		CommitTableFunc: func(ctx context.Context, req api.CommitRequest) (*api.CommitResult, error) {
			commitReq = req
			return &api.CommitResult{SnapshotID: 7, CommitID: "commit-native-rewrite-manifests", Verified: true}, nil
		},
	}
	writer := &recordingMaintenanceObjectWriter{payloads: make(map[string][]byte)}
	recorder := &fakeMaintenanceOrphanRecorder{}
	runner := NewNativeRewriteManifestsRunner(
		client,
		api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
		fakeRewriteManifestObjectReader{
			oldManifestListPath: manifestListBytes,
			oldManifestPath:     manifestBytes,
		},
		writer,
		CatalogRunnerOptions{
			OrphanRecorder: recorder,
			Now:            func() time.Time { return now },
			OrphanTTL:      time.Hour,
		},
	)
	result, err := runner.RunMaintenance(context.Background(), Request{
		AccountID:      7,
		CatalogID:      42,
		Namespace:      "sales",
		Table:          "orders",
		TargetRef:      "main",
		JobID:          "job-1",
		IdempotencyKey: "idem-native",
		Operation:      OperationRewriteManifests,
	})
	require.NoError(t, err)
	require.Equal(t, Result{SnapshotAfter: "7", RewrittenFileCount: 1, CommitID: "commit-native-rewrite-manifests", Verified: true}, result)
	require.Len(t, writer.paths, 2)
	require.Len(t, writer.payloads, 2)
	newManifestPath := writer.paths[0]
	newManifestListPath := writer.paths[1]
	require.Contains(t, newManifestPath, "/metadata/mo-rewrite-manifests/rw-"+api.PathHash("idem-native")+"/manifest-00000.avro")
	require.Contains(t, newManifestListPath, "/metadata/mo-rewrite-manifests/rw-"+api.PathHash("idem-native")+"/manifest-list.avro")
	rewrittenEntries, err := metadata.ReadManifest(writer.payloads[newManifestPath])
	require.NoError(t, err)
	require.Equal(t, "s3://warehouse/orders/data/part-1.parquet", rewrittenEntries[0].DataFile.FilePath)
	rewrittenList, err := metadata.ReadManifestList(writer.payloads[newManifestListPath])
	require.NoError(t, err)
	require.Equal(t, newManifestPath, rewrittenList[0].Path)

	require.Equal(t, "main", commitReq.TargetRef)
	require.Equal(t, "idem-native", commitReq.IdempotencyKey)
	require.Len(t, commitReq.Updates, 2)
	require.Equal(t, "add-snapshot", commitReq.Updates[0].Type)
	require.NotNil(t, commitReq.Updates[0].Snapshot)
	require.Equal(t, newManifestListPath, commitReq.Updates[0].Snapshot.ManifestList)
	require.NotNil(t, commitReq.Updates[0].Snapshot.ParentSnapshotID)
	require.Equal(t, int64(4), *commitReq.Updates[0].Snapshot.ParentSnapshotID)
	require.Equal(t, int64(1), commitReq.Updates[0].Snapshot.SequenceNumber)
	require.Equal(t, now.UnixMilli(), commitReq.Updates[0].Snapshot.TimestampMS)
	require.Equal(t, "rewrite_manifests", commitReq.Updates[0].Snapshot.Summary["operation"])
	require.Equal(t, "set-snapshot-ref", commitReq.Updates[1].Type)
	require.Equal(t, "main", commitReq.Updates[1].Ref)
	require.Equal(t, commitReq.Updates[0].Snapshot.SnapshotID, commitReq.Updates[1].SnapshotID)

	require.Len(t, recorder.candidates, 2)
	orphanPaths := make([]string, 0, len(recorder.candidates))
	for _, candidate := range recorder.candidates {
		require.Equal(t, now.Add(time.Hour), candidate.ExpireAt)
		orphanPaths = append(orphanPaths, candidate.FilePath)
	}
	require.ElementsMatch(t, []string{oldManifestListPath, oldManifestPath}, orphanPaths)
}

func TestCatalogRewriteDataFilesRunnerLoadsAndCommitsCatalogAction(t *testing.T) {
	var loadReq api.LoadTableRequest
	var commitReq api.CommitRequest
	client := &catalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			loadReq = req
			return &api.LoadTableResponse{
				Namespace:        req.Namespace,
				TableName:        req.Table,
				MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
				MetadataJSON:     expireMetadataJSON(),
			}, nil
		},
		CommitTableFunc: func(ctx context.Context, req api.CommitRequest) (*api.CommitResult, error) {
			commitReq = req
			return &api.CommitResult{SnapshotID: 6, CommitID: "commit-rewrite-data", Verified: true}, nil
		},
	}
	runner := NewCatalogRewriteDataFilesRunner(
		client,
		api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
		CatalogRunnerOptions{},
	)
	result, err := runner.RunMaintenance(context.Background(), Request{
		AccountID:      7,
		CatalogID:      42,
		Namespace:      "sales",
		Table:          "orders",
		TargetRef:      "main",
		JobID:          "job-1",
		IdempotencyKey: "idem-1",
		Operation:      OperationRewriteDataFiles,
		Options:        map[string]string{"target_file_size": "268435456"},
	})
	require.NoError(t, err)
	require.Equal(t, Result{SnapshotAfter: "6", CommitID: "commit-rewrite-data", Verified: true}, result)
	require.Equal(t, api.Namespace{"sales"}, loadReq.Namespace)
	require.Equal(t, "orders", loadReq.Table)
	require.Equal(t, "main", loadReq.Snapshots)
	require.Equal(t, "main", commitReq.TargetRef)
	require.Equal(t, "idem-1", commitReq.IdempotencyKey)
	require.Len(t, commitReq.Updates, 2)
	require.Equal(t, "rewrite-data-files", commitReq.Updates[0].Type)
	require.Equal(t, "s3://warehouse/orders/metadata/snap-4.avro", commitReq.Updates[0].FilePath)
	require.Equal(t, "4", commitReq.Updates[0].Payload["snapshot_id"])
	require.Equal(t, "268435456", commitReq.Updates[0].Payload["target_file_size"])
}

func expireMetadataJSON() []byte {
	return []byte(`{
		"format-version": 2,
		"table-uuid": "uuid-1",
		"location": "s3://warehouse/orders",
		"current-schema-id": 1,
		"schemas": [{"schema-id": 1, "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]}],
		"default-spec-id": 0,
		"partition-specs": [{"spec-id": 0, "fields": []}],
		"current-snapshot-id": 4,
		"refs": {"main": {"snapshot-id": 4, "type": "branch"}},
		"snapshots": [
			{"snapshot-id": 1, "timestamp-ms": 1767225600000, "manifest-list": "s3://warehouse/orders/metadata/snap-1.avro"},
			{"snapshot-id": 2, "parent-snapshot-id": 1, "timestamp-ms": 1767312000000, "manifest-list": "s3://warehouse/orders/metadata/snap-2.avro"},
			{"snapshot-id": 3, "parent-snapshot-id": 2, "timestamp-ms": 1767398400000, "manifest-list": "s3://warehouse/orders/metadata/snap-3.avro"},
			{"snapshot-id": 4, "parent-snapshot-id": 3, "timestamp-ms": 1767484800000, "manifest-list": "s3://warehouse/orders/metadata/snap-4.avro"}
		]
	}`)
}

type recordingMaintenanceObjectWriter struct {
	paths    []string
	payloads map[string][]byte
}

func (w *recordingMaintenanceObjectWriter) WriteObject(ctx context.Context, location string, payload []byte) error {
	w.paths = append(w.paths, location)
	if w.payloads != nil {
		w.payloads[location] = append([]byte(nil), payload...)
	}
	return nil
}

type maintenanceCatalogFactoryFunc func(context.Context, model.Catalog) (api.CatalogClient, error)

func (fn maintenanceCatalogFactoryFunc) NewClient(ctx context.Context, catalog model.Catalog) (api.CatalogClient, error) {
	return fn(ctx, catalog)
}
