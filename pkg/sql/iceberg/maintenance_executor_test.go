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
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergcatalog "github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/iceberg/maintenance"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

func TestMaintenanceProcedureExecutorRunsExpireSnapshots(t *testing.T) {
	now := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	store := &fakeMaintenanceStore{
		catalog: model.Catalog{
			AccountID:        7,
			CatalogID:        42,
			Name:             "ksa_gold",
			Type:             "rest",
			URI:              "https://catalog.example.com",
			CapabilitiesJSON: `{"commit":true}`,
		},
		principalMaps: []model.PrincipalMap{{
			AccountID:         7,
			CatalogID:         42,
			MORoleID:          11,
			MOUserID:          22,
			ExternalPrincipal: "maintenance-principal",
		}},
	}
	var loadReq api.LoadTableRequest
	var commitReq api.CommitRequest
	client := &icebergcatalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			loadReq = req
			return &api.LoadTableResponse{
				Namespace:        req.Namespace,
				TableName:        req.Table,
				MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
				MetadataJSON:     maintenanceExpireMetadataJSON(),
			}, nil
		},
		CommitTableFunc: func(ctx context.Context, req api.CommitRequest) (*api.CommitResult, error) {
			commitReq = req
			return &api.CommitResult{SnapshotID: 4, CommitID: "commit-expire", Verified: true}, nil
		},
	}
	factory := &fakeMaintenanceCatalogFactory{client: client}
	cfg := api.DefaultConfig()
	cfg.Enable = true
	cfg.Write.EnableWrite = true
	cfg.Write.EnableMaintenance = true
	cfg.Write.OrphanTTL = time.Hour
	executor := NewMaintenanceProcedureExecutor(store, MaintenanceProcedureExecutorOptions{
		Config:            cfg,
		Account:           api.AccountConfig{AccountID: 7, Enable: true},
		CatalogFactory:    factory,
		Now:               func() time.Time { return now },
		DefaultRetainLast: 3,
	})
	parsed, err := maintenance.ParseProcedureCall("iceberg_expire_snapshots", "ksa_gold.sales.orders", "older_than=2026-01-04 00:00:00")
	requireNoErr(t, err)
	result, err := executor.Execute(context.Background(), maintenance.ProcedureExecutionRequest{
		AccountID:   7,
		RoleID:      11,
		UserID:      22,
		StatementID: "stmt-1",
		Parsed:      parsed,
	})
	requireNoErr(t, err)
	if result.SnapshotAfter != "4" || result.CommitID != "commit-expire" || result.RemovedFileCount != 1 || !result.Verified {
		t.Fatalf("unexpected result: %+v", result)
	}
	if factory.catalog.Name != "ksa_gold" || factory.catalog.URI != "https://catalog.example.com" {
		t.Fatalf("catalog factory received wrong catalog: %+v", factory.catalog)
	}
	if strings.Join(loadReq.Namespace, ".") != "sales" || loadReq.Table != "orders" || loadReq.Snapshots != "all" {
		t.Fatalf("unexpected load request: %+v", loadReq)
	}
	if loadReq.ExternalPrincipal != "maintenance-principal" || commitReq.ExternalPrincipal != "maintenance-principal" {
		t.Fatalf("maintenance catalog requests did not carry external principal: load=%q commit=%q", loadReq.ExternalPrincipal, commitReq.ExternalPrincipal)
	}
	if commitReq.TargetRef != "main" || commitReq.IdempotencyKey != "stmt-1" || len(commitReq.Updates) != 1 || commitReq.Updates[0].Type != "remove-snapshots" {
		t.Fatalf("unexpected commit request: %+v", commitReq)
	}
	if len(store.jobs) != 1 || store.jobs[0].JobID != "stmt-1" || store.jobs[0].CatalogID != 42 {
		t.Fatalf("unexpected jobs: %+v", store.jobs)
	}
	requireMaintenanceStoreTransitions(t, store, []string{maintenance.StatusRunning, maintenance.StatusCommitted}, []string{"", ""})
	if len(store.orphans) != 1 || store.orphans[0].FilePathRedacted == "" || store.orphans[0].FilePathHash == "" {
		t.Fatalf("expected one post-commit orphan candidate, got %+v", store.orphans)
	}
}

func TestMaintenanceProcedureExecutorRunsRewriteManifests(t *testing.T) {
	store := &fakeMaintenanceStore{
		catalog: model.Catalog{
			AccountID:        7,
			CatalogID:        42,
			Name:             "ksa_gold",
			Type:             "rest",
			URI:              "https://catalog.example.com",
			CapabilitiesJSON: `{"commit":true}`,
		},
	}
	var loadReq api.LoadTableRequest
	var commitReq api.CommitRequest
	client := &icebergcatalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			loadReq = req
			return &api.LoadTableResponse{
				Namespace:        req.Namespace,
				TableName:        req.Table,
				MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
				MetadataJSON:     maintenanceExpireMetadataJSON(),
			}, nil
		},
		CommitTableFunc: func(ctx context.Context, req api.CommitRequest) (*api.CommitResult, error) {
			commitReq = req
			return &api.CommitResult{SnapshotID: 5, CommitID: "commit-rewrite-manifests", Verified: true}, nil
		},
	}
	cfg := api.DefaultConfig()
	cfg.Enable = true
	cfg.Write.EnableWrite = true
	cfg.Write.EnableMaintenance = true
	var verified bool
	executor := NewMaintenanceProcedureExecutor(store, MaintenanceProcedureExecutorOptions{
		Config:         cfg,
		Account:        api.AccountConfig{AccountID: 7, Enable: true},
		CatalogFactory: &fakeMaintenanceCatalogFactory{client: client},
		CommitVerifier: maintenance.CommitResultVerifierFunc(func(ctx context.Context, req maintenance.Request, plan *maintenance.CommitPlan, result api.CommitResult) (api.CommitResult, bool, error) {
			verified = true
			result.SnapshotID = 55
			result.CommitID = "verified-rewrite-manifests"
			return result, true, nil
		}),
	})
	parsed, err := maintenance.ParseProcedureCall("iceberg_rewrite_manifests", "ksa_gold.sales.orders", "ref=main")
	requireNoErr(t, err)
	result, err := executor.Execute(context.Background(), maintenance.ProcedureExecutionRequest{
		AccountID:   7,
		StatementID: "stmt-1",
		Parsed:      parsed,
	})
	requireNoErr(t, err)
	if result.SnapshotAfter != "55" || result.CommitID != "verified-rewrite-manifests" || !result.Verified || !verified {
		t.Fatalf("unexpected rewrite manifests result: %+v", result)
	}
	if strings.Join(loadReq.Namespace, ".") != "sales" || loadReq.Table != "orders" || loadReq.Snapshots != "all" {
		t.Fatalf("unexpected load request: %+v", loadReq)
	}
	if commitReq.TargetRef != "main" || commitReq.IdempotencyKey != "stmt-1" || len(commitReq.Updates) != 2 || commitReq.Updates[0].Type != "rewrite-manifests" {
		t.Fatalf("unexpected commit request: %+v", commitReq)
	}
	requireMaintenanceStoreTransitions(t, store, []string{maintenance.StatusRunning, maintenance.StatusCommitted}, []string{"", ""})
}

func TestMaintenanceProcedureExecutorNativeRewriteManifestsRequiresObjectCredentials(t *testing.T) {
	store := &fakeMaintenanceStore{
		catalog: model.Catalog{
			AccountID:        7,
			CatalogID:        42,
			Name:             "ksa_gold",
			Type:             "rest",
			URI:              "https://catalog.example.com",
			CapabilitiesJSON: `{"commit":true}`,
		},
	}
	cfg := api.DefaultConfig()
	cfg.Enable = true
	cfg.Write.EnableWrite = true
	cfg.Write.EnableMaintenance = true
	client := &icebergcatalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			return &api.LoadTableResponse{
				Namespace:        req.Namespace,
				TableName:        req.Table,
				MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
				MetadataJSON:     maintenanceExpireMetadataJSON(),
			}, nil
		},
	}
	executor := NewMaintenanceProcedureExecutor(store, MaintenanceProcedureExecutorOptions{
		Config:                    cfg,
		Account:                   api.AccountConfig{AccountID: 7, Enable: true},
		CatalogFactory:            &fakeMaintenanceCatalogFactory{client: client},
		UseNativeRewriteManifests: true,
	})
	parsed, err := maintenance.ParseProcedureCall("iceberg_rewrite_manifests", "ksa_gold.sales.orders", "ref=main")
	requireNoErr(t, err)
	_, err = executor.Execute(context.Background(), maintenance.ProcedureExecutionRequest{
		AccountID:   7,
		StatementID: "stmt-1",
		Parsed:      parsed,
	})
	if err == nil || !strings.Contains(err.Error(), "did not return usable storage credentials") {
		t.Fatalf("expected missing object credential error, got %v", err)
	}
	requireMaintenanceStoreTransitions(t, store, []string{maintenance.StatusRunning, maintenance.StatusFailed}, []string{"", string(api.ErrCredentialExpired)})
}

func TestMaintenanceProcedureExecutorRunsNativeRewriteManifests(t *testing.T) {
	ctx := context.Background()
	oldManifestPath := "s3://warehouse/orders/metadata/old-manifest.avro"
	oldManifestListPath := "s3://warehouse/orders/metadata/snap-4.avro"
	manifestBytes, err := encodeSQLIcebergTestManifest([]api.ManifestEntry{{
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
	requireNoErr(t, err)
	manifestListBytes, err := encodeSQLIcebergTestManifestList([]api.ManifestFile{{
		Path:               oldManifestPath,
		Length:             int64(len(manifestBytes)),
		Content:            api.ManifestContentData,
		ExistingFilesCount: 1,
		ExistingRowsCount:  10,
	}})
	requireNoErr(t, err)
	fs, err := fileservice.NewMemoryFS("iceberg-native-rewrite-manifests", fileservice.DisabledCacheConfig, nil)
	requireNoErr(t, err)
	writeMaintenanceMemoryFile(t, ctx, fs, maintenanceMemoryPath(oldManifestPath), manifestBytes)
	writeMaintenanceMemoryFile(t, ctx, fs, maintenanceMemoryPath(oldManifestListPath), manifestListBytes)

	store := &fakeMaintenanceStore{
		catalog: model.Catalog{
			AccountID:        7,
			CatalogID:        42,
			Name:             "ksa_gold",
			Type:             "rest",
			URI:              "https://catalog.example.com",
			CapabilitiesJSON: `{"commit":true}`,
		},
	}
	var commitReq api.CommitRequest
	client := &icebergcatalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			return &api.LoadTableResponse{
				Namespace:        req.Namespace,
				TableName:        req.Table,
				MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
				MetadataJSON:     maintenanceExpireMetadataJSON(),
			}, nil
		},
		CommitTableFunc: func(ctx context.Context, req api.CommitRequest) (*api.CommitResult, error) {
			commitReq = req
			return &api.CommitResult{SnapshotID: 6, CommitID: "commit-native-rewrite-manifests", Verified: true}, nil
		},
	}
	cfg := api.DefaultConfig()
	cfg.Enable = true
	cfg.Write.EnableWrite = true
	cfg.Write.EnableMaintenance = true
	executor := NewMaintenanceProcedureExecutor(store, MaintenanceProcedureExecutorOptions{
		Config:         cfg,
		Account:        api.AccountConfig{AccountID: 7, Enable: true},
		CatalogFactory: &fakeMaintenanceCatalogFactory{client: client},
		ObjectIOProvider: icebergio.ScopedProvider{
			FileService: fs,
		},
		ScopeForLocation: func(location string) icebergio.ObjectScope {
			return icebergio.ObjectScope{
				AccountID:       7,
				CatalogID:       42,
				StorageLocation: maintenanceMemoryPath(location),
				Endpoint:        "s3.me-central-1.amazonaws.com",
				Region:          "me-central-1",
				Bucket:          "warehouse",
				Principal:       "mo-cn-test",
			}
		},
		UseNativeRewriteManifests: true,
	})
	parsed, err := maintenance.ParseProcedureCall("iceberg_rewrite_manifests", "ksa_gold.sales.orders", "ref=main")
	requireNoErr(t, err)
	result, err := executor.Execute(ctx, maintenance.ProcedureExecutionRequest{
		AccountID:   7,
		StatementID: "stmt-1",
		Parsed:      parsed,
	})
	requireNoErr(t, err)
	if result.SnapshotAfter != "6" || result.CommitID != "commit-native-rewrite-manifests" || !result.Verified || result.RewrittenFileCount != 1 {
		t.Fatalf("unexpected native rewrite manifests result: %+v", result)
	}
	if commitReq.TargetRef != "main" || commitReq.IdempotencyKey != "stmt-1" || len(commitReq.Updates) != 2 || commitReq.Updates[0].Type != "add-snapshot" || commitReq.Updates[1].Type != "set-snapshot-ref" {
		t.Fatalf("unexpected commit request: %+v", commitReq)
	}
	snapshot := commitReq.Updates[0].Snapshot
	if snapshot == nil || snapshot.ParentSnapshotID == nil || *snapshot.ParentSnapshotID != 4 || snapshot.SnapshotID != commitReq.Updates[1].SnapshotID {
		t.Fatalf("unexpected add-snapshot update: %+v", commitReq.Updates[0])
	}
	newManifestPath := snapshot.ManifestList
	if newManifestPath == "" || !strings.Contains(newManifestPath, "/metadata/mo-rewrite-manifests/rw-"+api.PathHash("stmt-1")+"/manifest-list.avro") {
		t.Fatalf("unexpected native manifest list path: %q", newManifestPath)
	}
	readBack := readMaintenanceMemoryFile(t, ctx, fs, maintenanceMemoryPath(newManifestPath))
	rewrittenList, err := metadata.ReadManifestList(readBack)
	requireNoErr(t, err)
	if len(rewrittenList) != 1 || !strings.Contains(rewrittenList[0].Path, "/metadata/mo-rewrite-manifests/rw-"+api.PathHash("stmt-1")+"/manifest-00000.avro") {
		t.Fatalf("unexpected rewritten manifest list: %+v", rewrittenList)
	}
	rewrittenEntries, err := metadata.ReadManifest(readMaintenanceMemoryFile(t, ctx, fs, maintenanceMemoryPath(rewrittenList[0].Path)))
	requireNoErr(t, err)
	if len(rewrittenEntries) != 1 || rewrittenEntries[0].DataFile.FilePath != "s3://warehouse/orders/data/part-1.parquet" {
		t.Fatalf("unexpected rewritten manifest entries: %+v", rewrittenEntries)
	}
	requireMaintenanceStoreTransitions(t, store, []string{maintenance.StatusRunning, maintenance.StatusCommitted}, []string{"", ""})
}

func TestMaintenanceProcedureExecutorNativeRewriteDataFilesRequiresObjectCredentials(t *testing.T) {
	store := &fakeMaintenanceStore{
		catalog: model.Catalog{
			AccountID:        7,
			CatalogID:        42,
			Name:             "ksa_gold",
			Type:             "rest",
			URI:              "https://catalog.example.com",
			CapabilitiesJSON: `{"commit":true}`,
		},
	}
	cfg := api.DefaultConfig()
	cfg.Enable = true
	cfg.Write.EnableWrite = true
	cfg.Write.EnableMaintenance = true
	client := &icebergcatalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			return &api.LoadTableResponse{
				Namespace:        req.Namespace,
				TableName:        req.Table,
				MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
				MetadataJSON:     maintenanceExpireMetadataJSON(),
			}, nil
		},
	}
	executor := NewMaintenanceProcedureExecutor(store, MaintenanceProcedureExecutorOptions{
		Config:                    cfg,
		Account:                   api.AccountConfig{AccountID: 7, Enable: true},
		CatalogFactory:            &fakeMaintenanceCatalogFactory{client: client},
		UseNativeRewriteDataFiles: true,
		DataFileCompactor: maintenance.RewriteDataFilesCompactorFunc(func(ctx context.Context, req maintenance.RewriteDataFilesCompactRequest) (*maintenance.RewriteDataFilesCompactResult, error) {
			return nil, nil
		}),
	})
	parsed, err := maintenance.ParseProcedureCall("iceberg_rewrite_data_files", "ksa_gold.sales.orders", "ref=main")
	requireNoErr(t, err)
	_, err = executor.Execute(context.Background(), maintenance.ProcedureExecutionRequest{
		AccountID:   7,
		StatementID: "stmt-1",
		Parsed:      parsed,
	})
	if err == nil || !strings.Contains(err.Error(), "did not return usable storage credentials") {
		t.Fatalf("expected missing object credential error, got %v", err)
	}
	requireMaintenanceStoreTransitions(t, store, []string{maintenance.StatusRunning, maintenance.StatusFailed}, []string{"", string(api.ErrCredentialExpired)})
}

func TestMaintenanceProcedureExecutorRunsRewriteDataFiles(t *testing.T) {
	store := &fakeMaintenanceStore{
		catalog: model.Catalog{
			AccountID:        7,
			CatalogID:        42,
			Name:             "ksa_gold",
			Type:             "rest",
			URI:              "https://catalog.example.com",
			CapabilitiesJSON: `{"commit":true}`,
		},
	}
	var loadReq api.LoadTableRequest
	var commitReq api.CommitRequest
	client := &icebergcatalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			loadReq = req
			return &api.LoadTableResponse{
				Namespace:        req.Namespace,
				TableName:        req.Table,
				MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
				MetadataJSON:     maintenanceExpireMetadataJSON(),
			}, nil
		},
		CommitTableFunc: func(ctx context.Context, req api.CommitRequest) (*api.CommitResult, error) {
			commitReq = req
			return &api.CommitResult{SnapshotID: 6, CommitID: "commit-rewrite-data", Verified: true}, nil
		},
	}
	cfg := api.DefaultConfig()
	cfg.Enable = true
	cfg.Write.EnableWrite = true
	cfg.Write.EnableMaintenance = true
	executor := NewMaintenanceProcedureExecutor(store, MaintenanceProcedureExecutorOptions{
		Config:         cfg,
		Account:        api.AccountConfig{AccountID: 7, Enable: true},
		CatalogFactory: &fakeMaintenanceCatalogFactory{client: client},
	})
	parsed, err := maintenance.ParseProcedureCall("iceberg_rewrite_data_files", "ksa_gold.sales.orders", "ref=main,target_file_size=268435456")
	requireNoErr(t, err)
	result, err := executor.Execute(context.Background(), maintenance.ProcedureExecutionRequest{
		AccountID:   7,
		StatementID: "stmt-1",
		Parsed:      parsed,
	})
	requireNoErr(t, err)
	if result.SnapshotAfter != "6" || result.CommitID != "commit-rewrite-data" || !result.Verified {
		t.Fatalf("unexpected rewrite data files result: %+v", result)
	}
	if strings.Join(loadReq.Namespace, ".") != "sales" || loadReq.Table != "orders" || loadReq.Snapshots != "all" {
		t.Fatalf("unexpected load request: %+v", loadReq)
	}
	if commitReq.TargetRef != "main" || commitReq.IdempotencyKey != "stmt-1" || len(commitReq.Updates) != 2 || commitReq.Updates[0].Type != "rewrite-data-files" {
		t.Fatalf("unexpected commit request: %+v", commitReq)
	}
	if commitReq.Updates[0].Payload["target_file_size"] != "268435456" {
		t.Fatalf("rewrite-data-files payload did not preserve target size: %+v", commitReq.Updates[0].Payload)
	}
	requireMaintenanceStoreTransitions(t, store, []string{maintenance.StatusRunning, maintenance.StatusCommitted}, []string{"", ""})
}

func TestMaintenanceProcedureExecutorRunsNativeRewriteDataFilesWithDefaultCompactor(t *testing.T) {
	ctx := context.Background()
	manifestPath := "s3://warehouse/orders/metadata/data-manifest.avro"
	manifestListPath := "s3://warehouse/orders/metadata/snap-4.avro"
	firstPath := "s3://warehouse/orders/data/part-1.parquet"
	secondPath := "s3://warehouse/orders/data/part-2.parquet"
	firstData := maintenanceParquetDataFile(t, []int64{1, 2})
	secondData := maintenanceParquetDataFile(t, []int64{3, 4, 5})
	manifestBytes, err := encodeSQLIcebergTestManifest([]api.ManifestEntry{
		maintenanceRewriteDataEntry(firstPath, int64(len(firstData)), 2),
		maintenanceRewriteDataEntry(secondPath, int64(len(secondData)), 3),
	})
	requireNoErr(t, err)
	manifestListBytes, err := encodeSQLIcebergTestManifestList([]api.ManifestFile{{
		Path:               manifestPath,
		Length:             int64(len(manifestBytes)),
		Content:            api.ManifestContentData,
		ExistingFilesCount: 2,
		ExistingRowsCount:  5,
	}})
	requireNoErr(t, err)
	fs, err := fileservice.NewMemoryFS("iceberg-native-rewrite-data-files", fileservice.DisabledCacheConfig, nil)
	requireNoErr(t, err)
	writeMaintenanceMemoryFile(t, ctx, fs, maintenanceMemoryPath(firstPath), firstData)
	writeMaintenanceMemoryFile(t, ctx, fs, maintenanceMemoryPath(secondPath), secondData)
	writeMaintenanceMemoryFile(t, ctx, fs, maintenanceMemoryPath(manifestPath), manifestBytes)
	writeMaintenanceMemoryFile(t, ctx, fs, maintenanceMemoryPath(manifestListPath), manifestListBytes)

	store := &fakeMaintenanceStore{
		catalog: model.Catalog{
			AccountID:        7,
			CatalogID:        42,
			Name:             "ksa_gold",
			Type:             "rest",
			URI:              "https://catalog.example.com",
			CapabilitiesJSON: `{"commit":true}`,
		},
	}
	var commitReq api.CommitRequest
	client := &icebergcatalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			return &api.LoadTableResponse{
				Namespace:        req.Namespace,
				TableName:        req.Table,
				MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
				MetadataJSON:     maintenanceExpireMetadataJSON(),
			}, nil
		},
		CommitTableFunc: func(ctx context.Context, req api.CommitRequest) (*api.CommitResult, error) {
			commitReq = req
			return &api.CommitResult{SnapshotID: 8, CommitID: "commit-native-rewrite-data", Verified: true}, nil
		},
	}
	cfg := api.DefaultConfig()
	cfg.Enable = true
	cfg.Write.EnableWrite = true
	cfg.Write.EnableMaintenance = true
	executor := NewMaintenanceProcedureExecutor(store, MaintenanceProcedureExecutorOptions{
		Config:         cfg,
		Account:        api.AccountConfig{AccountID: 7, Enable: true},
		CatalogFactory: &fakeMaintenanceCatalogFactory{client: client},
		ObjectIOProvider: icebergio.ScopedProvider{
			FileService: fs,
		},
		ScopeForLocation: func(location string) icebergio.ObjectScope {
			return icebergio.ObjectScope{
				AccountID:       7,
				CatalogID:       42,
				StorageLocation: maintenanceMemoryPath(location),
				Endpoint:        "s3.me-central-1.amazonaws.com",
				Region:          "me-central-1",
				Bucket:          "warehouse",
				Principal:       "mo-cn-test",
			}
		},
		UseNativeRewriteDataFiles: true,
	})
	parsed, err := maintenance.ParseProcedureCall("iceberg_rewrite_data_files", "ksa_gold.sales.orders", "ref=main,target_file_size=1000000")
	requireNoErr(t, err)
	result, err := executor.Execute(ctx, maintenance.ProcedureExecutionRequest{
		AccountID:   7,
		StatementID: "stmt-1",
		Parsed:      parsed,
	})
	requireNoErr(t, err)
	if result.SnapshotAfter != "8" || result.CommitID != "commit-native-rewrite-data" || !result.Verified || result.RewrittenFileCount != 2 {
		t.Fatalf("unexpected native rewrite data files result: %+v", result)
	}
	if commitReq.TargetRef != "main" || commitReq.IdempotencyKey != "stmt-1" || len(commitReq.Updates) != 2 || commitReq.Updates[0].Type != "add-snapshot" || commitReq.Updates[1].Type != "set-snapshot-ref" {
		t.Fatalf("unexpected native rewrite data files commit request: %+v", commitReq)
	}
	snapshot := commitReq.Updates[0].Snapshot
	if snapshot == nil || snapshot.ParentSnapshotID == nil || *snapshot.ParentSnapshotID != 4 || snapshot.SnapshotID != commitReq.Updates[1].SnapshotID {
		t.Fatalf("unexpected add-snapshot update: %+v", commitReq.Updates[0])
	}
	newManifestListPath := snapshot.ManifestList
	if newManifestListPath == "" || !strings.Contains(newManifestListPath, "/metadata/mo-rewrite-data-files/rw-"+api.PathHash("stmt-1")+"/manifest-list.avro") {
		t.Fatalf("unexpected data manifest list path: %q", newManifestListPath)
	}
	manifestList, err := metadata.ReadManifestList(readMaintenanceMemoryFile(t, ctx, fs, maintenanceMemoryPath(newManifestListPath)))
	requireNoErr(t, err)
	var newManifestPath string
	for _, manifest := range manifestList {
		if strings.Contains(manifest.Path, "/metadata/mo-rewrite-data-files/rw-"+api.PathHash("stmt-1")+"/data-manifest.avro") {
			newManifestPath = manifest.Path
		}
	}
	if newManifestPath == "" {
		t.Fatalf("rewritten manifest list did not include data manifest: %+v", manifestList)
	}
	rewrittenEntries, err := metadata.ReadManifest(readMaintenanceMemoryFile(t, ctx, fs, maintenanceMemoryPath(newManifestPath)))
	requireNoErr(t, err)
	if len(rewrittenEntries) != 3 || rewrittenEntries[0].Status != api.ManifestEntryDeleted || rewrittenEntries[2].Status != api.ManifestEntryAdded {
		t.Fatalf("unexpected rewritten data manifest entries: %+v", rewrittenEntries)
	}
	replacementPath := rewrittenEntries[2].DataFile.FilePath
	if !strings.Contains(replacementPath, "/data/mo-rewrite-data-files/rw-"+api.PathHash("stmt-1")+"/group-1-") {
		t.Fatalf("unexpected replacement data path: %q", replacementPath)
	}
	replacementData := readMaintenanceMemoryFile(t, ctx, fs, maintenanceMemoryPath(replacementPath))
	if rows := maintenanceParquetRowCount(t, replacementData); rows != 5 {
		t.Fatalf("unexpected replacement row count: %d", rows)
	}
	if len(store.orphans) != 2 {
		t.Fatalf("expected source data files to be post-commit orphan candidates, got %+v", store.orphans)
	}
	requireMaintenanceStoreTransitions(t, store, []string{maintenance.StatusRunning, maintenance.StatusCommitted}, []string{"", ""})
}

func TestMaintenanceProcedureExecutorRejectsTagWriteByDefault(t *testing.T) {
	store := &fakeMaintenanceStore{
		catalog: model.Catalog{
			AccountID:        7,
			CatalogID:        42,
			Name:             "ksa_gold",
			Type:             "rest",
			URI:              "https://catalog.example.com",
			CapabilitiesJSON: `{"commit":true,"branch-tag":true}`,
		},
	}
	cfg := api.DefaultConfig()
	cfg.Enable = true
	cfg.Write.EnableWrite = true
	cfg.Write.EnableMaintenance = true
	executor := NewMaintenanceProcedureExecutor(store, MaintenanceProcedureExecutorOptions{
		Config:         cfg,
		Account:        api.AccountConfig{AccountID: 7, Enable: true},
		CatalogFactory: &fakeMaintenanceCatalogFactory{client: &icebergcatalog.MockClient{}},
	})
	parsed, err := maintenance.ParseProcedureCall("iceberg_rewrite_manifests", "ksa_gold.sales.orders", "ref=tag:release")
	requireNoErr(t, err)
	_, err = executor.Execute(context.Background(), maintenance.ProcedureExecutionRequest{
		AccountID:   7,
		StatementID: "stmt-1",
		Parsed:      parsed,
	})
	if err == nil || !strings.Contains(err.Error(), "ICEBERG_UNSUPPORTED_FEATURE") || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("expected tag read-only error, got %v", err)
	}
	if len(store.jobs) != 0 || len(store.statuses) != 0 {
		t.Fatalf("tag rejection should happen before job insert, jobs=%+v statuses=%+v", store.jobs, store.statuses)
	}
}

func TestMaintenanceProcedureExecutorAllowsExplicitTagMove(t *testing.T) {
	store := &fakeMaintenanceStore{
		catalog: model.Catalog{
			AccountID:        7,
			CatalogID:        42,
			Name:             "ksa_gold",
			Type:             "rest",
			URI:              "https://catalog.example.com",
			CapabilitiesJSON: `{"commit":true,"branch-tag":true}`,
		},
	}
	var commitReq api.CommitRequest
	client := &icebergcatalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			return &api.LoadTableResponse{
				Namespace:        req.Namespace,
				TableName:        req.Table,
				MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
				MetadataJSON:     maintenanceTagMetadataJSON(),
			}, nil
		},
		CommitTableFunc: func(ctx context.Context, req api.CommitRequest) (*api.CommitResult, error) {
			commitReq = req
			return &api.CommitResult{SnapshotID: 7, CommitID: "commit-tag", Verified: true}, nil
		},
	}
	cfg := api.DefaultConfig()
	cfg.Enable = true
	cfg.Write.EnableWrite = true
	cfg.Write.EnableMaintenance = true
	executor := NewMaintenanceProcedureExecutor(store, MaintenanceProcedureExecutorOptions{
		Config:         cfg,
		Account:        api.AccountConfig{AccountID: 7, Enable: true},
		CatalogFactory: &fakeMaintenanceCatalogFactory{client: client},
	})
	parsed, err := maintenance.ParseProcedureCall("iceberg_rewrite_manifests", "ksa_gold.sales.orders", "ref=tag:release,allow_tag_move=true")
	requireNoErr(t, err)
	result, err := executor.Execute(context.Background(), maintenance.ProcedureExecutionRequest{
		AccountID:   7,
		StatementID: "stmt-1",
		Parsed:      parsed,
	})
	requireNoErr(t, err)
	if result.CommitID != "commit-tag" || result.SnapshotAfter != "7" {
		t.Fatalf("unexpected tag move result: %+v", result)
	}
	if commitReq.TargetRef != "release" || len(commitReq.Requirements) != 1 || commitReq.Requirements[0].Ref != "release" {
		t.Fatalf("tag ref was not normalized into commit request: %+v", commitReq)
	}
}

type fakeMaintenanceCatalogFactory struct {
	catalog model.Catalog
	client  api.CatalogClient
	err     error
}

func (f *fakeMaintenanceCatalogFactory) NewClient(ctx context.Context, catalog model.Catalog) (api.CatalogClient, error) {
	f.catalog = catalog
	if f.err != nil {
		return nil, f.err
	}
	return f.client, nil
}

type fakeMaintenanceStore struct {
	catalog           model.Catalog
	principalMaps     []model.PrincipalMap
	residencyPolicies []model.ResidencyPolicy
	jobs              []model.MaintenanceJob
	statuses          []string
	categories        []string
	orphans           []model.OrphanFile
}

func (s *fakeMaintenanceStore) GetCatalogByName(ctx context.Context, accountID uint32, name string) (model.Catalog, error) {
	if strings.TrimSpace(s.catalog.URI) == "" {
		s.catalog.URI = "https://catalog.example.com/rest"
	}
	if s.catalog.CatalogID == 0 {
		s.catalog.CatalogID = 7
	}
	if s.catalog.AccountID == 0 {
		s.catalog.AccountID = accountID
	}
	return s.catalog, nil
}

func (s *fakeMaintenanceStore) ListPrincipalMaps(ctx context.Context, accountID uint32, catalogID uint64) ([]model.PrincipalMap, error) {
	if s.principalMaps != nil {
		return append([]model.PrincipalMap(nil), s.principalMaps...), nil
	}
	return []model.PrincipalMap{{
		AccountID:         accountID,
		CatalogID:         catalogID,
		MORoleID:          model.PrincipalUnspecifiedID,
		MOUserID:          model.PrincipalUnspecifiedID,
		ExternalPrincipal: "test-principal",
	}}, nil
}

func (s *fakeMaintenanceStore) ListResidencyPolicies(ctx context.Context, accountID uint32, catalogID uint64) ([]model.ResidencyPolicy, error) {
	if s.residencyPolicies != nil {
		return append([]model.ResidencyPolicy(nil), s.residencyPolicies...), nil
	}
	catalogURI := strings.TrimSpace(s.catalog.URI)
	if catalogURI == "" {
		catalogURI = "https://catalog.example.com/rest"
	}
	return []model.ResidencyPolicy{{
		ScopeType:         model.ResidencyScopeCluster,
		AccountID:         0,
		CatalogID:         catalogID,
		AllowedCatalogURI: catalogURI,
		AllowedEndpoint:   "catalog.example.com",
		AllowedRegion:     model.ResidencyWildcard,
		AllowedBucket:     model.ResidencyWildcard,
		PolicyState:       model.ResidencyPolicyEnabled,
	}}, nil
}

func (s *fakeMaintenanceStore) InsertMaintenanceJob(ctx context.Context, job model.MaintenanceJob) error {
	s.jobs = append(s.jobs, job)
	return nil
}

func (s *fakeMaintenanceStore) UpdateMaintenanceJobStatus(ctx context.Context, accountID uint32, jobID, status, errorCategory string, snapshotAfter string, rewrittenFileCount, removedFileCount uint64, expectedVersion uint64) error {
	s.statuses = append(s.statuses, status)
	s.categories = append(s.categories, errorCategory)
	return nil
}

func (s *fakeMaintenanceStore) InsertOrphanFile(ctx context.Context, file model.OrphanFile) error {
	s.orphans = append(s.orphans, file)
	return nil
}

func requireMaintenanceStoreTransitions(t *testing.T, store *fakeMaintenanceStore, statuses, categories []string) {
	t.Helper()
	if !sameMaintenanceStrings(store.statuses, statuses) {
		t.Fatalf("unexpected statuses: got %v want %v", store.statuses, statuses)
	}
	if !sameMaintenanceStrings(store.categories, categories) {
		t.Fatalf("unexpected categories: got %v want %v", store.categories, categories)
	}
}

func sameMaintenanceStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for idx := range got {
		if got[idx] != want[idx] {
			return false
		}
	}
	return true
}

func writeMaintenanceMemoryFile(t *testing.T, ctx context.Context, fs fileservice.ETLFileService, path string, data []byte) {
	t.Helper()
	err := fs.Write(ctx, fileservice.IOVector{
		FilePath: path,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(data)),
			Data:   append([]byte(nil), data...),
		}},
	})
	requireNoErr(t, err)
}

func readMaintenanceMemoryFile(t *testing.T, ctx context.Context, fs fileservice.ETLFileService, path string) []byte {
	t.Helper()
	vec := fileservice.IOVector{
		FilePath: path,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: -1}},
	}
	err := fs.Read(ctx, &vec)
	requireNoErr(t, err)
	if len(vec.Entries) != 1 {
		t.Fatalf("expected one memory file entry, got %d", len(vec.Entries))
	}
	return append([]byte(nil), vec.Entries[0].Data...)
}

func maintenanceMemoryPath(location string) string {
	return strings.TrimPrefix(location, "s3://")
}

func maintenanceParquetDataFile(t *testing.T, values []int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	schema := parquet.NewSchema("iceberg", parquet.Group{
		"id": parquet.FieldID(parquet.Leaf(parquet.Int64Type), 1),
	})
	writer := parquet.NewWriter(&buf, schema)
	rows := make([]parquet.Row, len(values))
	for idx, value := range values {
		rows[idx] = parquet.Row{parquet.Int64Value(value).Level(0, 0, 0)}
	}
	_, err := writer.WriteRows(rows)
	requireNoErr(t, err)
	requireNoErr(t, writer.Close())
	return buf.Bytes()
}

func maintenanceParquetRowCount(t *testing.T, data []byte) int64 {
	t.Helper()
	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	requireNoErr(t, err)
	return file.NumRows()
}

func maintenanceRewriteDataEntry(path string, fileSize int64, rows int64) api.ManifestEntry {
	return api.ManifestEntry{
		Status:         api.ManifestEntryExisting,
		SnapshotID:     4,
		SequenceNumber: 4,
		FileSequence:   4,
		DataFile: api.DataFile{
			Content:         api.DataFileContentData,
			FilePath:        path,
			FileFormat:      "parquet",
			RecordCount:     rows,
			FileSizeInBytes: fileSize,
			SpecID:          0,
		},
	}
}

func maintenanceExpireMetadataJSON() []byte {
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

func maintenanceTagMetadataJSON() []byte {
	return []byte(`{
		"format-version": 2,
		"table-uuid": "uuid-1",
		"location": "s3://warehouse/orders",
		"current-schema-id": 1,
		"schemas": [{"schema-id": 1, "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]}],
		"default-spec-id": 0,
		"partition-specs": [{"spec-id": 0, "fields": []}],
		"current-snapshot-id": 4,
		"refs": {
			"main": {"snapshot-id": 4, "type": "branch"},
			"release": {"snapshot-id": 3, "type": "tag"}
		},
		"snapshots": [
			{"snapshot-id": 3, "parent-snapshot-id": 2, "timestamp-ms": 1767398400000, "manifest-list": "s3://warehouse/orders/metadata/snap-3.avro"},
			{"snapshot-id": 4, "parent-snapshot-id": 3, "timestamp-ms": 1767484800000, "manifest-list": "s3://warehouse/orders/metadata/snap-4.avro"}
		]
	}`)
}
