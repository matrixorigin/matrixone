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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergcatalog "github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/icebergwrite"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestDMLDeleteRuntimeFactoryLoadsTableAndBuildsCoordinator(t *testing.T) {
	ctx := context.Background()
	rawMeta := []byte(`{
		"format-version": 2,
		"table-uuid": "table-uuid",
		"location": "s3://warehouse/gold/orders",
		"last-sequence-number": 44,
		"current-schema-id": 9,
		"schemas": [
			{"schema-id": 8, "fields": [{"id": 1, "name": "old_id", "required": false, "type": "long"}]},
			{"schema-id": 9, "fields": [{"id": 1, "name": "id", "required": false, "type": "long"}]}
		],
		"default-spec-id": 0,
		"current-snapshot-id": 30,
		"snapshots": [{"snapshot-id": 30, "sequence-number": 44, "timestamp-ms": 1000, "schema-id": 9}],
		"refs": {"audit": {"snapshot-id": 30, "type": "branch"}}
	}`)
	store := &fakeDMLDeleteRuntimeStore{
		catalog: model.Catalog{
			AccountID:        42,
			CatalogID:        7,
			Name:             "ksa_gold",
			Type:             "rest",
			Warehouse:        "s3://warehouse",
			CapabilitiesJSON: `{"commit":true,"branch-tag":true}`,
		},
	}
	client := &icebergcatalog.MockClient{
		GetConfigFunc: func(ctx context.Context, req api.GetConfigRequest) (*api.ConfigResponse, error) {
			require.Equal(t, "s3://warehouse", req.Warehouse)
			require.Equal(t, "test-principal", req.ExternalPrincipal)
			return &api.ConfigResponse{Prefix: "main|s3://warehouse"}, nil
		},
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			require.Equal(t, uint32(42), req.Catalog.AccountID)
			require.Equal(t, "main|s3://warehouse", req.Prefix)
			require.Equal(t, "test-principal", req.ExternalPrincipal)
			require.Equal(t, api.Namespace{"sales"}, req.Namespace)
			require.Equal(t, "orders", req.Table)
			return &api.LoadTableResponse{
				MetadataLocation: "s3://warehouse/gold/orders/metadata/v1.json",
				MetadataJSON:     rawMeta,
				TableToken:       "table-token",
				Capabilities:     api.CatalogCapabilities{MetricsReport: true},
			}, nil
		},
	}
	cfg := api.DefaultConfig()
	cfg.Enable = true
	cfg.Write.EnableWrite = true
	cfg.Write.EnableDML = true
	cfg.Write.EnableDelete = true
	factory := NewDMLDeleteRuntimeCoordinatorFactory(DMLDeleteRuntimeCoordinatorFactoryOptions{
		Store:          store,
		CatalogFactory: staticCatalogFactory{client: client},
		Config:         cfg,
		Now:            func() time.Time { return time.Unix(0, 1) },
	})
	coord, err := factory.NewCoordinator(ctx, icebergwrite.AppendRequest{
		Operation:      icebergwrite.OperationDelete,
		AccountID:      42,
		StatementID:    "stmt-1",
		IdempotencyKey: "stmt-1",
		CatalogName:    "ksa_gold",
		Namespace:      "sales",
		Table:          "orders",
		DefaultRef:     "main",
		DMLScan: icebergwrite.DMLScanMetadata{
			BaseSnapshotID: 30,
			BaseSchemaID:   9,
			Ref:            "audit",
			ObjectIORef:    "iceberg-object-io://test",
			DataFiles: []api.DataFile{{
				FilePath:    "s3://warehouse/gold/orders/data/a.parquet",
				RecordCount: 10,
				SpecID:      3,
			}},
		},
	})
	require.NoError(t, err)
	dmlCoord, ok := coord.(*DMLDeleteCoordinator)
	require.True(t, ok)
	require.Equal(t, api.Namespace{"sales"}, dmlCoord.spec.Base.Namespace)
	require.Equal(t, "orders", dmlCoord.spec.Base.Table)
	require.Equal(t, "audit", dmlCoord.spec.Base.TargetRef)
	require.Equal(t, "stmt-1", dmlCoord.spec.Base.IdempotencyKey)
	require.Equal(t, int64(30), dmlCoord.spec.Base.BaseSnapshotID)
	require.Equal(t, "table-uuid", dmlCoord.spec.Base.TableUUID)
	require.True(t, dmlCoord.spec.Base.CatalogCapabilities.BranchTag)
	require.True(t, dmlCoord.spec.Base.CatalogCapabilities.MetricsReport)
	require.Equal(t, 9, dmlCoord.spec.Schema.SchemaID)
	require.Equal(t, 1, len(dmlCoord.spec.DataFiles))

	exec, ok := dmlCoord.spec.Committer.(DMLActionExecutor)
	require.True(t, ok)
	require.Equal(t, "s3://warehouse/gold/orders", exec.TableLocation)
	require.Equal(t, int64(31), exec.SnapshotID)
	require.Equal(t, int64(45), exec.SequenceNumber)
	require.Equal(t, "main|s3://warehouse", exec.Catalog.Prefix)
	require.Equal(t, "table-token", exec.Catalog.TableToken)
}

func TestDMLDeleteRuntimeFactoryLoadsTableAndBuildsUpdateCoordinator(t *testing.T) {
	ctx := context.Background()
	rawMeta := []byte(`{
		"format-version": 2,
		"table-uuid": "table-uuid",
		"location": "s3://warehouse/gold/orders",
		"last-sequence-number": 44,
		"current-schema-id": 9,
		"schemas": [
			{"schema-id": 9, "fields": [{"id": 1, "name": "id", "required": false, "type": "long"}]}
		],
		"default-spec-id": 0,
		"current-snapshot-id": 30,
		"snapshots": [{"snapshot-id": 30, "sequence-number": 44, "timestamp-ms": 1000, "schema-id": 9}],
		"refs": {"audit": {"snapshot-id": 30, "type": "branch"}}
	}`)
	store := &fakeDMLDeleteRuntimeStore{
		catalog: model.Catalog{
			AccountID:        42,
			CatalogID:        7,
			Name:             "ksa_gold",
			Type:             "rest",
			CapabilitiesJSON: `{"commit":true,"branch-tag":true}`,
		},
	}
	client := &icebergcatalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			require.Equal(t, api.Namespace{"sales"}, req.Namespace)
			require.Equal(t, "orders", req.Table)
			return &api.LoadTableResponse{
				MetadataLocation: "s3://warehouse/gold/orders/metadata/v1.json",
				MetadataJSON:     rawMeta,
				TableToken:       "table-token",
			}, nil
		},
	}
	cfg := api.DefaultConfig()
	cfg.Enable = true
	cfg.Write.EnableWrite = true
	cfg.Write.EnableDML = true
	cfg.Write.EnableDelete = true
	factory := NewDMLDeleteRuntimeCoordinatorFactory(DMLDeleteRuntimeCoordinatorFactoryOptions{
		Store:          store,
		CatalogFactory: staticCatalogFactory{client: client},
		Config:         cfg,
		Now:            func() time.Time { return time.Unix(0, 1) },
	})
	coord, err := factory.NewCoordinator(ctx, icebergwrite.AppendRequest{
		Operation:      icebergwrite.OperationUpdate,
		AccountID:      42,
		StatementID:    "stmt-1",
		IdempotencyKey: "stmt-1",
		CatalogName:    "ksa_gold",
		Namespace:      "sales",
		Table:          "orders",
		DefaultRef:     "main",
		DMLScan: icebergwrite.DMLScanMetadata{
			BaseSnapshotID: 30,
			BaseSchemaID:   9,
			Ref:            "audit",
			ObjectIORef:    "iceberg-object-io://test",
			DataFiles: []api.DataFile{{
				FilePath:    "s3://warehouse/gold/orders/data/a.parquet",
				RecordCount: 10,
				SpecID:      3,
			}},
		},
	})
	require.NoError(t, err)
	updateCoord, ok := coord.(*DMLUpdateCoordinator)
	require.True(t, ok)
	require.Equal(t, api.Namespace{"sales"}, updateCoord.spec.Base.Namespace)
	require.Equal(t, "orders", updateCoord.spec.Base.Table)
	require.Equal(t, "audit", updateCoord.spec.Base.TargetRef)
	require.Equal(t, int64(30), updateCoord.spec.Base.BaseSnapshotID)
	require.Equal(t, "table-uuid", updateCoord.spec.Base.TableUUID)
	require.Equal(t, 9, updateCoord.spec.Schema.SchemaID)
	require.Equal(t, 1, len(updateCoord.spec.DataFiles))
	exec, ok := updateCoord.spec.Committer.(DMLActionExecutor)
	require.True(t, ok)
	require.Equal(t, int64(31), exec.SnapshotID)
	require.Equal(t, int64(45), exec.SequenceNumber)
	require.Equal(t, "table-token", exec.Catalog.TableToken)
}

func TestDMLDeleteRuntimeFactoryLoadsTableAndBuildsMergeCoordinator(t *testing.T) {
	ctx := context.Background()
	rawMeta := []byte(`{
		"format-version": 2,
		"table-uuid": "table-uuid",
		"location": "s3://warehouse/gold/orders",
		"last-sequence-number": 44,
		"current-schema-id": 9,
		"schemas": [
			{"schema-id": 9, "fields": [
				{"id": 1, "name": "id", "required": false, "type": "long"},
				{"id": 2, "name": "name", "required": false, "type": "string"}
			]}
		],
		"default-spec-id": 0,
		"current-snapshot-id": 30,
		"snapshots": [{"snapshot-id": 30, "sequence-number": 44, "timestamp-ms": 1000, "schema-id": 9}],
		"refs": {"audit": {"snapshot-id": 30, "type": "branch"}}
	}`)
	store := &fakeDMLDeleteRuntimeStore{
		catalog: model.Catalog{
			AccountID:        42,
			CatalogID:        7,
			Name:             "ksa_gold",
			Type:             "rest",
			CapabilitiesJSON: `{"commit":true,"branch-tag":true}`,
		},
	}
	client := &icebergcatalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			require.Equal(t, api.Namespace{"sales"}, req.Namespace)
			require.Equal(t, "orders", req.Table)
			return &api.LoadTableResponse{
				MetadataLocation: "s3://warehouse/gold/orders/metadata/v1.json",
				MetadataJSON:     rawMeta,
				TableToken:       "table-token",
			}, nil
		},
	}
	cfg := api.DefaultConfig()
	cfg.Enable = true
	cfg.Write.EnableWrite = true
	cfg.Write.EnableDML = true
	cfg.Write.EnableDelete = true
	factory := NewDMLDeleteRuntimeCoordinatorFactory(DMLDeleteRuntimeCoordinatorFactoryOptions{
		Store:          store,
		CatalogFactory: staticCatalogFactory{client: client},
		Config:         cfg,
		Now:            func() time.Time { return time.Unix(0, 1) },
	})
	coord, err := factory.NewCoordinator(ctx, icebergwrite.AppendRequest{
		Operation:      icebergwrite.OperationMerge,
		AccountID:      42,
		StatementID:    "stmt-merge",
		IdempotencyKey: "stmt-merge",
		CatalogName:    "ksa_gold",
		Namespace:      "sales",
		Table:          "orders",
		DefaultRef:     "main",
		DMLScan: icebergwrite.DMLScanMetadata{
			BaseSnapshotID: 30,
			BaseSchemaID:   9,
			Ref:            "audit",
			ObjectIORef:    "iceberg-object-io://test",
			DataFiles: []api.DataFile{{
				FilePath:    "s3://warehouse/gold/orders/data/a.parquet",
				RecordCount: 10,
				SpecID:      3,
			}},
		},
	})
	require.NoError(t, err)
	mergeCoord, ok := coord.(*DMLMergeCoordinator)
	require.True(t, ok)
	require.Equal(t, api.Namespace{"sales"}, mergeCoord.spec.Base.Namespace)
	require.Equal(t, "orders", mergeCoord.spec.Base.Table)
	require.Equal(t, "audit", mergeCoord.spec.Base.TargetRef)
	require.Equal(t, int64(30), mergeCoord.spec.Base.BaseSnapshotID)
	require.Equal(t, "table-uuid", mergeCoord.spec.Base.TableUUID)
	require.Equal(t, 9, mergeCoord.spec.Schema.SchemaID)
	require.Equal(t, 1, len(mergeCoord.spec.DataFiles))
	exec, ok := mergeCoord.spec.Committer.(DMLActionExecutor)
	require.True(t, ok)
	require.Equal(t, int64(31), exec.SnapshotID)
	require.Equal(t, int64(45), exec.SequenceNumber)
	require.Equal(t, "table-token", exec.Catalog.TableToken)
}

func TestDMLDeleteRuntimeFactoryLoadsTableAndBuildsOverwriteCoordinator(t *testing.T) {
	ctx := context.Background()
	rawMeta := []byte(`{
		"format-version": 2,
		"table-uuid": "table-uuid",
		"location": "s3://warehouse/gold/orders",
		"last-sequence-number": 44,
		"current-schema-id": 9,
		"schemas": [
			{"schema-id": 9, "fields": [{"id": 1, "name": "id", "required": false, "type": "long"}]}
		],
		"default-spec-id": 0,
		"partition-specs": [{"spec-id": 0, "fields": [{"source-id": 1, "field-id": 1000, "name": "region", "transform": "identity"}]}],
		"current-snapshot-id": 30,
		"snapshots": [{"snapshot-id": 30, "sequence-number": 44, "timestamp-ms": 1000, "schema-id": 9}],
		"refs": {"main": {"snapshot-id": 30, "type": "branch"}}
	}`)
	store := &fakeDMLDeleteRuntimeStore{
		catalog: model.Catalog{
			AccountID:        42,
			CatalogID:        7,
			Name:             "ksa_gold",
			Type:             "rest",
			CapabilitiesJSON: `{"commit":true}`,
		},
	}
	client := &icebergcatalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			require.Equal(t, api.Namespace{"sales"}, req.Namespace)
			require.Equal(t, "orders", req.Table)
			return &api.LoadTableResponse{
				MetadataLocation: "s3://warehouse/gold/orders/metadata/v1.json",
				MetadataJSON:     rawMeta,
				TableToken:       "table-token",
			}, nil
		},
	}
	cfg := api.DefaultConfig()
	cfg.Enable = true
	cfg.Write.EnableWrite = true
	cfg.Write.EnableDML = true
	cfg.Write.EnableDelete = true
	factory := NewDMLDeleteRuntimeCoordinatorFactory(DMLDeleteRuntimeCoordinatorFactoryOptions{
		Store:          store,
		CatalogFactory: staticCatalogFactory{client: client},
		Config:         cfg,
		Now:            func() time.Time { return time.Unix(0, 1) },
	})
	coord, err := factory.NewCoordinator(ctx, icebergwrite.AppendRequest{
		Operation:      icebergwrite.OperationOverwrite,
		AccountID:      42,
		StatementID:    "stmt-1",
		IdempotencyKey: "stmt-1",
		CatalogName:    "ksa_gold",
		Namespace:      "sales",
		Table:          "orders",
		DefaultRef:     "main",
		DMLScan: icebergwrite.DMLScanMetadata{
			BaseSnapshotID: 30,
			BaseSchemaID:   9,
			Ref:            "main",
			ObjectIORef:    "iceberg-object-io://test",
			OverwriteScope: string(dml.OverwritePartition),
			OverwritePartition: map[string]any{
				"region": "ksa",
			},
			DataFiles: []api.DataFile{{
				FilePath:    "s3://warehouse/gold/orders/data/a.parquet",
				RecordCount: 10,
				SpecID:      3,
				Partition:   map[string]any{"region": "ksa"},
			}, {
				FilePath:    "s3://warehouse/gold/orders/data/b.parquet",
				RecordCount: 10,
				SpecID:      3,
				Partition:   map[string]any{"region": "uae"},
			}},
		},
	})
	require.NoError(t, err)
	overwriteCoord := requireDMLOverwriteCoordinator(t, coord)
	require.Equal(t, api.Namespace{"sales"}, overwriteCoord.spec.Base.Namespace)
	require.Equal(t, "orders", overwriteCoord.spec.Base.Table)
	require.Equal(t, "main", overwriteCoord.spec.Base.TargetRef)
	require.Equal(t, int64(30), overwriteCoord.spec.Base.BaseSnapshotID)
	require.Equal(t, "table-uuid", overwriteCoord.spec.Base.TableUUID)
	require.Equal(t, 9, overwriteCoord.spec.Schema.SchemaID)
	require.Equal(t, 1, len(overwriteCoord.spec.AffectedDataFiles))
	require.Equal(t, "s3://warehouse/gold/orders/data/a.parquet", overwriteCoord.spec.AffectedDataFiles[0].FilePath)
	require.Equal(t, dml.OverwritePartition, overwriteCoord.spec.Scope)
	require.Equal(t, "ksa", overwriteCoord.spec.Partition["region"])
	exec, ok := overwriteCoord.spec.Committer.(DMLActionExecutor)
	require.True(t, ok)
	require.Equal(t, int64(31), exec.SnapshotID)
	require.Equal(t, int64(45), exec.SequenceNumber)
	require.Equal(t, "table-token", exec.Catalog.TableToken)
}

func TestDMLDeleteRuntimeFactoryAllowsSystemAccountZero(t *testing.T) {
	ctx := context.Background()
	rawMeta := []byte(`{
		"format-version": 2,
		"table-uuid": "table-uuid",
		"location": "s3://warehouse/gold/orders",
		"last-sequence-number": 44,
		"current-schema-id": 9,
		"schemas": [
			{"schema-id": 9, "fields": [{"id": 1, "name": "id", "required": false, "type": "long"}]}
		],
		"default-spec-id": 0,
		"partition-specs": [{"spec-id": 0, "fields": [{"source-id": 1, "field-id": 1000, "name": "region", "transform": "identity"}]}],
		"current-snapshot-id": 30,
		"snapshots": [{"snapshot-id": 30, "sequence-number": 44, "timestamp-ms": 1000, "schema-id": 9}],
		"refs": {"main": {"snapshot-id": 30, "type": "branch"}}
	}`)
	store := &fakeDMLDeleteRuntimeStore{
		catalog: model.Catalog{
			AccountID:        0,
			CatalogID:        7,
			Name:             "local",
			Type:             "rest",
			CapabilitiesJSON: `{"commit":true}`,
		},
	}
	client := &icebergcatalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			require.Equal(t, uint32(0), req.Catalog.AccountID)
			require.Equal(t, api.Namespace{"sales"}, req.Namespace)
			require.Equal(t, "orders", req.Table)
			return &api.LoadTableResponse{
				MetadataLocation: "s3://warehouse/gold/orders/metadata/v1.json",
				MetadataJSON:     rawMeta,
				TableToken:       "table-token",
			}, nil
		},
	}
	cfg := api.DefaultConfig()
	cfg.Enable = true
	cfg.Write.EnableWrite = true
	cfg.Write.EnableDML = true
	cfg.Write.EnableDelete = true
	factory := NewDMLDeleteRuntimeCoordinatorFactory(DMLDeleteRuntimeCoordinatorFactoryOptions{
		Store:          store,
		CatalogFactory: staticCatalogFactory{client: client},
		Config:         cfg,
		Now:            func() time.Time { return time.Unix(0, 1) },
	})
	coord, err := factory.NewCoordinator(ctx, icebergwrite.AppendRequest{
		Operation:      icebergwrite.OperationOverwrite,
		AccountID:      0,
		StatementID:    "stmt-1",
		IdempotencyKey: "stmt-1",
		CatalogName:    "local",
		Namespace:      "sales",
		Table:          "orders",
		DefaultRef:     "main",
		DMLScan: icebergwrite.DMLScanMetadata{
			BaseSnapshotID: 30,
			BaseSchemaID:   9,
			Ref:            "main",
			ObjectIORef:    "iceberg-object-io://test",
			OverwriteScope: string(dml.OverwritePartition),
			OverwritePartition: map[string]any{
				"region": "ksa",
			},
		},
	})
	require.NoError(t, err)
	overwriteCoord := requireDMLOverwriteCoordinator(t, coord)
	require.Equal(t, "orders", overwriteCoord.spec.Base.Table)
	exec, ok := overwriteCoord.spec.Committer.(DMLActionExecutor)
	require.True(t, ok)
	require.Equal(t, uint32(0), exec.Catalog.Catalog.AccountID)
	require.Equal(t, "table-token", exec.Catalog.TableToken)
}

func TestDMLDeleteRuntimeFactoryUsesWriteObjectProviderForOverwrite(t *testing.T) {
	ctx := context.Background()
	rawMeta := []byte(`{
		"format-version": 2,
		"table-uuid": "table-uuid",
		"location": "s3://warehouse/gold/orders",
		"last-sequence-number": 44,
		"current-schema-id": 9,
		"schemas": [
			{"schema-id": 9, "fields": [{"id": 1, "name": "id", "required": false, "type": "long"}]}
		],
		"default-spec-id": 0,
		"partition-specs": [{"spec-id": 0, "fields": [{"source-id": 1, "field-id": 1000, "name": "id", "transform": "identity"}]}],
		"current-snapshot-id": 30,
		"snapshots": [{"snapshot-id": 30, "sequence-number": 44, "timestamp-ms": 1000, "schema-id": 9}],
		"refs": {"main": {"snapshot-id": 30, "type": "branch"}}
	}`)
	store := &fakeDMLDeleteRuntimeStore{
		catalog: model.Catalog{
			AccountID:        42,
			CatalogID:        7,
			Name:             "ksa_gold",
			Type:             "rest",
			CapabilitiesJSON: `{"commit":true}`,
		},
	}
	client := &icebergcatalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			return &api.LoadTableResponse{
				MetadataLocation: "s3://warehouse/gold/orders/metadata/v1.json",
				MetadataJSON:     rawMeta,
				TableToken:       "table-token",
			}, nil
		},
	}
	fs, err := fileservice.NewMemoryFS("iceberg-dml-write-provider", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	scopeForLocation := func(location string) icebergio.ObjectScope {
		return icebergio.ObjectScope{
			AccountID:       42,
			CatalogID:       7,
			Endpoint:        "s3.me-central-1.amazonaws.com",
			Region:          "me-central-1",
			Bucket:          "warehouse",
			Principal:       "dml-test",
			StorageLocation: dmlRuntimeMemoryPath(location),
		}
	}
	cfg := api.DefaultConfig()
	cfg.Enable = true
	cfg.Write.EnableWrite = true
	cfg.Write.EnableDML = true
	cfg.Write.EnableDelete = true
	factory := NewDMLDeleteRuntimeCoordinatorFactory(DMLDeleteRuntimeCoordinatorFactoryOptions{
		Store:            store,
		CatalogFactory:   staticCatalogFactory{client: client},
		Config:           cfg,
		Now:              func() time.Time { return time.Unix(0, 1) },
		ObjectIOProvider: icebergio.ScopedProvider{FileService: fs},
		ScopeForLocation: scopeForLocation,
	})
	coord, err := factory.NewCoordinator(ctx, icebergwrite.AppendRequest{
		Operation:      icebergwrite.OperationOverwrite,
		AccountID:      42,
		StatementID:    "stmt-1",
		IdempotencyKey: "stmt-1",
		CatalogName:    "ksa_gold",
		Namespace:      "sales",
		Table:          "orders",
		DefaultRef:     "main",
		DMLScan: icebergwrite.DMLScanMetadata{
			BaseSnapshotID: 30,
			BaseSchemaID:   9,
			Ref:            "main",
			ObjectIORef:    "iceberg-object-io://read-only",
			OverwriteScope: string(dml.OverwritePartition),
			OverwritePartition: map[string]any{
				"id": int64(1),
			},
		},
	})
	require.NoError(t, err)
	overwriteCoord := requireDMLOverwriteCoordinator(t, coord)
	_, ok := overwriteCoord.spec.ObjectWriter.(icebergio.ProviderObjectWriter)
	require.True(t, ok)
	exec, ok := overwriteCoord.spec.Committer.(DMLActionExecutor)
	require.True(t, ok)
	_, ok = exec.Workflow.ManifestWriter.(icebergio.ProviderObjectWriter)
	require.True(t, ok)
}

func TestDMLDeleteRuntimeFactoryAllowsSchemaIDZeroAndPreservesBaseManifests(t *testing.T) {
	ctx := context.Background()
	manifestListLocation := "s3://warehouse/gold/orders/metadata/snap-30.avro"
	baseManifestLocation := "s3://warehouse/gold/orders/metadata/base-manifest.avro"
	baseManifestBytes, err := metadata.EncodeManifest([]api.ManifestEntry{{
		Status:     api.ManifestEntryAdded,
		SnapshotID: 30,
		DataFile: api.DataFile{
			Content:     api.DataFileContentData,
			FilePath:    "s3://warehouse/gold/orders/data/a.parquet",
			FileFormat:  "parquet",
			RecordCount: 10,
			SpecID:      0,
		},
	}})
	require.NoError(t, err)
	manifestListBytes, err := metadata.EncodeManifestList([]api.ManifestFile{{
		Path:            baseManifestLocation,
		Length:          int64(len(baseManifestBytes)),
		Content:         api.ManifestContentData,
		AddedSnapshotID: 30,
	}})
	require.NoError(t, err)
	fs, objectRef := registerDMLRuntimeMemoryObjectIO(t, ctx)
	writeDMLRuntimeMemoryFile(t, ctx, fs, manifestListLocation, manifestListBytes)
	writeDMLRuntimeMemoryFile(t, ctx, fs, baseManifestLocation, baseManifestBytes)
	rawMeta := []byte(`{
		"format-version": 2,
		"table-uuid": "table-uuid",
		"location": "s3://warehouse/gold/orders",
		"last-sequence-number": 44,
		"current-schema-id": 0,
		"schemas": [
			{"schema-id": 0, "fields": [{"id": 1, "name": "id", "required": false, "type": "long"}]}
		],
		"default-spec-id": 0,
		"partition-specs": [{"spec-id": 0, "fields": [{"source-id": 1, "field-id": 1000, "name": "id", "transform": "identity"}]}],
		"current-snapshot-id": 30,
		"snapshots": [{"snapshot-id": 30, "sequence-number": 44, "timestamp-ms": 1000, "schema-id": 0, "manifest-list": "s3://warehouse/gold/orders/metadata/snap-30.avro"}],
		"refs": {"main": {"snapshot-id": 30, "type": "branch"}}
	}`)
	factory := dmlRuntimeFactoryForRawMetadata(t, rawMeta, api.CatalogCapabilities{})
	coord, err := factory.NewCoordinator(ctx, icebergwrite.AppendRequest{
		Operation:      icebergwrite.OperationDelete,
		AccountID:      42,
		StatementID:    "stmt-1",
		IdempotencyKey: "stmt-1",
		CatalogName:    "ksa_gold",
		Namespace:      "sales",
		Table:          "orders",
		DefaultRef:     "main",
		DMLScan: icebergwrite.DMLScanMetadata{
			BaseSnapshotID: 30,
			BaseSchemaID:   0,
			Ref:            "main",
			ObjectIORef:    objectRef,
			DataFiles: []api.DataFile{{
				FilePath:    "s3://warehouse/gold/orders/data/a.parquet",
				RecordCount: 10,
				SpecID:      0,
			}},
		},
	})
	require.NoError(t, err)
	dmlCoord, ok := coord.(*DMLDeleteCoordinator)
	require.True(t, ok)
	require.Equal(t, 0, dmlCoord.spec.Base.BaseSchemaID)
	require.Equal(t, 0, dmlCoord.spec.Base.BaseSpecID)
	exec, ok := dmlCoord.spec.Committer.(DMLActionExecutor)
	require.True(t, ok)
	require.Len(t, exec.PreservedManifests, 1)
	require.Equal(t, baseManifestLocation, exec.PreservedManifests[0].Path)
	require.Len(t, exec.PreservedSources, 1)
	require.Equal(t, baseManifestLocation, exec.PreservedSources[0].Manifest.Path)
	require.Equal(t, "s3://warehouse/gold/orders/data/a.parquet", exec.PreservedSources[0].Entries[0].DataFile.FilePath)
}

func TestDMLDeleteRuntimeFactoryRejectsBareTagRefFromMetadata(t *testing.T) {
	ctx := context.Background()
	rawMeta := []byte(`{
		"format-version": 2,
		"table-uuid": "table-uuid",
		"location": "s3://warehouse/gold/orders",
		"last-sequence-number": 44,
		"current-schema-id": 9,
		"schemas": [
			{"schema-id": 9, "fields": [{"id": 1, "name": "id", "required": false, "type": "long"}]}
		],
		"default-spec-id": 0,
		"current-snapshot-id": 30,
		"snapshots": [{"snapshot-id": 30, "sequence-number": 44, "timestamp-ms": 1000, "schema-id": 9}],
		"refs": {"release_tag": {"snapshot-id": 30, "type": "tag"}}
	}`)
	factory := dmlRuntimeFactoryForRawMetadata(t, rawMeta, api.CatalogCapabilities{BranchTag: true})
	_, err := factory.NewCoordinator(ctx, icebergwrite.AppendRequest{
		Operation:      icebergwrite.OperationDelete,
		AccountID:      42,
		StatementID:    "stmt-1",
		IdempotencyKey: "stmt-1",
		CatalogName:    "ksa_gold",
		Namespace:      "sales",
		Table:          "orders",
		DefaultRef:     "main",
		DMLScan: icebergwrite.DMLScanMetadata{
			BaseSnapshotID: 30,
			BaseSchemaID:   9,
			Ref:            "release_tag",
			ObjectIORef:    "iceberg-object-io://test",
			DataFiles: []api.DataFile{{
				FilePath:    "s3://warehouse/gold/orders/data/a.parquet",
				RecordCount: 10,
				SpecID:      0,
			}},
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrUnsupportedFeature))
	require.Contains(t, err.Error(), "tag refs are read-only")
}

func TestDMLDeleteRuntimeFactorySharesOverwriteCoordinatorByStatement(t *testing.T) {
	ctx := context.Background()
	rawMeta := []byte(`{
		"format-version": 2,
		"table-uuid": "table-uuid",
		"location": "s3://warehouse/gold/orders",
		"last-sequence-number": 44,
		"current-schema-id": 9,
		"schemas": [
			{"schema-id": 9, "fields": [{"id": 1, "name": "id", "required": false, "type": "long"}]}
		],
		"default-spec-id": 0,
		"partition-specs": [{"spec-id": 0, "fields": [{"source-id": 1, "field-id": 1000, "name": "region", "transform": "identity"}]}],
		"current-snapshot-id": 30,
		"snapshots": [{"snapshot-id": 30, "sequence-number": 44, "timestamp-ms": 1000, "schema-id": 9}],
		"refs": {"main": {"snapshot-id": 30, "type": "branch"}}
	}`)
	factory := dmlRuntimeFactoryForRawMetadata(t, rawMeta, api.CatalogCapabilities{})
	req := icebergwrite.AppendRequest{
		Operation:      icebergwrite.OperationOverwrite,
		AccountID:      42,
		StatementID:    "stmt-shared",
		IdempotencyKey: "stmt-shared",
		CatalogName:    "ksa_gold",
		Namespace:      "sales",
		Table:          "orders",
		DefaultRef:     "main",
		DMLScan: icebergwrite.DMLScanMetadata{
			BaseSnapshotID: 30,
			BaseSchemaID:   9,
			Ref:            "main",
			ObjectIORef:    "iceberg-object-io://test",
			OverwriteScope: string(dml.OverwritePartition),
			OverwritePartition: map[string]any{
				"region": "ksa",
			},
		},
	}
	first, err := factory.NewCoordinator(ctx, req)
	require.NoError(t, err)
	second, err := factory.NewCoordinator(ctx, req)
	require.NoError(t, err)
	require.Same(t, first, second)
	requireDMLOverwriteCoordinator(t, first)
}

func TestDMLDeleteRuntimeFactoryFallsBackForAppendAndValidatesDeleteConfig(t *testing.T) {
	factory := NewDMLDeleteRuntimeCoordinatorFactory(DMLDeleteRuntimeCoordinatorFactoryOptions{})
	coord, err := factory.NewCoordinator(context.Background(), icebergwrite.AppendRequest{Operation: icebergwrite.OperationAppend})
	require.NoError(t, err)
	require.Nil(t, coord)

	_, err = factory.NewCoordinator(context.Background(), icebergwrite.AppendRequest{Operation: icebergwrite.OperationDelete})
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires a store")

	fromInternal := NewDMLDeleteRuntimeCoordinatorFactoryFromInternalSQLExecutor(nil, DMLDeleteRuntimeCoordinatorFactoryOptions{})
	require.NotNil(t, fromInternal.opts.Store)
	require.True(t, factory.requireResidencyPolicy())
	objectIOFactory := NewDMLDeleteRuntimeCoordinatorFactory(DMLDeleteRuntimeCoordinatorFactoryOptions{
		ObjectIOProvider: icebergio.ScopedProvider{},
	})
	require.False(t, objectIOFactory.requireResidencyPolicy())
}

func TestDMLRuntimeCoordinatorCacheLifecycle(t *testing.T) {
	ctx := context.Background()
	cache := &dmlRuntimeCoordinatorCache{entries: make(map[string]icebergwrite.Coordinator)}
	inner := &countingDMLRuntimeCoordinator{}
	first, err := cache.getOrCreate(ctx, "stmt", func() (icebergwrite.Coordinator, error) {
		return inner, nil
	})
	require.NoError(t, err)
	second, err := cache.getOrCreate(ctx, "stmt", func() (icebergwrite.Coordinator, error) {
		t.Fatal("cache should reuse the existing coordinator")
		return nil, nil
	})
	require.NoError(t, err)
	require.Same(t, first, second)
	require.Len(t, cache.entries, 1)

	require.NoError(t, first.Begin(ctx, icebergwrite.AppendRequest{StatementID: "stmt"}))
	require.NoError(t, first.Append(ctx, nil))
	require.NoError(t, first.(*dmlRuntimeSharedCoordinator).AppendWithProcess(nil, nil))
	require.Equal(t, 2, inner.appendCalls)
	require.NoError(t, first.Commit(ctx))
	require.Len(t, cache.entries, 0)

	third, err := cache.getOrCreate(ctx, "stmt", func() (icebergwrite.Coordinator, error) {
		return &countingDMLRuntimeCoordinator{}, nil
	})
	require.NoError(t, err)
	require.Len(t, cache.entries, 1)
	require.NoError(t, third.Abort(ctx, context.Canceled))
	require.Len(t, cache.entries, 0)
}

func TestDMLRuntimeSharedCoordinatorProcessAwareAppend(t *testing.T) {
	inner := &processAwareDMLRuntimeCoordinator{}
	shared := &dmlRuntimeSharedCoordinator{inner: inner}
	require.NoError(t, shared.AppendWithProcess(nil, nil))
	require.True(t, inner.processAwareCalled)
}

func TestObjectIORefDMLObjectWriterWritesAndReads(t *testing.T) {
	ctx := context.Background()
	fs, ref := registerDMLRuntimeMemoryObjectIO(t, ctx)
	writer := objectIORefDMLObjectWriter{ObjectIORef: ref}
	location := "s3://warehouse/gold/orders/delete/pos.parquet"

	require.Error(t, writer.WriteObject(ctx, "", []byte("payload")))
	require.Error(t, writer.WriteObject(ctx, location, nil))
	require.NoError(t, writer.WriteObject(ctx, location, []byte("payload")))
	read, err := writer.ReadManifestObject(ctx, location)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), read)

	manifestLocation := "s3://warehouse/gold/orders/metadata/delete-manifest.avro"
	require.NoError(t, writer.WriteManifestObject(ctx, manifestLocation, []byte("manifest")))
	manifest, err := writer.ReadManifestObject(ctx, manifestLocation)
	require.NoError(t, err)
	require.Equal(t, []byte("manifest"), manifest)

	vec := fileservice.IOVector{
		FilePath: dmlRuntimeMemoryPath(location),
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: -1}},
	}
	require.NoError(t, fs.Read(ctx, &vec))
	require.Equal(t, []byte("payload"), vec.Entries[0].Data)
}

func requireDMLOverwriteCoordinator(t *testing.T, coord icebergwrite.Coordinator) *DMLOverwriteCoordinator {
	t.Helper()
	if shared, ok := coord.(*dmlRuntimeSharedCoordinator); ok {
		coord = shared.inner
	}
	overwriteCoord, ok := coord.(*DMLOverwriteCoordinator)
	require.True(t, ok)
	return overwriteCoord
}

type fakeDMLDeleteRuntimeStore struct {
	catalog           model.Catalog
	principalMaps     []model.PrincipalMap
	residencyPolicies []model.ResidencyPolicy
}

func (s *fakeDMLDeleteRuntimeStore) GetCatalogByName(ctx context.Context, accountID uint32, name string) (model.Catalog, error) {
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

func (s *fakeDMLDeleteRuntimeStore) ListPrincipalMaps(ctx context.Context, accountID uint32, catalogID uint64) ([]model.PrincipalMap, error) {
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

func (s *fakeDMLDeleteRuntimeStore) ListResidencyPolicies(ctx context.Context, accountID uint32, catalogID uint64) ([]model.ResidencyPolicy, error) {
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

func (s *fakeDMLDeleteRuntimeStore) InsertPublishJob(ctx context.Context, job model.PublishJob) error {
	return nil
}

func (s *fakeDMLDeleteRuntimeStore) InsertOrphanFile(ctx context.Context, file model.OrphanFile) error {
	return nil
}

type staticCatalogFactory struct {
	client api.CatalogClient
}

func (f staticCatalogFactory) NewClient(ctx context.Context, catalog model.Catalog) (api.CatalogClient, error) {
	return f.client, nil
}

func dmlRuntimeFactoryForRawMetadata(t *testing.T, rawMeta []byte, caps api.CatalogCapabilities) DMLDeleteRuntimeCoordinatorFactory {
	t.Helper()
	store := &fakeDMLDeleteRuntimeStore{
		catalog: model.Catalog{
			AccountID:        42,
			CatalogID:        7,
			Name:             "ksa_gold",
			Type:             "rest",
			CapabilitiesJSON: `{"commit":true,"branch-tag":true}`,
		},
	}
	client := &icebergcatalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			return &api.LoadTableResponse{
				MetadataLocation: "s3://warehouse/gold/orders/metadata/v1.json",
				MetadataJSON:     rawMeta,
				TableToken:       "table-token",
				Capabilities:     caps,
			}, nil
		},
	}
	cfg := api.DefaultConfig()
	cfg.Enable = true
	cfg.Write.EnableWrite = true
	cfg.Write.EnableDML = true
	cfg.Write.EnableDelete = true
	return NewDMLDeleteRuntimeCoordinatorFactory(DMLDeleteRuntimeCoordinatorFactoryOptions{
		Store:          store,
		CatalogFactory: staticCatalogFactory{client: client},
		Config:         cfg,
		Now:            func() time.Time { return time.Unix(0, 1) },
	})
}

func registerDMLRuntimeMemoryObjectIO(t *testing.T, ctx context.Context) (fileservice.ETLFileService, string) {
	t.Helper()
	fs, err := fileservice.NewMemoryFS("iceberg-dml-runtime", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	ref, err := icebergio.RegisterObjectIOProvider(ctx, icebergio.ScopedProvider{FileService: fs}, func(location string) icebergio.ObjectScope {
		return icebergio.ObjectScope{
			AccountID:       42,
			CatalogID:       7,
			Endpoint:        "s3.me-central-1.amazonaws.com",
			Region:          "me-central-1",
			Bucket:          "warehouse",
			Principal:       "ksa-analytics",
			StorageLocation: dmlRuntimeMemoryPath(location),
		}
	}, time.Minute)
	require.NoError(t, err)
	t.Cleanup(func() { icebergio.ReleaseObjectIORef(ref) })
	return fs, ref
}

func writeDMLRuntimeMemoryFile(t *testing.T, ctx context.Context, fs fileservice.ETLFileService, location string, payload []byte) {
	t.Helper()
	err := fs.Write(ctx, fileservice.IOVector{
		FilePath: dmlRuntimeMemoryPath(location),
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(payload)),
			Data:   append([]byte(nil), payload...),
		}},
	})
	require.NoError(t, err)
}

func dmlRuntimeMemoryPath(location string) string {
	return strings.TrimPrefix(location, "s3://")
}

type countingDMLRuntimeCoordinator struct {
	appendCalls int
	committed   bool
}

func (c *countingDMLRuntimeCoordinator) Begin(context.Context, icebergwrite.AppendRequest) error {
	return nil
}

func (c *countingDMLRuntimeCoordinator) Append(context.Context, *batch.Batch) error {
	c.appendCalls++
	return nil
}

func (c *countingDMLRuntimeCoordinator) Commit(context.Context) error {
	c.committed = true
	return nil
}

func (c *countingDMLRuntimeCoordinator) Abort(context.Context, error) error {
	return nil
}

func (c *countingDMLRuntimeCoordinator) CommitAttempted() bool {
	return c.committed
}

type processAwareDMLRuntimeCoordinator struct {
	countingDMLRuntimeCoordinator
	processAwareCalled bool
}

func (c *processAwareDMLRuntimeCoordinator) AppendWithProcess(*process.Process, *batch.Batch) error {
	c.processAwareCalled = true
	return nil
}
