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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergcatalog "github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	icebergwritecore "github.com/matrixorigin/matrixone/pkg/iceberg/write"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/icebergwrite"
)

func TestAppendRuntimeCoordinatorCommitsPreservedManifestList(t *testing.T) {
	ctx := context.Background()
	tableLocation := "s3://warehouse/writer/gold_kpi"
	baseManifestLocation := tableLocation + "/metadata/base-manifest.avro"
	baseManifestListLocation := tableLocation + "/metadata/base-list.avro"
	baseManifestBytes, err := metadata.EncodeManifest([]api.ManifestEntry{{
		Status:         api.ManifestEntryAdded,
		SnapshotID:     30,
		SequenceNumber: 44,
		DataFile: api.DataFile{
			Content:     api.DataFileContentData,
			FilePath:    tableLocation + "/data/base.parquet",
			FileFormat:  "parquet",
			RecordCount: 2,
			SpecID:      0,
		},
	}})
	require.NoError(t, err)
	baseManifestListBytes, err := metadata.EncodeManifestList([]api.ManifestFile{{
		Path:            baseManifestLocation,
		Length:          int64(len(baseManifestBytes)),
		Content:         api.ManifestContentData,
		AddedSnapshotID: 30,
	}})
	require.NoError(t, err)

	fs, scopeForLocation := appendRuntimeMemoryObjectIO(t, ctx)
	writeAppendRuntimeMemoryFile(t, ctx, fs, baseManifestListLocation, baseManifestListBytes)
	writeAppendRuntimeMemoryFile(t, ctx, fs, baseManifestLocation, baseManifestBytes)

	rawMeta := []byte(`{
		"format-version": 2,
		"table-uuid": "gold-kpi-uuid",
		"location": "s3://warehouse/writer/gold_kpi",
		"last-sequence-number": 44,
		"current-schema-id": 0,
		"schemas": [
			{"schema-id": 0, "fields": [
				{"id": 1, "name": "id", "required": false, "type": "long"},
				{"id": 2, "name": "region", "required": false, "type": "string"},
				{"id": 3, "name": "amount", "required": false, "type": "long"}
			]}
		],
		"default-spec-id": 0,
		"partition-specs": [{"spec-id": 0, "fields": []}],
		"current-snapshot-id": 30,
		"snapshots": [{"snapshot-id": 30, "sequence-number": 44, "timestamp-ms": 1000, "schema-id": 0, "manifest-list": "s3://warehouse/writer/gold_kpi/metadata/base-list.avro"}],
		"refs": {"main": {"snapshot-id": 30, "type": "branch"}}
	}`)
	store := &fakeAppendRuntimeStore{
		catalog: model.Catalog{
			AccountID:        42,
			CatalogID:        7,
			Name:             "tier_a",
			Type:             "rest",
			URI:              "https://catalog.example.com/rest",
			Warehouse:        "s3://warehouse",
			CapabilitiesJSON: `{"commit":true}`,
		},
		mapping: model.TableMapping{
			AccountID:            42,
			DatabaseID:           100,
			TableID:              200,
			CatalogID:            7,
			Namespace:            "writer",
			TableName:            "gold_kpi",
			DefaultRef:           model.DefaultRefMain,
			ReadMode:             model.ReadModeAppendOnly,
			WriteMode:            model.WriteModeAppendOnly,
			WriterOwnerAccountID: 42,
			Version:              1,
		},
	}
	var commitReq api.CommitRequest
	client := &icebergcatalog.MockClient{
		GetConfigFunc: func(ctx context.Context, req api.GetConfigRequest) (*api.ConfigResponse, error) {
			require.Equal(t, "s3://warehouse", req.Warehouse)
			return &api.ConfigResponse{Prefix: "main|s3://warehouse"}, nil
		},
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			require.Equal(t, "main|s3://warehouse", req.Prefix)
			require.Equal(t, api.Namespace{"writer"}, req.Namespace)
			require.Equal(t, "gold_kpi", req.Table)
			return &api.LoadTableResponse{
				MetadataLocation: "s3://warehouse/writer/gold_kpi/metadata/v1.json",
				MetadataJSON:     rawMeta,
				TableToken:       "table-token",
				Capabilities:     api.CatalogCapabilities{Commit: true},
			}, nil
		},
		CommitTableFunc: func(ctx context.Context, req api.CommitRequest) (*api.CommitResult, error) {
			commitReq = req
			return &api.CommitResult{
				SnapshotID:           45,
				CommitID:             "commit-45",
				MetadataLocation:     "s3://warehouse/writer/gold_kpi/metadata/v2.json",
				MetadataLocationHash: api.PathHash("s3://warehouse/writer/gold_kpi/metadata/v2.json"),
			}, nil
		},
	}
	cfg := api.DefaultConfig()
	cfg.Enable = true
	cfg.Write.EnableWrite = true
	factory := NewAppendRuntimeCoordinatorFactory(AppendRuntimeCoordinatorFactoryOptions{
		Store:            store,
		CatalogFactory:   staticCatalogFactory{client: client},
		Config:           cfg,
		Now:              func() time.Time { return time.Unix(10, 0) },
		ObjectIOProvider: icebergio.ScopedProvider{FileService: fs},
		ScopeForLocation: scopeForLocation,
		SnapshotID:       func(time.Time, *api.TableMetadata) int64 { return 45 },
	})
	coord, err := factory.NewCoordinator(ctx, icebergwrite.AppendRequest{
		AccountID:      42,
		StatementID:    "stmt-append-117",
		IdempotencyKey: "stmt-append-117",
		CatalogName:    "tier_a",
		Namespace:      "writer",
		Table:          "gold_kpi",
		DefaultRef:     model.DefaultRefMain,
		Operation:      icebergwrite.OperationAppend,
		Attrs:          []string{"id", "region", "amount"},
		TableDef: &plan.TableDef{
			DbId:  100,
			TblId: 200,
		},
	})
	require.NoError(t, err)
	require.NoError(t, coord.Begin(ctx, icebergwrite.AppendRequest{}))
	bat, mp := appendGoldKPIBatch(t)
	defer bat.Clean(mp)
	require.NoError(t, coord.Append(ctx, bat))
	require.NoError(t, coord.Commit(ctx))

	require.Equal(t, "main|s3://warehouse", commitReq.Prefix)
	require.Equal(t, "table-token", commitReq.TableToken)
	require.Equal(t, "stmt-append-117", commitReq.IdempotencyKey)
	require.Len(t, commitReq.Updates, 2)
	require.Equal(t, "add-snapshot", commitReq.Updates[0].Type)
	require.NotNil(t, commitReq.Updates[0].Snapshot)
	snapshot := commitReq.Updates[0].Snapshot
	require.Equal(t, int64(45), snapshot.SnapshotID)
	require.NotNil(t, snapshot.ParentSnapshotID)
	require.Equal(t, int64(30), *snapshot.ParentSnapshotID)
	require.NotNil(t, snapshot.SchemaID)
	require.Equal(t, 0, *snapshot.SchemaID)
	require.Equal(t, int64(45), snapshot.SequenceNumber)
	require.Equal(t, "append", snapshot.Summary["operation"])
	require.Equal(t, "2", snapshot.Summary["added-records"])
	require.Equal(t, "stmt-append-117", snapshot.Summary["idempotency-key"])
	require.Equal(t, string(icebergwritecore.AppendSourceSQLInsert), snapshot.Summary["source-kind"])
	require.Equal(t, string(icebergwrite.OperationAppend), snapshot.Summary["mo-operation"])
	require.Equal(t, "set-snapshot-ref", commitReq.Updates[1].Type)
	require.Equal(t, model.DefaultRefMain, commitReq.Updates[1].Ref)

	manifestListBytes := readAppendRuntimeMemoryFile(t, ctx, fs, snapshot.ManifestList)
	manifestList, err := metadata.ReadManifestList(manifestListBytes)
	require.NoError(t, err)
	require.Len(t, manifestList, 2)
	require.Equal(t, baseManifestLocation, manifestList[0].Path)
	require.Equal(t, api.ManifestContentData, manifestList[1].Content)
	require.Equal(t, int64(45), manifestList[1].AddedSnapshotID)

	newManifestBytes := readAppendRuntimeMemoryFile(t, ctx, fs, manifestList[1].Path)
	entries, err := metadata.ReadManifest(newManifestBytes)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, api.ManifestEntryAdded, entries[0].Status)
	require.Equal(t, int64(45), entries[0].SnapshotID)
	require.Equal(t, int64(2), entries[0].DataFile.RecordCount)
	require.True(t, strings.HasPrefix(entries[0].DataFile.FilePath, tableLocation+"/data/mo-append/"))
}

func TestWriteRuntimeCoordinatorFactoryDispatchesAppendAndDML(t *testing.T) {
	ctx := context.Background()
	appendCoord := &fakeWriteRuntimeCoordinator{}
	dmlCoord := &fakeWriteRuntimeCoordinator{}
	factory := WriteRuntimeCoordinatorFactory{
		Append: icebergwrite.CoordinatorFactoryFunc(func(context.Context, icebergwrite.AppendRequest) (icebergwrite.Coordinator, error) {
			return appendCoord, nil
		}),
		DML: icebergwrite.CoordinatorFactoryFunc(func(context.Context, icebergwrite.AppendRequest) (icebergwrite.Coordinator, error) {
			return dmlCoord, nil
		}),
	}
	got, err := factory.NewCoordinator(ctx, icebergwrite.AppendRequest{Operation: icebergwrite.OperationAppend})
	require.NoError(t, err)
	require.Same(t, appendCoord, got)
	got, err = factory.NewCoordinator(ctx, icebergwrite.AppendRequest{Operation: icebergwrite.OperationDelete})
	require.NoError(t, err)
	require.Same(t, dmlCoord, got)
}

func TestAppendRuntimeVisibleBatchFiltersExtraNamedColumns(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"id", "__mo_rowid", "region"})
	idVec := vector.NewVec(types.T_int64.ToType())
	hiddenVec := vector.NewVec(types.T_int64.ToType())
	regionVec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendFixed[int64](idVec, 1, false, mp))
	require.NoError(t, vector.AppendFixed[int64](hiddenVec, 99, false, mp))
	require.NoError(t, vector.AppendBytes(regionVec, []byte("ksa"), false, mp))
	bat.Vecs[0] = idVec
	bat.Vecs[1] = hiddenVec
	bat.Vecs[2] = regionVec
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	visible := appendRuntimeVisibleBatch([]string{"id", "region"}, bat)
	require.NotSame(t, bat, visible)
	require.Equal(t, 1, visible.RowCount())
	require.Len(t, visible.Vecs, 2)
	require.Same(t, idVec, visible.Vecs[0])
	require.Same(t, regionVec, visible.Vecs[1])
}

func TestAppendRuntimeSharedCoordinatorCommitsOncePerStatement(t *testing.T) {
	ctx := context.Background()
	inner := &countingAppendCoordinator{}
	cache := &appendRuntimeCoordinatorCache{entries: make(map[string]*appendRuntimeSharedCoordinator)}
	req := icebergwrite.AppendRequest{
		AccountID:      42,
		Operation:      icebergwrite.OperationAppend,
		CatalogName:    "tlc",
		Namespace:      "public_nyc_tlc",
		Table:          "yellow_tripdata_export",
		DefaultRef:     model.DefaultRefMain,
		StatementID:    "stmt-export-1",
		IdempotencyKey: "stmt-export-1",
		ParallelID:     0,
		MaxParallel:    2,
	}
	key := appendRuntimeCoordinatorCacheKey(req)
	require.NotEmpty(t, key)

	builds := 0
	coord1, err := cache.getOrCreate(ctx, key, req, func() (icebergwrite.Coordinator, error) {
		builds++
		return inner, nil
	})
	require.NoError(t, err)
	req2 := req
	req2.ParallelID = 1
	coord2, err := cache.getOrCreate(ctx, key, req2, func() (icebergwrite.Coordinator, error) {
		builds++
		return &countingAppendCoordinator{}, nil
	})
	require.NoError(t, err)
	require.NotSame(t, coord1, coord2)
	require.Equal(t, 1, builds)

	require.NoError(t, coord1.Begin(ctx, req))
	require.NoError(t, coord2.Begin(ctx, req2))
	require.NoError(t, coord1.Append(ctx, batch.EmptyBatch))
	require.NoError(t, coord2.Append(ctx, batch.EmptyBatch))
	require.NoError(t, coord1.Commit(ctx))
	require.Equal(t, 0, inner.commitCalls)
	require.NoError(t, coord2.Commit(ctx))
	require.Equal(t, 1, inner.beginCalls)
	require.Equal(t, 2, inner.appendCalls)
	require.Equal(t, 1, inner.commitCalls)

	coord3, err := cache.getOrCreate(ctx, key, req, func() (icebergwrite.Coordinator, error) {
		builds++
		return &countingAppendCoordinator{}, nil
	})
	require.NoError(t, err)
	require.NotSame(t, coord1, coord3)
	require.Equal(t, 2, builds)
}

func appendGoldKPIBatch(t *testing.T) (*batch.Batch, *mpool.MPool) {
	t.Helper()
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"id", "region", "amount"})
	idVec := vector.NewVec(types.T_int64.ToType())
	regionVec := vector.NewVec(types.T_varchar.ToType())
	amountVec := vector.NewVec(types.T_int64.ToType())
	for _, row := range []struct {
		id     int64
		region string
		amount int64
	}{
		{id: 9001, region: "ksa", amount: 10},
		{id: 9002, region: "uae", amount: 20},
	} {
		require.NoError(t, vector.AppendFixed[int64](idVec, row.id, false, mp))
		require.NoError(t, vector.AppendBytes(regionVec, []byte(row.region), false, mp))
		require.NoError(t, vector.AppendFixed[int64](amountVec, row.amount, false, mp))
	}
	bat.Vecs[0] = idVec
	bat.Vecs[1] = regionVec
	bat.Vecs[2] = amountVec
	bat.SetRowCount(2)
	return bat, mp
}

func appendRuntimeMemoryObjectIO(t *testing.T, ctx context.Context) (fileservice.ETLFileService, icebergio.ObjectScopeForLocation) {
	t.Helper()
	fs, err := fileservice.NewMemoryFS("iceberg-append-runtime", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	return fs, func(location string) icebergio.ObjectScope {
		return icebergio.ObjectScope{
			AccountID:       42,
			CatalogID:       7,
			Endpoint:        "s3.me-central-1.amazonaws.com",
			Region:          "me-central-1",
			Bucket:          "warehouse",
			Principal:       "append-test",
			StorageLocation: appendRuntimeMemoryPath(location),
		}
	}
}

func writeAppendRuntimeMemoryFile(t *testing.T, ctx context.Context, fs fileservice.ETLFileService, location string, payload []byte) {
	t.Helper()
	err := fs.Write(ctx, fileservice.IOVector{
		FilePath: appendRuntimeMemoryPath(location),
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(payload)),
			Data:   append([]byte(nil), payload...),
		}},
	})
	require.NoError(t, err)
}

func readAppendRuntimeMemoryFile(t *testing.T, ctx context.Context, fs fileservice.ETLFileService, location string) []byte {
	t.Helper()
	vec := fileservice.IOVector{
		FilePath: appendRuntimeMemoryPath(location),
		Policy:   fileservice.SkipFullFilePreloads,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: -1}},
	}
	require.NoError(t, fs.Read(ctx, &vec))
	require.Len(t, vec.Entries, 1)
	return append([]byte(nil), vec.Entries[0].Data...)
}

func appendRuntimeMemoryPath(location string) string {
	return strings.TrimPrefix(location, "s3://")
}

type fakeAppendRuntimeStore struct {
	catalog model.Catalog
	mapping model.TableMapping
}

func (s *fakeAppendRuntimeStore) GetCatalogByName(ctx context.Context, accountID uint32, name string) (model.Catalog, error) {
	return s.catalog, nil
}

func (s *fakeAppendRuntimeStore) GetTableMapping(ctx context.Context, accountID uint32, databaseID uint64, tableID uint64) (model.TableMapping, error) {
	return s.mapping, nil
}

func (s *fakeAppendRuntimeStore) InsertPublishJob(ctx context.Context, job model.PublishJob) error {
	return nil
}

func (s *fakeAppendRuntimeStore) InsertOrphanFile(ctx context.Context, file model.OrphanFile) error {
	return nil
}

type fakeWriteRuntimeCoordinator struct{}

func (fakeWriteRuntimeCoordinator) Begin(context.Context, icebergwrite.AppendRequest) error {
	return nil
}
func (fakeWriteRuntimeCoordinator) Append(context.Context, *batch.Batch) error { return nil }
func (fakeWriteRuntimeCoordinator) Commit(context.Context) error               { return nil }
func (fakeWriteRuntimeCoordinator) Abort(context.Context, error) error         { return nil }

type countingAppendCoordinator struct {
	beginCalls  int
	appendCalls int
	commitCalls int
	abortCalls  int
}

func (c *countingAppendCoordinator) Begin(context.Context, icebergwrite.AppendRequest) error {
	c.beginCalls++
	return nil
}

func (c *countingAppendCoordinator) Append(context.Context, *batch.Batch) error {
	c.appendCalls++
	return nil
}

func (c *countingAppendCoordinator) Commit(context.Context) error {
	c.commitCalls++
	return nil
}

func (c *countingAppendCoordinator) Abort(context.Context, error) error {
	c.abortCalls++
	return nil
}
