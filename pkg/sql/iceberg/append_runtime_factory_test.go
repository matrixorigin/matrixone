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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

func TestAppendBaseSnapshotTreatsIcebergEmptyTableSentinelAsNoBase(t *testing.T) {
	rawMeta := []byte(`{
		"format-version": 2,
		"table-uuid": "empty-uuid",
		"location": "s3://warehouse/writer/empty_orders",
		"current-schema-id": 0,
		"schemas": [
			{"schema-id": 0, "fields": [
				{"id": 1, "name": "id", "required": false, "type": "long"}
			]}
		],
		"default-spec-id": 0,
		"partition-specs": [{"spec-id": 0, "fields": []}],
		"current-snapshot-id": -1,
		"snapshots": [],
		"snapshot-log": [],
		"refs": {}
	}`)
	meta, err := metadata.ParseTableMetadata(rawMeta, "s3://warehouse/writer/empty_orders/metadata/v1.json")
	require.NoError(t, err)

	snapshot, snapshotID, err := appendBaseSnapshot(context.Background(), meta, model.DefaultRefMain)
	require.NoError(t, err)
	require.Zero(t, snapshotID)
	require.Empty(t, snapshot.ManifestList)
}

func TestReadAppendBaseManifestListIsBounded(t *testing.T) {
	ctx := context.Background()
	fs, scopeForLocation := appendRuntimeMemoryObjectIO(t, ctx)
	const manifestList = "s3://warehouse/writer/orders/metadata/snap-30.avro"
	encoded, err := metadata.EncodeManifestList([]api.ManifestFile{{
		Path:            "s3://warehouse/writer/orders/metadata/m0.avro",
		PartitionSpecID: 0,
		Content:         api.ManifestContentData,
	}}, metadata.ManifestListWriteOptions{FormatVersion: 2, SnapshotID: 30})
	require.NoError(t, err)
	writeAppendRuntimeMemoryFile(t, ctx, fs, manifestList, encoded)
	reader := icebergio.ProviderObjectReader{
		Provider:         icebergio.ScopedProvider{FileService: fs},
		ScopeForLocation: scopeForLocation,
	}

	_, err = readAppendBaseManifestList(ctx, reader, api.Snapshot{ManifestList: manifestList}, int64(len(encoded))-1, 100)
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrPlanningLimitExceeded))

	manifests, err := readAppendBaseManifestList(ctx, reader, api.Snapshot{ManifestList: manifestList}, 1<<20, 100)
	require.NoError(t, err)
	require.Len(t, manifests, 1)
}

func TestAppendRuntimeCoordinatorCommitsPreservedManifestList(t *testing.T) {
	ctx := context.Background()
	tableLocation := "s3://warehouse/writer/gold_kpi"
	baseManifestLocation := tableLocation + "/metadata/base-manifest.avro"
	baseManifestListLocation := tableLocation + "/metadata/base-list.avro"
	baseManifestBytes, err := encodeSQLIcebergTestManifest([]api.ManifestEntry{{
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
	baseManifestListBytes, err := encodeSQLIcebergTestManifestList([]api.ManifestFile{{
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
			require.Equal(t, "test-principal", req.ExternalPrincipal)
			return &api.ConfigResponse{Prefix: "main|s3://warehouse"}, nil
		},
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			require.Equal(t, "main|s3://warehouse", req.Prefix)
			require.Equal(t, "test-principal", req.ExternalPrincipal)
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
			require.Equal(t, "test-principal", req.ExternalPrincipal)
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
	require.Zero(t, commitReq.Updates[1].MinSnapshotsToKeep)

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
	got, err = (WriteRuntimeCoordinatorFactory{}).NewCoordinator(ctx, icebergwrite.AppendRequest{Operation: icebergwrite.OperationAppend})
	require.NoError(t, err)
	require.Nil(t, got)
	got, err = (WriteRuntimeCoordinatorFactory{}).NewCoordinator(ctx, icebergwrite.AppendRequest{Operation: icebergwrite.OperationMerge})
	require.NoError(t, err)
	require.Nil(t, got)
	got, err = factory.NewCoordinator(ctx, icebergwrite.AppendRequest{Operation: "unknown"})
	require.Error(t, err)
	require.Nil(t, got)
	require.Contains(t, err.Error(), string(api.ErrUnsupportedFeature))
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

	visible, err := appendRuntimeVisibleBatch([]string{"id", "region"}, bat)
	require.NoError(t, err)
	require.NotSame(t, bat, visible)
	require.Equal(t, 1, visible.RowCount())
	require.Len(t, visible.Vecs, 2)
	require.Same(t, idVec, visible.Vecs[0])
	require.Same(t, regionVec, visible.Vecs[1])

	fallback := batch.New([]string{"missing", "also_missing", "region"})
	fallback.Vecs = bat.Vecs
	fallback.SetRowCount(1)
	visible, err = appendRuntimeVisibleBatch([]string{"id", "region"}, fallback)
	require.Error(t, err)
	require.Nil(t, visible)

	unnamed := batch.NewWithSize(3)
	unnamed.Vecs = []*vector.Vector{idVec, regionVec, hiddenVec}
	unnamed.SetRowCount(1)
	visible, err = appendRuntimeVisibleBatch([]string{"id", "region"}, unnamed)
	require.NoError(t, err)
	require.NotSame(t, unnamed, visible)
	require.Equal(t, []string{"id", "region"}, visible.Attrs)
	require.Equal(t, 1, visible.RowCount())
	require.Same(t, idVec, visible.Vecs[0])
	require.Same(t, regionVec, visible.Vecs[1])

	prefixNamed := batch.New([]string{"region", "id"})
	prefixNamed.Vecs = []*vector.Vector{regionVec, idVec, hiddenVec}
	prefixNamed.SetRowCount(1)
	visible, err = appendRuntimeVisibleBatch([]string{"id", "region"}, prefixNamed)
	require.NoError(t, err)
	require.Same(t, idVec, visible.Vecs[0])
	require.Same(t, regionVec, visible.Vecs[1])

	incomplete := batch.New([]string{"id"})
	incomplete.Vecs = []*vector.Vector{idVec, regionVec, hiddenVec}
	incomplete.SetRowCount(1)
	visible, err = appendRuntimeVisibleBatch([]string{"id", "region"}, incomplete)
	require.Error(t, err)
	require.Nil(t, visible)
}

func TestAppendRuntimeVisibleBatchPreservesCaseSensitiveNames(t *testing.T) {
	bat := batch.New([]string{"ID", "hidden", "id"})
	bat.Vecs = []*vector.Vector{
		vector.NewVec(types.T_int64.ToType()),
		vector.NewVec(types.T_int64.ToType()),
		vector.NewVec(types.T_int64.ToType()),
	}
	visible, err := appendRuntimeVisibleBatch([]string{"ID", "id"}, bat)
	require.NoError(t, err)
	require.Same(t, bat.Vecs[0], visible.Vecs[0])
	require.Same(t, bat.Vecs[2], visible.Vecs[1])

	visible, err = appendRuntimeVisibleBatch([]string{"Id"}, bat)
	require.Error(t, err)
	require.Nil(t, visible)

	reordered := batch.New([]string{"region", "id"})
	reordered.Vecs = []*vector.Vector{bat.Vecs[2], bat.Vecs[0]}
	visible, err = appendRuntimeVisibleBatch([]string{"id", "region"}, reordered)
	require.NoError(t, err)
	require.Same(t, bat.Vecs[0], visible.Vecs[0])
	require.Same(t, bat.Vecs[2], visible.Vecs[1])
}

func TestAppendRuntimeSharedCoordinatorCommitsOncePerStatement(t *testing.T) {
	ctx := context.Background()
	inner := &countingAppendCoordinator{}
	cache := &appendRuntimeCoordinatorCache{entries: make(map[string]*appendRuntimeCoordinatorCacheEntry)}
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
	require.NoError(t, coord1.(icebergwrite.ProcessAwareCoordinator).AppendWithProcess(nil, batch.EmptyBatch))
	require.NoError(t, coord2.Append(ctx, batch.EmptyBatch))
	require.NoError(t, coord1.Commit(ctx))
	require.Equal(t, 0, inner.commitCalls)
	require.NoError(t, coord2.Commit(ctx))
	require.Equal(t, 1, inner.beginCalls)
	require.Equal(t, 3, inner.appendCalls)
	require.Equal(t, 1, inner.commitCalls)

	coord3, err := cache.getOrCreate(ctx, key, req, func() (icebergwrite.Coordinator, error) {
		builds++
		return &countingAppendCoordinator{}, nil
	})
	require.NoError(t, err)
	require.NotSame(t, coord1, coord3)
	require.Equal(t, 2, builds)
}

func TestAppendRuntimeCoordinatorCacheBuildsDifferentStatementsConcurrently(t *testing.T) {
	ctx := context.Background()
	cache := &appendRuntimeCoordinatorCache{entries: make(map[string]*appendRuntimeCoordinatorCacheEntry)}
	entered := make(chan string, 2)
	release := make(chan struct{})
	results := make(chan error, 2)

	build := func(key string) {
		_, err := cache.getOrCreate(ctx, key, icebergwrite.AppendRequest{StatementID: key}, func() (icebergwrite.Coordinator, error) {
			entered <- key
			<-release
			return &countingAppendCoordinator{}, nil
		})
		results <- err
	}
	go build("stmt-1")
	go build("stmt-2")

	seen := make(map[string]bool, 2)
	for len(seen) < 2 {
		select {
		case key := <-entered:
			seen[key] = true
		case <-time.After(time.Second):
			t.Fatal("coordinator builds for different statements were serialized")
		}
	}
	close(release)
	for range 2 {
		require.NoError(t, <-results)
	}
}

func TestAppendRuntimeCoordinatorCacheKeyDoesNotCollideOnDelimiters(t *testing.T) {
	base := icebergwrite.AppendRequest{AccountID: 1, CatalogName: "catalog", Namespace: "sales", Table: "orders", StatementID: "stmt"}
	left := base
	left.CatalogName = "catalog\x1fsales"
	left.Namespace = "orders"
	right := base
	right.CatalogName = "catalog"
	right.Namespace = "sales\x1forders"
	require.NotEqual(t, appendRuntimeCoordinatorCacheKey(left), appendRuntimeCoordinatorCacheKey(right))
}

func TestAppendRuntimeCoordinatorCacheSingleflightsSameStatement(t *testing.T) {
	ctx := context.Background()
	cache := &appendRuntimeCoordinatorCache{entries: make(map[string]*appendRuntimeCoordinatorCacheEntry)}
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	results := make(chan error, 2)
	build := func() (icebergwrite.Coordinator, error) {
		entered <- struct{}{}
		<-release
		return &countingAppendCoordinator{}, nil
	}
	request := icebergwrite.AppendRequest{StatementID: "stmt-shared"}

	for range 2 {
		go func() {
			_, err := cache.getOrCreate(ctx, "stmt-shared", request, build)
			results <- err
		}()
	}
	<-entered
	close(release)
	for range 2 {
		require.NoError(t, <-results)
	}
	require.Len(t, entered, 0, "same-key cache miss must build only once")
}

func TestAppendRuntimeFactoryValidationAndHelpers(t *testing.T) {
	ctx := context.Background()
	cfg := api.DefaultConfig()
	cfg.Enable = true
	cfg.Write.EnableWrite = true
	validReq := icebergwrite.AppendRequest{
		AccountID:      42,
		StatementID:    "stmt-1",
		IdempotencyKey: "stmt-1",
		CatalogName:    "tier_a",
		Namespace:      "sales",
		Table:          "orders",
		Operation:      icebergwrite.OperationAppend,
		TableDef:       &plan.TableDef{DbId: 1, TblId: 2},
	}
	validFactory := NewAppendRuntimeCoordinatorFactory(AppendRuntimeCoordinatorFactoryOptions{
		Store:          &fakeAppendRuntimeStore{mapping: model.TableMapping{CatalogID: 7, WriteMode: model.WriteModeAppendOnly}},
		CatalogFactory: staticCatalogFactory{client: &icebergcatalog.MockClient{}},
		Config:         cfg,
	})
	fromInternal := NewAppendRuntimeCoordinatorFactoryFromInternalSQLExecutor(nil, AppendRuntimeCoordinatorFactoryOptions{})
	require.NotNil(t, fromInternal.opts.Store)
	require.NoError(t, validFactory.validateRuntimeRequest(ctx, validReq))
	require.True(t, validFactory.requireResidencyPolicy())
	objectIOFactory := validFactory
	objectIOFactory.opts.ObjectIOProvider = icebergio.ScopedProvider{}
	require.False(t, objectIOFactory.requireResidencyPolicy())

	for name, mutate := range map[string]func(*AppendRuntimeCoordinatorFactory, *icebergwrite.AppendRequest){
		"missing store": func(f *AppendRuntimeCoordinatorFactory, req *icebergwrite.AppendRequest) {
			f.opts.Store = nil
		},
		"missing catalog factory": func(f *AppendRuntimeCoordinatorFactory, req *icebergwrite.AppendRequest) {
			f.opts.CatalogFactory = nil
		},
		"write disabled": func(f *AppendRuntimeCoordinatorFactory, req *icebergwrite.AppendRequest) {
			f.opts.Config.Write.EnableWrite = false
		},
		"missing names": func(f *AppendRuntimeCoordinatorFactory, req *icebergwrite.AppendRequest) {
			req.Namespace = " "
		},
		"missing key": func(f *AppendRuntimeCoordinatorFactory, req *icebergwrite.AppendRequest) {
			req.StatementID = ""
			req.IdempotencyKey = ""
		},
		"missing object ids": func(f *AppendRuntimeCoordinatorFactory, req *icebergwrite.AppendRequest) {
			req.TableDef = &plan.TableDef{DbId: 1}
		},
	} {
		t.Run(name, func(t *testing.T) {
			factory := validFactory
			req := validReq
			mutate(&factory, &req)
			require.Error(t, factory.validateRuntimeRequest(ctx, req))
		})
	}

	mapping, err := validFactory.tableMapping(ctx, validReq, model.Catalog{CatalogID: 7})
	require.NoError(t, err)
	require.Equal(t, model.WriteModeAppendOnly, mapping.WriteMode)
	_, err = validFactory.tableMapping(ctx, validReq, model.Catalog{CatalogID: 99})
	require.Error(t, err)
	readOnlyFactory := validFactory
	readOnlyFactory.opts.Store = &fakeAppendRuntimeStore{mapping: model.TableMapping{CatalogID: 7, WriteMode: "readonly"}}
	_, err = readOnlyFactory.tableMapping(ctx, validReq, model.Catalog{CatalogID: 7})
	require.Error(t, err)

	require.Empty(t, appendRuntimeCoordinatorCacheKey(icebergwrite.AppendRequest{}))
	require.Equal(t, int32(0), appendRuntimeParallelScopeID(icebergwrite.AppendRequest{MaxParallel: 3, ParallelID: -1}))
	require.Equal(t, int32(2), appendRuntimeParallelScopeID(icebergwrite.AppendRequest{MaxParallel: 3, ParallelID: 2}))
	require.Contains(t, appendDataDir("stmt-1", 91), "snap-91")
	paths, err := buildAppendManifestPaths(ctx, "s3://warehouse/t", "stmt-1", 91)
	require.NoError(t, err)
	require.Contains(t, paths.ManifestPath, "data-manifest-snap-91.avro")
	_, err = buildAppendManifestPaths(ctx, "", "stmt-1", 91)
	require.Error(t, err)
	_, err = buildAppendManifestPaths(ctx, "s3://warehouse/t", "", 91)
	require.Error(t, err)

	creds := filterS3AccessCredentials([]api.StorageCredential{
		{Config: map[string]string{"s3.access-key-id": "AK", "s3.secret-access-key": "SK"}},
		{Config: map[string]string{"s3.access-key-id": "AK"}},
		{Config: map[string]string{"aws.access-key-id": "AK2", "aws.secret-access-key": "SK2"}},
	})
	require.Len(t, creds, 2)
	require.Nil(t, filterS3AccessCredentials(nil))
}

func TestAppendRuntimeSharedCoordinatorErrorStates(t *testing.T) {
	ctx := context.Background()
	require.Error(t, (*appendRuntimeSharedCoordinator)(nil).Begin(ctx, icebergwrite.AppendRequest{}))
	require.Error(t, (*appendRuntimeSharedCoordinatorScope)(nil).Append(ctx, batch.EmptyBatch))
	require.NoError(t, (*appendRuntimeSharedCoordinatorScope)(nil).Abort(ctx, nil))

	inner := &countingAppendCoordinator{}
	shared := newAppendRuntimeSharedCoordinator(inner, icebergwrite.AppendRequest{MaxParallel: 1})
	require.Error(t, shared.Append(ctx, batch.EmptyBatch))
	require.Error(t, shared.Commit(ctx))
	require.NoError(t, shared.Begin(ctx, icebergwrite.AppendRequest{}))
	require.NoError(t, shared.Commit(ctx))
	require.Equal(t, 1, inner.commitCalls)
	require.Error(t, shared.Append(ctx, batch.EmptyBatch))
	require.Error(t, shared.Begin(ctx, icebergwrite.AppendRequest{}))
	require.NoError(t, shared.Abort(ctx, nil))

	inner = &countingAppendCoordinator{}
	inner.abortErr = moerr.NewInternalErrorNoCtx("abort")
	shared = newAppendRuntimeSharedCoordinator(inner, icebergwrite.AppendRequest{MaxParallel: 1})
	require.NoError(t, shared.Begin(ctx, icebergwrite.AppendRequest{}))
	require.EqualError(t, shared.Abort(ctx, moerr.NewInternalErrorNoCtx("ignored cause")), "internal error: abort")
	require.Error(t, shared.Append(ctx, batch.EmptyBatch))
	require.Equal(t, 1, inner.abortCalls)
}

func TestAppendRuntimeObjectIOContextBranches(t *testing.T) {
	ctx := context.Background()
	catalog := model.Catalog{AccountID: 42, CatalogID: 7, Name: "tier_a", URI: "https://catalog.example.com/rest"}
	baseReq := api.CatalogRequest{Catalog: catalog}
	namespace := api.Namespace{"sales"}
	scopeForLocation := func(string) icebergio.ObjectScope {
		return icebergio.ObjectScope{AccountID: 42, CatalogID: 7, Bucket: "warehouse"}
	}

	fs, scope := appendRuntimeMemoryObjectIO(t, ctx)
	factory := NewAppendRuntimeCoordinatorFactory(AppendRuntimeCoordinatorFactoryOptions{
		Store:            &fakeAppendRuntimeStore{},
		ObjectIOProvider: icebergio.ScopedProvider{FileService: fs},
		ScopeForLocation: scope,
	})
	got, err := factory.objectIOContext(ctx, &icebergcatalog.MockClient{}, baseReq, &api.LoadTableResponse{}, namespace, "orders", catalog)
	require.NoError(t, err)
	require.NotNil(t, got.WriterProvider)
	require.NotNil(t, got.ReaderProvider)

	factory = NewAppendRuntimeCoordinatorFactory(AppendRuntimeCoordinatorFactoryOptions{
		Store:            &fakeAppendRuntimeStore{},
		ScopeForLocation: scopeForLocation,
	})
	got, err = factory.objectIOContext(ctx, &icebergcatalog.MockClient{}, baseReq, &api.LoadTableResponse{
		StorageCredentials: []api.StorageCredential{{
			Config: map[string]string{
				"s3.access-key-id":     "AK",
				"s3.secret-access-key": "SK",
			},
		}},
	}, namespace, "orders", catalog)
	require.NoError(t, err)
	require.NotNil(t, got.WriterProvider)

	_, err = factory.objectIOContext(ctx, &icebergcatalog.MockClient{
		LoadCredentialsFunc: func(context.Context, api.LoadCredentialsRequest) (*api.LoadCredentialsResponse, error) {
			return nil, api.NewError(api.ErrCatalogUnavailable, "down", nil)
		},
	}, baseReq, &api.LoadTableResponse{}, namespace, "orders", catalog)
	require.Error(t, err)

	_, err = factory.objectIOContext(ctx, &icebergcatalog.MockClient{}, baseReq, &api.LoadTableResponse{
		Capabilities: api.CatalogCapabilities{RemoteSigning: true},
	}, namespace, "orders", catalog)
	require.Error(t, err)

	_, err = factory.objectIOContext(ctx, &nilRemoteSignerClient{}, baseReq, &api.LoadTableResponse{
		Capabilities: api.CatalogCapabilities{RemoteSigning: true},
	}, namespace, "orders", catalog)
	require.Error(t, err)

	_, err = factory.objectIOContext(ctx, &icebergcatalog.MockClient{}, baseReq, &api.LoadTableResponse{}, namespace, "orders", catalog)
	require.Error(t, err)
}

func TestAppendRuntimeSchemaSpecAndReporterHelpers(t *testing.T) {
	ctx := context.Background()
	_, err := appendCurrentSchema(ctx, nil, "orders")
	require.Error(t, err)
	_, err = appendDefaultSpec(ctx, nil, "orders")
	require.Error(t, err)
	meta := &api.TableMetadata{}
	_, err = appendCurrentSchema(ctx, meta, "orders")
	require.Error(t, err)
	_, err = appendDefaultSpec(ctx, meta, "orders")
	require.Error(t, err)

	configured := &fakeMetricsReporter{}
	require.Same(t, configured, appendMetricsReporter(configured, &icebergcatalog.MockClient{}))
	reportingClient := &metricsCatalogClient{}
	require.Same(t, reportingClient, appendMetricsReporter(nil, reportingClient))
	require.Nil(t, appendMetricsReporter(nil, &icebergcatalog.MockClient{}))
	now := time.Unix(123, 0)
	factory := NewAppendRuntimeCoordinatorFactory(AppendRuntimeCoordinatorFactoryOptions{
		Now:        func() time.Time { return now },
		SnapshotID: func(time.Time, *api.TableMetadata) int64 { return 777 },
	})
	require.Equal(t, now, factory.now())
	require.Equal(t, int64(777), factory.nextSnapshotID(now, meta))
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
	catalog           model.Catalog
	mapping           model.TableMapping
	principalMaps     []model.PrincipalMap
	residencyPolicies []model.ResidencyPolicy
}

func (s *fakeAppendRuntimeStore) GetCatalogByName(ctx context.Context, accountID uint32, name string) (model.Catalog, error) {
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

func (s *fakeAppendRuntimeStore) GetTableMapping(ctx context.Context, accountID uint32, databaseID uint64, tableID uint64) (model.TableMapping, error) {
	return s.mapping, nil
}

func (s *fakeAppendRuntimeStore) ListPrincipalMaps(ctx context.Context, accountID uint32, catalogID uint64) ([]model.PrincipalMap, error) {
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

func (s *fakeAppendRuntimeStore) ListResidencyPolicies(ctx context.Context, accountID uint32, catalogID uint64) ([]model.ResidencyPolicy, error) {
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
	abortErr    error
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
	return c.abortErr
}

type nilRemoteSignerClient struct {
	icebergcatalog.MockClient
}

func (*nilRemoteSignerClient) NewRemoteSigner(api.CatalogRequest, map[string]string) icebergio.RemoteSigner {
	return nil
}

type fakeMetricsReporter struct{}

func (fakeMetricsReporter) ReportMetrics(context.Context, api.MetricsReportRequest) error {
	return nil
}

type metricsCatalogClient struct {
	icebergcatalog.MockClient
}

func (m *metricsCatalogClient) ReportMetrics(context.Context, api.MetricsReportRequest) error {
	return nil
}
