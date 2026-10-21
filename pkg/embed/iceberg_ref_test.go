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

package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	icebergmetadata "github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	sqliceberg "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	"github.com/stretchr/testify/require"
)

// #30522 and #30523: keep SQL compilation, runtime ref routing, native metadata
// planning, Parquet reads and append coordination real. Only catalog storage and
// object IO are injected, reusing the harness's cluster and four existing rows.
func runIcebergSQLEngineEmbeddedTypedRefs(t *testing.T, c Cluster, db *embeddedSQLSession, fixture *embeddedIcebergFixture) {
	const base = "s3://warehouse/embed/typed-refs/"
	schema := api.Schema{SchemaID: 0, Fields: []api.SchemaField{
		{ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeLong}},
		{ID: 2, Name: "hidden_key", Type: api.IcebergType{Kind: api.TypeLong}},
		{ID: 3, Name: "bucket", Type: api.IcebergType{Kind: api.TypeLong}},
		{ID: 4, Name: "amount", Type: api.IcebergType{Kind: api.TypeLong}},
	}}
	spec := api.PartitionSpec{SpecID: 0}
	manifests := make([]api.ManifestFile, 0, 2)
	for i, suffix := range []string{"old", "new"} {
		dataPath := "s3://warehouse/embed/orders/part-" + suffix + ".parquet"
		id := []int64{embeddedIcebergSnapshotOld, embeddedIcebergSnapshotCurrent}[i]
		manifestPath := base + suffix + ".avro"
		encoded, err := icebergmetadata.EncodeManifest([]api.ManifestEntry{{
			Status: api.ManifestEntryAdded, SnapshotID: id, SequenceNumber: int64(i + 1),
			DataFile: api.DataFile{Content: api.DataFileContentData, FilePath: dataPath, FileFormat: "parquet", RecordCount: 2, FileSizeInBytes: fixture.fileSizes[dataPath]},
		}}, icebergmetadata.ManifestWriteOptions{FormatVersion: 2, Schema: schema, PartitionSpec: spec, Content: api.ManifestContentData})
		require.NoError(t, err)
		writeEmbeddedIcebergObject(t, fixture.objectFS, strings.TrimPrefix(manifestPath, "s3://"), encoded)
		manifests = append(manifests, api.ManifestFile{Path: manifestPath, Length: int64(len(encoded)), Content: api.ManifestContentData, AddedSnapshotID: id, SequenceNumber: int64(i + 1), MinSequenceNumber: int64(i + 1)})
		list, err := icebergmetadata.EncodeManifestList(manifests, icebergmetadata.ManifestListWriteOptions{FormatVersion: 2, SnapshotID: id, SequenceNumber: int64(i + 1)})
		require.NoError(t, err)
		writeEmbeddedIcebergObject(t, fixture.objectFS, strings.TrimPrefix(base+suffix+"-list.avro", "s3://"), list)
	}
	currentID := embeddedIcebergSnapshotCurrent
	meta := api.TableMetadata{
		FormatVersion: 2, TableUUID: "typed-ref-fixture", Location: strings.TrimSuffix(base, "/"), LastSequenceNumber: 2,
		CurrentSchemaID: 0, Schemas: []api.Schema{schema}, DefaultSpecID: 0, PartitionSpecs: []api.PartitionSpec{spec}, CurrentSnapshotID: &currentID,
		Snapshots: []api.Snapshot{
			{SnapshotID: embeddedIcebergSnapshotOld, SequenceNumber: 1, TimestampMS: embeddedIcebergTimestampCutoff - 1000, ManifestList: base + "old-list.avro"},
			{SnapshotID: embeddedIcebergSnapshotCurrent, SequenceNumber: 2, TimestampMS: embeddedIcebergTimestampCutoff, ManifestList: base + "new-list.avro"},
		},
		Refs: map[string]api.SnapshotRef{"main": {SnapshotID: currentID, Type: "branch"}},
	}
	var mu sync.Mutex
	commits := 0
	client := &catalog.MockClient{
		GetConfigFunc: func(context.Context, api.GetConfigRequest) (*api.ConfigResponse, error) {
			return &api.ConfigResponse{Prefix: "main|s3://warehouse", Overrides: map[string]string{"nessie.is-nessie-catalog": "true"}}, nil
		},
		LoadTableFunc: func(context.Context, api.LoadTableRequest) (*api.LoadTableResponse, error) {
			mu.Lock()
			defer mu.Unlock()
			payload, err := json.Marshal(meta)
			if err != nil {
				return nil, err
			}
			payload = bytes.Replace(payload, []byte(`"Refs":`), []byte(`"refs":`), 1)
			return &api.LoadTableResponse{MetadataLocation: base + "metadata.json", MetadataJSON: payload, Capabilities: api.CatalogCapabilities{Commit: true}}, nil
		},
		CommitTableFunc: func(_ context.Context, req api.CommitRequest) (*api.CommitResult, error) {
			mu.Lock()
			defer mu.Unlock()
			for _, update := range req.Updates {
				if update.Type == "add-snapshot" {
					meta.Snapshots = append(meta.Snapshots, *update.Snapshot)
					currentID = update.Snapshot.SnapshotID
					meta.LastSequenceNumber = update.Snapshot.SequenceNumber
				}
				if update.Type == "set-snapshot-ref" {
					meta.Refs[update.Ref] = api.SnapshotRef{SnapshotID: update.SnapshotID, Type: "branch"}
				}
			}
			commits++
			return &api.CommitResult{SnapshotID: currentID, CommitID: "typed-main-append", MetadataLocation: base + "metadata.json"}, nil
		},
	}
	factory := catalog.NewFactory(catalog.WithNativeRESTClient(client))
	scope := func(location string) icebergio.ObjectScope {
		return icebergio.ObjectScope{
			AccountID: uint32(fixture.accountID), CatalogID: fixture.catalogID,
			Endpoint: fixture.endpoint, Region: "me-central-1", Bucket: "warehouse", Principal: "embedded-principal",
			StorageLocation: strings.TrimPrefix(location, "s3://"),
		}
	}
	provider := icebergio.ScopedProvider{FileService: fixture.objectFS}
	reader := icebergio.ProviderObjectReader{Provider: provider, ScopeForLocation: scope}
	cfg := api.DefaultConfig()
	cfg.Enable = true
	cfg.Write.EnableWrite = true
	cfg.Scan.ServerPlanningMode = api.ServerPlanningOff
	planner := icebergmetadata.RuntimeScanPlanner{
		CatalogFactory: factory, Metadata: icebergmetadata.NativeFacade{}, Config: cfg,
		ObjectReader: func(ctx context.Context, client api.CatalogClient, req api.ScanPlanRequest) (api.ObjectReader, icebergmetadata.ObjectReaderContext, error) {
			_, err := client.LoadCredentials(ctx, api.LoadCredentialsRequest{CatalogRequest: req.CatalogRequest, Namespace: req.Namespace, Table: req.Table})
			return reader, icebergmetadata.ObjectReaderContext{}, err
		},
	}
	installEmbeddedIcebergPlanner(t, c, &evolutionSQLPlanner{planner: planner, objectRef: fixture.objectRef})
	cn0, err := c.GetCNService(0)
	require.NoError(t, err)
	appendFactory := sqliceberg.NewAppendRuntimeCoordinatorFactoryFromInternalSQLExecutor(
		cn0.(*operator).reset.svc.(cnservice.Service).GetSQLExecutor(), sqliceberg.AppendRuntimeCoordinatorFactoryOptions{
			CatalogFactory: factory, Config: cfg, ObjectIOProvider: provider, ScopeForLocation: scope,
			SnapshotID: func(time.Time, *api.TableMetadata) int64 { return 303 },
		})
	installEmbeddedIcebergWriteCoordinator(t, c, &appendFactory)
	main := createEmbeddedIcebergTableWithRefModes(t, db, fixture, "ref_main", "typed_refs", "main", model.ReadModeAppendOnly, model.WriteModeAppendOnly)
	branch := createEmbeddedIcebergTableWithRefModes(t, db, fixture, "ref_branch", "typed_refs", "branch:main", model.ReadModeAppendOnly, model.WriteModeAppendOnly)
	pin := createEmbeddedIcebergTableWithRefModes(t, db, fixture, "ref_pin", "typed_refs", "snapshot:101", model.ReadModeAppendOnly, model.WriteModeAppendOnly)
	frontendDB := openIcebergTenantFrontendDB(t, c, db.accountName)
	t.Cleanup(func() { require.NoError(t, frontendDB.Close()) })
	frontendDB.SetMaxOpenConns(1)
	// The real frontend supplies the statement identity required by the append
	// coordinator. Keep reads and writes on that same public SQL surface.
	exec := func(stmt string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := frontendDB.ExecContext(ctx, stmt)
		return err
	}
	rows := func(table string) [][]int64 {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		result, err := frontendDB.QueryContext(ctx, "select id,amount from "+table+" order by id")
		require.NoError(t, err)
		defer result.Close()
		var rows [][]int64
		for result.Next() {
			var id, amount int64
			require.NoError(t, result.Scan(&id, &amount))
			rows = append(rows, []int64{id, amount})
		}
		require.NoError(t, result.Err())
		return rows
	}
	sum := func(table string) int64 {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var amount int64
		require.NoError(t, frontendDB.QueryRowContext(ctx, "select sum(amount) from "+table).Scan(&amount))
		return amount
	}
	oldRows := [][]int64{{1, 10}, {2, 20}}
	currentRows := [][]int64{{1, 10}, {2, 20}, {3, 30}, {4, 40}}
	require.Equal(t, currentRows, rows(main))
	require.Equal(t, currentRows, rows(branch))
	require.Equal(t, oldRows, rows(pin))
	require.Equal(t, oldRows, rows(main+" for iceberg snapshot 101"))
	require.Equal(t, currentRows, rows(pin+" for iceberg snapshot 202"))
	require.Equal(t, currentRows, rows(pin+" for iceberg ref main"))
	require.NoError(t, exec("set time_zone = '+00:00'"))
	require.Equal(t, currentRows, rows(pin+" for iceberg timestamp as of timestamp '2026-01-02 00:00:00'"))

	require.NoError(t, exec("insert into "+branch+" values (5,50,3,50)"))
	require.Equal(t, [][]int64{{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}}, rows(main))
	require.Equal(t, rows(main), rows(branch))
	require.Equal(t, oldRows, rows(pin), "a mapping pin must survive advancing main")
	err = exec("insert into " + pin + " values (6,60,3,60)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Iceberg pinned refs are read-only")
	mu.Lock()
	commitCount := commits
	mu.Unlock()
	require.Equal(t, 1, commitCount, "the rejected write must not commit")
	require.Equal(t, int64(150), sum(branch))
	require.Equal(t, int64(30), sum(pin))
}
