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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	"github.com/matrixorigin/matrixone/pkg/iceberg/maintenance"
	icebergmetadata "github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

type evolutionObjectReader map[string][]byte

func (r evolutionObjectReader) Read(_ context.Context, path string, offset, length int64) ([]byte, error) {
	data, ok := r[path]
	if !ok {
		return nil, fmt.Errorf("missing evolution object %s", path)
	}
	if offset >= int64(len(data)) {
		return nil, nil
	}
	end := int64(len(data))
	if length >= 0 && length < end-offset {
		end = offset + length
	}
	return data[offset:end], nil
}

type evolutionSQLPlanner struct {
	planner   api.ScanPlanner
	objectRef string
}

// #30435: exercise the real SQL compiler and native planner with the metadata
// emitted by external writers before their first commit. Reuse the harness's
// cluster and access setup; an empty object store rejects accidental data IO.
func runIcebergSQLEngineEmbeddedSnapshotlessTable(t *testing.T, c Cluster, db *embeddedSQLSession, fixture *embeddedIcebergFixture) {
	currentSnapshot := "-1"
	native := icebergmetadata.LocalScanPlanner{
		Metadata:     icebergmetadata.NativeFacade{},
		ObjectReader: evolutionObjectReader{},
		Catalog: &catalog.MockClient{LoadTableFunc: func(context.Context, api.LoadTableRequest) (*api.LoadTableResponse, error) {
			return &api.LoadTableResponse{
				MetadataLocation: "s3://warehouse/embed/empty/metadata.json",
				MetadataJSON: []byte(fmt.Sprintf(`{
					"format-version": 2, "location": "s3://warehouse/embed/empty",
					"current-schema-id": 0, "schemas": [{"schema-id": 0, "fields": [
						{"id": 1, "name": "id", "required": true, "type": "long"},
						{"id": 2, "name": "hidden_key", "required": false, "type": "long"},
						{"id": 3, "name": "bucket", "required": false, "type": "int"},
						{"id": 4, "name": "amount", "required": false, "type": "long"}
					]}], "default-spec-id": 0, "partition-specs": [{"spec-id": 0, "fields": []}],
					"current-snapshot-id": %s, "snapshots": []
				}`, currentSnapshot)),
			}, nil
		}},
	}
	installEmbeddedIcebergPlanner(t, c, evolutionSQLPlanner{planner: native, objectRef: fixture.objectRef})
	table := createEmbeddedIcebergTable(t, db, fixture, "empty_orders", "empty_orders", "append_only")
	for _, value := range []string{"-1", "null"} {
		currentSnapshot = value
		require.Equal(t, int64(0), queryInt64(t, db, "select count(*) from "+table), value)
		require.Empty(t, queryIntRows(t, db, "select id, amount from "+table+" where id > 0"), value)
	}
}

func (p evolutionSQLPlanner) PlanScan(ctx context.Context, req api.ScanPlanRequest) (*api.IcebergScanPlan, error) {
	plan, err := p.planner.PlanScan(ctx, req)
	if err == nil {
		plan.ObjectIORef = p.objectRef
	}
	return plan, err
}

// #30404, #30405 and #30406 share one real SQL reader fixture. Metadata and
// object IO are injected; native manifest planning, compaction, compilation and
// the ID-based external reader are all in the executed production path.
func runIcebergSQLEngineEmbeddedSchemaEvolution(t *testing.T, c Cluster, db *embeddedSQLSession, fixture *embeddedIcebergFixture) {
	const base = "s3://warehouse/embed/evolution/"
	const listPath = base + "list.avro"
	const dataManifest = base + "data.avro"
	const deleteManifest = base + "delete.avro"
	const dataPath = base + "old.parquet"
	const deletePath = base + "eq.parquet"
	old := api.Schema{SchemaID: 0, Fields: []api.SchemaField{
		{ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeLong}},
		{ID: 2, Name: "hidden_key", Type: api.IcebergType{Kind: api.TypeLong}},
		{ID: 3, Name: "bucket", Type: api.IcebergType{Kind: api.TypeLong}},
		{ID: 4, Name: "amount", Type: api.IcebergType{Kind: api.TypeLong}},
	}}
	current := api.Schema{SchemaID: 1, Fields: append([]api.SchemaField(nil), old.Fields...)}
	current.Fields[0].Name = "renamed_id"
	current.Fields = append(current.Fields, api.SchemaField{ID: 5, Name: "id", Type: api.IcebergType{Kind: api.TypeLong}})
	snapshotID := int64(10)
	oldSchemaID := 0
	meta := api.TableMetadata{FormatVersion: 2, Location: base, CurrentSchemaID: 1, Schemas: []api.Schema{old, current}, PartitionSpecs: []api.PartitionSpec{{SpecID: 0}}, CurrentSnapshotID: &snapshotID,
		Snapshots: []api.Snapshot{{SnapshotID: 10, SequenceNumber: 10, ManifestList: listPath, SchemaID: &oldSchemaID}}, Refs: map[string]api.SnapshotRef{"main": {SnapshotID: 10, Type: "branch"}}}
	data := embeddedIcebergDataParquet(t, []embeddedIcebergDataRow{{ID: 1, HiddenKey: 10, Bucket: 1, Amount: 10}, {ID: 2, HiddenKey: 20, Bucket: 1, Amount: 20}})
	deleted := embeddedIcebergEqualityDeleteParquet(t, 20)
	encode := func(file api.DataFile, content api.ManifestContent) []byte {
		payload, err := icebergmetadata.EncodeManifest([]api.ManifestEntry{{Status: api.ManifestEntryAdded, SnapshotID: 10, DataFile: file}}, icebergmetadata.ManifestWriteOptions{FormatVersion: 2, Schema: old, PartitionSpec: meta.PartitionSpecs[0], Content: content})
		require.NoError(t, err)
		return payload
	}
	dataBytes := encode(api.DataFile{Content: api.DataFileContentData, FilePath: dataPath, FileFormat: "parquet", RecordCount: 2, FileSizeInBytes: int64(len(data))}, api.ManifestContentData)
	deleteBytes := encode(api.DataFile{Content: api.DataFileContentEqualityDelete, FilePath: deletePath, FileFormat: "parquet", RecordCount: 1, FileSizeInBytes: int64(len(deleted)), EqualityIDs: []int{2}}, api.ManifestContentDeletes)
	manifests := []api.ManifestFile{{Path: dataManifest, Length: int64(len(dataBytes)), Content: api.ManifestContentData, SequenceNumber: 10, MinSequenceNumber: 10}, {Path: deleteManifest, Length: int64(len(deleteBytes)), Content: api.ManifestContentDeletes, SequenceNumber: 9, MinSequenceNumber: 9}}
	list, err := icebergmetadata.EncodeManifestList(manifests, icebergmetadata.ManifestListWriteOptions{FormatVersion: 2, SnapshotID: 10, SequenceNumber: 10})
	require.NoError(t, err)
	objects := evolutionObjectReader{listPath: list, dataManifest: dataBytes, deleteManifest: deleteBytes, dataPath: data, deletePath: deleted}
	writeEmbeddedIcebergObject(t, fixture.objectFS, "warehouse/embed/evolution/old.parquet", data)
	native := icebergmetadata.LocalScanPlanner{Metadata: icebergmetadata.NativeFacade{}, ObjectReader: objects, Catalog: &catalog.MockClient{LoadTableFunc: func(context.Context, api.LoadTableRequest) (*api.LoadTableResponse, error) {
		payload, err := json.Marshal(meta)
		if err != nil {
			return nil, err
		}
		payload = bytes.Replace(payload, []byte(`"Refs":`), []byte(`"refs":`), 1)
		return &api.LoadTableResponse{MetadataLocation: base + "metadata.json", MetadataJSON: payload}, nil
	}}}
	installEmbeddedIcebergPlanner(t, c, evolutionSQLPlanner{planner: native, objectRef: fixture.objectRef})
	table := fixture.databaseName + ".evolution"
	mustExecSQL(t, db, fmt.Sprintf("create external table %s (renamed_id bigint,id bigint,hidden_key bigint,bucket bigint,amount bigint) engine = iceberg with ('catalog'='%s','namespace'='sales','table'='evolution','ref'='main','read_mode'='merge_on_read','write_mode'='read_only')", table, fixture.catalogName))
	query := "select renamed_id,coalesce(id,cast(-1 as bigint)) from " + table + " order by renamed_id"
	want := [][]int64{{1, -1}, {2, -1}}
	require.Equal(t, want, queryIntRows(t, db, query), "schema-only rename must read the old file by field ID")
	options := map[string]string{"min_input_files": "1"}
	selection, err := (maintenance.RewriteDataFilesSelector{ObjectReader: objects}).Select(context.Background(), maintenance.RewriteDataFilesSelectionRequest{Snapshot: meta.Snapshots[0], Options: options})
	require.NoError(t, err)
	compacted, err := (maintenance.ParquetConcatRewriteDataFilesCompactor{ObjectReader: objects}).CompactRewriteDataFiles(context.Background(), maintenance.RewriteDataFilesCompactRequest{Metadata: &meta, Snapshot: meta.Snapshots[0], Selection: *selection, Options: options})
	require.NoError(t, err)
	require.Len(t, compacted.Objects, 1)
	replacement := compacted.Rewrites[0].ReplacementFiles[0]
	writeEmbeddedIcebergObject(t, fixture.objectFS, strings.TrimPrefix(replacement.FilePath, "s3://"), compacted.Objects[0].Payload)
	replacementBytes := encode(replacement, api.ManifestContentData)
	objects[dataManifest] = replacementBytes
	manifests[0].Length = int64(len(replacementBytes))
	manifests[0].SequenceNumber = 11
	manifests[0].MinSequenceNumber = 11
	objects[listPath], err = icebergmetadata.EncodeManifestList(manifests, icebergmetadata.ManifestListWriteOptions{FormatVersion: 2, SnapshotID: 10, SequenceNumber: 11})
	require.NoError(t, err)
	require.Equal(t, want, queryIntRows(t, db, query), "rewriting must preserve rows, renamed values and NULL for the reused name")
}
