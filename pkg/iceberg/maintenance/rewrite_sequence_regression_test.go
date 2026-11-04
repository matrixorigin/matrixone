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
	"bytes"
	"context"
	"github.com/parquet-go/parquet-go"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
)

// Rewriting must apply exactly the same sequence rules as an ordinary scan.
// Added v2 manifest entries inherit their sequence from the manifest list.
func TestRewriteDataFilesInheritedDeleteSequence(t *testing.T) {
	for _, tc := range []struct {
		name           string
		dataStatus     api.ManifestEntryStatus
		deleteStatus   api.ManifestEntryStatus
		deleteSequence int64
		dataSequence   int64
		want           []int64
	}{
		{"explicit_older_control", api.ManifestEntryExisting, api.ManifestEntryExisting, 9, 10, []int64{1, 2}},
		{"inherited_older_delete", api.ManifestEntryExisting, api.ManifestEntryAdded, 9, 10, []int64{1, 2}},
		{"inherited_same_sequence_delete", api.ManifestEntryExisting, api.ManifestEntryAdded, 10, 10, []int64{1, 2}},
		{"inherited_newer_delete_control", api.ManifestEntryExisting, api.ManifestEntryAdded, 11, 10, []int64{2}},
		{"inherited_data_older_delete", api.ManifestEntryAdded, api.ManifestEntryExisting, 9, 10, []int64{1, 2}},
		{"zero_delete_newer_data", api.ManifestEntryExisting, api.ManifestEntryExisting, 0, 10, []int64{1, 2}},
		{"zero_data_newer_delete", api.ManifestEntryExisting, api.ManifestEntryExisting, 1, 0, []int64{2}},
		{"both_zero_equality", api.ManifestEntryExisting, api.ManifestEntryExisting, 0, 0, []int64{1, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			const listPath = "s3://warehouse/orders/metadata/list.avro"
			const dataManifest = "s3://warehouse/orders/metadata/data.avro"
			const deleteManifest = "s3://warehouse/orders/metadata/delete.avro"
			const dataPath = "s3://warehouse/orders/data/input.parquet"
			const deletePath = "s3://warehouse/orders/delete/eq.parquet"
			data := rewriteDataFilesParquetBytes(t, []int64{1, 2})
			deleted := rewriteDataFilesParquetBytes(t, []int64{1})
			meta := maintenanceTestTableMetadata("s3://warehouse/orders")
			encode := func(status api.ManifestEntryStatus, sequence int64, file api.DataFile, content api.ManifestContent) []byte {
				payload, err := metadata.EncodeManifest([]api.ManifestEntry{{
					Status: status, SnapshotID: sequence, SequenceNumber: sequence, FileSequence: sequence, DataFile: file,
				}}, metadata.ManifestWriteOptions{FormatVersion: 2, Schema: meta.Schemas[0], PartitionSpec: meta.PartitionSpecs[0], Content: content})
				require.NoError(t, err)
				return payload
			}
			dataBytes := encode(tc.dataStatus, tc.dataSequence, api.DataFile{Content: api.DataFileContentData, FilePath: dataPath, FileFormat: "parquet", RecordCount: 2, FileSizeInBytes: int64(len(data))}, api.ManifestContentData)
			deleteBytes := encode(tc.deleteStatus, tc.deleteSequence, api.DataFile{Content: api.DataFileContentEqualityDelete, FilePath: deletePath, FileFormat: "parquet", RecordCount: 1, FileSizeInBytes: int64(len(deleted)), EqualityIDs: []int{1}}, api.ManifestContentDeletes)
			listBytes, err := metadata.EncodeManifestList([]api.ManifestFile{
				{Path: dataManifest, Length: int64(len(dataBytes)), Content: api.ManifestContentData, SequenceNumber: tc.dataSequence, MinSequenceNumber: tc.dataSequence, AddedSnapshotID: 10},
				{Path: deleteManifest, Length: int64(len(deleteBytes)), Content: api.ManifestContentDeletes, SequenceNumber: tc.deleteSequence, MinSequenceNumber: tc.deleteSequence, AddedSnapshotID: tc.deleteSequence},
			}, metadata.ManifestListWriteOptions{FormatVersion: 2, SnapshotID: 20, SequenceNumber: 20})
			require.NoError(t, err)
			reader := fakeRewriteManifestObjectReader{listPath: listBytes, dataManifest: dataBytes, deleteManifest: deleteBytes, dataPath: data, deletePath: deleted}
			snapshot := api.Snapshot{SnapshotID: 20, SequenceNumber: 20, ManifestList: listPath}
			options := map[string]string{"min_input_files": "1"}
			selection, err := (RewriteDataFilesSelector{ObjectReader: reader}).Select(ctx, RewriteDataFilesSelectionRequest{Snapshot: snapshot, Options: options})
			require.NoError(t, err)
			require.Len(t, selection.Groups, 1)
			result, err := (ParquetConcatRewriteDataFilesCompactor{ObjectReader: reader}).CompactRewriteDataFiles(ctx, RewriteDataFilesCompactRequest{Metadata: meta, Snapshot: snapshot, Selection: *selection, JobID: "loom-sequence", IdempotencyKey: "loom-sequence", Options: options})
			require.NoError(t, err)
			require.Len(t, result.Objects, 1)
			got := rewriteDataFilesParquetInt64Values(t, result.Objects[0].Payload)
			t.Logf("data sequence=10, delete sequence=%d, data status=%d, delete status=%d: rewritten rows=%v, expected=%v", tc.deleteSequence, tc.dataStatus, tc.deleteStatus, got, tc.want)
			require.Equal(t, tc.want, got, "compaction must preserve rows outside the equality delete's sequence scope")
		})
	}
}

func TestRewritePreservesResolvedManifestSequences(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sequence int64
		explicit bool
	}{
		{"inherited_added", 10, false}, {"explicit_zero_added", 0, true}, {"explicit_older_added", 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			meta := maintenanceTestTableMetadata("s3://warehouse/orders")
			raw := api.ManifestEntry{Status: api.ManifestEntryAdded, SequenceNumber: tc.sequence, FileSequence: tc.sequence, SequenceNumberSet: tc.explicit, FileSequenceSet: tc.explicit, DataFile: api.DataFile{FilePath: "s3://warehouse/orders/data/a.parquet", FileFormat: "parquet", RecordCount: 1, FileSizeInBytes: 10}}
			manifest := api.ManifestFile{Path: "s3://warehouse/orders/metadata/a.avro", SequenceNumber: 10, Content: api.ManifestContentData}
			normalized, err := metadata.NormalizeManifestEntry(raw, manifest)
			require.NoError(t, err)
			var memory int64
			manifests, objects, err := materializeRewriteDataFilesPreservedManifestsWithBudget(meta, "s3://warehouse/orders/metadata/new", []RewriteDataFilesPreservedManifest{{Manifest: manifest, Entries: []api.ManifestEntry{normalized}}}, nil, &memory, defaultRewriteDataFilesMaxRewriteBytes)
			require.NoError(t, err)
			require.Len(t, manifests, 1)
			require.Len(t, objects, 1)
			decoded, err := metadata.ReadManifest(objects[0].Payload)
			require.NoError(t, err)
			retained, err := metadata.NormalizeManifestEntry(decoded[0], api.ManifestFile{SequenceNumber: 99})
			require.NoError(t, err)
			require.Equal(t, tc.sequence, retained.SequenceNumber)
			require.Equal(t, tc.sequence, retained.FileSequence)
			existing, err := liveRewriteManifestEntries([]api.ManifestEntry{raw}, manifest)
			require.NoError(t, err)
			require.Equal(t, api.ManifestEntryExisting, existing[0].Status)
			payload, err := metadata.EncodeManifest(existing, metadata.ManifestWriteOptions{FormatVersion: 2, Schema: meta.Schemas[0], PartitionSpec: meta.PartitionSpecs[0], Content: api.ManifestContentData})
			require.NoError(t, err)
			decoded, err = metadata.ReadManifest(payload)
			require.NoError(t, err)
			retained, err = metadata.NormalizeManifestEntry(decoded[0], api.ManifestFile{SequenceNumber: 99})
			require.NoError(t, err)
			require.Equal(t, tc.sequence, retained.SequenceNumber)
			require.Equal(t, tc.sequence, retained.FileSequence)
		})
	}
}

func TestRewriteDeleteSequenceBoundaries(t *testing.T) {
	for _, tc := range []struct {
		data, deleted      int64
		equality, position bool
	}{
		{0, 0, false, true}, {1, 0, false, false}, {0, 1, true, true}, {10, 9, false, false}, {10, 10, false, true}, {10, 11, true, true},
	} {
		data := api.DataFile{SequenceNumber: tc.data}
		require.Equal(t, tc.equality, rewriteDataFilesDeleteSequenceApplies(data, api.DataFile{Content: api.DataFileContentEqualityDelete, SequenceNumber: tc.deleted}))
		require.Equal(t, tc.position, rewriteDataFilesDeleteSequenceApplies(data, api.DataFile{Content: api.DataFileContentPositionDelete, SequenceNumber: tc.deleted}))
	}
}

func TestRewriteAppliesDeleteBeforeDroppingKey(t *testing.T) {
	const dataPath = "s3://warehouse/orders/data/old.parquet"
	const deletePath = "s3://warehouse/orders/delete/eq.parquet"
	const manifestPath = "s3://warehouse/orders/metadata/deletes.avro"
	var data bytes.Buffer
	writer := parquet.NewWriter(&data, parquet.NewSchema("old", parquet.Group{"id": parquet.FieldID(parquet.Int(64), 1), "payload": parquet.FieldID(parquet.Int(64), 2)}))
	_, err := writer.WriteRows([]parquet.Row{{parquet.Int64Value(1).Level(0, 0, 0), parquet.Int64Value(10).Level(0, 0, 1)}, {parquet.Int64Value(2).Level(0, 0, 0), parquet.Int64Value(20).Level(0, 0, 1)}})
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	meta := maintenanceTestTableMetadata("s3://warehouse/orders")
	// Drop key ID 1 and rename payload ID 2 to the old key's name.
	meta.Schemas = append(meta.Schemas, api.Schema{SchemaID: 1, Fields: []api.SchemaField{{ID: 2, Name: "id", Type: api.IcebergType{Kind: api.TypeLong}}}})
	meta.CurrentSchemaID = 1
	deletes := rewriteDataFilesParquetBytes(t, []int64{1})
	manifest, err := metadata.EncodeManifest([]api.ManifestEntry{{Status: api.ManifestEntryAdded, DataFile: api.DataFile{Content: api.DataFileContentEqualityDelete, FilePath: deletePath, FileFormat: "parquet", RecordCount: 1, FileSizeInBytes: int64(len(deletes)), EqualityIDs: []int{1}}}}, metadata.ManifestWriteOptions{FormatVersion: 2, Schema: meta.Schemas[0], PartitionSpec: meta.PartitionSpecs[0], Content: api.ManifestContentDeletes})
	require.NoError(t, err)
	result, err := (ParquetConcatRewriteDataFilesCompactor{ObjectReader: fakeRewriteManifestObjectReader{dataPath: data.Bytes(), deletePath: deletes, manifestPath: manifest}}).CompactRewriteDataFiles(context.Background(), RewriteDataFilesCompactRequest{Metadata: meta, Snapshot: api.Snapshot{SnapshotID: 10}, Selection: RewriteDataFilesSelection{DeleteManifestCount: 1, DeleteManifests: []api.ManifestFile{{Path: manifestPath, Content: api.ManifestContentDeletes, SequenceNumber: 11}}, Groups: []RewriteDataFileGroup{{Candidates: []RewriteDataFileCandidate{{File: api.DataFile{FilePath: dataPath, FileFormat: "parquet", RecordCount: 2, FileSizeInBytes: int64(data.Len()), SequenceNumber: 10}}}}}}})
	require.NoError(t, err)
	require.Len(t, result.Objects, 1)
	require.Equal(t, []int64{20}, rewriteDataFilesParquetInt64Values(t, result.Objects[0].Payload))
}
