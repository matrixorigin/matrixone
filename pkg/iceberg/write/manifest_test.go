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

package write

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
)

func TestBuildAppendManifestsRoundTripsArtifacts(t *testing.T) {
	result, err := BuildAppendManifests(context.Background(), AppendManifestRequest{
		Append:           appendRequest(),
		SnapshotID:       200,
		SequenceNumber:   9,
		TimestampMS:      123456,
		ManifestPath:     "s3://warehouse/sales/orders/metadata/m-1.avro",
		ManifestListPath: "s3://warehouse/sales/orders/metadata/snap-200.avro",
		PreservedManifests: []api.ManifestFile{{
			Path:            "s3://warehouse/sales/orders/metadata/base-manifest.avro",
			Length:          111,
			Content:         api.ManifestContentData,
			AddedSnapshotID: 100,
		}},
	})
	require.NoError(t, err)
	require.Len(t, result.Entries, 1)
	require.NotEmpty(t, result.ManifestBytes)
	require.NotEmpty(t, result.ManifestListBytes)
	require.Equal(t, int64(200), result.ManifestFile.AddedSnapshotID)
	require.Equal(t, 7, result.ManifestFile.PartitionSpecID)
	require.Equal(t, 1, result.ManifestFile.AddedFilesCount)
	require.Equal(t, int64(2), result.ManifestFile.AddedRowsCount)
	require.Len(t, result.Attempt.ManifestFiles, 1)
	require.Equal(t, []string{"add-snapshot", "set-snapshot-ref"}, commitUpdateTypes(result.Attempt.Updates))
	require.NotNil(t, result.Attempt.Updates[0].Snapshot)
	require.Equal(t, int64(200), result.Attempt.Updates[0].Snapshot.SnapshotID)
	require.NotNil(t, result.Attempt.Updates[0].Snapshot.ParentSnapshotID)
	require.Equal(t, int64(100), *result.Attempt.Updates[0].Snapshot.ParentSnapshotID)
	require.Equal(t, int64(9), result.Attempt.Updates[0].Snapshot.SequenceNumber)
	require.NotNil(t, result.Attempt.Updates[0].Snapshot.SchemaID)
	require.Equal(t, 3, *result.Attempt.Updates[0].Snapshot.SchemaID)
	require.Equal(t, int64(123456), result.Attempt.Updates[0].Snapshot.TimestampMS)
	require.Equal(t, "s3://warehouse/sales/orders/metadata/snap-200.avro", result.Attempt.Updates[0].Snapshot.ManifestList)
	require.Equal(t, "append", result.Attempt.Updates[0].Snapshot.Summary["operation"])
	require.Equal(t, "main", result.Attempt.Updates[1].Ref)
	require.Equal(t, "branch", result.Attempt.Updates[1].RefType)
	require.Equal(t, int64(200), result.Attempt.Updates[1].SnapshotID)

	entries, err := metadata.ReadManifest(result.ManifestBytes)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, int64(200), entries[0].SnapshotID)
	require.Equal(t, "s3://warehouse/sales/orders/data/part-1.parquet", entries[0].DataFile.FilePath)

	manifests, err := metadata.ReadManifestList(result.ManifestListBytes)
	require.NoError(t, err)
	require.Len(t, manifests, 2)
	require.Equal(t, "s3://warehouse/sales/orders/metadata/base-manifest.avro", manifests[0].Path)
	require.Equal(t, result.ManifestFile.Path, manifests[1].Path)
	require.Equal(t, result.ManifestFile.Length, manifests[1].Length)
	assertRESTSnapshotCommitUpdates(t, result.Attempt.Updates, int64(200), int64(100), int64(9), 3, "s3://warehouse/sales/orders/metadata/snap-200.avro")
}

func TestBuildAppendManifestsPreservesAllBaseManifestsAndUsesRESTUpdates(t *testing.T) {
	req := appendRequest()
	result, err := BuildAppendManifests(context.Background(), AppendManifestRequest{
		Append:           req,
		SnapshotID:       204,
		SequenceNumber:   13,
		TimestampMS:      999999,
		ManifestPath:     "s3://warehouse/sales/orders/metadata/m-204.avro",
		ManifestListPath: "s3://warehouse/sales/orders/metadata/snap-204.avro",
		PreservedManifests: []api.ManifestFile{{
			Path:                 "s3://warehouse/sales/orders/metadata/base-data-a.avro",
			Length:               111,
			Content:              api.ManifestContentData,
			PartitionSpecID:      7,
			AddedSnapshotID:      90,
			AddedFilesCount:      2,
			AddedRowsCount:       20,
			ManifestPathRedacted: api.RedactPath("s3://warehouse/sales/orders/metadata/base-data-a.avro"),
			ManifestPathHash:     api.PathHash("s3://warehouse/sales/orders/metadata/base-data-a.avro"),
		}, {
			Path:                 "s3://warehouse/sales/orders/metadata/base-data-b.avro",
			Length:               222,
			Content:              api.ManifestContentData,
			PartitionSpecID:      7,
			AddedSnapshotID:      95,
			AddedFilesCount:      1,
			AddedRowsCount:       10,
			ManifestPathRedacted: api.RedactPath("s3://warehouse/sales/orders/metadata/base-data-b.avro"),
			ManifestPathHash:     api.PathHash("s3://warehouse/sales/orders/metadata/base-data-b.avro"),
		}, {
			Path:                 "s3://warehouse/sales/orders/metadata/base-delete.avro",
			Length:               333,
			Content:              api.ManifestContentDeletes,
			PartitionSpecID:      7,
			AddedSnapshotID:      98,
			AddedFilesCount:      1,
			ManifestPathRedacted: api.RedactPath("s3://warehouse/sales/orders/metadata/base-delete.avro"),
			ManifestPathHash:     api.PathHash("s3://warehouse/sales/orders/metadata/base-delete.avro"),
		}},
	})
	require.NoError(t, err)

	manifestList, err := metadata.ReadManifestList(result.ManifestListBytes)
	require.NoError(t, err)
	require.Equal(t, []string{
		"s3://warehouse/sales/orders/metadata/base-data-a.avro",
		"s3://warehouse/sales/orders/metadata/base-data-b.avro",
		"s3://warehouse/sales/orders/metadata/base-delete.avro",
		result.ManifestFile.Path,
	}, manifestFilePaths(manifestList))
	require.Equal(t, []api.ManifestContent{
		api.ManifestContentData,
		api.ManifestContentData,
		api.ManifestContentDeletes,
		api.ManifestContentData,
	}, manifestContents(manifestList))
	assertRESTSnapshotCommitUpdates(t, result.Attempt.Updates, int64(204), req.BaseSnapshotID, int64(13), req.BaseSchemaID, "s3://warehouse/sales/orders/metadata/snap-204.avro")
}

func TestNativeManifestCommitAdapterBuildsFallbackArtifacts(t *testing.T) {
	adapter := SelectManifestCommitAdapter("")
	require.Equal(t, ManifestCommitAdapterNative, adapter.AdapterName())

	result, err := adapter.BuildAppendManifests(context.Background(), AppendManifestRequest{
		Append:           appendRequest(),
		SnapshotID:       201,
		SequenceNumber:   10,
		ManifestPath:     "s3://warehouse/sales/orders/metadata/m-2.avro",
		ManifestListPath: "s3://warehouse/sales/orders/metadata/snap-201.avro",
	})
	require.NoError(t, err)
	require.Equal(t, int64(201), result.ManifestFile.AddedSnapshotID)
	require.Equal(t, []string{"add-snapshot", "set-snapshot-ref"}, commitUpdateTypes(result.Attempt.Updates))
}

func TestUnsupportedManifestCommitAdapterFailsFast(t *testing.T) {
	adapter := SelectManifestCommitAdapter("iceberg-go")
	require.Equal(t, "iceberg-go", adapter.AdapterName())

	_, err := adapter.BuildAppendManifests(context.Background(), AppendManifestRequest{
		Append:           appendRequest(),
		SnapshotID:       202,
		SequenceNumber:   11,
		ManifestPath:     "s3://warehouse/sales/orders/metadata/m-3.avro",
		ManifestListPath: "s3://warehouse/sales/orders/metadata/snap-202.avro",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "ICEBERG_UNSUPPORTED_FEATURE")
	require.Contains(t, err.Error(), "adapter=iceberg-go")
}

func TestManifestAttemptBuilderFeedsAppendWorkflow(t *testing.T) {
	builder := &ManifestAttemptBuilder{
		ManifestPath:     "s3://warehouse/sales/orders/metadata/m-4.avro",
		ManifestListPath: "s3://warehouse/sales/orders/metadata/snap-203.avro",
		SnapshotID:       203,
		SequenceNumber:   12,
	}
	committer := &fakeCommitter{results: []commitOutcome{{
		result: &api.CommitResult{SnapshotID: 203, MetadataLocationHash: "hash-203", CommitID: "commit-203"},
	}}}
	workflow := AppendWorkflow{Builder: builder, Committer: committer}

	result, err := workflow.CommitAppend(context.Background(), appendRequest())
	require.NoError(t, err)
	require.Equal(t, int64(203), result.SnapshotID)
	require.NotNil(t, builder.LastResult)
	require.Len(t, committer.requests, 1)
	require.Equal(t, []string{"add-snapshot", "set-snapshot-ref"}, commitUpdateTypes(committer.requests[0].Updates))
}

func commitUpdateTypes(updates []api.CommitUpdate) []string {
	out := make([]string, 0, len(updates))
	for _, update := range updates {
		out = append(out, update.Type)
	}
	return out
}

func assertRESTSnapshotCommitUpdates(t *testing.T, updates []api.CommitUpdate, snapshotID, parentSnapshotID, sequenceNumber int64, schemaID int, manifestListPath string) {
	t.Helper()
	require.Equal(t, []string{"add-snapshot", "set-snapshot-ref"}, commitUpdateTypes(updates))
	for _, update := range updates {
		require.NotContains(t, []string{"add-manifest", "set-manifest-list"}, update.Type)
	}
	require.NotNil(t, updates[0].Snapshot)
	snapshot := updates[0].Snapshot
	require.Equal(t, snapshotID, snapshot.SnapshotID)
	if parentSnapshotID > 0 {
		require.NotNil(t, snapshot.ParentSnapshotID)
		require.Equal(t, parentSnapshotID, *snapshot.ParentSnapshotID)
	} else {
		require.Nil(t, snapshot.ParentSnapshotID)
	}
	require.Equal(t, sequenceNumber, snapshot.SequenceNumber)
	require.NotZero(t, snapshot.TimestampMS)
	require.NotNil(t, snapshot.SchemaID)
	require.Equal(t, schemaID, *snapshot.SchemaID)
	require.Equal(t, manifestListPath, snapshot.ManifestList)
	require.NotEmpty(t, snapshot.Summary)
	require.Equal(t, "set-snapshot-ref", updates[1].Type)
	require.Equal(t, snapshotID, updates[1].SnapshotID)
}

func manifestContents(manifests []api.ManifestFile) []api.ManifestContent {
	out := make([]api.ManifestContent, 0, len(manifests))
	for _, manifest := range manifests {
		out = append(out, manifest.Content)
	}
	return out
}

func manifestFilePaths(manifests []api.ManifestFile) []string {
	out := make([]string, 0, len(manifests))
	for _, manifest := range manifests {
		out = append(out, manifest.Path)
	}
	return out
}
