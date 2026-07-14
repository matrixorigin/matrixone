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
	"io"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

func TestRewriteDataFilesSelectorGroupsSmallCurrentDataFiles(t *testing.T) {
	dataManifestPath := "s3://warehouse/orders/metadata/data-manifest.avro"
	deleteManifestPath := "s3://warehouse/orders/metadata/delete-manifest.avro"
	manifestListPath := "s3://warehouse/orders/metadata/snap-10.avro"
	dataEntries := []api.ManifestEntry{
		rewriteDataFileEntry("s3://warehouse/orders/data/part-1.parquet", 40, map[string]any{"region": "ksa"}, api.ManifestEntryExisting),
		rewriteDataFileEntry("s3://warehouse/orders/data/part-2.parquet", 50, map[string]any{"region": "ksa"}, api.ManifestEntryAdded),
		rewriteDataFileEntry("s3://warehouse/orders/data/part-large.parquet", 180, map[string]any{"region": "ksa"}, api.ManifestEntryExisting),
		rewriteDataFileEntry("s3://warehouse/orders/data/part-us.parquet", 30, map[string]any{"region": "us"}, api.ManifestEntryExisting),
		rewriteDataFileEntry("s3://warehouse/orders/data/part-us-2.parquet", 35, map[string]any{"region": "us"}, api.ManifestEntryDeleted),
	}
	dataManifestBytes, err := encodeMaintenanceTestManifest(dataEntries)
	require.NoError(t, err)
	deleteManifestBytes, err := encodeMaintenanceTestManifest([]api.ManifestEntry{{
		Status: api.ManifestEntryAdded,
		DataFile: api.DataFile{
			Content:         api.DataFileContentPositionDelete,
			FilePath:        "s3://warehouse/orders/delete/pos-1.parquet",
			FileFormat:      "parquet",
			FileSizeInBytes: 10,
		},
	}})
	require.NoError(t, err)
	manifestListBytes, err := encodeMaintenanceTestManifestList([]api.ManifestFile{
		{
			Path:              dataManifestPath,
			Length:            int64(len(dataManifestBytes)),
			Content:           api.ManifestContentData,
			ExistingRowsCount: 300,
		},
		{
			Path:    deleteManifestPath,
			Length:  int64(len(deleteManifestBytes)),
			Content: api.ManifestContentDeletes,
		},
	})
	require.NoError(t, err)

	selection, err := (RewriteDataFilesSelector{
		ObjectReader: fakeRewriteManifestObjectReader{
			manifestListPath:   manifestListBytes,
			dataManifestPath:   dataManifestBytes,
			deleteManifestPath: deleteManifestBytes,
		},
	}).Select(context.Background(), RewriteDataFilesSelectionRequest{
		Snapshot: api.Snapshot{SnapshotID: 10, ManifestList: manifestListPath},
		Options:  map[string]string{"target_file_size": "100", "min_input_files": "2"},
	})
	require.NoError(t, err)
	require.Equal(t, 1, selection.ScannedManifestCount)
	require.Equal(t, 1, selection.DeleteManifestCount)
	require.Len(t, selection.DeleteManifests, 1)
	require.Equal(t, deleteManifestPath, selection.DeleteManifests[0].Path)
	require.Len(t, selection.PreservedDataManifests, 1)
	require.Equal(t, dataManifestPath, selection.PreservedDataManifests[0].Manifest.Path)
	require.Len(t, selection.PreservedManifests, 1)
	require.Equal(t, deleteManifestPath, selection.PreservedManifests[0].Path)
	require.Equal(t, uint64(4), selection.ScannedFileCount)
	require.Equal(t, uint64(3), selection.CandidateFileCount)
	require.Equal(t, int64(120), selection.CandidateSizeBytes)
	require.Len(t, selection.Groups, 1)
	group := selection.Groups[0]
	require.Equal(t, rewriteDataFilesGroupKey(group.Candidates[0].File), group.PartitionKey)
	require.Equal(t, int64(90), group.TotalSizeBytes)
	require.Equal(t, int64(90), group.TotalRecords)
	require.Len(t, group.Candidates, 2)
	require.Equal(t, "s3://warehouse/orders/data/part-1.parquet", group.Candidates[0].File.FilePath)
	require.Equal(t, dataManifestPath, group.Candidates[0].ManifestPath)
	require.Equal(t, "s3://warehouse/orders/data/part-2.parquet", group.Candidates[1].File.FilePath)
}

func TestRewriteDataFilesSelectorChunksByMaxGroupSize(t *testing.T) {
	manifestPath := "s3://warehouse/orders/metadata/data-manifest.avro"
	manifestListPath := "s3://warehouse/orders/metadata/snap-10.avro"
	entries := []api.ManifestEntry{
		rewriteDataFileEntry("s3://warehouse/orders/data/part-1.parquet", 70, map[string]any{"day": int32(1)}, api.ManifestEntryExisting),
		rewriteDataFileEntry("s3://warehouse/orders/data/part-2.parquet", 80, map[string]any{"day": int32(1)}, api.ManifestEntryExisting),
		rewriteDataFileEntry("s3://warehouse/orders/data/part-3.parquet", 60, map[string]any{"day": int32(1)}, api.ManifestEntryExisting),
	}
	manifestBytes, err := encodeMaintenanceTestManifest(entries)
	require.NoError(t, err)
	manifestListBytes, err := encodeMaintenanceTestManifestList([]api.ManifestFile{{Path: manifestPath, Length: int64(len(manifestBytes)), Content: api.ManifestContentData}})
	require.NoError(t, err)

	selection, err := (RewriteDataFilesSelector{
		ObjectReader: fakeRewriteManifestObjectReader{
			manifestListPath: manifestListBytes,
			manifestPath:     manifestBytes,
		},
	}).Select(context.Background(), RewriteDataFilesSelectionRequest{
		Snapshot: api.Snapshot{SnapshotID: 10, ManifestList: manifestListPath},
		Options: map[string]string{
			"target_file_size": "100",
			"min_input_files":  "1",
			"max_group_size":   "150",
		},
	})
	require.NoError(t, err)
	require.Len(t, selection.Groups, 2)
	require.Equal(t, rewriteDataFilesGroupKey(selection.Groups[0].Candidates[0].File), selection.Groups[0].PartitionKey)
	require.Equal(t, int64(150), selection.Groups[0].TotalSizeBytes)
	require.Len(t, selection.Groups[0].Candidates, 2)
	require.Equal(t, int64(60), selection.Groups[1].TotalSizeBytes)
	require.Len(t, selection.Groups[1].Candidates, 1)
}

func TestRewriteDataFilesPartitionKeyNormalizesIntegerWidths(t *testing.T) {
	left := rewriteDataFilesPartitionKey(map[string]any{"day": int32(1), "region": "ksa"})
	right := rewriteDataFilesPartitionKey(map[string]any{"day": int64(1), "region": "ksa"})
	require.Equal(t, left, right)
	require.NotEqual(t,
		rewriteDataFilesPartitionKey(map[string]any{"a": "x;b=s:y", "b": "z"}),
		rewriteDataFilesPartitionKey(map[string]any{"a": "x", "b": "y;b=s:z"}),
	)
}

func TestBuildRewriteDataFilesManifestCommitMaterializesSnapshotUpdate(t *testing.T) {
	sourceOne := rewriteDataFileEntry("s3://warehouse/orders/data/part-1.parquet", 40, map[string]any{"region": "ksa"}, api.ManifestEntryExisting)
	sourceTwo := rewriteDataFileEntry("s3://warehouse/orders/data/part-2.parquet", 50, map[string]any{"region": "ksa"}, api.ManifestEntryExisting)
	group := RewriteDataFileGroup{
		PartitionSpecID: 0,
		PartitionKey:    "0|region=s:ksa",
		Candidates: []RewriteDataFileCandidate{
			{ManifestPath: "s3://warehouse/orders/metadata/data-manifest.avro", Entry: sourceOne, File: sourceOne.DataFile},
			{ManifestPath: "s3://warehouse/orders/metadata/data-manifest.avro", Entry: sourceTwo, File: sourceTwo.DataFile},
		},
		TotalSizeBytes: 90,
		TotalRecords:   90,
	}
	replacement := api.DataFile{
		Content:         api.DataFileContentData,
		FilePath:        "s3://warehouse/orders/data/compact-1.parquet",
		FileFormat:      "parquet",
		Partition:       map[string]any{"region": "ksa"},
		RecordCount:     90,
		FileSizeInBytes: 88,
		SpecID:          0,
	}

	result, err := BuildRewriteDataFilesManifestCommit(RewriteDataFilesMaterializeRequest{
		Snapshot:      api.Snapshot{SnapshotID: 10, ManifestList: "s3://warehouse/orders/metadata/snap-10.avro"},
		FormatVersion: 2,
		Schema: api.Schema{SchemaID: 9, Fields: []api.SchemaField{
			{ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeLong}},
			{ID: 2, Name: "region", Type: api.IcebergType{Kind: api.TypeString}},
		}},
		PartitionSpecs: []api.PartitionSpec{{SpecID: 0, Fields: []api.PartitionField{{
			SourceID: 2, FieldID: 1000, Name: "region", Transform: "identity",
		}}}},
		SnapshotID:         11,
		SequenceNumber:     12,
		TimestampMS:        123456,
		SchemaID:           9,
		TargetRef:          "compact",
		TargetRefType:      "branch",
		TargetRefRetention: api.SnapshotRef{MinSnapshotsToKeep: 3, MaxSnapshotAgeMS: 1_000, MaxRefAgeMS: 2_000},
		IdempotencyKey:     "idem-rdf",
		DataManifestPath:   "s3://warehouse/orders/metadata/mo-rewrite-data-files/rw-1/data-manifest.avro",
		ManifestListPath:   "s3://warehouse/orders/metadata/mo-rewrite-data-files/rw-1/manifest-list.avro",
		PreservedManifests: []api.ManifestFile{{
			Path:    "s3://warehouse/orders/metadata/data-manifest.avro",
			Length:  123,
			Content: api.ManifestContentData,
		}},
		Summary:  map[string]string{"custom": "kept"},
		Rewrites: []RewriteDataFileRewrite{{Group: group, ReplacementFiles: []api.DataFile{replacement}}},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), result.RewrittenFiles)
	require.Equal(t, uint64(1), result.AddedFiles)
	require.Len(t, result.Entries, 3)
	require.Equal(t, api.ManifestEntryDeleted, result.Entries[0].Status)
	require.Equal(t, api.ManifestEntryDeleted, result.Entries[1].Status)
	require.Equal(t, api.ManifestEntryAdded, result.Entries[2].Status)
	require.Equal(t, int64(10), result.Entries[0].SequenceNumber)
	require.Equal(t, int64(10), result.Entries[0].FileSequence)
	require.Equal(t, int64(11), result.Entries[2].SnapshotID)
	require.Equal(t, int64(12), result.Entries[2].SequenceNumber)
	require.Equal(t, int64(90), result.ManifestFile.AddedRowsCount)
	require.Equal(t, int64(90), result.ManifestFile.DeletedRowsCount)
	require.Equal(t, int64(88), result.ManifestFile.AddedFilesSizeInBytes)
	require.Equal(t, int64(90), result.ManifestFile.DeletedFilesSizeInBytes)

	entries, err := metadata.ReadManifest(result.ManifestBytes)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	require.Equal(t, "s3://warehouse/orders/data/compact-1.parquet", entries[2].DataFile.FilePath)
	manifestList, err := metadata.ReadManifestList(result.ManifestListBytes)
	require.NoError(t, err)
	require.Len(t, manifestList, 2)
	require.Equal(t, "s3://warehouse/orders/metadata/data-manifest.avro", manifestList[0].Path)
	require.Equal(t, result.ManifestFile.Path, manifestList[1].Path)

	require.Equal(t, "compact", result.Attempt.TargetRef)
	require.Equal(t, "idem-rdf", result.Attempt.IdempotencyKey)
	require.Equal(t, []api.CommitRequirement{{Type: "assert-ref-snapshot-id", Ref: "compact", SnapshotID: 10}}, result.Attempt.Requirements)
	require.Equal(t, []string{"add-snapshot", "set-snapshot-ref"}, rewriteDataFileCommitUpdateTypes(result.Attempt.Updates))
	require.NotNil(t, result.Attempt.Updates[0].Snapshot)
	require.Equal(t, int64(11), result.Attempt.Updates[0].Snapshot.SnapshotID)
	require.NotNil(t, result.Attempt.Updates[0].Snapshot.ParentSnapshotID)
	require.Equal(t, int64(10), *result.Attempt.Updates[0].Snapshot.ParentSnapshotID)
	require.Equal(t, int64(12), result.Attempt.Updates[0].Snapshot.SequenceNumber)
	require.NotNil(t, result.Attempt.Updates[0].Snapshot.SchemaID)
	require.Equal(t, 9, *result.Attempt.Updates[0].Snapshot.SchemaID)
	require.Equal(t, int64(123456), result.Attempt.Updates[0].Snapshot.TimestampMS)
	require.Equal(t, "s3://warehouse/orders/metadata/mo-rewrite-data-files/rw-1/manifest-list.avro", result.Attempt.Updates[0].Snapshot.ManifestList)
	require.Equal(t, int64(11), result.Attempt.Updates[1].SnapshotID)
	require.Equal(t, 3, result.Attempt.Updates[1].MinSnapshotsToKeep)
	require.Equal(t, int64(1_000), result.Attempt.Updates[1].MaxSnapshotAgeMS)
	require.Equal(t, int64(2_000), result.Attempt.Updates[1].MaxRefAgeMS)
	require.Equal(t, "2", result.Attempt.Summary["rewritten-files"])
	require.Equal(t, "1", result.Attempt.Summary["added-files"])
	require.Equal(t, "kept", result.Attempt.Summary["custom"])
}

func TestBuildRewriteDataFilesManifestCommitSeparatesPartitionSpecs(t *testing.T) {
	groupForSpec := func(specID int, suffix string) RewriteDataFileRewrite {
		source := rewriteDataFileEntry("s3://warehouse/orders/data/source-"+suffix+".parquet", 10, nil, api.ManifestEntryExisting)
		source.DataFile.SpecID = specID
		replacement := rewriteDataFileEntry("s3://warehouse/orders/data/replacement-"+suffix+".parquet", 10, nil, api.ManifestEntryAdded).DataFile
		replacement.SpecID = specID
		return RewriteDataFileRewrite{
			Group: RewriteDataFileGroup{
				PartitionSpecID: specID,
				Candidates: []RewriteDataFileCandidate{{
					Entry: source,
					File:  source.DataFile,
				}},
			},
			ReplacementFiles: []api.DataFile{replacement},
		}
	}
	result, err := BuildRewriteDataFilesManifestCommit(RewriteDataFilesMaterializeRequest{
		Snapshot:         api.Snapshot{SnapshotID: 10},
		FormatVersion:    2,
		Schema:           api.Schema{SchemaID: 9, Fields: []api.SchemaField{{ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeLong}}}},
		PartitionSpecs:   []api.PartitionSpec{{SpecID: 0}, {SpecID: 1}},
		SnapshotID:       11,
		SequenceNumber:   12,
		SchemaID:         9,
		IdempotencyKey:   "idem-multi-spec",
		DataManifestPath: "s3://warehouse/orders/metadata/data-manifest.avro",
		ManifestListPath: "s3://warehouse/orders/metadata/manifest-list.avro",
		Rewrites:         []RewriteDataFileRewrite{groupForSpec(1, "one"), groupForSpec(0, "zero")},
	})
	require.NoError(t, err)
	require.Len(t, result.ManifestFiles, 2)
	require.Len(t, result.ManifestObjects, 2)
	require.Equal(t, []int{0, 1}, []int{result.ManifestFiles[0].PartitionSpecID, result.ManifestFiles[1].PartitionSpecID})
	require.Contains(t, result.ManifestFiles[0].Path, "data-manifest-spec-0.avro")
	require.Contains(t, result.ManifestFiles[1].Path, "data-manifest-spec-1.avro")
	require.Len(t, result.Attempt.ManifestFiles, 2)
	manifestList, err := metadata.ReadManifestList(result.ManifestListBytes)
	require.NoError(t, err)
	require.Len(t, manifestList, 2)
	for idx, object := range result.ManifestObjects {
		entries, readErr := metadata.ReadManifest(object.Payload)
		require.NoError(t, readErr)
		require.Len(t, entries, 2)
		for _, entry := range entries {
			require.Zero(t, entry.DataFile.SpecID, "partition spec id is carried by the manifest list, not the v2 data_file struct")
		}
		require.Equal(t, idx, result.ManifestFiles[idx].PartitionSpecID)
	}
}

func TestBuildRewriteDataFilesManifestCommitValidatesReplacementFiles(t *testing.T) {
	source := rewriteDataFileEntry("s3://warehouse/orders/data/part-1.parquet", 40, map[string]any{"region": "ksa"}, api.ManifestEntryExisting)
	_, err := BuildRewriteDataFilesManifestCommit(RewriteDataFilesMaterializeRequest{
		Snapshot:         api.Snapshot{SnapshotID: 10},
		SnapshotID:       11,
		SequenceNumber:   11,
		TargetRef:        "main",
		IdempotencyKey:   "idem-rdf",
		DataManifestPath: "s3://warehouse/orders/metadata/data-manifest.avro",
		ManifestListPath: "s3://warehouse/orders/metadata/manifest-list.avro",
		Rewrites: []RewriteDataFileRewrite{{
			Group: RewriteDataFileGroup{Candidates: []RewriteDataFileCandidate{{File: source.DataFile}}},
		}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires replacement files")
}

func TestNativeRewriteDataFilesPlannerBuildsCommitPlanWithInjectedCompactor(t *testing.T) {
	manifestPath := "s3://warehouse/orders/metadata/data-manifest.avro"
	manifestListPath := "s3://warehouse/orders/metadata/snap-4.avro"
	entries := []api.ManifestEntry{
		rewriteDataFileEntry("s3://warehouse/orders/data/part-1.parquet", 40, map[string]any{"region": "ksa"}, api.ManifestEntryExisting),
		rewriteDataFileEntry("s3://warehouse/orders/data/part-2.parquet", 50, map[string]any{"region": "ksa"}, api.ManifestEntryExisting),
		rewriteDataFileEntry("s3://warehouse/orders/data/part-large.parquet", 200, map[string]any{"region": "ksa"}, api.ManifestEntryExisting),
	}
	manifestBytes, err := encodeMaintenanceTestManifest(entries)
	require.NoError(t, err)
	manifestListBytes, err := encodeMaintenanceTestManifestList([]api.ManifestFile{{Path: manifestPath, Length: int64(len(manifestBytes)), Content: api.ManifestContentData}})
	require.NoError(t, err)
	meta := expireMetadata(4)
	meta.Snapshots[3].SequenceNumber = 7
	replacement := api.DataFile{
		Content:         api.DataFileContentData,
		FilePath:        "s3://warehouse/orders/data/compact-1.parquet",
		FileFormat:      "parquet",
		Partition:       map[string]any{"region": "ksa"},
		RecordCount:     90,
		FileSizeInBytes: 88,
	}

	var compactReq RewriteDataFilesCompactRequest
	plan, err := (NativeRewriteDataFilesPlanner{
		Catalog: api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
		Loader:  MaintenanceTableMetadataLoaderFunc(func(ctx context.Context, req Request) (*api.TableMetadata, error) { return meta, nil }),
		Now:     func() time.Time { return time.Unix(0, 9_000) },
		Selector: RewriteDataFilesSelector{ObjectReader: fakeRewriteManifestObjectReader{
			manifestListPath: manifestListBytes,
			manifestPath:     manifestBytes,
		}},
		Compactor: RewriteDataFilesCompactorFunc(func(ctx context.Context, req RewriteDataFilesCompactRequest) (*RewriteDataFilesCompactResult, error) {
			compactReq = req
			require.Len(t, req.Selection.Groups, 1)
			return &RewriteDataFilesCompactResult{
				Rewrites: []RewriteDataFileRewrite{{
					Group:            req.Selection.Groups[0],
					ReplacementFiles: []api.DataFile{replacement},
				}},
				Objects: []ObjectWrite{{Location: replacement.FilePath, Payload: []byte("parquet bytes")}},
			}, nil
		}),
	}).BuildMaintenanceCommit(context.Background(), rewriteDataFilesRequest("ref=main,target_file_size=100,min_input_files=2"))
	require.NoError(t, err)
	require.Equal(t, int64(4), compactReq.Snapshot.SnapshotID)
	require.Equal(t, uint64(2), compactReq.Selection.CandidateFileCount)
	require.NotNil(t, plan.Attempt)
	require.Equal(t, "main", plan.Attempt.TargetRef)
	require.Equal(t, int64(4), plan.Attempt.BaseSnapshotID)
	require.Equal(t, []api.CommitRequirement{{Type: "assert-ref-snapshot-id", Ref: "main", SnapshotID: 4}}, plan.Attempt.Requirements)
	require.Equal(t, []string{"add-snapshot", "set-snapshot-ref"}, rewriteDataFileCommitUpdateTypes(plan.Attempt.Updates))
	require.NotNil(t, plan.Attempt.Updates[0].Snapshot)
	require.Equal(t, int64(9_000), plan.Attempt.Updates[0].Snapshot.SnapshotID)
	require.NotNil(t, plan.Attempt.Updates[0].Snapshot.ParentSnapshotID)
	require.Equal(t, int64(4), *plan.Attempt.Updates[0].Snapshot.ParentSnapshotID)
	require.Equal(t, int64(8), plan.Attempt.Updates[0].Snapshot.SequenceNumber)
	require.Equal(t, "2", plan.Attempt.Summary["rewritten-files"])
	require.Equal(t, "1", plan.Attempt.Summary["added-files"])
	require.Equal(t, "2", plan.Attempt.Summary["candidate-files"])
	require.Equal(t, uint64(2), plan.RewrittenFileCount)
	require.Len(t, plan.Objects, 4)
	require.Equal(t, replacement.FilePath, plan.Objects[0].Location)
	require.Contains(t, plan.Objects[1].Location, "/metadata/mo-rewrite-data-files/rw-"+api.PathHash("idem-1")+"/preserved-manifest-00001.avro")
	require.Contains(t, plan.Objects[2].Location, "/metadata/mo-rewrite-data-files/rw-"+api.PathHash("idem-1")+"/data-manifest.avro")
	require.Contains(t, plan.Objects[3].Location, "/metadata/mo-rewrite-data-files/rw-"+api.PathHash("idem-1")+"/manifest-list.avro")
	preservedEntries, err := metadata.ReadManifest(plan.Objects[1].Payload)
	require.NoError(t, err)
	require.Len(t, preservedEntries, 1)
	require.Equal(t, "s3://warehouse/orders/data/part-large.parquet", preservedEntries[0].DataFile.FilePath)
	newManifestList, err := metadata.ReadManifestList(plan.Objects[3].Payload)
	require.NoError(t, err)
	require.Len(t, newManifestList, 2)
	require.Equal(t, plan.Objects[1].Location, newManifestList[0].Path)
	require.Equal(t, plan.Objects[2].Location, newManifestList[1].Path)
	require.ElementsMatch(t, []string{
		"s3://warehouse/orders/data/part-1.parquet",
		"s3://warehouse/orders/data/part-2.parquet",
	}, plan.PostCommitOrphans)
}

func TestNativeRewriteDataFilesPlannerReturnsNoOpWhenNoCandidateGroups(t *testing.T) {
	manifestPath := "s3://warehouse/orders/metadata/data-manifest.avro"
	manifestListPath := "s3://warehouse/orders/metadata/snap-4.avro"
	manifestBytes, err := encodeMaintenanceTestManifest([]api.ManifestEntry{
		rewriteDataFileEntry("s3://warehouse/orders/data/part-large.parquet", 200, map[string]any{"region": "ksa"}, api.ManifestEntryExisting),
	})
	require.NoError(t, err)
	manifestListBytes, err := encodeMaintenanceTestManifestList([]api.ManifestFile{{Path: manifestPath, Length: int64(len(manifestBytes)), Content: api.ManifestContentData}})
	require.NoError(t, err)
	meta := expireMetadata(4)
	called := false

	plan, err := (NativeRewriteDataFilesPlanner{
		Catalog: api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
		Loader:  MaintenanceTableMetadataLoaderFunc(func(ctx context.Context, req Request) (*api.TableMetadata, error) { return meta, nil }),
		Selector: RewriteDataFilesSelector{ObjectReader: fakeRewriteManifestObjectReader{
			manifestListPath: manifestListBytes,
			manifestPath:     manifestBytes,
		}},
		Compactor: RewriteDataFilesCompactorFunc(func(ctx context.Context, req RewriteDataFilesCompactRequest) (*RewriteDataFilesCompactResult, error) {
			called = true
			return nil, nil
		}),
	}).BuildMaintenanceCommit(context.Background(), rewriteDataFilesRequest("ref=main,target_file_size=100,min_input_files=2"))
	require.NoError(t, err)
	require.True(t, plan.NoOp)
	require.Equal(t, int64(4), plan.NoOpSnapshotID)
	require.False(t, called)
}

func TestNativeRewriteDataFilesPlannerRejectsDeleteManifestsBeforeCompaction(t *testing.T) {
	dataManifestPath := "s3://warehouse/orders/metadata/data-manifest.avro"
	deleteManifestPath := "s3://warehouse/orders/metadata/delete-manifest.avro"
	manifestListPath := "s3://warehouse/orders/metadata/snap-4.avro"
	entries := []api.ManifestEntry{
		rewriteDataFileEntry("s3://warehouse/orders/data/part-1.parquet", 40, map[string]any{"region": "ksa"}, api.ManifestEntryExisting),
		rewriteDataFileEntry("s3://warehouse/orders/data/part-2.parquet", 50, map[string]any{"region": "ksa"}, api.ManifestEntryExisting),
	}
	manifestBytes, err := encodeMaintenanceTestManifest(entries)
	require.NoError(t, err)
	manifestListBytes, err := encodeMaintenanceTestManifestList([]api.ManifestFile{
		{Path: dataManifestPath, Length: int64(len(manifestBytes)), Content: api.ManifestContentData},
		{Path: deleteManifestPath, Length: 10, Content: api.ManifestContentDeletes},
	})
	require.NoError(t, err)
	meta := expireMetadata(4)
	called := false

	_, err = (NativeRewriteDataFilesPlanner{
		Catalog: api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
		Loader:  MaintenanceTableMetadataLoaderFunc(func(ctx context.Context, req Request) (*api.TableMetadata, error) { return meta, nil }),
		Selector: RewriteDataFilesSelector{ObjectReader: fakeRewriteManifestObjectReader{
			manifestListPath: manifestListBytes,
			dataManifestPath: manifestBytes,
		}},
		Compactor: RewriteDataFilesCompactorFunc(func(ctx context.Context, req RewriteDataFilesCompactRequest) (*RewriteDataFilesCompactResult, error) {
			called = true
			return nil, nil
		}),
	}).BuildMaintenanceCommit(context.Background(), rewriteDataFilesRequest("ref=main,target_file_size=100,min_input_files=2"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires a delete-aware compactor")
	require.False(t, called)
}

func TestParquetConcatRewriteDataFilesCompactorMergesAppendOnlyFiles(t *testing.T) {
	manifestListPath := "s3://warehouse/orders/metadata/snap-10.avro"
	manifestListBytes, err := encodeMaintenanceTestManifestList([]api.ManifestFile{{
		Path:    "s3://warehouse/orders/metadata/data-manifest.avro",
		Content: api.ManifestContentData,
	}})
	require.NoError(t, err)
	firstPath := "s3://warehouse/orders/data/part-1.parquet"
	secondPath := "s3://warehouse/orders/data/part-2.parquet"
	firstData := rewriteDataFilesParquetBytes(t, []int64{1, 2})
	secondData := rewriteDataFilesParquetBytes(t, []int64{3, 4, 5})
	group := RewriteDataFileGroup{
		PartitionSpecID: 0,
		PartitionKey:    "0|region=s:ksa",
		Candidates: []RewriteDataFileCandidate{
			{File: api.DataFile{Content: api.DataFileContentData, FilePath: firstPath, FileFormat: "parquet", Partition: map[string]any{"region": "ksa"}, RecordCount: 2, FileSizeInBytes: int64(len(firstData)), SpecID: 0}},
			{File: api.DataFile{Content: api.DataFileContentData, FilePath: secondPath, FileFormat: "parquet", Partition: map[string]any{"region": "ksa"}, RecordCount: 3, FileSizeInBytes: int64(len(secondData)), SpecID: 0}},
		},
		TotalRecords:   5,
		TotalSizeBytes: int64(len(firstData) + len(secondData)),
	}

	result, err := (ParquetConcatRewriteDataFilesCompactor{
		ObjectReader: fakeRewriteManifestObjectReader{
			manifestListPath: manifestListBytes,
			firstPath:        firstData,
			secondPath:       secondData,
		},
	}).CompactRewriteDataFiles(context.Background(), RewriteDataFilesCompactRequest{
		Metadata:       &api.TableMetadata{Location: "s3://warehouse/orders"},
		Snapshot:       api.Snapshot{SnapshotID: 10, ManifestList: manifestListPath},
		Selection:      RewriteDataFilesSelection{Groups: []RewriteDataFileGroup{group}},
		JobID:          "job-1",
		IdempotencyKey: "idem-1",
	})
	require.NoError(t, err)
	require.Len(t, result.Rewrites, 1)
	require.Len(t, result.Objects, 1)
	require.Equal(t, []string{result.Objects[0].Location}, result.OrphanPaths)
	replacement := result.Rewrites[0].ReplacementFiles[0]
	require.Equal(t, result.Objects[0].Location, replacement.FilePath)
	require.Contains(t, replacement.FilePath, "/data/mo-rewrite-data-files/rw-"+api.PathHash("idem-1")+"/group-1-")
	require.Equal(t, int64(5), replacement.RecordCount)
	require.Equal(t, int64(len(result.Objects[0].Payload)), replacement.FileSizeInBytes)
	require.Equal(t, map[string]any{"region": "ksa"}, replacement.Partition)
	require.Equal(t, int64(5), rewriteDataFilesParquetRowCount(t, result.Objects[0].Payload))
}

func TestParquetConcatRewriteDataFilesCompactorAllowsIrrelevantDeleteManifests(t *testing.T) {
	manifestListPath := "s3://warehouse/orders/metadata/snap-10.avro"
	deleteManifestPath := "s3://warehouse/orders/metadata/delete-manifest.avro"
	manifestListBytes, err := encodeMaintenanceTestManifestList([]api.ManifestFile{{
		Path:    deleteManifestPath,
		Content: api.ManifestContentDeletes,
	}})
	require.NoError(t, err)
	deleteManifestBytes, err := encodeMaintenanceTestManifest([]api.ManifestEntry{{
		Status: api.ManifestEntryAdded,
		DataFile: api.DataFile{
			Content:         api.DataFileContentEqualityDelete,
			FilePath:        "s3://warehouse/orders/delete/eq-us.parquet",
			FileFormat:      "parquet",
			FileSizeInBytes: 10,
			RecordCount:     1,
			EqualityIDs:     []int{1},
			Partition:       map[string]any{"region": "us"},
			SequenceNumber:  11,
		},
	}})
	require.NoError(t, err)
	firstPath := "s3://warehouse/orders/data/part-1.parquet"
	secondPath := "s3://warehouse/orders/data/part-2.parquet"
	firstData := rewriteDataFilesParquetBytes(t, []int64{1, 2})
	secondData := rewriteDataFilesParquetBytes(t, []int64{3})
	group := RewriteDataFileGroup{
		PartitionSpecID: 0,
		PartitionKey:    "0|region=s:ksa",
		Candidates: []RewriteDataFileCandidate{
			{File: api.DataFile{Content: api.DataFileContentData, FilePath: firstPath, FileFormat: "parquet", Partition: map[string]any{"region": "ksa"}, RecordCount: 2, FileSizeInBytes: int64(len(firstData)), SpecID: 0, SequenceNumber: 10}},
			{File: api.DataFile{Content: api.DataFileContentData, FilePath: secondPath, FileFormat: "parquet", Partition: map[string]any{"region": "ksa"}, RecordCount: 1, FileSizeInBytes: int64(len(secondData)), SpecID: 0, SequenceNumber: 10}},
		},
		TotalRecords:   3,
		TotalSizeBytes: int64(len(firstData) + len(secondData)),
	}

	result, err := (ParquetConcatRewriteDataFilesCompactor{
		ObjectReader: fakeRewriteManifestObjectReader{
			manifestListPath:   manifestListBytes,
			deleteManifestPath: deleteManifestBytes,
			firstPath:          firstData,
			secondPath:         secondData,
		},
	}).CompactRewriteDataFiles(context.Background(), RewriteDataFilesCompactRequest{
		Metadata:       &api.TableMetadata{Location: "s3://warehouse/orders"},
		Snapshot:       api.Snapshot{SnapshotID: 10, ManifestList: manifestListPath},
		Selection:      RewriteDataFilesSelection{DeleteManifestCount: 1, DeleteManifests: []api.ManifestFile{{Path: deleteManifestPath, Content: api.ManifestContentDeletes}}, Groups: []RewriteDataFileGroup{group}},
		JobID:          "job-1",
		IdempotencyKey: "idem-1",
	})
	require.NoError(t, err)
	require.Len(t, result.Rewrites, 1)
	require.Equal(t, int64(3), result.Rewrites[0].ReplacementFiles[0].RecordCount)
	require.Equal(t, int64(3), rewriteDataFilesParquetRowCount(t, result.Objects[0].Payload))
}

func TestParquetConcatRewriteDataFilesCompactorAppliesEqualityDeleteManifests(t *testing.T) {
	manifestListPath := "s3://warehouse/orders/metadata/snap-10.avro"
	deleteManifestPath := "s3://warehouse/orders/metadata/delete-manifest.avro"
	manifestListBytes, err := encodeMaintenanceTestManifestList([]api.ManifestFile{{
		Path:    deleteManifestPath,
		Content: api.ManifestContentDeletes,
	}})
	require.NoError(t, err)
	firstPath := "s3://warehouse/orders/data/part-1.parquet"
	secondPath := "s3://warehouse/orders/data/part-2.parquet"
	deleteFilePath := "s3://warehouse/orders/delete/eq-1.parquet"
	deleteManifestBytes, err := encodeMaintenanceTestManifest([]api.ManifestEntry{{
		Status: api.ManifestEntryAdded,
		DataFile: api.DataFile{
			Content:         api.DataFileContentEqualityDelete,
			FilePath:        deleteFilePath,
			FileFormat:      "parquet",
			FileSizeInBytes: 10,
			RecordCount:     1,
			EqualityIDs:     []int{1},
			Partition:       map[string]any{"region": "ksa"},
			SequenceNumber:  11,
		},
	}})
	require.NoError(t, err)
	firstData := rewriteDataFilesParquetBytes(t, []int64{1, 2})
	secondData := rewriteDataFilesParquetBytes(t, []int64{2, 3})
	deleteData := rewriteDataFilesParquetBytes(t, []int64{2})

	result, err := (ParquetConcatRewriteDataFilesCompactor{
		ObjectReader: fakeRewriteManifestObjectReader{
			manifestListPath:   manifestListBytes,
			deleteManifestPath: deleteManifestBytes,
			deleteFilePath:     deleteData,
			firstPath:          firstData,
			secondPath:         secondData,
		},
	}).CompactRewriteDataFiles(context.Background(), RewriteDataFilesCompactRequest{
		Metadata: &api.TableMetadata{Location: "s3://warehouse/orders"},
		Snapshot: api.Snapshot{SnapshotID: 10, ManifestList: manifestListPath},
		Selection: RewriteDataFilesSelection{DeleteManifestCount: 1, DeleteManifests: []api.ManifestFile{{Path: deleteManifestPath, Content: api.ManifestContentDeletes}}, Groups: []RewriteDataFileGroup{{
			PartitionSpecID: 0,
			PartitionKey:    "0|region=s:ksa",
			Candidates: []RewriteDataFileCandidate{
				{File: api.DataFile{Content: api.DataFileContentData, FilePath: firstPath, FileFormat: "parquet", Partition: map[string]any{"region": "ksa"}, RecordCount: 2, FileSizeInBytes: int64(len(firstData)), SpecID: 0, SequenceNumber: 10}},
				{File: api.DataFile{Content: api.DataFileContentData, FilePath: secondPath, FileFormat: "parquet", Partition: map[string]any{"region": "ksa"}, RecordCount: 2, FileSizeInBytes: int64(len(secondData)), SpecID: 0, SequenceNumber: 10}},
			},
		}}},
		JobID:          "job-1",
		IdempotencyKey: "idem-1",
	})
	require.NoError(t, err)
	require.Len(t, result.Rewrites, 1)
	require.Equal(t, int64(2), result.Rewrites[0].ReplacementFiles[0].RecordCount)
	require.Equal(t, []int64{1, 3}, rewriteDataFilesParquetInt64Values(t, result.Objects[0].Payload))
}

func TestParquetConcatRewriteDataFilesCompactorAppliesPositionDeleteManifests(t *testing.T) {
	manifestListPath := "s3://warehouse/orders/metadata/snap-10.avro"
	deleteManifestPath := "s3://warehouse/orders/metadata/delete-manifest.avro"
	manifestListBytes, err := encodeMaintenanceTestManifestList([]api.ManifestFile{{
		Path:    deleteManifestPath,
		Content: api.ManifestContentDeletes,
	}})
	require.NoError(t, err)
	dataPath := "s3://warehouse/orders/data/part-1.parquet"
	otherDataPath := "s3://warehouse/orders/data/part-other.parquet"
	deleteFilePath := "s3://warehouse/orders/delete/pos-1.parquet"
	deleteManifestBytes, err := encodeMaintenanceTestManifest([]api.ManifestEntry{{
		Status: api.ManifestEntryAdded,
		DataFile: api.DataFile{
			Content:            api.DataFileContentPositionDelete,
			FilePath:           deleteFilePath,
			FileFormat:         "parquet",
			FileSizeInBytes:    10,
			RecordCount:        3,
			ReferencedDataFile: dataPath,
			SequenceNumber:     10,
		},
	}})
	require.NoError(t, err)
	data := rewriteDataFilesParquetBytes(t, []int64{1, 2, 3, 4})
	deleteData := rewriteDataFilesPositionDeleteParquetBytes(t, []rewriteDataFilesPositionDeleteRow{
		{FilePath: dataPath, Pos: 1},
		{FilePath: dataPath, Pos: 3},
		{FilePath: otherDataPath, Pos: 0},
	})

	result, err := (ParquetConcatRewriteDataFilesCompactor{
		ObjectReader: fakeRewriteManifestObjectReader{
			manifestListPath:   manifestListBytes,
			deleteManifestPath: deleteManifestBytes,
			deleteFilePath:     deleteData,
			dataPath:           data,
		},
	}).CompactRewriteDataFiles(context.Background(), RewriteDataFilesCompactRequest{
		Metadata: &api.TableMetadata{Location: "s3://warehouse/orders"},
		Snapshot: api.Snapshot{SnapshotID: 10, ManifestList: manifestListPath},
		Selection: RewriteDataFilesSelection{DeleteManifestCount: 1, DeleteManifests: []api.ManifestFile{{Path: deleteManifestPath, Content: api.ManifestContentDeletes}}, Groups: []RewriteDataFileGroup{{
			PartitionSpecID: 0,
			PartitionKey:    "0|region=s:ksa",
			Candidates: []RewriteDataFileCandidate{
				{File: api.DataFile{Content: api.DataFileContentData, FilePath: dataPath, FileFormat: "parquet", Partition: map[string]any{"region": "ksa"}, RecordCount: 4, FileSizeInBytes: int64(len(data)), SpecID: 0, SequenceNumber: 10}},
			},
		}}},
		JobID:          "job-1",
		IdempotencyKey: "idem-1",
	})
	require.NoError(t, err)
	require.Len(t, result.Rewrites, 1)
	require.Equal(t, int64(2), result.Rewrites[0].ReplacementFiles[0].RecordCount)
	require.Equal(t, []int64{1, 3}, rewriteDataFilesParquetInt64Values(t, result.Objects[0].Payload))
}

func TestParquetConcatRewriteDataFilesCompactorSkipsDifferentSpecEqualityDeletes(t *testing.T) {
	manifestListPath := "s3://warehouse/orders/metadata/snap-10.avro"
	deleteManifestPath := "s3://warehouse/orders/metadata/delete-manifest.avro"
	manifestListBytes, err := encodeMaintenanceTestManifestList([]api.ManifestFile{{
		Path:            deleteManifestPath,
		Content:         api.ManifestContentDeletes,
		PartitionSpecID: 1,
	}})
	require.NoError(t, err)
	dataPath := "s3://warehouse/orders/data/part-1.parquet"
	deleteFilePath := "s3://warehouse/orders/delete/eq-1.parquet"
	deleteManifestBytes, err := encodeMaintenanceTestManifest([]api.ManifestEntry{{
		Status: api.ManifestEntryAdded,
		DataFile: api.DataFile{
			Content:         api.DataFileContentEqualityDelete,
			FilePath:        deleteFilePath,
			FileFormat:      "parquet",
			FileSizeInBytes: 10,
			RecordCount:     1,
			EqualityIDs:     []int{1},
			Partition:       map[string]any{"region": "ksa"},
			SpecID:          1,
			SequenceNumber:  11,
		},
	}})
	require.NoError(t, err)
	data := rewriteDataFilesParquetBytes(t, []int64{1, 2})
	deleteData := rewriteDataFilesParquetBytes(t, []int64{2})

	result, err := (ParquetConcatRewriteDataFilesCompactor{
		ObjectReader: fakeRewriteManifestObjectReader{
			manifestListPath:   manifestListBytes,
			deleteManifestPath: deleteManifestBytes,
			deleteFilePath:     deleteData,
			dataPath:           data,
		},
	}).CompactRewriteDataFiles(context.Background(), RewriteDataFilesCompactRequest{
		Metadata: &api.TableMetadata{Location: "s3://warehouse/orders"},
		Snapshot: api.Snapshot{SnapshotID: 10, ManifestList: manifestListPath},
		Selection: RewriteDataFilesSelection{DeleteManifestCount: 1, DeleteManifests: []api.ManifestFile{{Path: deleteManifestPath, Content: api.ManifestContentDeletes, PartitionSpecID: 1}}, Groups: []RewriteDataFileGroup{{
			PartitionSpecID: 0,
			PartitionKey:    "0|region=s:ksa",
			Candidates: []RewriteDataFileCandidate{
				{File: api.DataFile{Content: api.DataFileContentData, FilePath: dataPath, FileFormat: "parquet", Partition: map[string]any{"region": "ksa"}, RecordCount: 2, FileSizeInBytes: int64(len(data)), SpecID: 0, SequenceNumber: 10}},
			},
		}}},
		JobID:          "job-1",
		IdempotencyKey: "idem-1",
	})
	require.NoError(t, err)
	require.Len(t, result.Rewrites, 1)
	require.Equal(t, int64(2), result.Rewrites[0].ReplacementFiles[0].RecordCount)
	require.Equal(t, []int64{1, 2}, rewriteDataFilesParquetInt64Values(t, result.Objects[0].Payload))
}

func TestParquetConcatRewriteDataFilesCompactorUsesSelectionDeleteManifestCount(t *testing.T) {
	_, err := (ParquetConcatRewriteDataFilesCompactor{
		ObjectReader: fakeRewriteManifestObjectReader{},
	}).CompactRewriteDataFiles(context.Background(), RewriteDataFilesCompactRequest{
		Metadata: &api.TableMetadata{Location: "s3://warehouse/orders"},
		Snapshot: api.Snapshot{SnapshotID: 10, ManifestList: "s3://warehouse/orders/metadata/snap-10.avro"},
		Selection: RewriteDataFilesSelection{
			DeleteManifestCount: 1,
			Groups: []RewriteDataFileGroup{{
				Candidates: []RewriteDataFileCandidate{{File: api.DataFile{FilePath: "s3://warehouse/orders/data/part-1.parquet"}}},
			}},
		},
		JobID:          "job-1",
		IdempotencyKey: "idem-1",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "delete_manifests")
}

func TestRewriteDataFilesSelectorValidatesOptionsAndManifestList(t *testing.T) {
	_, err := (RewriteDataFilesSelector{}).Select(context.Background(), RewriteDataFilesSelectionRequest{
		Snapshot: api.Snapshot{SnapshotID: 10, ManifestList: "s3://warehouse/orders/metadata/snap-10.avro"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires an object reader")

	_, err = (RewriteDataFilesSelector{
		ObjectReader: fakeRewriteManifestObjectReader{},
	}).Select(context.Background(), RewriteDataFilesSelectionRequest{
		Snapshot: api.Snapshot{SnapshotID: 10},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires a target manifest list")

	_, err = (RewriteDataFilesSelector{
		ObjectReader: fakeRewriteManifestObjectReader{},
	}).Select(context.Background(), RewriteDataFilesSelectionRequest{
		Snapshot: api.Snapshot{SnapshotID: 10, ManifestList: "s3://warehouse/orders/metadata/snap-10.avro"},
		Options:  map[string]string{"min_input_files": "0"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "min_input_files must be positive")
}

func rewriteDataFileEntry(path string, size int64, partition map[string]any, status api.ManifestEntryStatus) api.ManifestEntry {
	return api.ManifestEntry{
		Status:         status,
		SnapshotID:     10,
		SequenceNumber: 10,
		FileSequence:   10,
		DataFile: api.DataFile{
			Content:         api.DataFileContentData,
			FilePath:        path,
			FileFormat:      "parquet",
			Partition:       partition,
			RecordCount:     size,
			FileSizeInBytes: size,
			SpecID:          0,
		},
	}
}

func rewriteDataFileCommitUpdateTypes(updates []api.CommitUpdate) []string {
	out := make([]string, 0, len(updates))
	for _, update := range updates {
		out = append(out, update.Type)
	}
	return out
}

func rewriteDataFilesParquetBytes(t *testing.T, values []int64) []byte {
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
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return buf.Bytes()
}

type rewriteDataFilesPositionDeleteRow struct {
	FilePath string
	Pos      int64
}

func rewriteDataFilesPositionDeleteParquetBytes(t *testing.T, rows []rewriteDataFilesPositionDeleteRow) []byte {
	t.Helper()
	var buf bytes.Buffer
	schema := parquet.NewSchema("delete", parquet.Group{
		"file_path": parquet.String(),
		"pos":       parquet.Leaf(parquet.Int64Type),
	})
	writer := parquet.NewWriter(&buf, schema)
	parquetRows := make([]parquet.Row, len(rows))
	for idx, row := range rows {
		parquetRows[idx] = parquet.Row{
			parquet.ValueOf(row.FilePath).Level(0, 0, 0),
			parquet.Int64Value(row.Pos).Level(0, 0, 1),
		}
	}
	_, err := writer.WriteRows(parquetRows)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return buf.Bytes()
}

func rewriteDataFilesParquetRowCount(t *testing.T, data []byte) int64 {
	t.Helper()
	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	return file.NumRows()
}

func rewriteDataFilesParquetInt64Values(t *testing.T, data []byte) []int64 {
	t.Helper()
	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	out := make([]int64, 0, file.NumRows())
	for _, rowGroup := range file.RowGroups() {
		rows := rowGroup.Rows()
		buffer := make([]parquet.Row, 16)
		for {
			n, readErr := rows.ReadRows(buffer)
			for idx := 0; idx < n; idx++ {
				value, ok := rewriteDataFilesRowValue(buffer[idx], 0)
				require.True(t, ok)
				out = append(out, value.Int64())
			}
			if readErr == io.EOF {
				break
			}
			require.NoError(t, readErr)
		}
		require.NoError(t, rows.Close())
	}
	return out
}
