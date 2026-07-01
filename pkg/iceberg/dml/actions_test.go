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

package dml

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
)

func TestDeletePrefersEqualityThenPositionThenRewrite(t *testing.T) {
	base := testBase()
	targets := []DeleteTarget{
		{
			DataFile:           dataFile("s3://warehouse/orders/data-1.parquet"),
			MatchedRows:        3,
			EqualityFieldIDs:   []int{1, 2},
			PredicateStable:    true,
			EqualityDeleteFile: deleteFile("s3://warehouse/orders/eq-delete-1.parquet"),
			HasRowOrdinal:      true,
			PositionDeleteFile: deleteFile("s3://warehouse/orders/pos-delete-1.parquet"),
		},
		{
			DataFile:           dataFile("s3://warehouse/orders/data-2.parquet"),
			MatchedRows:        1,
			HasRowOrdinal:      true,
			PositionDeleteFile: deleteFile("s3://warehouse/orders/pos-delete-2.parquet"),
		},
		{
			DataFile:         dataFile("s3://warehouse/orders/data-3.parquet"),
			MatchedRows:      10,
			ReplacementFiles: []api.DataFile{dataFile("s3://warehouse/orders/rewrite-3.parquet")},
		},
	}
	stream, err := (NativePlanner{}).PlanDelete(context.Background(), DeleteRequest{Base: base, Targets: targets})
	require.NoError(t, err)
	require.Equal(t, []ActionKind{ActionAddEqualityDelete, ActionAddPositionDelete, ActionRewriteDataFile}, actionKinds(stream.Actions))
	require.Equal(t, 2, stream.Profile.AddedDeleteFiles)
	require.Equal(t, 1, stream.Profile.RewrittenDataFiles)

	intent, err := BuildCommitIntent(*stream)
	require.NoError(t, err)
	require.Equal(t, "assert-ref-snapshot-id", intent.Requirements[0].Type)
	require.Equal(t, "main", intent.Requirements[0].Ref)
	require.Equal(t, int64(7), intent.Requirements[0].SnapshotID)
	require.Equal(t, "delete", intent.Summary["operation"])
	require.Equal(t, "2", intent.Summary["added-delete-files"])
	require.Equal(t, "1", intent.Summary["rewritten-data-files"])
	require.Equal(t, []ActionKind{ActionAddEqualityDelete, ActionAddPositionDelete, ActionRewriteDataFile}, actionKinds(intent.Actions))
}

func TestUpdateMergeOnReadCombinesDeleteAndAppend(t *testing.T) {
	stream, err := (NativePlanner{}).PlanUpdate(context.Background(), UpdateRequest{
		Base: testBase(),
		Mode: TableModeMergeOnRead,
		Targets: []UpdateTarget{{
			DeleteTarget: DeleteTarget{
				DataFile:           dataFile("s3://warehouse/orders/data-1.parquet"),
				MatchedRows:        2,
				EqualityFieldIDs:   []int{1},
				PredicateStable:    true,
				EqualityDeleteFile: deleteFile("s3://warehouse/orders/update-delete.parquet"),
			},
		}},
		AppendedDataFile: []api.DataFile{dataFile("s3://warehouse/orders/update-append.parquet")},
	})
	require.NoError(t, err)
	require.Equal(t, []ActionKind{ActionAddEqualityDelete, ActionAppendData}, actionKinds(stream.Actions))
	require.Equal(t, 1, stream.Profile.AddedDataFiles)
	require.Equal(t, int64(2), stream.Profile.MatchedRows)
}

func TestMergeCombinesMatchedDeletesUpdatesAndUnmatchedAppends(t *testing.T) {
	stream, err := (NativePlanner{}).PlanMerge(context.Background(), MergeRequest{
		Base: testBase(),
		Mode: TableModeMergeOnRead,
		MatchedDeletes: []DeleteTarget{{
			DataFile:           dataFile("s3://warehouse/orders/delete.parquet"),
			MatchedRows:        1,
			HasRowOrdinal:      true,
			PositionDeleteFile: deleteFile("s3://warehouse/orders/delete-pos.parquet"),
		}},
		MatchedUpdates: []UpdateTarget{{
			DeleteTarget: DeleteTarget{
				DataFile:           dataFile("s3://warehouse/orders/update.parquet"),
				MatchedRows:        2,
				EqualityFieldIDs:   []int{1},
				PredicateStable:    true,
				EqualityDeleteFile: deleteFile("s3://warehouse/orders/update-eq.parquet"),
			},
			ReplacementFiles: []api.DataFile{dataFile("s3://warehouse/orders/update-append.parquet")},
		}},
		UnmatchedAppends: []api.DataFile{dataFile("s3://warehouse/orders/insert.parquet")},
	})
	require.NoError(t, err)
	require.Equal(t, []ActionKind{ActionAddPositionDelete, ActionAddEqualityDelete, ActionAppendData, ActionAppendData}, actionKinds(stream.Actions))
	intent, err := BuildCommitIntent(*stream)
	require.NoError(t, err)
	require.Equal(t, "merge", intent.Summary["operation"])
	require.Equal(t, []ActionKind{ActionAddPositionDelete, ActionAddEqualityDelete, ActionAppendData, ActionAppendData}, actionKinds(intent.Actions))
}

func TestMergeMatchedUpdateAppendsReplacementFiles(t *testing.T) {
	stream, err := (NativePlanner{}).PlanMerge(context.Background(), MergeRequest{
		Base: testBase(),
		Mode: TableModeMergeOnRead,
		MatchedUpdates: []UpdateTarget{{
			DeleteTarget: DeleteTarget{
				DataFile:           dataFile("s3://warehouse/orders/update.parquet"),
				MatchedRows:        2,
				EqualityFieldIDs:   []int{1},
				PredicateStable:    true,
				EqualityDeleteFile: deleteFile("s3://warehouse/orders/update-eq.parquet"),
			},
			ReplacementFiles: []api.DataFile{dataFile("s3://warehouse/orders/update-append.parquet")},
		}},
	})
	require.NoError(t, err)
	require.Equal(t, []ActionKind{ActionAddEqualityDelete, ActionAppendData}, actionKinds(stream.Actions))
	require.Equal(t, "s3://warehouse/orders/update-append.parquet", stream.Actions[1].File.FilePath)
	require.Equal(t, 1, stream.Profile.AddedDataFiles)
}

func TestUpdateMergeOnReadRejectsDeleteOnlyUpdate(t *testing.T) {
	_, err := (NativePlanner{}).PlanUpdate(context.Background(), UpdateRequest{
		Base: testBase(),
		Mode: TableModeMergeOnRead,
		Targets: []UpdateTarget{{
			DeleteTarget: DeleteTarget{
				DataFile:           dataFile("s3://warehouse/orders/update.parquet"),
				MatchedRows:        2,
				EqualityFieldIDs:   []int{1},
				PredicateStable:    true,
				EqualityDeleteFile: deleteFile("s3://warehouse/orders/update-eq.parquet"),
			},
		}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires replacement data files")
}

func TestOverwriteBuildsSnapshotRequirementAndConflictSurface(t *testing.T) {
	stream, err := (NativePlanner{}).PlanOverwrite(context.Background(), OverwriteRequest{
		Base:              testBase(),
		Scope:             OverwritePartition,
		AffectedDataFiles: []api.DataFile{dataFile("s3://warehouse/orders/old.parquet")},
		ReplacementFiles:  []api.DataFile{dataFile("s3://warehouse/orders/new.parquet")},
	})
	require.NoError(t, err)
	require.Equal(t, []ActionKind{ActionDeleteDataFile, ActionAppendData}, actionKinds(stream.Actions))
	intent, err := BuildCommitIntent(*stream)
	require.NoError(t, err)
	require.Equal(t, "assert-ref-snapshot-id", intent.Requirements[0].Type)
	require.Equal(t, "overwrite", intent.Summary["operation"])
	require.Equal(t, "1", intent.Summary["deleted-data-files"])
}

func TestPlannerFallsBackWhenIcebergGoAdapterCannotBuildRowDelta(t *testing.T) {
	stream, err := (Planner{Adapter: fakeRowDeltaAdapter{supported: true}}).PlanDelete(context.Background(), DeleteRequest{
		Base: testBase(),
		Targets: []DeleteTarget{{
			DataFile:           dataFile("s3://warehouse/orders/data.parquet"),
			EqualityFieldIDs:   []int{1},
			PredicateStable:    true,
			EqualityDeleteFile: deleteFile("s3://warehouse/orders/delete.parquet"),
		}},
	})
	require.NoError(t, err)
	require.False(t, stream.Profile.UsedAdapter)
	require.Equal(t, []ActionKind{ActionAddEqualityDelete}, actionKinds(stream.Actions))
}

func TestDMLCommitIntentRejectsTagWritesByDefault(t *testing.T) {
	base := testBase()
	base.TargetRef = "tag:release"
	_, err := (NativePlanner{}).PlanOverwrite(context.Background(), OverwriteRequest{
		Base:             base,
		ReplacementFiles: []api.DataFile{dataFile("s3://warehouse/orders/new.parquet")},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrUnsupportedFeature))
}

func TestDMLCommitIntentAllowsExplicitTagMove(t *testing.T) {
	base := testBase()
	base.TargetRef = "tag:release"
	base.CatalogCapabilities = api.CatalogCapabilities{BranchTag: true}
	base.AllowTagMove = true
	stream, err := (NativePlanner{}).PlanOverwrite(context.Background(), OverwriteRequest{
		Base:             base,
		ReplacementFiles: []api.DataFile{dataFile("s3://warehouse/orders/new.parquet")},
	})
	require.NoError(t, err)
	require.Equal(t, "release", stream.Base.TargetRef)
	require.Equal(t, "tag", stream.Base.TargetRefType)
	intent, err := BuildCommitIntent(*stream)
	require.NoError(t, err)
	require.Equal(t, "release", intent.TargetRef)
	require.Equal(t, "release", intent.Requirements[0].Ref)
}

func TestAuditProfileCapturesDMLCounts(t *testing.T) {
	stream, err := (NativePlanner{}).PlanDelete(context.Background(), DeleteRequest{
		Base: testBase(),
		Targets: []DeleteTarget{{
			DataFile:           dataFile("s3://warehouse/orders/data.parquet"),
			MatchedRows:        4,
			HasRowOrdinal:      true,
			PositionDeleteFile: deleteFile("s3://warehouse/orders/delete.parquet"),
		}},
	})
	require.NoError(t, err)
	intent, err := BuildCommitIntent(*stream)
	require.NoError(t, err)
	audit := BuildAuditProfile(*stream, intent)
	require.Equal(t, "delete", audit["operation"])
	require.Equal(t, "4", audit["matched_rows"])
	require.Equal(t, "1", audit["position_delete_files"])
}

func TestBuildManifestCommitAttemptMaterializesDataAndDeleteManifests(t *testing.T) {
	stream, err := (NativePlanner{}).PlanUpdate(context.Background(), UpdateRequest{
		Base: testBase(),
		Mode: TableModeMergeOnRead,
		Targets: []UpdateTarget{{
			DeleteTarget: DeleteTarget{
				DataFile:           dataFile("s3://warehouse/orders/data-1.parquet"),
				MatchedRows:        2,
				EqualityFieldIDs:   []int{1},
				PredicateStable:    true,
				EqualityDeleteFile: deleteFile("s3://warehouse/orders/delete-1.parquet"),
			},
		}},
		AppendedDataFile: []api.DataFile{dataFile("s3://warehouse/orders/replacement-1.parquet")},
	})
	require.NoError(t, err)
	intent, err := BuildCommitIntent(*stream)
	require.NoError(t, err)

	result, err := BuildManifestCommitAttempt(context.Background(), ManifestMaterializeRequest{
		Intent:             *intent,
		SnapshotID:         99,
		SequenceNumber:     12,
		TimestampMS:        123456,
		DataManifestPath:   "s3://warehouse/orders/metadata/data-99.avro",
		DeleteManifestPath: "s3://warehouse/orders/metadata/delete-99.avro",
		ManifestListPath:   "s3://warehouse/orders/metadata/snap-99.avro",
		PreservedManifests: []api.ManifestFile{{
			Path:            "s3://warehouse/orders/metadata/base-manifest.avro",
			Length:          111,
			Content:         api.ManifestContentData,
			AddedSnapshotID: 7,
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, result.DataManifest)
	require.NotNil(t, result.DeleteManifest)
	require.Equal(t, api.ManifestContentData, result.DataManifest.Content)
	require.Equal(t, api.ManifestContentDeletes, result.DeleteManifest.Content)
	require.Equal(t, 1, result.DataManifest.AddedFilesCount)
	require.Equal(t, 1, result.DeleteManifest.AddedFilesCount)

	dataEntries, err := metadata.ReadManifest(result.DataManifestBytes)
	require.NoError(t, err)
	require.Equal(t, []api.ManifestEntryStatus{api.ManifestEntryAdded}, manifestEntryStatuses(dataEntries))
	require.Equal(t, api.DataFileContentData, dataEntries[0].DataFile.Content)

	deleteEntries, err := metadata.ReadManifest(result.DeleteManifestBytes)
	require.NoError(t, err)
	require.Equal(t, []api.ManifestEntryStatus{api.ManifestEntryAdded}, manifestEntryStatuses(deleteEntries))
	require.Equal(t, api.DataFileContentEqualityDelete, deleteEntries[0].DataFile.Content)
	require.Equal(t, []int{1}, deleteEntries[0].DataFile.EqualityIDs)

	manifestList, err := metadata.ReadManifestList(result.ManifestListBytes)
	require.NoError(t, err)
	require.Len(t, manifestList, 3)
	require.Equal(t, "s3://warehouse/orders/metadata/base-manifest.avro", manifestList[0].Path)
	require.Equal(t, result.DataManifest.Path, manifestList[1].Path)
	require.Equal(t, result.DeleteManifest.Path, manifestList[2].Path)
	require.Equal(t, []string{"add-snapshot", "set-snapshot-ref"}, commitUpdateTypes(result.Attempt.Updates))
	require.NotNil(t, result.Attempt.Updates[0].Snapshot)
	require.Equal(t, int64(99), result.Attempt.Updates[0].Snapshot.SnapshotID)
	require.NotNil(t, result.Attempt.Updates[0].Snapshot.ParentSnapshotID)
	require.Equal(t, int64(7), *result.Attempt.Updates[0].Snapshot.ParentSnapshotID)
	require.Equal(t, int64(12), result.Attempt.Updates[0].Snapshot.SequenceNumber)
	require.NotNil(t, result.Attempt.Updates[0].Snapshot.SchemaID)
	require.Equal(t, 3, *result.Attempt.Updates[0].Snapshot.SchemaID)
	require.Equal(t, int64(123456), result.Attempt.Updates[0].Snapshot.TimestampMS)
	require.Equal(t, "s3://warehouse/orders/metadata/snap-99.avro", result.Attempt.Updates[0].Snapshot.ManifestList)
	require.Equal(t, "update", result.Attempt.Updates[0].Snapshot.Summary["operation"])
	require.Equal(t, "main", result.Attempt.Updates[1].Ref)
	require.Equal(t, "branch", result.Attempt.Updates[1].RefType)
	require.Equal(t, int64(99), result.Attempt.Updates[1].SnapshotID)
	require.Equal(t, intent.Requirements, result.Attempt.Requirements)
	require.Equal(t, "update", result.Attempt.Summary["operation"])
	assertRESTSnapshotCommitUpdates(t, result.Attempt.Updates, int64(99), testBase().BaseSnapshotID, int64(12), testBase().BaseSchemaID, "s3://warehouse/orders/metadata/snap-99.avro")
}

func TestBuildManifestCommitAttemptRewritesPreservedManifestForFileDeletes(t *testing.T) {
	base := testBase()
	oldFile := dataFile("s3://warehouse/orders/data/old.parquet")
	keepFile := dataFile("s3://warehouse/orders/data/keep.parquet")
	otherFile := dataFile("s3://warehouse/orders/data/other.parquet")
	newFile := dataFile("s3://warehouse/orders/data/new.parquet")
	stream, err := (NativePlanner{}).PlanOverwrite(context.Background(), OverwriteRequest{
		Base:              base,
		Scope:             OverwritePartition,
		AffectedDataFiles: []api.DataFile{oldFile},
		ReplacementFiles:  []api.DataFile{newFile},
	})
	require.NoError(t, err)
	intent, err := BuildCommitIntent(*stream)
	require.NoError(t, err)
	mixedManifest := api.ManifestFile{Path: "s3://warehouse/orders/metadata/base-mixed.avro", Content: api.ManifestContentData, AddedFilesCount: 2}
	otherManifest := api.ManifestFile{Path: "s3://warehouse/orders/metadata/base-other.avro", Content: api.ManifestContentData, AddedFilesCount: 1}

	result, err := BuildManifestCommitAttempt(context.Background(), ManifestMaterializeRequest{
		Intent:           *intent,
		SnapshotID:       101,
		SequenceNumber:   14,
		DataManifestPath: "s3://warehouse/orders/metadata/data-101.avro",
		ManifestListPath: "s3://warehouse/orders/metadata/snap-101.avro",
		PreservedManifests: []api.ManifestFile{
			mixedManifest,
			otherManifest,
		},
		PreservedSources: []PreservedManifestSource{{
			Manifest: mixedManifest,
			Entries: []api.ManifestEntry{
				{Status: api.ManifestEntryAdded, SnapshotID: 7, DataFile: oldFile},
				{Status: api.ManifestEntryExisting, SnapshotID: 7, DataFile: keepFile},
			},
		}, {
			Manifest: otherManifest,
			Entries: []api.ManifestEntry{
				{Status: api.ManifestEntryExisting, SnapshotID: 7, DataFile: otherFile},
			},
		}},
	})
	require.NoError(t, err)
	require.Len(t, result.RewrittenPreservedManifests, 1)
	require.Equal(t, mixedManifest.Path, result.RewrittenPreservedManifests[0].OriginalPath)
	require.Equal(t, 1, result.RewrittenPreservedManifests[0].RemovedEntries)
	rewrittenEntries, err := metadata.ReadManifest(result.RewrittenPreservedManifests[0].ManifestBytes)
	require.NoError(t, err)
	require.Equal(t, []string{keepFile.FilePath}, manifestEntryPaths(rewrittenEntries))

	dataEntries, err := metadata.ReadManifest(result.DataManifestBytes)
	require.NoError(t, err)
	require.Equal(t, []api.ManifestEntryStatus{api.ManifestEntryDeleted, api.ManifestEntryAdded}, manifestEntryStatuses(dataEntries))
	require.Equal(t, oldFile.FilePath, dataEntries[0].DataFile.FilePath)
	require.Equal(t, newFile.FilePath, dataEntries[1].DataFile.FilePath)

	manifestList, err := metadata.ReadManifestList(result.ManifestListBytes)
	require.NoError(t, err)
	require.Equal(t, []string{
		result.RewrittenPreservedManifests[0].Manifest.Path,
		otherManifest.Path,
		result.DataManifest.Path,
	}, manifestFilePaths(manifestList))
	require.NotContains(t, manifestFilePaths(manifestList), mixedManifest.Path)
	require.Contains(t, result.RewrittenPreservedManifests[0].Manifest.Path, "preserved-manifest-0-"+api.PathHash(mixedManifest.Path)+".avro")
	require.Equal(t, 1, result.RewrittenPreservedManifests[0].Manifest.ExistingFilesCount)
	assertRESTSnapshotCommitUpdates(t, result.Attempt.Updates, int64(101), testBase().BaseSnapshotID, int64(14), testBase().BaseSchemaID, "s3://warehouse/orders/metadata/snap-101.avro")
}

func TestBuildManifestCommitAttemptDropsFullyDeletedPreservedManifest(t *testing.T) {
	oldFile := dataFile("s3://warehouse/orders/data/old.parquet")
	newFile := dataFile("s3://warehouse/orders/data/new.parquet")
	stream, err := (NativePlanner{}).PlanOverwrite(context.Background(), OverwriteRequest{
		Base:              testBase(),
		Scope:             OverwriteTable,
		AffectedDataFiles: []api.DataFile{oldFile},
		ReplacementFiles:  []api.DataFile{newFile},
	})
	require.NoError(t, err)
	intent, err := BuildCommitIntent(*stream)
	require.NoError(t, err)
	baseManifest := api.ManifestFile{Path: "s3://warehouse/orders/metadata/base.avro", Content: api.ManifestContentData, AddedFilesCount: 1}

	result, err := BuildManifestCommitAttempt(context.Background(), ManifestMaterializeRequest{
		Intent:             *intent,
		SnapshotID:         102,
		SequenceNumber:     15,
		DataManifestPath:   "s3://warehouse/orders/metadata/data-102.avro",
		ManifestListPath:   "s3://warehouse/orders/metadata/snap-102.avro",
		PreservedManifests: []api.ManifestFile{baseManifest},
		PreservedSources: []PreservedManifestSource{{
			Manifest: baseManifest,
			Entries: []api.ManifestEntry{{
				Status:     api.ManifestEntryAdded,
				SnapshotID: 7,
				DataFile:   oldFile,
			}},
		}},
	})
	require.NoError(t, err)
	require.Empty(t, result.RewrittenPreservedManifests)
	manifestList, err := metadata.ReadManifestList(result.ManifestListBytes)
	require.NoError(t, err)
	require.Equal(t, []string{result.DataManifest.Path}, manifestFilePaths(manifestList))
	assertRESTSnapshotCommitUpdates(t, result.Attempt.Updates, int64(102), testBase().BaseSnapshotID, int64(15), testBase().BaseSchemaID, "s3://warehouse/orders/metadata/snap-102.avro")
}

func TestBuildManifestCommitAttemptRequiresPreservedManifestsForFileDeletes(t *testing.T) {
	oldFile := dataFile("s3://warehouse/orders/data/old.parquet")
	newFile := dataFile("s3://warehouse/orders/data/new.parquet")
	stream, err := (NativePlanner{}).PlanOverwrite(context.Background(), OverwriteRequest{
		Base:              testBase(),
		Scope:             OverwritePartition,
		AffectedDataFiles: []api.DataFile{oldFile},
		ReplacementFiles:  []api.DataFile{newFile},
	})
	require.NoError(t, err)
	intent, err := BuildCommitIntent(*stream)
	require.NoError(t, err)

	_, err = BuildManifestCommitAttempt(context.Background(), ManifestMaterializeRequest{
		Intent:           *intent,
		SnapshotID:       103,
		SequenceNumber:   16,
		DataManifestPath: "s3://warehouse/orders/metadata/data-103.avro",
		ManifestListPath: "s3://warehouse/orders/metadata/snap-103.avro",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires preserved data manifests")
	require.Contains(t, err.Error(), "deleted_files")
}

func TestBuildManifestCommitAttemptRequiresDeleteManifestPath(t *testing.T) {
	stream, err := (NativePlanner{}).PlanDelete(context.Background(), DeleteRequest{
		Base: testBase(),
		Targets: []DeleteTarget{{
			DataFile:           dataFile("s3://warehouse/orders/data-1.parquet"),
			MatchedRows:        2,
			EqualityFieldIDs:   []int{1},
			PredicateStable:    true,
			EqualityDeleteFile: deleteFile("s3://warehouse/orders/delete-1.parquet"),
		}},
	})
	require.NoError(t, err)
	intent, err := BuildCommitIntent(*stream)
	require.NoError(t, err)
	_, err = BuildManifestCommitAttempt(context.Background(), ManifestMaterializeRequest{
		Intent:           *intent,
		SnapshotID:       99,
		SequenceNumber:   12,
		ManifestListPath: "s3://warehouse/orders/metadata/snap-99.avro",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "delete manifest path")
}

type fakeRowDeltaAdapter struct {
	supported bool
}

func (a fakeRowDeltaAdapter) Name() string { return "iceberg-go" }

func (a fakeRowDeltaAdapter) SupportsRowDelta() bool { return a.supported }

func (a fakeRowDeltaAdapter) BuildDelete(ctx context.Context, req DeleteRequest) (*ActionStream, bool, error) {
	return nil, false, nil
}

func (a fakeRowDeltaAdapter) BuildUpdate(ctx context.Context, req UpdateRequest) (*ActionStream, bool, error) {
	return nil, false, nil
}

func (a fakeRowDeltaAdapter) BuildMerge(ctx context.Context, req MergeRequest) (*ActionStream, bool, error) {
	return nil, false, nil
}

func testBase() CommitBase {
	return CommitBase{
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		TargetRef:      "main",
		BaseSnapshotID: 7,
		TableUUID:      "uuid-1",
		BaseSchemaID:   3,
		BaseSpecID:     2,
		IdempotencyKey: "idem-1",
	}
}

func dataFile(path string) api.DataFile {
	return api.DataFile{
		Content:         api.DataFileContentData,
		FilePath:        path,
		FileFormat:      "parquet",
		RecordCount:     10,
		FileSizeInBytes: 100,
		SpecID:          2,
	}
}

func deleteFile(path string) api.DataFile {
	return api.DataFile{
		FilePath:        path,
		FileFormat:      "parquet",
		RecordCount:     1,
		FileSizeInBytes: 50,
		SpecID:          2,
	}
}

func actionKinds(actions []Action) []ActionKind {
	out := make([]ActionKind, 0, len(actions))
	for _, action := range actions {
		out = append(out, action.Kind)
	}
	return out
}

func manifestEntryStatuses(entries []api.ManifestEntry) []api.ManifestEntryStatus {
	out := make([]api.ManifestEntryStatus, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.Status)
	}
	return out
}

func manifestEntryPaths(entries []api.ManifestEntry) []string {
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.DataFile.FilePath)
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
