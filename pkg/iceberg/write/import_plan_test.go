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
)

func TestBuildImportPlanPinsSnapshotAndBuildsProfile(t *testing.T) {
	scan := api.IcebergScanPlan{
		Snapshot: api.SnapshotPlan{
			SnapshotID:           200,
			MetadataLocationHash: "metadata-hash",
		},
		DataTasks: []api.DataFileTask{{
			DataFile: api.DataFile{FilePath: "s3://warehouse/t/data/a.parquet", RecordCount: 10},
		}, {
			DataFile: api.DataFile{FilePath: "s3://warehouse/t/data/b.parquet", RecordCount: 15},
		}},
		DeleteTasks: []api.DeleteFileTask{{
			DataFile: api.DataFile{FilePath: "s3://warehouse/t/delete/d.parquet", RecordCount: 3},
		}},
	}
	plan, err := BuildImportPlan(context.Background(), ImportPlanRequest{
		ScanPlan:    scan,
		TargetTable: "orders_native",
	})
	require.NoError(t, err)
	require.Equal(t, int64(200), plan.Snapshot.SnapshotID)
	require.Equal(t, int64(25), plan.Profile.RowCount)
	require.Equal(t, 2, plan.Profile.FileCount)
	require.Equal(t, "metadata-hash", plan.Profile.MetadataLocationHash)
	require.NotEmpty(t, plan.Profile.Checksum)

	again, err := BuildImportPlan(context.Background(), ImportPlanRequest{
		ScanPlan:    scan,
		TargetTable: "orders_native",
	})
	require.NoError(t, err)
	require.Equal(t, plan.Profile.Checksum, again.Profile.Checksum)
}

func TestImportChecksumUsesExistingOrComputedPathHash(t *testing.T) {
	snapshot := api.SnapshotPlan{SnapshotID: 42, MetadataLocationHash: "metadata"}
	data := []api.DataFileTask{{DataFile: api.DataFile{FilePath: "s3://warehouse/a", RecordCount: 5}}}
	deletes := []api.DeleteFileTask{{DataFile: api.DataFile{FilePath: "s3://warehouse/b"}}}
	want := importChecksum(snapshot, data, deletes)
	data[0].DataFile.FilePathHash = "  " + api.PathHash(data[0].DataFile.FilePath) + "  "
	deletes[0].DataFile.FilePathHash = "\t" + api.PathHash(deletes[0].DataFile.FilePath) + "\n"
	require.Equal(t, want, importChecksum(snapshot, data, deletes))
	data[0].DataFile.FilePath = "different location"
	deletes[0].DataFile.FilePath = "different location"
	require.Equal(t, want, importChecksum(snapshot, data, deletes), "precomputed hashes determine the checksum")
	data[0].DataFile.FilePathHash = "  "
	require.NotEqual(t, want, importChecksum(snapshot, data, deletes), "blank hash falls back to the path")
}

func TestImportPlanDataFileCopies(t *testing.T) {
	file := api.DataFile{
		Partition:    map[string]any{"region": "old"},
		ValueCounts:  map[int]int64{1: 10},
		LowerBounds:  map[int][]byte{1: {1}},
		SplitOffsets: []int64{7},
		EqualityIDs:  []int{5},
	}
	scan := api.IcebergScanPlan{
		Snapshot:    api.SnapshotPlan{SnapshotID: 1},
		DataTasks:   []api.DataFileTask{{DataFile: file}},
		DeleteTasks: []api.DeleteFileTask{{DataFile: file}},
	}
	plan, err := BuildImportPlan(context.Background(), ImportPlanRequest{ScanPlan: scan, TargetTable: "target"})
	require.NoError(t, err)
	file.Partition["region"] = "new"
	file.ValueCounts[1] = 20
	file.LowerBounds[1][0] = 2
	file.SplitOffsets[0] = 8
	file.EqualityIDs[0] = 6
	for _, copied := range []api.DataFile{plan.DataTasks[0].DataFile, plan.DeleteTasks[0].DataFile} {
		require.Equal(t, "old", copied.Partition["region"])
		require.Equal(t, int64(10), copied.ValueCounts[1])
		require.Equal(t, []byte{1}, copied.LowerBounds[1])
		require.Equal(t, []int64{7}, copied.SplitOffsets)
		require.Equal(t, []int{6}, copied.EqualityIDs, "existing equality ID ownership is retained")
	}
}

func TestBuildImportPlanRequiresResolvedSnapshot(t *testing.T) {
	_, err := BuildImportPlan(context.Background(), ImportPlanRequest{TargetTable: "orders_native"})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrConfigInvalid))
}

func TestImportExecutorPinsSnapshotForNativeSink(t *testing.T) {
	scan := importScanPlan()
	var captured NativeImportRequest
	executor := ImportExecutor{Sink: NativeImportSinkFunc(func(ctx context.Context, req NativeImportRequest) (*NativeImportResult, error) {
		captured = req
		return &NativeImportResult{
			SourceSnapshotID:     req.Profile.SourceSnapshotID,
			MetadataLocationHash: req.Profile.MetadataLocationHash,
			RowCount:             req.Profile.RowCount,
			FileCount:            req.Profile.FileCount,
			Checksum:             req.Profile.Checksum,
		}, nil
	})}

	result, err := executor.Execute(context.Background(), ImportPlanRequest{
		ScanPlan:       scan,
		TargetDatabase: "analytics",
		TargetTable:    "orders_native",
	})
	require.NoError(t, err)
	require.Equal(t, int64(200), result.SourceSnapshotID)
	require.Equal(t, "metadata-hash", result.MetadataLocationHash)
	require.Equal(t, int64(25), result.RowCount)
	require.Equal(t, 2, result.FileCount)
	require.NotEmpty(t, result.Checksum)
	require.Equal(t, "analytics", captured.TargetDatabase)
	require.Equal(t, "orders_native", captured.TargetTable)
	require.Equal(t, int64(200), captured.Snapshot.SnapshotID)
	require.Equal(t, scan.Snapshot.MetadataLocationHash, captured.Profile.MetadataLocationHash)
	require.Equal(t, int64(25), captured.Profile.RowCount)
	require.Equal(t, 2, captured.Profile.FileCount)
	require.Equal(t, result.Checksum, captured.Profile.Checksum)
	require.Len(t, captured.DataTasks, 2)
	require.Len(t, captured.DeleteTasks, 1)
}

func TestImportExecutorRejectsSnapshotMutationBySink(t *testing.T) {
	executor := ImportExecutor{Sink: NativeImportSinkFunc(func(ctx context.Context, req NativeImportRequest) (*NativeImportResult, error) {
		return &NativeImportResult{SourceSnapshotID: req.Profile.SourceSnapshotID + 1}, nil
	})}
	_, err := executor.Execute(context.Background(), ImportPlanRequest{
		ScanPlan:    importScanPlan(),
		TargetTable: "orders_native",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrInternal))
}

func TestImportExecutorRequiresNativeSink(t *testing.T) {
	_, err := (ImportExecutor{}).Execute(context.Background(), ImportPlanRequest{
		ScanPlan:    importScanPlan(),
		TargetTable: "orders_native",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrConfigInvalid))
}

func importScanPlan() api.IcebergScanPlan {
	return api.IcebergScanPlan{
		Snapshot: api.SnapshotPlan{
			SnapshotID:           200,
			MetadataLocationHash: "metadata-hash",
		},
		DataTasks: []api.DataFileTask{{
			DataFile: api.DataFile{FilePath: "s3://warehouse/t/data/a.parquet", RecordCount: 10},
		}, {
			DataFile: api.DataFile{FilePath: "s3://warehouse/t/data/b.parquet", RecordCount: 15},
		}},
		DeleteTasks: []api.DeleteFileTask{{
			DataFile: api.DataFile{FilePath: "s3://warehouse/t/delete/d.parquet", RecordCount: 3},
		}},
	}
}
