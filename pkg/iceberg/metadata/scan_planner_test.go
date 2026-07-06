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

package metadata

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	stderrors "errors"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

func TestLocalScanPlannerBuildsSnapshotTasksAndProfile(t *testing.T) {
	ctx := context.Background()
	fixture := newPlannerFixture(t, 2)
	planner := fixture.planner()
	planner.ManifestReadParallelism = 2
	planner.CredentialScope = "scope-hmac"

	plan, err := planner.PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest: cacheLoadTableRequest().CatalogRequest,
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "main",
		ProjectionIDs:  []int{1, 4},
		ResidualSQL:    "id > 10",
	})
	if err != nil {
		t.Fatalf("plan scan: %v", err)
	}
	if plan.Snapshot.SnapshotID != 22 || plan.Snapshot.SchemaID != 1 || plan.Snapshot.PlanningMode != localPlanningMode {
		t.Fatalf("unexpected snapshot plan: %+v", plan.Snapshot)
	}
	if strings.Contains(plan.Snapshot.MetadataLocation, "warehouse") || strings.Contains(plan.Snapshot.ManifestList, "warehouse") {
		t.Fatalf("snapshot paths should be redacted: %+v", plan.Snapshot)
	}
	if len(plan.DataTasks) != 2 || plan.Profile.DataFilesSelected != 2 || plan.Profile.DataFilesPruned != 1 {
		t.Fatalf("unexpected data tasks/profile: tasks=%+v profile=%+v", plan.DataTasks, plan.Profile)
	}
	if plan.Profile.DataFileBytesSelected != 200 || plan.Profile.DataFileBytesPruned != 0 {
		t.Fatalf("unexpected data file byte profile: %+v", plan.Profile)
	}
	for _, task := range plan.DataTasks {
		if task.CredentialScope != "scope-hmac" || task.ResidualFilter.ExpressionSQL != "id > 10" || task.ResidualFilter.AlwaysTrue {
			t.Fatalf("unexpected task residual/credential: %+v", task)
		}
		if task.DataFile.FilePathRedacted == "" || strings.Contains(task.DataFile.FilePathRedacted, "warehouse") {
			t.Fatalf("data file path should be redacted: %+v", task.DataFile)
		}
	}
	projected := map[int]bool{}
	for _, column := range plan.ColumnMapping {
		projected[column.FieldID] = column.Projected
	}
	if !projected[1] || projected[2] || projected[3] || !projected[4] {
		t.Fatalf("unexpected projected field map: %+v", projected)
	}
	if plan.Profile.ManifestListBytes == 0 || plan.Profile.ManifestBytes == 0 || plan.Profile.PlanningCacheMiss == 0 {
		t.Fatalf("profile should include planning bytes and cache misses: %+v", plan.Profile)
	}
	if fixture.reader.maxActive > 2 {
		t.Fatalf("manifest reads exceeded bounded parallelism: maxActive=%d", fixture.reader.maxActive)
	}
}

func TestLocalScanPlannerUsesCacheOnSecondPlanning(t *testing.T) {
	ctx := context.Background()
	fixture := newPlannerFixture(t, 1)
	planner := fixture.planner()

	first, err := planner.PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest: cacheLoadTableRequest().CatalogRequest,
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "main",
	})
	if err != nil {
		t.Fatalf("first plan: %v", err)
	}
	callsAfterFirst := fixture.reader.totalCalls()
	second, err := planner.PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest: cacheLoadTableRequest().CatalogRequest,
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "main",
	})
	if err != nil {
		t.Fatalf("second plan: %v", err)
	}
	if fixture.catalogLoads != 1 {
		t.Fatalf("fresh metadata cache should avoid second catalog load, got %d", fixture.catalogLoads)
	}
	if callsAfterSecond := fixture.reader.totalCalls(); callsAfterSecond != callsAfterFirst {
		t.Fatalf("fresh manifest cache should avoid second object read, before=%d after=%d", callsAfterFirst, callsAfterSecond)
	}
	if !first.ResidualFilter.AlwaysTrue {
		t.Fatalf("first residual should be always true")
	}
	if second.Profile.PlanningCacheHits < 3 {
		t.Fatalf("second plan should use metadata/list/manifest cache hits: %+v", second.Profile)
	}
}

func TestLocalScanPlannerLoadsAllSnapshotsForTimeTravel(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		req  api.ScanPlanRequest
		want string
	}{
		{
			name: "current ref",
			req: api.ScanPlanRequest{
				CatalogRequest: cacheLoadTableRequest().CatalogRequest,
				Namespace:      api.Namespace{"sales"},
				Table:          "orders",
				Ref:            "main",
			},
			want: "main",
		},
		{
			name: "snapshot id",
			req: api.ScanPlanRequest{
				CatalogRequest: cacheLoadTableRequest().CatalogRequest,
				Namespace:      api.Namespace{"sales"},
				Table:          "orders",
				Ref:            "main",
				Snapshot:       api.SnapshotSelector{SnapshotID: 22, HasSnapshotID: true, RefName: "main"},
			},
			want: "all",
		},
		{
			name: "timestamp",
			req: api.ScanPlanRequest{
				CatalogRequest: cacheLoadTableRequest().CatalogRequest,
				Namespace:      api.Namespace{"sales"},
				Table:          "orders",
				Ref:            "main",
				Snapshot:       api.SnapshotSelector{TimestampMS: 1710000200000, HasTimestampMS: true, RefName: "main"},
			},
			want: "all",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newPlannerFixture(t, 1)
			originalLoad := fixture.client.LoadTableFunc
			got := ""
			fixture.client.LoadTableFunc = func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
				got = req.Snapshots
				return originalLoad(ctx, req)
			}
			if _, err := fixture.planner().PlanScan(ctx, tt.req); err != nil {
				t.Fatalf("plan scan: %v", err)
			}
			if got != tt.want {
				t.Fatalf("unexpected REST snapshots selector got=%q want=%q", got, tt.want)
			}
		})
	}
}

func TestLocalScanPlannerRefreshesRefCacheAfterMetadataLoad(t *testing.T) {
	ctx := context.Background()
	fixture := newPlannerFixture(t, 1)
	refresher := &recordingRefCacheRefresher{}
	planner := fixture.planner()
	planner.RefCacheRefresher = refresher

	_, err := planner.PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest: cacheLoadTableRequest().CatalogRequest,
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "main",
	})
	if err != nil {
		t.Fatalf("plan scan: %v", err)
	}
	if refresher.calls != 1 || len(refresher.refs) != 2 {
		t.Fatalf("expected one ref cache refresh with two refs, calls=%d refs=%+v", refresher.calls, refresher.refs)
	}
	byName := make(map[string]model.RefCache)
	for _, ref := range refresher.refs {
		byName[ref.RefName] = ref
		if ref.AccountID != 1 || ref.CatalogID != 2 || ref.Namespace != "sales" || ref.TableName != "orders" || ref.Source != "catalog" {
			t.Fatalf("unexpected ref cache row: %+v", ref)
		}
	}
	if byName["main"].RefType != "branch" || byName["main"].SnapshotID != "22" {
		t.Fatalf("unexpected main ref cache row: %+v", byName["main"])
	}
	if byName["audit"].RefType != "tag" || byName["audit"].SnapshotID != "11" {
		t.Fatalf("unexpected audit ref cache row: %+v", byName["audit"])
	}
}

func TestLocalScanPlannerEnforcesPlanningLimits(t *testing.T) {
	ctx := context.Background()
	fixture := newPlannerFixture(t, 2)
	planner := fixture.planner()
	planner.MaxManifestFiles = 1
	_, err := planner.PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest: cacheLoadTableRequest().CatalogRequest,
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "main",
	})
	assertIcebergCode(t, err, api.ErrPlanningLimitExceeded)

	fixture = newPlannerFixture(t, 2)
	planner = fixture.planner()
	planner.MaxDataFiles = 1
	_, err = planner.PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest: cacheLoadTableRequest().CatalogRequest,
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "main",
	})
	assertIcebergCode(t, err, api.ErrPlanningLimitExceeded)
}

func TestLocalScanPlannerRejectsDeleteManifest(t *testing.T) {
	ctx := context.Background()
	fixture := newPlannerFixture(t, 1)
	fixture.facade.manifests[0].Content = api.ManifestContentDeletes
	planner := fixture.planner()
	_, err := planner.PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest: cacheLoadTableRequest().CatalogRequest,
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "main",
	})
	assertIcebergCode(t, err, api.ErrUnsupportedFeature)
}

func TestLocalScanPlannerRejectsServerPlanningRequiredMode(t *testing.T) {
	ctx := context.Background()
	fixture := newPlannerFixture(t, 1)
	planner := fixture.planner()
	planner.ServerPlanningMode = api.ServerPlanningRequired
	_, err := planner.PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest: cacheLoadTableRequest().CatalogRequest,
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "main",
	})
	assertIcebergCode(t, err, api.ErrServerPlanningRequired)
}

func TestLocalScanPlannerPrunesDataFilesByBounds(t *testing.T) {
	ctx := context.Background()
	fixture := newPlannerFixture(t, 2)
	firstManifest := fixture.facade.manifests[0].Path
	secondManifest := fixture.facade.manifests[1].Path
	fixture.facade.entries[firstManifest][0].DataFile.UpperBounds = map[int][]byte{1: icebergLongBound(50)}
	fixture.facade.entries[secondManifest][0].DataFile.LowerBounds = map[int][]byte{1: icebergLongBound(101)}
	fixture.facade.entries[secondManifest][0].DataFile.UpperBounds = map[int][]byte{1: icebergLongBound(200)}

	plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest: cacheLoadTableRequest().CatalogRequest,
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "main",
		ResidualSQL:    "id > 100",
		PrunePredicates: []api.PrunePredicate{{
			FieldID: 1,
			Op:      api.PruneOpGT,
			Literal: api.PruneLiteral{Kind: api.TypeLong, Int64: 100},
		}},
	})
	if err != nil {
		t.Fatalf("plan with data file bounds pruning: %v", err)
	}
	if len(plan.DataTasks) != 1 || !strings.Contains(plan.DataTasks[0].DataFile.FilePath, "file-1.parquet") {
		t.Fatalf("expected only second data file to remain, tasks=%+v", plan.DataTasks)
	}
	if plan.Profile.DataFilesSelected != 1 || plan.Profile.DataFilesPruned != 2 {
		t.Fatalf("unexpected data pruning profile: %+v", plan.Profile)
	}
	if plan.Profile.DataFileBytesSelected != 100 || plan.Profile.DataFileBytesPruned != 100 {
		t.Fatalf("unexpected data file byte pruning profile: %+v", plan.Profile)
	}
}

func TestLocalScanPlannerPlansAndPrunesRowGroupsFromParquetFooter(t *testing.T) {
	ctx := context.Background()
	fixture := newPlannerFixture(t, 1)
	manifest := fixture.facade.manifests[0].Path
	dataFile := &fixture.facade.entries[manifest][0].DataFile
	parquetData := writePlannerInt64ParquetWithRowGroups(t, []int64{1, 2, 101, 102}, 2)
	fixture.reader.data[dataFile.FilePath] = parquetData
	dataFile.RecordCount = 4
	dataFile.FileSizeInBytes = int64(len(parquetData))

	plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest:         cacheLoadTableRequest().CatalogRequest,
		Namespace:              api.Namespace{"sales"},
		Table:                  "orders",
		Ref:                    "main",
		EnableRowGroupPlanning: true,
		PrunePredicates: []api.PrunePredicate{{
			FieldID: 1,
			Op:      api.PruneOpGT,
			Literal: api.PruneLiteral{Kind: api.TypeLong, Int64: 50},
		}},
	})
	if err != nil {
		t.Fatalf("plan scan: %v", err)
	}
	if len(plan.DataTasks) != 1 {
		t.Fatalf("expected one row-group task after pruning, got %+v", plan.DataTasks)
	}
	rowGroups := plan.DataTasks[0].RowGroups
	if len(rowGroups) != 1 || rowGroups[0].Ordinal != 1 || rowGroups[0].StartRowOrdinal != 2 || rowGroups[0].RowCount != 2 {
		t.Fatalf("unexpected row group task: %+v", plan.DataTasks[0])
	}
	if plan.Profile.RowGroupsSelected != 1 || plan.Profile.RowGroupsPruned != 1 {
		t.Fatalf("unexpected row group profile: %+v", plan.Profile)
	}
}

func TestLocalScanPlannerKeepsFileTaskWhenRowGroupsDoNotPrune(t *testing.T) {
	ctx := context.Background()
	fixture := newPlannerFixture(t, 1)
	manifest := fixture.facade.manifests[0].Path
	dataFile := &fixture.facade.entries[manifest][0].DataFile
	parquetData := writePlannerInt64ParquetWithRowGroups(t, []int64{1, 2, 101, 102}, 2)
	fixture.reader.data[dataFile.FilePath] = parquetData
	dataFile.RecordCount = 4
	dataFile.FileSizeInBytes = int64(len(parquetData))

	plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest:         cacheLoadTableRequest().CatalogRequest,
		Namespace:              api.Namespace{"sales"},
		Table:                  "orders",
		Ref:                    "main",
		EnableRowGroupPlanning: true,
		PrunePredicates: []api.PrunePredicate{{
			FieldID: 1,
			Op:      api.PruneOpGT,
			Literal: api.PruneLiteral{Kind: api.TypeLong, Int64: 0},
		}},
	})
	if err != nil {
		t.Fatalf("plan scan: %v", err)
	}
	if len(plan.DataTasks) != 1 {
		t.Fatalf("expected one file-level task, got %+v", plan.DataTasks)
	}
	if len(plan.DataTasks[0].RowGroups) != 0 {
		t.Fatalf("expected file-level task to remain un-split when no row group was pruned, got %+v", plan.DataTasks[0])
	}
	if plan.Profile.RowGroupsSelected != 2 || plan.Profile.RowGroupsPruned != 0 {
		t.Fatalf("unexpected row group profile: %+v", plan.Profile)
	}
}

func TestLocalScanPlannerPrunesManifestByIdentityPartitionSummary(t *testing.T) {
	ctx := context.Background()
	fixture := newPlannerFixtureWithMetadata(t, 2, identityPartitionMetadataJSON())
	fixture.facade.manifests[0].Partitions = []api.PartitionFieldSummary{{
		LowerBound: icebergLongBound(1),
		UpperBound: icebergLongBound(10),
	}}
	fixture.facade.manifests[1].Partitions = []api.PartitionFieldSummary{{
		LowerBound: icebergLongBound(200),
		UpperBound: icebergLongBound(300),
	}}

	plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest: cacheLoadTableRequest().CatalogRequest,
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "main",
		ResidualSQL:    "id > 100",
		PrunePredicates: []api.PrunePredicate{{
			FieldID: 1,
			Op:      api.PruneOpGT,
			Literal: api.PruneLiteral{Kind: api.TypeLong, Int64: 100},
		}},
	})
	if err != nil {
		t.Fatalf("plan with manifest summary pruning: %v", err)
	}
	if len(plan.DataTasks) != 1 || !strings.Contains(plan.DataTasks[0].DataFile.FilePath, "file-1.parquet") {
		t.Fatalf("expected only second manifest's file to remain, tasks=%+v", plan.DataTasks)
	}
	if plan.Profile.ManifestsSelected != 1 || plan.Profile.ManifestsPruned != 1 {
		t.Fatalf("unexpected manifest pruning profile: %+v", plan.Profile)
	}
}

func TestLocalScanPlannerDoesNotPruneBucketOrTruncateTransforms(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name      string
		transform string
	}{
		{name: "bucket", transform: "bucket[16]"},
		{name: "truncate", transform: "truncate[4]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newPlannerFixtureWithMetadata(t, 2, idPartitionMetadataJSON(tc.transform))
			firstManifest := fixture.facade.manifests[0].Path
			secondManifest := fixture.facade.manifests[1].Path
			fixture.facade.manifests[0].Partitions = []api.PartitionFieldSummary{{
				LowerBound: icebergLongBound(1),
				UpperBound: icebergLongBound(10),
			}}
			fixture.facade.manifests[1].Partitions = []api.PartitionFieldSummary{{
				LowerBound: icebergLongBound(200),
				UpperBound: icebergLongBound(300),
			}}
			fixture.facade.entries[firstManifest][0].DataFile.Partition = map[string]any{"id": int64(1)}
			fixture.facade.entries[secondManifest][0].DataFile.Partition = map[string]any{"id": int64(200)}

			plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
				CatalogRequest: cacheLoadTableRequest().CatalogRequest,
				Namespace:      api.Namespace{"sales"},
				Table:          "orders",
				Ref:            "main",
				ResidualSQL:    "id > 100",
				PrunePredicates: []api.PrunePredicate{{
					FieldID: 1,
					Op:      api.PruneOpGT,
					Literal: api.PruneLiteral{Kind: api.TypeLong, Int64: 100},
				}},
			})
			if err != nil {
				t.Fatalf("plan with %s transform: %v", tc.transform, err)
			}
			if len(plan.DataTasks) != 2 {
				t.Fatalf("%s transform must not actively prune data files before golden vectors, tasks=%+v", tc.transform, plan.DataTasks)
			}
			if plan.Profile.ManifestsSelected != 2 || plan.Profile.ManifestsPruned != 0 {
				t.Fatalf("%s transform must not actively prune manifests, profile=%+v", tc.transform, plan.Profile)
			}
			if plan.Profile.DataFilesSelected != 2 || plan.Profile.DataFilesPruned != 1 {
				t.Fatalf("%s transform should only prune deleted entries, profile=%+v", tc.transform, plan.Profile)
			}
		})
	}
}

func TestIcebergBucketTruncateGoldenVectors(t *testing.T) {
	golden := loadBucketTruncateGolden(t)
	if golden.Source == "" {
		t.Fatalf("golden vector source must be recorded")
	}
	if len(golden.Bucket) != 5 || len(golden.Truncate) != 4 {
		t.Fatalf("unexpected golden vector coverage: bucket=%d truncate=%d", len(golden.Bucket), len(golden.Truncate))
	}
	requireBucketGolden(t, golden, "int", "0", "", 12)
	requireBucketGolden(t, golden, "long", "1234567890123456789", "", 1)
	requireBucketGolden(t, golden, "string", "abc", "", 10)
	requireBucketGolden(t, golden, "binary", "", "000102ff", 4)
	requireBucketGolden(t, golden, "decimal", "-1.23", "", 5)
	requireTruncateGolden(t, golden, "int", "-11", "", "-20", "")
	requireTruncateGolden(t, golden, "long", "1234567890123456789", "", "1234567890123456780", "")
	requireTruncateGolden(t, golden, "string", "abcde", "", "abcd", "")
	requireTruncateGolden(t, golden, "binary", "", "0001020304ff", "", "00010203")
}

func TestLocalScanPlannerBucketTruncateGoldenVectorsKeepActivePruningDisabled(t *testing.T) {
	golden := loadBucketTruncateGolden(t)
	cases := []struct {
		name            string
		transform       string
		literal         api.PruneLiteral
		predicateOp     api.PruneOp
		firstPartition  int64
		secondPartition int64
	}{
		{
			name:            "bucket int equality vector",
			transform:       "bucket[16]",
			literal:         api.PruneLiteral{Kind: api.TypeInt, Int64: 34},
			predicateOp:     api.PruneOpEQ,
			firstPartition:  int64(bucketGoldenValue(t, golden, "int", "34", "")),
			secondPartition: int64(bucketGoldenValue(t, golden, "int", "123456789", "")),
		},
		{
			name:            "truncate int range vector",
			transform:       "truncate[10]",
			literal:         api.PruneLiteral{Kind: api.TypeInt, Int64: 100},
			predicateOp:     api.PruneOpGT,
			firstPartition:  mustParseInt64(t, truncateGoldenValue(t, golden, "int", "-1", "", "")),
			secondPartition: mustParseInt64(t, truncateGoldenValue(t, golden, "int", "123456789", "", "")),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newPlannerFixtureWithMetadata(t, 2, idPartitionMetadataJSON(tc.transform))
			firstManifest := fixture.facade.manifests[0].Path
			secondManifest := fixture.facade.manifests[1].Path
			fixture.facade.manifests[0].Partitions = []api.PartitionFieldSummary{{
				LowerBound: icebergLongBound(tc.firstPartition),
				UpperBound: icebergLongBound(tc.firstPartition),
			}}
			fixture.facade.manifests[1].Partitions = []api.PartitionFieldSummary{{
				LowerBound: icebergLongBound(tc.secondPartition),
				UpperBound: icebergLongBound(tc.secondPartition),
			}}
			fixture.facade.entries[firstManifest][0].DataFile.Partition = map[string]any{"id": tc.firstPartition}
			fixture.facade.entries[secondManifest][0].DataFile.Partition = map[string]any{"id": tc.secondPartition}

			plan, err := fixture.planner().PlanScan(context.Background(), api.ScanPlanRequest{
				CatalogRequest: cacheLoadTableRequest().CatalogRequest,
				Namespace:      api.Namespace{"sales"},
				Table:          "orders",
				Ref:            "main",
				ResidualSQL:    "id predicate retained",
				PrunePredicates: []api.PrunePredicate{{
					FieldID: 1,
					Op:      tc.predicateOp,
					Literal: tc.literal,
				}},
			})
			if err != nil {
				t.Fatalf("plan with golden %s transform: %v", tc.transform, err)
			}
			if len(plan.DataTasks) != 2 {
				t.Fatalf("%s transform must not actively prune until production transform projection is enabled, tasks=%+v", tc.transform, plan.DataTasks)
			}
			if plan.Profile.ManifestsPruned != 0 || plan.Profile.DataFilesPruned != 1 {
				t.Fatalf("unexpected %s pruning profile: %+v", tc.transform, plan.Profile)
			}
		})
	}
}

func TestLocalScanPlannerPrunesManifestByDatePartitionTransforms(t *testing.T) {
	ctx := context.Background()
	literalDays := icebergDateDays(2024, time.January, 15)
	cases := []struct {
		name         string
		transform    string
		literalPart  int32
		olderPart    int32
		selectedPart int32
	}{
		{name: "year", transform: "year", literalPart: 54, olderPart: 53, selectedPart: 54},
		{name: "month", transform: "month", literalPart: 648, olderPart: 647, selectedPart: 648},
		{name: "day", transform: "day", literalPart: int32(literalDays), olderPart: int32(literalDays - 1), selectedPart: int32(literalDays)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newPlannerFixtureWithMetadata(t, 2, datePartitionMetadataJSON(tc.transform))
			fixture.facade.manifests[0].Partitions = []api.PartitionFieldSummary{{
				LowerBound: icebergIntBound(tc.olderPart),
				UpperBound: icebergIntBound(tc.olderPart),
			}}
			fixture.facade.manifests[1].Partitions = []api.PartitionFieldSummary{{
				LowerBound: icebergIntBound(tc.selectedPart),
				UpperBound: icebergIntBound(tc.selectedPart),
			}}

			plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
				CatalogRequest: cacheLoadTableRequest().CatalogRequest,
				Namespace:      api.Namespace{"sales"},
				Table:          "orders",
				Ref:            "main",
				ResidualSQL:    "created_at >= DATE '2024-01-15'",
				PrunePredicates: []api.PrunePredicate{{
					FieldID: 4,
					Op:      api.PruneOpGTE,
					Literal: api.PruneLiteral{Kind: api.TypeDate, Int64: literalDays},
				}},
			})
			if err != nil {
				t.Fatalf("plan with %s transform pruning: %v", tc.transform, err)
			}
			if len(plan.DataTasks) != 1 || !strings.Contains(plan.DataTasks[0].DataFile.FilePath, "file-1.parquet") {
				t.Fatalf("expected only selected %s manifest file to remain, tasks=%+v", tc.transform, plan.DataTasks)
			}
			if plan.Profile.ManifestsSelected != 1 || plan.Profile.ManifestsPruned != 1 {
				t.Fatalf("unexpected %s transform pruning profile: %+v", tc.transform, plan.Profile)
			}
		})
	}
}

func TestLocalScanPlannerKeepsLossyDateTransformBoundaryForStrictRanges(t *testing.T) {
	ctx := context.Background()
	literalDays := icebergDateDays(2026, time.June, 15)
	cases := []struct {
		name      string
		transform string
		part      int32
	}{
		{name: "year", transform: "year", part: int32(dateTransformYear(literalDays))},
		{name: "month", transform: "month", part: int32(dateTransformMonth(literalDays))},
	}
	for _, tc := range cases {
		t.Run(tc.name+"_gt_keeps_boundary_bucket", func(t *testing.T) {
			fixture := newPlannerFixtureWithMetadata(t, 2, datePartitionMetadataJSON(tc.transform))
			fixture.facade.manifests[0].Partitions = []api.PartitionFieldSummary{{
				LowerBound: icebergIntBound(tc.part - 1),
				UpperBound: icebergIntBound(tc.part - 1),
			}}
			fixture.facade.manifests[1].Partitions = []api.PartitionFieldSummary{{
				LowerBound: icebergIntBound(tc.part),
				UpperBound: icebergIntBound(tc.part),
			}}

			plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
				CatalogRequest: cacheLoadTableRequest().CatalogRequest,
				Namespace:      api.Namespace{"sales"},
				Table:          "orders",
				Ref:            "main",
				ResidualSQL:    "created_at > DATE '2026-06-15'",
				PrunePredicates: []api.PrunePredicate{{
					FieldID: 4,
					Op:      api.PruneOpGT,
					Literal: api.PruneLiteral{Kind: api.TypeDate, Int64: literalDays},
				}},
			})
			if err != nil {
				t.Fatalf("plan with %s strict gt boundary pruning: %v", tc.transform, err)
			}
			if len(plan.DataTasks) != 1 || !strings.Contains(plan.DataTasks[0].DataFile.FilePath, "file-1.parquet") {
				t.Fatalf("strict gt must keep boundary %s bucket, tasks=%+v", tc.transform, plan.DataTasks)
			}
			if plan.Profile.ManifestsSelected != 1 || plan.Profile.ManifestsPruned != 1 {
				t.Fatalf("unexpected strict gt %s profile: %+v", tc.transform, plan.Profile)
			}
		})

		t.Run(tc.name+"_lt_keeps_boundary_bucket", func(t *testing.T) {
			fixture := newPlannerFixtureWithMetadata(t, 2, datePartitionMetadataJSON(tc.transform))
			fixture.facade.manifests[0].Partitions = []api.PartitionFieldSummary{{
				LowerBound: icebergIntBound(tc.part),
				UpperBound: icebergIntBound(tc.part),
			}}
			fixture.facade.manifests[1].Partitions = []api.PartitionFieldSummary{{
				LowerBound: icebergIntBound(tc.part + 1),
				UpperBound: icebergIntBound(tc.part + 1),
			}}

			plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
				CatalogRequest: cacheLoadTableRequest().CatalogRequest,
				Namespace:      api.Namespace{"sales"},
				Table:          "orders",
				Ref:            "main",
				ResidualSQL:    "created_at < DATE '2026-06-15'",
				PrunePredicates: []api.PrunePredicate{{
					FieldID: 4,
					Op:      api.PruneOpLT,
					Literal: api.PruneLiteral{Kind: api.TypeDate, Int64: literalDays},
				}},
			})
			if err != nil {
				t.Fatalf("plan with %s strict lt boundary pruning: %v", tc.transform, err)
			}
			if len(plan.DataTasks) != 1 || !strings.Contains(plan.DataTasks[0].DataFile.FilePath, "file-0.parquet") {
				t.Fatalf("strict lt must keep boundary %s bucket, tasks=%+v", tc.transform, plan.DataTasks)
			}
			if plan.Profile.ManifestsSelected != 1 || plan.Profile.ManifestsPruned != 1 {
				t.Fatalf("unexpected strict lt %s profile: %+v", tc.transform, plan.Profile)
			}
		})
	}
}

func TestLocalScanPlannerKeepsLossyDatePartitionTupleBoundaryForStrictRanges(t *testing.T) {
	ctx := context.Background()
	literalDays := icebergDateDays(2026, time.June, 15)
	monthPart := int32(dateTransformMonth(literalDays))

	t.Run("gt_keeps_boundary_bucket", func(t *testing.T) {
		fixture := newPlannerFixtureWithMetadata(t, 2, datePartitionMetadataJSON("month"))
		firstManifest := fixture.facade.manifests[0].Path
		secondManifest := fixture.facade.manifests[1].Path
		fixture.facade.entries[firstManifest][0].DataFile.Partition = map[string]any{"created_day": monthPart - 1}
		fixture.facade.entries[secondManifest][0].DataFile.Partition = map[string]any{"created_day": monthPart}

		plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
			CatalogRequest: cacheLoadTableRequest().CatalogRequest,
			Namespace:      api.Namespace{"sales"},
			Table:          "orders",
			Ref:            "main",
			ResidualSQL:    "created_at > DATE '2026-06-15'",
			PrunePredicates: []api.PrunePredicate{{
				FieldID: 4,
				Op:      api.PruneOpGT,
				Literal: api.PruneLiteral{Kind: api.TypeDate, Int64: literalDays},
			}},
		})
		if err != nil {
			t.Fatalf("plan with strict gt date month tuple pruning: %v", err)
		}
		if len(plan.DataTasks) != 1 || !strings.Contains(plan.DataTasks[0].DataFile.FilePath, "file-1.parquet") {
			t.Fatalf("strict gt must keep boundary month tuple, tasks=%+v", plan.DataTasks)
		}
		if plan.Profile.DataFilesSelected != 1 || plan.Profile.DataFilesPruned != 2 {
			t.Fatalf("unexpected strict gt tuple profile: %+v", plan.Profile)
		}
	})

	t.Run("lt_keeps_boundary_bucket", func(t *testing.T) {
		fixture := newPlannerFixtureWithMetadata(t, 2, datePartitionMetadataJSON("month"))
		firstManifest := fixture.facade.manifests[0].Path
		secondManifest := fixture.facade.manifests[1].Path
		fixture.facade.entries[firstManifest][0].DataFile.Partition = map[string]any{"created_day": monthPart}
		fixture.facade.entries[secondManifest][0].DataFile.Partition = map[string]any{"created_day": monthPart + 1}

		plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
			CatalogRequest: cacheLoadTableRequest().CatalogRequest,
			Namespace:      api.Namespace{"sales"},
			Table:          "orders",
			Ref:            "main",
			ResidualSQL:    "created_at < DATE '2026-06-15'",
			PrunePredicates: []api.PrunePredicate{{
				FieldID: 4,
				Op:      api.PruneOpLT,
				Literal: api.PruneLiteral{Kind: api.TypeDate, Int64: literalDays},
			}},
		})
		if err != nil {
			t.Fatalf("plan with strict lt date month tuple pruning: %v", err)
		}
		if len(plan.DataTasks) != 1 || !strings.Contains(plan.DataTasks[0].DataFile.FilePath, "file-0.parquet") {
			t.Fatalf("strict lt must keep boundary month tuple, tasks=%+v", plan.DataTasks)
		}
		if plan.Profile.DataFilesSelected != 1 || plan.Profile.DataFilesPruned != 2 {
			t.Fatalf("unexpected strict lt tuple profile: %+v", plan.Profile)
		}
	})
}

func TestLocalScanPlannerKeepsLossyTimestampTransformBoundaryForStrictRanges(t *testing.T) {
	ctx := context.Background()
	literalTime := time.Date(2024, time.January, 15, 10, 30, 0, 0, time.UTC)
	literalMicros := literalTime.UnixMicro()
	cases := []struct {
		name      string
		transform string
		part      int32
	}{
		{name: "year", transform: "year", part: int32(timestampTransformYear(literalMicros))},
		{name: "month", transform: "month", part: int32(timestampTransformMonth(literalMicros))},
		{name: "day", transform: "day", part: int32(timestampTransformDay(literalMicros))},
		{name: "hour", transform: "hour", part: int32(floorDiv(literalMicros, int64(time.Hour/time.Microsecond)))},
	}
	for _, tc := range cases {
		t.Run(tc.name+"_gt_keeps_boundary_bucket", func(t *testing.T) {
			fixture := newPlannerFixtureWithMetadata(t, 2, timestampPartitionMetadataJSON(tc.transform))
			fixture.facade.manifests[0].Partitions = []api.PartitionFieldSummary{{
				LowerBound: icebergIntBound(tc.part - 1),
				UpperBound: icebergIntBound(tc.part - 1),
			}}
			fixture.facade.manifests[1].Partitions = []api.PartitionFieldSummary{{
				LowerBound: icebergIntBound(tc.part),
				UpperBound: icebergIntBound(tc.part),
			}}

			plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
				CatalogRequest: cacheLoadTableRequest().CatalogRequest,
				Namespace:      api.Namespace{"sales"},
				Table:          "orders",
				Ref:            "main",
				ResidualSQL:    "created_at > TIMESTAMP '2024-01-15 10:30:00'",
				PrunePredicates: []api.PrunePredicate{{
					FieldID: 4,
					Op:      api.PruneOpGT,
					Literal: api.PruneLiteral{Kind: api.TypeTimestampTZ, Int64: literalMicros, Normalized: true},
				}},
			})
			if err != nil {
				t.Fatalf("plan with %s strict gt boundary pruning: %v", tc.transform, err)
			}
			if len(plan.DataTasks) != 1 || !strings.Contains(plan.DataTasks[0].DataFile.FilePath, "file-1.parquet") {
				t.Fatalf("strict gt must keep boundary %s bucket, tasks=%+v", tc.transform, plan.DataTasks)
			}
		})

		t.Run(tc.name+"_lt_keeps_boundary_bucket", func(t *testing.T) {
			fixture := newPlannerFixtureWithMetadata(t, 2, timestampPartitionMetadataJSON(tc.transform))
			fixture.facade.manifests[0].Partitions = []api.PartitionFieldSummary{{
				LowerBound: icebergIntBound(tc.part),
				UpperBound: icebergIntBound(tc.part),
			}}
			fixture.facade.manifests[1].Partitions = []api.PartitionFieldSummary{{
				LowerBound: icebergIntBound(tc.part + 1),
				UpperBound: icebergIntBound(tc.part + 1),
			}}

			plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
				CatalogRequest: cacheLoadTableRequest().CatalogRequest,
				Namespace:      api.Namespace{"sales"},
				Table:          "orders",
				Ref:            "main",
				ResidualSQL:    "created_at < TIMESTAMP '2024-01-15 10:30:00'",
				PrunePredicates: []api.PrunePredicate{{
					FieldID: 4,
					Op:      api.PruneOpLT,
					Literal: api.PruneLiteral{Kind: api.TypeTimestampTZ, Int64: literalMicros, Normalized: true},
				}},
			})
			if err != nil {
				t.Fatalf("plan with %s strict lt boundary pruning: %v", tc.transform, err)
			}
			if len(plan.DataTasks) != 1 || !strings.Contains(plan.DataTasks[0].DataFile.FilePath, "file-0.parquet") {
				t.Fatalf("strict lt must keep boundary %s bucket, tasks=%+v", tc.transform, plan.DataTasks)
			}
		})
	}
}

func TestLocalScanPlannerPrunesManifestByNormalizedHourTransform(t *testing.T) {
	ctx := context.Background()
	fixture := newPlannerFixtureWithMetadata(t, 2, hourPartitionMetadataJSON())
	literalTime := time.Date(2024, time.January, 15, 10, 30, 0, 0, time.UTC)
	literalHour := int32(literalTime.Unix() / int64(time.Hour/time.Second))
	fixture.facade.manifests[0].Partitions = []api.PartitionFieldSummary{{
		LowerBound: icebergIntBound(literalHour - 1),
		UpperBound: icebergIntBound(literalHour - 1),
	}}
	fixture.facade.manifests[1].Partitions = []api.PartitionFieldSummary{{
		LowerBound: icebergIntBound(literalHour),
		UpperBound: icebergIntBound(literalHour),
	}}

	plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest: cacheLoadTableRequest().CatalogRequest,
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "main",
		ResidualSQL:    "created_at >= TIMESTAMP '2024-01-15 10:30:00'",
		PrunePredicates: []api.PrunePredicate{{
			FieldID: 4,
			Op:      api.PruneOpGTE,
			Literal: api.PruneLiteral{Kind: api.TypeTimestampTZ, Int64: literalTime.UnixMicro(), Normalized: true},
		}},
	})
	if err != nil {
		t.Fatalf("plan with normalized hour transform pruning: %v", err)
	}
	if len(plan.DataTasks) != 1 || !strings.Contains(plan.DataTasks[0].DataFile.FilePath, "file-1.parquet") {
		t.Fatalf("expected only selected hour manifest file to remain, tasks=%+v", plan.DataTasks)
	}
	if plan.Profile.ManifestsSelected != 1 || plan.Profile.ManifestsPruned != 1 {
		t.Fatalf("unexpected hour transform pruning profile: %+v", plan.Profile)
	}
}

func TestLocalScanPlannerPrunesDataFileByPartitionTuple(t *testing.T) {
	ctx := context.Background()
	fixture := newPlannerFixtureWithMetadata(t, 2, identityPartitionMetadataJSON())
	firstManifest := fixture.facade.manifests[0].Path
	secondManifest := fixture.facade.manifests[1].Path
	fixture.facade.entries[firstManifest][0].DataFile.Partition = map[string]any{"id": int64(10)}
	fixture.facade.entries[secondManifest][0].DataFile.Partition = map[string]any{"id": int64(200)}

	plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest: cacheLoadTableRequest().CatalogRequest,
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "main",
		ResidualSQL:    "id > 100",
		PrunePredicates: []api.PrunePredicate{{
			FieldID: 1,
			Op:      api.PruneOpGT,
			Literal: api.PruneLiteral{Kind: api.TypeLong, Int64: 100},
		}},
	})
	if err != nil {
		t.Fatalf("plan with partition tuple pruning: %v", err)
	}
	if len(plan.DataTasks) != 1 || !strings.Contains(plan.DataTasks[0].DataFile.FilePath, "file-1.parquet") {
		t.Fatalf("expected only second partition tuple file to remain, tasks=%+v", plan.DataTasks)
	}
	if plan.Profile.DataFilesSelected != 1 || plan.Profile.DataFilesPruned != 2 {
		t.Fatalf("unexpected partition tuple pruning profile: %+v", plan.Profile)
	}
}

func TestLocalScanPlannerPrunesZeroRecordDataFile(t *testing.T) {
	ctx := context.Background()
	fixture := newPlannerFixture(t, 2)
	firstManifest := fixture.facade.manifests[0].Path
	fixture.facade.entries[firstManifest][0].DataFile.RecordCount = 0

	plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest: cacheLoadTableRequest().CatalogRequest,
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "main",
	})
	if err != nil {
		t.Fatalf("plan with zero-record pruning: %v", err)
	}
	if len(plan.DataTasks) != 1 || !strings.Contains(plan.DataTasks[0].DataFile.FilePath, "file-1.parquet") {
		t.Fatalf("expected zero-record first data file to be pruned, tasks=%+v", plan.DataTasks)
	}
	if plan.Profile.DataFilesPruned != 2 {
		t.Fatalf("unexpected zero-record pruning profile: %+v", plan.Profile)
	}
	if plan.Profile.DataFileBytesPruned != 100 || plan.Profile.DataFileBytesSelected != 100 {
		t.Fatalf("unexpected zero-record byte profile: %+v", plan.Profile)
	}
}

func TestLocalScanPlannerRejectsInvalidDataFileSizeMetrics(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		update func(*api.DataFile)
	}{
		{
			name: "negative-record-count",
			update: func(file *api.DataFile) {
				file.RecordCount = -1
			},
		},
		{
			name: "negative-file-size",
			update: func(file *api.DataFile) {
				file.FileSizeInBytes = -1
			},
		},
		{
			name: "rows-without-file-size",
			update: func(file *api.DataFile) {
				file.RecordCount = 10
				file.FileSizeInBytes = 0
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newPlannerFixture(t, 1)
			manifest := fixture.facade.manifests[0].Path
			tc.update(&fixture.facade.entries[manifest][0].DataFile)
			_, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
				CatalogRequest: cacheLoadTableRequest().CatalogRequest,
				Namespace:      api.Namespace{"sales"},
				Table:          "orders",
				Ref:            "main",
			})
			assertIcebergCode(t, err, api.ErrMetadataInvalid)
		})
	}
}

func TestLocalScanPlannerPrunesAllNullDataFile(t *testing.T) {
	ctx := context.Background()
	fixture := newPlannerFixture(t, 2)
	firstManifest := fixture.facade.manifests[0].Path
	fixture.facade.entries[firstManifest][0].DataFile.ValueCounts = map[int]int64{1: 10}
	fixture.facade.entries[firstManifest][0].DataFile.NullValueCounts = map[int]int64{1: 10}

	plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest: cacheLoadTableRequest().CatalogRequest,
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "main",
		ResidualSQL:    "id = 7",
		PrunePredicates: []api.PrunePredicate{{
			FieldID: 1,
			Op:      api.PruneOpEQ,
			Literal: api.PruneLiteral{Kind: api.TypeLong, Int64: 7},
		}},
	})
	if err != nil {
		t.Fatalf("plan with all-null data file pruning: %v", err)
	}
	if len(plan.DataTasks) != 1 || !strings.Contains(plan.DataTasks[0].DataFile.FilePath, "file-1.parquet") {
		t.Fatalf("expected all-null first data file to be pruned, tasks=%+v", plan.DataTasks)
	}
	if plan.Profile.DataFilesPruned != 2 {
		t.Fatalf("unexpected all-null pruning profile: %+v", plan.Profile)
	}
}

func TestLocalScanPlannerPrunesAllNaNDataFile(t *testing.T) {
	ctx := context.Background()
	fixture := newPlannerFixtureWithMetadata(t, 2, doublePriceMetadataJSON())
	firstManifest := fixture.facade.manifests[0].Path
	fixture.facade.entries[firstManifest][0].DataFile.ValueCounts = map[int]int64{3: 10}
	fixture.facade.entries[firstManifest][0].DataFile.NaNValueCounts = map[int]int64{3: 10}

	plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest: cacheLoadTableRequest().CatalogRequest,
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "main",
		ResidualSQL:    "price > 1.25",
		PrunePredicates: []api.PrunePredicate{{
			FieldID: 3,
			Op:      api.PruneOpGT,
			Literal: api.PruneLiteral{Kind: api.TypeDouble, Float64: 1.25},
		}},
	})
	if err != nil {
		t.Fatalf("plan with all-NaN data file pruning: %v", err)
	}
	if len(plan.DataTasks) != 1 || !strings.Contains(plan.DataTasks[0].DataFile.FilePath, "file-1.parquet") {
		t.Fatalf("expected all-NaN first data file to be pruned, tasks=%+v", plan.DataTasks)
	}
	if plan.Profile.DataFilesPruned != 2 {
		t.Fatalf("unexpected all-NaN pruning profile: %+v", plan.Profile)
	}
}

func TestLocalScanPlannerPrunesDoubleBounds(t *testing.T) {
	ctx := context.Background()
	fixture := newPlannerFixtureWithMetadata(t, 2, doublePriceMetadataJSON())
	firstManifest := fixture.facade.manifests[0].Path
	secondManifest := fixture.facade.manifests[1].Path
	fixture.facade.entries[firstManifest][0].DataFile.UpperBounds = map[int][]byte{3: icebergDoubleBound(1.0)}
	fixture.facade.entries[secondManifest][0].DataFile.LowerBounds = map[int][]byte{3: icebergDoubleBound(2.0)}
	fixture.facade.entries[secondManifest][0].DataFile.UpperBounds = map[int][]byte{3: icebergDoubleBound(3.0)}

	plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest: cacheLoadTableRequest().CatalogRequest,
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "main",
		ResidualSQL:    "price > 1.25",
		PrunePredicates: []api.PrunePredicate{{
			FieldID: 3,
			Op:      api.PruneOpGT,
			Literal: api.PruneLiteral{Kind: api.TypeDouble, Float64: 1.25},
		}},
	})
	if err != nil {
		t.Fatalf("plan with double bounds pruning: %v", err)
	}
	if len(plan.DataTasks) != 1 || !strings.Contains(plan.DataTasks[0].DataFile.FilePath, "file-1.parquet") {
		t.Fatalf("expected only second double-bounds file to remain, tasks=%+v", plan.DataTasks)
	}
	if plan.Profile.DataFilesPruned != 2 {
		t.Fatalf("unexpected double bounds pruning profile: %+v", plan.Profile)
	}
}

func TestLocalScanPlannerDoesNotPruneTimestampTZBounds(t *testing.T) {
	ctx := context.Background()
	fixture := newPlannerFixture(t, 1)
	manifest := fixture.facade.manifests[0].Path
	fixture.facade.entries[manifest][0].DataFile.LowerBounds = map[int][]byte{4: icebergLongBound(0)}
	fixture.facade.entries[manifest][0].DataFile.UpperBounds = map[int][]byte{4: icebergLongBound(1)}

	plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest: cacheLoadTableRequest().CatalogRequest,
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "main",
		ResidualSQL:    "created_at > TIMESTAMP '2026-01-01 00:00:00'",
		PrunePredicates: []api.PrunePredicate{{
			FieldID: 4,
			Op:      api.PruneOpGT,
			Literal: api.PruneLiteral{Kind: api.TypeTimestampTZ, Int64: 1767225600000000},
		}},
	})
	if err != nil {
		t.Fatalf("plan with timestamptz pruning hint: %v", err)
	}
	if len(plan.DataTasks) != 1 || plan.Profile.DataFilesPruned != 1 {
		t.Fatalf("timestamptz bounds must not actively prune data files, tasks=%+v profile=%+v", plan.DataTasks, plan.Profile)
	}
}

func TestLocalScanPlannerPrunesNormalizedTimestampTZBounds(t *testing.T) {
	ctx := context.Background()
	fixture := newPlannerFixture(t, 2)
	firstManifest := fixture.facade.manifests[0].Path
	secondManifest := fixture.facade.manifests[1].Path
	literalMicros := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	fixture.facade.entries[firstManifest][0].DataFile.UpperBounds = map[int][]byte{4: icebergLongBound(literalMicros - int64(time.Hour/time.Microsecond))}
	fixture.facade.entries[secondManifest][0].DataFile.LowerBounds = map[int][]byte{4: icebergLongBound(literalMicros + int64(time.Hour/time.Microsecond))}
	fixture.facade.entries[secondManifest][0].DataFile.UpperBounds = map[int][]byte{4: icebergLongBound(literalMicros + 2*int64(time.Hour/time.Microsecond))}

	plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest: cacheLoadTableRequest().CatalogRequest,
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "main",
		ResidualSQL:    "created_at > TIMESTAMP '2026-01-01 00:00:00'",
		PrunePredicates: []api.PrunePredicate{{
			FieldID: 4,
			Op:      api.PruneOpGT,
			Literal: api.PruneLiteral{Kind: api.TypeTimestampTZ, Int64: literalMicros, Normalized: true},
		}},
	})
	if err != nil {
		t.Fatalf("plan with normalized timestamptz pruning hint: %v", err)
	}
	if len(plan.DataTasks) != 1 || !strings.Contains(plan.DataTasks[0].DataFile.FilePath, "file-1.parquet") {
		t.Fatalf("expected normalized timestamptz bounds to prune first data file, tasks=%+v", plan.DataTasks)
	}
	if plan.Profile.DataFilesPruned != 2 {
		t.Fatalf("unexpected normalized timestamptz pruning profile: %+v", plan.Profile)
	}
}

func TestLocalScanPlannerTimestampPruningUTCPlus3Golden(t *testing.T) {
	ctx := context.Background()
	ksa := time.FixedZone("UTC+3", 3*int(time.Hour/time.Second))
	localMidnight := time.Date(2026, time.January, 1, 0, 0, 0, 0, ksa)
	timestamptzMicros := localMidnight.UTC().UnixMicro()
	naiveUTCMicros := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	if timestamptzMicros != naiveUTCMicros-3*int64(time.Hour/time.Microsecond) {
		t.Fatalf("bad UTC+3 golden setup: timestamptz=%d naive=%d", timestamptzMicros, naiveUTCMicros)
	}

	t.Run("unnormalized_timestamptz_keeps_residual_only", func(t *testing.T) {
		fixture := newPlannerFixture(t, 2)
		firstManifest := fixture.facade.manifests[0].Path
		fixture.facade.entries[firstManifest][0].DataFile.LowerBounds = map[int][]byte{4: icebergLongBound(timestamptzMicros + int64(time.Hour/time.Microsecond))}
		fixture.facade.entries[firstManifest][0].DataFile.UpperBounds = map[int][]byte{4: icebergLongBound(timestamptzMicros + 2*int64(time.Hour/time.Microsecond))}

		plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
			CatalogRequest: cacheLoadTableRequest().CatalogRequest,
			Namespace:      api.Namespace{"sales"},
			Table:          "orders",
			Ref:            "main",
			ResidualSQL:    "created_at > TIMESTAMP '2026-01-01 00:00:00'",
			PrunePredicates: []api.PrunePredicate{{
				FieldID: 4,
				Op:      api.PruneOpGT,
				Literal: api.PruneLiteral{Kind: api.TypeTimestampTZ, Int64: naiveUTCMicros},
			}},
		})
		if err != nil {
			t.Fatalf("plan with unnormalized UTC+3 timestamptz predicate: %v", err)
		}
		if len(plan.DataTasks) != 2 || plan.Profile.DataFilesPruned != 1 {
			t.Fatalf("unnormalized timestamptz must not actively prune, tasks=%+v profile=%+v", plan.DataTasks, plan.Profile)
		}
	})

	t.Run("normalized_timestamptz_uses_utc_instant_micros", func(t *testing.T) {
		fixture := newPlannerFixture(t, 2)
		firstManifest := fixture.facade.manifests[0].Path
		secondManifest := fixture.facade.manifests[1].Path
		fixture.facade.entries[firstManifest][0].DataFile.UpperBounds = map[int][]byte{4: icebergLongBound(timestamptzMicros - int64(time.Hour/time.Microsecond))}
		fixture.facade.entries[secondManifest][0].DataFile.LowerBounds = map[int][]byte{4: icebergLongBound(timestamptzMicros + int64(time.Hour/time.Microsecond))}
		fixture.facade.entries[secondManifest][0].DataFile.UpperBounds = map[int][]byte{4: icebergLongBound(timestamptzMicros + 2*int64(time.Hour/time.Microsecond))}

		plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
			CatalogRequest: cacheLoadTableRequest().CatalogRequest,
			Namespace:      api.Namespace{"sales"},
			Table:          "orders",
			Ref:            "main",
			ResidualSQL:    "created_at > TIMESTAMP '2026-01-01 00:00:00'",
			PrunePredicates: []api.PrunePredicate{{
				FieldID: 4,
				Op:      api.PruneOpGT,
				Literal: api.PruneLiteral{Kind: api.TypeTimestampTZ, Int64: timestamptzMicros, Normalized: true},
			}},
		})
		if err != nil {
			t.Fatalf("plan with normalized UTC+3 timestamptz predicate: %v", err)
		}
		if len(plan.DataTasks) != 1 || !strings.Contains(plan.DataTasks[0].DataFile.FilePath, "file-1.parquet") {
			t.Fatalf("expected UTC+3 normalized timestamptz to prune only first file, tasks=%+v", plan.DataTasks)
		}
		if plan.Profile.DataFilesPruned != 2 {
			t.Fatalf("unexpected UTC+3 timestamptz pruning profile: %+v", plan.Profile)
		}
	})
}

func TestLocalScanPlannerPrunesNormalizedTimestampNoZoneBounds(t *testing.T) {
	ctx := context.Background()
	fixture := newPlannerFixtureWithMetadata(t, 2, timestampNoZoneMetadataJSON())
	firstManifest := fixture.facade.manifests[0].Path
	secondManifest := fixture.facade.manifests[1].Path
	literalMicros := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	fixture.facade.entries[firstManifest][0].DataFile.UpperBounds = map[int][]byte{4: icebergLongBound(literalMicros - int64(time.Hour/time.Microsecond))}
	fixture.facade.entries[secondManifest][0].DataFile.LowerBounds = map[int][]byte{4: icebergLongBound(literalMicros + int64(time.Hour/time.Microsecond))}
	fixture.facade.entries[secondManifest][0].DataFile.UpperBounds = map[int][]byte{4: icebergLongBound(literalMicros + 2*int64(time.Hour/time.Microsecond))}

	plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest: cacheLoadTableRequest().CatalogRequest,
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "main",
		ResidualSQL:    "created_at > TIMESTAMP '2026-01-01 00:00:00'",
		PrunePredicates: []api.PrunePredicate{{
			FieldID: 4,
			Op:      api.PruneOpGT,
			Literal: api.PruneLiteral{Kind: api.TypeTimestamp, Int64: literalMicros, Normalized: true},
		}},
	})
	if err != nil {
		t.Fatalf("plan with normalized timestamp predicate: %v", err)
	}
	if len(plan.DataTasks) != 1 || !strings.Contains(plan.DataTasks[0].DataFile.FilePath, "file-1.parquet") {
		t.Fatalf("expected normalized timestamp bounds to prune first data file, tasks=%+v", plan.DataTasks)
	}
	if plan.Profile.DataFilesPruned != 2 {
		t.Fatalf("unexpected normalized timestamp pruning profile: %+v", plan.Profile)
	}
}

type bucketTruncateGolden struct {
	Source   string                  `json:"source"`
	Bucket   []bucketGoldenSection   `json:"bucket"`
	Truncate []truncateGoldenSection `json:"truncate"`
}

type bucketGoldenSection struct {
	Type       string             `json:"type"`
	Scale      int                `json:"scale,omitempty"`
	NumBuckets int                `json:"num_buckets"`
	Cases      []bucketGoldenCase `json:"cases"`
}

type bucketGoldenCase struct {
	Input    string `json:"input,omitempty"`
	InputHex string `json:"input_hex,omitempty"`
	Expected int    `json:"expected"`
}

type truncateGoldenSection struct {
	Type  string               `json:"type"`
	Width int                  `json:"width"`
	Cases []truncateGoldenCase `json:"cases"`
}

type truncateGoldenCase struct {
	Input       string `json:"input,omitempty"`
	InputHex    string `json:"input_hex,omitempty"`
	Expected    string `json:"expected,omitempty"`
	ExpectedHex string `json:"expected_hex,omitempty"`
}

func loadBucketTruncateGolden(t *testing.T) bucketTruncateGolden {
	t.Helper()
	data, err := os.ReadFile("testdata/bucket_truncate_golden.json")
	if err != nil {
		t.Fatalf("read bucket/truncate golden vectors: %v", err)
	}
	var golden bucketTruncateGolden
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatalf("decode bucket/truncate golden vectors: %v", err)
	}
	return golden
}

func requireBucketGolden(t *testing.T, golden bucketTruncateGolden, typ, input, inputHex string, expected int) {
	t.Helper()
	if got := bucketGoldenValue(t, golden, typ, input, inputHex); got != expected {
		t.Fatalf("bucket golden %s input=%q hex=%q got %d, expected %d", typ, input, inputHex, got, expected)
	}
}

func bucketGoldenValue(t *testing.T, golden bucketTruncateGolden, typ, input, inputHex string) int {
	t.Helper()
	for _, section := range golden.Bucket {
		if section.Type != typ {
			continue
		}
		if section.NumBuckets != 16 {
			t.Fatalf("bucket golden %s uses unexpected bucket count %d", typ, section.NumBuckets)
		}
		for _, tc := range section.Cases {
			if tc.Input == input && tc.InputHex == inputHex {
				return tc.Expected
			}
		}
	}
	t.Fatalf("missing bucket golden %s input=%q hex=%q", typ, input, inputHex)
	return 0
}

func requireTruncateGolden(t *testing.T, golden bucketTruncateGolden, typ, input, inputHex, expected, expectedHex string) {
	t.Helper()
	want := expected
	if expectedHex != "" {
		want = expectedHex
	}
	if got := truncateGoldenValue(t, golden, typ, input, inputHex, expectedHex); got != want {
		t.Fatalf("truncate golden %s input=%q hex=%q got %q, expected %q", typ, input, inputHex, got, want)
	}
}

func truncateGoldenValue(t *testing.T, golden bucketTruncateGolden, typ, input, inputHex, expectedHex string) string {
	t.Helper()
	for _, section := range golden.Truncate {
		if section.Type != typ {
			continue
		}
		if section.Width != 4 && section.Width != 10 {
			t.Fatalf("truncate golden %s uses unexpected width %d", typ, section.Width)
		}
		for _, tc := range section.Cases {
			if tc.Input != input || tc.InputHex != inputHex {
				continue
			}
			if expectedHex != "" || tc.ExpectedHex != "" {
				if tc.ExpectedHex != expectedHex {
					t.Fatalf("truncate golden %s input=%q hex=%q got expected_hex=%q, expected %q", typ, input, inputHex, tc.ExpectedHex, expectedHex)
				}
				return tc.ExpectedHex
			}
			return tc.Expected
		}
	}
	t.Fatalf("missing truncate golden %s input=%q hex=%q", typ, input, inputHex)
	return ""
}

func mustParseInt64(t *testing.T, value string) int64 {
	t.Helper()
	out, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		t.Fatalf("parse int64 %q: %v", value, err)
	}
	return out
}

type plannerFixture struct {
	facade       *plannerMetadataFacade
	reader       *plannerObjectReader
	client       *catalog.MockClient
	cache        *Cache
	catalogLoads int
}

func newPlannerFixture(t *testing.T, manifestCount int) *plannerFixture {
	t.Helper()
	return newPlannerFixtureWithMetadata(t, manifestCount, sampleMetadataJSON)
}

func newPlannerFixtureWithMetadata(t *testing.T, manifestCount int, metadataJSON string) *plannerFixture {
	t.Helper()
	manifestListPath := "s3://warehouse/sales/orders/metadata/snap-22.avro"
	manifests := make([]api.ManifestFile, 0, manifestCount)
	entries := make(map[string][]api.ManifestEntry, manifestCount)
	data := map[string][]byte{
		manifestListPath: []byte("manifest-list"),
	}
	for i := 0; i < manifestCount; i++ {
		path := "s3://warehouse/sales/orders/metadata/m" + strconv.Itoa(i) + ".avro"
		manifests = append(manifests, api.ManifestFile{
			Path:            path,
			Length:          int64(10 + i),
			PartitionSpecID: 0,
			Content:         api.ManifestContentData,
		})
		data[path] = []byte(path)
		entries[path] = []api.ManifestEntry{{
			Status:     api.ManifestEntryAdded,
			SnapshotID: 22,
			DataFile: api.DataFile{
				Content:         api.DataFileContentData,
				FilePath:        "s3://warehouse/sales/orders/data/file-" + strconv.Itoa(i) + ".parquet",
				FileFormat:      "parquet",
				RecordCount:     10,
				FileSizeInBytes: 100,
				SpecID:          0,
			},
		}}
	}
	if manifestCount > 0 {
		entries[manifests[0].Path] = append(entries[manifests[0].Path], api.ManifestEntry{
			Status:     api.ManifestEntryDeleted,
			SnapshotID: 22,
			DataFile: api.DataFile{
				Content:    api.DataFileContentData,
				FilePath:   "s3://warehouse/sales/orders/data/deleted.parquet",
				FileFormat: "parquet",
				SpecID:     0,
			},
		})
	}

	fixture := &plannerFixture{
		cache:  NewCache(time.Minute),
		reader: &plannerObjectReader{data: data},
		facade: &plannerMetadataFacade{manifests: manifests, entries: entries},
	}
	fixture.client = &catalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			fixture.catalogLoads++
			return &api.LoadTableResponse{
				MetadataLocation: manifestListPath[:strings.LastIndex(manifestListPath, "/")] + "/v2.metadata.json",
				MetadataJSON:     []byte(metadataJSON),
				ETag:             "etag-1",
			}, nil
		},
	}
	return fixture
}

func icebergLongBound(value int64) []byte {
	out := make([]byte, 8)
	binary.LittleEndian.PutUint64(out, uint64(value))
	return out
}

func icebergIntBound(value int32) []byte {
	out := make([]byte, 4)
	binary.LittleEndian.PutUint32(out, uint32(value))
	return out
}

func icebergDoubleBound(value float64) []byte {
	out := make([]byte, 8)
	binary.LittleEndian.PutUint64(out, math.Float64bits(value))
	return out
}

func writePlannerInt64ParquetWithRowGroups(t *testing.T, values []int64, rowsPerGroup int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{
		"id": parquet.FieldID(parquet.Leaf(parquet.Int64Type), 1),
	})
	w := parquet.NewWriter(&buf, schema, parquet.MaxRowsPerRowGroup(rowsPerGroup))
	rows := make([]parquet.Row, len(values))
	for i, value := range values {
		rows[i] = parquet.Row{parquet.Int64Value(value).Level(0, 0, 0)}
	}
	if _, err := w.WriteRows(rows); err != nil {
		t.Fatalf("write parquet rows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close parquet writer: %v", err)
	}
	return buf.Bytes()
}

func icebergDateDays(year int, month time.Month, day int) int64 {
	date := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	return int64(date.Sub(time.Unix(0, 0).UTC()).Hours() / 24)
}

func identityPartitionMetadataJSON() string {
	return idPartitionMetadataJSON("identity")
}

func idPartitionMetadataJSON(transform string) string {
	return strings.Replace(sampleMetadataJSON,
		`{"source-id": 4, "field-id": 1000, "name": "created_day", "transform": "day"}`,
		`{"source-id": 1, "field-id": 1000, "name": "id", "transform": "`+transform+`"}`,
		1,
	)
}

func datePartitionMetadataJSON(transform string) string {
	out := strings.Replace(sampleMetadataJSON, `"type": "timestamptz"`, `"type": "date"`, 1)
	return strings.Replace(out, `"transform": "day"`, `"transform": "`+transform+`"`, 1)
}

func hourPartitionMetadataJSON() string {
	return timestampPartitionMetadataJSON("hour")
}

func timestampPartitionMetadataJSON(transform string) string {
	return strings.Replace(sampleMetadataJSON, `"transform": "day"`, `"transform": "`+transform+`"`, 1)
}

func timestampNoZoneMetadataJSON() string {
	return strings.Replace(sampleMetadataJSON, `"type": "timestamptz"`, `"type": "timestamp"`, 1)
}

func doublePriceMetadataJSON() string {
	return strings.Replace(sampleMetadataJSON, `"type": "decimal(12, 2)"`, `"type": "double"`, 1)
}

func (f *plannerFixture) planner() LocalScanPlanner {
	return LocalScanPlanner{
		Catalog:                 f.client,
		Metadata:                f.facade,
		ObjectReader:            f.reader,
		Cache:                   f.cache,
		CredentialHash:          "cred-hash",
		ManifestReadParallelism: 1,
		MaxManifestFiles:        100,
		MaxDataFiles:            100,
	}
}

type plannerMetadataFacade struct {
	manifests []api.ManifestFile
	entries   map[string][]api.ManifestEntry
	native    NativeFacade
}

func (f *plannerMetadataFacade) AdapterName() string {
	return "planner-test"
}

func (f *plannerMetadataFacade) ParseTableMetadata(ctx context.Context, data []byte, metadataLocation string) (*api.TableMetadata, error) {
	return f.native.ParseTableMetadata(ctx, data, metadataLocation)
}

func (f *plannerMetadataFacade) ReadManifestList(ctx context.Context, data []byte) ([]api.ManifestFile, error) {
	return append([]api.ManifestFile(nil), f.manifests...), nil
}

func (f *plannerMetadataFacade) ReadManifest(ctx context.Context, data []byte) ([]api.ManifestEntry, error) {
	path := string(data)
	return append([]api.ManifestEntry(nil), f.entries[path]...), nil
}

func (f *plannerMetadataFacade) ResolveSnapshot(ctx context.Context, meta *api.TableMetadata, selector api.SnapshotSelector) (api.Snapshot, error) {
	return f.native.ResolveSnapshot(ctx, meta, selector)
}

func (f *plannerMetadataFacade) DetectUnsupportedP0(ctx context.Context, meta *api.TableMetadata, manifests []api.ManifestFile, files []api.DataFile) ([]api.UnsupportedFeature, error) {
	return f.native.DetectUnsupportedP0(ctx, meta, manifests, files)
}

type plannerObjectReader struct {
	mu        sync.Mutex
	data      map[string][]byte
	calls     int
	active    int
	maxActive int
}

func (r *plannerObjectReader) Read(ctx context.Context, location string, offset, length int64) ([]byte, error) {
	r.mu.Lock()
	r.calls++
	r.active++
	if r.active > r.maxActive {
		r.maxActive = r.active
	}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.active--
		r.mu.Unlock()
	}()
	if strings.Contains(location, "/metadata/m") {
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-ctx.Done():
		return nil, api.WrapError(api.ErrObjectIO, "fake object read cancelled", nil, context.Cause(ctx))
	default:
	}
	data := r.data[location]
	if data == nil {
		return nil, api.NewError(api.ErrObjectIO, "missing fake object", map[string]string{"location": api.RedactPath(location)})
	}
	if offset > 0 {
		data = data[offset:]
	}
	if length >= 0 && int(length) < len(data) {
		data = data[:length]
	}
	return append([]byte(nil), data...), nil
}

func (r *plannerObjectReader) totalCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func assertIcebergCode(t *testing.T, err error, code api.ErrorCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s, got nil", code)
	}
	var icebergErr *api.IcebergError
	if !stderrors.As(err, &icebergErr) {
		t.Fatalf("expected IcebergError %s, got %T %v", code, err, err)
	}
	if icebergErr.Code != code {
		t.Fatalf("expected %s, got %s (%v)", code, icebergErr.Code, err)
	}
}
