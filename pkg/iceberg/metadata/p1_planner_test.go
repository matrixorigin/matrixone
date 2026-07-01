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
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func TestRowGroupSplitsCarryStartOrdinalAndPruneStats(t *testing.T) {
	meta, err := ParseTableMetadata([]byte(sampleMetadataJSON), "s3://warehouse/t/metadata/v1.json")
	if err != nil {
		t.Fatalf("parse metadata: %v", err)
	}
	schema, ok := meta.CurrentSchema()
	if !ok {
		t.Fatalf("missing schema")
	}
	splits := BuildRowGroupSplits([]RowGroupFooter{
		{Ordinal: 0, RowCount: 10, Bytes: 100, UpperBounds: map[int][]byte{1: icebergLongBound(10)}},
		{Ordinal: 1, RowCount: 15, Bytes: 150, LowerBounds: map[int][]byte{1: icebergLongBound(101)}, UpperBounds: map[int][]byte{1: icebergLongBound(200)}},
	})
	if len(splits) != 2 || splits[0].StartRowOrdinal != 0 || splits[1].StartRowOrdinal != 10 {
		t.Fatalf("unexpected row group starts: %+v", splits)
	}
	selected, pruned := PruneRowGroupSplits(meta, schema, 0, splits, []api.PrunePredicate{{
		FieldID: 1,
		Op:      api.PruneOpGT,
		Literal: api.PruneLiteral{Kind: api.TypeLong, Int64: 100},
	}})
	if pruned != 1 || len(selected) != 1 || selected[0].Ordinal != 1 {
		t.Fatalf("unexpected row group pruning selected=%+v pruned=%d", selected, pruned)
	}
}

func TestPairDeleteTasksPositionAndEquality(t *testing.T) {
	dataTasks := []api.DataFileTask{
		{DataFile: api.DataFile{FilePath: "s3://warehouse/t/data/a.parquet", FileFormat: "parquet", SpecID: 1, SequenceNumber: 10, Partition: map[string]any{"day": int32(1)}}},
		{DataFile: api.DataFile{FilePath: "s3://warehouse/t/data/b.parquet", FileFormat: "parquet", SpecID: 1, SequenceNumber: 10, Partition: map[string]any{"day": int32(2)}}},
	}
	deleteEntries := []deleteManifestEntry{
		{
			manifestPath: "s3://warehouse/t/metadata/delete-pos.avro",
			file: api.DataFile{
				Content:            api.DataFileContentPositionDelete,
				FilePath:           "s3://warehouse/t/delete/pos.parquet",
				FileFormat:         "parquet",
				ReferencedDataFile: "s3://warehouse/t/data/a.parquet",
				SequenceNumber:     10,
			},
		},
		{
			manifestPath: "s3://warehouse/t/metadata/delete-eq.avro",
			file: api.DataFile{
				Content:        api.DataFileContentEqualityDelete,
				FilePath:       "s3://warehouse/t/delete/eq.parquet",
				FileFormat:     "parquet",
				EqualityIDs:    []int{1},
				SpecID:         1,
				Partition:      map[string]any{"day": int64(2)},
				SequenceNumber: 12,
				DeleteSchemaID: 3,
			},
		},
		{
			manifestPath: "s3://warehouse/t/metadata/delete-pos-no-ref.avro",
			file: api.DataFile{
				Content:        api.DataFileContentPositionDelete,
				FilePath:       "s3://warehouse/t/delete/pos-no-ref.parquet",
				FileFormat:     "parquet",
				SpecID:         1,
				Partition:      map[string]any{"day": int64(2)},
				SequenceNumber: 10,
			},
		},
		{
			manifestPath: "s3://warehouse/t/metadata/delete-eq-same-seq.avro",
			file: api.DataFile{
				Content:        api.DataFileContentEqualityDelete,
				FilePath:       "s3://warehouse/t/delete/eq-same-seq.parquet",
				FileFormat:     "parquet",
				EqualityIDs:    []int{1},
				SpecID:         1,
				Partition:      map[string]any{"day": int64(1)},
				SequenceNumber: 10,
			},
		},
	}
	tasks, err := pairDeleteTasks(dataTasks, deleteEntries, "cred")
	if err != nil {
		t.Fatalf("pair delete tasks: %v", err)
	}
	if len(tasks) != 3 {
		t.Fatalf("expected three paired delete tasks, got %+v", tasks)
	}
	byDeletePath := make(map[string]api.DeleteFileTask)
	for _, task := range tasks {
		byDeletePath[task.DataFile.FilePath] = task
	}
	if byDeletePath["s3://warehouse/t/delete/pos.parquet"].AppliesToPath != dataTasks[0].DataFile.FilePath {
		t.Fatalf("position delete paired to wrong file: %+v", byDeletePath["s3://warehouse/t/delete/pos.parquet"])
	}
	noRef := byDeletePath["s3://warehouse/t/delete/pos-no-ref.parquet"]
	if noRef.AppliesToPath != dataTasks[1].DataFile.FilePath {
		t.Fatalf("position delete without referenced data file paired incorrectly: %+v", noRef)
	}
	eq := byDeletePath["s3://warehouse/t/delete/eq.parquet"]
	if eq.AppliesToPath != dataTasks[1].DataFile.FilePath || eq.DeleteSchemaID != 3 || eq.SequenceNumber != 12 {
		t.Fatalf("equality delete paired incorrectly: %+v", eq)
	}
}

func TestLocalScanPlannerMergeOnReadReadsDeleteManifest(t *testing.T) {
	ctx := context.Background()
	fixture := newPlannerFixture(t, 1)
	dataManifest := fixture.facade.manifests[0]
	deleteManifest := api.ManifestFile{
		Path:            "s3://warehouse/sales/orders/metadata/delete-m0.avro",
		PartitionSpecID: dataManifest.PartitionSpecID,
		Content:         api.ManifestContentDeletes,
		SequenceNumber:  11,
	}
	fixture.facade.manifests = append(fixture.facade.manifests, deleteManifest)
	fixture.reader.data[deleteManifest.Path] = []byte(deleteManifest.Path)
	fixture.facade.entries[dataManifest.Path][0].DataFile.SequenceNumber = 10
	fixture.facade.entries[deleteManifest.Path] = []api.ManifestEntry{{
		Status:         api.ManifestEntryAdded,
		SequenceNumber: 11,
		DataFile: api.DataFile{
			Content:            api.DataFileContentPositionDelete,
			FilePath:           "s3://warehouse/sales/orders/delete/pos.parquet",
			FileFormat:         "parquet",
			RecordCount:        1,
			FileSizeInBytes:    10,
			ReferencedDataFile: fixture.facade.entries[dataManifest.Path][0].DataFile.FilePath,
			SpecID:             0,
		},
	}}

	_, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest: cacheLoadTableRequest().CatalogRequest,
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "main",
	})
	if err == nil || !strings.Contains(err.Error(), "delete-manifest") {
		t.Fatalf("append-only scan should reject delete manifest, got %v", err)
	}

	plan, err := fixture.planner().PlanScan(ctx, api.ScanPlanRequest{
		CatalogRequest:    cacheLoadTableRequest().CatalogRequest,
		Namespace:         api.Namespace{"sales"},
		Table:             "orders",
		Ref:               "main",
		EnableDeleteApply: true,
	})
	if err != nil {
		t.Fatalf("merge-on-read plan: %v", err)
	}
	if len(plan.DeleteTasks) != 1 || plan.Profile.DeleteFilesSelected != 1 {
		t.Fatalf("expected one delete task, plan=%+v", plan)
	}
	if plan.DeleteTasks[0].AppliesToPath != fixture.facade.entries[dataManifest.Path][0].DataFile.FilePath {
		t.Fatalf("delete task applies to wrong file: %+v", plan.DeleteTasks[0])
	}
}
