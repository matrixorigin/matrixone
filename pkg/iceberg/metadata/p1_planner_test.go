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
	tasks, err := pairDeleteTasks(dataTasks, deleteEntries, nil, "cred")
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

func TestPairDeleteTasksTreatsZeroSpecAndSequenceAsValid(t *testing.T) {
	dataTasks := []api.DataFileTask{
		{DataFile: api.DataFile{FilePath: "s3://warehouse/t/data/spec-zero.parquet", FileFormat: "parquet", SpecID: 0, SequenceNumber: 0}},
		{DataFile: api.DataFile{FilePath: "s3://warehouse/t/data/spec-one.parquet", FileFormat: "parquet", SpecID: 1, SequenceNumber: 0}},
	}
	deleteEntries := []deleteManifestEntry{
		{manifestPath: "pos", file: api.DataFile{Content: api.DataFileContentPositionDelete, FilePath: "pos.parquet", FileFormat: "parquet", SpecID: 0, SequenceNumber: 0}},
		{manifestPath: "eq-zero", file: api.DataFile{Content: api.DataFileContentEqualityDelete, FilePath: "eq-zero.parquet", FileFormat: "parquet", EqualityIDs: []int{1}, SpecID: 0, SequenceNumber: 0}},
		{manifestPath: "eq-one", file: api.DataFile{Content: api.DataFileContentEqualityDelete, FilePath: "eq-one.parquet", FileFormat: "parquet", EqualityIDs: []int{1}, SpecID: 0, SequenceNumber: 1}},
	}
	tasks, err := pairDeleteTasks(dataTasks, deleteEntries, map[int]int{0: 0, 1: 1}, "")
	if err != nil {
		t.Fatalf("pair delete tasks: %v", err)
	}
	if len(tasks) != 4 {
		t.Fatalf("expected unpartitioned position and equality deletes for both specs, got %+v", tasks)
	}
	var positionCount, equalityCount int
	for _, task := range tasks {
		switch task.DataFile.FilePath {
		case "pos.parquet":
			positionCount++
		case "eq-zero.parquet":
			t.Fatalf("equality delete at the same sequence must not apply: %+v", tasks)
		case "eq-one.parquet":
			equalityCount++
		}
	}
	if positionCount != 2 || equalityCount != 2 {
		t.Fatalf("expected global position=2 and equality=2, got %+v", tasks)
	}
}

func TestPairDeleteTasksRejectsUnknownDeleteSpec(t *testing.T) {
	for _, file := range []api.DataFile{
		{Content: api.DataFileContentEqualityDelete, FilePath: "eq-delete.parquet", SpecID: 7, SequenceNumber: 2, EqualityIDs: []int{1}},
		{Content: api.DataFileContentPositionDelete, FilePath: "pos-delete.parquet", SpecID: 7, SequenceNumber: 2},
	} {
		_, err := pairDeleteTasks(
			[]api.DataFileTask{{DataFile: api.DataFile{FilePath: "data.parquet", SpecID: 1, SequenceNumber: 1}}},
			[]deleteManifestEntry{{file: file}},
			map[int]int{1: 1},
			"",
		)
		assertIcebergCode(t, err, api.ErrMetadataInvalid)
	}
}

func TestPairDeleteTasksStopsBeforeDeleteTaskFanoutExceedsLimit(t *testing.T) {
	dataTasks := []api.DataFileTask{
		{DataFile: api.DataFile{FilePath: "data-a.parquet", SpecID: 1, SequenceNumber: 1}},
		{DataFile: api.DataFile{FilePath: "data-b.parquet", SpecID: 1, SequenceNumber: 1}},
	}
	deleteEntries := []deleteManifestEntry{{
		manifestPath: "delete-manifest.avro",
		file: api.DataFile{
			Content:        api.DataFileContentEqualityDelete,
			FilePath:       "delete.parquet",
			SpecID:         1,
			SequenceNumber: 2,
			EqualityIDs:    []int{1},
		},
	}}

	_, err := pairDeleteTasksBounded(context.Background(), dataTasks, deleteEntries, nil, "", 1, api.ServerPlanningAuto)
	assertIcebergCode(t, err, api.ErrPlanningLimitExceeded)
}

func TestPairDeleteTasksChargesDerivedPlanningMemoryBeforeFanout(t *testing.T) {
	dataTasks := []api.DataFileTask{{DataFile: api.DataFile{FilePath: "data.parquet", SpecID: 0, SequenceNumber: 1}}}
	deleteEntries := []deleteManifestEntry{{
		manifestPath: "delete-manifest.avro",
		file: api.DataFile{
			Content:        api.DataFileContentEqualityDelete,
			FilePath:       "delete.parquet",
			SpecID:         0,
			SequenceNumber: 2,
			EqualityIDs:    []int{1},
		},
	}}
	memoryUsed := int64(0)
	_, err := pairDeleteTasksBoundedMemory(
		context.Background(),
		dataTasks,
		deleteEntries,
		map[int]int{0: 0},
		"",
		10,
		api.ServerPlanningAuto,
		&memoryUsed,
		1,
	)
	assertIcebergCode(t, err, api.ErrPlanningLimitExceeded)
}

func TestValidateP1DeleteFileRejectsInvalidDeleteMetadata(t *testing.T) {
	valid := api.DataFile{
		Content:          api.DataFileContentEqualityDelete,
		FilePath:         "s3://warehouse/t/delete/eq.parquet",
		FilePathRedacted: "<redacted:path>",
		FileFormat:       "parquet",
		RecordCount:      1,
		FileSizeInBytes:  10,
		EqualityIDs:      []int{1},
	}
	for name, mutate := range map[string]func(*api.DataFile){
		"negative records": func(file *api.DataFile) { file.RecordCount = -1 },
		"negative size":    func(file *api.DataFile) { file.FileSizeInBytes = -1 },
		"missing row size": func(file *api.DataFile) { file.FileSizeInBytes = 0 },
		"bad format":       func(file *api.DataFile) { file.FileFormat = "orc" },
		"missing equality": func(file *api.DataFile) { file.EqualityIDs = nil },
		"bad content":      func(file *api.DataFile) { file.Content = api.DataFileContent(99) },
		"encrypted key":    func(file *api.DataFile) { file.KeyMetadata = []byte("key") },
		"delete vector":    func(file *api.DataFile) { file.DeletionVectorPath = "s3://warehouse/t/delete/dv.bin" },
	} {
		t.Run(name, func(t *testing.T) {
			file := valid
			mutate(&file)
			if err := ValidateP1DeleteFile(file); err == nil {
				t.Fatalf("expected invalid delete metadata error")
			}
		})
	}
	position := valid
	position.Content = api.DataFileContentPositionDelete
	position.EqualityIDs = nil
	if err := ValidateP1DeleteFile(position); err != nil {
		t.Fatalf("position delete with optional referenced file should be valid: %v", err)
	}
}

func TestDeleteTaskIdentityDoesNotCollideOnDelimiters(t *testing.T) {
	if deleteTaskIdentity("a\x00b", "c", "d") == deleteTaskIdentity("a", "b\x00c", "d") {
		t.Fatal("delete task identities collided on path delimiters")
	}
}

func TestDeletePlannerHiddenMappingsAndPartitionTokens(t *testing.T) {
	mappings := []api.IcebergColumnMapping{
		{FieldID: 1, ColumnName: "id", Projected: true},
		{FieldID: 2, ColumnName: "region"},
	}
	schema := api.Schema{Fields: []api.SchemaField{
		{ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeLong}},
		{ID: 2, Name: "region", Type: api.IcebergType{Kind: api.TypeString}},
		{ID: 3, Name: "amount", Type: api.IcebergType{Kind: api.TypeInt}},
	}}
	out, err := addHiddenDeleteColumnMappings(mappings, schema, []api.DeleteFileTask{{
		DataFile: api.DataFile{Content: api.DataFileContentEqualityDelete, EqualityIDs: []int{2, 3}},
	}})
	if err != nil {
		t.Fatalf("add hidden delete columns: %v", err)
	}
	if len(out) != 3 || !out[1].Projected || !out[1].Hidden || out[2].FieldID != 3 || !out[2].Hidden {
		t.Fatalf("unexpected hidden mappings: %+v", out)
	}
	_, err = addHiddenDeleteColumnMappings(mappings, schema, []api.DeleteFileTask{{
		DataFile: api.DataFile{Content: api.DataFileContentEqualityDelete, EqualityIDs: []int{99}},
	}})
	if err == nil {
		t.Fatalf("expected unknown equality field id error")
	}
	unchanged, err := addHiddenDeleteColumnMappings(mappings, schema, nil)
	if err != nil || len(unchanged) != len(mappings) {
		t.Fatalf("unexpected no-op hidden mapping result: %+v err=%v", unchanged, err)
	}

	if !samePartitionScope(map[string]any{"day": int32(10), "bucket": uint8(2)}, map[string]any{"day": int64(10), "bucket": int16(2)}) {
		t.Fatalf("integer partition tokens should match across widths")
	}
	if samePartitionScope(map[string]any{"day": int32(10)}, map[string]any{"day": int32(11)}) {
		t.Fatalf("different partition values must not match")
	}
	if samePartitionScope(map[string]any{"day": int32(10)}, map[string]any{}) {
		t.Fatalf("missing partition key must not match")
	}
	for _, value := range []any{nil, true, int8(1), uint64(1) << 63, float32(1.5), float64(2.5), "ksa", []byte("bytes"), struct{ A int }{A: 7}} {
		if partitionValueToken(value) == "" {
			t.Fatalf("empty partition token for %#v", value)
		}
	}
	if firstNonZeroInt(0, 0, 7) != 7 || firstNonZeroInt64(0, 9) != 9 {
		t.Fatalf("first non-zero helpers returned unexpected values")
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
