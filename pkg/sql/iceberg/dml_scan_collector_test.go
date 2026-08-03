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

package iceberg

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func TestDMLMatchedScanCollectorAggregatesBatchesByDataFile(t *testing.T) {
	first, cleanupFirst := newMatchedScanBatch(t)
	defer cleanupFirst()
	second, cleanupSecond := newMatchedScanBatch(t)
	defer cleanupSecond()

	collector := NewDMLMatchedScanCollector(DMLMatchedScanCollectorSpec{
		DataFiles: []api.DataFile{
			{FilePath: "s3://warehouse/gold/orders/data/a.parquet", Partition: map[string]any{"region": "ksa"}, SpecID: 3},
			{FilePath: "s3://warehouse/gold/orders/data/b.parquet", Partition: map[string]any{"region": "ksa"}, SpecID: 3},
		},
		DataFilePathColumnIndex: 2,
		RowOrdinalColumnIndex:   3,
		EqualityFieldIDs:        []int{1, 2},
		EqualityColumnIndexes:   []int32{0, 1},
		PredicateStable:         true,
		IncludePositionRows:     true,
	})
	if err := collector.AddBatch(context.Background(), first); err != nil {
		t.Fatalf("add first batch: %v", err)
	}
	if err := collector.AddBatch(context.Background(), second); err != nil {
		t.Fatalf("add second batch: %v", err)
	}

	targets := collector.Targets()
	if len(targets) != 2 {
		t.Fatalf("expected two merged targets, got %+v", targets)
	}
	if targets[0].DataFile.FilePath != "s3://warehouse/gold/orders/data/a.parquet" ||
		len(targets[0].EqualityRows) != 4 ||
		len(targets[0].PositionRows) != 4 ||
		targets[0].PositionRows[3].Pos != 11 ||
		targets[0].EqualityRows[3].Values[2] != "bob" {
		t.Fatalf("unexpected merged first target: %+v", targets[0])
	}
	if targets[1].DataFile.FilePath != "s3://warehouse/gold/orders/data/b.parquet" ||
		len(targets[1].EqualityRows) != 2 ||
		len(targets[1].PositionRows) != 2 {
		t.Fatalf("unexpected merged second target: %+v", targets[1])
	}

	targets[0].EqualityRows = nil
	targetsAgain := collector.Targets()
	if len(targetsAgain[0].EqualityRows) != 4 {
		t.Fatalf("collector returned mutable target slices")
	}
}

func TestNewDMLMatchedScanCollectorForPlanDeduplicatesDataFiles(t *testing.T) {
	bat, cleanup := newMatchedScanBatch(t)
	defer cleanup()
	plan := &api.IcebergScanPlan{DataTasks: []api.DataFileTask{
		{DataFile: api.DataFile{FilePath: "s3://warehouse/gold/orders/data/a.parquet", SpecID: 3}},
		{DataFile: api.DataFile{FilePath: "s3://warehouse/gold/orders/data/a.parquet", SpecID: 3}},
		{DataFile: api.DataFile{FilePath: "s3://warehouse/gold/orders/data/b.parquet", SpecID: 3}},
	}}
	collector := NewDMLMatchedScanCollectorForPlan(plan, DMLMatchedScanCollectorSpec{
		DataFilePathColumnIndex: 2,
		RowOrdinalColumnIndex:   3,
		IncludePositionRows:     true,
	})
	if err := collector.AddBatch(context.Background(), bat); err != nil {
		t.Fatalf("add batch: %v", err)
	}
	targets := collector.Targets()
	if len(targets) != 2 {
		t.Fatalf("expected two targets from deduped plan files, got %+v", targets)
	}
}
