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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func TestBuildDMLMatchedDeleteTargetFromBatchCollectsPositionAndEqualityRows(t *testing.T) {
	bat, cleanup := newMatchedDeleteBatch(t)
	defer cleanup()

	target, err := BuildDMLMatchedDeleteTargetFromBatch(context.Background(), DMLMatchedRowsBatchRequest{
		DataFile: api.DataFile{
			FilePath: "s3://warehouse/gold/orders/data/part-1.parquet",
			Partition: map[string]any{
				"region": "ksa",
			},
			SpecID: 3,
		},
		Batch:                 bat,
		EqualityFieldIDs:      []int{1, 2},
		EqualityColumnIndexes: []int32{0, 1},
		PredicateStable:       true,
		IncludePositionRows:   true,
		StartRowOrdinal:       40,
	})
	if err != nil {
		t.Fatalf("build matched delete target: %v", err)
	}
	if !target.PredicateStable || !target.HasRowOrdinal {
		t.Fatalf("expected predicate-stable equality and row ordinals: %+v", target)
	}
	if len(target.EqualityRows) != 2 || len(target.PositionRows) != 2 {
		t.Fatalf("expected two equality and position rows: %+v", target)
	}
	if got := target.EqualityRows[0].Values[1]; got != int32(7) {
		t.Fatalf("unexpected first id equality value: %#v", got)
	}
	if got := target.EqualityRows[1].Values[2]; got != "bob" {
		t.Fatalf("unexpected second name equality value: %#v", got)
	}
	if target.PositionRows[0].FilePath != target.DataFile.FilePath || target.PositionRows[0].Pos != 40 || target.PositionRows[1].Pos != 41 {
		t.Fatalf("unexpected position rows: %+v", target.PositionRows)
	}
}

func TestBuildDMLMatchedDeleteTargetFromBatchEmptyBatchKeepsDataFileOnly(t *testing.T) {
	target, err := BuildDMLMatchedDeleteTargetFromBatch(context.Background(), DMLMatchedRowsBatchRequest{
		DataFile: api.DataFile{FilePath: "s3://warehouse/gold/orders/data/empty.parquet"},
		Batch:    batch.New([]string{"id"}),
	})
	if err != nil {
		t.Fatalf("empty matched batch: %v", err)
	}
	if target.DataFile.FilePath == "" || len(target.EqualityRows) != 0 || len(target.PositionRows) != 0 {
		t.Fatalf("unexpected empty target: %+v", target)
	}
}

func TestBuildDMLMatchedDeleteTargetFromBatchRejectsInvalidColumnIndex(t *testing.T) {
	bat, cleanup := newMatchedDeleteBatch(t)
	defer cleanup()

	_, err := BuildDMLMatchedDeleteTargetFromBatch(context.Background(), DMLMatchedRowsBatchRequest{
		DataFile:              api.DataFile{FilePath: "s3://warehouse/gold/orders/data/part-1.parquet"},
		Batch:                 bat,
		EqualityFieldIDs:      []int{1},
		EqualityColumnIndexes: []int32{9},
	})
	if err == nil || !strings.Contains(err.Error(), "column index out of range") {
		t.Fatalf("expected column index error, got %v", err)
	}
}

func TestBuildDMLMatchedDeleteTargetsFromScanBatchGroupsByDataFile(t *testing.T) {
	bat, cleanup := newMatchedScanBatch(t)
	defer cleanup()

	targets, err := BuildDMLMatchedDeleteTargetsFromScanBatch(context.Background(), DMLMatchedScanBatchRequest{
		Batch: bat,
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
	if err != nil {
		t.Fatalf("build scan batch targets: %v", err)
	}
	if len(targets) != 2 {
		t.Fatalf("expected two grouped targets, got %+v", targets)
	}
	if targets[0].DataFile.FilePath != "s3://warehouse/gold/orders/data/a.parquet" ||
		len(targets[0].EqualityRows) != 2 ||
		len(targets[0].PositionRows) != 2 ||
		targets[0].PositionRows[1].Pos != 11 ||
		targets[0].EqualityRows[1].Values[2] != "bob" ||
		!targets[0].PredicateStable ||
		!targets[0].HasRowOrdinal {
		t.Fatalf("unexpected first grouped target: %+v", targets[0])
	}
	if targets[1].DataFile.FilePath != "s3://warehouse/gold/orders/data/b.parquet" ||
		len(targets[1].EqualityRows) != 1 ||
		len(targets[1].PositionRows) != 1 ||
		targets[1].PositionRows[0].Pos != 40 ||
		targets[1].EqualityRows[0].Values[1] != int32(9) {
		t.Fatalf("unexpected second grouped target: %+v", targets[1])
	}
}

func TestBuildDMLMatchedDeleteTargetsFromScanBatchRejectsUnknownDataFile(t *testing.T) {
	bat, cleanup := newMatchedScanBatch(t)
	defer cleanup()

	_, err := BuildDMLMatchedDeleteTargetsFromScanBatch(context.Background(), DMLMatchedScanBatchRequest{
		Batch:                   bat,
		DataFiles:               []api.DataFile{{FilePath: "s3://warehouse/gold/orders/data/a.parquet"}},
		DataFilePathColumnIndex: 2,
		RowOrdinalColumnIndex:   3,
		IncludePositionRows:     true,
	})
	if err == nil || !strings.Contains(err.Error(), "unknown data file") {
		t.Fatalf("expected unknown data file error, got %v", err)
	}
}

func TestDMLMatchedBatchColumnHelpersCoverTypesAndErrors(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	bat := batch.New([]string{"id", "name", "extra"})
	bat.Vecs = []*vector.Vector{
		vector.NewVec(types.T_int32.ToType()),
		vector.NewVec(types.T_varchar.ToType()),
		vector.NewVec(types.T_int64.ToType()),
	}
	for _, row := range []struct {
		id    int32
		name  string
		extra int64
	}{{1, "alice", 10}} {
		if err := vector.AppendFixed[int32](bat.Vecs[0], row.id, false, mp); err != nil {
			t.Fatalf("append id: %v", err)
		}
		if err := vector.AppendBytes(bat.Vecs[1], []byte(row.name), false, mp); err != nil {
			t.Fatalf("append name: %v", err)
		}
		if err := vector.AppendFixed[int64](bat.Vecs[2], row.extra, false, mp); err != nil {
			t.Fatalf("append extra: %v", err)
		}
	}
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	indexes, err := dmlReplacementColumnIndexes(ctx, bat, []string{"name", "id"}, []int{2, 1})
	if err != nil || indexes[0] != 1 || indexes[1] != 0 {
		t.Fatalf("name-based replacement indexes failed indexes=%v err=%v", indexes, err)
	}
	fallbackBatch := batch.NewWithSize(2)
	fallbackBatch.Vecs[0] = bat.Vecs[2]
	fallbackBatch.Vecs[1] = bat.Vecs[0]
	fallbackBatch.SetRowCount(1)
	indexes, err = dmlReplacementColumnIndexes(ctx, fallbackBatch, []string{"extra", "id"}, []int{0, 1})
	if err != nil || indexes[0] != 0 || indexes[1] != 1 {
		t.Fatalf("fallback replacement indexes failed indexes=%v err=%v", indexes, err)
	}
	if _, err := dmlReplacementColumnIndexes(ctx, nil, []string{"id"}, []int{0}); err == nil {
		t.Fatalf("expected nil batch error")
	}
	if _, err := dmlReplacementColumnIndexes(ctx, fallbackBatch, []string{"id"}, nil); err == nil {
		t.Fatalf("expected fallback length error")
	}
	if _, err := dmlReplacementColumnIndexes(ctx, fallbackBatch, []string{"id"}, []int{9}); err == nil {
		t.Fatalf("expected fallback index error")
	}
	if got := dmlBatchVectorCount(nil); got != 0 {
		t.Fatalf("nil batch vector count = %d", got)
	}
	if _, err := dmlBatchColumnIndexByNameOrError(ctx, fallbackBatch, "missing", 7, "test"); err == nil {
		t.Fatalf("expected column index by name error")
	}
}

func TestDMLVectorValueCoversSupportedTypes(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	mustValue := func(vec *vector.Vector, want any) {
		t.Helper()
		got, err := dmlVectorValue(ctx, vec, 0)
		if err != nil {
			t.Fatalf("dml vector value: %v", err)
		}
		if got != want {
			t.Fatalf("unexpected value got=%#v want=%#v", got, want)
		}
		vec.Free(mp)
	}
	boolVec := vector.NewVec(types.T_bool.ToType())
	if err := vector.AppendFixed[bool](boolVec, true, false, mp); err != nil {
		t.Fatal(err)
	}
	mustValue(boolVec, true)
	i8 := vector.NewVec(types.T_int8.ToType())
	if err := vector.AppendFixed[int8](i8, 8, false, mp); err != nil {
		t.Fatal(err)
	}
	mustValue(i8, int8(8))
	i16 := vector.NewVec(types.T_int16.ToType())
	if err := vector.AppendFixed[int16](i16, 16, false, mp); err != nil {
		t.Fatal(err)
	}
	mustValue(i16, int16(16))
	i64 := vector.NewVec(types.T_int64.ToType())
	if err := vector.AppendFixed[int64](i64, 64, false, mp); err != nil {
		t.Fatal(err)
	}
	mustValue(i64, int64(64))
	u8 := vector.NewVec(types.T_uint8.ToType())
	if err := vector.AppendFixed[uint8](u8, 8, false, mp); err != nil {
		t.Fatal(err)
	}
	mustValue(u8, uint8(8))
	u16 := vector.NewVec(types.T_uint16.ToType())
	if err := vector.AppendFixed[uint16](u16, 16, false, mp); err != nil {
		t.Fatal(err)
	}
	mustValue(u16, uint16(16))
	u32 := vector.NewVec(types.T_uint32.ToType())
	if err := vector.AppendFixed[uint32](u32, 32, false, mp); err != nil {
		t.Fatal(err)
	}
	mustValue(u32, uint32(32))
	u64 := vector.NewVec(types.T_uint64.ToType())
	if err := vector.AppendFixed[uint64](u64, 64, false, mp); err != nil {
		t.Fatal(err)
	}
	mustValue(u64, uint64(64))
	f32 := vector.NewVec(types.T_float32.ToType())
	if err := vector.AppendFixed[float32](f32, 1.5, false, mp); err != nil {
		t.Fatal(err)
	}
	mustValue(f32, float32(1.5))
	f64 := vector.NewVec(types.T_float64.ToType())
	if err := vector.AppendFixed[float64](f64, 2.5, false, mp); err != nil {
		t.Fatal(err)
	}
	mustValue(f64, float64(2.5))
	dateVec := vector.NewVec(types.T_date.ToType())
	if err := vector.AppendFixed[types.Date](dateVec, types.Date(20454), false, mp); err != nil {
		t.Fatal(err)
	}
	mustValue(dateVec, int32(20454))
	dtVec := vector.NewVec(types.T_datetime.ToType())
	dt := types.DatetimeFromUnix(time.UTC, 1767225600)
	if err := vector.AppendFixed[types.Datetime](dtVec, dt, false, mp); err != nil {
		t.Fatal(err)
	}
	mustValue(dtVec, int64(dt))
	tsVec := vector.NewVec(types.T_timestamp.ToType())
	ts := types.UnixMicroToTimestamp(1767225600000000)
	if err := vector.AppendFixed[types.Timestamp](tsVec, ts, false, mp); err != nil {
		t.Fatal(err)
	}
	mustValue(tsVec, int64(ts))
	textVec := vector.NewVec(types.T_text.ToType())
	if err := vector.AppendBytes(textVec, []byte("ksa"), false, mp); err != nil {
		t.Fatal(err)
	}
	mustValue(textVec, "ksa")

	nullVec := vector.NewVec(types.T_int32.ToType())
	defer nullVec.Free(mp)
	if err := vector.AppendFixed[int32](nullVec, 0, true, mp); err != nil {
		t.Fatal(err)
	}
	got, err := dmlVectorValue(ctx, nullVec, 0)
	if err != nil || got != nil {
		t.Fatalf("null value got=%#v err=%v", got, err)
	}
	if _, err := dmlVectorValue(ctx, vector.NewVec(types.T_binary.ToType()), 0); err == nil {
		t.Fatalf("expected unsupported equality value type")
	}
	if _, err := dmlStringValue(ctx, vector.NewVec(types.T_bool.ToType()), 0); err == nil {
		t.Fatalf("expected unsupported path type")
	}
	if _, err := dmlInt64Value(ctx, nullVec, 0); err == nil {
		t.Fatalf("expected null row ordinal error")
	}
}

func newMatchedDeleteBatch(t *testing.T) (*batch.Batch, func()) {
	t.Helper()
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"id", "name"})
	idVec := vector.NewVec(types.T_int32.ToType())
	nameVec := vector.NewVec(types.T_varchar.ToType())
	for _, row := range []struct {
		id   int32
		name string
	}{
		{id: 7, name: "alice"},
		{id: 8, name: "bob"},
	} {
		if err := vector.AppendFixed[int32](idVec, row.id, false, mp); err != nil {
			t.Fatalf("append id: %v", err)
		}
		if err := vector.AppendBytes(nameVec, []byte(row.name), false, mp); err != nil {
			t.Fatalf("append name: %v", err)
		}
	}
	bat.Vecs[0] = idVec
	bat.Vecs[1] = nameVec
	bat.SetRowCount(2)
	return bat, func() { bat.Clean(mp) }
}

func newMatchedScanBatch(t *testing.T) (*batch.Batch, func()) {
	t.Helper()
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"id", "name", "_iceberg_data_file", "_iceberg_row_ordinal"})
	idVec := vector.NewVec(types.T_int32.ToType())
	nameVec := vector.NewVec(types.T_varchar.ToType())
	pathVec := vector.NewVec(types.T_varchar.ToType())
	ordinalVec := vector.NewVec(types.T_int64.ToType())
	for _, row := range []struct {
		id      int32
		name    string
		path    string
		ordinal int64
	}{
		{7, "alice", "s3://warehouse/gold/orders/data/a.parquet", 10},
		{8, "bob", "s3://warehouse/gold/orders/data/a.parquet", 11},
		{9, "cyd", "s3://warehouse/gold/orders/data/b.parquet", 40},
	} {
		if err := vector.AppendFixed[int32](idVec, row.id, false, mp); err != nil {
			t.Fatalf("append id: %v", err)
		}
		if err := vector.AppendBytes(nameVec, []byte(row.name), false, mp); err != nil {
			t.Fatalf("append name: %v", err)
		}
		if err := vector.AppendBytes(pathVec, []byte(row.path), false, mp); err != nil {
			t.Fatalf("append path: %v", err)
		}
		if err := vector.AppendFixed[int64](ordinalVec, row.ordinal, false, mp); err != nil {
			t.Fatalf("append ordinal: %v", err)
		}
	}
	bat.Vecs[0] = idVec
	bat.Vecs[1] = nameVec
	bat.Vecs[2] = pathVec
	bat.Vecs[3] = ordinalVec
	bat.SetRowCount(3)
	return bat, func() { bat.Clean(mp) }
}
