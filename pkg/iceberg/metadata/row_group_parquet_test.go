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
	"encoding/binary"
	"math"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func TestParquetValuePruneValueCoversSupportedBounds(t *testing.T) {
	tests := []struct {
		name string
		typ  api.IcebergTypeKind
		val  parquet.Value
		want pruneValue
		ok   bool
	}{
		{
			name: "int32 to iceberg int",
			typ:  api.TypeInt,
			val:  parquet.Int32Value(12),
			want: pruneValue{kind: pruneValueInt64, i64: 12},
			ok:   true,
		},
		{
			name: "int64 within int range",
			typ:  api.TypeDate,
			val:  parquet.Int64Value(math.MaxInt32),
			want: pruneValue{kind: pruneValueInt64, i64: math.MaxInt32},
			ok:   true,
		},
		{
			name: "int64 outside int range",
			typ:  api.TypeInt,
			val:  parquet.Int64Value(math.MaxInt32 + 1),
			ok:   false,
		},
		{
			name: "int32 to iceberg long",
			typ:  api.TypeLong,
			val:  parquet.Int32Value(34),
			want: pruneValue{kind: pruneValueInt64, i64: 34},
			ok:   true,
		},
		{
			name: "int64 to iceberg timestamp",
			typ:  api.TypeTimestamp,
			val:  parquet.Int64Value(35),
			want: pruneValue{kind: pruneValueInt64, i64: 35},
			ok:   true,
		},
		{
			name: "wrong physical type for long",
			typ:  api.TypeLong,
			val:  parquet.BooleanValue(true),
			ok:   false,
		},
		{
			name: "double to iceberg float",
			typ:  api.TypeFloat,
			val:  parquet.DoubleValue(1.75),
			want: pruneValue{kind: pruneValueFloat64, f64: 1.75},
			ok:   true,
		},
		{
			name: "wrong physical type for float",
			typ:  api.TypeFloat,
			val:  parquet.Int32Value(1),
			ok:   false,
		},
		{
			name: "float to iceberg double",
			typ:  api.TypeDouble,
			val:  parquet.FloatValue(1.25),
			want: pruneValue{kind: pruneValueFloat64, f64: 1.25},
			ok:   true,
		},
		{
			name: "double to iceberg double",
			typ:  api.TypeDouble,
			val:  parquet.DoubleValue(2.25),
			want: pruneValue{kind: pruneValueFloat64, f64: 2.25},
			ok:   true,
		},
		{
			name: "nan double is not prunable",
			typ:  api.TypeDouble,
			val:  parquet.DoubleValue(math.NaN()),
			ok:   false,
		},
		{
			name: "wrong physical type for double",
			typ:  api.TypeDouble,
			val:  parquet.Int64Value(1),
			ok:   false,
		},
		{
			name: "nan is not prunable",
			typ:  api.TypeFloat,
			val:  parquet.FloatValue(float32(math.NaN())),
			ok:   false,
		},
		{
			name: "byte array string",
			typ:  api.TypeString,
			val:  parquet.ByteArrayValue([]byte("ksa")),
			want: pruneValue{kind: pruneValueString, str: "ksa"},
			ok:   true,
		},
		{
			name: "fixed byte array string",
			typ:  api.TypeString,
			val:  parquet.FixedLenByteArrayValue([]byte("uae")),
			want: pruneValue{kind: pruneValueString, str: "uae"},
			ok:   true,
		},
		{
			name: "unsupported bool",
			typ:  api.TypeBoolean,
			val:  parquet.BooleanValue(true),
			ok:   false,
		},
		{
			name: "wrong physical type for string",
			typ:  api.TypeString,
			val:  parquet.Int32Value(1),
			ok:   false,
		},
		{
			name: "null value",
			typ:  api.TypeLong,
			val:  parquet.Value{},
			ok:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parquetValuePruneValue(api.IcebergType{Kind: tt.typ}, tt.val)
			if ok != tt.ok {
				t.Fatalf("ok=%v want %v", ok, tt.ok)
			}
			if ok && got != tt.want {
				t.Fatalf("got %+v want %+v", got, tt.want)
			}
		})
	}
}

func TestEncodePruneBoundCoversIcebergPrimitiveEncodings(t *testing.T) {
	intBound, ok := encodePruneBound(api.IcebergType{Kind: api.TypeInt}, pruneValue{kind: pruneValueInt64, i64: -7})
	if !ok || int32(binary.LittleEndian.Uint32(intBound)) != -7 {
		t.Fatalf("unexpected int bound: ok=%v bytes=%v", ok, intBound)
	}
	longBound, ok := encodePruneBound(api.IcebergType{Kind: api.TypeLong}, pruneValue{kind: pruneValueInt64, i64: -9})
	if !ok || int64(binary.LittleEndian.Uint64(longBound)) != -9 {
		t.Fatalf("unexpected long bound: ok=%v bytes=%v", ok, longBound)
	}
	floatBound, ok := encodePruneBound(api.IcebergType{Kind: api.TypeFloat}, pruneValue{kind: pruneValueFloat64, f64: 1.5})
	if !ok || math.Float32frombits(binary.LittleEndian.Uint32(floatBound)) != float32(1.5) {
		t.Fatalf("unexpected float bound: ok=%v bytes=%v", ok, floatBound)
	}
	doubleBound, ok := encodePruneBound(api.IcebergType{Kind: api.TypeDouble}, pruneValue{kind: pruneValueFloat64, f64: 2.5})
	if !ok || math.Float64frombits(binary.LittleEndian.Uint64(doubleBound)) != 2.5 {
		t.Fatalf("unexpected double bound: ok=%v bytes=%v", ok, doubleBound)
	}
	stringBound, ok := encodePruneBound(api.IcebergType{Kind: api.TypeString}, pruneValue{kind: pruneValueString, str: "ksa"})
	if !ok || string(stringBound) != "ksa" {
		t.Fatalf("unexpected string bound: ok=%v bytes=%v", ok, stringBound)
	}
	if _, ok := encodePruneBound(api.IcebergType{Kind: api.TypeInt}, pruneValue{kind: pruneValueInt64, i64: math.MaxInt32 + 1}); ok {
		t.Fatalf("out-of-range int bound should not encode")
	}
	if _, ok := encodePruneBound(api.IcebergType{Kind: api.TypeFloat}, pruneValue{kind: pruneValueFloat64, f64: math.NaN()}); ok {
		t.Fatalf("NaN float bound should not encode")
	}
	if _, ok := encodePruneBound(api.IcebergType{Kind: api.TypeBoolean}, pruneValue{kind: pruneValueInt64, i64: 1}); ok {
		t.Fatalf("unsupported boolean bound should not encode")
	}
	for _, test := range []struct {
		typ   api.IcebergTypeKind
		value pruneValue
	}{
		{api.TypeLong, pruneValue{kind: pruneValueString, str: "wrong"}},
		{api.TypeFloat, pruneValue{kind: pruneValueInt64, i64: 1}},
		{api.TypeDouble, pruneValue{kind: pruneValueInt64, i64: 1}},
		{api.TypeString, pruneValue{kind: pruneValueInt64, i64: 1}},
	} {
		if _, ok := encodePruneBound(api.IcebergType{Kind: test.typ}, test.value); ok {
			t.Fatalf("wrong prune value kind should not encode for %s", test.typ)
		}
	}
}

func TestRowGroupMemoryHelpers(t *testing.T) {
	if err := reserveRowGroupTaskMemory(nil, 1, api.ServerPlanningOff, api.RowGroupSplit{}); err != nil {
		t.Fatalf("nil accounting must be accepted: %v", err)
	}
	split := api.RowGroupSplit{
		LowerBounds: map[int][]byte{1: {1}}, UpperBounds: map[int][]byte{1: {2}},
		NullValueCounts: map[int]int64{1: 0}, ValueCounts: map[int]int64{1: 1},
	}
	if got := rowGroupSplitMemory(split); got <= 160 {
		t.Fatalf("expected nested split memory, got %d", got)
	}
	fields := schemaFieldsByID(api.Schema{Fields: []api.SchemaField{{ID: 1, Name: "id"}}})
	if fields[1].Name != "id" {
		t.Fatalf("unexpected schema fields: %+v", fields)
	}
	if ids := parquetLeafColumnFieldIDs(nil, fields); ids != nil {
		t.Fatalf("nil parquet root must return nil ids: %v", ids)
	}
}

func TestEstimateIcebergParquetRowGroupBytes(t *testing.T) {
	tests := []struct {
		name          string
		fileSize      int64
		totalRows     int64
		rowGroupRows  int64
		rowGroupCount int
		want          int64
	}{
		{name: "empty file fallback", want: 1},
		{name: "proportional by rows", fileSize: 1000, totalRows: 100, rowGroupRows: 25, rowGroupCount: 4, want: 250},
		{name: "minimum proportional byte", fileSize: 1, totalRows: 100, rowGroupRows: 1, rowGroupCount: 4, want: 1},
		{name: "fallback by group count", fileSize: 999, rowGroupCount: 3, want: 333},
		{name: "minimum group byte", fileSize: 2, rowGroupCount: 4, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := estimateIcebergParquetRowGroupBytes(tt.fileSize, tt.totalRows, tt.rowGroupRows, tt.rowGroupCount)
			if got != tt.want {
				t.Fatalf("got %d want %d", got, tt.want)
			}
		})
	}
}
