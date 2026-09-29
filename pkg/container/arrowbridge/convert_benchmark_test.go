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

package arrowbridge

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type arrowBridgeBenchmarkCase struct {
	name         string
	record       arrow.RecordBatch
	targets      []TargetColumn
	logicalBytes int64
}

func BenchmarkArrowBridgeMaterializeAB(b *testing.B) {
	const rows = 4096
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	cases := []arrowBridgeBenchmarkCase{
		makeNumericDecimalBenchmarkCase(b, alloc, rows),
		makeTimestampShortStringBenchmarkCase(b, alloc, rows),
		makeLongBinaryBenchmarkCase(b, alloc, rows),
	}
	defer func() {
		for _, test := range cases {
			test.record.Release()
		}
		alloc.AssertSize(b, 0)
	}()

	for _, test := range cases {
		plan, err := BindLoad(context.Background(), test.record.Schema(), test.targets, MatchByName)
		if err != nil {
			b.Fatal(err)
		}
		for _, forceMaterialize := range []bool{false, true} {
			policy := "borrow"
			if forceMaterialize {
				policy = "materialize"
			}
			b.Run(test.name+"/"+policy, func(b *testing.B) {
				mp := mpool.MustNewZero()
				var stats ConvertStats
				b.ReportAllocs()
				b.SetBytes(test.logicalBytes)
				b.ResetTimer()
				for index := 0; index < b.N; index++ {
					converted, current, err := plan.Convert(context.Background(), test.record, mp, ConvertOptions{
						ForceMaterialize: forceMaterialize,
					})
					if err != nil {
						b.Fatal(err)
					}
					stats = current
					converted.Clean(mp)
				}
				b.StopTimer()
				if mp.CurrNB() != 0 {
					b.Fatalf("bridge retained %d MPool bytes", mp.CurrNB())
				}
				b.ReportMetric(float64(stats.EligiblePayloadBytes), "eligible_B/op")
				b.ReportMetric(float64(stats.BorrowedPayloadBytes), "borrowed_B/op")
				b.ReportMetric(float64(stats.MaterializedPayloadBytes), "copied_B/op")
				b.ReportMetric(float64(stats.RetainedCapacityBytes), "retained_B/op")
			})
		}
	}
}

func makeNumericDecimalBenchmarkCase(
	b *testing.B,
	alloc memory.Allocator,
	rows int,
) arrowBridgeBenchmarkCase {
	b.Helper()
	ints := array.NewInt64Builder(alloc)
	intValues := make([]int64, rows)
	valid := make([]bool, rows)
	for index := range intValues {
		intValues[index] = int64(index)
		valid[index] = index%17 != 0
	}
	ints.AppendValues(intValues, valid)
	intArray := ints.NewArray()
	ints.Release()

	decimalType := &arrow.Decimal128Type{Precision: 18, Scale: 2}
	decimals := array.NewDecimal128Builder(alloc, decimalType)
	for index := 0; index < rows; index++ {
		decimals.Append(decimal128.FromI64(int64(index * 100)))
	}
	decimalArray := decimals.NewArray()
	decimals.Release()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "d", Type: decimalType},
	}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{intArray, decimalArray}, int64(rows))
	intArray.Release()
	decimalArray.Release()
	return arrowBridgeBenchmarkCase{
		name: "numeric_decimal", record: record,
		targets: []TargetColumn{
			{Name: "i", Type: types.T_int64.ToType()},
			{Name: "d", Type: types.New(types.T_decimal128, 18, 2)},
		},
		logicalBytes: int64(rows * (8 + 16)),
	}
}

func makeTimestampShortStringBenchmarkCase(
	b *testing.B,
	alloc memory.Allocator,
	rows int,
) arrowBridgeBenchmarkCase {
	b.Helper()
	timestampType := &arrow.TimestampType{Unit: arrow.Microsecond}
	timestamps := array.NewTimestampBuilder(alloc, timestampType)
	strings := array.NewStringBuilder(alloc)
	baseMicros := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	var logicalBytes int64
	for index := 0; index < rows; index++ {
		timestamps.Append(arrow.Timestamp(baseMicros + int64(index)*1_000_000))
		value := fmt.Sprintf("event-%04d", index)
		strings.Append(value)
		logicalBytes += 8 + int64(len(value))
	}
	timestampArray := timestamps.NewArray()
	stringArray := strings.NewArray()
	timestamps.Release()
	strings.Release()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: timestampType},
		{Name: "message", Type: arrow.BinaryTypes.String},
	}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{timestampArray, stringArray}, int64(rows))
	timestampArray.Release()
	stringArray.Release()
	return arrowBridgeBenchmarkCase{
		name: "timestamp_short_string", record: record,
		targets: []TargetColumn{
			{Name: "ts", Type: types.New(types.T_timestamp, 0, 6)},
			{Name: "message", Type: types.T_varchar.ToType()},
		},
		logicalBytes: logicalBytes,
	}
}

func makeLongBinaryBenchmarkCase(
	b *testing.B,
	alloc memory.Allocator,
	rows int,
) arrowBridgeBenchmarkCase {
	b.Helper()
	binaries := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
	payload := make([]byte, 256)
	for index := range payload {
		payload[index] = byte(index)
	}
	for index := 0; index < rows; index++ {
		binaries.Append(payload)
	}
	binaryArray := binaries.NewArray()
	binaries.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "payload", Type: arrow.BinaryTypes.Binary}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{binaryArray}, int64(rows))
	binaryArray.Release()
	return arrowBridgeBenchmarkCase{
		name: "long_binary", record: record,
		targets:      []TargetColumn{{Name: "payload", Type: types.T_blob.ToType()}},
		logicalBytes: int64(rows * len(payload)),
	}
}
