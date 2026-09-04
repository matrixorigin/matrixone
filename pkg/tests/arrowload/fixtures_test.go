// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package arrowload holds release-level, end-to-end BVT coverage for
// `LOAD DATA ... format='arrow'` (issue #23684). Tests run against dedicated,
// non-shared embedded clusters (see cluster_test.go) so the `cn.frontend.arrow-load`
// rollout gates can be flipped on here without affecting any other package's shared
// embedded cluster or the default `etc/launch*` BVT configuration, which both keep
// the feature off by default.
package arrowload

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

const (
	containerFile   = "file"
	containerStream = "stream"
)

// writeArrowFile writes an Arrow IPC payload (File or Stream container) built by
// emit to a real file under dir and returns its absolute path, so it can be
// referenced directly from SQL as {'filepath'='<path>','format'='arrow'}. Callers
// that need several fixture files visible to one glob/pattern LOAD (multi-object
// tests) pass the same dir to each fixture call; single-file tests just pass a
// fresh t.TempDir().
func writeArrowFile(
	t testing.TB,
	dir, filename, container string,
	schema *arrow.Schema,
	emit func(alloc memory.Allocator, write func(arrow.RecordBatch) error),
) string {
	t.Helper()
	alloc := memory.NewGoAllocator()
	var out bytes.Buffer
	switch container {
	case containerFile:
		w, err := ipc.NewFileWriter(&out, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
		require.NoError(t, err)
		emit(alloc, w.Write)
		require.NoError(t, w.Close())
	case containerStream:
		w := ipc.NewWriter(&out, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
		emit(alloc, w.Write)
		require.NoError(t, w.Close())
	default:
		t.Fatalf("unknown arrow container %q", container)
	}
	path := filepath.Join(dir, filename)
	require.NoError(t, os.WriteFile(path, out.Bytes(), 0o600))
	return path
}

// --- representative schema 1: numeric/decimal-heavy -------------------------------

// numericRow is one logical row of the numeric/decimal-heavy fixture. Target table:
// `id BIGINT NOT NULL, amount DECIMAL(18,2), score DOUBLE, flag BOOL`.
type numericRow struct {
	id         int64
	amount     int64 // raw decimal128 mantissa at scale 2; ignored if amountNull
	amountNull bool
	score      float64
	scoreNull  bool
	flag       bool
	flagNull   bool
}

func numericSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 18, Scale: 2}, Nullable: true},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "flag", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	}, nil)
}

// fixtureNumeric writes rows split into batches of at most batchSize rows each, so
// callers can control record-batch fan-out for parallel-shard coverage.
func fixtureNumeric(t *testing.T, dir, filename, container string, rows []numericRow, batchSize int) string {
	t.Helper()
	schema := numericSchema()
	return writeArrowFile(t, dir, filename, container, schema, func(alloc memory.Allocator, write func(arrow.RecordBatch) error) {
		for start := 0; start < len(rows); start += batchSize {
			end := min(start+batchSize, len(rows))
			batch := rows[start:end]
			builder := array.NewRecordBuilder(alloc, schema)
			ids := make([]int64, len(batch))
			amounts := make([]decimal128.Num, len(batch))
			amountValid := make([]bool, len(batch))
			scores := make([]float64, len(batch))
			scoreValid := make([]bool, len(batch))
			flags := make([]bool, len(batch))
			flagValid := make([]bool, len(batch))
			for i, row := range batch {
				ids[i] = row.id
				amounts[i] = decimal128.FromI64(row.amount)
				amountValid[i] = !row.amountNull
				scores[i] = row.score
				scoreValid[i] = !row.scoreNull
				flags[i] = row.flag
				flagValid[i] = !row.flagNull
			}
			builder.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
			builder.Field(1).(*array.Decimal128Builder).AppendValues(amounts, amountValid)
			builder.Field(2).(*array.Float64Builder).AppendValues(scores, scoreValid)
			builder.Field(3).(*array.BooleanBuilder).AppendValues(flags, flagValid)
			record := builder.NewRecordBatch()
			require.NoError(t, write(record))
			record.Release()
			builder.Release()
		}
	})
}

// --- representative schema 2: timestamp + short string + dictionary --------------

// timestampRow is one logical row of the timestamp/string fixture. Target table:
// `id BIGINT NOT NULL, ts DATETIME(6), d DATE, name VARCHAR(50)`.
type timestampRow struct {
	id       int64
	ts       time.Time
	tsNull   bool
	date     time.Time
	dateNull bool
	name     string
	nameNull bool
}

func timestampDictSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
		{Name: "d", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{
			Name:     "name",
			Type:     &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String},
			Nullable: true,
		},
	}, nil)
}

func date32FromTime(ts time.Time) arrow.Date32 {
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	days := int32(ts.UTC().Sub(epoch).Hours() / 24)
	return arrow.Date32(days)
}

// fixtureTimestampDict writes a single record batch (dictionary IPC replay is
// simplest with one base dictionary + one record batch, which is all this BVT
// needs. Dictionary base/delta epoch replay itself is already covered by
// pkg/sql/colexec/external/arrowio's own unit/fuzz suite).
func fixtureTimestampDict(t *testing.T, dir, filename, container string, rows []timestampRow, dictValues []string) string {
	t.Helper()
	schema := timestampDictSchema()
	return writeArrowFile(t, dir, filename, container, schema, func(alloc memory.Allocator, write func(arrow.RecordBatch) error) {
		idBuilder := array.NewInt64Builder(alloc)
		tsBuilder := array.NewTimestampBuilder(alloc, schema.Field(1).Type.(*arrow.TimestampType))
		dateBuilder := array.NewDate32Builder(alloc)
		for _, row := range rows {
			idBuilder.Append(row.id)
			if row.tsNull {
				tsBuilder.AppendNull()
			} else {
				tsBuilder.Append(arrow.Timestamp(row.ts.UTC().UnixMicro()))
			}
			if row.dateNull {
				dateBuilder.AppendNull()
			} else {
				dateBuilder.Append(date32FromTime(row.date))
			}
		}
		idArr := idBuilder.NewArray()
		tsArr := tsBuilder.NewArray()
		dateArr := dateBuilder.NewArray()
		idBuilder.Release()
		tsBuilder.Release()
		dateBuilder.Release()

		dictType := schema.Field(3).Type.(*arrow.DictionaryType)
		indexBuilder := array.NewInt8Builder(alloc)
		for _, row := range rows {
			if row.nameNull {
				indexBuilder.AppendNull()
				continue
			}
			idx := int8(-1)
			for i, v := range dictValues {
				if v == row.name {
					idx = int8(i)
					break
				}
			}
			require.GreaterOrEqualf(t, idx, int8(0), "name %q not present in dictionary value set", row.name)
			indexBuilder.Append(idx)
		}
		indexArr := indexBuilder.NewArray()
		indexBuilder.Release()
		valueBuilder := array.NewStringBuilder(alloc)
		valueBuilder.AppendValues(dictValues, nil)
		valueArr := valueBuilder.NewArray()
		valueBuilder.Release()
		nameArr := array.NewDictionaryArray(dictType, indexArr, valueArr)
		indexArr.Release()
		valueArr.Release()

		record := array.NewRecordBatch(schema, []arrow.Array{idArr, tsArr, dateArr, nameArr}, int64(len(rows)))
		idArr.Release()
		tsArr.Release()
		dateArr.Release()
		nameArr.Release()
		require.NoError(t, write(record))
		record.Release()
	})
}

// --- representative schema 3: long binary payloads --------------------------------

// binaryRow is one logical row of the long-binary fixture. Target table:
// `id BIGINT NOT NULL, payload VARBINARY(200)`.
type binaryRow struct {
	id      int64
	payload []byte
}

func longBinarySchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "payload", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)
}

func fixtureLongBinary(t *testing.T, dir, filename, container string, rows []binaryRow) string {
	t.Helper()
	schema := longBinarySchema()
	return writeArrowFile(t, dir, filename, container, schema, func(alloc memory.Allocator, write func(arrow.RecordBatch) error) {
		builder := array.NewRecordBuilder(alloc, schema)
		ids := make([]int64, len(rows))
		payloads := make([][]byte, len(rows))
		for i, row := range rows {
			ids[i] = row.id
			payloads[i] = row.payload
			require.Greaterf(t, len(row.payload), 23,
				"long-binary fixture row %d must exceed the 23-byte inline threshold", i)
		}
		builder.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
		builder.Field(1).(*array.BinaryBuilder).AppendValues(payloads, nil)
		record := builder.NewRecordBatch()
		require.NoError(t, write(record))
		record.Release()
		builder.Release()
	})
}

// --- generic id/name fixture, used for multi-object schema-mismatch and ---------
// --- constraint-violation coverage -------------------------------------------------

// idNameRow is one logical row of the generic id/name fixture. Target table:
// `id BIGINT NOT NULL, name VARCHAR(50)`. Both fields are nullable in the Arrow
// schema (id is nullable here even though the MO target column is NOT NULL) so a
// null id value exercises MO's NOT NULL constraint check at conversion/insert time
// rather than a schema-fingerprint mismatch.
type idNameRow struct {
	id       int64
	idNull   bool
	name     string
	nameNull bool
}

func idNameSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
}

func fixtureIDName(t *testing.T, dir, filename, container string, batches [][]idNameRow) string {
	t.Helper()
	schema := idNameSchema()
	return writeArrowFile(t, dir, filename, container, schema, func(alloc memory.Allocator, write func(arrow.RecordBatch) error) {
		for _, rows := range batches {
			builder := array.NewRecordBuilder(alloc, schema)
			ids := make([]int64, len(rows))
			idValid := make([]bool, len(rows))
			names := make([]string, len(rows))
			nameValid := make([]bool, len(rows))
			for i, row := range rows {
				ids[i] = row.id
				idValid[i] = !row.idNull
				names[i] = row.name
				nameValid[i] = !row.nameNull
			}
			builder.Field(0).(*array.Int64Builder).AppendValues(ids, idValid)
			builder.Field(1).(*array.StringBuilder).AppendValues(names, nameValid)
			record := builder.NewRecordBatch()
			require.NoError(t, write(record))
			record.Release()
			builder.Release()
		}
	})
}

func fixtureInt64Pair(
	t *testing.T,
	dir, filename, container string,
	first, second []int64,
) string {
	t.Helper()
	require.Len(t, second, len(first))
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "source_first", Type: arrow.PrimitiveTypes.Int64},
		{Name: "source_second", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	return writeArrowFile(t, dir, filename, container, schema, func(
		alloc memory.Allocator,
		write func(arrow.RecordBatch) error,
	) {
		builder := array.NewRecordBuilder(alloc, schema)
		builder.Field(0).(*array.Int64Builder).AppendValues(first, nil)
		builder.Field(1).(*array.Int64Builder).AppendValues(second, nil)
		record := builder.NewRecordBatch()
		require.NoError(t, write(record))
		record.Release()
		builder.Release()
	})
}

// fixtureIDNameMismatchedIDType writes the same logical (id, name) shape but with
// `id` typed as float64 instead of int64, so pairing this file with one produced by
// fixtureIDName in the same multi-object LOAD trips the cross-object schema
// fingerprint check (design invariant I2/I6) rather than any single-file type error.
func fixtureIDNameMismatchedIDType(t *testing.T, dir, filename, container string, ids []float64, names []string) string {
	t.Helper()
	require.Equal(t, len(ids), len(names))
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	return writeArrowFile(t, dir, filename, container, schema, func(alloc memory.Allocator, write func(arrow.RecordBatch) error) {
		builder := array.NewRecordBuilder(alloc, schema)
		builder.Field(0).(*array.Float64Builder).AppendValues(ids, nil)
		builder.Field(1).(*array.StringBuilder).AppendValues(names, nil)
		record := builder.NewRecordBatch()
		require.NoError(t, write(record))
		record.Release()
		builder.Release()
	})
}

// --- overflow fixture: a value that cannot widen into a narrower target column ---

// fixtureInt64Overflow writes a single BIGINT-ish int64 column with one
// out-of-int32-range value, for use against an `INT NOT NULL` target column.
func fixtureInt64Overflow(t *testing.T, dir, filename, container string, values []int64) string {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "v", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
	return writeArrowFile(t, dir, filename, container, schema, func(alloc memory.Allocator, write func(arrow.RecordBatch) error) {
		builder := array.NewRecordBuilder(alloc, schema)
		builder.Field(0).(*array.Int64Builder).AppendValues(values, nil)
		record := builder.NewRecordBatch()
		require.NoError(t, write(record))
		record.Release()
		builder.Release()
	})
}

// --- large fixture: many record batches, for multi-CN parallel fan-out and the ---
// --- KILL-QUERY cancellation test --------------------------------------------------

const (
	largeFixtureBatches   = 100
	largeFixtureBatchRows = 1000
	largeFixtureRows      = largeFixtureBatches * largeFixtureBatchRows
)

// fixtureLarge writes an IPC File (so record-batch parallel shard fan-out applies)
// with largeFixtureRows rows of `id BIGINT NOT NULL, payload VARCHAR(64) NOT NULL`
// spread across largeFixtureBatches record batches, giving both the multi-CN
// parallel-shard test and the cancel-mid-LOAD test enough real decode/insert work to
// observe or interrupt without relying on a sleep.
func fixtureLarge(t testing.TB) (path string, schemaDDL string) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "payload", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	path = writeArrowFile(t, t.TempDir(), "large.arrow", containerFile, schema, func(alloc memory.Allocator, write func(arrow.RecordBatch) error) {
		for b := 0; b < largeFixtureBatches; b++ {
			builder := array.NewRecordBuilder(alloc, schema)
			ids := make([]int64, largeFixtureBatchRows)
			payloads := make([]string, largeFixtureBatchRows)
			for r := 0; r < largeFixtureBatchRows; r++ {
				id := int64(b*largeFixtureBatchRows + r)
				ids[r] = id
				payloads[r] = fmt.Sprintf("payload-row-%08d-%s", id, strings.Repeat("x", 32))
			}
			builder.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
			builder.Field(1).(*array.StringBuilder).AppendValues(payloads, nil)
			record := builder.NewRecordBatch()
			require.NoError(t, write(record))
			record.Release()
			builder.Release()
		}
	})
	return path, "id BIGINT NOT NULL, payload VARCHAR(128) NOT NULL"
}
