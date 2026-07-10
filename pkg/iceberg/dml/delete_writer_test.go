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
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func TestWritePositionDeleteFileUsesIcebergReservedSchemaAndSortedRows(t *testing.T) {
	ctx := context.Background()
	var buf bytes.Buffer
	file, err := WritePositionDeleteFile(ctx, &buf, PositionDeleteWriteRequest{
		FilePath:           "s3://warehouse/orders/delete/pos-1.parquet",
		ReferencedDataFile: "s3://warehouse/orders/data/a.parquet",
		Partition:          map[string]any{"created_day": int32(19895)},
		SpecID:             7,
		DeleteSchemaID:     9,
		Rows: []PositionDeleteRow{
			{FilePath: "s3://warehouse/orders/data/a.parquet", Pos: 8},
			{FilePath: "s3://warehouse/orders/data/a.parquet", Pos: 2},
		},
	})
	require.NoError(t, err)
	require.Equal(t, api.DataFileContentPositionDelete, file.Content)
	require.Equal(t, "parquet", file.FileFormat)
	require.Equal(t, int64(2), file.RecordCount)
	require.Equal(t, int64(buf.Len()), file.FileSizeInBytes)
	require.Equal(t, "s3://warehouse/orders/data/a.parquet", file.ReferencedDataFile)
	require.Equal(t, 7, file.SpecID)
	require.Equal(t, 9, file.DeleteSchemaID)
	require.Equal(t, int64(2), file.ValueCounts[positionDeletePosFieldID])
	require.Equal(t, int64(0), file.NullValueCounts[positionDeleteFilePathFieldID])
	require.Equal(t, int64(2), int64(binary.LittleEndian.Uint64(file.LowerBounds[positionDeletePosFieldID])))
	require.Equal(t, int64(8), int64(binary.LittleEndian.Uint64(file.UpperBounds[positionDeletePosFieldID])))
	require.Equal(t, int32(19895), file.Partition["created_day"])
	require.NotEmpty(t, file.FilePathHash)
	require.True(t, strings.HasPrefix(file.FilePathRedacted, "<redacted:path:"))

	pf, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	require.Equal(t, int64(2), pf.NumRows())
	require.Equal(t, positionDeleteFilePathFieldID, pf.Root().Column("file_path").ID())
	require.Equal(t, positionDeletePosFieldID, pf.Root().Column("pos").ID())
	requireDeleteParquetColumnPlainEncoded(t, pf, "file_path")
	rows := readDeleteRows(t, buf.Bytes(), 2)
	require.Equal(t, "s3://warehouse/orders/data/a.parquet", string(rows[0][0].ByteArray()))
	require.Equal(t, int64(2), rows[0][1].Int64())
	require.Equal(t, int64(8), rows[1][1].Int64())
}

func TestWritePositionDeleteFileRejectsMultipleReferencedFiles(t *testing.T) {
	var buf bytes.Buffer
	_, err := WritePositionDeleteFile(context.Background(), &buf, PositionDeleteWriteRequest{
		FilePath: "s3://warehouse/orders/delete/pos-1.parquet",
		Rows: []PositionDeleteRow{
			{FilePath: "s3://warehouse/orders/data/a.parquet", Pos: 1},
			{FilePath: "s3://warehouse/orders/data/b.parquet", Pos: 2},
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "one referenced data file")
}

func TestWritePositionDeleteObjectWritesPayloadThroughObjectWriter(t *testing.T) {
	ctx := context.Background()
	writer := &recordingDeleteObjectWriter{}
	file, err := WritePositionDeleteObject(ctx, writer, PositionDeleteWriteRequest{
		FilePath:           "s3://warehouse/orders/delete/pos-1.parquet",
		ReferencedDataFile: "s3://warehouse/orders/data/a.parquet",
		Rows: []PositionDeleteRow{
			{FilePath: "s3://warehouse/orders/data/a.parquet", Pos: 2},
		},
	})
	require.NoError(t, err)
	payload := writer.objects[file.FilePath]
	require.NotEmpty(t, payload)
	require.Equal(t, int64(len(payload)), file.FileSizeInBytes)
	pf, err := parquet.OpenFile(bytes.NewReader(payload), int64(len(payload)))
	require.NoError(t, err)
	require.Equal(t, int64(1), pf.NumRows())
}

func TestWriteEqualityDeleteFileUsesTableFieldIDsAndMetadata(t *testing.T) {
	ctx := context.Background()
	var buf bytes.Buffer
	schema := api.Schema{SchemaID: 9, Fields: []api.SchemaField{
		{ID: 1, Name: "id", Required: true, Type: api.IcebergType{Kind: api.TypeLong}},
		{ID: 2, Name: "region", Type: api.IcebergType{Kind: api.TypeString}},
		{ID: 3, Name: "ignored", Type: api.IcebergType{Kind: api.TypeString}},
	}}
	file, err := WriteEqualityDeleteFile(ctx, &buf, EqualityDeleteWriteRequest{
		FilePath:       "s3://warehouse/orders/delete/eq-1.parquet",
		Schema:         schema,
		EqualityIDs:    []int{1, 2},
		Partition:      map[string]any{"created_day": int32(19895)},
		SpecID:         7,
		DeleteSchemaID: schema.SchemaID,
		Rows: []EqualityDeleteRow{
			{Values: map[int]any{1: int64(44), 2: "ksa"}},
			{Values: map[int]any{1: int32(45), 2: nil}},
		},
	})
	require.NoError(t, err)
	require.Equal(t, api.DataFileContentEqualityDelete, file.Content)
	require.Equal(t, []int{1, 2}, file.EqualityIDs)
	require.Equal(t, int64(2), file.RecordCount)
	require.Equal(t, int64(buf.Len()), file.FileSizeInBytes)
	require.Equal(t, 7, file.SpecID)
	require.Equal(t, schema.SchemaID, file.DeleteSchemaID)
	require.Equal(t, int64(2), file.ValueCounts[1])
	require.Equal(t, int64(1), file.NullValueCounts[2])
	require.Equal(t, int64(44), int64(binary.LittleEndian.Uint64(file.LowerBounds[1])))
	require.Equal(t, int64(45), int64(binary.LittleEndian.Uint64(file.UpperBounds[1])))
	require.Equal(t, []byte("ksa"), file.LowerBounds[2])
	require.Equal(t, []byte("ksa"), file.UpperBounds[2])

	pf, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	require.Equal(t, int64(2), pf.NumRows())
	require.Equal(t, 1, pf.Root().Column("id").ID())
	require.Equal(t, 2, pf.Root().Column("region").ID())
	require.Nil(t, pf.Root().Column("ignored"))
	requireDeleteParquetColumnPlainEncoded(t, pf, "region")
	rows := readDeleteRows(t, buf.Bytes(), 2)
	require.Equal(t, int64(44), rows[0][0].Int64())
	require.Equal(t, "ksa", string(rows[0][1].ByteArray()))
	require.Equal(t, int64(45), rows[1][0].Int64())
	require.True(t, rows[1][1].IsNull())
}

func TestWriteEqualityDeleteObjectWritesPayloadThroughObjectWriter(t *testing.T) {
	ctx := context.Background()
	writer := &recordingDeleteObjectWriter{}
	file, err := WriteEqualityDeleteObject(ctx, writer, EqualityDeleteWriteRequest{
		FilePath: "s3://warehouse/orders/delete/eq-1.parquet",
		Schema: api.Schema{SchemaID: 9, Fields: []api.SchemaField{
			{ID: 1, Name: "id", Required: true, Type: api.IcebergType{Kind: api.TypeLong}},
		}},
		EqualityIDs:    []int{1},
		DeleteSchemaID: 9,
		Rows:           []EqualityDeleteRow{{Values: map[int]any{1: int64(44)}}},
	})
	require.NoError(t, err)
	payload := writer.objects[file.FilePath]
	require.NotEmpty(t, payload)
	require.Equal(t, int64(len(payload)), file.FileSizeInBytes)
	pf, err := parquet.OpenFile(bytes.NewReader(payload), int64(len(payload)))
	require.NoError(t, err)
	require.Equal(t, 1, pf.Root().Column("id").ID())
}

func TestWriteEqualityDeleteFileMaterializesDeleteManifest(t *testing.T) {
	ctx := context.Background()
	var buf bytes.Buffer
	deleteFile, err := WriteEqualityDeleteFile(ctx, &buf, EqualityDeleteWriteRequest{
		FilePath: "s3://warehouse/orders/delete/eq-1.parquet",
		Schema: api.Schema{SchemaID: 9, Fields: []api.SchemaField{
			{ID: 1, Name: "id", Required: true, Type: api.IcebergType{Kind: api.TypeLong}},
		}},
		EqualityIDs:    []int{1},
		SpecID:         7,
		DeleteSchemaID: 9,
		Rows:           []EqualityDeleteRow{{Values: map[int]any{1: int64(44)}}},
	})
	require.NoError(t, err)
	stream := ActionStream{
		Operation: OperationDelete,
		Base: CommitBase{
			Namespace:      api.Namespace{"sales"},
			Table:          "orders",
			BaseSnapshotID: 10,
			IdempotencyKey: "stmt-1",
		},
		Actions: []Action{{Kind: ActionAddEqualityDelete, DeleteFile: deleteFile}},
	}
	intent, err := BuildCommitIntent(stream)
	require.NoError(t, err)
	materialized, err := BuildManifestCommitAttempt(ctx, withDMLTestManifestMetadata(ManifestMaterializeRequest{
		Intent:             *intent,
		SnapshotID:         11,
		SequenceNumber:     12,
		DeleteManifestPath: "s3://warehouse/orders/metadata/delete-manifest.avro",
		ManifestListPath:   "s3://warehouse/orders/metadata/manifest-list.avro",
	}, 7))
	require.NoError(t, err)
	require.Nil(t, materialized.DataManifest)
	require.NotNil(t, materialized.DeleteManifest)
	require.Len(t, materialized.DeleteEntries, 1)
	require.Equal(t, api.ManifestContentDeletes, materialized.DeleteManifest.Content)
	require.Equal(t, api.DataFileContentEqualityDelete, materialized.DeleteEntries[0].DataFile.Content)
	require.Equal(t, []int{1}, materialized.DeleteEntries[0].DataFile.EqualityIDs)
	require.Equal(t, []string{"add-snapshot", "set-snapshot-ref"}, commitUpdateTypes(materialized.Attempt.Updates))
}

func TestCanonicalDeleteValueCoversSupportedTypesAndErrors(t *testing.T) {
	ctx := context.Background()
	ts := time.Date(2026, 7, 6, 1, 2, 3, 4000, time.FixedZone("KSA", 3*3600))
	tests := []struct {
		name  string
		typ   api.IcebergType
		value any
		want  any
	}{
		{name: "bool", typ: api.IcebergType{Kind: api.TypeBoolean}, value: true, want: true},
		{name: "int8 to int32", typ: api.IcebergType{Kind: api.TypeInt}, value: int8(7), want: int32(7)},
		{name: "int16 date to int32", typ: api.IcebergType{Kind: api.TypeDate}, value: int16(8), want: int32(8)},
		{name: "int to int32", typ: api.IcebergType{Kind: api.TypeInt}, value: int(9), want: int32(9)},
		{name: "int32 to int64", typ: api.IcebergType{Kind: api.TypeLong}, value: int32(10), want: int64(10)},
		{name: "time to timestamp micros", typ: api.IcebergType{Kind: api.TypeTimestamp}, value: ts, want: ts.UTC().UnixMicro()},
		{name: "time to timestamptz micros", typ: api.IcebergType{Kind: api.TypeTimestampTZ}, value: ts, want: ts.UTC().UnixMicro()},
		{name: "float", typ: api.IcebergType{Kind: api.TypeFloat}, value: float32(1.25), want: float32(1.25)},
		{name: "double from float32", typ: api.IcebergType{Kind: api.TypeDouble}, value: float32(2.5), want: float64(2.5)},
		{name: "double", typ: api.IcebergType{Kind: api.TypeDouble}, value: float64(3.5), want: float64(3.5)},
		{name: "string", typ: api.IcebergType{Kind: api.TypeString}, value: "ksa", want: "ksa"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := canonicalDeleteValue(ctx, tt.typ, tt.value)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}

	_, err := canonicalDeleteValue(ctx, api.IcebergType{Kind: api.TypeInt}, int64(math.MaxInt64))
	require.Error(t, err)
	require.Contains(t, err.Error(), "value type")
	_, err = canonicalDeleteValue(ctx, api.IcebergType{Kind: api.TypeInt}, math.MaxInt32+1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "out of range")
	_, err = canonicalDeleteValue(ctx, api.IcebergType{Kind: api.TypeLong}, ts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "value type")
	_, err = canonicalDeleteValue(ctx, api.IcebergType{Kind: api.TypeUUID}, "uuid")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported")
}

func TestDeleteMetricBoundsCoverTypesAndNaN(t *testing.T) {
	ctx := context.Background()
	fields := []api.SchemaField{
		{ID: 1, Name: "flag", Type: api.IcebergType{Kind: api.TypeBoolean}},
		{ID: 2, Name: "i", Type: api.IcebergType{Kind: api.TypeInt}},
		{ID: 3, Name: "l", Type: api.IcebergType{Kind: api.TypeLong}},
		{ID: 4, Name: "f", Type: api.IcebergType{Kind: api.TypeFloat}},
		{ID: 5, Name: "d", Type: api.IcebergType{Kind: api.TypeDouble}},
		{ID: 6, Name: "s", Type: api.IcebergType{Kind: api.TypeString}},
	}
	metrics := newDeleteMetrics(fields)
	metrics.observeNull(6)
	metrics.observe(ctx, 1, true)
	metrics.observe(ctx, 1, false)
	metrics.observe(ctx, 2, int32(20))
	metrics.observe(ctx, 2, int32(-3))
	metrics.observe(ctx, 3, int64(200))
	metrics.observe(ctx, 3, int64(100))
	metrics.observe(ctx, 4, float32(4.5))
	metrics.observe(ctx, 4, float32(1.5))
	metrics.observe(ctx, 4, float32(math.NaN()))
	metrics.observe(ctx, 5, float64(9.25))
	metrics.observe(ctx, 5, float64(2.25))
	metrics.observe(ctx, 6, "z")
	metrics.observe(ctx, 6, "a")
	metrics.observe(ctx, 99, "ignored")

	require.Equal(t, int64(1), metrics.nullCounts[6])
	require.Equal(t, int64(1), metrics.nanCounts[4])
	require.Equal(t, int64(-3), decodeDeleteMetricValue(api.IcebergType{Kind: api.TypeInt}, metrics.lowerBounds[2]))
	require.Equal(t, int64(20), decodeDeleteMetricValue(api.IcebergType{Kind: api.TypeInt}, metrics.upperBounds[2]))
	require.Equal(t, int64(100), decodeDeleteMetricValue(api.IcebergType{Kind: api.TypeLong}, metrics.lowerBounds[3]))
	require.Equal(t, int64(200), decodeDeleteMetricValue(api.IcebergType{Kind: api.TypeLong}, metrics.upperBounds[3]))
	require.Equal(t, "a", decodeDeleteMetricValue(api.IcebergType{Kind: api.TypeString}, metrics.lowerBounds[6]))
	require.Equal(t, "z", decodeDeleteMetricValue(api.IcebergType{Kind: api.TypeString}, metrics.upperBounds[6]))
	require.Equal(t, map[int]int64{1: 7, 2: 7, 3: 7, 4: 7, 5: 7, 6: 7}, metrics.valueCounts(7))

	file := deleteDataFile("s3://warehouse/orders/delete/eq.parquet", api.DataFileContentEqualityDelete, 7, 128, map[string]any{"p": "v"}, 2, 3, metrics)
	require.Equal(t, api.DataFileContentEqualityDelete, file.Content)
	require.Equal(t, int64(7), file.RecordCount)
	require.Equal(t, "v", file.Partition["p"])
	require.NotEmpty(t, file.FilePathHash)
	require.True(t, strings.HasPrefix(file.FilePathRedacted, "<redacted:path:"))
	require.Equal(t, int64(1), file.NullValueCounts[6])
	require.Equal(t, int64(1), file.NaNValueCounts[4])
	require.Equal(t, int64(7), file.ValueCounts[1])
}

func TestDeleteBoundHelpersCoverUnsupportedAndComparison(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		typ   api.IcebergType
		value any
	}{
		{api.IcebergType{Kind: api.TypeBoolean}, false},
		{api.IcebergType{Kind: api.TypeDate}, int32(20)},
		{api.IcebergType{Kind: api.TypeTimestampTZ}, int64(123)},
		{api.IcebergType{Kind: api.TypeFloat}, float32(1.25)},
		{api.IcebergType{Kind: api.TypeDouble}, float64(2.5)},
		{api.IcebergType{Kind: api.TypeString}, "abc"},
	} {
		encoded, cmp, err := encodeDeleteBound(ctx, tt.typ, tt.value)
		require.NoError(t, err)
		require.NotEmpty(t, encoded)
		require.Equal(t, 0, compareDeleteMetricValue(cmp, decodeDeleteMetricValue(tt.typ, encoded)))
	}
	_, _, err := encodeDeleteBound(ctx, api.IcebergType{Kind: api.TypeUUID}, "uuid")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported")
	require.Equal(t, -1, compareDeleteMetricValue(false, true))
	require.Equal(t, 1, compareDeleteMetricValue(int64(3), int64(2)))
	require.Equal(t, -1, compareDeleteMetricValue(float64(1), float64(2)))
	require.Equal(t, 1, compareDeleteMetricValue("z", "a"))
	require.True(t, isDeleteNaN(float32(math.NaN())))
	require.True(t, isDeleteNaN(math.NaN()))
	require.False(t, isDeleteNaN("nan"))
}

func readDeleteRows(t *testing.T, data []byte, count int) []parquet.Row {
	t.Helper()
	reader := parquet.NewReader(bytes.NewReader(data))
	defer func() {
		require.NoError(t, reader.Close())
	}()
	rows := make([]parquet.Row, count)
	n, err := reader.ReadRows(rows)
	if err != nil && err != io.EOF {
		require.NoError(t, err)
	}
	require.Equal(t, count, n)
	return rows
}

func requireDeleteParquetColumnPlainEncoded(t *testing.T, file *parquet.File, path string) {
	t.Helper()
	for _, rowGroup := range file.Metadata().RowGroups {
		for _, column := range rowGroup.Columns {
			if strings.Join(column.MetaData.PathInSchema, ".") != path {
				continue
			}
			encodings := make([]string, 0, len(column.MetaData.Encoding))
			for _, encoding := range column.MetaData.Encoding {
				encodings = append(encodings, encoding.String())
			}
			require.Contains(t, encodings, "PLAIN")
			require.NotContains(t, encodings, "DELTA_LENGTH_BYTE_ARRAY")
			return
		}
	}
	require.Failf(t, "column not found", "column path %s was not found in parquet metadata", path)
}

type recordingDeleteObjectWriter struct {
	objects map[string][]byte
}

func (w *recordingDeleteObjectWriter) WriteObject(ctx context.Context, location string, payload []byte) error {
	if w.objects == nil {
		w.objects = make(map[string][]byte)
	}
	w.objects[location] = append([]byte(nil), payload...)
	return nil
}
