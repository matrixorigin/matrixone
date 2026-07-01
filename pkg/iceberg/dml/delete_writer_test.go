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
	"strings"
	"testing"

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
	materialized, err := BuildManifestCommitAttempt(ctx, ManifestMaterializeRequest{
		Intent:             *intent,
		SnapshotID:         11,
		SequenceNumber:     12,
		DeleteManifestPath: "s3://warehouse/orders/metadata/delete-manifest.avro",
		ManifestListPath:   "s3://warehouse/orders/metadata/manifest-list.avro",
	})
	require.NoError(t, err)
	require.Nil(t, materialized.DataManifest)
	require.NotNil(t, materialized.DeleteManifest)
	require.Len(t, materialized.DeleteEntries, 1)
	require.Equal(t, api.ManifestContentDeletes, materialized.DeleteManifest.Content)
	require.Equal(t, api.DataFileContentEqualityDelete, materialized.DeleteEntries[0].DataFile.Content)
	require.Equal(t, []int{1}, materialized.DeleteEntries[0].DataFile.EqualityIDs)
	require.Equal(t, []string{"add-snapshot", "set-snapshot-ref"}, commitUpdateTypes(materialized.Attempt.Updates))
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
