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

package maintenance

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func TestRewriteDataFilesSchemaEvolutionUsesFieldIDs(t *testing.T) {
	for _, tc := range []struct {
		name     string
		column   string
		fieldID  int
		wantNull bool
	}{
		{"unchanged_control", "id", 1, false},
		{"rename_same_id", "renamed_id", 1, false},
		{"drop_and_readd_same_name", "id", 2, true},
		{"new_optional_column_control", "new_id", 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const listPath = "s3://warehouse/orders/metadata/list.avro"
			const dataPath = "s3://warehouse/orders/data/old.parquet"
			data := rewriteDataFilesParquetBytes(t, []int64{7})
			list, err := encodeMaintenanceTestManifestList(nil)
			require.NoError(t, err)
			meta := maintenanceTestTableMetadata("s3://warehouse/orders")
			meta.CurrentSchemaID = 1
			meta.Schemas = append(meta.Schemas, api.Schema{SchemaID: 1, Fields: []api.SchemaField{{ID: tc.fieldID, Name: tc.column, Type: api.IcebergType{Kind: api.TypeLong}}}})
			result, err := (ParquetConcatRewriteDataFilesCompactor{ObjectReader: fakeRewriteManifestObjectReader{listPath: list, dataPath: data}}).CompactRewriteDataFiles(context.Background(), RewriteDataFilesCompactRequest{
				Metadata:  meta,
				Snapshot:  api.Snapshot{SnapshotID: 10, ManifestList: listPath},
				Selection: RewriteDataFilesSelection{Groups: []RewriteDataFileGroup{{PartitionSpecID: 0, Candidates: []RewriteDataFileCandidate{{File: api.DataFile{Content: api.DataFileContentData, FilePath: dataPath, FileFormat: "parquet", RecordCount: 1, FileSizeInBytes: int64(len(data))}}}}}},
				JobID:     "loom-field-ids", IdempotencyKey: "loom-field-ids",
			})
			require.NoError(t, err)
			require.Len(t, result.Objects, 1)
			file, err := parquet.OpenFile(bytes.NewReader(result.Objects[0].Payload), int64(len(result.Objects[0].Payload)))
			require.NoError(t, err)
			rows := file.RowGroups()[0].Rows()
			t.Cleanup(func() { require.NoError(t, rows.Close()) })
			output := make([]parquet.Row, 1)
			n, err := rows.ReadRows(output)
			if err != nil {
				require.ErrorIs(t, err, io.EOF)
			}
			require.Equal(t, 1, n)
			require.Len(t, output[0], 1)
			value := output[0][0]
			t.Logf("input: id(field-id=1)=7; output: %s(field-id=%d)=%v; wantNull=%v", tc.column, tc.fieldID, value, tc.wantNull)
			require.Equal(t, tc.wantNull, value.IsNull(), "schema evolution must map columns by field ID")
			if !tc.wantNull {
				require.Equal(t, int64(7), value.Int64())
			}
		})
	}
}

// This helper materializes the actual replacement file; the oracle reads its
// physical field IDs and values, independently of the projection implementation.
func compactEvolutionFiles(t *testing.T, fields []api.SchemaField, schemas []*parquet.Schema, rows [][]parquet.Row, limit int64) (*RewriteDataFilesCompactResult, error) {
	t.Helper()
	const listPath = "s3://warehouse/orders/metadata/evolution-list.avro"
	list, err := encodeMaintenanceTestManifestList(nil)
	require.NoError(t, err)
	reader := fakeRewriteManifestObjectReader{listPath: list}
	group := RewriteDataFileGroup{}
	for i, schema := range schemas {
		var buf bytes.Buffer
		writer := parquet.NewWriter(&buf, schema)
		_, err := writer.WriteRows(rows[i])
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		path := fmt.Sprintf("s3://warehouse/orders/data/evolution-%d.parquet", i)
		reader[path] = buf.Bytes()
		group.Candidates = append(group.Candidates, RewriteDataFileCandidate{File: api.DataFile{Content: api.DataFileContentData, FilePath: path, FileFormat: "parquet", RecordCount: int64(len(rows[i])), FileSizeInBytes: int64(buf.Len())}})
	}
	meta := maintenanceTestTableMetadata("s3://warehouse/orders")
	meta.Schemas[0].Fields = fields
	return (ParquetConcatRewriteDataFilesCompactor{ObjectReader: reader, MaxMemoryBytes: limit}).CompactRewriteDataFiles(context.Background(), RewriteDataFilesCompactRequest{Metadata: meta, Snapshot: api.Snapshot{SnapshotID: 10, ManifestList: listPath}, Selection: RewriteDataFilesSelection{Groups: []RewriteDataFileGroup{group}}})
}

func TestRewriteDataFilesProjectionMixedSchemas(t *testing.T) {
	// ID 1 moves from physical column 1 to output column 0. ID 2 moves in
	// the other direction and promotes float->double; ID 3 is newly added.
	old := parquet.NewSchema("old", parquet.Group{"z_old": parquet.FieldID(parquet.Int(32), 1), "a_old": parquet.FieldID(parquet.Leaf(parquet.FloatType), 2)})
	current := parquet.NewSchema("current", parquet.Group{"a_new": parquet.FieldID(parquet.Optional(parquet.Int(64)), 1), "z_new": parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.DoubleType)), 2)})
	fields := []api.SchemaField{{ID: 1, Name: "a_new", Type: api.IcebergType{Kind: api.TypeLong}}, {ID: 2, Name: "z_new", Type: api.IcebergType{Kind: api.TypeDouble}}, {ID: 3, Name: "missing", Type: api.IcebergType{Kind: api.TypeLong}}}
	result, err := compactEvolutionFiles(t, fields, []*parquet.Schema{old, current}, [][]parquet.Row{
		{{parquet.FloatValue(1.5).Level(0, 0, 0), parquet.Int32Value(7).Level(0, 0, 1)}},
		{{parquet.Int64Value(8).Level(0, 1, 0), parquet.NullValue().Level(0, 0, 1)}},
	}, 0)
	require.NoError(t, err)
	file, err := parquet.OpenFile(bytes.NewReader(result.Objects[0].Payload), int64(len(result.Objects[0].Payload)))
	require.NoError(t, err)
	require.Equal(t, []int{1, 3, 2}, []int{file.Schema().Fields()[0].ID(), file.Schema().Fields()[1].ID(), file.Schema().Fields()[2].ID()})
	reader := parquet.NewReader(file)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })
	output := make([]parquet.Row, 2)
	n, err := reader.ReadRows(output)
	if err != nil {
		require.ErrorIs(t, err, io.EOF)
	}
	require.Equal(t, 2, n)
	require.Equal(t, int64(7), output[0][0].Int64())
	require.Equal(t, 1.5, output[0][2].Double())
	require.True(t, output[0][1].IsNull())
	require.Equal(t, int64(8), output[1][0].Int64())
	require.True(t, output[1][1].IsNull())
	require.True(t, output[1][2].IsNull())
}

func TestRewriteDataFilesProjectionRejectsUnsafeInputs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		field  api.SchemaField
		source *parquet.Schema
		row    parquet.Row
		limit  int64
	}{
		{name: "missing_required", field: api.SchemaField{ID: 2, Name: "new", Required: true, Type: api.IcebergType{Kind: api.TypeLong}}},
		{name: "missing_initial_default", field: api.SchemaField{ID: 2, Name: "new", InitialDefault: []byte("7"), Type: api.IcebergType{Kind: api.TypeLong}}},
		{name: "narrowing", field: api.SchemaField{ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeInt}}},
		{name: "missing_ids", source: parquet.NewSchema("legacy", parquet.Group{"id": parquet.Int(64)})},
		{name: "duplicate_ids", source: parquet.NewSchema("duplicate", parquet.Group{"a": parquet.FieldID(parquet.Int(64), 1), "b": parquet.FieldID(parquet.Int(64), 1)}), row: parquet.Row{parquet.Int64Value(7).Level(0, 0, 0), parquet.Int64Value(8).Level(0, 0, 1)}},
		{name: "required_null", field: api.SchemaField{ID: 1, Name: "id", Required: true, Type: api.IcebergType{Kind: api.TypeLong}}, source: parquet.NewSchema("nullable", parquet.Group{"id": parquet.FieldID(parquet.Optional(parquet.Int(64)), 1)}), row: parquet.Row{parquet.NullValue().Level(0, 0, 0)}},
		{name: "projection_budget", limit: 3*rewriteDataFilesPageBufferBytes + 1024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.field.ID == 0 {
				tc.field = api.SchemaField{ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeLong}}
			}
			if tc.source == nil {
				tc.source = parquet.NewSchema("source", parquet.Group{"id": parquet.FieldID(parquet.Int(64), 1)})
			}
			if tc.row == nil {
				tc.row = parquet.Row{parquet.Int64Value(7).Level(0, 0, 0)}
			}
			result, err := compactEvolutionFiles(t, []api.SchemaField{tc.field}, []*parquet.Schema{tc.source}, [][]parquet.Row{{tc.row}}, tc.limit)
			require.Error(t, err)
			require.Nil(t, result)
		})
	}
}

func TestRewriteDataFilesProjectionIdentity(t *testing.T) {
	long := parquet.FieldID(parquet.Int(64), 1)
	optional := parquet.FieldID(parquet.Optional(parquet.Int(64)), 1)
	other := parquet.FieldID(parquet.Int(64), 2)
	for _, tc := range []struct {
		name              string
		source, target    parquet.Group
		identity, invalid bool
	}{
		{"required", parquet.Group{"id": long}, parquet.Group{"id": long}, true, false},
		{"optional", parquet.Group{"id": optional}, parquet.Group{"id": optional}, true, false},
		{"rename", parquet.Group{"id": long}, parquet.Group{"renamed": long}, true, false},
		{"plain_signed_equivalent", parquet.Group{"id": parquet.FieldID(parquet.Leaf(parquet.Int64Type), 1)}, parquet.Group{"id": long}, true, false},
		{"reorder", parquet.Group{"a": long, "z": other}, parquet.Group{"a": other, "z": long}, false, false},
		{"reused_name", parquet.Group{"id": optional}, parquet.Group{"id": parquet.FieldID(parquet.Optional(parquet.Int(64)), 2)}, false, false},
		{"added", parquet.Group{"id": long}, parquet.Group{"id": long, "new": parquet.FieldID(parquet.Optional(parquet.Int(64)), 2)}, false, false},
		{"dropped", parquet.Group{"id": long, "old": other}, parquet.Group{"id": long}, false, false},
		{"promotion", parquet.Group{"id": parquet.FieldID(parquet.Int(32), 1)}, parquet.Group{"id": long}, false, false},
		{"relax_required", parquet.Group{"id": long}, parquet.Group{"id": optional}, false, false},
		{"require_optional", parquet.Group{"id": optional}, parquet.Group{"id": long}, false, false},
		{"missing_ids", parquet.Group{"id": parquet.Int(64)}, parquet.Group{"id": parquet.Int(64)}, false, true},
		{"duplicate_ids", parquet.Group{"a": long, "z": long}, parquet.Group{"a": long, "z": long}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := newRewriteDataFilesProjection(nil, parquet.NewSchema("target", tc.target), parquet.NewSchema("source", tc.source))
			if tc.invalid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.identity, plan.identity)
		})
	}
}

func TestRewriteDataFilesProjectionAcrossRowBatches(t *testing.T) {
	// 129 rows are the minimum that reuses the 128-row reader/projection
	// scratch. Byte arrays and NULLs must survive reuse on both paths.
	for _, identity := range []bool{false, true} {
		t.Run(fmt.Sprintf("identity=%t", identity), func(t *testing.T) {
			source := parquet.NewSchema("old", parquet.Group{"a_text": parquet.FieldID(parquet.String(), 2), "z_id": parquet.FieldID(parquet.Int(64), 1)})
			if identity {
				source = parquet.NewSchema("current", parquet.Group{"a_id": parquet.FieldID(parquet.Optional(parquet.Int(64)), 1), "z_text": parquet.FieldID(parquet.Optional(parquet.String()), 2)})
			}
			rows := make([]parquet.Row, 129)
			for i := range rows {
				if identity {
					text := parquet.ByteArrayValue([]byte(fmt.Sprintf("row-%d", i))).Level(0, 1, 1)
					if i%2 == 0 {
						text = parquet.NullValue().Level(0, 0, 1)
					}
					rows[i] = parquet.Row{parquet.Int64Value(int64(i)).Level(0, 1, 0), text}
				} else {
					rows[i] = parquet.Row{parquet.ByteArrayValue([]byte(fmt.Sprintf("row-%d", i))).Level(0, 0, 0), parquet.Int64Value(int64(i)).Level(0, 0, 1)}
				}
			}
			result, err := compactEvolutionFiles(t, []api.SchemaField{{ID: 1, Name: "a_id", Type: api.IcebergType{Kind: api.TypeLong}}, {ID: 2, Name: "z_text", Type: api.IcebergType{Kind: api.TypeString}}}, []*parquet.Schema{source}, [][]parquet.Row{rows}, 0)
			require.NoError(t, err)
			reader := parquet.NewReader(bytes.NewReader(result.Objects[0].Payload))
			t.Cleanup(func() { require.NoError(t, reader.Close()) })
			output := make([]parquet.Row, len(rows))
			n, err := reader.ReadRows(output)
			if err != nil {
				require.ErrorIs(t, err, io.EOF)
			}
			require.Equal(t, len(rows), n)
			for i, row := range output {
				require.Equal(t, int64(i), row[0].Int64())
				if identity && i%2 == 0 {
					require.True(t, row[1].IsNull())
				} else {
					require.Equal(t, fmt.Sprintf("row-%d", i), string(row[1].ByteArray()))
				}
			}
		})
	}
}
