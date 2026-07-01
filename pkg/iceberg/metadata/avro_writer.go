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
	"bytes"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func WriteManifestList(w io.Writer, manifests []api.ManifestFile) error {
	enc, err := newIcebergOCFEncoder(manifestListWriterSchema, w)
	if err != nil {
		return api.WrapError(api.ErrMetadataInvalid, "Iceberg manifest list OCF encoder could not be created", nil, err)
	}
	for _, manifest := range manifests {
		if err := enc.Encode(manifestFileRecord(manifest)); err != nil {
			_ = enc.Close()
			return api.WrapError(api.ErrMetadataInvalid, "Iceberg manifest list OCF encode failed", map[string]string{"path": manifest.ManifestPathHash}, err)
		}
	}
	if err := enc.Close(); err != nil {
		return api.WrapError(api.ErrMetadataInvalid, "Iceberg manifest list OCF close failed", nil, err)
	}
	return nil
}

func WriteManifest(w io.Writer, entries []api.ManifestEntry) error {
	schema := manifestWriterSchema(partitionFields(entries))
	enc, err := newIcebergOCFEncoder(schema, w)
	if err != nil {
		return api.WrapError(api.ErrMetadataInvalid, "Iceberg manifest OCF encoder could not be created", nil, err)
	}
	for _, entry := range entries {
		if err := enc.Encode(manifestEntryRecord(entry)); err != nil {
			_ = enc.Close()
			return api.WrapError(api.ErrMetadataInvalid, "Iceberg manifest OCF encode failed", map[string]string{"path": entry.DataFile.FilePathHash}, err)
		}
	}
	if err := enc.Close(); err != nil {
		return api.WrapError(api.ErrMetadataInvalid, "Iceberg manifest OCF close failed", nil, err)
	}
	return nil
}

func newIcebergOCFEncoder(schema string, w io.Writer) (*ocf.Encoder, error) {
	return ocf.NewEncoder(schema, w, ocf.WithSchemaMarshaler(func(avro.Schema) ([]byte, error) {
		return []byte(schema), nil
	}))
}

func EncodeManifestList(manifests []api.ManifestFile) ([]byte, error) {
	var buf bytes.Buffer
	if err := WriteManifestList(&buf, manifests); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func EncodeManifest(entries []api.ManifestEntry) ([]byte, error) {
	var buf bytes.Buffer
	if err := WriteManifest(&buf, entries); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func manifestFileRecord(manifest api.ManifestFile) map[string]any {
	content := int32(0)
	if manifest.Content == api.ManifestContentDeletes {
		content = 1
	}
	return map[string]any{
		"manifest_path":        manifest.Path,
		"manifest_length":      manifest.Length,
		"partition_spec_id":    int32(manifest.PartitionSpecID),
		"content":              content,
		"sequence_number":      manifest.SequenceNumber,
		"min_sequence_number":  manifest.MinSequenceNumber,
		"added_snapshot_id":    manifest.AddedSnapshotID,
		"added_files_count":    int32(manifest.AddedFilesCount),
		"existing_files_count": int32(manifest.ExistingFilesCount),
		"deleted_files_count":  int32(manifest.DeletedFilesCount),
		"added_rows_count":     manifest.AddedRowsCount,
		"existing_rows_count":  manifest.ExistingRowsCount,
		"deleted_rows_count":   manifest.DeletedRowsCount,
		"partitions":           partitionSummaryRecords(manifest.Partitions),
		"key_metadata":         manifest.KeyMetadata,
	}
}

func manifestEntryRecord(entry api.ManifestEntry) map[string]any {
	return map[string]any{
		"status":               int32(entry.Status),
		"snapshot_id":          entry.SnapshotID,
		"sequence_number":      entry.SequenceNumber,
		"file_sequence_number": entry.FileSequence,
		"data_file":            dataFileRecord(entry.DataFile),
	}
}

func dataFileRecord(file api.DataFile) map[string]any {
	return map[string]any{
		"content":                 int32(file.Content),
		"file_path":               file.FilePath,
		"file_format":             strings.ToUpper(firstNonEmpty(file.FileFormat, "parquet")),
		"partition":               normalizePartitionRecord(file.Partition),
		"record_count":            file.RecordCount,
		"file_size_in_bytes":      file.FileSizeInBytes,
		"column_sizes":            intLongRecords(file.ColumnSizes),
		"value_counts":            intLongRecords(file.ValueCounts),
		"null_value_counts":       intLongRecords(file.NullValueCounts),
		"nan_value_counts":        intLongRecords(file.NaNValueCounts),
		"lower_bounds":            intBytesRecords(file.LowerBounds),
		"upper_bounds":            intBytesRecords(file.UpperBounds),
		"split_offsets":           file.SplitOffsets,
		"equality_ids":            int32Slice(file.EqualityIDs),
		"sort_order_id":           int32(file.SortOrderID),
		"spec_id":                 int32(file.SpecID),
		"referenced_data_file":    file.ReferencedDataFile,
		"delete_schema_id":        int32(file.DeleteSchemaID),
		"key_metadata":            file.KeyMetadata,
		"encryption_key_metadata": file.EncryptionKeyMetadata,
	}
}

func partitionSummaryRecords(in []api.PartitionFieldSummary) []map[string]any {
	out := make([]map[string]any, 0, len(in))
	for _, summary := range in {
		out = append(out, map[string]any{
			"contains_null": summary.ContainsNull,
			"contains_nan":  summary.ContainsNaN,
			"lower_bound":   summary.LowerBound,
			"upper_bound":   summary.UpperBound,
		})
	}
	return out
}

func intLongRecords(in map[int]int64) []map[string]any {
	if len(in) == 0 {
		return []map[string]any{}
	}
	keys := make([]int, 0, len(in))
	for key := range in {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	out := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		out = append(out, map[string]any{"key": int32(key), "value": in[key]})
	}
	return out
}

func intBytesRecords(in map[int][]byte) []map[string]any {
	if len(in) == 0 {
		return []map[string]any{}
	}
	keys := make([]int, 0, len(in))
	for key := range in {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	out := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		out = append(out, map[string]any{"key": int32(key), "value": in[key]})
	}
	return out
}

func int32Slice(in []int) []int32 {
	out := make([]int32, 0, len(in))
	for _, value := range in {
		out = append(out, int32(value))
	}
	return out
}

func normalizePartitionRecord(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		switch v := value.(type) {
		case int:
			out[key] = int32(v)
		case int32, int64, string, bool:
			out[key] = v
		default:
			out[key] = int32(0)
		}
	}
	return out
}

func partitionFields(entries []api.ManifestEntry) []partitionField {
	typesByName := make(map[string]string)
	idsByName := make(map[string]int)
	for _, entry := range entries {
		for key, value := range entry.DataFile.Partition {
			if _, ok := typesByName[key]; !ok {
				typesByName[key] = avroTypeForPartitionValue(value)
			}
			if id := entry.DataFile.PartitionFieldIDs[key]; id != 0 {
				idsByName[key] = id
			}
		}
	}
	names := make([]string, 0, len(typesByName))
	for name := range typesByName {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]partitionField, 0, len(names))
	for i, name := range names {
		id := idsByName[name]
		if id == 0 {
			id = 1000 + i
		}
		out = append(out, partitionField{Name: name, Type: typesByName[name], ID: id})
	}
	return out
}

func avroTypeForPartitionValue(value any) string {
	switch value.(type) {
	case bool:
		return "boolean"
	case int64:
		return "long"
	case string:
		return "string"
	default:
		return "int"
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

type partitionField struct {
	Name string
	Type string
	ID   int
}

const manifestListWriterSchema = `{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {"name": "manifest_path", "type": "string", "field-id": 500},
    {"name": "manifest_length", "type": "long", "field-id": 501},
    {"name": "partition_spec_id", "type": "int", "field-id": 502},
    {"name": "content", "type": ["null", "int"], "default": null, "field-id": 517},
    {"name": "sequence_number", "type": ["null", "long"], "default": null, "field-id": 515},
    {"name": "min_sequence_number", "type": ["null", "long"], "default": null, "field-id": 516},
    {"name": "added_snapshot_id", "type": ["null", "long"], "default": null, "field-id": 503},
    {"name": "added_files_count", "type": ["null", "int"], "default": null, "field-id": 504},
    {"name": "existing_files_count", "type": ["null", "int"], "default": null, "field-id": 505},
    {"name": "deleted_files_count", "type": ["null", "int"], "default": null, "field-id": 506},
    {"name": "added_rows_count", "type": ["null", "long"], "default": null, "field-id": 512},
    {"name": "existing_rows_count", "type": ["null", "long"], "default": null, "field-id": 513},
    {"name": "deleted_rows_count", "type": ["null", "long"], "default": null, "field-id": 514},
    {"name": "partitions", "type": ["null", {"type": "array", "items": {
      "type": "record",
      "name": "r508",
      "fields": [
        {"name": "contains_null", "type": "boolean", "field-id": 509},
        {"name": "contains_nan", "type": ["null", "boolean"], "default": null, "field-id": 518},
        {"name": "lower_bound", "type": ["null", "bytes"], "default": null, "field-id": 510},
        {"name": "upper_bound", "type": ["null", "bytes"], "default": null, "field-id": 511}
      ]
    }, "element-id": 508}], "default": null, "field-id": 507},
    {"name": "key_metadata", "type": ["null", "bytes"], "default": null, "field-id": 519}
  ]
}`

func manifestWriterSchema(fields []partitionField) string {
	var partition strings.Builder
	partition.WriteString(`{"type":"record","name":"r102","fields":[`)
	for i, field := range fields {
		if i > 0 {
			partition.WriteString(",")
		}
		partition.WriteString(`{"name":"`)
		partition.WriteString(field.Name)
		partition.WriteString(`","type":["null","`)
		partition.WriteString(field.Type)
		partition.WriteString(`"],"default":null,"field-id":`)
		partition.WriteString(strconv.Itoa(field.ID))
		partition.WriteString(`}`)
	}
	partition.WriteString(`]}`)
	return `{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    {"name": "status", "type": "int", "field-id": 0},
    {"name": "snapshot_id", "type": ["null", "long"], "default": null, "field-id": 1},
    {"name": "sequence_number", "type": ["null", "long"], "default": null, "field-id": 3},
    {"name": "file_sequence_number", "type": ["null", "long"], "default": null, "field-id": 4},
    {"name": "data_file", "type": {
      "type": "record",
      "name": "r2",
      "fields": [
        {"name": "content", "type": ["null", "int"], "default": null, "field-id": 134},
        {"name": "file_path", "type": "string", "field-id": 100},
        {"name": "file_format", "type": "string", "field-id": 101},
        {"name": "spec_id", "type": ["null", "int"], "default": null, "field-id": 141},
        {"name": "partition", "type": ` + partition.String() + `, "field-id": 102},
        {"name": "record_count", "type": "long", "field-id": 103},
        {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
        {"name": "column_sizes", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k117_v118", "fields": [{"name": "key", "type": "int", "field-id": 117}, {"name": "value", "type": "long", "field-id": 118}]}, "logicalType": "map"}], "default": null, "field-id": 108},
        {"name": "value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k119_v120", "fields": [{"name": "key", "type": "int", "field-id": 119}, {"name": "value", "type": "long", "field-id": 120}]}, "logicalType": "map"}], "default": null, "field-id": 109},
        {"name": "null_value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k121_v122", "fields": [{"name": "key", "type": "int", "field-id": 121}, {"name": "value", "type": "long", "field-id": 122}]}, "logicalType": "map"}], "default": null, "field-id": 110},
        {"name": "nan_value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k138_v139", "fields": [{"name": "key", "type": "int", "field-id": 138}, {"name": "value", "type": "long", "field-id": 139}]}, "logicalType": "map"}], "default": null, "field-id": 137},
        {"name": "lower_bounds", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k126_v127", "fields": [{"name": "key", "type": "int", "field-id": 126}, {"name": "value", "type": "bytes", "field-id": 127}]}, "logicalType": "map"}], "default": null, "field-id": 125},
        {"name": "upper_bounds", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k129_v130", "fields": [{"name": "key", "type": "int", "field-id": 129}, {"name": "value", "type": "bytes", "field-id": 130}]}, "logicalType": "map"}], "default": null, "field-id": 128},
        {"name": "key_metadata", "type": ["null", "bytes"], "default": null, "field-id": 131},
        {"name": "split_offsets", "type": ["null", {"type": "array", "items": "long", "element-id": 133}], "default": null, "field-id": 132},
        {"name": "equality_ids", "type": ["null", {"type": "array", "items": "int", "element-id": 136}], "default": null, "field-id": 135},
        {"name": "sort_order_id", "type": ["null", "int"], "default": null, "field-id": 140},
        {"name": "referenced_data_file", "type": ["null", "string"], "default": null, "field-id": 10000},
        {"name": "delete_schema_id", "type": ["null", "int"], "default": null, "field-id": 10001},
        {"name": "encryption_key_metadata", "type": ["null", "bytes"], "default": null, "field-id": 10002}
      ]
    }, "field-id": 2}
  ]
}`
}
