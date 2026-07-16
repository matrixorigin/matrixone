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
	"bufio"
	"bytes"
	"testing"

	"github.com/hamba/avro/v2/ocf"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

const manifestListTestSchema = `{
  "type": "record",
  "name": "manifest_file",
  "namespace": "org.apache.iceberg",
  "fields": [
    {"name": "manifest_path", "type": "string"},
    {"name": "manifest_length", "type": "long"},
    {"name": "partition_spec_id", "type": "int"},
    {"name": "content", "type": "int"},
    {"name": "sequence_number", "type": "long"},
    {"name": "min_sequence_number", "type": "long"},
    {"name": "added_snapshot_id", "type": "long"},
    {"name": "added_files_count", "type": "int"},
    {"name": "existing_files_count", "type": "int"},
    {"name": "deleted_files_count", "type": "int"},
    {"name": "added_rows_count", "type": "long"},
    {"name": "existing_rows_count", "type": "long"},
    {"name": "deleted_rows_count", "type": "long"},
    {"name": "partitions", "type": {"type": "array", "items": {
      "type": "record",
      "name": "field_summary",
      "fields": [
        {"name": "contains_null", "type": "boolean"},
        {"name": "contains_nan", "type": "boolean"},
        {"name": "lower_bound", "type": "bytes"},
        {"name": "upper_bound", "type": "bytes"}
      ]
    }}},
    {"name": "key_metadata", "type": "bytes"}
  ]
}`

const manifestEntryTestSchema = `{
  "type": "record",
  "name": "manifest_entry",
  "namespace": "org.apache.iceberg",
  "fields": [
    {"name": "status", "type": "int"},
    {"name": "snapshot_id", "type": "long"},
    {"name": "sequence_number", "type": "long"},
    {"name": "file_sequence_number", "type": "long"},
    {"name": "data_file", "type": {
      "type": "record",
      "name": "data_file",
      "fields": [
        {"name": "content", "type": "int"},
        {"name": "file_path", "type": "string"},
        {"name": "file_format", "type": "string"},
        {"name": "partition", "type": {
          "type": "record",
          "name": "partition_tuple",
          "fields": [
            {"name": "created_day", "type": "int"}
          ]
        }},
        {"name": "record_count", "type": "long"},
        {"name": "file_size_in_bytes", "type": "long"},
        {"name": "column_sizes", "type": {"type": "array", "items": {"type": "record", "name": "col_size", "fields": [{"name": "key", "type": "int"}, {"name": "value", "type": "long"}]}}},
        {"name": "value_counts", "type": {"type": "array", "items": {"type": "record", "name": "value_count", "fields": [{"name": "key", "type": "int"}, {"name": "value", "type": "long"}]}}},
        {"name": "null_value_counts", "type": {"type": "array", "items": {"type": "record", "name": "null_count", "fields": [{"name": "key", "type": "int"}, {"name": "value", "type": "long"}]}}},
        {"name": "nan_value_counts", "type": {"type": "array", "items": {"type": "record", "name": "nan_count", "fields": [{"name": "key", "type": "int"}, {"name": "value", "type": "long"}]}}},
        {"name": "lower_bounds", "type": {"type": "array", "items": {"type": "record", "name": "lower_bound", "fields": [{"name": "key", "type": "int"}, {"name": "value", "type": "bytes"}]}}},
        {"name": "upper_bounds", "type": {"type": "array", "items": {"type": "record", "name": "upper_bound", "fields": [{"name": "key", "type": "int"}, {"name": "value", "type": "bytes"}]}}},
        {"name": "split_offsets", "type": {"type": "array", "items": "long"}},
        {"name": "equality_ids", "type": {"type": "array", "items": "int"}},
        {"name": "sort_order_id", "type": "int"},
        {"name": "spec_id", "type": "int"},
        {"name": "key_metadata", "type": "bytes"},
        {"name": "encryption_key_metadata", "type": "bytes"}
      ]
    }}
  ]
}`

func TestReadManifestList(t *testing.T) {
	data := encodeOCF(t, manifestListTestSchema, []map[string]any{
		{
			"manifest_path":        "s3://warehouse/t/metadata/m0.avro",
			"manifest_length":      int64(1234),
			"partition_spec_id":    int32(7),
			"content":              int32(0),
			"sequence_number":      int64(9),
			"min_sequence_number":  int64(8),
			"added_snapshot_id":    int64(22),
			"added_files_count":    int32(3),
			"existing_files_count": int32(4),
			"deleted_files_count":  int32(1),
			"added_rows_count":     int64(30),
			"existing_rows_count":  int64(40),
			"deleted_rows_count":   int64(10),
			"partitions": []map[string]any{
				{"contains_null": false, "contains_nan": false, "lower_bound": []byte{1}, "upper_bound": []byte{9}},
			},
			"key_metadata": []byte{},
		},
	})
	manifests, err := ReadManifestList(data)
	if err != nil {
		t.Fatalf("read manifest list: %v", err)
	}
	if len(manifests) != 1 {
		t.Fatalf("expected one manifest, got %d", len(manifests))
	}
	manifest := manifests[0]
	if manifest.Path != "s3://warehouse/t/metadata/m0.avro" || manifest.PartitionSpecID != 7 || manifest.AddedFilesCount != 3 {
		t.Fatalf("unexpected manifest: %+v", manifest)
	}
	if manifest.ManifestPathHash == "" || bytes.Equal(manifest.Partitions[0].LowerBound, []byte{9}) {
		t.Fatalf("manifest hash or bounds not decoded: %+v", manifest)
	}
	if err := ValidateP0ManifestFile(manifest); err != nil {
		t.Fatalf("data manifest should be accepted: %v", err)
	}
}

func TestReadManifestListRejectsInvalidContentAndMetrics(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "unknown content", mutate: func(record map[string]any) { record["content"] = int32(9) }},
		{name: "negative rows", mutate: func(record map[string]any) { record["existing_rows_count"] = int64(-1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := validManifestListTestRecord()
			tc.mutate(record)
			_, err := ReadManifestList(encodeOCF(t, manifestListTestSchema, []map[string]any{record}))
			assertIcebergCode(t, err, api.ErrMetadataInvalid)
		})
	}
}

func validManifestListTestRecord() map[string]any {
	return map[string]any{
		"manifest_path":        "s3://warehouse/t/metadata/m0.avro",
		"manifest_length":      int64(1234),
		"partition_spec_id":    int32(7),
		"content":              int32(0),
		"sequence_number":      int64(9),
		"min_sequence_number":  int64(8),
		"added_snapshot_id":    int64(22),
		"added_files_count":    int32(3),
		"existing_files_count": int32(4),
		"deleted_files_count":  int32(1),
		"added_rows_count":     int64(30),
		"existing_rows_count":  int64(40),
		"deleted_rows_count":   int64(10),
		"partitions":           []map[string]any{},
		"key_metadata":         []byte{},
	}
}

func TestReadManifest(t *testing.T) {
	data := encodeOCF(t, manifestEntryTestSchema, []map[string]any{
		{
			"status":               int32(1),
			"snapshot_id":          int64(22),
			"sequence_number":      int64(9),
			"file_sequence_number": int64(9),
			"data_file": map[string]any{
				"content":                 int32(0),
				"file_path":               "s3://warehouse/t/data/00001.parquet",
				"file_format":             "PARQUET",
				"partition":               map[string]any{"created_day": int32(19815)},
				"record_count":            int64(100),
				"file_size_in_bytes":      int64(2048),
				"column_sizes":            []map[string]any{{"key": int32(1), "value": int64(1024)}},
				"value_counts":            []map[string]any{{"key": int32(1), "value": int64(100)}},
				"null_value_counts":       []map[string]any{{"key": int32(1), "value": int64(0)}},
				"nan_value_counts":        []map[string]any{},
				"lower_bounds":            []map[string]any{{"key": int32(1), "value": []byte{0}}},
				"upper_bounds":            []map[string]any{{"key": int32(1), "value": []byte{100}}},
				"split_offsets":           []int64{4, 1024},
				"equality_ids":            []int32{},
				"sort_order_id":           int32(0),
				"spec_id":                 int32(7),
				"key_metadata":            []byte{},
				"encryption_key_metadata": []byte{},
			},
		},
	})
	entries, err := ReadManifest(data)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected one entry, got %d", len(entries))
	}
	entry := entries[0]
	if entry.Status != api.ManifestEntryAdded || entry.DataFile.FileFormat != "parquet" || entry.DataFile.RecordCount != 100 {
		t.Fatalf("unexpected entry: %+v", entry)
	}
	if entry.DataFile.ColumnSizes[1] != 1024 || entry.DataFile.ValueCounts[1] != 100 || len(entry.DataFile.SplitOffsets) != 2 {
		t.Fatalf("metrics not decoded: %+v", entry.DataFile)
	}
	if err := ValidateP0DataFile(entry.DataFile); err != nil {
		t.Fatalf("parquet data file should be accepted: %v", err)
	}
}

func TestReadManifestRejectsInvalidEntryEnumsAndMetrics(t *testing.T) {
	tests := []struct {
		name        string
		status      int32
		content     int32
		recordCount int64
		fileSize    int64
	}{
		{name: "unknown status", status: 9, content: 0, recordCount: 1, fileSize: 1},
		{name: "unknown content", status: 1, content: 9, recordCount: 1, fileSize: 1},
		{name: "negative metrics", status: 2, content: 0, recordCount: -1, fileSize: 1},
		{name: "missing nonempty size", status: 2, content: 0, recordCount: 1, fileSize: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := encodeOCF(t, manifestEntryTestSchema, []map[string]any{
				manifestEntryTestRecord(tc.status, tc.content, tc.recordCount, tc.fileSize),
			})
			_, err := ReadManifest(data)
			assertIcebergCode(t, err, api.ErrMetadataInvalid)
		})
	}
}

func manifestEntryTestRecord(status, content int32, recordCount, fileSize int64) map[string]any {
	return map[string]any{
		"status":               status,
		"snapshot_id":          int64(22),
		"sequence_number":      int64(9),
		"file_sequence_number": int64(9),
		"data_file": map[string]any{
			"content":                 content,
			"file_path":               "s3://warehouse/t/data/invalid.parquet",
			"file_format":             "PARQUET",
			"partition":               map[string]any{"created_day": int32(1)},
			"record_count":            recordCount,
			"file_size_in_bytes":      fileSize,
			"column_sizes":            []map[string]any{},
			"value_counts":            []map[string]any{},
			"null_value_counts":       []map[string]any{},
			"nan_value_counts":        []map[string]any{},
			"lower_bounds":            []map[string]any{},
			"upper_bounds":            []map[string]any{},
			"split_offsets":           []int64{},
			"equality_ids":            []int32{},
			"sort_order_id":           int32(0),
			"spec_id":                 int32(7),
			"key_metadata":            []byte{},
			"encryption_key_metadata": []byte{},
		},
	}
}

func TestReadOCFRecordsRejectsCompressedBlockExpansion(t *testing.T) {
	const schema = `{"type":"record","name":"compressed_record","fields":[{"name":"payload","type":"bytes"}]}`
	var buf bytes.Buffer
	enc, err := ocf.NewEncoder(schema, &buf, ocf.WithCodec(ocf.Deflate))
	if err != nil {
		t.Fatalf("new compressed ocf encoder: %v", err)
	}
	if err := enc.Encode(map[string]any{"payload": bytes.Repeat([]byte{'x'}, 64<<10)}); err != nil {
		t.Fatalf("encode compressed ocf record: %v", err)
	}
	if err := enc.Close(); err != nil {
		t.Fatalf("close compressed ocf encoder: %v", err)
	}

	visited := 0
	err = readOCFRecords(bytes.NewReader(buf.Bytes()), "compressed_test", 10, 1<<20, func(_ int, record map[string]any) error {
		visited++
		if len(record["payload"].([]byte)) != 64<<10 {
			t.Fatalf("unexpected decoded payload length: %d", len(record["payload"].([]byte)))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read bounded compressed OCF: %v", err)
	}
	if visited != 1 {
		t.Fatalf("expected one compressed record, got %d", visited)
	}

	err = readOCFRecords(bytes.NewReader(buf.Bytes()), "compressed_test", 10, 1024, func(int, map[string]any) error {
		t.Fatal("oversized compressed record must not be visited")
		return nil
	})
	assertIcebergCode(t, err, api.ErrPlanningLimitExceeded)
}

func TestReadOCFRecordsRejectsNonPositiveAndUnderdeclaredBlocks(t *testing.T) {
	const schema = `{"type":"record","name":"item","fields":[{"name":"id","type":"long"}]}`
	data := encodeOCF(t, schema, []map[string]any{{"id": int64(1)}, {"id": int64(2)}})
	blockOffset := firstOCFBlockOffset(t, data)
	if data[blockOffset] != byte(4) {
		t.Fatalf("expected one two-record OCF block, count byte=%d", data[blockOffset])
	}

	zeroCount := append([]byte(nil), data...)
	zeroCount[blockOffset] = 0
	err := readOCFRecords(bytes.NewReader(zeroCount), "zero_count", 10, 1<<20, func(int, map[string]any) error {
		t.Fatal("zero-count block must not be visited")
		return nil
	})
	assertIcebergCode(t, err, api.ErrMetadataInvalid)

	underdeclared := append([]byte(nil), data...)
	underdeclared[blockOffset] = 2 // Avro zig-zag encoding for a declared count of one.
	err = readOCFRecords(bytes.NewReader(underdeclared), "underdeclared", 10, 1<<20, func(int, map[string]any) error { return nil })
	assertIcebergCode(t, err, api.ErrMetadataInvalid)
}

func firstOCFBlockOffset(t *testing.T, data []byte) int {
	t.Helper()
	source := bytes.NewReader(data)
	reader := bufio.NewReaderSize(source, 4096)
	_, err := readBoundedOCFHeader(reader, "test", 1<<20)
	if err != nil {
		t.Fatalf("read OCF header: %v", err)
	}
	return len(data) - source.Len() - reader.Buffered()
}

func encodeOCF(t *testing.T, schema string, records []map[string]any) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := ocf.NewEncoder(schema, &buf)
	if err != nil {
		t.Fatalf("new ocf encoder: %v", err)
	}
	for _, record := range records {
		if err := enc.Encode(record); err != nil {
			t.Fatalf("encode ocf record: %v", err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatalf("close ocf encoder: %v", err)
	}
	return buf.Bytes()
}
