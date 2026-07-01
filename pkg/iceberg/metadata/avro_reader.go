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
	"reflect"
	"strconv"
	"strings"

	"github.com/hamba/avro/v2/ocf"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func ReadManifestList(data []byte) ([]api.ManifestFile, error) {
	return ReadManifestListFromReader(bytes.NewReader(data))
}

func ReadManifestListFromReader(r io.Reader) ([]api.ManifestFile, error) {
	records, err := readOCFRecords(r, "manifest_list")
	if err != nil {
		return nil, err
	}
	out := make([]api.ManifestFile, 0, len(records))
	for i, record := range records {
		manifest, err := manifestFileFromRecord(record)
		if err != nil {
			return nil, api.WrapError(api.ErrMetadataInvalid, "Iceberg manifest list entry is invalid", map[string]string{"entry": strconv.Itoa(i)}, err)
		}
		out = append(out, manifest)
	}
	return out, nil
}

func ReadManifest(data []byte) ([]api.ManifestEntry, error) {
	return ReadManifestFromReader(bytes.NewReader(data))
}

func ReadManifestFromReader(r io.Reader) ([]api.ManifestEntry, error) {
	records, err := readOCFRecords(r, "manifest")
	if err != nil {
		return nil, err
	}
	out := make([]api.ManifestEntry, 0, len(records))
	for i, record := range records {
		entry, err := manifestEntryFromRecord(record)
		if err != nil {
			return nil, api.WrapError(api.ErrMetadataInvalid, "Iceberg manifest entry is invalid", map[string]string{"entry": strconv.Itoa(i)}, err)
		}
		out = append(out, entry)
	}
	return out, nil
}

func readOCFRecords(r io.Reader, operation string) ([]map[string]any, error) {
	dec, err := ocf.NewDecoder(r)
	if err != nil {
		return nil, api.WrapError(api.ErrMetadataInvalid, "Iceberg Avro OCF decoder could not be created", map[string]string{"operation": operation}, err)
	}
	defer dec.Close()
	var out []map[string]any
	for dec.HasNext() {
		var raw any
		if err := dec.Decode(&raw); err != nil {
			return nil, api.WrapError(api.ErrMetadataInvalid, "Iceberg Avro OCF record decode failed", map[string]string{"operation": operation}, err)
		}
		record, ok := raw.(map[string]any)
		if !ok {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg Avro OCF record is not a map", map[string]string{"operation": operation})
		}
		out = append(out, record)
	}
	if err := dec.Error(); err != nil {
		return nil, api.WrapError(api.ErrMetadataInvalid, "Iceberg Avro OCF stream failed", map[string]string{"operation": operation}, err)
	}
	return out, nil
}

func manifestFileFromRecord(record map[string]any) (api.ManifestFile, error) {
	path, err := requiredString(record, "manifest_path")
	if err != nil {
		return api.ManifestFile{}, err
	}
	content := api.ManifestContentData
	if n, ok := optionalInt(record, "content"); ok && n != 0 {
		content = api.ManifestContentDeletes
	}
	manifest := api.ManifestFile{
		Path:                     path,
		Length:                   optionalInt64Value(record, "manifest_length"),
		PartitionSpecID:          optionalIntValue(record, "partition_spec_id"),
		Content:                  content,
		SequenceNumber:           optionalInt64Value(record, "sequence_number"),
		MinSequenceNumber:        optionalInt64Value(record, "min_sequence_number"),
		AddedSnapshotID:          optionalInt64Value(record, "added_snapshot_id"),
		AddedFilesCount:          optionalIntValue(record, "added_files_count"),
		ExistingFilesCount:       optionalIntValue(record, "existing_files_count"),
		DeletedFilesCount:        optionalIntValue(record, "deleted_files_count"),
		AddedRowsCount:           optionalInt64Value(record, "added_rows_count"),
		ExistingRowsCount:        optionalInt64Value(record, "existing_rows_count"),
		DeletedRowsCount:         optionalInt64Value(record, "deleted_rows_count"),
		AddedFilesSizeInBytes:    optionalInt64Value(record, "added_files_size_in_bytes"),
		ExistingFilesSizeInBytes: optionalInt64Value(record, "existing_files_size_in_bytes"),
		DeletedFilesSizeInBytes:  optionalInt64Value(record, "deleted_files_size_in_bytes"),
		ReferencedDataFilesCount: optionalIntValue(record, "referenced_data_files_count"),
		KeyMetadata:              optionalBytesValue(record, "key_metadata"),
		ManifestPathRedacted:     api.RedactPath(path),
		ManifestPathHash:         api.PathHash(path),
	}
	if firstRowID, ok := optionalInt64(record, "first_row_id"); ok {
		manifest.FirstRowID = &firstRowID
	}
	manifest.Partitions = partitionSummaries(record["partitions"])
	return manifest, nil
}

func manifestEntryFromRecord(record map[string]any) (api.ManifestEntry, error) {
	dataFileRaw := unwrapUnion(record["data_file"])
	dataFileRecord, ok := dataFileRaw.(map[string]any)
	if !ok {
		return api.ManifestEntry{}, api.NewError(api.ErrMetadataInvalid, "Iceberg manifest entry is missing data_file", nil)
	}
	file, err := dataFileFromRecord(dataFileRecord)
	if err != nil {
		return api.ManifestEntry{}, err
	}
	entry := api.ManifestEntry{
		Status:         api.ManifestEntryStatus(optionalIntValue(record, "status")),
		SnapshotID:     optionalInt64Value(record, "snapshot_id"),
		SequenceNumber: optionalInt64Value(record, "sequence_number"),
		FileSequence:   optionalInt64Value(record, "file_sequence_number"),
		DataFile:       file,
	}
	entry.DataFile.SequenceNumber = entry.SequenceNumber
	entry.DataFile.FileSequenceNumber = entry.FileSequence
	return entry, nil
}

func dataFileFromRecord(record map[string]any) (api.DataFile, error) {
	path, err := requiredString(record, "file_path")
	if err != nil {
		return api.DataFile{}, err
	}
	content := api.DataFileContent(optionalIntValue(record, "content"))
	file := api.DataFile{
		Content:               content,
		FilePath:              path,
		FileFormat:            strings.ToLower(strings.TrimSpace(optionalStringValue(record, "file_format"))),
		Partition:             optionalRecordValue(record, "partition"),
		RecordCount:           optionalInt64Value(record, "record_count"),
		FileSizeInBytes:       optionalInt64Value(record, "file_size_in_bytes"),
		ColumnSizes:           intLongMap(record["column_sizes"]),
		ValueCounts:           intLongMap(record["value_counts"]),
		NullValueCounts:       intLongMap(record["null_value_counts"]),
		NaNValueCounts:        intLongMap(record["nan_value_counts"]),
		LowerBounds:           intBytesMap(record["lower_bounds"]),
		UpperBounds:           intBytesMap(record["upper_bounds"]),
		SplitOffsets:          int64Slice(record["split_offsets"]),
		EqualityIDs:           intSlice(record["equality_ids"]),
		SortOrderID:           optionalIntValue(record, "sort_order_id"),
		SpecID:                optionalIntValue(record, "spec_id"),
		ReferencedDataFile:    optionalStringValue(record, "referenced_data_file"),
		DeleteSchemaID:        optionalIntValue(record, "delete_schema_id"),
		KeyMetadata:           optionalBytesValue(record, "key_metadata"),
		EncryptionKeyMetadata: optionalBytesValue(record, "encryption_key_metadata"),
		FilePathRedacted:      api.RedactPath(path),
		FilePathHash:          api.PathHash(path),
	}
	if firstRowID, ok := optionalInt64(record, "first_row_id"); ok {
		file.FirstRowID = &firstRowID
	}
	if deletionVector := unwrapUnion(record["deletion_vector"]); deletionVector != nil {
		file.DeletionVectorPath = "present"
	}
	return file, nil
}

func partitionSummaries(raw any) []api.PartitionFieldSummary {
	values := anySlice(unwrapUnion(raw))
	if len(values) == 0 {
		return nil
	}
	out := make([]api.PartitionFieldSummary, 0, len(values))
	for _, value := range values {
		record, ok := unwrapUnion(value).(map[string]any)
		if !ok {
			continue
		}
		summary := api.PartitionFieldSummary{
			ContainsNull: optionalBoolValue(record, "contains_null"),
			ContainsNaN:  optionalBoolValue(record, "contains_nan"),
			LowerBound:   optionalBytesValue(record, "lower_bound"),
			UpperBound:   optionalBytesValue(record, "upper_bound"),
		}
		out = append(out, summary)
	}
	return out
}

func intLongMap(raw any) map[int]int64 {
	values := anySlice(unwrapUnion(raw))
	if len(values) == 0 {
		return nil
	}
	out := make(map[int]int64, len(values))
	for _, value := range values {
		record, ok := unwrapUnion(value).(map[string]any)
		if !ok {
			continue
		}
		key, keyOK := optionalInt(record, "key")
		val, valOK := optionalInt64(record, "value")
		if keyOK && valOK {
			out[key] = val
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func intBytesMap(raw any) map[int][]byte {
	values := anySlice(unwrapUnion(raw))
	if len(values) == 0 {
		return nil
	}
	out := make(map[int][]byte, len(values))
	for _, value := range values {
		record, ok := unwrapUnion(value).(map[string]any)
		if !ok {
			continue
		}
		key, keyOK := optionalInt(record, "key")
		val := bytesFromAny(unwrapUnion(record["value"]))
		if keyOK && val != nil {
			out[key] = val
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func int64Slice(raw any) []int64 {
	values := anySlice(unwrapUnion(raw))
	if len(values) == 0 {
		return nil
	}
	out := make([]int64, 0, len(values))
	for _, value := range values {
		if n, ok := int64FromAny(unwrapUnion(value)); ok {
			out = append(out, n)
		}
	}
	return out
}

func intSlice(raw any) []int {
	values := anySlice(unwrapUnion(raw))
	if len(values) == 0 {
		return nil
	}
	out := make([]int, 0, len(values))
	for _, value := range values {
		if n, ok := intFromAny(unwrapUnion(value)); ok {
			out = append(out, n)
		}
	}
	return out
}

func optionalRecordValue(record map[string]any, key string) map[string]any {
	if out, ok := record[key].(map[string]any); ok {
		return out
	}
	if out := stringKeyMap(record[key]); out != nil {
		return out
	}
	value := unwrapUnion(record[key])
	if value == nil {
		return nil
	}
	out, ok := value.(map[string]any)
	if !ok {
		return stringKeyMap(value)
	}
	return out
}

func stringKeyMap(value any) map[string]any {
	rv := reflect.ValueOf(value)
	if !rv.IsValid() || rv.Kind() != reflect.Map || rv.Type().Key().Kind() != reflect.String {
		return nil
	}
	out := make(map[string]any, rv.Len())
	iter := rv.MapRange()
	for iter.Next() {
		out[iter.Key().String()] = iter.Value().Interface()
	}
	return out
}

func requiredString(record map[string]any, key string) (string, error) {
	value := optionalStringValue(record, key)
	if strings.TrimSpace(value) == "" {
		return "", api.NewError(api.ErrMetadataInvalid, "Iceberg Avro record is missing required string", map[string]string{"field": key})
	}
	return value, nil
}

func optionalStringValue(record map[string]any, key string) string {
	value := unwrapUnion(record[key])
	if s, ok := value.(string); ok {
		return s
	}
	return ""
}

func optionalBoolValue(record map[string]any, key string) bool {
	value := unwrapUnion(record[key])
	if b, ok := value.(bool); ok {
		return b
	}
	return false
}

func optionalIntValue(record map[string]any, key string) int {
	if n, ok := optionalInt(record, key); ok {
		return n
	}
	return 0
}

func optionalInt(record map[string]any, key string) (int, bool) {
	return intFromAny(unwrapUnion(record[key]))
}

func optionalInt64Value(record map[string]any, key string) int64 {
	if n, ok := optionalInt64(record, key); ok {
		return n
	}
	return 0
}

func optionalInt64(record map[string]any, key string) (int64, bool) {
	return int64FromAny(unwrapUnion(record[key]))
}

func optionalBytesValue(record map[string]any, key string) []byte {
	return bytesFromAny(unwrapUnion(record[key]))
}

func unwrapUnion(value any) any {
	if value == nil {
		return nil
	}
	record, ok := value.(map[string]any)
	if !ok || len(record) != 1 {
		return value
	}
	for _, nested := range record {
		return nested
	}
	return nil
}

func anySlice(value any) []any {
	switch v := value.(type) {
	case []any:
		return v
	case nil:
		return nil
	default:
		return nil
	}
}

func intFromAny(value any) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int32:
		return int(v), true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	default:
		return 0, false
	}
}

func int64FromAny(value any) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case float64:
		return int64(v), true
	default:
		return 0, false
	}
}

func bytesFromAny(value any) []byte {
	switch v := value.(type) {
	case []byte:
		return append([]byte(nil), v...)
	case string:
		return []byte(v)
	default:
		return nil
	}
}
