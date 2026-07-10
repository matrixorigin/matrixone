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
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

type ManifestWriteOptions struct {
	FormatVersion int
	Schema        api.Schema
	PartitionSpec api.PartitionSpec
	Content       api.ManifestContent
}

type ManifestListWriteOptions struct {
	FormatVersion    int
	SnapshotID       int64
	ParentSnapshotID *int64
	SequenceNumber   int64
}

func WriteManifestList(w io.Writer, manifests []api.ManifestFile, opts ManifestListWriteOptions) error {
	metadata, err := manifestListOCFMetadata(opts)
	if err != nil {
		return err
	}
	for _, manifest := range manifests {
		if strings.TrimSpace(manifest.Path) == "" {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg manifest list contains a manifest without a path", nil)
		}
		if manifest.Length < 0 {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg manifest list contains a manifest with negative length", map[string]string{
				"path": manifest.ManifestPathHash,
			})
		}
		if manifest.Content != "" && manifest.Content != api.ManifestContentData && manifest.Content != api.ManifestContentDeletes {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg manifest list contains an invalid manifest content type", map[string]string{
				"path":    manifest.ManifestPathHash,
				"content": string(manifest.Content),
			})
		}
	}
	enc, err := newIcebergOCFEncoder(manifestListWriterSchema, w, metadata)
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

func WriteManifest(w io.Writer, entries []api.ManifestEntry, opts ManifestWriteOptions) error {
	if err := validateManifestWriteOptions(opts, entries); err != nil {
		return err
	}
	fields, err := manifestPartitionFields(opts.Schema, opts.PartitionSpec)
	if err != nil {
		return err
	}
	schema := manifestWriterSchema(fields)
	manifestMetadata, err := manifestOCFMetadata(opts)
	if err != nil {
		return err
	}
	enc, err := newIcebergOCFEncoder(schema, w, manifestMetadata)
	if err != nil {
		return api.WrapError(api.ErrMetadataInvalid, "Iceberg manifest OCF encoder could not be created", nil, err)
	}
	for _, entry := range entries {
		record, recordErr := manifestEntryRecord(entry, fields)
		if recordErr != nil {
			_ = enc.Close()
			return recordErr
		}
		if err := enc.Encode(record); err != nil {
			_ = enc.Close()
			return api.WrapError(api.ErrMetadataInvalid, "Iceberg manifest OCF encode failed", map[string]string{"path": entry.DataFile.FilePathHash}, err)
		}
	}
	if err := enc.Close(); err != nil {
		return api.WrapError(api.ErrMetadataInvalid, "Iceberg manifest OCF close failed", nil, err)
	}
	return nil
}

func newIcebergOCFEncoder(schema string, w io.Writer, metadata ...map[string][]byte) (*ocf.Encoder, error) {
	opts := []ocf.EncoderFunc{ocf.WithSchemaMarshaler(func(avro.Schema) ([]byte, error) {
		return []byte(schema), nil
	})}
	if len(metadata) > 0 && len(metadata[0]) > 0 {
		opts = append(opts, ocf.WithMetadata(metadata[0]))
	}
	return ocf.NewEncoder(schema, w, opts...)
}

func EncodeManifestList(manifests []api.ManifestFile, opts ManifestListWriteOptions) ([]byte, error) {
	var buf bytes.Buffer
	if err := WriteManifestList(&buf, manifests, opts); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func manifestListOCFMetadata(opts ManifestListWriteOptions) (map[string][]byte, error) {
	if opts.FormatVersion != 2 {
		return nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg manifest list writer supports format version 2 only", map[string]string{
			"format_version": strconv.Itoa(opts.FormatVersion),
		})
	}
	if opts.SnapshotID <= 0 {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg manifest list writer requires a snapshot id", nil)
	}
	if opts.SequenceNumber < 0 {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg manifest list writer requires a non-negative sequence number", nil)
	}
	parentSnapshotID := "null"
	if opts.ParentSnapshotID != nil {
		parentSnapshotID = strconv.FormatInt(*opts.ParentSnapshotID, 10)
	}
	return map[string][]byte{
		"format-version":     []byte(strconv.Itoa(opts.FormatVersion)),
		"snapshot-id":        []byte(strconv.FormatInt(opts.SnapshotID, 10)),
		"parent-snapshot-id": []byte(parentSnapshotID),
		"sequence-number":    []byte(strconv.FormatInt(opts.SequenceNumber, 10)),
	}, nil
}

func EncodeManifest(entries []api.ManifestEntry, opts ManifestWriteOptions) ([]byte, error) {
	var buf bytes.Buffer
	if err := WriteManifest(&buf, entries, opts); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func validateManifestWriteOptions(opts ManifestWriteOptions, entries []api.ManifestEntry) error {
	if opts.FormatVersion != 2 {
		return api.NewError(api.ErrUnsupportedFeature, "Iceberg manifest writer supports format version 2 only", map[string]string{
			"format_version": strconv.Itoa(opts.FormatVersion),
		})
	}
	if len(opts.Schema.Fields) == 0 {
		return api.NewError(api.ErrConfigInvalid, "Iceberg manifest writer requires a table schema", nil)
	}
	if opts.Content != api.ManifestContentData && opts.Content != api.ManifestContentDeletes {
		return api.NewError(api.ErrConfigInvalid, "Iceberg manifest writer requires data or deletes content", nil)
	}
	for _, entry := range entries {
		if entry.Status < api.ManifestEntryExisting || entry.Status > api.ManifestEntryDeleted {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg manifest entry status is invalid", nil)
		}
		if strings.TrimSpace(entry.DataFile.FilePath) == "" {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg manifest entry requires a data file path", nil)
		}
		if entry.DataFile.RecordCount < 0 || entry.DataFile.FileSizeInBytes < 0 {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg manifest entry contains negative file metrics", map[string]string{
				"path": entry.DataFile.FilePathHash,
			})
		}
		if entry.DataFile.Content < api.DataFileContentData || entry.DataFile.Content > api.DataFileContentEqualityDelete {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg manifest entry file content is invalid", map[string]string{
				"path": entry.DataFile.FilePathHash,
			})
		}
		isDelete := entry.DataFile.Content != api.DataFileContentData
		if (opts.Content == api.ManifestContentDeletes) != isDelete {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg manifest content does not match data file content", map[string]string{
				"manifest_content": string(opts.Content),
				"path":             entry.DataFile.FilePathHash,
			})
		}
		if entry.DataFile.SpecID != opts.PartitionSpec.SpecID {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg manifest data file partition spec does not match manifest partition spec", map[string]string{
				"manifest_spec_id": strconv.Itoa(opts.PartitionSpec.SpecID),
				"file_spec_id":     strconv.Itoa(entry.DataFile.SpecID),
				"path":             entry.DataFile.FilePathHash,
			})
		}
	}
	return nil
}

func manifestOCFMetadata(opts ManifestWriteOptions) (map[string][]byte, error) {
	schemaJSON, err := json.Marshal(struct {
		Type               string            `json:"type"`
		SchemaID           int               `json:"schema-id"`
		Fields             []api.SchemaField `json:"fields"`
		IdentifierFieldIDs []int             `json:"identifier-field-ids,omitempty"`
	}{
		Type:               "struct",
		SchemaID:           opts.Schema.SchemaID,
		Fields:             opts.Schema.Fields,
		IdentifierFieldIDs: opts.Schema.IdentifierFieldIDs,
	})
	if err != nil {
		return nil, api.WrapError(api.ErrMetadataInvalid, "Iceberg manifest table schema could not be encoded", nil, err)
	}
	partitionSpecJSON, err := json.Marshal(opts.PartitionSpec.Fields)
	if err != nil {
		return nil, api.WrapError(api.ErrMetadataInvalid, "Iceberg manifest partition spec could not be encoded", nil, err)
	}
	return map[string][]byte{
		"schema":            schemaJSON,
		"schema-id":         []byte(strconv.Itoa(opts.Schema.SchemaID)),
		"partition-spec":    partitionSpecJSON,
		"partition-spec-id": []byte(strconv.Itoa(opts.PartitionSpec.SpecID)),
		"format-version":    []byte(strconv.Itoa(opts.FormatVersion)),
		"content":           []byte(opts.Content),
	}, nil
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
		"key_metadata":         optionalBytes(manifest.KeyMetadata),
	}
}

func manifestEntryRecord(entry api.ManifestEntry, fields []partitionField) (map[string]any, error) {
	dataFile, err := dataFileRecord(entry.DataFile, fields)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"status":               int32(entry.Status),
		"snapshot_id":          entry.SnapshotID,
		"sequence_number":      entry.SequenceNumber,
		"file_sequence_number": entry.FileSequence,
		"data_file":            dataFile,
	}, nil
}

func dataFileRecord(file api.DataFile, fields []partitionField) (map[string]any, error) {
	partition, err := normalizePartitionRecord(file.Partition, fields)
	if err != nil {
		return nil, api.WrapError(api.ErrMetadataInvalid, "Iceberg manifest partition tuple is invalid", map[string]string{"path": file.FilePathHash}, err)
	}
	return map[string]any{
		"content":              int32(file.Content),
		"file_path":            file.FilePath,
		"file_format":          strings.ToUpper(firstNonEmpty(file.FileFormat, "parquet")),
		"partition":            partition,
		"record_count":         file.RecordCount,
		"file_size_in_bytes":   file.FileSizeInBytes,
		"column_sizes":         intLongRecords(file.ColumnSizes),
		"value_counts":         intLongRecords(file.ValueCounts),
		"null_value_counts":    intLongRecords(file.NullValueCounts),
		"nan_value_counts":     intLongRecords(file.NaNValueCounts),
		"lower_bounds":         intBytesRecords(file.LowerBounds),
		"upper_bounds":         intBytesRecords(file.UpperBounds),
		"split_offsets":        file.SplitOffsets,
		"equality_ids":         optionalInt32Slice(file.EqualityIDs),
		"sort_order_id":        int32(file.SortOrderID),
		"referenced_data_file": optionalString(file.ReferencedDataFile),
		"key_metadata":         optionalBytes(file.KeyMetadata),
	}, nil
}

func partitionSummaryRecords(in []api.PartitionFieldSummary) []map[string]any {
	if len(in) == 0 {
		return nil
	}
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

func optionalInt32Slice(in []int) any {
	if len(in) == 0 {
		return nil
	}
	out := make([]int32, 0, len(in))
	for _, value := range in {
		out = append(out, int32(value))
	}
	return out
}

func optionalString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func optionalBytes(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func normalizePartitionRecord(in map[string]any, fields []partitionField) (map[string]any, error) {
	known := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		known[field.Name] = struct{}{}
	}
	for name := range in {
		if _, ok := known[name]; !ok {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg partition tuple contains a field outside the partition spec", map[string]string{
				"field": name,
			})
		}
	}
	out := make(map[string]any, len(fields))
	for _, field := range fields {
		value := in[field.Name]
		if value == nil {
			out[field.Name] = nil
			continue
		}
		normalized, err := normalizePartitionValue(value, field)
		if err != nil {
			return nil, err
		}
		out[field.Name] = normalized
	}
	return out, nil
}

func normalizePartitionValue(value any, field partitionField) (any, error) {
	switch field.Kind {
	case api.TypeBoolean:
		if v, ok := value.(bool); ok {
			return v, nil
		}
	case api.TypeInt, api.TypeDate:
		switch v := value.(type) {
		case int:
			if v >= math.MinInt32 && v <= math.MaxInt32 {
				return int32(v), nil
			}
		case int32:
			return v, nil
		case int64:
			if v >= math.MinInt32 && v <= math.MaxInt32 {
				return int32(v), nil
			}
		}
	case api.TypeLong, api.TypeTime, api.TypeTimestamp, api.TypeTimestampTZ:
		switch v := value.(type) {
		case int:
			return int64(v), nil
		case int32:
			return int64(v), nil
		case int64:
			return v, nil
		}
	case api.TypeFloat:
		if v, ok := value.(float32); ok {
			return v, nil
		}
	case api.TypeDouble:
		switch v := value.(type) {
		case float32:
			return float64(v), nil
		case float64:
			return v, nil
		}
	case api.TypeDecimal:
		switch v := value.(type) {
		case int64:
			denominator := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(field.Scale)), nil)
			return new(big.Rat).SetFrac(big.NewInt(v), denominator), nil
		case *big.Rat:
			return v, nil
		case big.Rat:
			return &v, nil
		}
	case api.TypeString:
		if v, ok := value.(string); ok {
			return v, nil
		}
	case api.TypeBinary:
		if v, ok := value.([]byte); ok {
			return v, nil
		}
	}
	return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg partition value does not match partition type", map[string]string{
		"field": field.Name,
		"type":  string(field.Kind),
	})
}

func manifestPartitionFields(schema api.Schema, spec api.PartitionSpec) ([]partitionField, error) {
	if len(spec.Fields) == 0 {
		return nil, nil
	}
	byID := make(map[int]api.SchemaField, len(schema.Fields))
	for _, field := range schema.Fields {
		byID[field.ID] = field
	}
	out := make([]partitionField, 0, len(spec.Fields))
	for _, part := range spec.Fields {
		source, ok := byID[part.SourceID]
		if !ok {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg partition source field is missing from schema", map[string]string{
				"field":     part.Name,
				"source_id": strconv.Itoa(part.SourceID),
			})
		}
		field, err := manifestPartitionField(part, source.Type)
		if err != nil {
			return nil, err
		}
		out = append(out, field)
	}
	return out, nil
}

func manifestPartitionField(part api.PartitionField, source api.IcebergType) (partitionField, error) {
	transform := strings.ToLower(strings.TrimSpace(part.Transform))
	kind := source.Kind
	switch {
	case transform == "year", transform == "month", transform == "day", transform == "hour", strings.HasPrefix(transform, "bucket["):
		kind = api.TypeInt
	case transform == "", transform == "identity", strings.HasPrefix(transform, "truncate["):
	default:
		return partitionField{}, api.NewError(api.ErrUnsupportedFeature, "Iceberg manifest partition transform is unsupported", map[string]string{
			"field":     part.Name,
			"transform": part.Transform,
		})
	}
	typeJSON, err := manifestPartitionAvroType(kind, source, part.FieldID)
	if err != nil {
		return partitionField{}, err
	}
	return partitionField{Name: part.Name, TypeJSON: typeJSON, ID: part.FieldID, Kind: kind, Scale: source.Scale}, nil
}

func manifestPartitionAvroType(kind api.IcebergTypeKind, source api.IcebergType, fieldID int) (string, error) {
	switch kind {
	case api.TypeBoolean:
		return `"boolean"`, nil
	case api.TypeInt, api.TypeDate:
		return `"int"`, nil
	case api.TypeLong, api.TypeTime, api.TypeTimestamp, api.TypeTimestampTZ:
		return `"long"`, nil
	case api.TypeFloat:
		return `"float"`, nil
	case api.TypeDouble:
		return `"double"`, nil
	case api.TypeDecimal:
		if source.Precision <= 0 || source.Scale < 0 || source.Scale > source.Precision {
			return "", api.NewError(api.ErrMetadataInvalid, "Iceberg decimal partition type is invalid", nil)
		}
		return fmt.Sprintf(`{"type":"bytes","logicalType":"decimal","precision":%d,"scale":%d}`, source.Precision, source.Scale), nil
	case api.TypeString:
		return `"string"`, nil
	case api.TypeBinary:
		return `"bytes"`, nil
	default:
		return "", api.NewError(api.ErrUnsupportedFeature, "Iceberg manifest partition type is unsupported", map[string]string{
			"field_id": strconv.Itoa(fieldID),
			"type":     source.String(),
		})
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
	Name     string
	TypeJSON string
	ID       int
	Kind     api.IcebergTypeKind
	Scale    int
}

const manifestListWriterSchema = `{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {"name": "manifest_path", "type": "string", "field-id": 500},
    {"name": "manifest_length", "type": "long", "field-id": 501},
    {"name": "partition_spec_id", "type": "int", "field-id": 502},
    {"name": "content", "type": "int", "default": 0, "field-id": 517},
    {"name": "sequence_number", "type": "long", "default": 0, "field-id": 515},
    {"name": "min_sequence_number", "type": "long", "default": 0, "field-id": 516},
    {"name": "added_snapshot_id", "type": "long", "field-id": 503},
    {"name": "added_files_count", "type": "int", "field-id": 504},
    {"name": "existing_files_count", "type": "int", "field-id": 505},
    {"name": "deleted_files_count", "type": "int", "field-id": 506},
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
    {"name": "added_rows_count", "type": "long", "field-id": 512},
    {"name": "existing_rows_count", "type": "long", "field-id": 513},
    {"name": "deleted_rows_count", "type": "long", "field-id": 514},
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
		partition.WriteString(`{"name":`)
		partition.WriteString(strconv.Quote(field.Name))
		partition.WriteString(`,"type":["null",`)
		partition.WriteString(field.TypeJSON)
		partition.WriteString(`],"default":null,"field-id":`)
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
        {"name": "content", "type": "int", "default": 0, "field-id": 134},
        {"name": "file_path", "type": "string", "field-id": 100},
        {"name": "file_format", "type": "string", "field-id": 101},
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
        {"name": "referenced_data_file", "type": ["null", "string"], "default": null, "field-id": 143}
      ]
    }, "field-id": 2}
  ]
}`
}
