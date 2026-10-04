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
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/hamba/avro/v2/ocf"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func TestWriteManifestRoundTrip(t *testing.T) {
	entries := []api.ManifestEntry{{
		Status:         api.ManifestEntryAdded,
		SnapshotID:     22,
		SequenceNumber: 9,
		FileSequence:   9,
		DataFile: api.DataFile{
			Content:          api.DataFileContentData,
			FilePath:         "s3://warehouse/t/data/00001.parquet",
			FileFormat:       "parquet",
			Partition:        map[string]any{"created_day": int32(19815)},
			RecordCount:      100,
			FileSizeInBytes:  2048,
			ColumnSizes:      map[int]int64{1: 1024},
			ValueCounts:      map[int]int64{1: 100},
			NullValueCounts:  map[int]int64{1: 0},
			LowerBounds:      map[int][]byte{1: {0}},
			UpperBounds:      map[int][]byte{1: {100}},
			SplitOffsets:     []int64{4, 1024},
			SortOrderID:      0,
			SpecID:           7,
			FilePathHash:     api.PathHash("s3://warehouse/t/data/00001.parquet"),
			FilePathRedacted: api.RedactPath("s3://warehouse/t/data/00001.parquet"),
		},
	}}
	data, err := EncodeManifest(entries, manifestTestOptions(api.ManifestContentData, api.SchemaField{
		ID: 10, Name: "created_at", Type: api.IcebergType{Kind: api.TypeDate},
	}, api.PartitionField{SourceID: 10, FieldID: 1000, Name: "created_day", Transform: "identity"}))
	require.NoError(t, err)
	got, err := ReadManifest(data)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, api.ManifestEntryAdded, got[0].Status)
	require.Equal(t, "parquet", got[0].DataFile.FileFormat)
	require.Equal(t, int64(100), got[0].DataFile.RecordCount)
	require.Equal(t, int64(1024), got[0].DataFile.ColumnSizes[1])
	require.Equal(t, 19815, got[0].DataFile.Partition["created_day"])
}

func TestEncodeManifestBoundedRejectsOversizedOutput(t *testing.T) {
	entries := []api.ManifestEntry{{
		Status:     api.ManifestEntryAdded,
		SnapshotID: 22,
		DataFile: api.DataFile{
			Content:         api.DataFileContentData,
			FilePath:        "s3://warehouse/t/data/00001.parquet",
			FileFormat:      "parquet",
			RecordCount:     1,
			FileSizeInBytes: 10,
			SpecID:          7,
		},
	}}
	opts := manifestTestOptions(api.ManifestContentData, api.SchemaField{
		ID: 10, Name: "created_at", Type: api.IcebergType{Kind: api.TypeDate},
	}, api.PartitionField{SourceID: 10, FieldID: 1000, Name: "created_day", Transform: "identity"})

	_, err := EncodeManifestBounded(entries, opts, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrPlanningLimitExceeded))

	unbounded, err := EncodeManifest(entries, opts)
	require.NoError(t, err)
	// The bound is a peak allowance, so leave room for old+new backing arrays
	// during deterministic buffer growth.
	peakLimit := int64(len(unbounded)) * 4
	bounded, err := EncodeManifestBounded(entries, opts, peakLimit)
	require.NoError(t, err)
	roundTrip, err := ReadManifest(bounded)
	require.NoError(t, err)
	require.Len(t, roundTrip, 1)
	require.Equal(t, entries[0].DataFile.FilePath, roundTrip[0].DataFile.FilePath)
	require.LessOrEqual(t, int64(cap(bounded)), peakLimit)
}

func TestEncodeManifestListBoundedRejectsOversizedOutput(t *testing.T) {
	manifests := []api.ManifestFile{{
		Path:            "s3://warehouse/t/metadata/manifest.avro",
		Length:          10,
		Content:         api.ManifestContentData,
		AddedSnapshotID: 22,
	}}
	opts := ManifestListWriteOptions{FormatVersion: 2, SnapshotID: 22, SequenceNumber: 1}

	_, err := EncodeManifestListBounded(manifests, opts, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrPlanningLimitExceeded))
}

func TestManifestWritersRejectAvroIntTruncation(t *testing.T) {
	_, err := EncodeManifestList([]api.ManifestFile{{
		Path:            "s3://warehouse/t/metadata/manifest.avro",
		Length:          10,
		PartitionSpecID: int(int64(math.MaxInt32) + 1),
		Content:         api.ManifestContentData,
	}}, ManifestListWriteOptions{FormatVersion: 2, SnapshotID: 22, SequenceNumber: 1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Avro int range")

	entries := []api.ManifestEntry{{
		Status:     api.ManifestEntryAdded,
		SnapshotID: 22,
		DataFile: api.DataFile{
			Content:         api.DataFileContentEqualityDelete,
			FilePath:        "s3://warehouse/t/delete/eq.parquet",
			FileFormat:      "parquet",
			RecordCount:     1,
			FileSizeInBytes: 10,
			SpecID:          7,
			EqualityIDs:     []int{int(int64(math.MaxInt32) + 1)},
		},
	}}
	_, err = EncodeManifest(entries, manifestTestOptions(api.ManifestContentDeletes, api.SchemaField{
		ID: 10, Name: "created_at", Type: api.IcebergType{Kind: api.TypeDate},
	}, api.PartitionField{SourceID: 10, FieldID: 1000, Name: "created_day", Transform: "identity"}))
	require.Error(t, err)
	require.Contains(t, err.Error(), "Avro int range")
}

func TestWriteManifestV2AddedEntryUsesInheritedSequenceNumbers(t *testing.T) {
	data, err := EncodeManifest([]api.ManifestEntry{{
		Status:         api.ManifestEntryAdded,
		SnapshotID:     22,
		SequenceNumber: 9,
		FileSequence:   9,
		DataFile: api.DataFile{
			FilePath:        "s3://warehouse/t/data/00001.parquet",
			FileFormat:      "parquet",
			RecordCount:     1,
			FileSizeInBytes: 10,
			SpecID:          7,
		},
	}}, manifestTestOptions(api.ManifestContentData, api.SchemaField{
		ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeLong},
	}))
	require.NoError(t, err)

	dec, err := ocf.NewDecoder(bytes.NewReader(data))
	require.NoError(t, err)
	defer dec.Close()
	require.True(t, dec.HasNext())
	var raw any
	require.NoError(t, dec.Decode(&raw))
	record := raw.(map[string]any)
	require.Nil(t, unwrapUnion(record["sequence_number"]))
	require.Nil(t, unwrapUnion(record["file_sequence_number"]))
}

func TestWriteManifestOCFHeaderPreservesIcebergSchemaProperties(t *testing.T) {
	entries := []api.ManifestEntry{{
		Status:         api.ManifestEntryAdded,
		SnapshotID:     22,
		SequenceNumber: 9,
		FileSequence:   9,
		DataFile: api.DataFile{
			Content:    api.DataFileContentData,
			FilePath:   "s3://warehouse/t/data/00001.parquet",
			FileFormat: "parquet",
			Partition:  map[string]any{"region": "ksa"},
			PartitionFieldIDs: map[string]int{
				"region": 1000,
			},
			RecordCount:     100,
			FileSizeInBytes: 2048,
			SpecID:          7,
			ColumnSizes:     map[int]int64{1: 1024},
			ValueCounts:     map[int]int64{1: 100},
		},
	}}
	data, err := EncodeManifest(entries, manifestTestOptions(api.ManifestContentData, api.SchemaField{
		ID: 11, Name: "region", Type: api.IcebergType{Kind: api.TypeString},
	}, api.PartitionField{SourceID: 11, FieldID: 1000, Name: "region", Transform: "identity"}))
	require.NoError(t, err)

	dec, err := ocf.NewDecoder(bytes.NewReader(data))
	require.NoError(t, err)
	metadata := dec.Metadata()
	headerSchema := string(metadata["avro.schema"])
	for _, want := range []string{
		`"field-id": 108`,
		`"logicalType": "map"`,
		`"field-id": 117`,
		`"field-id": 118`,
		`"field-id":1000`,
	} {
		require.Contains(t, headerSchema, want)
	}
	require.False(t, strings.Contains(headerSchema, `"column_sizes"`+`: [`), "schema should be the Iceberg schema, not hamba's stripped rendering")
	for key, want := range map[string]string{
		"schema-id":         "1",
		"partition-spec-id": "7",
		"format-version":    "2",
		"content":           "data",
	} {
		require.Equal(t, want, string(metadata[key]), key)
	}
	require.JSONEq(t, `{"type":"struct","schema-id":1,"fields":[{"id":11,"name":"region","required":false,"type":"string"}]}`, string(metadata["schema"]))
	require.JSONEq(t, `[{"source-id":11,"field-id":1000,"name":"region","transform":"identity"}]`, string(metadata["partition-spec"]))
}

func TestWriteManifestRoundTripPreservesStandardPositionDeleteMetadata(t *testing.T) {
	entries := []api.ManifestEntry{{
		Status:         api.ManifestEntryAdded,
		SnapshotID:     22,
		SequenceNumber: 9,
		FileSequence:   9,
		DataFile: api.DataFile{
			Content:            api.DataFileContentPositionDelete,
			FilePath:           "s3://warehouse/t/delete/pos.parquet",
			FileFormat:         "parquet",
			RecordCount:        1,
			FileSizeInBytes:    128,
			ReferencedDataFile: "s3://warehouse/t/data/00001.parquet",
			DeleteSchemaID:     3,
			SpecID:             7,
		},
	}}
	data, err := EncodeManifest(entries, manifestTestOptions(api.ManifestContentDeletes, api.SchemaField{
		ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeLong},
	}))
	require.NoError(t, err)
	decoder, err := ocf.NewDecoder(bytes.NewReader(data))
	require.NoError(t, err)
	writerSchema := string(decoder.Metadata()["avro.schema"])
	require.Contains(t, writerSchema, `"referenced_data_file"`)
	require.Contains(t, writerSchema, `"field-id": 143`)
	require.NotContains(t, writerSchema, `"spec_id"`)
	require.NotContains(t, writerSchema, `"delete_schema_id"`)
	require.NotContains(t, writerSchema, `"encryption_key_metadata"`)
	got, err := ReadManifest(data)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, api.DataFileContentPositionDelete, got[0].DataFile.Content)
	require.Equal(t, "s3://warehouse/t/data/00001.parquet", got[0].DataFile.ReferencedDataFile)
	require.Zero(t, got[0].DataFile.DeleteSchemaID, "delete schema id is internal state and is not an Iceberg v2 manifest field")
}

func TestWriteManifestPreservesNullStringPartitionTypeAndValue(t *testing.T) {
	entries := []api.ManifestEntry{{
		Status:         api.ManifestEntryAdded,
		SnapshotID:     22,
		SequenceNumber: 9,
		FileSequence:   9,
		DataFile: api.DataFile{
			Content:           api.DataFileContentData,
			FilePath:          "s3://warehouse/t/data/region=null/00001.parquet",
			FileFormat:        "parquet",
			Partition:         map[string]any{"region": nil},
			RecordCount:       1,
			FileSizeInBytes:   100,
			SpecID:            7,
			FilePathHash:      "file-hash",
			PartitionFieldIDs: map[string]int{"region": 1000},
		},
	}}
	data, err := EncodeManifest(entries, manifestTestOptions(api.ManifestContentData, api.SchemaField{
		ID: 11, Name: "region", Type: api.IcebergType{Kind: api.TypeString},
	}, api.PartitionField{SourceID: 11, FieldID: 1000, Name: "region", Transform: "identity"}))
	require.NoError(t, err)

	dec, err := ocf.NewDecoder(bytes.NewReader(data))
	require.NoError(t, err)
	require.Contains(t, string(dec.Metadata()["avro.schema"]), `"name":"region","type":["null","string"]`)
	got, err := ReadManifest(data)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Contains(t, got[0].DataFile.Partition, "region")
	require.Nil(t, got[0].DataFile.Partition["region"])
}

func TestWriteManifestRejectsPartitionFieldsOutsideSpec(t *testing.T) {
	entries := []api.ManifestEntry{{
		Status: api.ManifestEntryAdded,
		DataFile: api.DataFile{
			Content:    api.DataFileContentData,
			FilePath:   "s3://warehouse/t/data/part.parquet",
			FileFormat: "parquet",
			Partition:  map[string]any{"unexpected": "ksa"},
			SpecID:     7,
		},
	}}
	_, err := EncodeManifest(entries, manifestTestOptions(api.ManifestContentData, api.SchemaField{
		ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeLong},
	}))
	require.Error(t, err)
	require.Contains(t, err.Error(), "partition tuple is invalid")
}

func TestWriteManifestSupportsHighScaleDecimalPartition(t *testing.T) {
	entries := []api.ManifestEntry{{
		Status: api.ManifestEntryAdded,
		DataFile: api.DataFile{
			Content:    api.DataFileContentData,
			FilePath:   "s3://warehouse/t/data/price=0.00000000000000000001/part.parquet",
			FileFormat: "parquet",
			Partition:  map[string]any{"price": int64(1)},
			SpecID:     7,
		},
	}}
	data, err := EncodeManifest(entries, manifestTestOptions(api.ManifestContentData, api.SchemaField{
		ID: 2, Name: "price", Type: api.IcebergType{Kind: api.TypeDecimal, Precision: 38, Scale: 20},
	}, api.PartitionField{SourceID: 2, FieldID: 1000, Name: "price", Transform: "identity"}))
	require.NoError(t, err)
	require.NotEmpty(t, data)
}

func manifestTestOptions(content api.ManifestContent, schemaField api.SchemaField, partitionFields ...api.PartitionField) ManifestWriteOptions {
	return ManifestWriteOptions{
		FormatVersion: 2,
		Schema: api.Schema{
			SchemaID: 1,
			Fields:   []api.SchemaField{schemaField},
		},
		PartitionSpec: api.PartitionSpec{SpecID: 7, Fields: partitionFields},
		Content:       content,
	}
}

func TestWriteManifestListRoundTrip(t *testing.T) {
	manifests := []api.ManifestFile{{
		Path:                  "s3://warehouse/t/metadata/m0.avro",
		Length:                1234,
		PartitionSpecID:       7,
		Content:               api.ManifestContentData,
		SequenceNumber:        9,
		AddedSnapshotID:       22,
		AddedFilesCount:       1,
		AddedRowsCount:        100,
		AddedFilesSizeInBytes: 2048,
		Partitions: []api.PartitionFieldSummary{{
			LowerBound: []byte{1},
			UpperBound: []byte{9},
		}},
		ManifestPathHash:     api.PathHash("s3://warehouse/t/metadata/m0.avro"),
		ManifestPathRedacted: api.RedactPath("s3://warehouse/t/metadata/m0.avro"),
	}}
	var buf bytes.Buffer
	parentSnapshotID := int64(21)
	require.NoError(t, WriteManifestList(&buf, manifests, ManifestListWriteOptions{
		FormatVersion:    2,
		SnapshotID:       22,
		ParentSnapshotID: &parentSnapshotID,
		SequenceNumber:   9,
	}))
	decoder, err := ocf.NewDecoder(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.Equal(t, []byte("2"), decoder.Metadata()["format-version"])
	require.Equal(t, []byte("22"), decoder.Metadata()["snapshot-id"])
	require.Equal(t, []byte("21"), decoder.Metadata()["parent-snapshot-id"])
	require.Equal(t, []byte("9"), decoder.Metadata()["sequence-number"])
	got, err := ReadManifestList(buf.Bytes())
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "s3://warehouse/t/metadata/m0.avro", got[0].Path)
	require.Equal(t, 1, got[0].AddedFilesCount)
	require.Equal(t, int64(100), got[0].AddedRowsCount)
	require.Equal(t, []byte{1}, got[0].Partitions[0].LowerBound)
}

func TestValidateManifestWriteOptionsRejectsInvalidInputs(t *testing.T) {
	validOptions := ManifestWriteOptions{
		FormatVersion: 2,
		Schema: api.Schema{SchemaID: 1, Fields: []api.SchemaField{{
			ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeLong},
		}}},
		PartitionSpec: api.PartitionSpec{SpecID: 0},
		Content:       api.ManifestContentData,
	}
	validEntry := api.ManifestEntry{Status: api.ManifestEntryAdded, SnapshotID: 1, DataFile: api.DataFile{
		Content: api.DataFileContentData, FilePath: "s3://warehouse/t/data.parquet",
		RecordCount: 1, FileSizeInBytes: 1,
	}}

	tests := []struct {
		name   string
		mutate func(*ManifestWriteOptions, *api.ManifestEntry)
	}{
		{"format version", func(o *ManifestWriteOptions, _ *api.ManifestEntry) { o.FormatVersion = 1 }},
		{"empty schema", func(o *ManifestWriteOptions, _ *api.ManifestEntry) { o.Schema.Fields = nil }},
		{"content", func(o *ManifestWriteOptions, _ *api.ManifestEntry) { o.Content = "invalid" }},
		{"schema id", func(o *ManifestWriteOptions, _ *api.ManifestEntry) { o.Schema.SchemaID = -1 }},
		{"field id", func(o *ManifestWriteOptions, _ *api.ManifestEntry) { o.Schema.Fields[0].ID = 0 }},
		{"identifier id", func(o *ManifestWriteOptions, _ *api.ManifestEntry) { o.Schema.IdentifierFieldIDs = []int{0} }},
		{"partition id", func(o *ManifestWriteOptions, _ *api.ManifestEntry) {
			o.PartitionSpec.Fields = []api.PartitionField{{SourceID: 0, FieldID: 100, Name: "part"}}
		}},
		{"entry status", func(_ *ManifestWriteOptions, e *api.ManifestEntry) { e.Status = -1 }},
		{"empty path", func(_ *ManifestWriteOptions, e *api.ManifestEntry) { e.DataFile.FilePath = "" }},
		{"negative metric", func(_ *ManifestWriteOptions, e *api.ManifestEntry) { e.DataFile.RecordCount = -1 }},
		{"data id", func(_ *ManifestWriteOptions, e *api.ManifestEntry) { e.DataFile.SortOrderID = -1 }},
		{"file content", func(_ *ManifestWriteOptions, e *api.ManifestEntry) { e.DataFile.Content = 3 }},
		{"content mismatch", func(_ *ManifestWriteOptions, e *api.ManifestEntry) {
			e.DataFile.Content = api.DataFileContentPositionDelete
		}},
		{"spec mismatch", func(_ *ManifestWriteOptions, e *api.ManifestEntry) { e.DataFile.SpecID = 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := validOptions
			opts.Schema.Fields = append([]api.SchemaField(nil), validOptions.Schema.Fields...)
			entry := validEntry
			test.mutate(&opts, &entry)
			require.Error(t, validateManifestWriteOptions(opts, []api.ManifestEntry{entry}))
		})
	}
	require.NoError(t, validateManifestWriteOptions(validOptions, []api.ManifestEntry{validEntry}))
}

func TestManifestWriterValidationEdges(t *testing.T) {
	firstRowID := int64(1)
	validManifest := api.ManifestFile{Path: "s3://warehouse/t/metadata/m.avro", Content: api.ManifestContentData, FirstRowID: &firstRowID}
	require.NoError(t, validateManifestFileForWrite(validManifest))
	for _, manifest := range []api.ManifestFile{
		{},
		{Path: validManifest.Path, Length: -1},
		{Path: validManifest.Path, PartitionSpecID: -1},
		{Path: validManifest.Path, FirstRowID: func() *int64 { v := int64(-1); return &v }()},
		{Path: validManifest.Path, Content: "invalid"},
	} {
		require.Error(t, validateManifestFileForWrite(manifest))
	}

	validFile := api.DataFile{SpecID: 0, SortOrderID: 0, DeleteSchemaID: 0}
	require.NoError(t, validateDataFileIntRanges(validFile))
	invalidFiles := []api.DataFile{
		{SpecID: -1},
		{EqualityIDs: []int{0}},
		{ColumnSizes: map[int]int64{0: 1}},
		{ValueCounts: map[int]int64{1: -1}},
		{LowerBounds: map[int][]byte{0: {1}}},
		{SplitOffsets: []int64{-1}},
	}
	for _, file := range invalidFiles {
		require.Error(t, validateDataFileIntRanges(file))
	}
}

func TestValidateNestedIcebergTypeRanges(t *testing.T) {
	primitive := api.IcebergType{Kind: api.TypeLong}
	require.NoError(t, validateIcebergTypeIntRanges(api.IcebergType{
		Kind: api.TypeStruct, Fields: []api.SchemaField{{ID: 1, Name: "nested", Type: primitive}},
	}, "field"))
	require.Error(t, validateIcebergTypeIntRanges(api.IcebergType{
		Kind: api.TypeStruct, Fields: []api.SchemaField{{ID: 0, Name: "nested", Type: primitive}},
	}, "field"))
	require.Error(t, validateIcebergTypeIntRanges(api.IcebergType{Kind: api.TypeList, ElementID: 0, Element: &primitive}, "field"))
	invalidNested := api.IcebergType{Kind: api.TypeStruct, Fields: []api.SchemaField{{ID: 0, Name: "bad", Type: primitive}}}
	require.Error(t, validateIcebergTypeIntRanges(api.IcebergType{Kind: api.TypeList, ElementID: 2, Element: &invalidNested}, "field"))
	require.Error(t, validateIcebergTypeIntRanges(api.IcebergType{Kind: api.TypeMap, KeyID: 1, Key: &primitive}, "field"))
	require.Error(t, validateIcebergTypeIntRanges(api.IcebergType{
		Kind: api.TypeMap, KeyID: 1, Key: &invalidNested, ValueID: 2, Value: &primitive,
	}, "field"))
	require.Error(t, validateIcebergTypeIntRanges(api.IcebergType{
		Kind: api.TypeMap, KeyID: 1, Key: &primitive, ValueID: 2, Value: &invalidNested,
	}, "field"))
	require.NoError(t, validateIcebergTypeIntRanges(api.IcebergType{
		Kind: api.TypeMap, KeyID: 1, Key: &primitive, ValueID: 2, Value: &primitive,
	}, "field"))
}

func TestNormalizeManifestPartitionValues(t *testing.T) {
	tests := []struct {
		kind  api.IcebergTypeKind
		scale int
		value any
	}{
		{api.TypeBoolean, 0, true},
		{api.TypeInt, 0, int(1)},
		{api.TypeDate, 0, int64(2)},
		{api.TypeLong, 0, int32(3)},
		{api.TypeTime, 0, int(4)},
		{api.TypeTimestamp, 0, int64(5)},
		{api.TypeFloat, 0, float32(1.5)},
		{api.TypeDouble, 0, float32(2.5)},
		{api.TypeDouble, 0, float64(3.5)},
		{api.TypeDecimal, 2, int64(123)},
		{api.TypeDecimal, 2, big.NewRat(123, 100)},
		{api.TypeDecimal, 2, *big.NewRat(123, 100)},
		{api.TypeString, 0, "value"},
		{api.TypeBinary, 0, []byte{1}},
	}
	for _, test := range tests {
		_, err := normalizePartitionValue(test.value, partitionField{Name: "part", Kind: test.kind, Scale: test.scale})
		require.NoError(t, err, "kind=%s value=%T", test.kind, test.value)
	}
	_, err := normalizePartitionValue(int64(math.MaxInt32)+1, partitionField{Name: "part", Kind: api.TypeInt})
	require.Error(t, err)
	_, err = normalizePartitionValue("true", partitionField{Name: "part", Kind: api.TypeBoolean})
	require.Error(t, err)
}

func TestManifestPartitionTypeValidation(t *testing.T) {
	primitiveKinds := []api.IcebergTypeKind{
		api.TypeBoolean, api.TypeInt, api.TypeDate, api.TypeLong, api.TypeTime,
		api.TypeTimestamp, api.TypeTimestampTZ, api.TypeFloat, api.TypeDouble,
		api.TypeString, api.TypeBinary,
	}
	for _, kind := range primitiveKinds {
		_, err := manifestPartitionAvroType(kind, api.IcebergType{Kind: kind}, 1)
		require.NoError(t, err, "kind=%s", kind)
	}
	_, err := manifestPartitionAvroType(api.TypeDecimal, api.IcebergType{Kind: api.TypeDecimal, Precision: 10, Scale: 2}, 1)
	require.NoError(t, err)
	_, err = manifestPartitionAvroType(api.TypeDecimal, api.IcebergType{Kind: api.TypeDecimal, Precision: 0}, 1)
	require.Error(t, err)
	_, err = manifestPartitionAvroType(api.TypeUUID, api.IcebergType{Kind: api.TypeUUID}, 1)
	require.Error(t, err)

	_, err = manifestPartitionField(api.PartitionField{Name: "part", FieldID: 100, Transform: "void"}, api.IcebergType{Kind: api.TypeLong})
	require.Error(t, err)
	field, err := manifestPartitionField(api.PartitionField{Name: "part", FieldID: 100, Transform: "bucket[16]"}, api.IcebergType{Kind: api.TypeLong})
	require.NoError(t, err)
	require.Equal(t, api.TypeInt, field.Kind)
	_, err = manifestPartitionFields(api.Schema{}, api.PartitionSpec{Fields: []api.PartitionField{{Name: "part", SourceID: 1}}})
	require.Error(t, err)
	require.Equal(t, "value", firstNonEmpty("", " value ", "ignored"))
	require.Empty(t, firstNonEmpty("", "  "))
}

func TestManifestRecordEncodingHelpers(t *testing.T) {
	require.Nil(t, optionalInt32Slice(nil))
	require.Equal(t, []int32{1, 2}, optionalInt32Slice([]int{1, 2}))
	require.Nil(t, optionalString("  "))
	require.Equal(t, "value", optionalString("value"))
	require.Nil(t, optionalBytes(nil))
	require.Equal(t, []byte{1}, optionalBytes([]byte{1}))
	require.Nil(t, partitionSummaryRecords(nil))
	require.Len(t, partitionSummaryRecords([]api.PartitionFieldSummary{{ContainsNull: true}}), 1)

	fields := []partitionField{{Name: "part", Kind: api.TypeInt}}
	record, err := normalizePartitionRecord(nil, fields)
	require.NoError(t, err)
	require.Contains(t, record, "part")
	require.Nil(t, record["part"])
	_, err = normalizePartitionRecord(map[string]any{"unknown": 1}, fields)
	require.Error(t, err)
	_, err = normalizePartitionRecord(map[string]any{"part": "invalid"}, fields)
	require.Error(t, err)

	dataManifest := manifestFileRecord(api.ManifestFile{Content: api.ManifestContentData})
	deleteManifest := manifestFileRecord(api.ManifestFile{Content: api.ManifestContentDeletes})
	require.Equal(t, int32(0), dataManifest["content"])
	require.Equal(t, int32(1), deleteManifest["content"])
}
