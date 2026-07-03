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
	data, err := EncodeManifest(entries)
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
			ColumnSizes:     map[int]int64{1: 1024},
			ValueCounts:     map[int]int64{1: 100},
		},
	}}
	data, err := EncodeManifest(entries)
	require.NoError(t, err)

	dec, err := ocf.NewDecoder(bytes.NewReader(data))
	require.NoError(t, err)
	headerSchema := string(dec.Metadata()["avro.schema"])
	for _, want := range []string{
		`"field-id": 108`,
		`"logicalType": "map"`,
		`"field-id": 117`,
		`"field-id": 118`,
		`"field-id": 1000`,
	} {
		require.Contains(t, headerSchema, want)
	}
	require.False(t, strings.Contains(headerSchema, `"column_sizes"`+`: [`), "schema should be the Iceberg schema, not hamba's stripped rendering")
}

func TestWriteManifestRoundTripPreservesPositionDeleteMetadata(t *testing.T) {
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
	data, err := EncodeManifest(entries)
	require.NoError(t, err)
	got, err := ReadManifest(data)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, api.DataFileContentPositionDelete, got[0].DataFile.Content)
	require.Equal(t, "s3://warehouse/t/data/00001.parquet", got[0].DataFile.ReferencedDataFile)
	require.Equal(t, 3, got[0].DataFile.DeleteSchemaID)
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
	require.NoError(t, WriteManifestList(&buf, manifests))
	got, err := ReadManifestList(buf.Bytes())
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "s3://warehouse/t/metadata/m0.avro", got[0].Path)
	require.Equal(t, 1, got[0].AddedFilesCount)
	require.Equal(t, int64(100), got[0].AddedRowsCount)
	require.Equal(t, []byte{1}, got[0].Partitions[0].LowerBound)
}
