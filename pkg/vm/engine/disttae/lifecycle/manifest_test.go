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

package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestArchiveManifestV1GoldenBytesAndRoundTrip(t *testing.T) {
	manifest := archiveManifestV1GoldenFixture()
	encoded, _, err := MarshalArchiveManifest(manifest)
	require.NoError(t, err)
	const golden = `{"manifest_format_version":1,"hash_formula_version":1,"canonical_encoder_version":1,"root_id":"root-1","attempt_id":"attempt-1","schema":{"format_version":1,"source_table_id":"9007199254740993","source_table_version":7,"source_database_name":"db","source_table_name":"events","columns":[{"ordinal":0,"source_column_id":"9007199254740995","name":"id","type_id":28,"width":0,"scale":0,"not_null":true,"auto_increment":false}]},"schema_digest":"01ab000000000000000000000000000000000000000000000000000000000000","content_hash":"02cd000000000000000000000000000000000000000000000000000000000000","row_count":"9007199254740997","logical_bytes":"9007199254740999","total_chunk_count":"1","files":[{"file_ordinal":0,"key":"archive/root-1/attempt-1/payload-000000-write.parquet","size":"134217728","sha256":"03ef000000000000000000000000000000000000000000000000000000000000","chunks":[{"chunk_ordinal":"0","file_ordinal":0,"row_group_ordinal":0,"row_count":"9007199254740997","logical_bytes":"9007199254740999","canonical_content_hash":"04aa000000000000000000000000000000000000000000000000000000000000"}]}],"verification_status":"FULL_READBACK_VERIFIED"}`
	require.Equal(t, golden, string(encoded))

	decoded, err := ParseArchiveManifest(encoded)
	require.NoError(t, err)
	require.Equal(t, manifest, decoded)
}

func TestArchiveManifestV1RejectsNonCanonicalAndAmbiguousJSON(t *testing.T) {
	encoded, _, err := MarshalArchiveManifest(archiveManifestV1GoldenFixture())
	require.NoError(t, err)
	canonical := string(encoded)
	tests := []struct {
		name    string
		encoded string
	}{
		{
			name: "unknown-version",
			encoded: strings.Replace(
				canonical,
				`"manifest_format_version":1`,
				`"manifest_format_version":2`,
				1,
			),
		},
		{
			name: "unknown-field",
			encoded: strings.Replace(
				canonical,
				`"verification_status":`,
				`"unknown":1,"verification_status":`,
				1,
			),
		},
		{
			name: "duplicate-field",
			encoded: strings.Replace(
				canonical,
				`"manifest_format_version":1`,
				`"manifest_format_version":1,"manifest_format_version":1`,
				1,
			),
		},
		{name: "trailing-json", encoded: canonical + `{}`},
		{name: "non-canonical-whitespace", encoded: " " + canonical},
		{
			name: "uint64-json-number",
			encoded: strings.Replace(
				canonical,
				`"row_count":"9007199254740997"`,
				`"row_count":9007199254740997`,
				1,
			),
		},
		{
			name: "uint64-leading-zero",
			encoded: strings.Replace(
				canonical,
				`"row_count":"9007199254740997"`,
				`"row_count":"09007199254740997"`,
				1,
			),
		},
		{
			name: "digest-not-lower-case",
			encoded: strings.Replace(
				canonical,
				`"schema_digest":"01ab`,
				`"schema_digest":"01AB`,
				1,
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseArchiveManifest([]byte(test.encoded))
			require.Error(t, err)
		})
	}
}

func TestArchiveManifestV1RejectsBoundsBeforeUse(t *testing.T) {
	_, err := ParseArchiveManifest(make([]byte, maxArchiveManifestBytes+1))
	require.ErrorContains(t, err, "certified range")

	manifest := archiveManifestV1GoldenFixture()
	manifest.RootID = strings.Repeat("r", maxArchiveIdentityBytes)
	_, _, err = MarshalArchiveManifest(manifest)
	require.NoError(t, err)
	manifest.RootID += "r"
	_, _, err = MarshalArchiveManifest(manifest)
	require.ErrorContains(t, err, "identity")
	manifest.RootID = string([]byte{0xff})
	_, _, err = MarshalArchiveManifest(manifest)
	require.ErrorContains(t, err, "identity")

	manifest = archiveManifestV1GoldenFixture()
	manifest.Files[0].Key = strings.Repeat("k", maxArchiveObjectKeyBytes)
	_, _, err = MarshalArchiveManifest(manifest)
	require.NoError(t, err)
	manifest.Files[0].Key += "k"
	_, _, err = MarshalArchiveManifest(manifest)
	require.ErrorContains(t, err, "file identity")

	manifest = archiveManifestV1GoldenFixture()
	manifest.Files[0].Size = maxArchivePayloadPhysicalBytes + 1
	_, _, err = MarshalArchiveManifest(manifest)
	require.ErrorContains(t, err, "file identity")

	manifest = archiveManifestV1GoldenFixture()
	manifest.Schema.Columns[0].DefaultExpression = strings.Repeat(
		"d",
		maxArchiveManifestString,
	)
	encoded, _, err := MarshalArchiveManifest(manifest)
	require.NoError(t, err)
	_, err = ParseArchiveManifest(encoded)
	require.NoError(t, err)
	manifest.Schema.Columns[0].DefaultExpression += "d"
	_, _, err = MarshalArchiveManifest(manifest)
	require.ErrorContains(t, err, "metadata exceeds")

	manifest = archiveManifestV1GoldenFixture()
	manifest.Schema.Columns = make([]SchemaColumn, maxArchiveSchemaColumns)
	for index := range manifest.Schema.Columns {
		manifest.Schema.Columns[index] = SchemaColumn{
			Ordinal:        uint32(index),
			SourceColumnID: uint64(index),
			Name:           "c" + strconv.Itoa(index),
			TypeID:         int32(types.T_uint64),
		}
	}
	encoded, _, err = MarshalArchiveManifest(manifest)
	require.NoError(t, err)
	_, err = ParseArchiveManifest(encoded)
	require.NoError(t, err)
	manifest.Schema.Columns = append(manifest.Schema.Columns, SchemaColumn{})
	_, _, err = MarshalArchiveManifest(manifest)
	require.ErrorContains(t, err, "column count")

	manifest = archiveManifestV1GoldenFixture()
	manifest.Schema.Columns[0].SourceColumnID = 0
	_, _, err = MarshalArchiveManifest(manifest)
	require.NoError(t, err, "MO source column ID zero is valid lineage")

	manifest = archiveManifestV1GoldenFixture()
	manifest.Schema.Columns[0].EnumValues = "read,write"
	_, _, err = MarshalArchiveManifest(manifest)
	require.ErrorContains(t, err, "unsupported encoded SQL type")
}

func TestArchiveManifestV1AcceptsFileCapAndRejectsExtraChunk(t *testing.T) {
	manifest := archiveManifestV1GoldenFixture()
	manifest.Files = make([]ArchiveFile, maxArchiveChunksPerDataset)
	manifest.RowCount = maxArchiveChunksPerDataset
	manifest.LogicalBytes = maxArchiveChunksPerDataset
	manifest.TotalChunkCount = maxArchiveChunksPerDataset
	for index := range manifest.Files {
		manifest.Files[index] = ArchiveFile{
			FileOrdinal: uint32(index),
			Key:         "payload-" + strconv.Itoa(index),
			Size:        1,
			Chunks: []ArchiveChunk{{
				ChunkOrdinal:    uint64(index),
				FileOrdinal:     uint32(index),
				RowGroupOrdinal: 0,
				RowCount:        1,
				LogicalBytes:    1,
			}},
		}
	}
	encoded, _, err := MarshalArchiveManifest(manifest)
	require.NoError(t, err)
	_, err = ParseArchiveManifest(encoded)
	require.NoError(t, err)

	manifest = archiveManifestV1GoldenFixture()
	manifest.Files[0].Chunks = append(
		manifest.Files[0].Chunks,
		manifest.Files[0].Chunks[0],
	)
	_, _, err = MarshalArchiveManifest(manifest)
	require.ErrorContains(t, err, "exactly one row group")
}

func TestArchiveManifestV1RejectsArrayPreflightOverflow(t *testing.T) {
	encoded := `{"manifest_format_version":1,"files":[` +
		strings.Repeat(`{},`, maxArchiveJSONArrayElements) + `{}` +
		`]}`
	_, err := ParseArchiveManifest([]byte(encoded))
	require.ErrorContains(t, err, "array exceeds element limit")
}

func TestReadArchiveManifestChecksStatBeforeBodyRead(t *testing.T) {
	store := &oversizeManifestReadStore{
		statSize: int64(maxArchiveManifestBytes + 1),
	}
	_, err := ReadArchiveManifest(context.Background(), store, "manifest.json")
	require.ErrorContains(t, err, "certified range")
	require.Zero(t, store.exactReads)
}

func TestArchiveDatasetHashV1ByteContract(t *testing.T) {
	schemaDigest := [sha256.Size]byte{1, 2, 3, 4}
	chunks := []ArchiveChunk{
		{
			ChunkOrdinal:         0,
			RowCount:             17,
			LogicalBytes:         101,
			CanonicalContentHash: [sha256.Size]byte{0xaa},
		},
		{
			ChunkOrdinal:         1,
			RowCount:             23,
			LogicalBytes:         202,
			CanonicalContentHash: [sha256.Size]byte{0xbb},
		},
	}

	sum := sha256.New()
	_, _ = sum.Write([]byte("matrixone/lifecycle/archive-dataset/v1"))
	writeManifestTestUint16(sum, 1)
	_, _ = sum.Write(schemaDigest[:])
	writeManifestTestUint64(sum, 2)
	for _, chunk := range chunks {
		writeManifestTestUint64(sum, chunk.ChunkOrdinal)
		writeManifestTestUint64(sum, chunk.RowCount)
		writeManifestTestUint64(sum, chunk.LogicalBytes)
		_, _ = sum.Write(chunk.CanonicalContentHash[:])
	}
	var expected [sha256.Size]byte
	copy(expected[:], sum.Sum(nil))

	require.Equal(t, expected, computeArchiveDatasetHash(schemaDigest, chunks))
}

func TestArchiveManifestRejectsDatasetChunkCountAboveCertifiedLimit(t *testing.T) {
	files := make([]ArchiveFile, maxArchiveChunksPerDataset+1)
	for index := range files {
		files[index] = ArchiveFile{
			FileOrdinal: uint32(index),
			Chunks: []ArchiveChunk{{
				ChunkOrdinal:    uint64(index),
				FileOrdinal:     uint32(index),
				RowGroupOrdinal: 0,
			}},
		}
	}
	manifest := archiveManifestV1GoldenFixture()
	manifest.TotalChunkCount = uint64(len(files))
	manifest.Files = files
	err := validateArchiveManifestShape(manifest)
	require.ErrorContains(t, err, "certified chunk limit")
}

func archiveManifestV1GoldenFixture() *ArchiveManifest {
	return &ArchiveManifest{
		ManifestFormatVersion: archiveManifestFormatVersion,
		HashFormulaVersion:    archiveHashFormulaVersion,
		CanonicalEncoder:      canonicalEncoderVersion,
		RootID:                "root-1",
		AttemptID:             "attempt-1",
		Schema: SchemaDescriptor{
			FormatVersion:      schemaDescriptorFormatVersion,
			SourceTableID:      9007199254740993,
			SourceTableVersion: 7,
			SourceDatabaseName: "db",
			SourceTableName:    "events",
			Columns: []SchemaColumn{{
				Ordinal:        0,
				SourceColumnID: 9007199254740995,
				Name:           "id",
				TypeID:         int32(types.T_uint64),
				NotNull:        true,
			}},
		},
		SchemaDigest:    [sha256.Size]byte{0x01, 0xab},
		ContentHash:     [sha256.Size]byte{0x02, 0xcd},
		RowCount:        9007199254740997,
		LogicalBytes:    9007199254740999,
		TotalChunkCount: 1,
		Files: []ArchiveFile{{
			FileOrdinal: 0,
			Key:         "archive/root-1/attempt-1/payload-000000-write.parquet",
			Size:        maxArchivePayloadPhysicalBytes,
			SHA256:      [sha256.Size]byte{0x03, 0xef},
			Chunks: []ArchiveChunk{{
				ChunkOrdinal:         0,
				FileOrdinal:          0,
				RowGroupOrdinal:      0,
				RowCount:             9007199254740997,
				LogicalBytes:         9007199254740999,
				CanonicalContentHash: [sha256.Size]byte{0x04, 0xaa},
			}},
		}},
		VerificationStatus: "FULL_READBACK_VERIFIED",
	}
}

type oversizeManifestReadStore struct {
	statSize   int64
	exactReads int
}

func (*oversizeManifestReadStore) Put(context.Context, string, []byte) error {
	return errors.New("unexpected Put")
}

func (*oversizeManifestReadStore) Get(context.Context, string) ([]byte, error) {
	return nil, errors.New("unexpected unbounded Get")
}

func (store *oversizeManifestReadStore) Stat(context.Context, string) (int64, error) {
	return store.statSize, nil
}

func (store *oversizeManifestReadStore) GetExact(
	context.Context,
	string,
	int64,
) ([]byte, error) {
	store.exactReads++
	return nil, errors.New("unexpected exact read")
}

type manifestTestWriter interface {
	Write([]byte) (int, error)
}

func writeManifestTestUint16(writer manifestTestWriter, value uint16) {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}

func writeManifestTestUint64(writer manifestTestWriter, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}
