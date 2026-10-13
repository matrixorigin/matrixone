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
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const (
	archiveManifestFormatVersion uint16 = 1
	archiveHashFormulaVersion    uint16 = 1
	// Archive Manifest is control metadata, not a payload. The limit is checked
	// against provider metadata before allocating the object body and again by
	// ParseArchiveManifest for non-provider callers.
	maxArchiveManifestBytes = 16 << 20
	// One payload is one Restore Chunk/Parquet Row Group. Bound its compressed
	// representation as well as its logical rows/bytes so an external object
	// cannot force an unbounded allocation before digest verification.
	maxArchivePayloadPhysicalBytes = 128 << 20
	// Phase 1 writes exactly one Row Group per payload file, so this one
	// certified bound caps payload files, Restore Chunk Receipts, Manifest
	// collection growth, and Restore aggregation memory at the same time.
	maxArchiveChunksPerDataset  = 4096
	maxArchiveSchemaColumns     = 4096
	maxArchiveManifestString    = 64 << 10
	maxArchiveObjectKeyBytes    = 4096
	maxArchiveIdentityBytes     = 256
	maxArchiveSQLNameBytes      = 1024
	maxArchiveJSONDepth         = 16
	maxArchiveJSONObjectFields  = 64
	maxArchiveJSONArrayElements = 4096
)

var archiveDatasetHashDomain = []byte("matrixone/lifecycle/archive-dataset/v1")

type ArchiveManifest struct {
	ManifestFormatVersion uint16                 `json:"manifest_format_version"`
	HashFormulaVersion    uint16                 `json:"hash_formula_version"`
	CanonicalEncoder      uint16                 `json:"canonical_encoder_version"`
	RootID                string                 `json:"root_id"`
	AttemptID             string                 `json:"attempt_id"`
	Schema                SchemaDescriptor       `json:"schema"`
	SchemaDigest          [32]byte               `json:"schema_digest"`
	ContentHash           [32]byte               `json:"content_hash"`
	RowCount              uint64                 `json:"row_count"`
	LogicalBytes          uint64                 `json:"logical_bytes"`
	TotalChunkCount       uint64                 `json:"total_chunk_count"`
	Files                 []ArchiveFile          `json:"files"`
	AutoIncrementMaxima   []AutoIncrementMax     `json:"auto_increment_maxima,omitempty"`
	LifecycleRange        *ArchiveLifecycleRange `json:"lifecycle_range,omitempty"`
	VerificationStatus    string                 `json:"verification_status"`
}

type ArchiveFile struct {
	FileOrdinal uint32         `json:"file_ordinal"`
	Key         string         `json:"key"`
	Size        uint64         `json:"size"`
	SHA256      [32]byte       `json:"sha256"`
	Chunks      []ArchiveChunk `json:"chunks"`
}

type ArchiveChunk struct {
	ChunkOrdinal         uint64   `json:"chunk_ordinal"`
	FileOrdinal          uint32   `json:"file_ordinal"`
	RowGroupOrdinal      uint32   `json:"row_group_ordinal"`
	RowCount             uint64   `json:"row_count"`
	LogicalBytes         uint64   `json:"logical_bytes"`
	CanonicalContentHash [32]byte `json:"canonical_content_hash"`
}

type AutoIncrementMax struct {
	ColumnOrdinal uint32 `json:"column_ordinal"`
	Value         string `json:"value"`
}

// archiveManifestV1Wire is the permanent on-provider V1 byte contract. Keep
// this separate from ArchiveManifest: Go integer and byte-array defaults are
// not a portable JSON protocol. Every uint64 is a canonical decimal string
// and every digest is lower-case hexadecimal.
type archiveManifestV1Wire struct {
	ManifestFormatVersion uint16                        `json:"manifest_format_version"`
	HashFormulaVersion    uint16                        `json:"hash_formula_version"`
	CanonicalEncoder      uint16                        `json:"canonical_encoder_version"`
	RootID                string                        `json:"root_id"`
	AttemptID             string                        `json:"attempt_id"`
	Schema                archiveSchemaDescriptorV1Wire `json:"schema"`
	SchemaDigest          string                        `json:"schema_digest"`
	ContentHash           string                        `json:"content_hash"`
	RowCount              string                        `json:"row_count"`
	LogicalBytes          string                        `json:"logical_bytes"`
	TotalChunkCount       string                        `json:"total_chunk_count"`
	Files                 []archiveFileV1Wire           `json:"files"`
	AutoIncrementMaxima   []archiveAutoIncrementV1Wire  `json:"auto_increment_maxima,omitempty"`
	LifecycleRange        *archiveLifecycleRangeV1Wire  `json:"lifecycle_range,omitempty"`
	VerificationStatus    string                        `json:"verification_status"`
}

type archiveLifecycleRangeV1Wire struct {
	SourceColumnID string `json:"source_column_id"`
	TypeID         int32  `json:"type_id"`
	Min            string `json:"min"`
	Max            string `json:"max"`
}

type archiveSchemaDescriptorV1Wire struct {
	FormatVersion      uint16                      `json:"format_version"`
	SourceTableID      string                      `json:"source_table_id"`
	SourceTableVersion uint32                      `json:"source_table_version"`
	SourceDatabaseName string                      `json:"source_database_name"`
	SourceTableName    string                      `json:"source_table_name"`
	Columns            []archiveSchemaColumnV1Wire `json:"columns"`
}

type archiveSchemaColumnV1Wire struct {
	Ordinal           uint32 `json:"ordinal"`
	SourceColumnID    string `json:"source_column_id"`
	Name              string `json:"name"`
	TypeID            int32  `json:"type_id"`
	Width             int32  `json:"width"`
	Scale             int32  `json:"scale"`
	EnumValues        string `json:"enum_values,omitempty"`
	NotNull           bool   `json:"not_null"`
	AutoIncrement     bool   `json:"auto_increment"`
	DefaultExpression string `json:"default_expression,omitempty"`
}

type archiveFileV1Wire struct {
	FileOrdinal uint32               `json:"file_ordinal"`
	Key         string               `json:"key"`
	Size        string               `json:"size"`
	SHA256      string               `json:"sha256"`
	Chunks      []archiveChunkV1Wire `json:"chunks"`
}

type archiveChunkV1Wire struct {
	ChunkOrdinal         string `json:"chunk_ordinal"`
	FileOrdinal          uint32 `json:"file_ordinal"`
	RowGroupOrdinal      uint32 `json:"row_group_ordinal"`
	RowCount             string `json:"row_count"`
	LogicalBytes         string `json:"logical_bytes"`
	CanonicalContentHash string `json:"canonical_content_hash"`
}

type archiveAutoIncrementV1Wire struct {
	ColumnOrdinal uint32 `json:"column_ordinal"`
	Value         string `json:"value"`
}

func MarshalArchiveManifest(manifest *ArchiveManifest) ([]byte, [32]byte, error) {
	if err := validateArchiveManifestShape(manifest); err != nil {
		return nil, [32]byte{}, err
	}
	encoded, err := json.Marshal(archiveManifestToV1Wire(manifest))
	if err != nil {
		return nil, [32]byte{}, err
	}
	if len(encoded) > maxArchiveManifestBytes {
		return nil, [32]byte{}, moerr.NewInternalErrorNoCtxf(
			"Lifecycle archive manifest exceeds the certified byte limit %d",
			maxArchiveManifestBytes,
		)
	}
	return encoded, sha256.Sum256(encoded), nil
}

func ParseArchiveManifest(encoded []byte) (*ArchiveManifest, error) {
	if len(encoded) == 0 || len(encoded) > maxArchiveManifestBytes {
		return nil, moerr.NewInternalErrorNoCtxf(
			"Lifecycle archive manifest size %d is outside the certified range",
			len(encoded),
		)
	}
	if err := validateArchiveManifestVersionPrefix(encoded); err != nil {
		return nil, err
	}
	if err := validateArchiveManifestJSON(encoded); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var wire archiveManifestV1Wire
	if err := decoder.Decode(&wire); err != nil {
		return nil, err
	}
	if err := requireArchiveManifestJSONEOF(decoder); err != nil {
		return nil, err
	}
	manifest, err := archiveManifestFromV1Wire(wire)
	if err != nil {
		return nil, err
	}
	if err := validateArchiveManifestShape(manifest); err != nil {
		return nil, err
	}
	canonical, _, err := MarshalArchiveManifest(manifest)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(encoded, canonical) {
		return nil, moerr.NewInternalErrorNoCtxf("Lifecycle archive manifest is not canonical V1 JSON")
	}
	return manifest, nil
}

func validateArchiveManifestShape(manifest *ArchiveManifest) error {
	if manifest == nil {
		return moerr.NewInternalErrorNoCtxf("nil Lifecycle archive manifest")
	}
	if manifest.ManifestFormatVersion != archiveManifestFormatVersion {
		return moerr.NewInternalErrorNoCtxf(
			"unsupported Lifecycle manifest version %d",
			manifest.ManifestFormatVersion,
		)
	}
	if manifest.HashFormulaVersion != archiveHashFormulaVersion {
		return moerr.NewInternalErrorNoCtxf(
			"unsupported Lifecycle archive hash formula %d",
			manifest.HashFormulaVersion,
		)
	}
	if manifest.CanonicalEncoder != canonicalEncoderVersion {
		return moerr.NewInternalErrorNoCtxf(
			"unsupported Lifecycle canonical encoder %d",
			manifest.CanonicalEncoder,
		)
	}
	if !validArchiveManifestString(manifest.RootID, maxArchiveIdentityBytes, true) ||
		!validArchiveManifestString(manifest.AttemptID, maxArchiveIdentityBytes, true) {
		return moerr.NewInternalErrorNoCtxf("Lifecycle archive manifest identity is invalid")
	}
	if err := validateArchiveSchemaShape(manifest.Schema); err != nil {
		return err
	}
	switch manifest.VerificationStatus {
	case "SOURCE_ENCODED", "FULL_READBACK_VERIFIED":
	default:
		return moerr.NewInternalErrorNoCtxf("Lifecycle archive verification status is invalid")
	}
	if len(manifest.Files) > maxArchiveChunksPerDataset {
		return moerr.NewInternalErrorNoCtxf(
			"Lifecycle manifest exceeds the certified chunk limit %d",
			maxArchiveChunksPerDataset,
		)
	}
	if manifest.TotalChunkCount != uint64(len(manifest.Files)) {
		return moerr.NewInternalErrorNoCtxf(
			"Lifecycle manifest chunk count %d does not match file count %d",
			manifest.TotalChunkCount,
			len(manifest.Files),
		)
	}
	var rows uint64
	var logicalBytes uint64
	fileKeys := make(map[string]struct{}, len(manifest.Files))
	for fileIndex, file := range manifest.Files {
		if file.FileOrdinal != uint32(fileIndex) {
			return moerr.NewInternalErrorNoCtxf("Lifecycle archive file ordinals are not continuous")
		}
		if !validArchiveManifestString(file.Key, maxArchiveObjectKeyBytes, true) ||
			file.Size == 0 || file.Size > maxArchivePayloadPhysicalBytes {
			return moerr.NewInternalErrorNoCtxf("Lifecycle archive file identity is invalid")
		}
		if _, exists := fileKeys[file.Key]; exists {
			return moerr.NewInternalErrorNoCtxf("Lifecycle archive file keys are not unique")
		}
		fileKeys[file.Key] = struct{}{}
		if len(file.Chunks) != 1 {
			return moerr.NewInternalErrorNoCtxf("Lifecycle Phase 1 requires exactly one row group per payload file")
		}
		chunk := file.Chunks[0]
		if chunk.ChunkOrdinal != uint64(fileIndex) ||
			chunk.FileOrdinal != uint32(fileIndex) ||
			chunk.RowGroupOrdinal != 0 {
			return moerr.NewInternalErrorNoCtxf("Lifecycle archive chunk ordinals are not canonical")
		}
		if chunk.RowCount == 0 || chunk.LogicalBytes == 0 {
			return moerr.NewInternalErrorNoCtxf("Lifecycle archive chunk size is invalid")
		}
		if ^uint64(0)-rows < chunk.RowCount ||
			^uint64(0)-logicalBytes < chunk.LogicalBytes {
			return moerr.NewInternalErrorNoCtxf("Lifecycle archive manifest totals overflow uint64")
		}
		rows += chunk.RowCount
		logicalBytes += chunk.LogicalBytes
	}
	if rows != manifest.RowCount || logicalBytes != manifest.LogicalBytes {
		return moerr.NewInternalErrorNoCtxf("Lifecycle archive manifest totals do not match chunks")
	}
	if len(manifest.AutoIncrementMaxima) > maxArchiveSchemaColumns {
		return moerr.NewInternalErrorNoCtxf("Lifecycle archive auto-increment collection exceeds limit")
	}
	var previous uint32
	for index, maximum := range manifest.AutoIncrementMaxima {
		if int(maximum.ColumnOrdinal) >= len(manifest.Schema.Columns) ||
			!manifest.Schema.Columns[maximum.ColumnOrdinal].AutoIncrement {
			return moerr.NewInternalErrorNoCtxf("Lifecycle archive auto-increment ordinal is invalid")
		}
		if index > 0 && maximum.ColumnOrdinal <= previous {
			return moerr.NewInternalErrorNoCtxf("Lifecycle archive auto-increment ordinals are not canonical")
		}
		if !validArchiveManifestString(maximum.Value, maxArchiveManifestString, true) {
			return moerr.NewInternalErrorNoCtxf("Lifecycle archive auto-increment maximum is too large")
		}
		parsed, ok := new(big.Int).SetString(maximum.Value, 10)
		if !ok || parsed.Sign() <= 0 {
			return moerr.NewInternalErrorNoCtxf("Lifecycle archive auto-increment maximum is invalid")
		}
		previous = maximum.ColumnOrdinal
	}
	if manifest.LifecycleRange != nil {
		if manifest.LifecycleRange.Min > manifest.LifecycleRange.Max {
			return moerr.NewInternalErrorNoCtxf("Lifecycle archive range is invalid")
		}
		if _, err := lifecycleRangeColumnOrdinal(
			manifest.Schema,
			*manifest.LifecycleRange,
		); err != nil {
			return err
		}
	}
	return nil
}

func validateArchiveSchemaShape(schema SchemaDescriptor) error {
	if schema.FormatVersion != schemaDescriptorFormatVersion || schema.SourceTableID == 0 {
		return moerr.NewInternalErrorNoCtxf("Lifecycle archive schema identity is invalid")
	}
	if !validArchiveManifestString(schema.SourceDatabaseName, maxArchiveSQLNameBytes, true) ||
		!validArchiveManifestString(schema.SourceTableName, maxArchiveSQLNameBytes, true) {
		return moerr.NewInternalErrorNoCtxf("Lifecycle archive schema name is invalid")
	}
	if len(schema.Columns) == 0 || len(schema.Columns) > maxArchiveSchemaColumns {
		return moerr.NewInternalErrorNoCtxf("Lifecycle archive schema column count is invalid")
	}
	for index, column := range schema.Columns {
		// MO user column IDs legitimately start at zero. The ordinal/name pair
		// must be present; SourceColumnID is lineage and zero is not a sentinel.
		if column.Ordinal != uint32(index) ||
			!validArchiveManifestString(column.Name, maxArchiveSQLNameBytes, true) {
			return moerr.NewInternalErrorNoCtxf("Lifecycle archive schema column identity is invalid")
		}
		if !validArchiveManifestString(column.EnumValues, maxArchiveManifestString, false) ||
			!validArchiveManifestString(column.DefaultExpression, maxArchiveManifestString, false) {
			return moerr.NewInternalErrorNoCtxf("Lifecycle archive schema column metadata exceeds limit")
		}
		if !isPhase1ArchiveColumnSupported(
			types.T(column.TypeID),
			column.EnumValues,
		) {
			return moerr.NewInternalErrorNoCtxf(
				"Lifecycle archive schema column %s uses an unsupported encoded SQL type",
				column.Name,
			)
		}
	}
	return nil
}

func validArchiveManifestString(value string, maxBytes int, required bool) bool {
	return (!required || value != "") && len(value) <= maxBytes && utf8.ValidString(value)
}

func archiveManifestToV1Wire(manifest *ArchiveManifest) archiveManifestV1Wire {
	wire := archiveManifestV1Wire{
		ManifestFormatVersion: manifest.ManifestFormatVersion,
		HashFormulaVersion:    manifest.HashFormulaVersion,
		CanonicalEncoder:      manifest.CanonicalEncoder,
		RootID:                manifest.RootID,
		AttemptID:             manifest.AttemptID,
		Schema:                archiveSchemaToV1Wire(manifest.Schema),
		SchemaDigest:          archiveDigestToV1Wire(manifest.SchemaDigest),
		ContentHash:           archiveDigestToV1Wire(manifest.ContentHash),
		RowCount:              strconv.FormatUint(manifest.RowCount, 10),
		LogicalBytes:          strconv.FormatUint(manifest.LogicalBytes, 10),
		TotalChunkCount:       strconv.FormatUint(manifest.TotalChunkCount, 10),
		Files:                 make([]archiveFileV1Wire, len(manifest.Files)),
		VerificationStatus:    manifest.VerificationStatus,
	}
	for index, file := range manifest.Files {
		wireFile := archiveFileV1Wire{
			FileOrdinal: file.FileOrdinal,
			Key:         file.Key,
			Size:        strconv.FormatUint(file.Size, 10),
			SHA256:      archiveDigestToV1Wire(file.SHA256),
			Chunks:      make([]archiveChunkV1Wire, len(file.Chunks)),
		}
		for chunkIndex, chunk := range file.Chunks {
			wireFile.Chunks[chunkIndex] = archiveChunkV1Wire{
				ChunkOrdinal:         strconv.FormatUint(chunk.ChunkOrdinal, 10),
				FileOrdinal:          chunk.FileOrdinal,
				RowGroupOrdinal:      chunk.RowGroupOrdinal,
				RowCount:             strconv.FormatUint(chunk.RowCount, 10),
				LogicalBytes:         strconv.FormatUint(chunk.LogicalBytes, 10),
				CanonicalContentHash: archiveDigestToV1Wire(chunk.CanonicalContentHash),
			}
		}
		wire.Files[index] = wireFile
	}
	if len(manifest.AutoIncrementMaxima) > 0 {
		wire.AutoIncrementMaxima = make(
			[]archiveAutoIncrementV1Wire,
			len(manifest.AutoIncrementMaxima),
		)
		for index, maximum := range manifest.AutoIncrementMaxima {
			wire.AutoIncrementMaxima[index] = archiveAutoIncrementV1Wire(maximum)
		}
	}
	if manifest.LifecycleRange != nil {
		wire.LifecycleRange = &archiveLifecycleRangeV1Wire{
			SourceColumnID: strconv.FormatUint(manifest.LifecycleRange.SourceColumnID, 10),
			TypeID:         manifest.LifecycleRange.TypeID,
			Min:            strconv.FormatInt(manifest.LifecycleRange.Min, 10),
			Max:            strconv.FormatInt(manifest.LifecycleRange.Max, 10),
		}
	}
	return wire
}

func archiveSchemaToV1Wire(schema SchemaDescriptor) archiveSchemaDescriptorV1Wire {
	wire := archiveSchemaDescriptorV1Wire{
		FormatVersion:      schema.FormatVersion,
		SourceTableID:      strconv.FormatUint(schema.SourceTableID, 10),
		SourceTableVersion: schema.SourceTableVersion,
		SourceDatabaseName: schema.SourceDatabaseName,
		SourceTableName:    schema.SourceTableName,
		Columns:            make([]archiveSchemaColumnV1Wire, len(schema.Columns)),
	}
	for index, column := range schema.Columns {
		wire.Columns[index] = archiveSchemaColumnV1Wire{
			Ordinal:           column.Ordinal,
			SourceColumnID:    strconv.FormatUint(column.SourceColumnID, 10),
			Name:              column.Name,
			TypeID:            column.TypeID,
			Width:             column.Width,
			Scale:             column.Scale,
			EnumValues:        column.EnumValues,
			NotNull:           column.NotNull,
			AutoIncrement:     column.AutoIncrement,
			DefaultExpression: column.DefaultExpression,
		}
	}
	return wire
}

func archiveManifestFromV1Wire(wire archiveManifestV1Wire) (*ArchiveManifest, error) {
	schema, err := archiveSchemaFromV1Wire(wire.Schema)
	if err != nil {
		return nil, err
	}
	schemaDigest, err := archiveDigestFromV1Wire("schema_digest", wire.SchemaDigest)
	if err != nil {
		return nil, err
	}
	contentHash, err := archiveDigestFromV1Wire("content_hash", wire.ContentHash)
	if err != nil {
		return nil, err
	}
	rowCount, err := archiveUint64FromV1Wire("row_count", wire.RowCount)
	if err != nil {
		return nil, err
	}
	logicalBytes, err := archiveUint64FromV1Wire("logical_bytes", wire.LogicalBytes)
	if err != nil {
		return nil, err
	}
	totalChunkCount, err := archiveUint64FromV1Wire(
		"total_chunk_count",
		wire.TotalChunkCount,
	)
	if err != nil {
		return nil, err
	}
	manifest := &ArchiveManifest{
		ManifestFormatVersion: wire.ManifestFormatVersion,
		HashFormulaVersion:    wire.HashFormulaVersion,
		CanonicalEncoder:      wire.CanonicalEncoder,
		RootID:                wire.RootID,
		AttemptID:             wire.AttemptID,
		Schema:                schema,
		SchemaDigest:          schemaDigest,
		ContentHash:           contentHash,
		RowCount:              rowCount,
		LogicalBytes:          logicalBytes,
		TotalChunkCount:       totalChunkCount,
		Files:                 make([]ArchiveFile, len(wire.Files)),
		VerificationStatus:    wire.VerificationStatus,
	}
	for index, wireFile := range wire.Files {
		size, err := archiveUint64FromV1Wire("files.size", wireFile.Size)
		if err != nil {
			return nil, err
		}
		fileDigest, err := archiveDigestFromV1Wire("files.sha256", wireFile.SHA256)
		if err != nil {
			return nil, err
		}
		file := ArchiveFile{
			FileOrdinal: wireFile.FileOrdinal,
			Key:         wireFile.Key,
			Size:        size,
			SHA256:      fileDigest,
			Chunks:      make([]ArchiveChunk, len(wireFile.Chunks)),
		}
		for chunkIndex, wireChunk := range wireFile.Chunks {
			chunkOrdinal, err := archiveUint64FromV1Wire(
				"files.chunks.chunk_ordinal",
				wireChunk.ChunkOrdinal,
			)
			if err != nil {
				return nil, err
			}
			chunkRows, err := archiveUint64FromV1Wire(
				"files.chunks.row_count",
				wireChunk.RowCount,
			)
			if err != nil {
				return nil, err
			}
			chunkBytes, err := archiveUint64FromV1Wire(
				"files.chunks.logical_bytes",
				wireChunk.LogicalBytes,
			)
			if err != nil {
				return nil, err
			}
			chunkDigest, err := archiveDigestFromV1Wire(
				"files.chunks.canonical_content_hash",
				wireChunk.CanonicalContentHash,
			)
			if err != nil {
				return nil, err
			}
			file.Chunks[chunkIndex] = ArchiveChunk{
				ChunkOrdinal:         chunkOrdinal,
				FileOrdinal:          wireChunk.FileOrdinal,
				RowGroupOrdinal:      wireChunk.RowGroupOrdinal,
				RowCount:             chunkRows,
				LogicalBytes:         chunkBytes,
				CanonicalContentHash: chunkDigest,
			}
		}
		manifest.Files[index] = file
	}
	if len(wire.AutoIncrementMaxima) > 0 {
		manifest.AutoIncrementMaxima = make(
			[]AutoIncrementMax,
			len(wire.AutoIncrementMaxima),
		)
		for index, maximum := range wire.AutoIncrementMaxima {
			manifest.AutoIncrementMaxima[index] = AutoIncrementMax(maximum)
		}
	}
	if wire.LifecycleRange != nil {
		columnID, err := archiveUint64FromV1Wire(
			"lifecycle_range.source_column_id",
			wire.LifecycleRange.SourceColumnID,
		)
		if err != nil {
			return nil, err
		}
		minimum, err := archiveInt64FromV1Wire("lifecycle_range.min", wire.LifecycleRange.Min)
		if err != nil {
			return nil, err
		}
		maximum, err := archiveInt64FromV1Wire("lifecycle_range.max", wire.LifecycleRange.Max)
		if err != nil {
			return nil, err
		}
		manifest.LifecycleRange = &ArchiveLifecycleRange{
			SourceColumnID: columnID,
			TypeID:         wire.LifecycleRange.TypeID,
			Min:            minimum,
			Max:            maximum,
		}
	}
	return manifest, nil
}

func archiveSchemaFromV1Wire(wire archiveSchemaDescriptorV1Wire) (SchemaDescriptor, error) {
	tableID, err := archiveUint64FromV1Wire("schema.source_table_id", wire.SourceTableID)
	if err != nil {
		return SchemaDescriptor{}, err
	}
	schema := SchemaDescriptor{
		FormatVersion:      wire.FormatVersion,
		SourceTableID:      tableID,
		SourceTableVersion: wire.SourceTableVersion,
		SourceDatabaseName: wire.SourceDatabaseName,
		SourceTableName:    wire.SourceTableName,
		Columns:            make([]SchemaColumn, len(wire.Columns)),
	}
	for index, wireColumn := range wire.Columns {
		columnID, err := archiveUint64FromV1Wire(
			"schema.columns.source_column_id",
			wireColumn.SourceColumnID,
		)
		if err != nil {
			return SchemaDescriptor{}, err
		}
		schema.Columns[index] = SchemaColumn{
			Ordinal:           wireColumn.Ordinal,
			SourceColumnID:    columnID,
			Name:              wireColumn.Name,
			TypeID:            wireColumn.TypeID,
			Width:             wireColumn.Width,
			Scale:             wireColumn.Scale,
			EnumValues:        wireColumn.EnumValues,
			NotNull:           wireColumn.NotNull,
			AutoIncrement:     wireColumn.AutoIncrement,
			DefaultExpression: wireColumn.DefaultExpression,
		}
	}
	return schema, nil
}

func archiveUint64FromV1Wire(field, value string) (uint64, error) {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return 0, moerr.NewInternalErrorNoCtxf("Lifecycle archive manifest %s is not a canonical uint64", field)
	}
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return 0, moerr.NewInternalErrorNoCtxf("Lifecycle archive manifest %s is not a canonical uint64", field)
		}
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, moerr.NewInternalErrorNoCtxf("Lifecycle archive manifest %s is not a uint64: %v", field, err)
	}
	return parsed, nil
}

func archiveInt64FromV1Wire(field, value string) (int64, error) {
	if value == "" || value == "+0" || value == "-0" ||
		(len(value) > 1 && value[0] == '0') ||
		(len(value) > 2 && value[0] == '-' && value[1] == '0') {
		return 0, moerr.NewInternalErrorNoCtxf(
			"Lifecycle archive manifest %s is not a canonical int64",
			field,
		)
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || strconv.FormatInt(parsed, 10) != value {
		return 0, moerr.NewInternalErrorNoCtxf(
			"Lifecycle archive manifest %s is not an int64",
			field,
		)
	}
	return parsed, nil
}

func archiveDigestToV1Wire(digest [sha256.Size]byte) string {
	return hex.EncodeToString(digest[:])
}

func archiveDigestFromV1Wire(field, value string) ([sha256.Size]byte, error) {
	if len(value) != sha256.Size*2 {
		return [sha256.Size]byte{}, moerr.NewInternalErrorNoCtxf(
			"Lifecycle archive manifest %s is not a SHA-256 digest",
			field,
		)
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return [sha256.Size]byte{}, moerr.NewInternalErrorNoCtxf(
			"Lifecycle archive manifest %s is not a SHA-256 digest: %v",
			field,
			err,
		)
	}
	var digest [sha256.Size]byte
	copy(digest[:], decoded)
	return digest, nil
}

func validateArchiveManifestVersionPrefix(encoded []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	opening, err := decoder.Token()
	if err != nil {
		return err
	}
	if opening != json.Delim('{') || !decoder.More() {
		return moerr.NewInternalErrorNoCtxf("Lifecycle archive manifest must be a non-empty JSON object")
	}
	field, err := decoder.Token()
	if err != nil {
		return err
	}
	if field != "manifest_format_version" {
		return moerr.NewInternalErrorNoCtxf("Lifecycle archive manifest version must be the first field")
	}
	version, err := decoder.Token()
	if err != nil {
		return err
	}
	number, ok := version.(json.Number)
	if !ok || number.String() != strconv.FormatUint(uint64(archiveManifestFormatVersion), 10) {
		return moerr.NewInternalErrorNoCtxf("unsupported Lifecycle archive manifest version %v", version)
	}
	return nil
}

func validateArchiveManifestJSON(encoded []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := scanArchiveManifestJSONValue(decoder, 0); err != nil {
		return err
	}
	return requireArchiveManifestJSONEOF(decoder)
}

func scanArchiveManifestJSONValue(decoder *json.Decoder, depth int) error {
	if depth > maxArchiveJSONDepth {
		return moerr.NewInternalErrorNoCtxf("Lifecycle archive manifest exceeds JSON depth limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			seen := make(map[string]struct{})
			fields := 0
			for decoder.More() {
				fields++
				if fields > maxArchiveJSONObjectFields {
					return moerr.NewInternalErrorNoCtxf("Lifecycle archive manifest object exceeds field limit")
				}
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok || len(key) == 0 || len(key) > maxArchiveSQLNameBytes {
					return moerr.NewInternalErrorNoCtxf("Lifecycle archive manifest object key is invalid")
				}
				if _, exists := seen[key]; exists {
					return moerr.NewInternalErrorNoCtxf("Lifecycle archive manifest has duplicate field %q", key)
				}
				seen[key] = struct{}{}
				if err := scanArchiveManifestJSONValue(decoder, depth+1); err != nil {
					return err
				}
			}
			closing, err := decoder.Token()
			if err != nil {
				return err
			}
			if closing != json.Delim('}') {
				return moerr.NewInternalErrorNoCtxf("Lifecycle archive manifest object is not closed")
			}
		case '[':
			elements := 0
			for decoder.More() {
				elements++
				if elements > maxArchiveJSONArrayElements {
					return moerr.NewInternalErrorNoCtxf("Lifecycle archive manifest array exceeds element limit")
				}
				if err := scanArchiveManifestJSONValue(decoder, depth+1); err != nil {
					return err
				}
			}
			closing, err := decoder.Token()
			if err != nil {
				return err
			}
			if closing != json.Delim(']') {
				return moerr.NewInternalErrorNoCtxf("Lifecycle archive manifest array is not closed")
			}
		default:
			return moerr.NewInternalErrorNoCtxf("Lifecycle archive manifest contains invalid delimiter")
		}
	case string:
		if len(value) > maxArchiveManifestString {
			return moerr.NewInternalErrorNoCtxf("Lifecycle archive manifest string exceeds byte limit")
		}
	case json.Number:
		if len(value.String()) > 32 {
			return moerr.NewInternalErrorNoCtxf("Lifecycle archive manifest number exceeds byte limit")
		}
	case bool, nil:
	default:
		return moerr.NewInternalErrorNoCtxf("Lifecycle archive manifest contains unsupported JSON token")
	}
	return nil
}

func requireArchiveManifestJSONEOF(decoder *json.Decoder) error {
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return moerr.NewInternalErrorNoCtxf("Lifecycle archive manifest contains trailing JSON")
		}
		return err
	}
	return nil
}

func computeArchiveDatasetHash(
	schemaDigest [32]byte,
	chunks []ArchiveChunk,
) [32]byte {
	sum := sha256.New()
	_, _ = sum.Write(archiveDatasetHashDomain)
	var number [8]byte
	binary.BigEndian.PutUint16(number[:2], archiveHashFormulaVersion)
	_, _ = sum.Write(number[:2])
	_, _ = sum.Write(schemaDigest[:])
	binary.BigEndian.PutUint64(number[:], uint64(len(chunks)))
	_, _ = sum.Write(number[:])
	for _, chunk := range chunks {
		binary.BigEndian.PutUint64(number[:], chunk.ChunkOrdinal)
		_, _ = sum.Write(number[:])
		binary.BigEndian.PutUint64(number[:], chunk.RowCount)
		_, _ = sum.Write(number[:])
		binary.BigEndian.PutUint64(number[:], chunk.LogicalBytes)
		_, _ = sum.Write(number[:])
		_, _ = sum.Write(chunk.CanonicalContentHash[:])
	}
	var result [32]byte
	copy(result[:], sum.Sum(nil))
	return result
}

func archiveManifestKey(prefix string, digest [32]byte) string {
	return fmt.Sprintf("%s/manifest-%s.json", prefix, hex.EncodeToString(digest[:]))
}
