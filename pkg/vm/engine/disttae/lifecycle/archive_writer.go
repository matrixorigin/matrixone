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
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/parquet-go/parquet-go"
)

var (
	ErrArchiveRowTooLarge    = moerr.NewInternalErrorNoCtx("Lifecycle archive row exceeds certified restore chunk limit")
	ErrCleanupRootNotDurable = moerr.NewInternalErrorNoCtx("Lifecycle cleanup root is not durable")
	ErrArchiveWriterClosed   = moerr.NewInternalErrorNoCtx("Lifecycle archive writer is closed")
)

type ArchiveStore interface {
	Put(ctx context.Context, key string, value []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
}

type ArchiveSideEffectGuard interface {
	EnsureDurable(ctx context.Context, rootID, attemptID string) error
}

type ArchiveWriterConfig struct {
	RootID               string
	AttemptID            string
	Prefix               string
	WriteID              string
	Schema               SchemaDescriptor
	SchemaDigest         [32]byte
	MaxRestoreChunkRows  uint64
	MaxChunkLogicalBytes uint64
	MaxPhysicalBytes     uint64
	Faults               FaultInjector
}

type ArchiveWriter struct {
	config        ArchiveWriterConfig
	store         ArchiveStore
	guard         ArchiveSideEffectGuard
	schema        *parquet.Schema
	pending       []archiveRow
	pendingBytes  uint64
	physicalBytes uint64
	files         []ArchiveFile
	guarded       bool
	closed        bool
	autoMaxima    map[uint32]*big.Int
	faults        FaultInjector
}

type archiveRow struct {
	cells   []CanonicalCell
	parquet map[string]any
	bytes   uint64
}

func NewArchiveWriter(
	ctx context.Context,
	config ArchiveWriterConfig,
	store ArchiveStore,
	guard ArchiveSideEffectGuard,
) (*ArchiveWriter, error) {
	if store == nil || guard == nil {
		return nil, moerr.NewInvalidInput(ctx, "Lifecycle archive store and cleanup guard are required")
	}
	if config.RootID == "" || config.AttemptID == "" ||
		config.Prefix == "" || config.WriteID == "" {
		return nil, moerr.NewInvalidInput(ctx, "Lifecycle archive immutable identity is incomplete")
	}
	if config.MaxRestoreChunkRows == 0 || config.MaxChunkLogicalBytes == 0 ||
		config.MaxPhysicalBytes == 0 {
		return nil, moerr.NewInvalidInput(ctx, "Lifecycle archive writer limits must be positive")
	}
	if config.Schema.FormatVersion != schemaDescriptorFormatVersion {
		return nil, moerr.NewInvalidInput(ctx, "Lifecycle archive schema version is invalid")
	}
	digest, err := config.Schema.Digest()
	if err != nil {
		return nil, err
	}
	if digest != config.SchemaDigest {
		return nil, moerr.NewInvalidInput(ctx, "Lifecycle archive schema digest mismatch")
	}
	parquetSchema, err := buildArchiveParquetSchema(config.Schema)
	if err != nil {
		return nil, err
	}
	faults := config.Faults
	if faults == nil {
		faults = NoLifecycleFaults{}
	}
	return &ArchiveWriter{
		config:     config,
		store:      store,
		guard:      guard,
		schema:     parquetSchema,
		autoMaxima: make(map[uint32]*big.Int),
		faults:     faults,
	}, nil
}

func (writer *ArchiveWriter) WriteBatch(
	ctx context.Context,
	value *batch.Batch,
	selected *nulls.Nulls,
) error {
	if writer.closed {
		return ErrArchiveWriterClosed
	}
	if value == nil {
		return nil
	}
	if len(value.Vecs) != len(writer.config.Schema.Columns) {
		return moerr.NewInvalidInputf(
			ctx,
			"Lifecycle archive batch has %d columns, schema has %d",
			len(value.Vecs),
			len(writer.config.Schema.Columns),
		)
	}
	for row := 0; row < value.RowCount(); row++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if selected != nil && !selected.Contains(uint64(row)) {
			continue
		}
		archiveValue, err := writer.rowFromBatch(ctx, value, row)
		if err != nil {
			return err
		}
		chunkOverhead := canonicalArchiveChunkOverhead(writer.config.SchemaDigest)
		if archiveValue.bytes+chunkOverhead > writer.config.MaxChunkLogicalBytes {
			return ErrArchiveRowTooLarge
		}
		if len(writer.pending) > 0 &&
			(uint64(len(writer.pending)) >= writer.config.MaxRestoreChunkRows ||
				writer.pendingBytes+archiveValue.bytes+chunkOverhead >
					writer.config.MaxChunkLogicalBytes) {
			if err := writer.flushChunk(ctx); err != nil {
				return err
			}
		}
		if err := accumulateAutoIncrementMaxima(
			writer.config.Schema,
			archiveValue.cells,
			writer.autoMaxima,
		); err != nil {
			return err
		}
		writer.pending = append(writer.pending, archiveValue)
		writer.pendingBytes += archiveValue.bytes
	}
	return nil
}

func (writer *ArchiveWriter) Close(
	ctx context.Context,
) (*ArchiveManifest, string, error) {
	if writer.closed {
		return nil, "", ErrArchiveWriterClosed
	}
	writer.closed = true
	if err := writer.flushChunk(ctx); err != nil {
		return nil, "", err
	}
	chunks := make([]ArchiveChunk, 0, len(writer.files))
	var rows uint64
	var logicalBytes uint64
	for _, file := range writer.files {
		chunk := file.Chunks[0]
		chunks = append(chunks, chunk)
		rows += chunk.RowCount
		logicalBytes += chunk.LogicalBytes
	}
	manifest := &ArchiveManifest{
		ManifestFormatVersion: archiveManifestFormatVersion,
		HashFormulaVersion:    archiveHashFormulaVersion,
		CanonicalEncoder:      canonicalEncoderVersion,
		RootID:                writer.config.RootID,
		AttemptID:             writer.config.AttemptID,
		Schema:                writer.config.Schema,
		SchemaDigest:          writer.config.SchemaDigest,
		ContentHash:           computeArchiveDatasetHash(writer.config.SchemaDigest, chunks),
		RowCount:              rows,
		LogicalBytes:          logicalBytes,
		TotalChunkCount:       uint64(len(chunks)),
		Files:                 writer.files,
		AutoIncrementMaxima:   encodeAutoIncrementMaxima(writer.autoMaxima),
		VerificationStatus:    "SOURCE_ENCODED",
	}
	encoded, digest, err := MarshalArchiveManifest(manifest)
	if err != nil {
		return nil, "", err
	}
	key := archiveManifestKey(writer.config.Prefix, digest)
	if err := writer.ensureGuard(ctx); err != nil {
		return nil, "", err
	}
	if err := writer.faults.Inject(ctx, FaultBeforeManifestPut); err != nil {
		return nil, "", err
	}
	if err := writer.put(ctx, key, encoded); err != nil {
		return nil, "", err
	}
	if err := writer.faults.Inject(ctx, FaultAfterManifestPut); err != nil {
		return nil, "", err
	}
	if err := writer.faults.Inject(ctx, FaultBeforeFullReadback); err != nil {
		return nil, "", err
	}
	verified, err := ReadAndVerifyArchive(ctx, writer.store, key)
	if err != nil {
		return nil, "", err
	}
	verified.VerificationStatus = "FULL_READBACK_VERIFIED"
	verifiedEncoded, verifiedDigest, err := MarshalArchiveManifest(verified)
	if err != nil {
		return nil, "", err
	}
	verifiedKey := archiveManifestKey(writer.config.Prefix, verifiedDigest)
	if err := writer.faults.Inject(ctx, FaultBeforeManifestPut); err != nil {
		return nil, "", err
	}
	if err := writer.put(ctx, verifiedKey, verifiedEncoded); err != nil {
		return nil, "", err
	}
	if err := writer.faults.Inject(ctx, FaultAfterManifestPut); err != nil {
		return nil, "", err
	}
	if err := writer.faults.Inject(ctx, FaultBeforeFullReadback); err != nil {
		return nil, "", err
	}
	// Payloads were already read back and canonically verified above. The
	// second immutable write changes only the Manifest verification status;
	// read back that content-addressed Manifest itself instead of downloading
	// every payload a second time (which would double Archive GET cost at TB
	// scale).
	verified, err = ReadArchiveManifest(ctx, writer.store, verifiedKey)
	if err != nil {
		return nil, "", err
	}
	if verified.VerificationStatus != "FULL_READBACK_VERIFIED" {
		return nil, "", moerr.NewInternalErrorNoCtxf(
			"Lifecycle persisted Manifest is not full-readback verified",
		)
	}
	if err := writer.faults.Inject(ctx, FaultAfterFullReadback); err != nil {
		return nil, "", err
	}
	return verified, verifiedKey, nil
}

func (writer *ArchiveWriter) flushChunk(ctx context.Context) error {
	if len(writer.pending) == 0 {
		return nil
	}
	if len(writer.files) >= maxArchiveChunksPerDataset {
		return moerr.NewInternalErrorNoCtxf(
			"RESOURCE_BLOCKED: Lifecycle Dataset exceeds the certified chunk limit %d",
			maxArchiveChunksPerDataset,
		)
	}
	if err := writer.ensureGuard(ctx); err != nil {
		return err
	}
	ordinal := len(writer.files)
	rows := make([]any, len(writer.pending))
	encoder := NewCanonicalValueEncoder(writer.config.SchemaDigest)
	for index, row := range writer.pending {
		rows[index] = row.parquet
		if err := encoder.WriteRow(ctx, row.cells); err != nil {
			return err
		}
	}
	var output bytes.Buffer
	parquetWriter := parquet.NewGenericWriter[any](
		&output,
		writer.schema,
		parquet.MaxRowsPerRowGroup(int64(len(rows))),
		parquet.Compression(&parquet.Zstd),
	)
	if _, err := parquetWriter.Write(rows); err != nil {
		return err
	}
	if err := parquetWriter.Close(); err != nil {
		return err
	}
	key := fmt.Sprintf(
		"%s/payload-%06d-%s.parquet",
		writer.config.Prefix,
		ordinal,
		writer.config.WriteID,
	)
	payload := output.Bytes()
	if uint64(len(payload)) > maxArchivePayloadPhysicalBytes {
		return moerr.NewInternalErrorNoCtxf(
			"RESOURCE_BLOCKED: Lifecycle Archive payload exceeds the certified physical limit",
		)
	}
	if err := writer.faults.Inject(ctx, FaultBeforePayloadPut); err != nil {
		return err
	}
	if err := writer.put(ctx, key, payload); err != nil {
		return err
	}
	if err := writer.faults.Inject(ctx, FaultAfterPayloadPut); err != nil {
		return err
	}
	fileHash := sha256.Sum256(payload)
	writer.files = append(writer.files, ArchiveFile{
		FileOrdinal: uint32(ordinal),
		Key:         key,
		Size:        uint64(len(payload)),
		SHA256:      fileHash,
		Chunks: []ArchiveChunk{{
			ChunkOrdinal:         uint64(ordinal),
			FileOrdinal:          uint32(ordinal),
			RowGroupOrdinal:      0,
			RowCount:             encoder.RowCount(),
			LogicalBytes:         encoder.LogicalBytes(),
			CanonicalContentHash: encoder.Sum(),
		}},
	})
	writer.pending = nil
	writer.pendingBytes = 0
	return nil
}

func (writer *ArchiveWriter) put(
	ctx context.Context,
	key string,
	value []byte,
) error {
	bytes := uint64(len(value))
	if bytes > writer.config.MaxPhysicalBytes ||
		writer.physicalBytes > writer.config.MaxPhysicalBytes-bytes {
		return moerr.NewInternalErrorNoCtxf(
			"RESOURCE_BLOCKED: Lifecycle Archive exceeds its reserved physical bytes",
		)
	}
	if err := writer.store.Put(ctx, key, value); err != nil {
		return err
	}
	writer.physicalBytes += bytes
	return nil
}

func (writer *ArchiveWriter) ensureGuard(ctx context.Context) error {
	if writer.guarded {
		return nil
	}
	if err := writer.guard.EnsureDurable(
		ctx,
		writer.config.RootID,
		writer.config.AttemptID,
	); err != nil {
		return err
	}
	writer.guarded = true
	return nil
}

func (writer *ArchiveWriter) rowFromBatch(
	ctx context.Context,
	value *batch.Batch,
	row int,
) (archiveRow, error) {
	cells := make([]CanonicalCell, len(value.Vecs))
	parquetValue := make(map[string]any, len(value.Vecs))
	for columnIndex, vec := range value.Vecs {
		column := writer.config.Schema.Columns[columnIndex]
		if int32(vec.GetType().Oid) != column.TypeID ||
			vec.GetType().Width != column.Width ||
			vec.GetType().Scale != column.Scale {
			return archiveRow{}, moerr.NewInvalidInputf(
				ctx,
				"Lifecycle archive column %d type does not match frozen schema",
				columnIndex,
			)
		}
		cell := CanonicalCell{
			Type: *vec.GetType(),
			Null: vec.GetNulls().Contains(uint64(row)),
		}
		var err error
		if !cell.Null {
			cell.Value, err = canonicalValueFromVector(vec, row)
			if err != nil {
				return archiveRow{}, err
			}
			if data, ok := cell.Value.([]byte); ok {
				cell.Value = append([]byte(nil), data...)
			}
		}
		cells[columnIndex] = cell
		parquetValue[archiveParquetColumnName(column)] = nil
		if !cell.Null {
			parquetValue[archiveParquetColumnName(column)], err =
				canonicalCellToParquet(column, cell)
			if err != nil {
				return archiveRow{}, err
			}
		}
	}
	logicalBytes, err := canonicalArchiveRowLogicalBytes(ctx, writer.config.SchemaDigest, cells)
	if err != nil {
		return archiveRow{}, err
	}
	return archiveRow{cells: cells, parquet: parquetValue, bytes: logicalBytes}, nil
}

func canonicalArchiveRowLogicalBytes(
	ctx context.Context,
	schemaDigest [32]byte,
	cells []CanonicalCell,
) (uint64, error) {
	encoder := NewCanonicalValueEncoder(schemaDigest)
	overhead := encoder.LogicalBytes()
	if err := encoder.WriteRow(ctx, cells); err != nil {
		return 0, err
	}
	return encoder.LogicalBytes() - overhead, nil
}

func canonicalArchiveChunkOverhead(schemaDigest [32]byte) uint64 {
	return NewCanonicalValueEncoder(schemaDigest).LogicalBytes()
}

func buildArchiveParquetSchema(descriptor SchemaDescriptor) (*parquet.Schema, error) {
	group := make(parquet.Group, len(descriptor.Columns))
	for _, column := range descriptor.Columns {
		node, err := archiveParquetNode(column)
		if err != nil {
			return nil, err
		}
		group[archiveParquetColumnName(column)] = parquet.Optional(node)
	}
	return parquet.NewSchema("matrixone_lifecycle_archive", group), nil
}

func archiveParquetColumnName(column SchemaColumn) string {
	return fmt.Sprintf("c%06d_%s", column.Ordinal, strings.ReplaceAll(column.Name, ".", "_"))
}

func archiveParquetNode(column SchemaColumn) (parquet.Node, error) {
	switch types.T(column.TypeID) {
	case types.T_bool:
		return parquet.Leaf(parquet.BooleanType), nil
	case types.T_int8, types.T_int16, types.T_int32:
		return parquet.Int(32), nil
	case types.T_int64:
		return parquet.Int(64), nil
	case types.T_uint8, types.T_uint16, types.T_uint32:
		return parquet.Uint(32), nil
	case types.T_bit, types.T_uint64:
		return parquet.Uint(64), nil
	case types.T_float32:
		return parquet.Leaf(parquet.FloatType), nil
	case types.T_float64:
		return parquet.Leaf(parquet.DoubleType), nil
	case types.T_binary, types.T_varbinary, types.T_blob:
		return parquet.Leaf(parquet.ByteArrayType), nil
	case types.T_char, types.T_varchar, types.T_text, types.T_json,
		types.T_date, types.T_datetime, types.T_timestamp, types.T_time,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_uuid:
		return parquet.String(), nil
	case types.T_enum:
		return parquet.Uint(32), nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf("unsupported Lifecycle Parquet type %s", types.T(column.TypeID))
	}
}

func canonicalCellToParquet(column SchemaColumn, cell CanonicalCell) (any, error) {
	switch types.T(column.TypeID) {
	case types.T_bool:
		return cell.Value.(bool), nil
	case types.T_int8:
		return int32(cell.Value.(int8)), nil
	case types.T_int16:
		return int32(cell.Value.(int16)), nil
	case types.T_int32:
		return cell.Value.(int32), nil
	case types.T_int64:
		return cell.Value.(int64), nil
	case types.T_uint8:
		return uint32(cell.Value.(uint8)), nil
	case types.T_uint16:
		return uint32(cell.Value.(uint16)), nil
	case types.T_uint32:
		return cell.Value.(uint32), nil
	case types.T_bit, types.T_uint64:
		return cell.Value.(uint64), nil
	case types.T_float32:
		return cell.Value.(float32), nil
	case types.T_float64:
		return cell.Value.(float64), nil
	case types.T_char, types.T_varchar, types.T_text:
		return string(cell.Value.([]byte)), nil
	case types.T_binary, types.T_varbinary, types.T_blob:
		return cell.Value.([]byte), nil
	case types.T_json:
		return string(cell.Value.([]byte)), nil
	case types.T_date:
		return cell.Value.(types.Date).String(), nil
	case types.T_datetime:
		return cell.Value.(types.Datetime).String2(column.Scale), nil
	case types.T_timestamp:
		return cell.Value.(types.Timestamp).String2(time.UTC, column.Scale), nil
	case types.T_time:
		return cell.Value.(types.Time).String2(column.Scale), nil
	case types.T_decimal64:
		return cell.Value.(types.Decimal64).Format(column.Scale), nil
	case types.T_decimal128:
		return cell.Value.(types.Decimal128).Format(column.Scale), nil
	case types.T_decimal256:
		return cell.Value.(types.Decimal256).Format(column.Scale), nil
	case types.T_uuid:
		return cell.Value.(types.Uuid).String(), nil
	case types.T_enum:
		return uint32(cell.Value.(types.Enum)), nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf("unsupported Lifecycle Parquet value type %s", types.T(column.TypeID))
	}
}

func checkedInt64(value uint64) (int64, error) {
	if value > math.MaxInt64 {
		return 0, moerr.NewInternalErrorNoCtxf("value %d exceeds int64", value)
	}
	return int64(value), nil
}

func accumulateAutoIncrementMaxima(
	schema SchemaDescriptor,
	cells []CanonicalCell,
	maxima map[uint32]*big.Int,
) error {
	if len(cells) != len(schema.Columns) {
		return moerr.NewInternalErrorNoCtxf("Lifecycle auto-increment row width mismatch")
	}
	for ordinal, column := range schema.Columns {
		if !column.AutoIncrement || cells[ordinal].Null {
			continue
		}
		value, err := autoIncrementBigInt(cells[ordinal])
		if err != nil {
			return moerr.NewInternalErrorNoCtxf(
				"Lifecycle auto-increment column %s: %v",
				column.Name,
				err,
			)
		}
		// MO's existing increment service advances its offset only for explicit
		// positive values. Signed AUTO_INCREMENT columns may legally contain
		// zero or negative historical rows; persisting those as an unsigned
		// Restore offset would make an otherwise valid Dataset unpublishable.
		if value.Sign() <= 0 {
			continue
		}
		current := maxima[column.Ordinal]
		if current == nil || value.Cmp(current) > 0 {
			maxima[column.Ordinal] = new(big.Int).Set(value)
		}
	}
	return nil
}

func autoIncrementBigInt(cell CanonicalCell) (*big.Int, error) {
	value := new(big.Int)
	switch typed := cell.Value.(type) {
	case int8:
		value.SetInt64(int64(typed))
	case int16:
		value.SetInt64(int64(typed))
	case int32:
		value.SetInt64(int64(typed))
	case int64:
		value.SetInt64(typed)
	case uint8:
		value.SetUint64(uint64(typed))
	case uint16:
		value.SetUint64(uint64(typed))
	case uint32:
		value.SetUint64(uint64(typed))
	case uint64:
		value.SetUint64(typed)
	default:
		return nil, moerr.NewInternalErrorNoCtxf("unsupported value type %T", cell.Value)
	}
	return value, nil
}

func encodeAutoIncrementMaxima(
	maxima map[uint32]*big.Int,
) []AutoIncrementMax {
	if len(maxima) == 0 {
		return nil
	}
	ordinals := make([]uint32, 0, len(maxima))
	for ordinal := range maxima {
		ordinals = append(ordinals, ordinal)
	}
	sort.Slice(ordinals, func(left, right int) bool {
		return ordinals[left] < ordinals[right]
	})
	result := make([]AutoIncrementMax, 0, len(ordinals))
	for _, ordinal := range ordinals {
		result = append(result, AutoIncrementMax{
			ColumnOrdinal: ordinal,
			Value:         maxima[ordinal].String(),
		})
	}
	return result
}
