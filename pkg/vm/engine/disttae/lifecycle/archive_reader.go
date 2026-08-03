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
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/parquet-go/parquet-go"
)

func ReadAndVerifyArchive(
	ctx context.Context,
	store ArchiveStore,
	manifestKey string,
) (*ArchiveManifest, error) {
	manifest, err := ReadArchiveManifest(ctx, store, manifestKey)
	if err != nil {
		return nil, err
	}
	verifiedChunks := make([]ArchiveChunk, 0, len(manifest.Files))
	autoMaxima := make(map[uint32]*big.Int)
	var rows uint64
	var logicalBytes uint64
	for _, file := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		payload, err := readArchivePayload(ctx, store, file)
		if err != nil {
			return nil, err
		}
		if uint64(len(payload)) != file.Size || sha256.Sum256(payload) != file.SHA256 {
			return nil, fmt.Errorf("Lifecycle archive payload %s digest mismatch", file.Key)
		}
		decodedRows, chunk, err := decodeArchivePayload(
			ctx,
			payload,
			manifest.Schema,
			manifest.SchemaDigest,
			file.Chunks[0],
			file.Chunks[0].RowCount,
			file.Chunks[0].LogicalBytes,
		)
		if err != nil {
			return nil, err
		}
		for _, row := range decodedRows {
			if err := accumulateAutoIncrementMaxima(
				manifest.Schema,
				row,
				autoMaxima,
			); err != nil {
				return nil, err
			}
		}
		verifiedChunks = append(verifiedChunks, chunk)
		rows += chunk.RowCount
		logicalBytes += chunk.LogicalBytes
	}
	if rows != manifest.RowCount || logicalBytes != manifest.LogicalBytes {
		return nil, fmt.Errorf("Lifecycle archive readback totals mismatch")
	}
	if computeArchiveDatasetHash(manifest.SchemaDigest, verifiedChunks) !=
		manifest.ContentHash {
		return nil, fmt.Errorf("Lifecycle archive content hash mismatch")
	}
	if !slices.Equal(
		encodeAutoIncrementMaxima(autoMaxima),
		manifest.AutoIncrementMaxima,
	) {
		return nil, fmt.Errorf("Lifecycle archive auto-increment maxima mismatch")
	}
	result := *manifest
	result.VerificationStatus = "FULL_READBACK_VERIFIED"
	return &result, nil
}

func ReadArchiveManifest(
	ctx context.Context,
	store ArchiveStore,
	manifestKey string,
) (*ArchiveManifest, error) {
	manifestStore, ok := store.(archiveManifestReadStore)
	if !ok {
		return nil, fmt.Errorf(
			"Lifecycle Archive Store does not support bounded Manifest reads",
		)
	}
	manifestSize, err := manifestStore.Stat(ctx, manifestKey)
	if err != nil {
		return nil, err
	}
	if manifestSize <= 0 || manifestSize > int64(maxArchiveManifestBytes) {
		return nil, fmt.Errorf(
			"Lifecycle archive manifest size %d is outside the certified range",
			manifestSize,
		)
	}
	encoded, err := manifestStore.GetExact(ctx, manifestKey, manifestSize)
	if err != nil {
		return nil, err
	}
	manifestSizeAfterRead, err := manifestStore.Stat(ctx, manifestKey)
	if err != nil {
		return nil, err
	}
	if manifestSizeAfterRead != manifestSize || int64(len(encoded)) != manifestSize {
		return nil, fmt.Errorf("Lifecycle archive manifest changed during bounded read")
	}
	manifestDigest := sha256.Sum256(encoded)
	expectedManifestDigest, err := manifestDigestFromKey(manifestKey)
	if err != nil {
		return nil, err
	}
	if manifestDigest != expectedManifestDigest {
		return nil, fmt.Errorf("Lifecycle archive manifest digest mismatch")
	}
	manifest, err := ParseArchiveManifest(encoded)
	if err != nil {
		return nil, err
	}
	schemaDigest, err := manifest.Schema.Digest()
	if err != nil {
		return nil, err
	}
	if schemaDigest != manifest.SchemaDigest {
		return nil, fmt.Errorf("Lifecycle archive schema digest mismatch")
	}
	return manifest, nil
}

func ReadArchiveChunk(
	ctx context.Context,
	store ArchiveStore,
	manifest *ArchiveManifest,
	chunkOrdinal uint64,
	maxRows uint64,
	maxLogicalBytes uint64,
) ([][]CanonicalCell, ArchiveChunk, error) {
	if manifest == nil ||
		chunkOrdinal >= uint64(len(manifest.Files)) ||
		maxRows == 0 ||
		maxLogicalBytes == 0 {
		return nil, ArchiveChunk{}, fmt.Errorf("Lifecycle Restore chunk input is invalid")
	}
	file := manifest.Files[chunkOrdinal]
	expected := file.Chunks[0]
	if expected.RowCount > maxRows || expected.LogicalBytes > maxLogicalBytes {
		return nil, ArchiveChunk{}, fmt.Errorf(
			"Lifecycle Restore chunk exceeds the certified limit",
		)
	}
	payload, err := readArchivePayload(ctx, store, file)
	if err != nil {
		return nil, ArchiveChunk{}, err
	}
	if uint64(len(payload)) != file.Size || sha256.Sum256(payload) != file.SHA256 {
		return nil, ArchiveChunk{}, fmt.Errorf(
			"Lifecycle archive payload %s digest mismatch",
			file.Key,
		)
	}
	rows, actual, err := decodeArchivePayload(
		ctx,
		payload,
		manifest.Schema,
		manifest.SchemaDigest,
		expected,
		maxRows,
		maxLogicalBytes,
	)
	if err != nil {
		return nil, ArchiveChunk{}, err
	}
	return rows, actual, nil
}

func readArchivePayload(
	ctx context.Context,
	store ArchiveStore,
	file ArchiveFile,
) ([]byte, error) {
	if file.Size == 0 || file.Size > maxArchivePayloadPhysicalBytes {
		return nil, fmt.Errorf(
			"Lifecycle archive payload %s size %d is outside the certified range",
			file.Key,
			file.Size,
		)
	}
	bounded, ok := store.(archiveManifestReadStore)
	if !ok {
		return nil, fmt.Errorf(
			"Lifecycle Archive Store does not support bounded payload reads",
		)
	}
	before, err := bounded.Stat(ctx, file.Key)
	if err != nil {
		return nil, err
	}
	if before != int64(file.Size) {
		return nil, fmt.Errorf(
			"Lifecycle archive payload %s size changed from %d to %d",
			file.Key,
			file.Size,
			before,
		)
	}
	payload, err := bounded.GetExact(ctx, file.Key, before)
	if err != nil {
		return nil, err
	}
	after, err := bounded.Stat(ctx, file.Key)
	if err != nil {
		return nil, err
	}
	if after != before {
		return nil, fmt.Errorf(
			"Lifecycle archive payload %s changed during bounded read",
			file.Key,
		)
	}
	return payload, nil
}

func verifyArchivePayload(
	ctx context.Context,
	payload []byte,
	schema SchemaDescriptor,
	schemaDigest [32]byte,
	expected ArchiveChunk,
) (ArchiveChunk, error) {
	_, actual, err := decodeArchivePayload(
		ctx,
		payload,
		schema,
		schemaDigest,
		expected,
		expected.RowCount,
		expected.LogicalBytes,
	)
	return actual, err
}

func decodeArchivePayload(
	ctx context.Context,
	payload []byte,
	schema SchemaDescriptor,
	schemaDigest [32]byte,
	expected ArchiveChunk,
	maxRows uint64,
	maxLogicalBytes uint64,
) ([][]CanonicalCell, ArchiveChunk, error) {
	file, err := parquet.OpenFile(bytes.NewReader(payload), int64(len(payload)))
	if err != nil {
		return nil, ArchiveChunk{}, err
	}
	if len(file.RowGroups()) != 1 {
		return nil, ArchiveChunk{}, fmt.Errorf("Lifecycle payload must contain exactly one row group")
	}
	reader := parquet.NewGenericReader[any](file)
	defer reader.Close()
	encoder := NewCanonicalValueEncoder(schemaDigest)
	buffer := make([]any, 256)
	rows := make([][]CanonicalCell, 0, min(expected.RowCount, maxRows))
	for {
		if err := ctx.Err(); err != nil {
			return nil, ArchiveChunk{}, err
		}
		count, readErr := reader.Read(buffer)
		for index := 0; index < count; index++ {
			cells, err := archiveParquetRowToCanonical(schema, buffer[index])
			if err != nil {
				return nil, ArchiveChunk{}, err
			}
			if err := encoder.WriteRow(ctx, cells); err != nil {
				return nil, ArchiveChunk{}, err
			}
			if encoder.RowCount() > maxRows ||
				encoder.LogicalBytes() > maxLogicalBytes {
				return nil, ArchiveChunk{}, fmt.Errorf(
					"Lifecycle Restore decoded chunk exceeds the certified limit",
				)
			}
			rows = append(rows, cells)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, ArchiveChunk{}, readErr
		}
	}
	actual := expected
	actual.RowCount = encoder.RowCount()
	actual.LogicalBytes = encoder.LogicalBytes()
	actual.CanonicalContentHash = encoder.Sum()
	if actual.RowCount != expected.RowCount ||
		actual.LogicalBytes != expected.LogicalBytes ||
		actual.CanonicalContentHash != expected.CanonicalContentHash {
		return nil, ArchiveChunk{}, fmt.Errorf(
			"Lifecycle archive row group %d content mismatch",
			expected.ChunkOrdinal,
		)
	}
	return rows, actual, nil
}

func archiveParquetRowToCanonical(
	schema SchemaDescriptor,
	value any,
) ([]CanonicalCell, error) {
	row, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("unexpected Lifecycle Parquet row %T", value)
	}
	cells := make([]CanonicalCell, len(schema.Columns))
	for index, column := range schema.Columns {
		raw, ok := row[archiveParquetColumnName(column)]
		if !ok {
			return nil, fmt.Errorf("Lifecycle Parquet row misses column %s", column.Name)
		}
		cell := CanonicalCell{
			Type: types.Type{
				Oid:   types.T(column.TypeID),
				Width: column.Width,
				Scale: column.Scale,
			},
			Null: raw == nil,
		}
		var err error
		if raw != nil {
			cell.Value, err = parquetValueToCanonical(column, raw)
			if err != nil {
				return nil, err
			}
		}
		cells[index] = cell
	}
	return cells, nil
}

func parquetValueToCanonical(column SchemaColumn, raw any) (any, error) {
	oid := types.T(column.TypeID)
	switch oid {
	case types.T_bool:
		return parquetBool(raw)
	case types.T_int8:
		value, err := parquetInt64(raw)
		return int8(value), err
	case types.T_int16:
		value, err := parquetInt64(raw)
		return int16(value), err
	case types.T_int32:
		value, err := parquetInt64(raw)
		return int32(value), err
	case types.T_int64:
		return parquetInt64(raw)
	case types.T_uint8:
		value, err := parquetUint64(raw)
		return uint8(value), err
	case types.T_uint16:
		value, err := parquetUint64(raw)
		return uint16(value), err
	case types.T_uint32:
		value, err := parquetUint64(raw)
		return uint32(value), err
	case types.T_bit, types.T_uint64:
		return parquetUint64(raw)
	case types.T_float32:
		value, err := parquetFloat64(raw)
		return float32(value), err
	case types.T_float64:
		return parquetFloat64(raw)
	case types.T_char, types.T_varchar, types.T_text:
		return []byte(parquetString(raw)), nil
	case types.T_binary, types.T_varbinary, types.T_blob:
		switch value := raw.(type) {
		case []byte:
			return append([]byte(nil), value...), nil
		case string:
			return []byte(value), nil
		default:
			return nil, fmt.Errorf("unexpected binary Parquet value %T", raw)
		}
	case types.T_json:
		return []byte(parquetString(raw)), nil
	case types.T_date:
		return types.ParseDateCast(parquetString(raw))
	case types.T_datetime:
		return types.ParseDatetime(parquetString(raw), column.Scale)
	case types.T_timestamp:
		return types.ParseTimestamp(time.UTC, parquetString(raw), column.Scale)
	case types.T_time:
		return types.ParseTime(parquetString(raw), column.Scale)
	case types.T_decimal64:
		return types.ParseDecimal64(parquetString(raw), column.Width, column.Scale)
	case types.T_decimal128:
		return types.ParseDecimal128(parquetString(raw), column.Width, column.Scale)
	case types.T_decimal256:
		return types.ParseDecimal256(parquetString(raw), column.Width, column.Scale)
	case types.T_uuid:
		return types.ParseUuid(parquetString(raw))
	case types.T_enum:
		value, err := parquetUint64(raw)
		if err != nil {
			return nil, err
		}
		return types.Enum(value), nil
	default:
		return nil, fmt.Errorf("unsupported Lifecycle Parquet type %s", oid)
	}
}

func parquetString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(value)
	}
}

func parquetBool(value any) (bool, error) {
	typed, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("unexpected bool Parquet value %T", value)
	}
	return typed, nil
}

func parquetInt64(value any) (int64, error) {
	switch typed := value.(type) {
	case int:
		return int64(typed), nil
	case int32:
		return int64(typed), nil
	case int64:
		return typed, nil
	case uint32:
		return int64(typed), nil
	case uint64:
		return checkedInt64(typed)
	default:
		return 0, fmt.Errorf("unexpected integer Parquet value %T", value)
	}
}

func parquetUint64(value any) (uint64, error) {
	switch typed := value.(type) {
	case int:
		if typed < 0 {
			return 0, fmt.Errorf("negative unsigned Parquet value")
		}
		return uint64(typed), nil
	case int32:
		if typed < 0 {
			return 0, fmt.Errorf("negative unsigned Parquet value")
		}
		return uint64(typed), nil
	case int64:
		if typed < 0 {
			return 0, fmt.Errorf("negative unsigned Parquet value")
		}
		return uint64(typed), nil
	case uint32:
		return uint64(typed), nil
	case uint64:
		return typed, nil
	default:
		return 0, fmt.Errorf("unexpected unsigned Parquet value %T", value)
	}
}

func parquetFloat64(value any) (float64, error) {
	switch typed := value.(type) {
	case float32:
		return float64(typed), nil
	case float64:
		return typed, nil
	default:
		return 0, fmt.Errorf("unexpected float Parquet value %T", value)
	}
}

func manifestDigestFromKey(key string) ([32]byte, error) {
	base := path.Base(key)
	if !strings.HasPrefix(base, "manifest-") || !strings.HasSuffix(base, ".json") {
		return [32]byte{}, fmt.Errorf("invalid Lifecycle manifest key %s", key)
	}
	value := strings.TrimSuffix(strings.TrimPrefix(base, "manifest-"), ".json")
	if len(value) != sha256.Size*2 {
		return [32]byte{}, fmt.Errorf("invalid Lifecycle manifest digest %s", value)
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return [32]byte{}, err
	}
	var digest [32]byte
	copy(digest[:], decoded)
	return digest, nil
}

func parseArchiveOrdinal(value string) (uint64, error) {
	return strconv.ParseUint(value, 10, 64)
}
