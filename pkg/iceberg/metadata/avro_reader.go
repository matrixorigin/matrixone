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
	"compress/flate"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/golang/snappy"
	"github.com/hamba/avro/v2"
	"github.com/klauspost/compress/zstd"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

const (
	defaultOCFDecodedBytesLimit int64 = 256 << 20
	defaultOCFRecordLimit             = 1_000_000
	ocfMetadataEntryMemoryBytes int64 = 64
	ocfDecodedRecordMemoryBytes int64 = 1024
	ocfDeflateScratchBytes      int64 = 64 << 10
	ocfZstdFixedScratchBytes    int64 = 128 << 10
)

func ReadManifestList(data []byte) ([]api.ManifestFile, error) {
	return ReadManifestListFromReader(bytes.NewReader(data))
}

func ReadManifestListFromReader(r io.Reader) ([]api.ManifestFile, error) {
	return ReadManifestListFromReaderBounded(r, 0)
}

func ReadManifestListFromReaderBounded(r io.Reader, maxRecords int) ([]api.ManifestFile, error) {
	return ReadManifestListFromReaderWithLimits(r, maxRecords, defaultOCFDecodedBytesLimit)
}

func ReadManifestListFromReaderWithLimits(r io.Reader, maxRecords int, maxDecodedBytes int64) ([]api.ManifestFile, error) {
	out := make([]api.ManifestFile, 0)
	err := readOCFRecords(r, "manifest_list", maxRecords, maxDecodedBytes, func(i int, record map[string]any) error {
		manifest, err := manifestFileFromRecord(record)
		if err != nil {
			return api.WrapError(api.ErrMetadataInvalid, "Iceberg manifest list entry is invalid", map[string]string{"entry": strconv.Itoa(i)}, err)
		}
		out = append(out, manifest)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadManifest(data []byte) ([]api.ManifestEntry, error) {
	return ReadManifestFromReader(bytes.NewReader(data))
}

func ReadManifestFromReader(r io.Reader) ([]api.ManifestEntry, error) {
	return ReadManifestFromReaderBounded(r, 0)
}

func ReadManifestFromReaderBounded(r io.Reader, maxRecords int) ([]api.ManifestEntry, error) {
	return ReadManifestFromReaderWithLimits(r, maxRecords, defaultOCFDecodedBytesLimit)
}

func ReadManifestFromReaderWithLimits(r io.Reader, maxRecords int, maxDecodedBytes int64) ([]api.ManifestEntry, error) {
	out := make([]api.ManifestEntry, 0)
	err := readOCFRecords(r, "manifest", maxRecords, maxDecodedBytes, func(i int, record map[string]any) error {
		entry, err := manifestEntryFromRecord(record)
		if err != nil {
			return api.WrapError(api.ErrMetadataInvalid, "Iceberg manifest entry is invalid", map[string]string{"entry": strconv.Itoa(i)}, err)
		}
		out = append(out, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func readOCFRecords(r io.Reader, operation string, maxRecords int, maxDecodedBytes int64, visit func(int, map[string]any) error) error {
	if r == nil {
		return api.NewError(api.ErrMetadataInvalid, "Iceberg Avro OCF reader is nil", map[string]string{"operation": operation})
	}
	if maxRecords <= 0 {
		maxRecords = defaultOCFRecordLimit
	}
	if maxDecodedBytes <= 0 {
		return ocfPlanningLimit(operation, "decoded_bytes", maxDecodedBytes)
	}
	// Even a tiny encoded record becomes several Go maps/slices/interfaces.
	// Tie record fanout to the same memory limit so an OCF containing millions
	// of nearly-empty records cannot bypass the byte bound via allocator
	// overhead. Direct callers may request a smaller explicit record limit.
	memoryRecordLimit := maxDecodedBytes / ocfDecodedRecordMemoryBytes
	if memoryRecordLimit < 1 {
		return ocfPlanningLimit(operation, "record_memory", maxDecodedBytes)
	}
	if memoryRecordLimit < int64(maxRecords) {
		maxRecords = int(memoryRecordLimit)
	}
	reader := bufio.NewReaderSize(r, 4096)
	header, err := readBoundedOCFHeader(reader, operation, maxDecodedBytes)
	if err != nil {
		return err
	}
	schema, err := avro.Parse(string(header.schema))
	if err != nil {
		return api.WrapError(api.ErrMetadataInvalid, "Iceberg Avro OCF schema is invalid", map[string]string{"operation": operation}, err)
	}
	configLimit := maxDecodedBytes
	if configLimit > int64(math.MaxInt) {
		configLimit = int64(math.MaxInt)
	}
	sliceLimit := configLimit / 16
	if sliceLimit < 1 {
		sliceLimit = 1
	}
	decoderConfig := avro.Config{
		MaxByteSliceSize:  int(configLimit),
		MaxSliceAllocSize: int(sliceLimit),
	}.Freeze()
	recordIndex := 0
	decodedBytes := int64(0)
	for {
		count, err := readAvroLong(reader)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return api.WrapError(api.ErrMetadataInvalid, "Iceberg Avro OCF block count is invalid", map[string]string{"operation": operation}, err)
		}
		if count <= 0 {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg Avro OCF block has a non-positive record count", map[string]string{"operation": operation})
		}
		size, err := readAvroLong(reader)
		if err != nil {
			return api.WrapError(api.ErrMetadataInvalid, "Iceberg Avro OCF block size is invalid", map[string]string{"operation": operation}, err)
		}
		if size < 0 {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg Avro OCF block has a negative size", map[string]string{"operation": operation})
		}
		if count > int64(maxRecords-recordIndex) {
			return ocfPlanningLimit(operation, "records", int64(maxRecords))
		}
		remaining := maxDecodedBytes - decodedBytes
		if remaining <= 0 || size > remaining || size > int64(math.MaxInt) {
			return ocfPlanningLimit(operation, "decoded_bytes", maxDecodedBytes)
		}
		block := make([]byte, int(size))
		if _, err := io.ReadFull(reader, block); err != nil {
			return api.WrapError(api.ErrMetadataInvalid, "Iceberg Avro OCF block is truncated", map[string]string{"operation": operation}, err)
		}
		var sync [16]byte
		if _, err := io.ReadFull(reader, sync[:]); err != nil {
			return api.WrapError(api.ErrMetadataInvalid, "Iceberg Avro OCF sync marker is truncated", map[string]string{"operation": operation}, err)
		}
		if sync != header.sync {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg Avro OCF sync marker does not match", map[string]string{"operation": operation})
		}
		decoded, err := decodeOCFBlock(header.codec, block, remaining, operation)
		if err != nil {
			return err
		}
		decodedBytes += int64(len(decoded))
		decoder := decoderConfig.NewDecoder(schema, bytes.NewReader(decoded))
		for i := int64(0); i < count; i++ {
			var raw any
			if err := decoder.Decode(&raw); err != nil {
				return api.WrapError(api.ErrMetadataInvalid, "Iceberg Avro OCF record decode failed", map[string]string{"operation": operation}, err)
			}
			record, ok := raw.(map[string]any)
			if !ok {
				return api.NewError(api.ErrMetadataInvalid, "Iceberg Avro OCF record is not a map", map[string]string{"operation": operation})
			}
			if err := visit(recordIndex, record); err != nil {
				return err
			}
			recordIndex++
		}
		var trailing any
		if err := decoder.Decode(&trailing); err == nil {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg Avro OCF block contains more records than its declared count", map[string]string{"operation": operation})
		} else if !errors.Is(err, io.EOF) {
			return api.WrapError(api.ErrMetadataInvalid, "Iceberg Avro OCF block has trailing invalid data", map[string]string{"operation": operation}, err)
		}
	}
}

type boundedOCFHeader struct {
	schema []byte
	codec  string
	sync   [16]byte
}

func readBoundedOCFHeader(reader *bufio.Reader, operation string, maxBytes int64) (boundedOCFHeader, error) {
	var magic [4]byte
	if _, err := io.ReadFull(reader, magic[:]); err != nil {
		return boundedOCFHeader{}, api.WrapError(api.ErrMetadataInvalid, "Iceberg Avro OCF header is truncated", map[string]string{"operation": operation}, err)
	}
	if magic != [4]byte{'O', 'b', 'j', 1} {
		return boundedOCFHeader{}, api.NewError(api.ErrMetadataInvalid, "Iceberg Avro OCF magic is invalid", map[string]string{"operation": operation})
	}
	metadata := make(map[string][]byte)
	used := int64(len(magic))
	entries := 0
	for {
		count, err := readAvroLong(reader)
		if err != nil {
			return boundedOCFHeader{}, api.WrapError(api.ErrMetadataInvalid, "Iceberg Avro OCF metadata block is invalid", map[string]string{"operation": operation}, err)
		}
		if count == 0 {
			break
		}
		if count < 0 {
			if count == math.MinInt64 {
				return boundedOCFHeader{}, api.NewError(api.ErrMetadataInvalid, "Iceberg Avro OCF metadata count overflows", map[string]string{"operation": operation})
			}
			count = -count
			blockSize, err := readAvroLong(reader)
			if err != nil || blockSize < 0 {
				return boundedOCFHeader{}, api.WrapError(api.ErrMetadataInvalid, "Iceberg Avro OCF metadata block size is invalid", map[string]string{"operation": operation}, err)
			}
			if blockSize > maxBytes-used {
				return boundedOCFHeader{}, ocfPlanningLimit(operation, "header_bytes", maxBytes)
			}
		}
		if count > int64(defaultOCFRecordLimit-entries) {
			return boundedOCFHeader{}, ocfPlanningLimit(operation, "header_entries", defaultOCFRecordLimit)
		}
		for i := int64(0); i < count; i++ {
			// Account for the map bucket, string/slice headers and allocator
			// metadata even when a malicious OCF uses zero-length keys/values.
			if used > maxBytes-ocfMetadataEntryMemoryBytes {
				return boundedOCFHeader{}, ocfPlanningLimit(operation, "header_bytes", maxBytes)
			}
			used += ocfMetadataEntryMemoryBytes
			key, err := readBoundedAvroBytes(reader, maxBytes-used, operation)
			if err != nil {
				if isPlanningLimitError(err) {
					return boundedOCFHeader{}, err
				}
				return boundedOCFHeader{}, api.WrapError(api.ErrMetadataInvalid, "Iceberg Avro OCF metadata key is invalid", map[string]string{"operation": operation}, err)
			}
			used += int64(len(key))
			value, err := readBoundedAvroBytes(reader, maxBytes-used, operation)
			if err != nil {
				if isPlanningLimitError(err) {
					return boundedOCFHeader{}, err
				}
				return boundedOCFHeader{}, api.WrapError(api.ErrMetadataInvalid, "Iceberg Avro OCF metadata value is invalid", map[string]string{"operation": operation}, err)
			}
			used += int64(len(value))
			metadata[string(key)] = value
			entries++
		}
	}
	var sync [16]byte
	if _, err := io.ReadFull(reader, sync[:]); err != nil {
		return boundedOCFHeader{}, api.WrapError(api.ErrMetadataInvalid, "Iceberg Avro OCF header sync marker is truncated", map[string]string{"operation": operation}, err)
	}
	schema := metadata["avro.schema"]
	if len(schema) == 0 {
		return boundedOCFHeader{}, api.NewError(api.ErrMetadataInvalid, "Iceberg Avro OCF header has no schema", map[string]string{"operation": operation})
	}
	return boundedOCFHeader{
		schema: schema,
		codec:  strings.TrimSpace(string(metadata["avro.codec"])),
		sync:   sync,
	}, nil
}

func readAvroLong(reader io.ByteReader) (int64, error) {
	var encoded uint64
	for shift := uint(0); shift < 64; shift += 7 {
		value, err := reader.ReadByte()
		if err != nil {
			if shift == 0 {
				return 0, err
			}
			return 0, io.ErrUnexpectedEOF
		}
		if shift == 63 && value > 1 {
			return 0, api.NewError(api.ErrMetadataInvalid, "Avro long overflows int64", nil)
		}
		encoded |= uint64(value&0x7f) << shift
		if value&0x80 == 0 {
			return int64(encoded>>1) ^ -int64(encoded&1), nil
		}
	}
	return 0, api.NewError(api.ErrMetadataInvalid, "Avro long is too long", nil)
}

func readBoundedAvroBytes(reader *bufio.Reader, maxBytes int64, operation string) ([]byte, error) {
	size, err := readAvroLong(reader)
	if err != nil {
		return nil, err
	}
	if size < 0 {
		return nil, api.NewError(api.ErrMetadataInvalid, "Avro byte string has a negative size", nil)
	}
	if size > maxBytes || size > int64(math.MaxInt) {
		return nil, ocfPlanningLimit(operation, "header_bytes", maxBytes)
	}
	data := make([]byte, int(size))
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, err
	}
	return data, nil
}

func decodeOCFBlock(codec string, block []byte, remaining int64, operation string) ([]byte, error) {
	switch codec {
	case "", "null":
		if int64(len(block)) > remaining {
			return nil, ocfPlanningLimit(operation, "decoded_bytes", remaining)
		}
		return block, nil
	case "deflate", "snappy", "zstandard":
	default:
		return nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg Avro OCF codec is unsupported", map[string]string{
			"operation": operation,
			"codec":     codec,
		})
	}
	decodeLimit := remaining - int64(len(block))
	if decodeLimit <= 0 {
		return nil, ocfPlanningLimit(operation, "decoded_bytes", remaining)
	}
	var decoded []byte
	var err error
	switch codec {
	case "deflate":
		if decodeLimit <= ocfDeflateScratchBytes {
			return nil, ocfPlanningLimit(operation, "decoded_bytes", remaining)
		}
		stream := flate.NewReader(bytes.NewReader(block))
		decoded, err = readDecodedBlock(stream, decodeLimit-ocfDeflateScratchBytes)
		err = errors.Join(err, stream.Close())
	case "snappy":
		if len(block) < 4 {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg Avro OCF snappy block has no checksum", map[string]string{"operation": operation})
		}
		payload := block[:len(block)-4]
		decodedSize, sizeErr := snappy.DecodedLen(payload)
		if sizeErr != nil {
			err = sizeErr
		} else if int64(decodedSize) > decodeLimit {
			return nil, ocfPlanningLimit(operation, "decoded_bytes", decodeLimit)
		} else {
			decoded, err = snappy.Decode(nil, payload)
			if err == nil && crc32.ChecksumIEEE(decoded) != binary.BigEndian.Uint32(block[len(block)-4:]) {
				err = api.NewError(api.ErrMetadataInvalid, "Iceberg Avro OCF snappy checksum does not match", nil)
			}
		}
	case "zstandard":
		workingMemory := decodeLimit - ocfZstdFixedScratchBytes
		if workingMemory <= 1 {
			return nil, ocfPlanningLimit(operation, "decoded_bytes", remaining)
		}
		outputLimit := workingMemory / 2
		decoderMemory := workingMemory - outputLimit
		stream, streamErr := zstd.NewReader(
			bytes.NewReader(block),
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxMemory(uint64(decoderMemory)),
			zstd.WithDecoderMaxWindow(uint64(decoderMemory)),
		)
		if streamErr != nil {
			err = streamErr
		} else {
			decoded, err = readDecodedBlock(stream, outputLimit)
			stream.Close()
		}
	}
	if err != nil {
		var icebergErr *api.IcebergError
		if errors.As(err, &icebergErr) {
			return nil, err
		}
		return nil, api.WrapError(api.ErrMetadataInvalid, "Iceberg Avro OCF block decompression failed", map[string]string{
			"operation": operation,
			"codec":     codec,
		}, err)
	}
	return decoded, nil
}

func readDecodedBlock(reader io.Reader, limit int64) ([]byte, error) {
	data, err := api.ReadAllBounded(reader, limit)
	if err != nil {
		if errors.Is(err, api.ErrMaterializationLimitExceeded) {
			return nil, api.NewError(api.ErrPlanningLimitExceeded, "Iceberg Avro OCF decoded block exceeds the memory limit", map[string]string{
				"limit_bytes": strconv.FormatInt(limit, 10),
			})
		}
		return nil, err
	}
	return data, nil
}

func ocfPlanningLimit(operation, dimension string, limit int64) error {
	return api.NewError(api.ErrPlanningLimitExceeded, "Iceberg Avro OCF exceeds the planning limit", map[string]string{
		"operation": operation,
		"dimension": dimension,
		"limit":     strconv.FormatInt(limit, 10),
	})
}

func manifestFileFromRecord(record map[string]any) (api.ManifestFile, error) {
	path, err := requiredString(record, "manifest_path")
	if err != nil {
		return api.ManifestFile{}, err
	}
	content := api.ManifestContentData
	if n, ok := optionalInt(record, "content"); ok {
		switch n {
		case 0:
		case 1:
			content = api.ManifestContentDeletes
		default:
			return api.ManifestFile{}, api.NewError(api.ErrMetadataInvalid, "Iceberg manifest content type is invalid", map[string]string{
				"path": api.RedactPath(path),
			})
		}
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
		if firstRowID < 0 {
			return api.ManifestFile{}, api.NewError(api.ErrMetadataInvalid, "Iceberg manifest first row id is negative", map[string]string{
				"path": api.RedactPath(path),
			})
		}
		manifest.FirstRowID = &firstRowID
	}
	if manifest.Length < 0 || manifest.PartitionSpecID < 0 || manifest.SequenceNumber < 0 || manifest.MinSequenceNumber < 0 || manifest.AddedSnapshotID < 0 ||
		manifest.AddedFilesCount < 0 || manifest.ExistingFilesCount < 0 || manifest.DeletedFilesCount < 0 ||
		manifest.AddedRowsCount < 0 || manifest.ExistingRowsCount < 0 || manifest.DeletedRowsCount < 0 ||
		manifest.AddedFilesSizeInBytes < 0 || manifest.ExistingFilesSizeInBytes < 0 || manifest.DeletedFilesSizeInBytes < 0 ||
		manifest.ReferencedDataFilesCount < 0 {
		return api.ManifestFile{}, api.NewError(api.ErrMetadataInvalid, "Iceberg manifest has negative sequence or file metrics", map[string]string{
			"path": api.RedactPath(path),
		})
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
	status := api.ManifestEntryStatus(optionalIntValue(record, "status"))
	if status < api.ManifestEntryExisting || status > api.ManifestEntryDeleted {
		return api.ManifestEntry{}, api.NewError(api.ErrMetadataInvalid, "Iceberg manifest entry status is invalid", map[string]string{
			"status": strconv.Itoa(int(status)),
		})
	}
	entry := api.ManifestEntry{
		Status:         status,
		SnapshotID:     optionalInt64Value(record, "snapshot_id"),
		SequenceNumber: optionalInt64Value(record, "sequence_number"),
		FileSequence:   optionalInt64Value(record, "file_sequence_number"),
		DataFile:       file,
	}
	if entry.SequenceNumber < 0 || entry.FileSequence < 0 {
		return api.ManifestEntry{}, api.NewError(api.ErrMetadataInvalid, "Iceberg manifest entry sequence number is negative", map[string]string{
			"file": api.RedactPath(file.FilePath),
		})
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
	if content < api.DataFileContentData || content > api.DataFileContentEqualityDelete {
		return api.DataFile{}, api.NewError(api.ErrMetadataInvalid, "Iceberg data file content type is invalid", map[string]string{
			"path": api.RedactPath(path),
		})
	}
	columnSizes, err := intLongMap(record["column_sizes"])
	if err != nil {
		return api.DataFile{}, err
	}
	valueCounts, err := intLongMap(record["value_counts"])
	if err != nil {
		return api.DataFile{}, err
	}
	nullValueCounts, err := intLongMap(record["null_value_counts"])
	if err != nil {
		return api.DataFile{}, err
	}
	nanValueCounts, err := intLongMap(record["nan_value_counts"])
	if err != nil {
		return api.DataFile{}, err
	}
	lowerBounds, err := intBytesMap(record["lower_bounds"])
	if err != nil {
		return api.DataFile{}, err
	}
	upperBounds, err := intBytesMap(record["upper_bounds"])
	if err != nil {
		return api.DataFile{}, err
	}
	splitOffsets, err := int64Slice(record["split_offsets"])
	if err != nil {
		return api.DataFile{}, err
	}
	equalityIDs, err := intSlice(record["equality_ids"])
	if err != nil {
		return api.DataFile{}, err
	}
	file := api.DataFile{
		Content:               content,
		FilePath:              path,
		FileFormat:            strings.ToLower(strings.TrimSpace(optionalStringValue(record, "file_format"))),
		Partition:             optionalRecordValue(record, "partition"),
		RecordCount:           optionalInt64Value(record, "record_count"),
		FileSizeInBytes:       optionalInt64Value(record, "file_size_in_bytes"),
		ColumnSizes:           columnSizes,
		ValueCounts:           valueCounts,
		NullValueCounts:       nullValueCounts,
		NaNValueCounts:        nanValueCounts,
		LowerBounds:           lowerBounds,
		UpperBounds:           upperBounds,
		SplitOffsets:          splitOffsets,
		EqualityIDs:           equalityIDs,
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
		if firstRowID < 0 {
			return api.DataFile{}, api.NewError(api.ErrMetadataInvalid, "Iceberg data file first row id is negative", map[string]string{"path": api.RedactPath(path)})
		}
		file.FirstRowID = &firstRowID
	}
	if deletionVector := unwrapUnion(record["deletion_vector"]); deletionVector != nil {
		file.DeletionVectorPath = "present"
	}
	if file.RecordCount < 0 || file.FileSizeInBytes < 0 {
		return api.DataFile{}, api.NewError(api.ErrMetadataInvalid, "Iceberg data file has negative metrics", map[string]string{
			"path": api.RedactPath(path),
		})
	}
	if file.RecordCount > 0 && file.FileSizeInBytes == 0 {
		return api.DataFile{}, api.NewError(api.ErrMetadataInvalid, "Iceberg data file with rows is missing file size", map[string]string{
			"path": api.RedactPath(path),
		})
	}
	if err := validateDataFileIntRanges(file); err != nil {
		return api.DataFile{}, err
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

func intLongMap(raw any) (map[int]int64, error) {
	unwrapped := unwrapUnion(raw)
	if unwrapped == nil {
		return nil, nil
	}
	values, ok := unwrapped.([]any)
	if !ok {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg Avro integer metric map is invalid", nil)
	}
	if len(values) == 0 {
		return nil, nil
	}
	out := make(map[int]int64, len(values))
	for _, value := range values {
		record, ok := unwrapUnion(value).(map[string]any)
		if !ok {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg Avro integer metric entry is invalid", nil)
		}
		key, keyOK := optionalInt(record, "key")
		val, valOK := optionalInt64(record, "value")
		if !keyOK || !valOK || key <= 0 || val < 0 {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg Avro integer metric entry has an invalid key or value", nil)
		}
		if _, exists := out[key]; exists {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg Avro integer metric map has a duplicate key", nil)
		}
		out[key] = val
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func intBytesMap(raw any) (map[int][]byte, error) {
	unwrapped := unwrapUnion(raw)
	if unwrapped == nil {
		return nil, nil
	}
	values, ok := unwrapped.([]any)
	if !ok {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg Avro bound map is invalid", nil)
	}
	if len(values) == 0 {
		return nil, nil
	}
	out := make(map[int][]byte, len(values))
	for _, value := range values {
		record, ok := unwrapUnion(value).(map[string]any)
		if !ok {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg Avro bound entry is invalid", nil)
		}
		key, keyOK := optionalInt(record, "key")
		val := bytesFromAny(unwrapUnion(record["value"]))
		if !keyOK || key <= 0 || val == nil {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg Avro bound entry has an invalid key or value", nil)
		}
		if _, exists := out[key]; exists {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg Avro bound map has a duplicate key", nil)
		}
		out[key] = val
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func int64Slice(raw any) ([]int64, error) {
	unwrapped := unwrapUnion(raw)
	if unwrapped == nil {
		return nil, nil
	}
	values, ok := unwrapped.([]any)
	if !ok {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg Avro long list is invalid", nil)
	}
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]int64, 0, len(values))
	for _, value := range values {
		n, ok := int64FromAny(unwrapUnion(value))
		if !ok || n < 0 {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg Avro long list contains an invalid value", nil)
		}
		out = append(out, n)
	}
	return out, nil
}

func intSlice(raw any) ([]int, error) {
	unwrapped := unwrapUnion(raw)
	if unwrapped == nil {
		return nil, nil
	}
	values, ok := unwrapped.([]any)
	if !ok {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg Avro integer list is invalid", nil)
	}
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]int, 0, len(values))
	for _, value := range values {
		n, ok := intFromAny(unwrapUnion(value))
		if !ok || n <= 0 {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg Avro integer list contains an invalid value", nil)
		}
		out = append(out, n)
	}
	return out, nil
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
		converted := int(v)
		return converted, int64(converted) == v
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
