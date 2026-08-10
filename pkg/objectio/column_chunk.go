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

package objectio

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"

	"github.com/pierrec/lz4/v4"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

const (
	columnChunkTargetBytes         = 8 << 20
	columnChunkMaxExtraOriginBytes = columnChunkTargetBytes
	columnChunkHeaderSize          = 16
	columnChunkEntrySize           = 24
)

var columnChunkMagic = [8]byte{'M', 'O', 'C', 'O', 'L', 'C', 'H', '1'}

type columnChunkMeta struct {
	rowStart, rowCount uint32
	offset, length     uint32
	originSize         uint32
	algorithm          uint8
}

func marshalColumnVectorWindow(
	vec *vector.Vector, start, end int, mp *mpool.MPool,
) ([]byte, error) {
	window, err := vec.CloneWindowWithAllocation(start, end, mp, nil)
	if err != nil {
		return nil, err
	}
	defer window.Free(mp)
	var buf bytes.Buffer
	header := IOEntryHeader{Type: IOET_ColData, Version: IOET_ColumnData_CurrVer}
	buf.Write(EncodeIOEntryHeader(&header))
	if err = window.MarshalBinaryWithBuffer(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// encodeChunkedColumn returns chunked=false when the source representation
// cannot be partitioned without unbounded physical amplification. The caller
// must retain the already-serialized legacy column in that case.
func encodeChunkedColumn(vec *vector.Vector) ([]byte, bool, error) {
	if vec == nil || vec.Length() == 0 {
		return nil, false, moerr.NewInvalidInputNoCtx("cannot chunk an empty object column")
	}
	// A non-disjoint varlen area may contain many descriptors for the same
	// payload (for example UnionBatch of a broadcast constant). Window cloning
	// would materialize that shared payload once per logical row. Keep the
	// compact legacy representation instead of multiplying it by row count.
	if vec.GetType().IsVarlen() && !vec.VarlenaAreaIsDisjoint() {
		return nil, false, nil
	}
	fullSize, err := vec.MarshalBinarySize()
	if err != nil {
		return nil, false, err
	}
	fullSize += IOEntryHeaderSize
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	rowsPerChunk := max(1, int(int64(vec.Length())*columnChunkTargetBytes/int64(max(1, fullSize))))
	metas := make([]columnChunkMeta, 0, (vec.Length()+rowsPerChunk-1)/rowsPerChunk)
	payloads := make([][]byte, 0, cap(metas))
	totalOriginSize := 0
	for start := 0; start < vec.Length(); {
		end := min(vec.Length(), start+rowsPerChunk)
		var encoded []byte
		for {
			encoded, err = marshalColumnVectorWindow(vec, start, end, mp)
			if err != nil {
				return nil, false, err
			}
			if len(encoded) <= columnChunkTargetBytes {
				break
			}
			if end-start == 1 {
				return nil, false, nil
			}
			end = start + max(1, (end-start)/2)
		}
		totalOriginSize += len(encoded)
		// Even representations marked disjoint may carry per-window metadata.
		// Bound all such overhead additively before allocating compressed output;
		// this is a defensive backstop against future vector representations.
		if totalOriginSize > fullSize+columnChunkMaxExtraOriginBytes {
			return nil, false, nil
		}
		bound := lz4.CompressBlockBound(len(encoded))
		compressed := make([]byte, bound)
		n, compressErr := lz4.CompressBlock(encoded, compressed, nil)
		if compressErr != nil {
			return nil, false, compressErr
		}
		algorithm := uint8(compress.Lz4)
		if n == 0 || n >= len(encoded) {
			compressed = bytes.Clone(encoded)
			n = len(compressed)
			algorithm = uint8(compress.None)
		} else {
			compressed = compressed[:n]
		}
		metas = append(metas, columnChunkMeta{
			rowStart: uint32(start), rowCount: uint32(end - start),
			length: uint32(n), originSize: uint32(len(encoded)), algorithm: algorithm,
		})
		payloads = append(payloads, compressed)
		start = end
	}
	headerSize := columnChunkHeaderSize + len(metas)*columnChunkEntrySize
	totalSize := headerSize
	for _, payload := range payloads {
		totalSize += len(payload)
	}
	output := make([]byte, totalSize)
	copy(output[:8], columnChunkMagic[:])
	binary.LittleEndian.PutUint32(output[8:12], uint32(vec.Length()))
	binary.LittleEndian.PutUint32(output[12:16], uint32(len(metas)))
	offset := headerSize
	for i := range metas {
		metas[i].offset = uint32(offset)
		encodeColumnChunkMeta(output[columnChunkHeaderSize+i*columnChunkEntrySize:], metas[i])
		copy(output[offset:], payloads[i])
		offset += len(payloads[i])
	}
	return output, true, nil
}

func encodeColumnChunkMeta(dst []byte, meta columnChunkMeta) {
	binary.LittleEndian.PutUint32(dst[0:4], meta.rowStart)
	binary.LittleEndian.PutUint32(dst[4:8], meta.rowCount)
	binary.LittleEndian.PutUint32(dst[8:12], meta.offset)
	binary.LittleEndian.PutUint32(dst[12:16], meta.length)
	binary.LittleEndian.PutUint32(dst[16:20], meta.originSize)
	dst[20] = meta.algorithm
}

func parseColumnChunkHeader(data []byte, extentLength uint32) (uint32, []columnChunkMeta, error) {
	if len(data) < columnChunkHeaderSize || !bytes.Equal(data[:8], columnChunkMagic[:]) {
		return 0, nil, moerr.NewInvalidInputNoCtx("invalid chunked object column header")
	}
	totalRows := binary.LittleEndian.Uint32(data[8:12])
	count := binary.LittleEndian.Uint32(data[12:16])
	headerSize := uint64(columnChunkHeaderSize) + uint64(count)*columnChunkEntrySize
	if headerSize > uint64(len(data)) {
		return 0, nil, io.ErrUnexpectedEOF
	}
	if totalRows == 0 || count == 0 || headerSize > uint64(extentLength) {
		return 0, nil, moerr.NewInvalidInputNoCtx("invalid chunked object column size")
	}
	metas := make([]columnChunkMeta, count)
	var expectedRow uint64
	expectedOffset := headerSize
	for i := range metas {
		src := data[columnChunkHeaderSize+i*columnChunkEntrySize:]
		metas[i] = columnChunkMeta{
			rowStart:   binary.LittleEndian.Uint32(src[0:4]),
			rowCount:   binary.LittleEndian.Uint32(src[4:8]),
			offset:     binary.LittleEndian.Uint32(src[8:12]),
			length:     binary.LittleEndian.Uint32(src[12:16]),
			originSize: binary.LittleEndian.Uint32(src[16:20]), algorithm: src[20],
		}
		meta := metas[i]
		if uint64(meta.rowStart) != expectedRow || meta.rowCount == 0 ||
			uint64(meta.offset) != expectedOffset ||
			uint64(meta.offset)+uint64(meta.length) > uint64(extentLength) ||
			(meta.algorithm != uint8(compress.None) && meta.algorithm != uint8(compress.Lz4)) {
			return 0, nil, moerr.NewInvalidInputNoCtx("invalid chunked object column entry")
		}
		expectedRow += uint64(meta.rowCount)
		expectedOffset += uint64(meta.length)
	}
	if expectedRow != uint64(totalRows) || expectedOffset != uint64(extentLength) {
		return 0, nil, moerr.NewInvalidInputNoCtx("chunked object column row count mismatch")
	}
	return totalRows, metas, nil
}

func decompressColumnChunk(data []byte, meta columnChunkMeta) ([]byte, error) {
	if meta.algorithm == uint8(compress.None) {
		if uint32(len(data)) != meta.originSize {
			return nil, moerr.NewInvalidInputNoCtx("invalid uncompressed object column chunk")
		}
		return bytes.Clone(data), nil
	}
	output := make([]byte, meta.originSize)
	n, err := lz4.UncompressBlock(data, output)
	if err != nil {
		return nil, err
	}
	if n != len(output) {
		return nil, moerr.NewInvalidInputNoCtx("chunked object column decompressed size mismatch")
	}
	return output, nil
}

func decodeChunkedColumn(
	ctx context.Context,
	data []byte,
	allocator fileservice.CacheDataAllocator,
) (fscache.Data, error) {
	_, metas, err := parseColumnChunkHeader(data, uint32(len(data)))
	if err != nil {
		return nil, err
	}
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	var dst *vector.Vector
	defer func() {
		if dst != nil {
			dst.Free(mp)
		}
	}()
	for _, meta := range metas {
		chunk, chunkErr := decompressColumnChunk(data[meta.offset:meta.offset+meta.length], meta)
		if chunkErr != nil {
			return nil, chunkErr
		}
		decoded, decodeErr := Decode(chunk)
		if decodeErr != nil {
			return nil, decodeErr
		}
		source, ok := decoded.(*vector.Vector)
		if !ok {
			return nil, moerr.NewInvalidInputNoCtx("chunked object column does not contain a vector")
		}
		if dst == nil {
			dst = vector.NewVec(*source.GetType())
		}
		if err = dst.UnionBatch(source, 0, source.Length(), nil, mp); err != nil {
			return nil, err
		}
	}
	var encoded bytes.Buffer
	header := IOEntryHeader{Type: IOET_ColData, Version: IOET_ColumnData_CurrVer}
	encoded.Write(EncodeIOEntryHeader(&header))
	if err = dst.MarshalBinaryWithBuffer(&encoded); err != nil {
		return nil, err
	}
	return allocator.CopyToCacheData(ctx, encoded.Bytes()), nil
}

func chunkedColumnHeaderReadSize(prefix []byte, extentLength uint32) (int, error) {
	if len(prefix) < columnChunkHeaderSize || !bytes.Equal(prefix[:8], columnChunkMagic[:]) {
		return 0, moerr.NewInvalidInputNoCtx("invalid chunked object column prefix")
	}
	count := binary.LittleEndian.Uint32(prefix[12:16])
	size := uint64(columnChunkHeaderSize) + uint64(count)*columnChunkEntrySize
	if size > uint64(extentLength) || size > uint64(^uint(0)>>1) {
		return 0, moerr.NewInvalidInputNoCtx("chunked object column header is too large")
	}
	return int(size), nil
}
