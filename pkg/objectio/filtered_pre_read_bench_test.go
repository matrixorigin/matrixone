//go:build linux || darwin

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"slices"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

const (
	filteredPRERows = 8192
	filteredPREDims = 768
)

// The smaller-chunk encoder is test-only. It uses the same on-disk chunk
// directory, window serialization, and per-window LZ4 decision as the writer;
// only the target size differs. The 8 MiB case uses encodeChunkedColumn itself.
func encodeFilteredPREChunks(source *vector.Vector, target int) ([]byte, int, error) {
	fullSize, err := source.MarshalBinarySize()
	if err != nil {
		return nil, 0, err
	}
	fullSize += IOEntryHeaderSize
	rowsPerChunk := max(1, int(int64(source.Length())*int64(target)/int64(max(1, fullSize))))
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	metas := make([]columnChunkMeta, 0, (source.Length()+rowsPerChunk-1)/rowsPerChunk)
	payloads := make([][]byte, 0, cap(metas))
	totalOriginSize := 0
	for start := 0; start < source.Length(); {
		end := min(source.Length(), start+rowsPerChunk)
		var encoded []byte
		for {
			encoded, err = marshalColumnVectorWindow(source, start, end, mp)
			if err != nil {
				return nil, 0, err
			}
			if len(encoded) <= target {
				break
			}
			if end-start == 1 {
				return nil, 0, fmt.Errorf("single vector row exceeds %d-byte chunk target", target)
			}
			end = start + max(1, (end-start)/2)
		}
		totalOriginSize += len(encoded)
		if totalOriginSize > fullSize+columnChunkMaxExtraOriginBytes {
			return nil, 0, fmt.Errorf("chunk windows exceed bounded source overhead")
		}
		compressed := make([]byte, lz4.CompressBlockBound(len(encoded)))
		n, err := lz4.CompressBlock(encoded, compressed, nil)
		if err != nil {
			return nil, 0, err
		}
		algorithm := uint8(compress.Lz4)
		if n == 0 || n >= len(encoded) {
			compressed, n, algorithm = bytes.Clone(encoded), len(encoded), compress.None
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
	copy(output, columnChunkMagic[:])
	binary.LittleEndian.PutUint32(output[8:12], uint32(source.Length()))
	binary.LittleEndian.PutUint32(output[12:16], uint32(len(metas)))
	offset := headerSize
	for i := range metas {
		metas[i].offset = uint32(offset)
		encodeColumnChunkMeta(output[columnChunkHeaderSize+i*columnChunkEntrySize:], metas[i])
		copy(output[offset:], payloads[i])
		offset += len(payloads[i])
	}
	return output, len(metas), nil
}

func newFilteredPREVector(t testing.TB, mp *mpool.MPool) *vector.Vector {
	t.Helper()
	source := vector.NewVec(types.T_array_float32.ToType())
	t.Cleanup(func() { source.Free(mp) })
	values := make([]float32, filteredPREDims)
	var state uint32 = 0x91e10da5
	for row := 0; row < filteredPRERows; row++ {
		for dim := range values {
			state ^= state << 13
			state ^= state >> 17
			state ^= state << 5
			values[dim] = float32(int32(state)) / float32(1<<31)
		}
		// Exact zero and a selected equal-distance pair pin ordering and ties.
		if row == 1501 {
			clear(values)
		} else if row == 101 || row == 701 {
			for dim := range values {
				values[dim] = 0.001
			}
		}
		require.NoError(t, vector.AppendArray(source, values, false, mp))
	}
	return source
}

func BenchmarkFilteredPREReadGranularity(b *testing.B) {
	ctx := context.Background()
	mp := newTopNTestMP(b)
	source := newFilteredPREVector(b, mp)
	fs, err := fileservice.NewS3FS(ctx, fileservice.ObjectStorageArguments{
		Name: "filtered-pre-read", Endpoint: "disk", Bucket: b.TempDir(),
	}, fileservice.DisabledCacheConfig, nil, false, false)
	require.NoError(b, err)
	b.Cleanup(func() { fs.Close(ctx) })
	chunk8M, ok, err := encodeChunkedColumn(source)
	require.NoError(b, err)
	require.True(b, ok)
	matching8M, _, err := encodeFilteredPREChunks(source, columnChunkTargetBytes)
	require.NoError(b, err)
	require.Equal(b, chunk8M, matching8M, "test encoder must match writer bytes at the real chunk target")
	chunk256K, smallChunkCount, err := encodeFilteredPREChunks(source, 256<<10)
	require.NoError(b, err)
	chunk512K, mediumChunkCount, err := encodeFilteredPREChunks(source, 512<<10)
	require.NoError(b, err)
	chunk1M, largeChunkCount, err := encodeFilteredPREChunks(source, 1<<20)
	require.NoError(b, err)
	for _, payload := range [][]byte{chunk8M, chunk256K, chunk512K, chunk1M} {
		rows, _, err := parseColumnChunkHeader(payload, uint32(len(payload)))
		require.NoError(b, err)
		require.Equal(b, uint32(filteredPRERows), rows)
	}
	formats := []struct {
		name, method string
		payload      []byte
		chunks       int
	}{
		{name: "legacy", method: "whole"},
		{name: "chunk8M", method: "stream", payload: chunk8M, chunks: int(binary.LittleEndian.Uint32(chunk8M[12:16]))},
		{name: "chunk1M", method: "stream", payload: chunk1M, chunks: largeChunkCount},
		{name: "chunk512K", method: "stream", payload: chunk512K, chunks: mediumChunkCount},
		{name: "chunk256K", method: "stream", payload: chunk256K, chunks: smallChunkCount},
	}
	selections := []struct {
		name string
		rows []int64
	}{
		{name: "sparse9", rows: []int64{101, 701, 1501, 2401, 3201, 4101, 5101, 6501, 7901}},
		{name: "dense_all"},
	}
	newOp := func() *IndexReaderTopOp {
		op := newTopNTestOp(5)
		op.NumVec = types.ArrayToBytes(make([]float32, filteredPREDims))
		return op
	}
	// Construct the reference in the parent fixture, not in a sibling benchmark.
	// Selecting only one chunked leaf must not require the legacy leaf to run.
	referenceLocation, _, _ := persistTopNTestColumn(b, fs, source, nil)
	for _, selection := range selections {
		expectedRows, expectedDistances, _, err := benchmarkReadWholeTopN(
			ctx, *source.GetType(), fs, referenceLocation, selection.rows, newOp(), mp,
		)
		require.NoError(b, err)
		for _, format := range formats {
			name := format.name + "/" + format.method + "/" + selection.name
			b.Run(name, func(b *testing.B) {
				location, ext, _ := persistTopNTestColumn(b, fs, source, format.payload)
				tracked := &topNBenchFS{FileService: fs}
				read := func() ([]int64, []float64, error) {
					if format.method == "stream" {
						rows, dists, _, err := ReadColumnTopN(ctx, 0, *source.GetType(), tracked, location, selection.rows, newOp(), mp, 0)
						return rows, dists, err
					}
					rows, dists, _, err := benchmarkReadWholeTopN(ctx, *source.GetType(), tracked, location, selection.rows, newOp(), mp)
					return rows, dists, err
				}
				rows, dists, err := read()
				require.NoError(b, err)
				require.Equal(b, expectedRows, rows)
				require.Equal(b, expectedDistances, dists)
				tracked.ranges, tracked.bytes, tracked.decoded, tracked.maxDecoded = 0, 0, 0, 0
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					rows, dists, err = read()
					if err != nil || !slices.Equal(rows, expectedRows) || !slices.Equal(dists, expectedDistances) {
						b.Fatalf("incorrect TopN rows=%v distances=%v: %v", rows, dists, err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(tracked.ranges)/float64(b.N), "read-ranges/op")
				b.ReportMetric(float64(tracked.bytes)/float64(b.N), "requested-B/op")
				b.ReportMetric(float64(tracked.decoded)/float64(b.N), "decoded-B/op")
				b.ReportMetric(float64(tracked.maxDecoded), "max-decoded-B")
				b.ReportMetric(float64(ext.Length()), "column-stored-B")
				if format.chunks != 0 {
					b.ReportMetric(float64(format.chunks), "chunks")
				}
			})
		}
	}
}

// Encoding is measured separately because a smaller read granule also makes
// each write produce more vector windows and compression frames. These times
// exclude the common first full-column serialization and object upload.
func BenchmarkFilteredPREEncodeGranularity(b *testing.B) {
	mp := newTopNTestMP(b)
	source := newFilteredPREVector(b, mp)
	for _, target := range []struct {
		name string
		size int
	}{
		{name: "writer8M", size: columnChunkTargetBytes},
		{name: "test1M", size: 1 << 20},
		{name: "test512K", size: 512 << 10},
		{name: "test256K", size: 256 << 10},
	} {
		b.Run(target.name, func(b *testing.B) {
			var payload []byte
			var chunks int
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if target.size == columnChunkTargetBytes {
					var ok bool
					var err error
					payload, ok, err = encodeChunkedColumn(source)
					if err != nil || !ok {
						b.Fatalf("writer encoding failed: chunked=%v err=%v", ok, err)
					}
					chunks = int(binary.LittleEndian.Uint32(payload[12:16]))
				} else {
					var err error
					payload, chunks, err = encodeFilteredPREChunks(source, target.size)
					if err != nil {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(len(payload)), "encoded-B")
			b.ReportMetric(float64(chunks), "chunks")
		})
	}
}
