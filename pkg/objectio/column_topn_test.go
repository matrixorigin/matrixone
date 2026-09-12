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
	"context"
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pierrec/lz4/v4"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	metricV2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

// Build the existing format directly so boundary UTs need only a few rows,
// rather than an 8 MiB vector just to cross the production writer threshold.
func encodeTopNTestChunks(t testing.TB, source *vector.Vector, rowsPerChunk int, mp *mpool.MPool) ([]byte, []columnChunkMeta) {
	t.Helper()
	count := (source.Length() + rowsPerChunk - 1) / rowsPerChunk
	payloads := make([][]byte, 0, count)
	metas := make([]columnChunkMeta, 0, count)
	for start := 0; start < source.Length(); start += rowsPerChunk {
		end := min(source.Length(), start+rowsPerChunk)
		payload, err := marshalColumnVectorWindow(source, start, end, mp)
		require.NoError(t, err)
		meta := columnChunkMeta{rowStart: uint32(start), rowCount: uint32(end - start), originSize: uint32(len(payload)), algorithm: compress.None}
		if len(metas)%2 == 0 {
			compressed := make([]byte, lz4.CompressBlockBound(len(payload)))
			n, err := lz4.CompressBlock(payload, compressed, nil)
			require.NoError(t, err)
			if n > 0 && n < len(payload) {
				payload, meta.algorithm = compressed[:n], compress.Lz4
			}
		}
		meta.length = uint32(len(payload))
		metas, payloads = append(metas, meta), append(payloads, payload)
	}
	encoded := make([]byte, columnChunkHeaderSize+len(metas)*columnChunkEntrySize)
	copy(encoded, columnChunkMagic[:])
	binary.LittleEndian.PutUint32(encoded[8:12], uint32(source.Length()))
	binary.LittleEndian.PutUint32(encoded[12:16], uint32(len(metas)))
	for i := range metas {
		metas[i].offset = uint32(len(encoded))
		encodeColumnChunkMeta(encoded[columnChunkHeaderSize+i*columnChunkEntrySize:], metas[i])
		encoded = append(encoded, payloads[i]...)
	}
	return encoded, metas
}

func persistTopNTestColumn(t testing.TB, fs fileservice.FileService, source *vector.Vector, payload []byte) (Location, Extent, ObjectDataMeta) {
	t.Helper()
	id := NewObjectid()
	name := BuildObjectNameWithObjectID(&id)
	writer, err := NewObjectWriter(name, fs, 0, []uint16{0}, nil)
	require.NoError(t, err)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = source // caller owns source
	bat.SetRowCount(source.Length())
	block, err := writer.Write(bat)
	require.NoError(t, err)
	if payload != nil {
		original := block.ColumnMeta(0).Location()
		block.ColumnMeta(0).setLocation(NewExtent(compress.Lz4Chunked, 0, uint32(len(payload)), original.OriginSize()))
		writer.blocks[SchemaData][0].data[0] = payload
	}
	blocks, err := writer.WriteEnd(context.Background())
	require.NoError(t, err)
	location := BuildLocation(name, blocks[0].GetExtent(), uint32(source.Length()), 0)
	meta, err := FastLoadObjectMeta(context.Background(), &location, false, fs)
	require.NoError(t, err)
	dataMeta := meta.MustGetMeta(SchemaData)
	return location, dataMeta.GetBlockMeta(0).ColumnMeta(0).Location(), dataMeta
}

type topNReadRange struct {
	offset, size int64
	policy       fileservice.Policy
}

type topNTrackingFS struct {
	fileservice.FileService
	ext        Extent
	reads      []topNReadRange
	live, peak int
	afterRead  func(*fileservice.IOVector) error
}

type topNOwnedData struct {
	fscache.Data
	owner *topNTrackingFS
}

func (d *topNOwnedData) Release() { d.Data.Release(); d.owner.live-- }

type topNValidatedOwnedData struct {
	*topNOwnedData
	marker validatedVectorCacheDataMarker
}

func (d *topNValidatedOwnedData) validatedVectorSnapshot() []byte {
	return d.marker.validatedVectorSnapshot()
}
func (d *topNValidatedOwnedData) validatedVectorBackingForScope() []byte {
	return d.marker.validatedVectorBackingForScope()
}

func (f *topNTrackingFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	if err := f.FileService.Read(ctx, v); err != nil {
		return err
	}
	for i := range v.Entries {
		e := &v.Entries[i]
		if e.Offset < int64(f.ext.Offset()) || e.Offset >= int64(f.ext.Offset())+int64(f.ext.Length()) {
			continue
		}
		f.reads = append(f.reads, topNReadRange{e.Offset, e.Size, v.Policy})
		if e.CachedData != nil {
			owned := &topNOwnedData{Data: e.CachedData, owner: f}
			if marker, ok := e.CachedData.(validatedVectorCacheDataMarker); ok {
				e.CachedData = &topNValidatedOwnedData{topNOwnedData: owned, marker: marker}
			} else {
				e.CachedData = owned
			}
			f.live++
			f.peak = max(f.peak, f.live)
		}
	}
	if f.afterRead != nil {
		return f.afterRead(v)
	}
	return nil
}

func newTopNTestMP(t testing.TB) *mpool.MPool {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	return mp
}

func newTopNTestVector(t testing.TB, mp *mpool.MPool, values []float32) *vector.Vector {
	source := vector.NewVec(types.T_array_float32.ToType())
	t.Cleanup(func() { source.Free(mp) })
	for _, value := range values {
		require.NoError(t, vector.AppendArray(source, []float32{value, 0}, false, mp))
	}
	return source
}

func newTopNTestOp(limit uint64) *IndexReaderTopOp {
	return &IndexReaderTopOp{Typ: types.T_array_float32, MetricType: metric.Metric_L2sqDistance, NumVec: types.ArrayToBytes([]float32{0, 0}), Limit: limit}
}

func TestReadColumnTopNChunkSelections(t *testing.T) {
	mp := newTopNTestMP(t)
	source := newTopNTestVector(t, mp, []float32{6, 1, 1, 0, 3, 2})
	fs, err := fileservice.NewMemoryFS("topn", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(context.Background()) })
	payload, metas := encodeTopNTestChunks(t, source, 2, mp)
	location, ext, _ := persistTopNTestColumn(t, fs, source, payload)
	for _, tc := range []struct {
		name      string
		selected  []int64
		limit     uint64
		want      []int64
		distances []float64
		chunks    int
	}{
		{"all", nil, 2, []int64{1, 3}, []float64{1, 0}, 3},
		{"empty", []int64{}, 2, []int64{}, []float64{}, 0},
		{"sparse", []int64{0, 4, 5}, 2, []int64{4, 5}, []float64{9, 4}, 2},
		{"duplicates", []int64{1, 1, 2, 3}, 3, []int64{1, 1, 3}, []float64{1, 1, 0}, 2},
		{"invalid coordinates", []int64{-10, -1, 1, 3, 6, 99}, 2, []int64{1, 3}, []float64{1, 0}, 2},
		{"unsorted ties", []int64{2, 1}, 1, []int64{2}, []float64{1}, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracked := &topNTrackingFS{FileService: fs, ext: ext}
			before := slices.Clone(tc.selected)
			rows, distances, _, err := ReadColumnTopN(context.Background(), 0, *source.GetType(), tracked, location, tc.selected, newTopNTestOp(tc.limit), mp, 0)
			require.NoError(t, err)
			require.Equal(t, tc.want, rows)
			require.Equal(t, tc.distances, distances)
			require.Equal(t, before, tc.selected)
			require.Zero(t, tracked.live)
			require.Equal(t, 1, tracked.peak)
			if tc.chunks < 0 {
				require.Equal(t, []topNReadRange{{int64(ext.Offset()), int64(ext.Length()), 0}}, tracked.reads)
			} else {
				require.Len(t, tracked.reads, 2+tc.chunks)
				for _, read := range tracked.reads {
					require.True(t, read.policy.Any(fileservice.SkipFullFilePreloads))
				}
				for _, read := range tracked.reads[2:] {
					found := false
					for _, meta := range metas {
						found = found || (read.offset == int64(ext.Offset())+int64(meta.offset) && read.size == int64(meta.length))
					}
					require.True(t, found, "only individual payload extents may be read")
				}
			}
		})
	}
}

func TestReadColumnTopNChunkFailureCleanup(t *testing.T) {
	mp := newTopNTestMP(t)
	source := newTopNTestVector(t, mp, []float32{6, 1, 1, 0, 3, 2})
	fs, err := fileservice.NewMemoryFS("topn-errors", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(context.Background()) })
	payload, _ := encodeTopNTestChunks(t, source, 2, mp)
	location, ext, _ := persistTopNTestColumn(t, fs, source, payload)
	for _, cancelRead := range []bool{false, true} {
		t.Run(map[bool]string{false: "partial read error", true: "cancellation"}[cancelRead], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			injected := errors.New("injected second-chunk error")
			tracked := &topNTrackingFS{FileService: fs, ext: ext}
			tracked.afterRead = func(*fileservice.IOVector) error {
				if len(tracked.reads) == 4 {
					if cancelRead {
						cancel()
					} else {
						return injected
					}
				}
				return nil
			}
			rows, distances, _, err := ReadColumnTopN(ctx, 0, *source.GetType(), tracked, location, nil, newTopNTestOp(2), mp, 0)
			if cancelRead {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, injected)
			}
			require.Nil(t, rows)
			require.Nil(t, distances)
			require.Zero(t, tracked.live)
			require.Equal(t, 1, tracked.peak)
			require.Len(t, tracked.reads, 4, "no reconstruction retry after streaming fails")
		})
	}
}

func TestReadColumnTopNChunkRangesAndSeededHeap(t *testing.T) {
	mp := newTopNTestMP(t)
	source := newTopNTestVector(t, mp, []float32{6, 1, 1, 0, 3, 2})
	require.NoError(t, vector.AppendArray(source, []float32(nil), true, mp))
	require.NoError(t, vector.AppendArray(source, []float32{float32(math.NaN()), 0}, false, mp))
	require.NoError(t, vector.AppendArray(source, []float32{float32(math.Inf(1)), 0}, false, mp))
	fs, err := fileservice.NewMemoryFS("topn-ranges", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(context.Background()) })
	payload, _ := encodeTopNTestChunks(t, source, 2, mp)
	location, _, _ := persistTopNTestColumn(t, fs, source, payload)
	for _, tc := range []struct {
		name      string
		configure func(*IndexReaderTopOp)
		want      []int64
		distances []float64
	}{
		{"seeded heap", func(op *IndexReaderTopOp) { op.DistHeap = Float64Heap{0.5} }, []int64{3}, []float64{0}},
		{"exclusive lower inclusive upper", func(op *IndexReaderTopOp) {
			op.LowerBoundType = plan.BoundType_EXCLUSIVE
			op.LowerBound = 1
			op.UpperBoundType = plan.BoundType_INCLUSIVE
			op.UpperBound = 9
		}, []int64{5}, []float64{4}},
		{"NaN range", func(op *IndexReaderTopOp) { op.UpperBoundType = plan.BoundType_INCLUSIVE; op.UpperBound = math.NaN() }, []int64{}, []float64{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := newTopNTestOp(1)
			tc.configure(op)
			rows, distances, _, err := ReadColumnTopN(context.Background(), 0, *source.GetType(), fs, location, nil, op, mp, 0)
			require.NoError(t, err)
			require.Equal(t, tc.want, rows)
			require.Equal(t, tc.distances, distances)
		})
	}
}

func TestReadColumnTopNCachePaths(t *testing.T) {
	ctx := context.Background()
	mp := newTopNTestMP(t)
	source := newTopNTestVector(t, mp, []float32{6, 1, 1, 0, 3, 2})
	capacity := toml.ByteSize(1 << 20)
	fs, err := fileservice.NewS3FS(ctx, fileservice.ObjectStorageArguments{Name: "topn-cache", Endpoint: "disk", Bucket: t.TempDir()}, fileservice.CacheConfig{MemoryCapacity: &capacity}, nil, false, false)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	payload, _ := encodeTopNTestChunks(t, source, 2, mp)
	location, ext, meta := persistTopNTestColumn(t, fs, source, payload)
	whole, err := ReadOneBlock(ctx, &meta, location.Name().UnsafeString(), 0, []uint16{0}, []types.Type{*source.GetType()}, mp, fs, 0)
	require.NoError(t, err)
	whole.Release()
	tracked := &topNTrackingFS{FileService: fs, ext: ext}
	rows, _, cached, err := ReadColumnTopN(ctx, 0, *source.GetType(), tracked, location, nil, newTopNTestOp(2), mp, 0)
	require.NoError(t, err)
	require.True(t, cached)
	require.Empty(t, tracked.reads)
	require.Equal(t, []int64{1, 3}, rows)
	fs.FlushCache(ctx)
	for pass := 0; pass < 2; pass++ {
		tracked = &topNTrackingFS{FileService: fs, ext: ext}
		rows, _, cached, err = ReadColumnTopN(ctx, 0, *source.GetType(), tracked, location, nil, newTopNTestOp(2), mp, 0)
		require.NoError(t, err)
		require.Equal(t, pass == 1, cached)
		require.Len(t, tracked.reads, 5)
		require.Equal(t, []int64{1, 3}, rows)
		require.Zero(t, tracked.live)
		require.Equal(t, 1, tracked.peak)
	}
	// A legacy extent must not pay for the extra whole-column cache probe.
	legacy, legacyExtent, _ := persistTopNTestColumn(t, fs, source, nil)
	tracked = &topNTrackingFS{FileService: fs, ext: legacyExtent}
	rows, _, _, err = ReadColumnTopN(ctx, 0, *source.GetType(), tracked, legacy, nil, newTopNTestOp(2), mp, 0)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 3}, rows)
	require.Len(t, tracked.reads, 1)
	require.Zero(t, tracked.live)
}

func TestReadColumnTopNChunkSharingAndClose(t *testing.T) {
	for _, closeService := range []bool{false, true} {
		t.Run(map[bool]string{false: "independent queries", true: "close while chunk is held"}[closeService], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			mp := newTopNTestMP(t)
			source := newTopNTestVector(t, mp, []float32{6, 1, 1, 0, 3, 2})
			capacity := toml.ByteSize(128 << 10)
			fs, err := fileservice.NewS3FS(ctx, fileservice.ObjectStorageArguments{Name: "chunk-share", Endpoint: "disk", Bucket: t.TempDir()}, fileservice.CacheConfig{MemoryCapacity: &capacity}, nil, false, false)
			require.NoError(t, err)
			t.Cleanup(func() { fs.Close(context.Background()) })
			payload, metas := encodeTopNTestChunks(t, source, 2, mp)
			location, ext, _ := persistTopNTestColumn(t, fs, source, payload)
			fs.FlushCache(ctx)
			pinned := fs.AllocateCacheData(ctx, int(capacity))
			t.Cleanup(pinned.Release)
			beforeReserved := promtest.ToFloat64(metricV2.SharedDecodeReserved)
			entered, unblock := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(unblock) })
			first := &topNTrackingFS{FileService: fs, ext: ext}
			var shared fscache.Data
			firstOffset := int64(ext.Offset()) + int64(metas[0].offset)
			first.afterRead = func(v *fileservice.IOVector) error {
				if v.Entries[0].Offset == firstOffset {
					shared = v.Entries[0].CachedData.(*topNValidatedOwnedData).Data
					close(entered)
					<-unblock
				}
				return nil
			}
			type result struct {
				rows []int64
				err  error
			}
			results, finished := make(chan result, 1), make(chan struct{})
			go func() {
				defer close(finished)
				rows, _, _, err := ReadColumnTopN(ctx, 0, *source.GetType(), first, location, nil, newTopNTestOp(2), mp, 0)
				results <- result{rows, err}
			}()
			t.Cleanup(func() { release(); <-finished })
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("first query did not reach its chunk")
			}
			original := shared.Bytes()
			if closeService {
				closed := make(chan struct{})
				go func() { fs.Close(ctx); close(closed) }()
				select {
				case <-closed:
				case <-ctx.Done():
					t.Fatal("close waited for Top-K consumption")
				}
			} else {
				second := &topNTrackingFS{FileService: fs, ext: ext}
				second.afterRead = func(v *fileservice.IOVector) error {
					if v.Entries[0].Offset == firstOffset {
						require.Same(t, shared, v.Entries[0].CachedData.(*topNValidatedOwnedData).Data)
					}
					return nil
				}
				op := newTopNTestOp(1)
				op.NumVec = types.ArrayToBytes([]float32{4, 0})
				rows, distances, _, err := ReadColumnTopN(ctx, 0, *source.GetType(), second, location, []int64{0, 1, 3, 4, 5}, op, mp, 0)
				require.NoError(t, err)
				require.Equal(t, []int64{4}, rows)
				require.Equal(t, []float64{1}, distances)
				require.Zero(t, second.live)
			}
			require.Equal(t, original, shared.Bytes(), "other queries and Close cannot mutate borrowed data")
			release()
			got := <-results
			if closeService {
				require.ErrorContains(t, got.err, "closed")
				require.Nil(t, got.rows)
			} else {
				require.NoError(t, got.err)
				require.Equal(t, []int64{1, 3}, got.rows)
			}
			require.Zero(t, first.live)
			require.Equal(t, 1, first.peak)
			require.Equal(t, beforeReserved, promtest.ToFloat64(metricV2.SharedDecodeReserved))
		})
	}
}

func TestReadColumnTopNMalformedChunks(t *testing.T) {
	mp := newTopNTestMP(t)
	source := newTopNTestVector(t, mp, []float32{6, 1, 1, 0, 3, 2})
	fs, err := fileservice.NewMemoryFS("topn-malformed", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(context.Background()) })
	for _, tc := range []struct {
		name, want string
		mutate     func([]byte, []columnChunkMeta) []byte
	}{
		{"short prefix", "prefix", func(b []byte, _ []columnChunkMeta) []byte { return b[:8] }},
		{"bad magic", "prefix", func(b []byte, _ []columnChunkMeta) []byte { b[0] ^= 255; return b }},
		{"block rows", "block row count", func(b []byte, m []columnChunkMeta) []byte {
			binary.LittleEndian.PutUint32(b[8:12], 5)
			m[2].rowCount--
			encodeColumnChunkMeta(b[columnChunkHeaderSize+2*columnChunkEntrySize:], m[2])
			return b
		}},
		{"decoded length", "decompressed size", func(b []byte, m []columnChunkMeta) []byte {
			require.Equal(t, uint8(compress.Lz4), m[0].algorithm)
			m[0].originSize++
			encodeColumnChunkMeta(b[columnChunkHeaderSize:], m[0])
			return b
		}},
		{"payload rows", "payload row count", func(b []byte, m []columnChunkMeta) []byte {
			m[0].rowCount--
			m[1].rowStart--
			m[1].rowCount++
			encodeColumnChunkMeta(b[columnChunkHeaderSize:], m[0])
			encodeColumnChunkMeta(b[columnChunkHeaderSize+columnChunkEntrySize:], m[1])
			return b
		}},
		{"later corrupt payload", "invalid object column data type", func(b []byte, m []columnChunkMeta) []byte {
			require.Equal(t, uint8(compress.None), m[1].algorithm)
			b[m[1].offset] = 255
			return b
		}},
		{"directory overlap", "invalid chunked object column entry", func(b []byte, m []columnChunkMeta) []byte {
			m[1].offset--
			encodeColumnChunkMeta(b[columnChunkHeaderSize+columnChunkEntrySize:], m[1])
			return b
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload, metas := encodeTopNTestChunks(t, source, 2, mp)
			payload = tc.mutate(payload, metas)
			location, ext, _ := persistTopNTestColumn(t, fs, source, payload)
			tracked := &topNTrackingFS{FileService: fs, ext: ext}
			rows, distances, _, err := ReadColumnTopN(context.Background(), 0, *source.GetType(), tracked, location, nil, newTopNTestOp(2), mp, 0)
			require.ErrorContains(t, err, tc.want)
			require.Nil(t, rows)
			require.Nil(t, distances)
			require.Zero(t, tracked.live)
			require.LessOrEqual(t, tracked.peak, 1)
		})
	}
}

func TestReadColumnTopNChunkTypes(t *testing.T) {
	for _, tc := range []struct {
		kind   types.T
		encode func(float32) []byte
	}{
		{types.T_array_float32, func(v float32) []byte { return types.ArrayToBytes([]float32{v, 0}) }},
		{types.T_array_float64, func(v float32) []byte { return types.ArrayToBytes([]float64{float64(v), 0}) }},
		{types.T_array_bf16, func(v float32) []byte { return types.ArrayToBytes(types.Float32ToBF16Slice([]float32{v, 0})) }},
		{types.T_array_float16, func(v float32) []byte { return types.ArrayToBytes(types.Float32ToFloat16Slice([]float32{v, 0})) }},
		{types.T_array_int8, func(v float32) []byte { return types.ArrayToBytes([]int8{int8(v), 0}) }},
		{types.T_array_uint8, func(v float32) []byte { return types.ArrayToBytes([]uint8{uint8(v), 0}) }},
	} {
		t.Run(tc.kind.String(), func(t *testing.T) {
			mp := newTopNTestMP(t)
			source := vector.NewVec(tc.kind.ToType())
			t.Cleanup(func() { source.Free(mp) })
			for _, value := range []float32{6, 1, 1, 0, 3, 2} {
				require.NoError(t, vector.AppendBytes(source, tc.encode(value), false, mp))
			}
			fs, err := fileservice.NewMemoryFS("topn-types", fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			t.Cleanup(func() { fs.Close(context.Background()) })
			payload, _ := encodeTopNTestChunks(t, source, 2, mp)
			location, _, _ := persistTopNTestColumn(t, fs, source, payload)
			op := newTopNTestOp(2)
			op.Typ = tc.kind
			op.NumVec = tc.encode(0)
			rows, distances, _, err := ReadColumnTopN(context.Background(), 0, *source.GetType(), fs, location, nil, op, mp, 0)
			require.NoError(t, err)
			require.Equal(t, []int64{1, 3}, rows)
			require.Equal(t, []float64{1, 0}, distances)
		})
	}
}

func TestReadColumnTopNLaterChunkTypeMismatch(t *testing.T) {
	mp := newTopNTestMP(t)
	source := newTopNTestVector(t, mp, []float32{6, 1, 1, 0, 3, 2})
	fs, err := fileservice.NewMemoryFS("topn-mixed-types", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(context.Background()) })
	for _, kind := range []types.T{types.T_array_float32, types.T_array_float64} {
		t.Run(kind.String(), func(t *testing.T) {
			// Same kind/different width exercises consistency across chunks;
			// float64 also exercises the declared-kind check after a valid chunk.
			wrong := vector.NewVec(types.New(kind, 1, 0))
			defer wrong.Free(mp)
			for range 2 {
				if kind == types.T_array_float32 {
					require.NoError(t, vector.AppendArray(wrong, []float32{1}, false, mp))
				} else {
					require.NoError(t, vector.AppendArray(wrong, []float64{1}, false, mp))
				}
			}
			replacement, err := marshalColumnVectorWindow(wrong, 0, 2, mp)
			require.NoError(t, err)
			payload, metas := encodeTopNTestChunks(t, source, 2, mp)
			second := metas[1]
			require.Equal(t, uint8(compress.None), second.algorithm)
			require.Equal(t, len(replacement), int(second.length), "both tiny varlen payloads use inline values")
			copy(payload[second.offset:second.offset+second.length], replacement)
			location, ext, _ := persistTopNTestColumn(t, fs, source, payload)
			tracked := &topNTrackingFS{FileService: fs, ext: ext}
			rows, distances, _, err := ReadColumnTopN(context.Background(), 0, *source.GetType(), tracked, location, nil, newTopNTestOp(2), mp, 0)
			require.ErrorContains(t, err, "payload type mismatch")
			require.Nil(t, rows)
			require.Nil(t, distances)
			require.Zero(t, tracked.live)
			require.Len(t, tracked.reads, 4)
		})
	}
}

type topNOffsetFS struct {
	fileservice.FileService
	base      int64
	maxOffset int64
}

func (f *topNOffsetFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	for i := range v.Entries {
		f.maxOffset = max(f.maxOffset, v.Entries[i].Offset)
		v.Entries[i].Offset -= f.base
	}
	err := f.FileService.Read(ctx, v)
	for i := range v.Entries {
		v.Entries[i].Offset += f.base
	}
	return err
}

func TestChunkTopNWideOffsetsAndSkippedPayload(t *testing.T) {
	mp := newTopNTestMP(t)
	source := newTopNTestVector(t, mp, []float32{6, 1, 1, 0, 3, 2})
	payload, metas := encodeTopNTestChunks(t, source, 2, mp)
	// The middle payload is intentionally invalid, but none of its rows are
	// selected. Its directory entry remains valid and must not trigger a read.
	payload[metas[1].offset] = 255
	fs, err := fileservice.NewMemoryFS("topn-offset", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(context.Background()) })
	require.NoError(t, fs.Write(context.Background(), fileservice.IOVector{FilePath: "column", Entries: []fileservice.IOEntry{{Size: int64(len(payload)), Data: payload}}}))
	const base = int64(math.MaxUint32 - 32)
	shifted := &topNOffsetFS{FileService: fs, base: base}
	ext := NewExtent(compress.Lz4Chunked, uint32(base), uint32(len(payload)), uint32(source.Size()))
	rows, distances, _, err := readChunkedColumnTopN(context.Background(), "column", ext, 6, types.T_array_float32, []int64{0, 4}, newTopNTestOp(2), shifted, 0)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 4}, rows)
	require.Equal(t, []float64{36, 9}, distances)
	require.Greater(t, shifted.maxOffset, int64(math.MaxUint32))
}

func TestReadColumnTopNNonFiniteAndRejectedInputs(t *testing.T) {
	mp := newTopNTestMP(t)
	source := newTopNTestVector(t, mp, []float32{float32(math.NaN()), float32(math.Inf(1)), 1, float32(math.Inf(-1))})
	fs, err := fileservice.NewMemoryFS("topn-inputs", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(context.Background()) })
	payload, _ := encodeTopNTestChunks(t, source, 2, mp)
	location, ext, _ := persistTopNTestColumn(t, fs, source, payload)
	rows, distances, _, err := ReadColumnTopN(context.Background(), 0, *source.GetType(), fs, location, nil, newTopNTestOp(2), mp, 0)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1}, rows)
	require.True(t, math.IsNaN(distances[0]))
	require.True(t, math.IsInf(distances[1], 1))
	for _, tc := range []struct {
		name string
		op   *IndexReaderTopOp
		want string
	}{
		{"nil operator", nil, "nil vector topn"},
		{"zero limit", newTopNTestOp(0), "limit must be positive"},
		{"overflow limit", newTopNTestOp(math.MaxUint64), "overflows int"},
		{"unknown metric", &IndexReaderTopOp{Typ: types.T_array_float32, MetricType: metric.MetricType(255), Limit: 1}, "invalid distance type"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracked := &topNTrackingFS{FileService: fs, ext: ext}
			rows, distances, _, err := ReadColumnTopN(context.Background(), 0, *source.GetType(), tracked, location, nil, tc.op, mp, 0)
			require.ErrorContains(t, err, tc.want)
			require.Nil(t, rows)
			require.Nil(t, distances)
			require.Zero(t, tracked.live)
		})
	}
	_, _, _, err = ReadColumnTopN(context.Background(), 0, *source.GetType(), nil, nil, nil, nil, nil, 0)
	require.ErrorContains(t, err, "nil mpool")
}
