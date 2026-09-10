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
	"io"
	"testing"
	"testing/iotest"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

type blockTopNTestFS struct {
	fileservice.FileService
	requests    [][]topNReadRange
	conversions int
	decoded     int64
	maxDecoded  int64
}

func (f *blockTopNTestFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	ranges := make([]topNReadRange, len(v.Entries))
	for i, e := range v.Entries {
		ranges[i] = topNReadRange{e.Offset, e.Size, v.Policy}
	}
	f.requests = append(f.requests, ranges)
	return f.FileService.Read(ctx, v)
}

func (f *blockTopNTestFS) DecodeFromBytes(ctx context.Context, v *fileservice.IOVector, data []byte) error {
	original := v.Entries[0].ToCacheData
	v.Entries[0].ToCacheData = func(ctx context.Context, reader io.Reader, data []byte, a fileservice.CacheDataAllocator) (fscache.Data, error) {
		out, err := original(ctx, reader, data, a)
		if out != nil && v.Entries[0].DecodeSharing.Codec != "" {
			f.conversions++
			f.decoded += out.Capacity()
			f.maxDecoded = max(f.maxDecoded, out.Capacity())
		}
		return out, err
	}
	defer func() { v.Entries[0].ToCacheData = original }()
	return fileservice.DecodeFromBytes(ctx, f.FileService, v, data)
}

func newBlockTopNTestFS(t testing.TB) *fileservice.S3FS {
	capacity := toml.ByteSize(64 << 20)
	fs, err := fileservice.NewS3FS(context.Background(), fileservice.ObjectStorageArguments{
		Name: "block-topn", Endpoint: "disk", Bucket: t.TempDir()}, fileservice.CacheConfig{MemoryCapacity: &capacity}, nil, false, false)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(context.Background()) })
	return fs
}

func persistBlockTopNTest(t testing.TB, fs fileservice.FileService, source *vector.Vector, payload []byte, mp *mpool.MPool) (Location, ObjectDataMeta) {
	t.Helper()
	bat := batch.NewWithSize(3)
	bat.Vecs[0], bat.Vecs[1], bat.Vecs[2] = vector.NewVec(types.T_int64.ToType()), source, vector.NewVec(types.T_array_float32.ToType())
	defer bat.Vecs[0].Free(mp)
	defer bat.Vecs[2].Free(mp)
	for row := range source.Length() {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(row), false, mp))
		require.NoError(t, vector.AppendArray(bat.Vecs[2], []float32{float32(10 + row)}, false, mp))
	}
	bat.SetRowCount(source.Length())
	id := NewObjectid()
	name := BuildObjectNameWithObjectID(&id)
	writer, err := NewObjectWriter(name, fs, 0, []uint16{0, 1, 2}, nil)
	require.NoError(t, err)
	block, err := writer.Write(bat)
	require.NoError(t, err)
	if payload != nil {
		old := block.ColumnMeta(1).Location()
		block.ColumnMeta(1).setLocation(NewExtent(compress.Lz4Chunked, 0, uint32(len(payload)), old.OriginSize()))
		writer.blocks[SchemaData][0].data[1] = payload
	}
	blocks, err := writer.WriteEnd(context.Background())
	require.NoError(t, err)
	location := BuildLocation(name, blocks[0].GetExtent(), uint32(source.Length()), 0)
	meta, err := FastLoadObjectMeta(context.Background(), &location, false, fs)
	require.NoError(t, err)
	return location, meta.MustGetMeta(SchemaData)
}

func readBlockTopNTest(t testing.TB, fs fileservice.FileService, location Location, mp *mpool.MPool, op *IndexReaderTopOp) *BlockTopNRead {
	t.Helper()
	r, err := ReadBlockForTopN(context.Background(), []uint16{3, 0, 1, 2},
		[]types.Type{types.T_int64.ToType(), types.T_int64.ToType(), types.T_array_float32.ToType(), types.T_array_float32.ToType()},
		fs, location, 2, op, mp, fileservice.SkipFullFilePreloads)
	require.NoError(t, err)
	t.Cleanup(r.Release)
	return r
}

func TestBlockTopNSelections(t *testing.T) {
	mp := newTopNTestMP(t)
	source := newTopNTestVector(t, mp, []float32{4, 1, 3, 2, 6, 5})
	payload, _ := encodeTopNTestChunks(t, source, 2, mp)
	storage := newBlockTopNTestFS(t)
	location, _ := persistBlockTopNTest(t, storage, source, payload, mp)
	storage.FlushCache(t.Context())
	pin := storage.AllocateCacheData(t.Context(), 64<<20)
	t.Cleanup(pin.Release)
	fs := &blockTopNTestFS{FileService: storage}
	for _, tc := range []struct {
		name           string
		selected, want []int64
		dists          []float64
		chunks         int
	}{
		{"dense", nil, []int64{1, 3}, []float64{1, 4}, 3},
		{"sparse", []int64{0, 2}, []int64{0, 2}, []float64{16, 9}, 2},
		{"empty", []int64{}, []int64{}, []float64{}, 0},
		{"unsorted", []int64{5, 0, 3, 1}, []int64{3, 1}, []float64{4, 1}, 3},
		{"duplicate", []int64{1, 1, 3}, []int64{1, 1}, []float64{1, 1}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs.requests, fs.conversions = nil, 0
			r := readBlockTopNTest(t, fs, location, mp, newTopNTestOp(2))
			require.IsType(t, &scopedChunkedData{}, r.block.Entries[2].CachedData)
			require.Len(t, fs.requests, 1)
			require.Len(t, fs.requests[0], 3, "synthetic predecessor must not split or shift the physical read")
			rows, dists, err := r.TopN(t.Context(), tc.selected)
			require.NoError(t, err)
			require.Equal(t, tc.want, rows)
			require.Equal(t, tc.dists, dists)
			require.Equal(t, tc.chunks, fs.conversions)
			require.Len(t, fs.requests, 1, "TopN must not fetch chunks again")
			dst := vector.NewVec(types.T_array_float32.ToType())
			defer dst.Free(mp)
			require.NoError(t, CopyCachedVectorRows(dst, r.Entry(3).CachedData, rows, mp))
			for i, row := range rows {
				require.Equal(t, []float32{float32(10 + row)}, types.BytesToArray[float32](dst.GetBytesAt(i)))
			}
			r.Release()
			r.Release()
			_, _, err = r.TopN(t.Context(), nil)
			require.ErrorContains(t, err, "released")
		})
	}
}

func TestReadBlockBySearchAndTopNReusesFilterColumns(t *testing.T) {
	mp := newTopNTestMP(t)
	source := newTopNTestVector(t, mp, []float32{4, 1, 3, 2, 6, 5})
	payload, _ := encodeTopNTestChunks(t, source, 2, mp)
	storage := newBlockTopNTestFS(t)
	location, meta := persistBlockTopNTest(t, storage, source, payload, mp)
	filterExtent := meta.GetBlockMeta(0).ColumnMeta(0).Location()
	topExtent := meta.GetBlockMeta(0).ColumnMeta(1).Location()
	storage.FlushCache(t.Context())
	fs := &blockTopNTestFS{FileService: storage}

	pk := vector.NewVec(types.T_int64.ToType())
	payloadOut := vector.NewVec(types.T_array_float32.ToType())
	defer pk.Free(mp)
	defer payloadOut.Free(mp)
	op := newTopNTestOp(2)
	rows, distances, _, err := ReadBlockBySearchAndTopN(
		t.Context(),
		[]uint16{0},
		[]types.Type{types.T_int64.ToType()},
		[]uint16{0, 2},
		[]types.Type{types.T_int64.ToType(), types.T_array_float32.ToType()},
		[]*vector.Vector{pk, payloadOut},
		1,
		types.T_array_float32.ToType(),
		func(filters []vector.Vector) ([]int64, error) {
			require.Len(t, filters, 1)
			require.Equal(t, []int64{0, 1, 2, 3, 4, 5}, vector.MustFixedColWithTypeCheck[int64](&filters[0]))
			return []int64{0, 2, 4}, nil
		},
		op,
		fs,
		location,
		mp,
		fileservice.SkipFullFilePreloads,
	)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 2}, rows)
	require.Equal(t, []float64{16, 9}, distances)
	require.Equal(t, []int64{0, 2}, vector.MustFixedColWithTypeCheck[int64](pk))
	require.Equal(t, []float32{10}, types.BytesToArray[float32](payloadOut.GetBytesAt(0)))
	require.Equal(t, []float32{12}, types.BytesToArray[float32](payloadOut.GetBytesAt(1)))
	require.Equal(t, 1, countTopNReadsWithin(fs.requests, filterExtent), "filter/output column must be read once")
	require.Positive(t, countTopNReadsWithin(fs.requests, topExtent))
}

func TestReadBlockBySearchAndTopNEmptyFilterSkipsVector(t *testing.T) {
	mp := newTopNTestMP(t)
	source := newTopNTestVector(t, mp, []float32{4, 1, 3, 2})
	payload, _ := encodeTopNTestChunks(t, source, 2, mp)
	storage := newBlockTopNTestFS(t)
	location, meta := persistBlockTopNTest(t, storage, source, payload, mp)
	topExtent := meta.GetBlockMeta(0).ColumnMeta(1).Location()
	storage.FlushCache(t.Context())
	fs := &blockTopNTestFS{FileService: storage}
	out := vector.NewVec(types.T_int64.ToType())
	defer out.Free(mp)

	rows, distances, _, err := ReadBlockBySearchAndTopN(
		t.Context(),
		[]uint16{0},
		[]types.Type{types.T_int64.ToType()},
		[]uint16{0},
		[]types.Type{types.T_int64.ToType()},
		[]*vector.Vector{out},
		1,
		types.T_array_float32.ToType(),
		func([]vector.Vector) ([]int64, error) { return []int64{}, nil },
		newTopNTestOp(2),
		fs,
		location,
		mp,
		fileservice.SkipFullFilePreloads,
	)
	require.NoError(t, err)
	require.Empty(t, rows)
	require.Empty(t, distances)
	require.Zero(t, out.Length())
	require.Zero(t, countTopNReadsWithin(fs.requests, topExtent), "empty membership must not read vector chunks")
}

func TestReadBlockBySearchAndTopNLegacyVector(t *testing.T) {
	mp := newTopNTestMP(t)
	source := newTopNTestVector(t, mp, []float32{4, 1, 3, 2})
	storage := newBlockTopNTestFS(t)
	location, _ := persistBlockTopNTest(t, storage, source, nil, mp)
	storage.FlushCache(t.Context())
	pk := vector.NewVec(types.T_int64.ToType())
	defer pk.Free(mp)

	rows, distances, _, err := ReadBlockBySearchAndTopN(
		t.Context(),
		[]uint16{0},
		[]types.Type{types.T_int64.ToType()},
		[]uint16{0},
		[]types.Type{types.T_int64.ToType()},
		[]*vector.Vector{pk},
		1,
		types.T_array_float32.ToType(),
		func([]vector.Vector) ([]int64, error) { return []int64{0, 1, 3}, nil },
		newTopNTestOp(2),
		storage,
		location,
		mp,
		fileservice.SkipFullFilePreloads,
	)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 3}, rows)
	require.Equal(t, []float64{1, 4}, distances)
	require.Equal(t, []int64{1, 3}, vector.MustFixedColWithTypeCheck[int64](pk))
}

func TestReadBlockBySearchAndTopNRejectsInvalidSelections(t *testing.T) {
	mp := newTopNTestMP(t)
	source := newTopNTestVector(t, mp, []float32{4, 1, 3, 2})
	payload, _ := encodeTopNTestChunks(t, source, 2, mp)
	storage := newBlockTopNTestFS(t)
	location, _ := persistBlockTopNTest(t, storage, source, payload, mp)
	for _, test := range []struct {
		name string
		rows []int64
	}{
		{name: "nil", rows: nil},
		{name: "duplicate", rows: []int64{1, 1}},
		{name: "unsorted", rows: []int64{2, 1}},
		{name: "negative", rows: []int64{-1}},
		{name: "past end", rows: []int64{4}},
	} {
		t.Run(test.name, func(t *testing.T) {
			out := vector.NewVec(types.T_int64.ToType())
			defer out.Free(mp)
			_, _, _, err := ReadBlockBySearchAndTopN(
				t.Context(),
				[]uint16{0},
				[]types.Type{types.T_int64.ToType()},
				[]uint16{0},
				[]types.Type{types.T_int64.ToType()},
				[]*vector.Vector{out},
				1,
				types.T_array_float32.ToType(),
				func([]vector.Vector) ([]int64, error) { return test.rows, nil },
				newTopNTestOp(2),
				storage,
				location,
				mp,
				fileservice.SkipFullFilePreloads,
			)
			require.Error(t, err)
			require.Zero(t, out.Length())
		})
	}
}

func TestReadBlockBySearchAndTopNValidatesContract(t *testing.T) {
	type arguments struct {
		filterColumns      []uint16
		filterTypes        []types.Type
		outputColumns      []uint16
		outputTypes        []types.Type
		outputDestinations []*vector.Vector
		topColumn          uint16
		topType            types.Type
		selectRows         func([]vector.Vector) ([]int64, error)
		topReader          *IndexReaderTopOp
		mp                 *mpool.MPool
	}

	mp := newTopNTestMP(t)
	out := vector.NewVec(types.T_int64.ToType())
	defer out.Free(mp)
	valid := func() arguments {
		return arguments{
			filterColumns:      []uint16{0},
			filterTypes:        []types.Type{types.T_int64.ToType()},
			outputColumns:      []uint16{0},
			outputTypes:        []types.Type{types.T_int64.ToType()},
			outputDestinations: []*vector.Vector{out},
			topColumn:          1,
			topType:            types.T_array_float32.ToType(),
			selectRows:         func([]vector.Vector) ([]int64, error) { return []int64{}, nil },
			topReader:          newTopNTestOp(1),
			mp:                 mp,
		}
	}
	for _, test := range []struct {
		name    string
		wantErr string
		mutate  func(*arguments)
	}{
		{name: "empty filter", wantErr: "invalid exact-filter columns", mutate: func(a *arguments) {
			a.filterColumns = nil
			a.filterTypes = nil
		}},
		{name: "mismatched output", wantErr: "invalid exact-filter output columns", mutate: func(a *arguments) {
			a.outputTypes = nil
		}},
		{name: "nil selector", wantErr: "nil exact-filter block topn input", mutate: func(a *arguments) {
			a.selectRows = nil
		}},
		{name: "ordered topn", wantErr: "unsupported exact-filter vector topn input", mutate: func(a *arguments) {
			a.topReader.OrderedLimit = true
		}},
		{name: "filter is vector column", wantErr: "invalid exact-filter column", mutate: func(a *arguments) {
			a.filterColumns = []uint16{1}
		}},
		{name: "duplicate filter", wantErr: "duplicate exact-filter column", mutate: func(a *arguments) {
			a.filterColumns = []uint16{0, 0}
			a.filterTypes = []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}
		}},
		{name: "nil output", wantErr: "invalid exact-filter output column", mutate: func(a *arguments) {
			a.outputDestinations = []*vector.Vector{nil}
		}},
		{name: "duplicate output", wantErr: "duplicate exact-filter output column", mutate: func(a *arguments) {
			a.outputColumns = []uint16{0, 0}
			a.outputTypes = []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}
			a.outputDestinations = []*vector.Vector{out, out}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := valid()
			test.mutate(&a)
			_, _, _, err := ReadBlockBySearchAndTopN(
				t.Context(), a.filterColumns, a.filterTypes, a.outputColumns, a.outputTypes,
				a.outputDestinations, a.topColumn, a.topType, a.selectRows, a.topReader,
				nil, Location{}, a.mp, fileservice.SkipFullFilePreloads,
			)
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

func countTopNReadsWithin(requests [][]topNReadRange, extent Extent) int {
	count := 0
	start := int64(extent.Offset())
	end := start + int64(extent.Length())
	for _, request := range requests {
		for _, entry := range request {
			if entry.offset >= start && entry.offset < end {
				count++
			}
		}
	}
	return count
}

func TestBlockTopNCacheInteroperability(t *testing.T) {
	mp := newTopNTestMP(t)
	source := newTopNTestVector(t, mp, []float32{4, 1, 3, 2, 6, 5})
	payload, metas := encodeTopNTestChunks(t, source, 2, mp)
	storage := newBlockTopNTestFS(t)
	location, meta := persistBlockTopNTest(t, storage, source, payload, mp)
	storage.FlushCache(t.Context())
	fs := &blockTopNTestFS{FileService: storage}
	r := readBlockTopNTest(t, fs, location, mp, newTopNTestOp(2))
	_, _, err := r.TopN(t.Context(), []int64{0, 1})
	require.NoError(t, err)
	require.Equal(t, 1, fs.conversions)
	r.Release()
	fs.conversions = 0
	r = readBlockTopNTest(t, fs, location, mp, newTopNTestOp(2))
	require.Empty(t, r.chunks.Entries, "partial probes must release their pins and use the combined read")
	require.IsType(t, &scopedChunkedData{}, r.Entry(2).CachedData)
	_, _, err = r.TopN(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, 2, fs.conversions, "the already-decoded chunk remains a cache hit")
	r.Release()
	probe := fileservice.IOVector{FilePath: location.Name().UnsafeString(), Entries: []fileservice.IOEntry{newColumnIOEntry(meta.GetBlockMeta(0).ColumnMeta(1).Location(), columnCacheConstructorFactory)}}
	require.NoError(t, storage.ReadCache(t.Context(), &probe))
	require.Nil(t, probe.Entries[0].CachedData, "compressed data must not enter the decoded-column cache")
	fs.conversions, fs.requests = 0, nil
	r = readBlockTopNTest(t, fs, location, mp, newTopNTestOp(2))
	require.Len(t, r.chunks.Entries, len(metas))
	require.False(t, r.FromCache(), "synthetic columns retain the existing non-cache accounting")
	require.True(t, r.Entry(1).WasFromCache())
	require.True(t, r.Entry(3).WasFromCache())
	_, _, err = r.TopN(t.Context(), nil)
	require.NoError(t, err)
	require.Zero(t, fs.conversions, "self-warmed chunks must not decode again")
	r.Release()
	// The ordinary reader can still reconstruct and cache a full column.
	whole, err := ReadOneBlock(t.Context(), &meta, location.Name().UnsafeString(), 0, []uint16{1}, []types.Type{types.T_array_float32.ToType()}, mp, storage, fileservice.SkipFullFilePreloads)
	require.NoError(t, err)
	owner := whole.Entries[0].CachedData
	defer whole.Release()
	r = readBlockTopNTest(t, fs, location, mp, newTopNTestOp(2))
	require.Same(t, owner, r.Entry(2).CachedData)
	require.Nil(t, r.metas)
	r.Release()
	rows, dists, _, err := ReadColumnTopN(t.Context(), 1, types.T_array_float32.ToType(), storage, location, nil, newTopNTestOp(2), mp, 0)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 3}, rows)
	require.Equal(t, []float64{1, 4}, dists)
}

func TestBlockTopNThresholdAndErrors(t *testing.T) {
	mp := newTopNTestMP(t)
	source := newTopNTestVector(t, mp, []float32{4, 1, 3, 2, 6, 5})
	payload, metas := encodeTopNTestChunks(t, source, 2, mp)
	storage := newBlockTopNTestFS(t)
	location, _ := persistBlockTopNTest(t, storage, source, payload, mp)
	op := newTopNTestOp(2)
	op.UpperBoundType, op.UpperBound = plan.BoundType_EXCLUSIVE, 4
	r := readBlockTopNTest(t, storage, location, mp, op)
	rows, dists, err := r.TopN(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{1}, rows)
	require.Equal(t, []float64{1}, dists)
	r.Release()
	// Corrupt the uncompressed middle payload, not its directory. Skipping that
	// chunk is legal; consuming it fails without retrying another storage path.
	payload[metas[1].offset] ^= 255
	bad, _ := persistBlockTopNTest(t, storage, source, payload, mp)
	fs := &blockTopNTestFS{FileService: storage}
	r = readBlockTopNTest(t, fs, bad, mp, newTopNTestOp(2))
	_, _, err = r.TopN(t.Context(), []int64{0, 1})
	require.NoError(t, err)
	_, _, err = r.TopN(t.Context(), nil)
	require.Error(t, err)
	require.Len(t, fs.requests, 1)
	r.Release()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	r = readBlockTopNTest(t, storage, location, mp, newTopNTestOp(2))
	_, _, err = r.TopN(ctx, nil)
	require.ErrorIs(t, err, context.Canceled)
	r.Release()
}

func TestScopedChunkedDataContract(t *testing.T) {
	mp := newTopNTestMP(t)
	source := newTopNTestVector(t, mp, []float32{1, 2, 3, 4})
	payload, _ := encodeTopNTestChunks(t, source, 2, mp)
	data, err := scopedChunkedConstructor(t.Context(), bytes.NewReader(payload), nil, fileservice.DefaultCacheDataAllocator())
	require.NoError(t, err)
	defer data.Release()
	require.Equal(t, int64(len(payload)), data.Size())
	require.GreaterOrEqual(t, data.Capacity(), data.Size())
	snapshot := data.Bytes()
	snapshot[0] ^= 255
	require.Equal(t, payload, data.Bytes(), "Bytes must not expose the immutable encoded backing")
	sliced := data.Slice(8)
	sliced.Bytes()[0] ^= 255
	sliced.Release()
	require.Equal(t, payload, data.Bytes(), "Slice must not change the shared owner")
	require.False(t, data.(fscache.DataCacheAdmission).CacheAdmissionAllowed(nil))
	data.Retain()
	data.Release()
	_, err = scopedChunkedConstructor(t.Context(), iotest.ErrReader(io.ErrUnexpectedEOF), nil, fileservice.DefaultCacheDataAllocator())
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	_, err = scopedChunkedConstructor(t.Context(), nil, []byte("invalid"), fileservice.DefaultCacheDataAllocator())
	require.Error(t, err)
}

func TestBlockTopNReadRejection(t *testing.T) {
	mp := newTopNTestMP(t)
	for _, position := range []int{-1, 1} {
		r, err := ReadBlockForTopN(t.Context(), []uint16{0}, []types.Type{types.T_array_float32.ToType()}, nil, Location{}, position, newTopNTestOp(1), mp, 0)
		require.Error(t, err)
		require.Nil(t, r)
	}
	r, err := ReadBlockForTopN(t.Context(), []uint16{0}, []types.Type{types.T_array_float32.ToType()}, nil, Location{}, 0, newTopNTestOp(1), mp, 0)
	require.Error(t, err)
	require.Nil(t, r)
	storage := newBlockTopNTestFS(t)
	source := newTopNTestVector(t, mp, []float32{1, 2, 3, 4})
	payload, _ := encodeTopNTestChunks(t, source, 2, mp)
	payload[0] ^= 255
	location, _ := persistBlockTopNTest(t, storage, source, payload, mp)
	r, err = ReadBlockForTopN(t.Context(), []uint16{0, 1, 2}, []types.Type{types.T_int64.ToType(), types.T_array_float32.ToType(), types.T_array_float32.ToType()}, storage, location, 1, newTopNTestOp(1), mp, 0)
	require.Error(t, err)
	require.Nil(t, r)
}

type wrappedBlockTopNFS struct{ fileservice.FileService }
type wrappedBlockTopNData struct{ fscache.Data }

func (f *wrappedBlockTopNFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	if err := f.FileService.Read(ctx, v); err != nil {
		return err
	}
	for i := range v.Entries {
		if v.Entries[i].CachedData != nil {
			v.Entries[i].CachedData = &wrappedBlockTopNData{v.Entries[i].CachedData}
		}
	}
	return nil
}

func TestBlockTopNWrappedSource(t *testing.T) {
	mp := newTopNTestMP(t)
	source := newTopNTestVector(t, mp, []float32{4, 1, 3, 2})
	payload, _ := encodeTopNTestChunks(t, source, 2, mp)
	storage := newBlockTopNTestFS(t)
	location, _ := persistBlockTopNTest(t, storage, source, payload, mp)
	r := readBlockTopNTest(t, &wrappedBlockTopNFS{storage}, location, mp, newTopNTestOp(2))
	require.NotEmpty(t, r.encoded)
	rows, dists, err := r.TopN(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 3}, rows)
	require.Equal(t, []float64{1, 4}, dists)
	r.Release()
}
