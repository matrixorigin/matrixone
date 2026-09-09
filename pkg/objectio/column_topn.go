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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
)

// ReadColumnTopN keeps all decoded views inside their complete IOVector scope.
// Chunked cache misses consume one chunk at a time; only owned row coordinates
// and distances escape. Legacy and unsorted selections retain the whole-column
// path, including its selection order and equal-distance winners.
func ReadColumnTopN(
	ctx context.Context, column uint16, typ types.Type, fs fileservice.FileService,
	location Location, selected []int64, top *IndexReaderTopOp, mp *mpool.MPool,
	policy fileservice.Policy,
) ([]int64, []float64, bool, error) {
	if mp == nil {
		return nil, nil, false, moerr.NewInvalidInputNoCtx("nil mpool for object column topn")
	}
	meta, err := FastLoadObjectMeta(ctx, &location, false, fs)
	if err != nil {
		return nil, nil, false, err
	}
	dataMeta := meta.MustGetMeta(SchemaData)
	return readColumnTopNWithMeta(ctx, &dataMeta, location.Name().UnsafeString(), location.ID(),
		column, typ, selected, top, mp, fs, policy)
}

func readColumnTopNWithMeta(
	ctx context.Context, meta *ObjectDataMeta, name string, block uint16,
	column uint16, typ types.Type, selected []int64, top *IndexReaderTopOp,
	mp *mpool.MPool, fs fileservice.FileService, policy fileservice.Policy,
) ([]int64, []float64, bool, error) {
	blk := meta.GetBlockMeta(uint32(block))
	var ext Extent
	if column < SEQNUM_UPPER && column <= blk.GetMaxSeqnum() && blk.ColumnMeta(column).DataType() != 0 {
		ext = blk.ColumnMeta(column).Location()
	}
	if ext == nil || ext.Alg() != compress.Lz4Chunked || !typ.Oid.IsArrayRelate() ||
		top == nil || top.Typ != typ.Oid || top.OrderedLimit || top.Desc || !slices.IsSorted(selected) {
		ioVec, err := ReadOneBlock(ctx, meta, name, block, []uint16{column}, []types.Type{typ}, mp, fs, policy, ShareScopedDecodedColumn)
		if err != nil {
			return nil, nil, false, err
		}
		defer ioVec.Release()
		rows, distances, err := SearchCachedVectorTopN(ctx, ioVec.Entries[0], selected, top)
		return rows, distances, ioVec.Entries[0].WasFromCache(), err
	}

	// Do not trade an existing decoded full-column hit for chunk reads. This
	// cache-only probe neither reconstructs the column nor performs storage I/O.
	probe := fileservice.IOVector{FilePath: name, Policy: policy,
		Entries: []fileservice.IOEntry{newColumnIOEntry(ext, columnCacheConstructorFactory)}}
	if err := fs.ReadCache(ctx, &probe); err != nil {
		probe.ReleaseReadResultOnError()
		return nil, nil, false, err
	}
	if probe.Entries[0].CachedData != nil {
		defer probe.Release()
		recordTopNReadBytes(ctx, probe.Entries[0].Size)
		rows, distances, err := SearchCachedVectorTopN(ctx, probe.Entries[0], selected, top)
		return rows, distances, true, err
	}
	probe.ReleaseReadResultOnError()
	return readChunkedColumnTopN(ctx, name, ext, blk.GetRows(), typ.Oid, selected, top, fs, policy)
}

type chunkTopNReader struct {
	name      string
	ext       Extent
	fs        fileservice.FileService
	policy    fileservice.Policy
	fromCache bool
	kind      types.T
	chunkType types.Type
	hasType   bool
}

func recordTopNReadBytes(ctx context.Context, size int64) {
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.ReadSize.Add(size)
	})
}

func (r *chunkTopNReader) read(ctx context.Context, offset, length, originSize uint32, algorithm uint8, column bool) (fileservice.IOVector, error) {
	entry := fileservice.IOEntry{
		// Widen before addition: relative offsets must not wrap at 4 GiB.
		Offset: int64(r.ext.Offset()) + int64(offset), Size: int64(length), CachedDataSize: int64(originSize),
		ToCacheData: constructorFactory(int64(originSize), algorithm),
	}
	if column {
		entry.ToCacheData = columnCacheConstructorFactory(int64(originSize), algorithm)
		entry.ValidateCacheData = validateVectorCacheData
		entry.DecodeSharing = fileservice.DecodeSharing{
			Codec: "objectio-validated-column-v1", Parameters: [2]uint64{uint64(algorithm), uint64(originSize)},
		}
	}
	ioVec := fileservice.IOVector{FilePath: r.name, Policy: r.policy, Entries: []fileservice.IOEntry{entry}}
	if err := r.fs.Read(ctx, &ioVec); err != nil {
		ioVec.ReleaseReadResultOnError()
		return fileservice.IOVector{}, err
	}
	r.fromCache = r.fromCache && ioVec.Entries[0].WasFromCache()
	recordTopNReadBytes(ctx, ioVec.Entries[0].Size)
	return ioVec, nil
}

func (r *chunkTopNReader) directory(ctx context.Context) (uint32, []columnChunkMeta, error) {
	if r.ext.Length() < columnChunkHeaderSize {
		return 0, nil, moerr.NewInvalidInputNoCtx("invalid chunked object column prefix")
	}
	prefix, err := r.read(ctx, 0, columnChunkHeaderSize, columnChunkHeaderSize, compress.None, false)
	if err != nil {
		return 0, nil, err
	}
	headerSize, err := chunkedColumnHeaderReadSize(prefix.Entries[0].CachedData.Bytes(), r.ext.Length())
	prefix.Release()
	if err != nil {
		return 0, nil, err
	}
	header, err := r.read(ctx, 0, uint32(headerSize), uint32(headerSize), compress.None, false)
	if err != nil {
		return 0, nil, err
	}
	defer header.Release()
	return parseColumnChunkHeader(header.Entries[0].CachedData.Bytes(), r.ext.Length())
}

func (r *chunkTopNReader) consume(ctx context.Context, meta columnChunkMeta, selected []int64, ordinalBase int, acc *vectorTopAccumulator) error {
	ioVec, err := r.read(ctx, meta.offset, meta.length, meta.originSize, meta.algorithm, true)
	if err != nil {
		return err
	}
	defer ioVec.Release()
	data := ioVec.Entries[0].CachedData
	if data.Size() != int64(meta.originSize) {
		return moerr.NewInvalidInputNoCtx("chunked object column decompressed size mismatch")
	}
	var source vector.Vector
	if err = bindCachedVectorForScope(&source, data); err != nil {
		return err
	}
	defer source.Free(nil)
	if source.Length() != int(meta.rowCount) {
		return moerr.NewInvalidInputNoCtx("chunked object column payload row count mismatch")
	}
	if source.GetType().Oid != r.kind || (r.hasType && *source.GetType() != r.chunkType) {
		return moerr.NewInvalidInputNoCtx("chunked object column payload type mismatch")
	}
	r.chunkType, r.hasType = *source.GetType(), true
	return acc.consume(ctx, &source, int64(meta.rowStart), selected, ordinalBase)
}

func readChunkedColumnTopN(
	ctx context.Context, name string, ext Extent, rows uint32, kind types.T,
	selected []int64, top *IndexReaderTopOp, fs fileservice.FileService, policy fileservice.Policy,
) ([]int64, []float64, bool, error) {
	r := chunkTopNReader{name: name, ext: ext, fs: fs, kind: kind,
		policy: policy | fileservice.SkipFullFilePreloads, fromCache: true}
	totalRows, metas, err := r.directory(ctx)
	if err != nil {
		return nil, nil, false, err
	}
	if totalRows != rows {
		return nil, nil, false, moerr.NewInvalidInputNoCtx("chunked object column block row count mismatch")
	}
	count := int(totalRows)
	if selected != nil {
		count = len(selected)
	}
	acc, err := newVectorTopAccumulator(ctx, top, count)
	if err != nil {
		return nil, nil, false, err
	}
	position := 0
	for _, meta := range metas {
		if err = ctx.Err(); err != nil {
			return nil, nil, false, err
		}
		if acc.emptyRange {
			break
		}
		var chunkRows []int64
		ordinalBase := int(meta.rowStart)
		if selected != nil {
			for position < len(selected) && selected[position] < int64(meta.rowStart) {
				position++
			}
			start := position
			for position < len(selected) && selected[position] < int64(meta.rowStart)+int64(meta.rowCount) {
				position++
			}
			if start == position {
				continue
			}
			chunkRows, ordinalBase = selected[start:position], start
		}
		if err = r.consume(ctx, meta, chunkRows, ordinalBase, &acc); err != nil {
			return nil, nil, false, err
		}
	}
	if err = ctx.Err(); err != nil {
		return nil, nil, false, err
	}
	winners, distances, err := acc.finish()
	return winners, distances, r.fromCache, err
}
