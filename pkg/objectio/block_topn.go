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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

// BlockTopNRead owns one query-local block read. Entry exposes only sibling
// columns; the selected column must be consumed through TopN. Borrowed views
// cannot escape Release, and methods must not be used concurrently.
type BlockTopNRead struct {
	block, chunks fileservice.IOVector
	reader        chunkTopNReader
	metas         []columnChunkMeta
	encoded       []byte // owned snapshot only when an external wrapper hides the scoped marker
	top           *IndexReaderTopOp
	mp            *mpool.MPool
	position      int
	rows          uint32
	fromCache     bool
	released      bool
}

func (r *BlockTopNRead) Entry(position int) fileservice.IOEntry { return r.block.Entries[position] }
func (r *BlockTopNRead) FromCache() bool                        { return r.fromCache }

func (r *BlockTopNRead) Release() {
	if r == nil || r.released {
		return
	}
	r.released = true
	r.block.ReleaseReadResultOnError()
	r.chunks.ReleaseReadResultOnError()
	r.block.Entries, r.chunks.Entries, r.metas = nil, nil, nil
	r.encoded = nil
}

// ReadBlockForTopN preserves a combined storage request while allowing TopN to
// consume the selected chunked column after the caller evaluates its filters.
func ReadBlockForTopN(
	ctx context.Context, columns []uint16, typs []types.Type, fs fileservice.FileService,
	location Location, position int, top *IndexReaderTopOp, mp *mpool.MPool, policy fileservice.Policy,
) (result *BlockTopNRead, err error) {
	if len(columns) != len(typs) || position < 0 || position >= len(columns) || mp == nil || top == nil {
		return nil, moerr.NewInvalidInputNoCtx("invalid fused block topn input")
	}
	meta, err := FastLoadObjectMeta(ctx, &location, false, fs)
	if err != nil {
		return nil, err
	}
	dataMeta := meta.MustGetMeta(SchemaData)
	blk := dataMeta.GetBlockMeta(uint32(location.ID()))
	r := &BlockTopNRead{position: position, top: top, mp: mp, rows: blk.GetRows(), fromCache: true,
		reader: chunkTopNReader{name: location.Name().UnsafeString(), fs: fs,
			policy: policy | fileservice.SkipFullFilePreloads, kind: typs[position].Oid}}
	defer func() {
		if err != nil {
			r.Release()
		}
	}()
	column := columns[position]
	if column < SEQNUM_UPPER && column <= blk.GetMaxSeqnum() && blk.ColumnMeta(column).DataType() != 0 {
		r.reader.ext = blk.ColumnMeta(column).Location()
	}
	eligible := r.reader.ext != nil && r.reader.ext.Alg() == compress.Lz4Chunked &&
		typs[position].Oid.IsArrayRelate() && top.Typ == typs[position].Oid && !top.OrderedLimit && !top.Desc
	if !eligible {
		r.block, err = ReadOneBlockWithScopedDecode(ctx, &dataMeta, r.reader.name, location.ID(), columns, typs, mp, fs, policy, position)
	} else {
		err = r.readSources(ctx, &dataMeta, location.ID(), columns, typs, policy)
	}
	if err != nil {
		return nil, err
	}
	r.block.ReleaseReadBuffers()
	for i := range r.block.Entries {
		if i == position && len(r.chunks.Entries) > 0 {
			continue
		}
		r.fromCache = r.fromCache && r.block.Entries[i].WasFromCache()
	}
	return r, nil
}

func (r *BlockTopNRead) readSources(ctx context.Context, meta *ObjectDataMeta, blk uint16, columns []uint16, typs []types.Type, policy fileservice.Policy) error {
	whole := fileservice.IOVector{FilePath: r.reader.name, Policy: policy,
		Entries: []fileservice.IOEntry{newColumnIOEntry(r.reader.ext, columnCacheConstructorFactory)}}
	defer whole.ReleaseReadResultOnError()
	if err := r.reader.fs.ReadCache(ctx, &whole); err != nil {
		return err
	}
	if whole.Entries[0].CachedData != nil {
		if err := r.readSiblings(ctx, meta, blk, columns, typs, policy); err != nil {
			return err
		}
		r.block.Entries[r.position] = whole.Entries[0]
		whole.Entries[0] = fileservice.IOEntry{} // transfer all cache ownership hooks
		recordTopNReadBytes(ctx, int64(r.reader.ext.Length()))
		return nil
	}
	if err := r.probeChunks(ctx); err != nil {
		return err
	}
	if len(r.chunks.Entries) > 0 {
		recordTopNReadBytes(ctx, int64(r.reader.ext.Length()))
		return r.readSiblings(ctx, meta, blk, columns, typs, policy)
	}
	ext := r.reader.ext
	entry := fileservice.IOEntry{Offset: int64(ext.Offset()), Size: int64(ext.Length()), CachedDataSize: int64(ext.Length()),
		ValidateCacheData: validateVectorCacheData,
		DecodeSharing:     fileservice.DecodeSharing{Codec: "objectio-scoped-chunked-v1", Parameters: [2]uint64{uint64(ext.Alg()), uint64(ext.OriginSize())}},
		ToCacheData:       scopedChunkedConstructor}
	var err error
	r.block, err = readOneBlockWithMeta(ctx, meta, r.reader.name, blk, columns, typs, r.mp, r.reader.fs,
		columnCacheConstructorFactory, policy, r.position, &entry)
	if err != nil {
		return err
	}
	data := r.encodedBytes()
	if data == nil && !isValidatedVectorCacheData(r.block.Entries[r.position].CachedData) {
		// FileService decorators may hide the concrete scoped result. Preserve
		// correctness through an owned snapshot instead of requiring them to
		// implement an ObjectIO-private marker. Ordinary decoded data stays on
		// the existing cached-vector path.
		buf := r.block.Entries[r.position].CachedData.Bytes()
		if len(buf) >= len(columnChunkMagic) && bytes.Equal(buf[:len(columnChunkMagic)], columnChunkMagic[:]) {
			r.encoded = bytes.Clone(buf)
			data = r.encoded
		}
	}
	if data != nil {
		var rows uint32
		rows, r.metas, err = parseColumnChunkHeader(data, ext.Length())
		if err != nil {
			return err
		}
		if rows != r.rows {
			return moerr.NewInvalidInputNoCtx("chunked object column block row count mismatch")
		}
		// Populate precisely the same header keys used by the range reader.
		for _, size := range []uint32{columnChunkHeaderSize, uint32(columnChunkHeaderSize + len(r.metas)*columnChunkEntrySize)} {
			v := fileservice.IOVector{FilePath: r.reader.name, Policy: r.reader.policy,
				Entries: []fileservice.IOEntry{r.reader.entry(0, size, size, compress.None, false)}}
			if err := fileservice.DecodeFromBytes(ctx, r.reader.fs, &v, data[:size]); err != nil {
				return err
			}
			v.Release()
		}
	}
	return nil
}

func (r *BlockTopNRead) encodedBytes() []byte {
	if raw, ok := r.block.Entries[r.position].CachedData.(*scopedChunkedData); ok {
		return raw.data.Bytes()
	}
	return r.encoded
}

func (r *BlockTopNRead) readSiblings(ctx context.Context, meta *ObjectDataMeta, blk uint16, columns []uint16, typs []types.Type, policy fileservice.Policy) error {
	cols := append(slices.Clone(columns[:r.position]), columns[r.position+1:]...)
	types := append(slices.Clone(typs[:r.position]), typs[r.position+1:]...)
	v, err := ReadOneBlock(ctx, meta, r.reader.name, blk, cols, types, r.mp, r.reader.fs, policy)
	if err != nil {
		return err
	}
	entries := make([]fileservice.IOEntry, len(columns))
	copy(entries, v.Entries[:r.position])
	copy(entries[r.position+1:], v.Entries[r.position:])
	v.Entries = entries
	r.block = v
	return nil
}

func (r *BlockTopNRead) probeChunks(ctx context.Context) error {
	prefix := fileservice.IOVector{FilePath: r.reader.name, Policy: r.reader.policy,
		Entries: []fileservice.IOEntry{r.reader.entry(0, columnChunkHeaderSize, columnChunkHeaderSize, compress.None, false)}}
	defer prefix.ReleaseReadResultOnError()
	if err := r.reader.fs.ReadCache(ctx, &prefix); err != nil {
		return err
	}
	if prefix.Entries[0].CachedData == nil {
		return nil
	}
	size, err := chunkedColumnHeaderReadSize(prefix.Entries[0].CachedData.Bytes(), r.reader.ext.Length())
	if err != nil {
		return err
	}
	header := fileservice.IOVector{FilePath: r.reader.name, Policy: r.reader.policy,
		Entries: []fileservice.IOEntry{r.reader.entry(0, uint32(size), uint32(size), compress.None, false)}}
	defer header.ReleaseReadResultOnError()
	if err := r.reader.fs.ReadCache(ctx, &header); err != nil {
		return err
	}
	if header.Entries[0].CachedData == nil {
		return nil
	}
	rows, metas, err := parseColumnChunkHeader(header.Entries[0].CachedData.Bytes(), r.reader.ext.Length())
	if err != nil {
		return err
	}
	if rows != r.rows {
		return moerr.NewInvalidInputNoCtx("chunked object column block row count mismatch")
	}
	r.chunks = fileservice.IOVector{FilePath: r.reader.name, Policy: r.reader.policy, Entries: make([]fileservice.IOEntry, len(metas))}
	for i, meta := range metas {
		r.chunks.Entries[i] = r.reader.entry(meta.offset, meta.length, meta.originSize, meta.algorithm, true)
	}
	if err := r.reader.fs.ReadCache(ctx, &r.chunks); err != nil {
		return err
	}
	for _, entry := range r.chunks.Entries {
		if entry.CachedData == nil {
			r.chunks.ReleaseReadResultOnError()
			r.chunks.Entries = nil
			return nil
		}
	}
	r.metas = metas
	return nil
}

// scopedChunkedData is never admitted at the full decoded-column cache key.
// Only ObjectIO can borrow its immutable encoded backing inside a read scope.
type scopedChunkedData struct{ data fscache.Data }

func (d *scopedChunkedData) Bytes() []byte                                 { return bytes.Clone(d.data.Bytes()) }
func (d *scopedChunkedData) Size() int64                                   { return d.data.Size() }
func (d *scopedChunkedData) Capacity() int64                               { return d.data.Capacity() }
func (d *scopedChunkedData) Retain()                                       { d.data.Retain() }
func (d *scopedChunkedData) Release()                                      { d.data.Release() }
func (d *scopedChunkedData) CacheAdmissionAllowed(*fscache.DataOwner) bool { return false }
func (d *scopedChunkedData) Slice(n int) fscache.Data {
	return fileservice.NewBytes(bytes.Clone(d.data.Bytes()[:n]))
}

func scopedChunkedConstructor(ctx context.Context, reader io.Reader, data []byte, allocator fileservice.CacheDataAllocator) (fscache.Data, error) {
	if len(data) == 0 {
		var err error
		data, err = io.ReadAll(reader)
		if err != nil {
			return nil, err
		}
	}
	if uint64(len(data)) > uint64(^uint32(0)) {
		return nil, moerr.NewInvalidInputNoCtx("chunked column exceeds extent size")
	}
	if _, _, err := parseColumnChunkHeader(data, uint32(len(data))); err != nil {
		return nil, err
	}
	return &scopedChunkedData{data: allocator.CopyToCacheData(ctx, data)}, nil
}

func (r *BlockTopNRead) chunk(ctx context.Context, i int) (fileservice.IOEntry, func(), error) {
	if len(r.chunks.Entries) > 0 {
		return r.chunks.Entries[i], func() {}, nil
	}
	meta := r.metas[i]
	raw := r.encodedBytes()
	v := fileservice.IOVector{FilePath: r.reader.name, Policy: r.reader.policy,
		Entries: []fileservice.IOEntry{r.reader.entry(meta.offset, meta.length, meta.originSize, meta.algorithm, true)}}
	if err := fileservice.DecodeFromBytes(ctx, r.reader.fs, &v, raw[uint64(meta.offset):uint64(meta.offset)+uint64(meta.length)]); err != nil {
		return fileservice.IOEntry{}, nil, err
	}
	return v.Entries[0], v.Release, nil
}

func (r *BlockTopNRead) TopN(ctx context.Context, selected []int64) ([]int64, []float64, error) {
	if r.released {
		return nil, nil, moerr.NewInvalidStateNoCtx("block topn read already released")
	}
	if r.metas == nil {
		return SearchCachedVectorTopN(ctx, r.block.Entries[r.position], selected, r.top)
	}
	reader := r.reader // type-validation state is local to this traversal
	if !slices.IsSorted(selected) || r.top.OrderedLimit || r.top.Desc || r.top.Typ != reader.kind {
		return r.wholeTopN(ctx, &reader, selected)
	}
	return searchChunkedTopN(ctx, r.rows, r.metas, selected, r.top,
		func(i int, meta columnChunkMeta, rows []int64, ordinal int, acc *vectorTopAccumulator) error {
			entry, release, err := r.chunk(ctx, i)
			if err != nil {
				return err
			}
			defer release()
			return reader.consumeData(ctx, entry.CachedData, meta, rows, ordinal, acc)
		})
}

func (r *BlockTopNRead) wholeTopN(ctx context.Context, reader *chunkTopNReader, selected []int64) ([]int64, []float64, error) {
	var whole *vector.Vector
	defer func() {
		if whole != nil {
			whole.Free(r.mp)
		}
	}()
	for i, meta := range r.metas {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		entry, release, err := r.chunk(ctx, i)
		if err != nil {
			return nil, nil, err
		}
		var source vector.Vector
		err = reader.bindChunk(&source, entry.CachedData, meta)
		if err == nil {
			if whole == nil {
				whole = vector.NewVec(*source.GetType())
			}
			err = whole.UnionBatch(&source, 0, source.Length(), nil, r.mp)
		}
		source.Free(nil)
		release()
		if err != nil {
			return nil, nil, err
		}
	}
	return TopNVector(ctx, selected, whole, r.top)
}
