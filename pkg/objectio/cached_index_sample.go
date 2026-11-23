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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

type cachedObjectBlocks struct {
	data  []byte
	index []byte
	start uint16
	count uint32
}

// The cache holds immutable metadata. The returned slice remains live after
// eviction, and never leaves this synchronous observation.
func tryCachedObjectBlocks(ctx context.Context, location Location, maxBlocks int) (cachedObjectBlocks, bool) {
	if ctx == nil || len(location) != LocationLen || location.IsEmpty() || maxBlocks <= 0 || context.Cause(ctx) != nil {
		return cachedObjectBlocks{}, false
	}
	raw, ok := metaCache.Get(ctx, encodeCacheKey(*location.ShortName(), cacheKeyTypeMeta))
	if !ok {
		return cachedObjectBlocks{}, false
	}
	if len(raw) < IOEntryHeaderSize+metaHeaderLen {
		return cachedObjectBlocks{}, false
	}
	header := DecodeIOEntryHeader(raw)
	if header.Type != IOET_ObjMeta || header.Version != IOET_ObjectMeta_V3 {
		return cachedObjectBlocks{}, false
	}
	prefix := raw[IOEntryHeaderSize:]
	if types.DecodeUint16(prefix[:dataMetaCount]) == 0 {
		return cachedObjectBlocks{}, false
	}
	offset := uint64(types.DecodeUint32(prefix[dataMetaCount:tombstoneMetaCountOff]))
	if offset < metaHeaderLen || offset > uint64(len(prefix)) || uint64(len(prefix))-offset < headerLen {
		return cachedObjectBlocks{}, false
	}
	data := prefix[offset:]
	h := BlockHeader(data[:headerLen])
	blocks := uint32(h.Sequence())
	if blocks == 0 || uint64(blocks) > uint64(maxBlocks) || uint64(h.StartID())+uint64(blocks) > 1<<16 || *h.ShortName() != *location.ShortName() {
		return cachedObjectBlocks{}, false
	}
	columnEnd := uint64(headerLen) + uint64(h.MetaColumnCount())*colMetaLen
	indexEnd := columnEnd + blockCountLen + uint64(blocks)*posLen
	if indexEnd > uint64(len(data)) {
		return cachedObjectBlocks{}, false
	}
	idx := data[columnEnd:indexEnd]
	if types.DecodeUint32(idx[:blockCountLen]) != blocks {
		return cachedObjectBlocks{}, false
	}
	return cachedObjectBlocks{data: data, index: idx, start: h.StartID(), count: blocks}, true
}

func (m cachedObjectBlocks) block(i uint32) (BlockObject, bool) {
	if i >= m.count {
		return nil, false
	}
	pos := uint64(blockCountLen) + uint64(i)*posLen
	offset := uint64(types.DecodeUint32(m.index[pos : pos+blockOffset]))
	length := uint64(types.DecodeUint32(m.index[pos+blockOffset : pos+posLen]))
	minimum := uint64(headerLen) + uint64(BlockHeader(m.data[:headerLen]).MetaColumnCount())*colMetaLen + uint64(len(m.index))
	if offset < minimum || offset > uint64(len(m.data)) || length < headerLen || length > uint64(len(m.data))-offset {
		return nil, false
	}
	block := BlockObject(m.data[offset : offset+length])
	if uint32(BlockHeader(block[:headerLen]).Sequence()) != uint32(m.start)+i {
		return nil, false
	}
	return block, true
}

const maxCachedSampleColumns = 4

type CachedInt32Block struct {
	Rows    uint32
	Extents [maxCachedSampleColumns][ExtentSize]byte
}

// CachedInt32Blocks copies only physical extents for the requested columns.
// A miss or unsupported column aborts the entire object observation.
func CachedInt32Blocks(ctx context.Context, location Location, seqnums []uint16, dst []CachedInt32Block) bool {
	if len(seqnums) == 0 || len(seqnums) > maxCachedSampleColumns {
		return false
	}
	view, ok := tryCachedObjectBlocks(ctx, location, len(dst))
	if !ok || int(view.count) != len(dst) {
		return false
	}
	for i := range dst {
		block, ok := view.block(uint32(i))
		if !ok || context.Cause(ctx) != nil {
			return false
		}
		h := BlockHeader(block[:headerLen])
		end := uint64(headerLen) + uint64(h.MetaColumnCount())*colMetaLen
		if h.Rows() == 0 || h.Rows() > BlockMaxRows || end > uint64(len(block)) {
			return false
		}
		dst[i].Rows = h.Rows()
		for j, seq := range seqnums {
			if seq > h.MaxSeqnum() || seq >= h.MetaColumnCount() {
				return false
			}
			column := GetColumnMeta(seq, block)
			extent := column.Location()
			if types.T(column.DataType()) != types.T_int32 || extent.Length() == 0 || extent.OriginSize() == 0 ||
				extent.Alg() != compress.None && extent.Alg() != compress.Lz4 {
				return false
			}
			copy(dst[i].Extents[j][:], extent)
		}
	}
	return true
}

type CachedInt32Row struct {
	Values  [maxCachedSampleColumns]int32
	Present uint8
}

// ReadCachedInt32Rows uses the normal column cache keys and borrows leases for
// one block. Every preselected row must be present. The result owns scalar
// values only and can outlive eviction.
func ReadCachedInt32Rows(
	ctx context.Context, fs fileservice.FileService, name string, block CachedInt32Block,
	numCols int, picks []int64, dst []CachedInt32Row, mp *mpool.MPool,
) bool {
	// These concrete backends use only memory when no vector caches are set.
	switch fs.(type) {
	case *fileservice.LocalFS, *fileservice.S3FS:
	default:
		return false
	}
	if ctx == nil || context.Cause(ctx) != nil || mp == nil || numCols <= 0 || numCols > maxCachedSampleColumns ||
		len(picks) == 0 || len(dst) != len(picks) {
		return false
	}
	var entries [maxCachedSampleColumns]fileservice.IOEntry
	ioVec := fileservice.IOVector{FilePath: name, Entries: entries[:numCols]}
	for i := range ioVec.Entries {
		ioVec.Entries[i] = newColumnIOEntry(Extent(block.Extents[i][:]), columnCacheConstructorFactory)
	}
	defer ioVec.ReleaseReadResultOnError()
	if err := fs.ReadCache(ctx, &ioVec); err != nil {
		return false
	}
	for _, entry := range ioVec.Entries {
		if entry.CachedData == nil {
			return false
		}
	}
	clear(dst)
	for i, entry := range ioVec.Entries {
		if context.Cause(ctx) != nil || !copyCachedInt32Samples(entry, i, picks, dst, mp) {
			return false
		}
	}
	return true
}

func copyCachedInt32Samples(entry fileservice.IOEntry, pos int, picks []int64, dst []CachedInt32Row, mp *mpool.MPool) bool {
	vec := vector.NewVec(types.T_int32.ToType())
	defer vec.Free(mp)
	if err := CopyCachedVectorRows(vec, entry.CachedData, picks, mp); err != nil {
		return false
	}
	for i := range dst {
		if !vec.IsNull(uint64(i)) {
			dst[i].Values[pos] = vector.GetFixedAtNoTypeCheck[int32](vec, i)
			dst[i].Present |= 1 << pos
		}
	}
	return true
}
