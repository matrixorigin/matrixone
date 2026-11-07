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

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// CachedColumnMeta borrows immutable metadata for one synchronous observation.
// Cache eviction drops a reference; it does not recycle or modify these bytes.
// Only copied column values leave this view. It must not enter a plan cache.
type CachedColumnMeta struct {
	data   []byte
	index  []byte
	seq    uint16
	start  uint16
	blocks uint32
}

type ColumnBlockMeta struct {
	Rows    uint32
	ZoneMap [ZoneMapSize]byte
}

// TryCachedColumnMeta never loads, prefetches, or admits metadata on a miss.
// maxBlocks bounds subsequent visits; callers charge their own shared budget.
func TryCachedColumnMeta(ctx context.Context, location Location, seq uint16, maxBlocks int) (CachedColumnMeta, bool) {
	if ctx == nil || len(location) != LocationLen || location.IsEmpty() || maxBlocks <= 0 || context.Cause(ctx) != nil {
		return CachedColumnMeta{}, false
	}
	raw, _, ok, _ := metaCache.TryGet(encodeCacheKey(*location.ShortName(), cacheKeyTypeMeta), nil)
	if !ok {
		return CachedColumnMeta{}, false
	}
	// metaBytes is Go-managed and Release is a no-op; the view's slice keeps
	// its immutable backing alive even after the cache evicts the entry.
	defer raw.Release()
	if len(raw) < IOEntryHeaderSize+metaHeaderLen {
		return CachedColumnMeta{}, false
	}
	header := DecodeIOEntryHeader(raw)
	if header.Type != IOET_ObjMeta || header.Version != IOET_ObjectMeta_V3 {
		return CachedColumnMeta{}, false
	}
	prefix := raw[IOEntryHeaderSize:]
	if types.DecodeUint16(prefix[:dataMetaCount]) == 0 {
		return CachedColumnMeta{}, false
	}
	offset := uint64(types.DecodeUint32(prefix[dataMetaCount:tombstoneMetaCountOff]))
	if offset < metaHeaderLen || offset > uint64(len(prefix)) || uint64(len(prefix))-offset < headerLen {
		return CachedColumnMeta{}, false
	}
	data := prefix[offset:]
	h := BlockHeader(data[:headerLen])
	blocks := uint32(h.Sequence())
	if blocks == 0 || uint64(blocks) > uint64(maxBlocks) || uint64(h.StartID())+uint64(blocks) > 1<<16 || *h.ShortName() != *location.ShortName() {
		return CachedColumnMeta{}, false
	}
	columnEnd := uint64(headerLen) + uint64(h.MetaColumnCount())*colMetaLen
	indexEnd := columnEnd + blockCountLen + uint64(blocks)*posLen
	if seq > h.MaxSeqnum() || seq >= h.MetaColumnCount() || indexEnd > uint64(len(data)) {
		return CachedColumnMeta{}, false
	}
	idx := data[columnEnd:indexEnd]
	if types.DecodeUint32(idx[:blockCountLen]) != blocks {
		return CachedColumnMeta{}, false
	}
	return CachedColumnMeta{data: data, index: idx, seq: seq, start: h.StartID(), blocks: blocks}, true
}

func (m CachedColumnMeta) BlockCount() uint32 { return m.blocks }

func (m CachedColumnMeta) Object() (ColumnBlockMeta, bool) {
	return checkedColumnBlock(m.data, m.seq, false)
}

func (m CachedColumnMeta) Block(i uint32) (ColumnBlockMeta, bool) {
	if i >= m.blocks {
		return ColumnBlockMeta{}, false
	}
	pos := uint64(blockCountLen) + uint64(i)*posLen
	if pos+posLen > uint64(len(m.index)) {
		return ColumnBlockMeta{}, false
	}
	offset := uint64(types.DecodeUint32(m.index[pos : pos+blockOffset]))
	length := uint64(types.DecodeUint32(m.index[pos+blockOffset : pos+posLen]))
	// Block payloads follow the complete object header, column and index area.
	minimum := uint64(headerLen) + uint64(BlockHeader(m.data[:headerLen]).MetaColumnCount())*colMetaLen + uint64(len(m.index))
	if offset < minimum || offset > uint64(len(m.data)) || length < headerLen || length > uint64(len(m.data))-offset {
		return ColumnBlockMeta{}, false
	}
	block := m.data[offset : offset+length]
	if uint32(BlockHeader(block[:headerLen]).Sequence()) != uint32(m.start)+i {
		return ColumnBlockMeta{}, false
	}
	return checkedColumnBlock(block, m.seq, true)
}

func checkedColumnBlock(data []byte, seq uint16, block bool) (ColumnBlockMeta, bool) {
	if len(data) < headerLen {
		return ColumnBlockMeta{}, false
	}
	h := BlockHeader(data[:headerLen])
	end := uint64(headerLen) + uint64(h.MetaColumnCount())*colMetaLen
	if seq > h.MaxSeqnum() || seq >= h.MetaColumnCount() || end > uint64(len(data)) || h.Rows() == 0 || block && h.Rows() > BlockMaxRows {
		return ColumnBlockMeta{}, false
	}
	column := data[headerLen+uint64(seq)*colMetaLen : headerLen+(uint64(seq)+1)*colMetaLen]
	var out ColumnBlockMeta
	out.Rows = h.Rows()
	copy(out.ZoneMap[:], column[zoneMapOff:zoneMapOff+zoneMapLen])
	if !ZoneMap(out.ZoneMap[:]).Valid() {
		return ColumnBlockMeta{}, false
	}
	return out, true
}
