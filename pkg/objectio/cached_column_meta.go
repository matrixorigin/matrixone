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

import "context"

// CachedColumnMeta borrows immutable metadata for one synchronous observation.
// Cache eviction drops a reference; it does not recycle or modify these bytes.
// Only copied column values leave this view. It must not enter a plan cache.
type CachedColumnMeta struct {
	blocks cachedObjectBlocks
	seq    uint16
}

type ColumnBlockMeta struct {
	Rows    uint32
	ZoneMap [ZoneMapSize]byte
}

// TryCachedColumnMeta never loads, prefetches, or admits metadata on a miss.
// maxBlocks bounds subsequent visits; callers charge their own shared budget.
func TryCachedColumnMeta(ctx context.Context, location Location, seq uint16, maxBlocks int) (CachedColumnMeta, bool) {
	blocks, ok := tryCachedObjectBlocks(ctx, location, maxBlocks)
	if !ok {
		return CachedColumnMeta{}, false
	}
	h := BlockHeader(blocks.data[:headerLen])
	if seq > h.MaxSeqnum() || seq >= h.MetaColumnCount() {
		return CachedColumnMeta{}, false
	}
	return CachedColumnMeta{blocks: blocks, seq: seq}, true
}

func (m CachedColumnMeta) BlockCount() uint32 { return m.blocks.count }

func (m CachedColumnMeta) Object() (ColumnBlockMeta, bool) {
	return checkedColumnBlock(m.blocks.data, m.seq, false)
}

func (m CachedColumnMeta) Block(i uint32) (ColumnBlockMeta, bool) {
	block, ok := m.blocks.block(i)
	if !ok {
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
