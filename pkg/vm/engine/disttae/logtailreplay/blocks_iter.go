// Copyright 2023 Matrix Origin
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

package logtailreplay

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/tidwall/btree"
)

type BlocksIter interface {
	Next() bool
	Close() error
	Entry() BlockEntry
}

type blocksIter struct {
	ts          types.TS
	iter        btree.IterG[BlockEntry]
	firstCalled bool
}

func (p *PartitionState) NewBlocksIter(ts types.TS) *blocksIter {
	iter := p.blocks.Copy().Iter()
	ret := &blocksIter{
		ts:   ts,
		iter: iter,
	}
	return ret
}

var _ BlocksIter = new(blocksIter)

func (b *blocksIter) Next() bool {
	for {

		if !b.firstCalled {
			if !b.iter.First() {
				return false
			}
			b.firstCalled = true
		} else {
			if !b.iter.Next() {
				return false
			}
		}

		entry := b.iter.Item()

		if !entry.Visible(b.ts) {
			// not visible
			continue
		}

		return true
	}
}

func (b *blocksIter) Entry() BlockEntry {
	return b.iter.Item()
}

func (b *blocksIter) Close() error {
	b.iter.Release()
	return nil
}

type dirtyBlocksIter struct {
	iter        btree.IterG[BlockEntry]
	firstCalled bool
}

func (p *PartitionState) NewDirtyBlocksIter() *dirtyBlocksIter {
	iter := p.dirtyBlocks.Copy().Iter()
	ret := &dirtyBlocksIter{
		iter: iter,
	}
	return ret
}

var _ BlocksIter = new(dirtyBlocksIter)

func (b *dirtyBlocksIter) Next() bool {
	if !b.firstCalled {
		if !b.iter.First() {
			return false
		}
		b.firstCalled = true
		return true
	}
	return b.iter.Next()
}

func (b *dirtyBlocksIter) Entry() BlockEntry {
	return b.iter.Item()
}

func (b *dirtyBlocksIter) Close() error {
	b.iter.Release()
	return nil
}

// GetChangedBlocksBetween get changed blocks between two timestamps
func (p *PartitionState) GetChangedBlocksBetween(
	begin types.TS,
	end types.TS,
) (
	deleted []types.Blockid,
	inserted []types.Blockid,
) {

	iter := p.blockIndexByTS.Copy().Iter()
	defer iter.Release()

	for ok := iter.Seek(BlockIndexByTSEntry{
		Time: begin,
	}); ok; ok = iter.Next() {
		entry := iter.Item()

		if entry.Time.Greater(end) {
			break
		}

		if entry.IsDelete {
			deleted = append(deleted, entry.BlockID)
		} else {
			if !entry.IsAppendable {
				inserted = append(inserted, entry.BlockID)
			}
		}

	}

	return
}
