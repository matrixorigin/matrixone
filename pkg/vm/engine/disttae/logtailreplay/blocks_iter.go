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

// GetBlksBetween get changed blocks between (startTS, endTS]
func (p *PartitionState) GetBlksBetween(
	startTS types.TS,
	endTS types.TS) (
	deletedBlks []types.Blockid,
	createdBlks []types.Blockid) {
	iter := p.blockIndexByTS.Iter()
	defer iter.Release()

	var minBlockid types.Blockid
	for ok := iter.Seek(
		BlockIndexByTSEntry{
			Time:    startTS,
			BlockID: minBlockid,
		}); ok; ok = iter.Next() {
		entry := iter.Item()
		if entry.Time.Equal(startTS) {
			continue
		}
		if entry.Time.Greater(endTS) {
			break
		}
		if entry.IsDelete {
			deletedBlks = append(deletedBlks, entry.BlockID)
		} else {
			createdBlks = append(createdBlks, entry.BlockID)
		}
	}
	return deletedBlks, createdBlks
}
