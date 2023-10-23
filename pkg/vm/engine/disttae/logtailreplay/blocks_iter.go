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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/tidwall/btree"
)

type ObjectsIter interface {
	Next() bool
	Close() error
	Entry() ObjectEntry
}

type objectsIter struct {
	ts          types.TS
	iter        btree.IterG[ObjectEntry]
	firstCalled bool
}

// not accurate!  only used by stats
func (p *PartitionState) ApproxObjectsNum() int {
	return p.dataObjects.Len()
}

func (p *PartitionState) NewObjectsIter(ts types.TS) (*objectsIter, error) {
	if ts.Less(p.minTS) {
		return nil, moerr.NewTxnStaleNoCtx()
	}
	iter := p.dataObjects.Copy().Iter()
	ret := &objectsIter{
		ts:   ts,
		iter: iter,
	}
	return ret, nil
}

var _ ObjectsIter = new(objectsIter)

func (b *objectsIter) Next() bool {
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

func (b *objectsIter) Entry() ObjectEntry {
	return b.iter.Item()
}

func (b *objectsIter) Close() error {
	b.iter.Release()
	return nil
}

type BlocksIter interface {
	Next() bool
	Close() error
	Entry() types.Blockid
}

type dirtyBlocksIter struct {
	iter        btree.IterG[types.Blockid]
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

func (b *dirtyBlocksIter) Entry() types.Blockid {
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

func (p *PartitionState) GetBockInfo(bid types.Blockid) (loc catalog.ObjectLocation, commitTs types.TS, ok bool) {
	iter := p.blockDeltas.Copy().Iter()
	defer iter.Release()

	if ok := iter.Seek(BlockDeltaEntry{
		BlockID: bid,
	}); ok {
		e := iter.Item()
		return e.DeltaLoc, e.CommitTs, true
	}
	return catalog.ObjectLocation{}, types.TS{}, false
}

//func (p *PartitionState) GetBockCommitTs(bid types.Blockid) (ts types.TS, ok bool) {
//	iter := p.blockDeltas.Copy().Iter()
//	defer iter.Release()
//
//	if ok := iter.Seek(BlockDeltaEntry{
//		BlockID: bid,
//	}); ok {
//		return iter.Item().CommitTs, true
//	}
//	return types.TS{}, false
//}

func (p *PartitionState) BlockPersisted(blockID types.Blockid) bool {
	iter := p.dataObjects.Copy().Iter()
	defer iter.Release()

	if ok := iter.Seek(ObjectEntry{
		ShortObjName: *objectio.ShortName(&blockID),
	}); ok {
		return true
	}
	return false
}
