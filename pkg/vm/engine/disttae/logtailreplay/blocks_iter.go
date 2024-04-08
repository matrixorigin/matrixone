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
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/reusee/pt"
	"github.com/tidwall/btree"
)

type ObjectsIter interface {
	Next() bool
	Close() error
	Entry() ObjectEntry
}

type objectsIter struct {
	ts          types.TS
	iter        *pt.Iter[ObjectIndexByCreateTSEntry]
	firstCalled bool
	current     ObjectIndexByCreateTSEntry
}

// not accurate!  only used by stats
func (p *PartitionState) ApproxObjectsNum() int {
	return p.dataObjects.Length()
}

func (p *PartitionState) NewObjectsIter(ts types.TS) (*objectsIter, error) {
	if ts.Less(&p.minTS) {
		return nil, moerr.NewTxnStaleNoCtx()
	}
	iter := p.dataObjectsByCreateTS.NewIter()
	ret := &objectsIter{
		ts:   ts,
		iter: iter,
	}
	return ret, nil
}

var _ ObjectsIter = new(objectsIter)

func (b *objectsIter) Next() bool {
	for {
		var entry ObjectIndexByCreateTSEntry
		var ok bool
		if !b.firstCalled {
			pivot := ObjectIndexByCreateTSEntry{
				ObjectInfo{
					CreateTime: b.ts,
				},
			}
			entry, ok = b.iter.Seek(pivot)
			if !ok {
				return false
			}
			b.firstCalled = true
		} else {
			entry, ok = b.iter.Next()
			if !ok {
				return false
			}
		}

		if !entry.Visible(b.ts) {
			// not visible
			continue
		}

		b.current = entry

		return true
	}
}

func (b *objectsIter) Entry() ObjectEntry {
	return ObjectEntry{
		ObjectInfo: b.current.ObjectInfo,
	}
}

func (b *objectsIter) Close() error {
	b.iter.Close()
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

// GetChangedObjsBetween get changed objects between [begin, end],
// notice that if an object is created after begin and deleted before end, it will be ignored.
func (p *PartitionState) GetChangedObjsBetween(
	begin types.TS,
	end types.TS,
) (
	deleted map[objectio.ObjectNameShort]struct{},
	inserted map[objectio.ObjectNameShort]struct{},
) {
	inserted = make(map[objectio.ObjectNameShort]struct{})
	deleted = make(map[objectio.ObjectNameShort]struct{})

	iter := p.objectIndexByTS.Copy().Iter()
	defer iter.Release()

	for ok := iter.Seek(ObjectIndexByTSEntry{
		Time: begin,
	}); ok; ok = iter.Next() {
		entry := iter.Item()

		if entry.Time.Greater(&end) {
			break
		}

		if entry.IsDelete {
			// if the object is inserted and deleted between [begin, end], it will be ignored.
			if _, ok := inserted[entry.ShortObjName]; !ok {
				deleted[entry.ShortObjName] = struct{}{}
			} else {
				delete(inserted, entry.ShortObjName)
			}
		} else {
			inserted[entry.ShortObjName] = struct{}{}
		}

	}
	return
}

func (p *PartitionState) GetBockDeltaLoc(bid types.Blockid) (objectio.ObjectLocation, types.TS, bool) {
	iter := p.blockDeltas.Copy().Iter()
	defer iter.Release()

	pivot := BlockDeltaEntry{
		BlockID: bid,
	}
	if ok := iter.Seek(pivot); ok {
		e := iter.Item()
		if e.BlockID.Compare(bid) == 0 {
			return e.DeltaLoc, e.CommitTs, true
		}
	}
	return objectio.ObjectLocation{}, types.TS{}, false
}

func (p *PartitionState) BlockPersisted(blockID types.Blockid) bool {
	iter := p.dataObjects.NewIter()
	defer iter.Close()

	pivot := ObjectEntry{}
	objectio.SetObjectStatsShortName(&pivot.ObjectStats, objectio.ShortName(&blockID))
	if e, ok := iter.Seek(pivot); ok {
		if bytes.Equal(e.ObjectShortName()[:], objectio.ShortName(&blockID)[:]) {
			return true
		}
	}
	return false
}

func (p *PartitionState) GetObject(name objectio.ObjectNameShort) (ObjectInfo, bool) {
	iter := p.dataObjects.NewIter()
	defer iter.Close()

	pivot := ObjectEntry{}
	objectio.SetObjectStatsShortName(&pivot.ObjectStats, &name)
	if e, ok := iter.Seek(pivot); ok {
		if bytes.Equal(e.ObjectShortName()[:], name[:]) {
			return e.ObjectInfo, true
		}
	}
	return ObjectInfo{}, false
}
