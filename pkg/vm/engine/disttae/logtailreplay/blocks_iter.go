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
	"github.com/tidwall/btree"
)

type ObjectsIter interface {
	Next() bool
	Close() error
	Entry() ObjectEntry
}

type objectsIter struct {
	ts          types.TS
	iter        btree.IterG[ObjectIndexByCreateTSEntry]
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
	iter := p.dataObjectsByCreateTS.Copy().Iter()
	ret := &objectsIter{
		ts:   ts,
		iter: iter,
	}
	return ret, nil
}

var _ ObjectsIter = new(objectsIter)

func (b *objectsIter) Next() bool {
	for {

		pivot := ObjectIndexByCreateTSEntry{
			ObjectInfo{
				CreateTime: b.ts.Next(),
			},
		}
		if !b.firstCalled {
			if !b.iter.Seek(pivot) {
				if !b.iter.Last() {
					return false
				}
			}
			b.firstCalled = true
		} else {
			if !b.iter.Prev() {
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
	return ObjectEntry{
		ObjectInfo: b.iter.Item().ObjectInfo,
	}
}

func (b *objectsIter) Close() error {
	b.iter.Release()
	return nil
}

// for test
type ObjectsIterForIndexByTS struct {
	iter        btree.IterG[ObjectIndexByTSEntry]
	firstCalled bool
	p           *PartitionState
}

func (p *PartitionState) NewObjectsIterForIndexByTS() (*ObjectsIterForIndexByTS, error) {
	iter := p.objectIndexByTS.Copy().Iter()
	ret := &ObjectsIterForIndexByTS{
		iter: iter,
		p:    p,
	}
	return ret, nil
}

//var _ ObjectsIter = new(ObjectsIterForIndexByTS)

func (b *ObjectsIterForIndexByTS) Next() bool {
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
		return true
	}
}

func (b *ObjectsIterForIndexByTS) Entry() ObjectIndexByTSEntry {
	return b.iter.Item()
	//obj, ok := b.p.GetObject(shortName)
	//if !ok {
	//	panic(fmt.Sprintf("xxxx object not found:%s", shortName.Segmentid().ToString()))
	//}
}

func (b *ObjectsIterForIndexByTS) Close() error {
	b.iter.Release()
	b.p = nil
	return nil
}

type objectsIterForTest struct {
	iter        btree.IterG[ObjectEntry]
	firstCalled bool
}

func (p *PartitionState) NewObjectsIterForTest() (*objectsIterForTest, error) {
	iter := p.dataObjects.Copy().Iter()
	ret := &objectsIterForTest{
		iter: iter,
	}
	return ret, nil
}

var _ ObjectsIter = new(objectsIterForTest)

func (b *objectsIterForTest) Next() bool {
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
		return true
	}
}

func (b *objectsIterForTest) Entry() ObjectEntry {
	return b.iter.Item()
}

func (b *objectsIterForTest) Close() error {
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

// GetChangedObjsBetween get changed objects between [begin, end],
// notice that if an object is created after begin and deleted before end, it will be ignored.
func (p *PartitionState) GetChangedObjsBetween(
	begin types.TS,
	end types.TS,
) (
	deleted []objectio.ObjectNameShort,
	inserted []objectio.ObjectNameShort,
) {
	insertedObj := make(map[objectio.ObjectNameShort]struct{})
	iter := p.objectIndexByTS.Copy().Iter()
	defer iter.Release()

	for ok := iter.Seek(ObjectIndexByTSEntry{
		Time: begin,
	}); ok; ok = iter.Next() {
		entry := iter.Item()

		if entry.Time.Greater(end) {
			break
		}

		if entry.IsDelete {
			// if the object is inserted and deleted between [begin, end], it will be ignored.
			if _, ok := insertedObj[entry.ShortObjName]; !ok {
				deleted = append(deleted, entry.ShortObjName)
			} else {
				delete(insertedObj, entry.ShortObjName)
			}
		} else {
			insertedObj[entry.ShortObjName] = struct{}{}
			//inserted = append(inserted, entry.ShortObjName)
		}

	}
	for k := range insertedObj {
		inserted = append(inserted, k)
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
	iter := p.dataObjects.Copy().Iter()
	defer iter.Release()

	pivot := ObjectEntry{}
	objectio.SetObjectStatsShortName(&pivot.ObjectStats, objectio.ShortName(&blockID))
	if ok := iter.Seek(pivot); ok {
		e := iter.Item()
		if bytes.Equal(e.ObjectShortName()[:], objectio.ShortName(&blockID)[:]) {
			return true
		}
	}
	return false
}

func (p *PartitionState) GetObject(name objectio.ObjectNameShort) (ObjectInfo, bool) {
	iter := p.dataObjects.Copy().Iter()
	defer iter.Release()

	pivot := ObjectEntry{}
	objectio.SetObjectStatsShortName(&pivot.ObjectStats, &name)
	if ok := iter.Seek(pivot); ok {
		e := iter.Item()
		if bytes.Equal(e.ObjectShortName()[:], name[:]) {
			return iter.Item().ObjectInfo, true
		}
	}
	return ObjectInfo{}, false
}
