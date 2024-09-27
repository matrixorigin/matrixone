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
	"fmt"

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
	onlyVisible bool
	ts          types.TS
	iter        btree.IterG[ObjectEntry]
}

var _ ObjectsIter = new(objectsIter)

func (b *objectsIter) Next() bool {
	for b.iter.Next() {
		entry := b.iter.Item()
		if b.onlyVisible && !entry.Visible(b.ts) {
			// not visible
			continue
		}
		return true
	}
	return false
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

// ApproxDataObjectsNum not accurate!  only used by stats
func (p *PartitionState) ApproxDataObjectsNum() int {
	return p.dataObjectsNameIndex.Len()
}

func (p *PartitionState) ApproxTombstoneObjectsNum() int {
	return p.tombstoneObjectsNameIndex.Len()
}

func (p *PartitionState) newTombstoneObjectsIter(
	snapshot types.TS,
	onlyVisible bool) (ObjectsIter, error) {

	iter := p.tombstoneObjectDTSIndex.Copy().Iter()
	if onlyVisible {
		pivot := ObjectEntry{
			ObjectInfo{
				DeleteTime: snapshot,
			},
		}

		iter.Seek(pivot)
		if !iter.Prev() && p.tombstoneObjectDTSIndex.Len() > 0 {
			// reset iter only when seeked to the first item
			iter.Release()
			iter = p.tombstoneObjectDTSIndex.Copy().Iter()
		}
	}

	ret := &objectsIter{
		onlyVisible: onlyVisible,
		ts:          snapshot,
		iter:        iter,
	}
	return ret, nil
}

func (p *PartitionState) newDataObjectIter(
	snapshot types.TS,
	onlyVisible bool) (ObjectsIter, error) {

	iter := p.dataObjectsNameIndex.Copy().Iter()
	ret := &objectsIter{
		onlyVisible: onlyVisible,
		ts:          snapshot,
		iter:        iter,
	}
	return ret, nil
}

func (p *PartitionState) NewObjectsIter(
	snapshot types.TS,
	onlyVisible bool,
	visitTombstone bool) (ObjectsIter, error) {

	if snapshot.LT(&p.minTS) {
		msg := fmt.Sprintf("(%s<%s)", snapshot.ToString(), p.minTS.ToString())
		return nil, moerr.NewTxnStaleNoCtx(msg)
	}

	if visitTombstone {
		return p.newTombstoneObjectsIter(snapshot, onlyVisible)
	} else {
		return p.newDataObjectIter(snapshot, onlyVisible)
	}
}

func (p *PartitionState) NewDirtyBlocksIter() BlocksIter {
	//iter := p.dirtyBlocks.Copy().Iter()

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

	iter := p.dataObjectTSIndex.Copy().Iter()
	defer iter.Release()

	for ok := iter.Seek(ObjectIndexByTSEntry{
		Time: begin,
	}); ok; ok = iter.Next() {
		entry := iter.Item()

		if entry.Time.GT(&end) {
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

func (p *PartitionState) BlockPersisted(blockID *types.Blockid) bool {
	iter := p.dataObjectsNameIndex.Copy().Iter()
	defer iter.Release()

	pivot := ObjectEntry{}
	objectio.SetObjectStatsShortName(&pivot.ObjectStats, objectio.ShortName(blockID))
	if ok := iter.Seek(pivot); ok {
		e := iter.Item()
		if bytes.Equal(e.ObjectShortName()[:], objectio.ShortName(blockID)[:]) {
			return true
		}
	}
	return false
}

func (p *PartitionState) CollectVisibleObjectsBetween(
	start, end types.TS,
	isTombstone bool,
) (stats []objectio.ObjectStats) {

	iter := p.dataObjectTSIndex.Copy().Iter()
	defer iter.Release()

	if !iter.Seek(ObjectIndexByTSEntry{
		Time: start,
	}) {
		return
	}

	nameIdx := p.dataObjectsNameIndex.Copy()

	for ok := true; ok; ok = iter.Next() {
		entry := iter.Item()

		if entry.IsDelete {
			continue
		}

		var ss objectio.ObjectStats
		objectio.SetObjectStatsShortName(&ss, &entry.ShortObjName)

		val, exist := nameIdx.Get(ObjectEntry{
			ObjectInfo{
				ObjectStats: ss,
			},
		})

		if !exist {
			continue
		}

		// if deleted before end
		if !val.DeleteTime.IsEmpty() && val.DeleteTime.LE(&end) {
			continue
		}

		// if created in [start, end]
		if val.CreateTime.LT(&start) && val.CreateTime.GT(&end) {
			continue
		}

		stats = append(stats, val.ObjectStats)
	}

	return
}

func (p *PartitionState) CheckIfObjectDeletedBeforeTS(
	ts types.TS,
	isTombstone bool,
	objId *objectio.ObjectId,
) bool {

	var tree *btree.BTreeG[ObjectEntry]
	if isTombstone {
		tree = p.tombstoneObjectsNameIndex.Copy()
	} else {
		tree = p.dataObjectsNameIndex.Copy()
	}

	var stats objectio.ObjectStats
	objectio.SetObjectStatsShortName(&stats, (*objectio.ObjectNameShort)(objId))
	val, exist := tree.Get(ObjectEntry{
		ObjectInfo{
			ObjectStats: stats,
		},
	})

	if !exist {
		return true
	}

	return !val.DeleteTime.IsEmpty() && val.DeleteTime.LE(&ts)
}

func (p *PartitionState) GetObject(name objectio.ObjectNameShort) (ObjectInfo, bool) {
	iter := p.dataObjectsNameIndex.Copy().Iter()
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

func (p *PartitionState) CollectTombstoneObjects(
	snapshot types.TS,
	statsSlice *objectio.ObjectStatsSlice,
) (err error) {

	if p.ApproxTombstoneObjectsNum() == 0 {
		return
	}

	iter, err := p.NewObjectsIter(snapshot, true, true)
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.Next() {
		item := iter.Entry()
		(*statsSlice).Append(item.ObjectStats[:])
	}

	return nil
}
