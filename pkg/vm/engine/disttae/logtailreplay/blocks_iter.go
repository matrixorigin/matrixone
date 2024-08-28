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
	return ObjectEntry{
		ObjectInfo: b.iter.Item().ObjectInfo,
	}
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
	return p.dataObjects.Len()
}
func (p *PartitionState) ApproxTombstoneObjectsNum() int {
	return p.tombstoneObjets.Len()
}

func (p *PartitionState) NewObjectsIter(
	ts types.TS,
	onlyVisible bool,
	visitTombstone bool) (ObjectsIter, error) {

	if ts.Less(&p.minTS) {
		msg := fmt.Sprintf("(%s<%s)", ts.ToString(), p.minTS.ToString())
		return nil, moerr.NewTxnStaleNoCtx(msg)
	}

	var iter btree.IterG[ObjectEntry]
	if visitTombstone {
		iter = p.tombstoneObjets.Copy().Iter()
	} else {
		iter = p.dataObjects.Copy().Iter()
	}

	ret := &objectsIter{
		onlyVisible: onlyVisible,
		ts:          ts,
		iter:        iter,
	}
	return ret, nil
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

func (p *PartitionState) GetBlockDeltaLoc(bid types.Blockid) (objectio.ObjectLocation, types.TS, bool) {
	iter := p.tombstoneObjets.Copy().Iter()
	defer iter.Release()

	pivot := ObjectEntry{}
	objectio.SetObjectStatsShortName(&pivot.ObjectStats, objectio.ShortName(&bid))
	if ok := iter.Seek(pivot); ok {
		e := iter.Item()
		if bytes.Equal(e.ObjectShortName()[:], objectio.ShortName(&bid)[:]) {
			return objectio.ObjectLocation(e.ObjectLocation()), e.CommitTS, true
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
