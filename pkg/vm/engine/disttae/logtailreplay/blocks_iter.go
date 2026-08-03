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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/tidwall/btree"
)

const lifecycleObjectEntryMetaBytes = uint64(objectio.ObjectStatsLen + 24)

// VisibleDataObjectPage is a bounded view over the current data Object index.
// LastObjectName is an opaque resume hint. Callers must never use a page as a
// retirement proof; the final TAE transaction has to validate exact identity.
type VisibleDataObjectPage struct {
	Objects        []objectio.ObjectEntry
	LastObjectName *objectio.ObjectNameShort
	End            bool
	MetaBytes      uint64
}

type LifecycleTombstoneSelectionLimits struct {
	MaxScannedObjects  int
	MaxSelectedObjects int
	MaxMetaBytes       uint64
}

type objectsIter struct {
	onlyVisible bool
	ts          types.TS
	iter        btree.IterG[objectio.ObjectEntry]
}

var _ objectio.ObjectIter = new(objectsIter)

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

func (b *objectsIter) Entry() objectio.ObjectEntry {
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

func (p *PartitionState) ApproxInMemTombstones() int {
	return p.inMemTombstoneRowIdIndex.Len()
}

func (p *PartitionState) ApproxInMemRows() int {
	return p.rows.Len()
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
	onlyVisible bool) (objectio.ObjectIter, error) {

	iter := p.tombstoneObjectDTSIndex.Iter()
	if onlyVisible {
		pivot := objectio.ObjectEntry{
			DeleteTime: snapshot,
		}

		iter.Seek(pivot)
		if !iter.Prev() && p.tombstoneObjectDTSIndex.Len() > 0 {
			// reset iter only when seeked to the first item
			iter.Release()
			iter = p.tombstoneObjectDTSIndex.Iter()
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
	onlyVisible bool) (objectio.ObjectIter, error) {

	iter := p.dataObjectsNameIndex.Iter()
	ret := &objectsIter{
		onlyVisible: onlyVisible,
		ts:          snapshot,
		iter:        iter,
	}
	return ret, nil
}

func (p *PartitionState) IsNil() bool {
	return p == nil
}

func (p *PartitionState) NewObjectsIter(
	snapshot types.TS,
	onlyVisible bool,
	visitTombstone bool,
) (objectio.ObjectIter, error) {
	if !p.IsEmpty() && snapshot.LT(&p.start) {
		return nil, moerr.NewTxnStaleNoCtxf(
			"(%s<%s)",
			snapshot.ToString(), p.start.ToString(),
		)
	}

	if visitTombstone {
		return p.newTombstoneObjectsIter(snapshot, onlyVisible)
	} else {
		return p.newDataObjectIter(snapshot, onlyVisible)
	}
}

// ScanVisibleDataObjectsPage seeks directly in the existing Object-name B-tree
// and returns at most maxObjects/maxMetaBytes of visible, non-appendable data
// Objects. It deliberately does not build a table-wide intermediate slice.
func (p *PartitionState) ScanVisibleDataObjectsPage(
	ctx context.Context,
	snapshot types.TS,
	after *objectio.ObjectNameShort,
	maxObjects int,
	maxMetaBytes uint64,
) (VisibleDataObjectPage, error) {
	if maxObjects <= 0 {
		return VisibleDataObjectPage{}, moerr.NewInvalidInput(ctx, "Lifecycle maxObjects must be positive")
	}
	if maxMetaBytes < lifecycleObjectEntryMetaBytes {
		return VisibleDataObjectPage{}, moerr.NewInvalidInput(
			ctx,
			"Lifecycle maxMetaBytes cannot hold one Object entry",
		)
	}
	if err := ctx.Err(); err != nil {
		return VisibleDataObjectPage{}, err
	}
	if !p.IsEmpty() && snapshot.LT(&p.start) {
		return VisibleDataObjectPage{}, moerr.NewTxnStaleNoCtxf(
			"(%s<%s)",
			snapshot.ToString(), p.start.ToString(),
		)
	}

	page := VisibleDataObjectPage{
		Objects: make([]objectio.ObjectEntry, 0, maxObjects),
	}
	iter := p.dataObjectsNameIndex.Iter()
	defer iter.Release()

	var ok bool
	if after == nil {
		ok = iter.First()
	} else {
		pivotStats := objectio.NewObjectStats()
		if err := objectio.SetObjectStatsShortName(pivotStats, after); err != nil {
			return VisibleDataObjectPage{}, err
		}
		ok = iter.Seek(objectio.ObjectEntry{ObjectStats: *pivotStats})
		if ok {
			item := iter.Item()
			if bytes.Equal(item.ObjectShortName()[:], after[:]) {
				ok = iter.Next()
			}
		}
	}

	for ; ok; ok = iter.Next() {
		if err := ctx.Err(); err != nil {
			return VisibleDataObjectPage{}, err
		}
		entry := iter.Item()
		if !entry.Visible(snapshot) || entry.GetAppendable() {
			continue
		}
		if len(page.Objects) == maxObjects ||
			page.MetaBytes+lifecycleObjectEntryMetaBytes > maxMetaBytes {
			page.End = false
			return page, nil
		}
		page.Objects = append(page.Objects, entry)
		page.MetaBytes += lifecycleObjectEntryMetaBytes
		last := *entry.ObjectShortName()
		page.LastObjectName = &last
	}
	page.End = true
	return page, nil
}

// SelectLifecycleTombstoneObjects returns a conservative physical superset for
// SyncProtection. The existing snapshot reader remains the authority for
// visibility and may consume any Tombstone Object in this protected superset.
// A Tombstone Object is excluded only when a valid RowID ZoneMap proves that it
// cannot reference any source Data Object. Missing, legacy, malformed, or
// unexpected ZoneMaps are conservatively selected instead of being treated as
// unrelated.
func (p *PartitionState) SelectLifecycleTombstoneObjects(
	ctx context.Context,
	snapshot types.TS,
	sourceObjectIDs []objectio.ObjectId,
	limits LifecycleTombstoneSelectionLimits,
) ([]objectio.ObjectEntry, int, error) {
	if len(sourceObjectIDs) == 0 {
		return nil, 0, moerr.NewInvalidInput(ctx, "Lifecycle tombstone selection requires Data Objects")
	}
	if limits.MaxScannedObjects <= 0 ||
		limits.MaxSelectedObjects <= 0 ||
		limits.MaxMetaBytes < lifecycleObjectEntryMetaBytes {
		return nil, 0, moerr.NewInvalidInput(ctx, "Lifecycle tombstone selection limits are invalid")
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if !p.IsEmpty() && snapshot.LT(&p.start) {
		return nil, 0, moerr.NewTxnStaleNoCtxf(
			"(%s<%s)",
			snapshot.ToString(), p.start.ToString(),
		)
	}

	selected := make([]objectio.ObjectEntry, 0)
	scanned := 0
	metaBytes := uint64(0)
	iter, err := p.NewObjectsIter(snapshot, true, true)
	if err != nil {
		return nil, 0, err
	}
	defer iter.Close()
	for iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, scanned, err
		}
		scanned++
		if scanned > limits.MaxScannedObjects {
			return nil, scanned, moerr.NewInvalidInput(
				ctx,
				"Lifecycle tombstone scan exceeds the certified Object limit",
			)
		}
		entry := iter.Entry()
		zoneMap := entry.SortKeyZoneMap()
		include := !zoneMap.Valid() ||
			zoneMap.GetType() != types.T_Rowid ||
			len(zoneMap.GetMinBuf()) != types.RowidSize ||
			len(zoneMap.GetMaxBuf()) != types.RowidSize
		if !include {
			for index := range sourceObjectIDs {
				if zoneMap.RowidPrefixEq(sourceObjectIDs[index][:]) {
					include = true
					break
				}
			}
		}
		if !include {
			continue
		}
		if len(selected) == limits.MaxSelectedObjects ||
			metaBytes+lifecycleObjectEntryMetaBytes > limits.MaxMetaBytes {
			return nil, scanned, moerr.NewInvalidInput(
				ctx,
				"Lifecycle tombstone protection set exceeds its certified limit",
			)
		}
		selected = append(selected, entry)
		metaBytes += lifecycleObjectEntryMetaBytes
	}
	return selected, scanned, nil
}

func (p *PartitionState) NewDirtyBlocksIter() BlocksIter {
	//iter := p.dirtyBlocks.Copy().Iter()

	return nil
}

// In concurrent delete scenario, the following case may happen:
//
// / txn1   cn: write s3 tombstone      dn: commit s3 tombstone
// /	    |                              |
// /	----+---------------------+--------+-----------+---------->
// /	                          |                    |
// / txn2           cn: delete mem row(blocked)        cn: query PrimaryKeysMayBeModified and it returns false, which is wrong
//
// what PrimaryKeysMayBeModified does:
//  1. no mem rows in partition state
//  2. lastFlushTimestamp > from
//  3. it boils down to PKPersistedBetween, where dataobjects are empty and tombstones are ignored
func (p *PartitionState) HasTombstoneChanged(from, to types.TS) (exist bool) {
	if p.tombstoneObjectDTSIndex.Len() == 0 {
		return false
	}
	iter := p.tombstoneObjectDTSIndex.Iter()
	defer iter.Release()

	// Created after from
	if iter.Seek(objectio.ObjectEntry{CreateTime: from}) {
		return true
	}

	iter.First()
	// Deleted after from
	ok := iter.Seek(objectio.ObjectEntry{DeleteTime: from})
	if ok {
		item := iter.Item()
		return !item.DeleteTime.IsEmpty()
	}
	return false
}

// GetChangedTombstoneObjsBetween returns tombstone objects whose CreateTime >= from or DeleteTime >= from.
func (p *PartitionState) GetChangedTombstoneObjsBetween(from types.TS) (objs []objectio.ObjectEntry) {
	if p.tombstoneObjectDTSIndex.Len() == 0 {
		return
	}
	iter := p.tombstoneObjectDTSIndex.Iter()

	// tombstoneObjectDTSIndex is sorted by DeleteTime asc (empty=MaxTs last).
	// Seek to DeleteTime=from, step back one.
	// Live objects (DeleteTime=empty=MaxTs) are always at the end and will be visited.
	pivot := objectio.ObjectEntry{DeleteTime: from}
	iter.Seek(pivot)
	if !iter.Prev() && p.tombstoneObjectDTSIndex.Len() > 0 {
		// Seeked to the first item; reset iter so Next() starts from the beginning.
		iter.Release()
		iter = p.tombstoneObjectDTSIndex.Iter()
	}

	for ok := iter.Next(); ok; ok = iter.Next() {
		entry := iter.Item()
		if entry.CreateTime.GE(&from) {
			objs = append(objs, entry)
		} else if !entry.DeleteTime.IsEmpty() && entry.DeleteTime.GE(&from) {
			objs = append(objs, entry)
		}
	}
	iter.Release()
	return
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

	iter := p.dataObjectTSIndex.Iter()
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
	iter := p.dataObjectsNameIndex.Iter()
	defer iter.Release()

	pivot := objectio.ObjectEntry{}
	objectio.SetObjectStatsShortName(&pivot.ObjectStats, objectio.ShortName(blockID))
	if ok := iter.Seek(pivot); ok {
		e := iter.Item()
		if bytes.Equal(e.ObjectShortName()[:], objectio.ShortName(blockID)[:]) {
			return true
		}
	}
	return false
}

func (p *PartitionState) CollectObjectsBetween(
	start, end types.TS,
) (insertList, deletedList []objectio.ObjectStats) {

	iter := p.dataObjectTSIndex.Iter()
	defer iter.Release()

	if !iter.Seek(ObjectIndexByTSEntry{
		Time: start,
	}) {
		return
	}

	nameIdx := p.dataObjectsNameIndex

	for ok := true; ok; ok = iter.Next() {
		entry := iter.Item()

		if entry.Time.GT(&end) {
			break
		}

		var ss objectio.ObjectStats
		objectio.SetObjectStatsShortName(&ss, &entry.ShortObjName)

		val, exist := nameIdx.Get(objectio.ObjectEntry{
			ObjectStats: ss,
		})

		if !exist {
			continue
		}

		// case1: no soft delete
		if val.DeleteTime.IsEmpty() {
			insertList = append(insertList, val.ObjectStats)
		} else {
			if val.CreateTime.LT(&start) {
				// create --------- delete
				//          start -------- end
				if val.DeleteTime.LE(&end) {
					deletedList = append(deletedList, val.ObjectStats)
				}
			} else {
				//        create ---------- delete
				// start ------------ end
				if val.DeleteTime.GT(&end) {
					insertList = append(insertList, val.ObjectStats)
				}
			}
		}
	}

	return
}

func (p *PartitionState) CheckIfObjectDeletedBeforeTS(
	ts types.TS,
	isTombstone bool,
	objId *objectio.ObjectId,
) bool {

	var tree *btree.BTreeG[objectio.ObjectEntry]
	if isTombstone {
		tree = p.tombstoneObjectsNameIndex
	} else {
		tree = p.dataObjectsNameIndex
	}

	var stats objectio.ObjectStats
	objectio.SetObjectStatsShortName(&stats, (*objectio.ObjectNameShort)(objId))
	val, exist := tree.Get(objectio.ObjectEntry{
		ObjectStats: stats,
	})

	if !exist {
		return true
	}

	return !val.DeleteTime.IsEmpty() && val.DeleteTime.LE(&ts)
}

func (p *PartitionState) GetObject(name objectio.ObjectNameShort) (objectio.ObjectEntry, bool) {
	iter := p.dataObjectsNameIndex.Iter()
	defer iter.Release()

	pivot := objectio.ObjectEntry{}
	objectio.SetObjectStatsShortName(&pivot.ObjectStats, &name)
	if ok := iter.Seek(pivot); ok {
		e := iter.Item()
		if bytes.Equal(e.ObjectShortName()[:], name[:]) {
			return iter.Item(), true
		}
	}
	return objectio.ObjectEntry{}, false
}

func (p *PartitionState) CollectTombstoneObjects(
	snapshot types.TS,
	appendTo func(stats *objectio.ObjectStats),
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
		appendTo(&item.ObjectStats)
	}

	return nil
}
