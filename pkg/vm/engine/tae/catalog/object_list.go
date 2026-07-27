// Copyright 2021 Matrix Origin
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

package catalog

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/tidwall/btree"
	"go.uber.org/zap"
)

const (
	ObjectState_Create_Active uint8 = iota
	ObjectState_Create_PrepareCommit
	ObjectState_Create_ApplyCommit
	ObjectState_Delete_Active
	ObjectState_Delete_PrepareCommit
	ObjectState_Delete_ApplyCommit
)

/*
ObjectList keeps entries in six contiguous groups:

 1. appendable serving C entries
 2. appendable C entries with a D counterpart
 3. appendable D entries
 4. non-appendable serving C entries
 5. non-appendable C entries with a D counterpart
 6. non-appendable D entries

Within each group entries are ordered by their entry commit timestamp
(CreatedAt for C entries and DeletedAt for D entries), then by object name.
Uncommitted entries therefore sit at the end of their own group instead of in
one global transaction-active zone.

The C and D entries for a dropped object remain separate tree items. Callers
that need only the latest version must skip C entries having a D counterpart.
*/

type ObjectList struct {
	isTombstone bool
	sync.RWMutex
	objectID_index map[objectio.ObjectId]objectListIndex
	tree           atomic.Pointer[btree.BTreeG[*ObjectEntry]]
}

type objectListIndex struct {
	ts    types.TS
	group ObjectListGroup
}

func newObjectEntryTree() *btree.BTreeG[*ObjectEntry] {
	opts := btree.Options{
		Degree:  64,
		NoLocks: true,
	}
	return btree.NewBTreeGOptions((*ObjectEntry).Less, opts)
}

func NewObjectList(isTombstone bool) *ObjectList {
	list := &ObjectList{
		objectID_index: make(map[types.Objectid]objectListIndex),
		isTombstone:    isTombstone,
	}
	list.tree.Store(newObjectEntryTree())
	return list
}

//// read part

func getObjectEntry(it *btree.IterG[*ObjectEntry], pivot *ObjectEntry) *ObjectEntry {
	ok := it.Seek(pivot)
	if !ok {
		logutil.Errorf("object not found seek: %s", pivot.ID().ShortStringEx())
		return nil
	}
	obj := it.Item()
	if !obj.ID().EQ(pivot.ID()) {
		logutil.Errorf("object not found cmp: %s %s", obj.ID().ShortStringEx(), pivot.ID().ShortStringEx())
		return nil
	}
	return obj
}

func (l *ObjectList) getNodes(id *objectio.ObjectId, latestOnly bool) []*ObjectEntry {
	l.RLock()
	index, ok := l.objectID_index[*id]
	tree := l.tree.Load()
	l.RUnlock()
	if !ok {
		return nil
	}
	return l.getNodesSnap(tree, index, id, latestOnly)
}

// getNodes returns the create and delete (if exists) entries of the object with the given objectID
func (l *ObjectList) getNodesSnap(
	tree *btree.BTreeG[*ObjectEntry],
	index objectListIndex,
	id *objectio.ObjectId,
	latestOnly bool,
) []*ObjectEntry {
	it := tree.Iter()
	defer it.Release()

	key := makeObjectListKey(index.group, index.ts, id)

	obj := getObjectEntry(&it, key)
	if obj == nil {
		return nil
	}

	ret := []*ObjectEntry{obj}

	// the obj is a del Entry, try to find the create entry
	if !latestOnly && obj.prevVersion != nil {
		if !obj.prevVersion.ID().EQ(id) {
			panic("logic error")
		}
		ret = append(ret, obj.prevVersion)
	}
	return ret
}

func (l *ObjectList) GetLastestNode(id *objectio.ObjectId) *ObjectEntry {
	nodes := l.getNodes(id, true)
	if len(nodes) == 0 {
		return nil
	}
	return nodes[0]
}

func (l *ObjectList) GetAllNodes(id *objectio.ObjectId) []*ObjectEntry {
	return l.getNodes(id, false)
}

func (l *ObjectList) GetObjectByID(objectID *objectio.ObjectId) (obj *ObjectEntry, err error) {
	obj = l.GetLastestNode(objectID)
	if obj == nil {
		logutil.Debug("GetObjectByID not found", zap.String("obj", objectID.ShortStringEx()))
		err = moerr.GetOkExpectedEOB()
	}
	return
}

/// write part

func (l *ObjectList) UpdateReplayTs(entry *ObjectEntry, ts types.TS) *ObjectEntry {
	l.Lock()
	defer l.Unlock()
	oldIndex, ok := l.objectID_index[*entry.ID()]
	if !ok {
		panic("replay object index not found")
	}
	oldTree := l.tree.Load()
	newTree := oldTree.Copy()
	oldKey := makeObjectListKey(oldIndex.group, oldIndex.ts, entry.ID())
	if _, deleted := newTree.Delete(oldKey); !deleted {
		panic("replay object not found")
	}

	updated := entry
	if err := updated.EntryMVCCNode.ApplyCommit(ts); err != nil {
		panic(err)
	}
	if entry.IsDEntry() {
		if _, deleted := newTree.Delete(entry.prevVersion); !deleted {
			panic("replay object create entry not found")
		}
		newTree.Set(entry.prevVersion)
	}
	newTree.Set(updated)
	if updated.objData != nil {
		updated.objData.UpdateMeta(updated)
	}
	l.objectID_index[*entry.ID()] = objectListIndex{
		ts:    updated.ObjectListCommitTS(),
		group: updated.ObjectListGroup(),
	}
	if !l.tree.CompareAndSwap(oldTree, newTree) {
		panic("concurrent mutation")
	}
	return updated
}

// 1. del\ins\updated should all belong to the same object
// 2. del and ins should be two entry with different sort key, like different DeleteAt, so modify deletes the del entry (if not nil), inserts the ins entry and updates index map according to the ins entry
// 3. updated will be inserted into the tree, and the index map WON'T be updated. The Caller make sure the updated entry has the same sort key as the target entry.
// 4. all operations are atomic from the view of the caller of modify
func (l *ObjectList) modify(del, ins, updated *ObjectEntry) (deleted, replaced1, replaced2 bool) {
	l.Lock()
	defer l.Unlock()
	oldIndex, existed := l.objectID_index[*ins.ID()]
	l.objectID_index[*ins.ID()] = objectListIndex{
		ts:    ins.ObjectListCommitTS(),
		group: ins.ObjectListGroup(),
	}

	oldTree := l.tree.Load()
	newTree := oldTree.Copy()

	if del != nil {
		if del.IsTombstone != l.isTombstone {
			panic("logic error")
		}
		_, deleted = newTree.Delete(del)
	}
	// The first D entry moves its C counterpart from the create-only group to
	// the create-with-drop group. The old implementation shared one timestamp
	// ordering for both forms; the grouped ordering requires an explicit move.
	if existed &&
		(oldIndex.group == ObjectListGroupAppendableCreate ||
			oldIndex.group == ObjectListGroupNonAppendableCreate) &&
		ins.IsDEntry() && ins.prevVersion != nil {
		oldC := makeObjectListKey(oldIndex.group, oldIndex.ts, ins.ID())
		newTree.Delete(oldC)
		newTree.Set(ins.prevVersion)
	}
	// Rolling back a drop performs the inverse transition. Remove the
	// create-with-drop counterpart before restoring the serving C entry.
	if existed &&
		(oldIndex.group == ObjectListGroupAppendableDrop ||
			oldIndex.group == ObjectListGroupNonAppendableDrop) &&
		!ins.HasDropIntent() && del != nil && del.prevVersion != nil {
		newTree.Delete(del.prevVersion)
	}
	if updated != nil {
		_, replaced2 = newTree.Set(updated)
	}
	_, replaced1 = newTree.Set(ins)
	ok := l.tree.CompareAndSwap(oldTree, newTree)
	if !ok {
		panic("concurrent mutation")
	}
	return
}

// Set inserts a brand the objectstate, used in CreateObject
func (l *ObjectList) Set(object *ObjectEntry) {
	_, replaced, _ := l.modify(nil, object, nil)
	if replaced {
		logutil.Error("Object list Set replaced", zap.String("obj", object.ID().ShortStringEx()), zap.Uint64("tableID", object.table.ID))
	}
}

// DropObjectByID appends a delete node as a marker, used in SoftDeleteObject
func (l *ObjectList) DropObjectByID(
	objectID *objectio.ObjectId,
	txn txnif.TxnReader,
) (
	droppedObj *ObjectEntry,
	isNew bool,
	err error,
) {
	obj, err := l.GetObjectByID(objectID)
	if err != nil {
		return
	}
	if obj.HasDropIntent() {
		logutil.Error("DropObjectByID HasDropIntent", zap.String("obj", objectID.ShortStringEx()))
		return nil, false, moerr.GetOkExpectedEOB()
	}
	if !obj.DeleteNode.IsEmpty() {
		panic("logic error")
	}
	needWait, txnToWait := obj.CreateNode.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		txnToWait.GetTxnState(true)
	}
	if err := obj.CreateNode.CheckConflict(txn); err != nil {
		return nil, false, err
	}
	droppedObj, updatedCEntry, isNew := obj.GetDropEntry(txn)
	if !isNew && obj.IsCreating() {
		tableDesc := fmt.Sprintf("%v-%s", obj.table.ID, obj.table.GetLastestSchema(false).Name)
		logutil.Error("DropObjectByID IsCreating", zap.String("obj", objectID.ShortStringEx()), zap.String("table", tableDesc))
		return nil, false, moerr.NewNYINoCtx("DropObjectByID creating obj.")
	}
	// insert the D Entry and update the C Entry
	l.modify(nil, droppedObj, updatedCEntry)
	return
}

// UpdateObjectInfo must be called after DropObjectByID in a txn refer to flushTableTail
func (l *ObjectList) UpdateObjectInfo(
	obj *ObjectEntry,
	txn txnif.TxnReader,
	stats *objectio.ObjectStats,
) (isNew bool, err error) {
	needWait, txnToWait := obj.GetLastMVCCNode().NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		txnToWait.GetTxnState(true)
	}
	if err := obj.GetLastMVCCNode().CheckConflict(txn); err != nil {
		return false, err
	}
	newDroppedObj, udpateCEntry, isNew := obj.GetUpdateEntry(txn, stats)
	if isNew {
		tableDesc := fmt.Sprintf("%v-%s", obj.table.ID, obj.table.GetLastestSchema(false).Name)
		logutil.Error("UpdateObjectInfo Before Deleting", zap.String("obj", obj.ID().ShortStringEx()), zap.String("table", tableDesc))
		return false, moerr.NewNYINoCtx("UpdateObjectInfo before deleting.")
	}
	// replace the D entry and update the C entry
	l.modify(nil, newDroppedObj, udpateCEntry)
	return
}

// deleteEntryLocked deletes all entries with the given objectID, used in GC & Rollback
func (l *ObjectList) DeleteAllEntries(id *objectio.ObjectId) error {
	l.Lock()
	defer l.Unlock()
	index, ok := l.objectID_index[*id]
	if !ok {
		return nil
	}
	oldTree := l.tree.Load()
	newTree := oldTree.Copy()
	objs := l.getNodesSnap(newTree, index, id, false)
	for _, obj := range objs {
		newTree.Delete(obj)
		delete(l.objectID_index, *obj.ID())
	}
	ok = l.tree.CompareAndSwap(oldTree, newTree)
	if !ok {
		panic("concurrent mutation")
	}
	return nil
}

func (l *ObjectList) UpdateCreateTS(id *objectio.ObjectId, ts types.TS) (*ObjectEntry, error) {
	l.Lock()
	defer l.Unlock()
	oldIndex, ok := l.objectID_index[*id]
	if !ok {
		return nil, moerr.GetOkExpectedEOB()
	}
	oldTree := l.tree.Load()
	newTree := oldTree.Copy()
	nodes := l.getNodesSnap(newTree, oldIndex, id, true)
	if len(nodes) == 0 {
		return nil, moerr.GetOkExpectedEOB()
	}
	oldNode := nodes[0]
	newNode := oldNode.Clone()
	if oldNode.IsDEntry() {
		newPrev := oldNode.prevVersion.Clone()
		newPrev.CreatedAt = ts
		newPrev.CreateNode = txnbase.NewTxnMVCCNodeWithTS(ts)
		newPrev.nextVersion = newNode
		newNode.CreatedAt = ts
		newNode.CreateNode = txnbase.NewTxnMVCCNodeWithTS(ts)
		newNode.prevVersion = newPrev
		newTree.Delete(oldNode)
		newTree.Delete(oldNode.prevVersion)
		newTree.Set(newNode)
		newTree.Set(newPrev)
	} else {
		newNode.CreatedAt = ts
		newNode.CreateNode = txnbase.NewTxnMVCCNodeWithTS(ts)
		newTree.Delete(oldNode)
		newTree.Set(newNode)
	}
	l.objectID_index[*id] = objectListIndex{
		ts:    newNode.ObjectListCommitTS(),
		group: newNode.ObjectListGroup(),
	}
	if !l.tree.CompareAndSwap(oldTree, newTree) {
		panic("concurrent mutation")
	}
	return newNode, nil
}

// WaitUntilCommitted checks the uncommitted tail of every group. When it
// returns, all creating objects that can be visible to ts have committed.
func (l *ObjectList) WaitUntilCommitted(ts types.TS) {
	it := l.tree.Load().Iter()
	defer it.Release()
	for group := ObjectListGroupAppendableCreate; group <= ObjectListGroupNonAppendableDrop; group++ {
		for ok := SeekObjectListGroup(&it, group, txnif.UncommitTS); ok; ok = it.Next() {
			obj := it.Item()
			if obj.ObjectListGroup() != group {
				break
			}
			if obj.IsCommitted() {
				continue
			}
			if needWait, txn := obj.CreateNode.NeedWaitCommitting(ts); needWait {
				txn.GetTxnState(true)
			}
			if needWait, txn := obj.DeleteNode.NeedWaitCommitting(ts); needWait {
				txn.GetTxnState(true)
			}
		}
	}
}

// Iterator part

var _iterPool = sync.Pool{New: func() any {
	return &VisibleCommittedObjectIt{}
}}

type VisibleCommittedObjectIt struct {
	iters       [4]btree.IterG[*ObjectEntry]
	items       [4]*ObjectEntry
	curr        *ObjectEntry
	txn         txnif.TxnReader
	isMockTxn   bool
	initialized bool
}

var visibleObjectGroups = [...]ObjectListGroup{
	ObjectListGroupAppendableCreate,
	ObjectListGroupAppendableCreateWithDrop,
	ObjectListGroupNonAppendableCreate,
	ObjectListGroupNonAppendableCreateWithDrop,
}

// MakeVisibleCommittedObjectIt returns an iterator that iterates over committed objects visible to the given txn
// two cases:
// 2. normal txn, wait if needed, return committed non-dropped objects
// 1. txn is mock txn, no waiting, only return committed non-dropped objects, used for status check

func (l *ObjectList) MakeVisibleCommittedObjectIt(txn txnif.TxnReader) *VisibleCommittedObjectIt {
	it := _iterPool.Get().(*VisibleCommittedObjectIt)
	tree := l.tree.Load()
	for i := range it.iters {
		it.iters[i] = tree.Iter()
	}
	it.txn = txn
	it.isMockTxn = len(txn.GetCtx()) == 0
	return it
}

func (it *VisibleCommittedObjectIt) advance(groupIndex int, first bool) {
	group := visibleObjectGroups[groupIndex]
	iter := &it.iters[groupIndex]
	var ok bool
	if first {
		ok = SeekObjectListGroupReverse(iter, group, txnif.UncommitTS)
	} else {
		ok = iter.Prev()
	}
	for {
		if !ok {
			it.items[groupIndex] = nil
			return
		}
		entry := iter.Item()
		if entry.ObjectListGroup() != group {
			it.items[groupIndex] = nil
			return
		}
		if it.isMockTxn {
			if !entry.IsCreating() && !entry.HasDCounterpart() {
				it.items[groupIndex] = entry
				return
			}
		} else if entry.IsVisible(it.txn) {
			if !entry.HasDCounterpart() || !entry.GetNextVersion().IsVisible(it.txn) {
				it.items[groupIndex] = entry
				return
			}
		}
		ok = iter.Prev()
	}
}

func (it *VisibleCommittedObjectIt) Next() bool {
	if !it.initialized {
		for i := range it.iters {
			it.advance(i, true)
		}
		it.initialized = true
	}
	selected := -1
	for i, item := range it.items {
		if item == nil {
			continue
		}
		if selected == -1 ||
			it.items[selected].CreatedAt.LT(&item.CreatedAt) ||
			(it.items[selected].CreatedAt.EQ(&item.CreatedAt) &&
				bytes.Compare(it.items[selected].ObjectShortName()[:], item.ObjectShortName()[:]) < 0) {
			selected = i
		}
	}
	if selected == -1 {
		return false
	}
	it.curr = it.items[selected]
	it.advance(selected, false)
	return true
}

func (it *VisibleCommittedObjectIt) Item() *ObjectEntry {
	return it.curr
}

func (it *VisibleCommittedObjectIt) Release() {
	if it.txn == nil {
		logutil.Errorf("attempt to put iter %p into pool twice", it)
		return
	}
	for i := range it.iters {
		it.iters[i].Release()
		it.items[i] = nil
	}
	it.curr = nil
	it.txn = nil
	it.initialized = false
	it.isMockTxn = false
	_iterPool.Put(it)
}

// utils

// Show returns a string representation of the objectlist
func (l *ObjectList) Show() string {
	l.RLock()
	defer l.RUnlock()
	tree := l.tree.Load()
	it := tree.Iter()
	defer it.Release()
	ret := ""
	for it.Next() {
		ret += " " + it.Item().StringWithLevel(common.PPL2) + "\n"
	}
	ret += "objectID_index:\n"
	for id, index := range l.objectID_index {
		ret += fmt.Sprintf(" %s: %s-%d\n", id.ShortStringEx(), index.ts.ToString(), index.group)
	}
	return ret
}

func makeObjectListKey(group ObjectListGroup, ts types.TS, id *objectio.ObjectId) *ObjectEntry {
	appendable := group < ObjectListGroupNonAppendableCreate
	key := &ObjectEntry{
		EntryMVCCNode: EntryMVCCNode{CreatedAt: ts},
		ObjectMVCCNode: ObjectMVCCNode{
			ObjectStats: *objectio.NewObjectStatsWithObjectID(id, appendable, false, false),
		},
	}
	switch group {
	case ObjectListGroupAppendableCreateWithDrop, ObjectListGroupNonAppendableCreateWithDrop:
		key.nextVersion = &ObjectEntry{}
	case ObjectListGroupAppendableDrop, ObjectListGroupNonAppendableDrop:
		key.CreatedAt = types.TS{}
		key.DeletedAt = ts
		key.prevVersion = &ObjectEntry{}
	}
	return key
}

func SeekObjectListGroup(
	it *btree.IterG[*ObjectEntry],
	group ObjectListGroup,
	ts types.TS,
) bool {
	var minID objectio.ObjectId
	if !it.Seek(makeObjectListKey(group, ts, &minID)) {
		return false
	}
	return it.Item().ObjectListGroup() == group
}

func SeekObjectListGroupBefore(
	it *btree.IterG[*ObjectEntry],
	group ObjectListGroup,
	ts types.TS,
) bool {
	var minID objectio.ObjectId
	if it.Seek(makeObjectListKey(group, ts, &minID)) {
		if !it.Prev() {
			return false
		}
	} else if !it.Last() {
		return false
	}
	return it.Item().ObjectListGroup() == group
}

func SeekObjectListGroupReverse(
	it *btree.IterG[*ObjectEntry],
	group ObjectListGroup,
	ts types.TS,
) bool {
	var maxID objectio.ObjectId
	for i := range maxID {
		maxID[i] = 0xff
	}
	if it.Seek(makeObjectListKey(group, ts, &maxID)) {
		item := it.Item()
		if item.ObjectListGroup() == group {
			commitTS := item.ObjectListCommitTS()
			if commitTS.LE(&ts) {
				return true
			}
		}
		if !it.Prev() {
			return false
		}
	} else if !it.Last() {
		return false
	}
	return it.Item().ObjectListGroup() == group
}
