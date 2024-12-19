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
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
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

Denote entries with prevVersion != nil as D entries, which means DeletedAt is not empty, and other entries as C entries.
   a. prevVersion == nil && nextVersion == nil: serving C entries (4, 10, 11, 15)
   b. prevVersion == nil && nextVersion != nil: C entries having dropping intent (1, 2, 3, 8, 9)
   c. prevVersion != nil && nextVersion == nil: D entries, the couterpart set of category b. (4, 5, 6, 13, 14)
   d. prevVersion != nil && nextVersion != nil: impossible

ac - appendable C entry
ad - appendable D entry
nc - non-appendable C entry
nd - non-appendable D entry

              +------------+
           +--+---------+  |
         +--+--+-----+  |  |
         |  |  |     |  |  |
         1  2  3  4  5  6  7  8  9 10 11    12 13 14 15
        ac ac ac ac ad ad ad nc nc ac ac  [ ac nd nd nc:TxnActiveZone]
                              |  |              |  |
                              +--+--------------+  |
                                 +-----------------+

ObjectList properties:
0. Entries are sorted by max(CreatedAt, DeletedAt). For txn active entries, sorted by objectName further.
1. Elements are splitted into two groups by `IsCommitted`: [committed entries...] [txn active entries]
2. Category-a + Category-c produce the same result as the ObjectList under the previous implementation(modified inplace & sorted by called time of CreateObject), that's what we want in RecurLoop.


Other examples of object states:
Txn Active Object ( LastestNode.Txn != nil ) (12, 13, 14, 15)
- Creating Object ( CreatedAt = MaxU64, DeletedAt = 0 ) (12, 15)
    - Active: CreateNode.Prepare = MaxU64
    - InQueue: CreateNode.Prepare != MaxU64 (parepare ts is allocated in the queue)
- Deleting Object ( 0 < CreatedAt < MaxU64, DeletedAt = MaxU64 ) (13, 14)
    - Active: DeleteNode.Prepare = MaxU64
    - InQueue: DeleteNode.Prepare != MaxU64

Committed Object ( LastestNode.Txn == nil ) && ( 0 < CreatedAt < MaxU64, 0 <= DeletedAt < MaxU64 ) (1 - 11)

*/

type ObjectList struct {
	isTombstone bool
	*sync.RWMutex
	maxTs_objectID map[objectio.ObjectId]types.TS
	tree           atomic.Pointer[btree.BTreeG[*ObjectEntry]]
}

func NewObjectList(isTombstone bool) *ObjectList {
	opts := btree.Options{
		Degree:  64,
		NoLocks: true,
	}
	tree := btree.NewBTreeGOptions((*ObjectEntry).Less, opts)
	list := &ObjectList{
		RWMutex:        &sync.RWMutex{},
		maxTs_objectID: make(map[types.Objectid]types.TS),
		isTombstone:    isTombstone,
	}
	list.tree.Store(tree)
	return list
}

//// read part

func getObjectEntry(it btree.IterG[*ObjectEntry], pivot *ObjectEntry) *ObjectEntry {
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
	ts, ok := l.maxTs_objectID[*id]
	tree := l.tree.Load()
	l.RUnlock()
	if !ok {
		return nil
	}
	return l.getNodesSnap(tree, ts, id, latestOnly)
}

// getNodes returns the create and delete (if exists) entries of the object with the given objectID
func (l *ObjectList) getNodesSnap(
	tree *btree.BTreeG[*ObjectEntry],
	ts types.TS,
	id *objectio.ObjectId,
	latestOnly bool,
) []*ObjectEntry {
	it := tree.Iter()
	defer it.Release()

	key := &ObjectEntry{
		EntryMVCCNode:  EntryMVCCNode{DeletedAt: ts},
		ObjectMVCCNode: ObjectMVCCNode{*objectio.NewObjectStatsWithObjectID(id, true, false, false)},
	}

	obj := getObjectEntry(it, key)
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
		logutil.Error("GetObjectByID", zap.String("obj", objectID.ShortStringEx()))
		err = moerr.GetOkExpectedEOB()
	}
	return
}

/// write part

func (l *ObjectList) UpdateReplayTs(id *objectio.ObjectId, ts types.TS) {
	l.Lock()
	defer l.Unlock()
	l.maxTs_objectID[*id] = ts
}

// 1. del\ins\updated should all belong to the same object
// 2. del and ins should be two entry with different sort key, like different DeleteAt, so modify deletes the del entry (if not nil), inserts the ins entry and updates index map according to the ins entry
// 3. updated will be inserted into the tree, and the index map WON'T be updated. The Caller make sure the updated entry has the same sort key as the target entry.
// 4. all operations are atomic from the view of the caller of modify
func (l *ObjectList) modify(del, ins, updated *ObjectEntry) (deleted, replaced1, replaced2 bool) {
	maxTs := ins.CreatedAt
	if maxTs.LT(&ins.DeletedAt) {
		maxTs = ins.DeletedAt
	}
	l.Lock()
	defer l.Unlock()
	l.maxTs_objectID[*ins.ID()] = maxTs

	oldTree := l.tree.Load()
	newTree := oldTree.Copy()

	if del != nil {
		if del.IsTombstone != l.isTombstone {
			panic("logic error")
		}
		_, deleted = newTree.Delete(del)
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
	ts, ok := l.maxTs_objectID[*id]
	if !ok {
		return nil
	}
	oldTree := l.tree.Load()
	newTree := oldTree.Copy()
	objs := l.getNodesSnap(newTree, ts, id, false)
	for _, obj := range objs {
		newTree.Delete(obj)
		delete(l.maxTs_objectID, *obj.ID())
	}
	ok = l.tree.CompareAndSwap(oldTree, newTree)
	if !ok {
		panic("concurrent mutation")
	}
	return nil
}

// WaitUntilCommitted waits for entries in txn active zone with prepareTS > ts to move to committed zone.
// As CreateObject will be called in txn queue, when WaitUntilCommitted returns, all creating objects in txn active zone are invisible to ts, because they are created after ts.
func (l *ObjectList) WaitUntilCommitted(ts types.TS) {
	it := l.tree.Load().Iter()
	for ok := it.Last(); ok; ok = it.Prev() {
		obj := it.Item()
		if obj.IsCommitted() {
			break
		}
		if needWait, txn := obj.CreateNode.NeedWaitCommitting(ts); needWait {
			txn.GetTxnState(true)
		}
		if needWait, txn := obj.DeleteNode.NeedWaitCommitting(ts); needWait {
			txn.GetTxnState(true)
		}
	}
}

// Iterator part

type VisibleCommittedObjectIt struct {
	iter        btree.IterG[*ObjectEntry]
	curr        *ObjectEntry
	txn         txnif.TxnReader
	isMockTxn   bool
	dropped     map[objectio.ObjectId]struct{} // TODO(aptend): sync.Pool
	firstCalled bool
}

// MakeVisibleCommittedObjectIt returns an iterator that iterates over committed objects visible to the given txn
// two cases:
// 2. normal txn, wait if needed, return committed non-dropped objects
// 1. txn is mock txn, no waiting, only return committed non-dropped objects, used for status check

func (l *ObjectList) MakeVisibleCommittedObjectIt(txn txnif.TxnReader) *VisibleCommittedObjectIt {
	tree := l.tree.Load()
	it := tree.Iter()
	return &VisibleCommittedObjectIt{
		iter:      it,
		txn:       txn,
		isMockTxn: len(txn.GetCtx()) == 0,
		dropped:   make(map[objectio.ObjectId]struct{}),
	}
}

func (it *VisibleCommittedObjectIt) Next() bool {
	var ok bool
	for {
		if !it.firstCalled {
			ok = it.iter.Last()
			it.firstCalled = true
		} else {
			ok = it.iter.Prev()
		}
		if !ok {
			return false
		}
		entry := it.iter.Item()

		if it.isMockTxn {
			// mockTxn can see all objects
			if entry.IsDEntry() || entry.IsCreating() { // exclude D entries & creating C entries
				continue
			}
			if !entry.HasDCounterpart() { // pick only serving committed C entries
				it.curr = entry
				return true
			}
			// ignore being dropped C entries
		} else if entry.IsVisible(it.txn) {
			if entry.IsDEntry() { // exclude D entries
				continue
			}
			if !entry.HasDCounterpart() || !entry.GetNextVersion().IsVisible(it.txn) {
				// 1. serving committed C entries or creating C entry created by this txn
				// 2. C entried being dropped by other invisible txn
				it.curr = entry
				return true
			}
		}
	}
}

func (it *VisibleCommittedObjectIt) Item() *ObjectEntry {
	return it.curr
}

func (it *VisibleCommittedObjectIt) Release() {
	it.iter.Release()
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
	ret += "maxTs_objectID:\n"
	for id, ts := range l.maxTs_objectID {
		ret += fmt.Sprintf(" %s: %s\n", id.ShortStringEx(), ts.ToString())
	}
	return ret
}
