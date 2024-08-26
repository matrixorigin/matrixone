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
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/tidwall/btree"
)

const (
	ObjectState_Create_Active uint8 = iota
	ObjectState_Create_PrepareCommit
	ObjectState_Create_ApplyCommit
	ObjectState_Delete_Active
	ObjectState_Delete_PrepareCommit
	ObjectState_Delete_ApplyCommit
)

type ObjectList struct {
	isTombstone bool
	*sync.RWMutex
	sortHint_objectID map[objectio.ObjectId]uint64
	tree              atomic.Pointer[btree.BTreeG[*ObjectEntry]]
}

func NewObjectList(isTombstone bool) *ObjectList {
	opts := btree.Options{
		Degree:  64,
		NoLocks: true,
	}
	tree := btree.NewBTreeGOptions((*ObjectEntry).Less, opts)
	list := &ObjectList{
		RWMutex:           &sync.RWMutex{},
		sortHint_objectID: make(map[types.Objectid]uint64),
		isTombstone:       isTombstone,
	}
	list.tree.Store(tree)
	return list
}

func (l *ObjectList) GetObjectByID(objectID *objectio.ObjectId) (obj *ObjectEntry, err error) {
	l.RLock()
	sortHint := l.sortHint_objectID[*objectID]
	l.RUnlock()
	obj = l.GetLastestNode(sortHint)
	if obj == nil {
		err = moerr.GetOkExpectedEOB()
	}
	return
}

func (l *ObjectList) deleteEntryLocked(sortHint uint64) error {
	l.Lock()
	defer l.Unlock()
	oldTree := l.tree.Load()
	newTree := oldTree.Copy()
	objs := l.GetAllNodes(sortHint)
	for _, obj := range objs {
		newTree.Delete(obj)
	}
	ok := l.tree.CompareAndSwap(oldTree, newTree)
	if !ok {
		panic("concurrent mutation")
	}
	return nil
}

func (l *ObjectList) GetAllNodes(sortHint uint64) []*ObjectEntry {
	it := l.tree.Load().Iter()
	defer it.Release()
	key := &ObjectEntry{
		ObjectNode:  ObjectNode{SortHint: sortHint},
		ObjectState: ObjectState_Create_Active,
	}
	ok := it.Seek(key)
	if !ok {
		return nil
	}
	obj := it.Item()
	if obj.SortHint != sortHint {
		return nil
	}
	ret := []*ObjectEntry{it.Item()}
	for it.Next() {
		obj := it.Item()
		if obj.SortHint != sortHint {
			break
		}
		ret = append(ret, obj)
	}
	return ret
}

func (l *ObjectList) GetLastestNode(sortHint uint64) *ObjectEntry {
	objs := l.GetAllNodes(sortHint)
	if len(objs) == 0 {
		return nil
	}
	return objs[len(objs)-1]
}

func (l *ObjectList) DropObjectByID(objectID *objectio.ObjectId, txn txnif.TxnReader) (droppedObj *ObjectEntry, isNew bool, err error) {
	obj, err := l.GetObjectByID(objectID)
	if err != nil {
		return
	}
	if obj.HasDropIntent() {
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
	droppedObj, isNew = obj.GetDropEntry(txn)
	l.Update(droppedObj, obj)
	return
}
func (l *ObjectList) Set(object *ObjectEntry, registerSortHint bool) {
	if object.IsTombstone != l.isTombstone {
		panic("logic error")
	}
	l.Lock()
	defer l.Unlock()
	if registerSortHint {
		// todo remove it
		for _, sortHint := range l.sortHint_objectID {
			if sortHint == object.SortHint {
				panic("logic error")
			}
		}
		l.sortHint_objectID[*object.ID()] = object.SortHint
	} else {
		sortHint, ok := l.sortHint_objectID[*object.ID()]
		if !ok || sortHint != object.SortHint {
			panic("logic error")
		}
	}
	oldTree := l.tree.Load()
	newTree := oldTree.Copy()
	newTree.Set(object)
	ok := l.tree.CompareAndSwap(oldTree, newTree)
	if !ok {
		panic("concurrent mutation")
	}
}
func (l *ObjectList) Update(new, old *ObjectEntry) {
	l.Lock()
	defer l.Unlock()
	oldTree := l.tree.Load()
	newTree := oldTree.Copy()
	if new.IsTombstone != l.isTombstone {
		panic("logic error")
	}
	newTree.Delete(old)
	newTree.Set(new)
	ok := l.tree.CompareAndSwap(oldTree, newTree)
	if !ok {
		panic("concurrent mutation")
	}
}
func (l *ObjectList) Delete(obj *ObjectEntry) {
	l.Lock()
	defer l.Unlock()
	oldTree := l.tree.Load()
	newTree := oldTree.Copy()
	newTree.Delete(obj)
	ok := l.tree.CompareAndSwap(oldTree, newTree)
	if !ok {
		panic("concurrent mutation")
	}
}
func (l *ObjectList) UpdateObjectInfo(obj *ObjectEntry, txn txnif.TxnReader, stats *objectio.ObjectStats) (isNew bool, err error) {
	needWait, txnToWait := obj.GetLastMVCCNode().NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		txnToWait.GetTxnState(true)
	}
	if err := obj.GetLastMVCCNode().CheckConflict(txn); err != nil {
		return false, err
	}
	newObj, isNew := obj.GetUpdateEntry(txn, stats)
	l.Set(newObj, false)
	return
}

func (l *ObjectList) SetSorted(sortHint uint64) {
	objs := l.GetAllNodes(sortHint)
	if len(objs) == 0 {
		panic("logic error")
	}
	obj := objs[len(objs)-1]
	newObj := obj.GetSortedEntry()
	l.Set(newObj, false)
}
