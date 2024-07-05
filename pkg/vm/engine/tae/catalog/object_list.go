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
	*sync.RWMutex
	sortHint_objectID map[objectio.ObjectId]uint64
	tree              atomic.Pointer[btree.BTreeG[*ObjectEntry]]
}

func NewObjectList() *ObjectList {
	opts := btree.Options{
		Degree:  64,
		NoLocks: true,
	}
	tree := btree.NewBTreeGOptions((*ObjectEntry).Less, opts)
	list := &ObjectList{
		RWMutex:           &sync.RWMutex{},
		sortHint_objectID: make(map[types.Objectid]uint64),
	}
	list.tree.Store(tree)
	return list
}

func (l *ObjectList) GetObjectByID(id *types.Objectid) (obj *ObjectEntry, err error) {
	obj = l.GetLastestNode(id)
	if obj == nil {
		err = moerr.GetOkExpectedEOB()
	}
	return
}

func (l *ObjectList) deleteEntryLocked(obj *objectio.ObjectId) error {
	l.Lock()
	defer l.Unlock()
	oldTree := l.tree.Load()
	newTree := oldTree.Copy()
	objs := l.GetAllNodes(obj)
	for _, obj := range objs {
		newTree.Delete(obj)
	}
	ok := l.tree.CompareAndSwap(oldTree, newTree)
	if !ok {
		panic("concurrent mutation")
	}
	return nil
}

func (l *ObjectList) GetAllNodes(objID *objectio.ObjectId) []*ObjectEntry {
	it := l.tree.Load().Iter()
	defer it.Release()
	hint, ok := l.sortHint_objectID[*objID]
	if !ok {
		panic("logic error")
		// return nil
	}
	key := &ObjectEntry{
		ObjectNode:  ObjectNode{SortHint: hint},
		ObjectState: ObjectState_Create_Active,
	}
	ok = it.Seek(key)
	if !ok {
		return nil
	}
	obj := it.Item()
	if obj.ID != *objID {
		return nil
	}
	ret := []*ObjectEntry{it.Item()}
	for it.Next() {
		obj := it.Item()
		if obj.ID != *objID {
			break
		}
		ret = append(ret, obj)
	}
	return ret
}

func (l *ObjectList) GetLastestNode(objID *objectio.ObjectId) *ObjectEntry {
	objs := l.GetAllNodes(objID)
	if len(objs) == 0 {
		return nil
	}
	return objs[len(objs)-1]
}

func (l *ObjectList) DropObjectByID(id *objectio.ObjectId, txn txnif.TxnReader) (droppedObj *ObjectEntry, isNew bool, err error) {
	obj, err := l.GetObjectByID(id)
	if err != nil {
		return
	}
	if obj.HasDropIntent() {
		return nil, false, moerr.GetOkExpectedEOB()
	}
	if obj.DeleteNode != nil {
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
	l.Lock()
	defer l.Unlock()
	if registerSortHint {
		// todo remove it
		for _, sortHint := range l.sortHint_objectID {
			if sortHint == object.SortHint {
				panic("logic error")
			}
		}
		l.sortHint_objectID[object.ID] = object.SortHint
	} else {
		sortHint, ok := l.sortHint_objectID[object.ID]
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
	newTree.Set(new)
	newTree.Delete(old)
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

func (l *ObjectList) SetSorted(id *objectio.ObjectId) {
	objs := l.GetAllNodes(id)
	if len(objs) == 0 {
		panic("logic error")
	}
	obj := objs[len(objs)-1]
	newObj := obj.GetSortedEntry()
	l.Set(newObj, false)
	return
}
