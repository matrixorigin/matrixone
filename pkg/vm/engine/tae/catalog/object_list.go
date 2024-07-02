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
	*btree.BTreeG[*ObjectEntry]
}

func NewObjectList() *ObjectList {
	return &ObjectList{
		RWMutex: &sync.RWMutex{},
		BTreeG:  btree.NewBTreeG((*ObjectEntry).Less),
	}
}

func (l *ObjectList) Copy() *ObjectList {
	// if l.RWMutex != nil {
	l.RLock()
	defer l.RUnlock()
	// }
	return &ObjectList{
		BTreeG: l.BTreeG.Copy(),
	}
}

func (l *ObjectList) GetObjectByID(id *types.Objectid) (obj *ObjectEntry, err error) {
	obj = l.GetLastestNode(id)
	if obj == nil {
		err = moerr.GetOkExpectedEOB()
	}
	return
}

func (l *ObjectList) Insert(obj *ObjectEntry) {
	l.Set(obj)
}

func (l *ObjectList) deleteEntryLocked(obj *objectio.ObjectId) error {
	objs := l.GetAllNodes(obj)
	for _, obj := range objs {
		l.Delete(obj)
	}
	return nil
}

func (l *ObjectList) GetAllNodes(objID *objectio.ObjectId) []*ObjectEntry {
	it := l.Iter()
	key := &ObjectEntry{
		ID:          *objID,
		ObjectState: ObjectState_Create_Active,
	}
	ok := it.Seek(key)
	if !ok {
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
	l.Lock()
	defer l.Unlock()
	objs := l.GetAllNodes(id)
	if len(objs) == 0 {
		return nil, false, moerr.GetOkExpectedEOB()
	}
	obj := objs[len(objs)-1]
	if obj.HasDropIntent() {
		return nil, false, moerr.GetOkExpectedEOB()
	}
	needWait, txnToWait := obj.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		txnToWait.GetTxnState(true)
	}
	if err := obj.CheckConflict(txn); err != nil {
		return nil, false, err
	}
	droppedObj, isNew = obj.GetDropEntry(txn)
	if isNew {
		l.Insert(droppedObj)
	}
	return
}

func (l *ObjectList) UpdateObjectInfo(id *objectio.ObjectId, txn txnif.TxnReader, stats *objectio.ObjectStats) (isNew bool, err error) {
	l.Lock()
	defer l.Unlock()
	objs := l.GetAllNodes(id)
	if len(objs) == 0 {
		return false, moerr.GetOkExpectedEOB()
	}
	obj := objs[len(objs)-1]
	needWait, txnToWait := obj.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		txnToWait.GetTxnState(true)
	}
	if err := obj.CheckConflict(txn); err != nil {
		return false, err
	}
	newObj, isNew := obj.GetUpdateEntry(txn, stats)
	if isNew {
		l.Insert(newObj)
	}
	return
}
