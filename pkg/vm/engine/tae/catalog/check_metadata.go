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
	// "time"

	// "github.com/matrixorigin/matrixone/pkg/container/types"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	// "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

func (catalog *Catalog) CheckMetadata() {
	logutil.Infof("[MetadataCheck] Start")
	p := &LoopProcessor{}
	p.ObjectFn = catalog.checkObject
	p.TombstoneFn = catalog.checkTombstone
	catalog.RecurLoop(p)
	logutil.Infof("[MetadataCheck] End")
}
func (catalog *Catalog) checkTombstone(t data.Tombstone) error {
	obj := t.GetObject().(*ObjectEntry)
	_, err := obj.GetTable().GetObjectByID(&obj.ID)
	if err != nil {
		logutil.Warnf("[MetadataCheck] tombstone and object doesn't match, err %v, obj %v, tombstone %v",
			err,
			obj.PPString(3, 0, ""),
			t.String(3, 0, ""))
	}
	t.CheckTombstone()
	return nil
}
func (catalog *Catalog) checkObject(o *ObjectEntry) error {
	switch o.ObjectState {
	case ObjectState_Create_Active:
		checkMVCCNode(o.CreateNode, State_Active, true, o.IsAppendable(), o)
		if o.DeleteNode != nil {
			logutil.Warnf("[MetadataCheck] creating object has delete node, obj %v", o.StringWithLevel(3))
		}
	case ObjectState_Create_PrepareCommit:
		checkMVCCNode(o.CreateNode, State_PrepareCommit, true, o.IsAppendable(), o)
		if o.DeleteNode != nil {
			logutil.Warnf("[MetadataCheck] creating object has delete node, obj %v", o.StringWithLevel(3))
		}
	case ObjectState_Create_ApplyCommit:
		checkMVCCNode(o.CreateNode, State_ApplyCommit, true, o.IsAppendable(), o)
		if o.DeleteNode != nil {
			logutil.Warnf("[MetadataCheck] creating object has delete node, obj %v", o.StringWithLevel(3))
		}
	case ObjectState_Delete_Active:
		checkMVCCNode(o.CreateNode, State_ApplyCommit, true, o.IsAppendable(), o)
		checkMVCCNode(o.DeleteNode, State_Active, false, o.IsAppendable(), o)
	case ObjectState_Delete_PrepareCommit:
		checkMVCCNode(o.CreateNode, State_ApplyCommit, true, o.IsAppendable(), o)
		checkMVCCNode(o.DeleteNode, State_PrepareCommit, false, o.IsAppendable(), o)
	case ObjectState_Delete_ApplyCommit:
		checkMVCCNode(o.CreateNode, State_ApplyCommit, true, o.IsAppendable(), o)
		checkMVCCNode(o.DeleteNode, State_ApplyCommit, false, o.IsAppendable(), o)
	}
	return nil
}

const (
	State_Active = iota
	State_PrepareCommit
	State_ApplyCommit
)

func checkMVCCNode(node *MVCCNode[*ObjectMVCCNode], state int, create bool, appendable bool, o *ObjectEntry) {
	if node == nil {
		logutil.Warnf("[MetadataCheck] empty mvcc node, obj %v", o.StringWithLevel(3))
	}
	if create {
		if !node.DeletedAt.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong delete ts %v, obj %v", node.DeletedAt.ToString(), o.StringWithLevel(3))
		}
		switch state {
		case State_Active:
			if !node.CreatedAt.Equal(&txnif.UncommitTS) {
				logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", node.CreatedAt.ToString(), o.StringWithLevel(3))
			}
		case State_PrepareCommit:
			if !node.CreatedAt.Equal(&txnif.UncommitTS) {
				logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", node.CreatedAt.ToString(), o.StringWithLevel(3))
			}
		case State_ApplyCommit:
			if node.CreatedAt.Equal(&txnif.UncommitTS) || node.CreatedAt.IsEmpty() {
				logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", node.CreatedAt.ToString(), o.StringWithLevel(3))
			}
		default:
			panic(fmt.Sprintf("invalid state %v", state))
		}
	}
	if !create {
		if node.CreatedAt.Equal(&txnif.UncommitTS) || node.CreatedAt.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", node.DeletedAt.ToString(), o.StringWithLevel(3))
		}
		switch state {
		case State_Active:
			if !node.DeletedAt.Equal(&txnif.UncommitTS) {
				logutil.Warnf("[MetadataCheck] wrong delete ts %v, obj %v", node.CreatedAt.ToString(), o.StringWithLevel(3))
			}
		case State_PrepareCommit:
			if !node.DeletedAt.Equal(&txnif.UncommitTS) {
				logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", node.CreatedAt.ToString(), o.StringWithLevel(3))
			}
		case State_ApplyCommit:
			if node.DeletedAt.Equal(&txnif.UncommitTS) || node.CreatedAt.IsEmpty() {
				logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", node.CreatedAt.ToString(), o.StringWithLevel(3))
			}
		default:
			panic(fmt.Sprintf("invalid state %v", state))
		}
	}
	if node.Start.IsEmpty() {
		logutil.Warnf("[MetadataCheck] wrong start ts %v, obj %v", node.Start.ToString(), o.StringWithLevel(3))
	}
	switch state {
	case State_Active:
		if !node.Prepare.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong prepare ts %v, obj %v", node.Prepare.ToString(), o.StringWithLevel(3))
		}
		if !node.End.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong end ts %v, obj %v", node.End.ToString(), o.StringWithLevel(3))
		}
		if node.Txn == nil {
			logutil.Warnf("[MetadataCheck] txn not existed, obj %v", o.StringWithLevel(3))
		}
	case State_PrepareCommit:
		if !node.Prepare.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong prepare ts %v, obj %v", node.Prepare.ToString(), o.StringWithLevel(3))
		}
		if !node.End.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong end ts %v, obj %v", node.End.ToString(), o.StringWithLevel(3))
		}
		if node.Txn == nil {
			logutil.Warnf("[MetadataCheck] txn not existed, obj %v", o.StringWithLevel(3))
		}
	case State_ApplyCommit:
		if !node.Prepare.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong prepare ts %v, obj %v", node.Prepare.ToString(), o.StringWithLevel(3))
		}
		if node.End.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong end ts %v, obj %v", node.End.ToString(), o.StringWithLevel(3))
		}
		if node.Txn != nil {
			logutil.Warnf("[MetadataCheck] txn existed, obj %v", o.StringWithLevel(3))
		}
	default:
		panic(fmt.Sprintf("invalid state %d", state))
	}
	if appendable && create {
		if !node.BaseNode.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong object stats %v, obj %v", node.BaseNode.String(), o.StringWithLevel(3))
		}
	} else {
		if node.BaseNode.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong object stats %v, obj %v", node.BaseNode.String(), o.StringWithLevel(3))
		}
	}
}
