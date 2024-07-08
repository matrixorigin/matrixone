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
		if o.CreateNode == nil {
			logutil.Warnf("[MetadataCheck] object hasn't create node, obj %v", o.StringWithLevel(3))
		}
		if !o.CreateNode.CreatedAt.Equal(&txnif.UncommitTS) {
			logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", o.CreateNode.CreatedAt.ToString(), o.StringWithLevel(3))
		}
		if !o.CreateNode.DeletedAt.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong delete ts %v, obj %v", o.CreateNode.DeletedAt.ToString(), o.StringWithLevel(3))
		}
		if o.CreateNode.Start.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong start ts %v, obj %v", o.CreateNode.Start.ToString(), o.StringWithLevel(3))
		}
		if !o.CreateNode.Prepare.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong prepare ts %v, obj %v", o.CreateNode.Prepare.ToString(), o.StringWithLevel(3))
		}
		if !o.CreateNode.End.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong end ts %v, obj %v", o.CreateNode.End.ToString(), o.StringWithLevel(3))
		}
		if o.CreateNode.Txn == nil {
			logutil.Warnf("[MetadataCheck] txn not existed, obj %v", o.StringWithLevel(3))
		}
		if o.IsAppendable() {
			if !o.CreateNode.BaseNode.IsEmpty() {
				logutil.Warnf("[MetadataCheck] wrong object stats %v, obj %v", o.CreateNode.BaseNode.String(), o.StringWithLevel(3))
			}
		} else {
			if o.CreateNode.BaseNode.IsEmpty() {
				logutil.Warnf("[MetadataCheck] wrong object stats %v, obj %v", o.CreateNode.BaseNode.String(), o.StringWithLevel(3))
			}
		}
		if o.DeleteNode != nil {
			logutil.Warnf("[MetadataCheck] creating object has delete node, obj %v", o.StringWithLevel(3))
		}
	case ObjectState_Create_PrepareCommit:
		if o.CreateNode == nil {
			logutil.Warnf("[MetadataCheck] object hasn't create node, obj %v", o.StringWithLevel(3))
		}
		if !o.CreateNode.CreatedAt.Equal(&txnif.UncommitTS) {
			logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", o.CreateNode.CreatedAt.ToString(), o.StringWithLevel(3))
		}
		if !o.CreateNode.DeletedAt.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong delete ts %v, obj %v", o.CreateNode.DeletedAt.ToString(), o.StringWithLevel(3))
		}
		if o.CreateNode.Start.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong start ts %v, obj %v", o.CreateNode.Start.ToString(), o.StringWithLevel(3))
		}
		if o.CreateNode.Prepare.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong prepare ts %v, obj %v", o.CreateNode.Prepare.ToString(), o.StringWithLevel(3))
		}
		if !o.CreateNode.End.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong end ts %v, obj %v", o.CreateNode.End.ToString(), o.StringWithLevel(3))
		}
		if o.CreateNode.Txn == nil {
			logutil.Warnf("[MetadataCheck] txn not existed, obj %v", o.StringWithLevel(3))
		}
		if o.IsAppendable() {
			if !o.CreateNode.BaseNode.IsEmpty() {
				logutil.Warnf("[MetadataCheck] wrong object stats %v, obj %v", o.CreateNode.BaseNode.String(), o.StringWithLevel(3))
			}
		} else {
			if o.CreateNode.BaseNode.IsEmpty() {
				logutil.Warnf("[MetadataCheck] wrong object stats %v, obj %v", o.CreateNode.BaseNode.String(), o.StringWithLevel(3))
			}
		}
		if o.DeleteNode != nil {
			logutil.Warnf("[MetadataCheck] creating object has delete node, obj %v", o.StringWithLevel(3))
		}
	case ObjectState_Create_ApplyCommit:
		if o.CreateNode == nil {
			logutil.Warnf("[MetadataCheck] object hasn't create node, obj %v", o.StringWithLevel(3))
		}
		if !o.CreateNode.CreatedAt.Equal(&txnif.UncommitTS) {
			logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", o.CreateNode.CreatedAt.ToString(), o.StringWithLevel(3))
		}
		if !o.CreateNode.DeletedAt.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong delete ts %v, obj %v", o.CreateNode.DeletedAt.ToString(), o.StringWithLevel(3))
		}
		if o.CreateNode.Start.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong start ts %v, obj %v", o.CreateNode.Start.ToString(), o.StringWithLevel(3))
		}
		if o.CreateNode.Prepare.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong prepare ts %v, obj %v", o.CreateNode.Prepare.ToString(), o.StringWithLevel(3))
		}
		if o.CreateNode.End.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong end ts %v, obj %v", o.CreateNode.End.ToString(), o.StringWithLevel(3))
		}
		if o.CreateNode.Txn != nil {
			logutil.Warnf("[MetadataCheck] txn not existed, obj %v", o.StringWithLevel(3))
		}
		if o.IsAppendable() {
			if !o.CreateNode.BaseNode.IsEmpty() {
				logutil.Warnf("[MetadataCheck] wrong object stats %v, obj %v", o.CreateNode.BaseNode.String(), o.StringWithLevel(3))
			}
		} else {
			if o.CreateNode.BaseNode.IsEmpty() {
				logutil.Warnf("[MetadataCheck] wrong object stats %v, obj %v", o.CreateNode.BaseNode.String(), o.StringWithLevel(3))
			}
		}
		if o.DeleteNode != nil {
			logutil.Warnf("[MetadataCheck] creating object has delete node, obj %v", o.StringWithLevel(3))
		}
	case ObjectState_Delete_Active:
		if o.CreateNode == nil {
			logutil.Warnf("[MetadataCheck] object hasn't create node, obj %v", o.StringWithLevel(3))
		}
		if !o.CreateNode.CreatedAt.Equal(&txnif.UncommitTS) {
			logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", o.CreateNode.CreatedAt.ToString(), o.StringWithLevel(3))
		}
		if !o.CreateNode.DeletedAt.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong delete ts %v, obj %v", o.CreateNode.DeletedAt.ToString(), o.StringWithLevel(3))
		}
		if o.CreateNode.Start.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong start ts %v, obj %v", o.CreateNode.Start.ToString(), o.StringWithLevel(3))
		}
		if o.CreateNode.Prepare.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong prepare ts %v, obj %v", o.CreateNode.Prepare.ToString(), o.StringWithLevel(3))
		}
		if !o.CreateNode.End.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong end ts %v, obj %v", o.CreateNode.End.ToString(), o.StringWithLevel(3))
		}
		if o.CreateNode.Txn == nil {
			logutil.Warnf("[MetadataCheck] txn not existed, obj %v", o.StringWithLevel(3))
		}
		if o.IsAppendable() {
			if !o.CreateNode.BaseNode.IsEmpty() {
				logutil.Warnf("[MetadataCheck] wrong object stats %v, obj %v", o.CreateNode.BaseNode.String(), o.StringWithLevel(3))
			}
		} else {
			if o.CreateNode.BaseNode.IsEmpty() {
				logutil.Warnf("[MetadataCheck] wrong object stats %v, obj %v", o.CreateNode.BaseNode.String(), o.StringWithLevel(3))
			}
		}
		if o.DeleteNode == nil {
			logutil.Warnf("[MetadataCheck] object doesn't have delete node, obj %v", o.StringWithLevel(3))
		}
		if !o.DeleteNode.CreatedAt.Equal(&txnif.UncommitTS) {
			logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", o.CreateNode.CreatedAt.ToString(), o.StringWithLevel(3))
		}
		if !o.DeleteNode.DeletedAt.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong delete ts %v, obj %v", o.CreateNode.DeletedAt.ToString(), o.StringWithLevel(3))
		}
		if o.DeleteNode.Start.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong start ts %v, obj %v", o.CreateNode.Start.ToString(), o.StringWithLevel(3))
		}
		if o.DeleteNode.Prepare.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong prepare ts %v, obj %v", o.CreateNode.Prepare.ToString(), o.StringWithLevel(3))
		}
		if !o.DeleteNode.End.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong end ts %v, obj %v", o.CreateNode.End.ToString(), o.StringWithLevel(3))
		}
		if o.DeleteNode.Txn == nil {
			logutil.Warnf("[MetadataCheck] txn not existed, obj %v", o.StringWithLevel(3))
		}
		if o.IsAppendable() {
			if !o.DeleteNode.BaseNode.IsEmpty() {
				logutil.Warnf("[MetadataCheck] wrong object stats %v, obj %v", o.CreateNode.BaseNode.String(), o.StringWithLevel(3))
			}
		} else {
			if o.DeleteNode.BaseNode.IsEmpty() {
				logutil.Warnf("[MetadataCheck] wrong object stats %v, obj %v", o.CreateNode.BaseNode.String(), o.StringWithLevel(3))
			}
		}
	case ObjectState_Delete_PrepareCommit:
	case ObjectState_Delete_ApplyCommit:
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
		switch state {
		case State_Active:
			if !node.CreatedAt.Equal(&txnif.UncommitTS) {
				logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", o.CreateNode.CreatedAt.ToString(), o.StringWithLevel(3))
			}
		case State_PrepareCommit:
			if !node.CreatedAt.Equal(&txnif.UncommitTS) {
				logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", o.CreateNode.CreatedAt.ToString(), o.StringWithLevel(3))
			}
		case State_ApplyCommit:
			if node.CreatedAt.Equal(&txnif.UncommitTS) || node.CreatedAt.IsEmpty() {
				logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", o.CreateNode.CreatedAt.ToString(), o.StringWithLevel(3))
			}
		default:
			panic(fmt.Sprintf("invalid state %v", state))
		}
	}
	if !create {
		if node.CreatedAt.Equal(&txnif.UncommitTS) || node.CreatedAt.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", o.DeleteNode.DeletedAt.ToString(), o.StringWithLevel(3))
		}
		switch state {
		case State_Active:
			if !node.DeletedAt.Equal(&txnif.UncommitTS) {
				logutil.Warnf("[MetadataCheck] wrong delete ts %v, obj %v", o.DeleteNode.CreatedAt.ToString(), o.StringWithLevel(3))
			}
		case State_PrepareCommit:
			if !node.DeletedAt.Equal(&txnif.UncommitTS) {
				logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", o.DeleteNode.CreatedAt.ToString(), o.StringWithLevel(3))
			}
		case State_ApplyCommit:
			if node.DeletedAt.Equal(&txnif.UncommitTS) || node.CreatedAt.IsEmpty() {
				logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", o.DeleteNode.CreatedAt.ToString(), o.StringWithLevel(3))
			}
		default:
			panic(fmt.Sprintf("invalid state %v", state))
		}
	}
	if !node.DeletedAt.IsEmpty() {
		logutil.Warnf("[MetadataCheck] wrong delete ts %v, obj %v", o.CreateNode.DeletedAt.ToString(), o.StringWithLevel(3))
	}
	if node.Start.IsEmpty() {
		logutil.Warnf("[MetadataCheck] wrong start ts %v, obj %v", o.CreateNode.Start.ToString(), o.StringWithLevel(3))
	}
	if node.Prepare.IsEmpty() {
		logutil.Warnf("[MetadataCheck] wrong prepare ts %v, obj %v", o.CreateNode.Prepare.ToString(), o.StringWithLevel(3))
	}
	if !node.End.IsEmpty() {
		logutil.Warnf("[MetadataCheck] wrong end ts %v, obj %v", o.CreateNode.End.ToString(), o.StringWithLevel(3))
	}
	if node.Txn == nil {
		logutil.Warnf("[MetadataCheck] txn not existed, obj %v", o.StringWithLevel(3))
	}
	if appendable {
		if !node.BaseNode.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong object stats %v, obj %v", o.CreateNode.BaseNode.String(), o.StringWithLevel(3))
		}
	} else {
		if node.BaseNode.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong object stats %v, obj %v", o.CreateNode.BaseNode.String(), o.StringWithLevel(3))
		}
	}
}
