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

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

func (catalog *Catalog) CheckMetadata() {
	logutil.Infof("[MetadataCheck] Start")
	p := &LoopProcessor{}
	p.ObjectFn = catalog.checkObject
	p.TombstoneFn = catalog.checkObject
	catalog.RecurLoop(p)
	logutil.Infof("[MetadataCheck] End")
}
func (catalog *Catalog) checkObject(o *ObjectEntry) error {
	switch o.ObjectState {
	case ObjectState_Create_Active:
		if !o.CreatedAt.Equal(&txnif.UncommitTS) {
			logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", o.CreatedAt.ToString(), o.StringWithLevel(3))
		}
		if !o.DeletedAt.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong delete ts %v, obj %v", o.DeletedAt.ToString(), o.StringWithLevel(3))
		}
		if o.IsAppendable() {
			if !o.ObjectMVCCNode.IsEmpty() {
				logutil.Warnf("[MetadataCheck] wrong object stats %v, obj %v", o.ObjectMVCCNode.String(), o.StringWithLevel(3))
			}
		}
		checkMVCCNode(&o.CreateNode, State_Active, o)
		checkMVCCNode(&o.DeleteNode, State_Empty, o)
	case ObjectState_Create_PrepareCommit:
		if !o.CreatedAt.Equal(&txnif.UncommitTS) {
			logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", o.CreatedAt.ToString(), o.StringWithLevel(3))
		}
		if !o.DeletedAt.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong delete ts %v, obj %v", o.DeletedAt.ToString(), o.StringWithLevel(3))
		}
		if o.IsAppendable() {
			if !o.ObjectMVCCNode.IsEmpty() {
				logutil.Warnf("[MetadataCheck] wrong object stats %v, obj %v", o.ObjectMVCCNode.String(), o.StringWithLevel(3))
			}
		}
		checkMVCCNode(&o.CreateNode, State_PrepareCommit, o)
		checkMVCCNode(&o.DeleteNode, State_Empty, o)
	case ObjectState_Create_ApplyCommit:
		if o.CreatedAt.Equal(&txnif.UncommitTS) || o.CreatedAt.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", o.CreatedAt.ToString(), o.StringWithLevel(3))
		}
		if !o.DeletedAt.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong delete ts %v, obj %v", o.DeletedAt.ToString(), o.StringWithLevel(3))
		}
		if o.IsAppendable() {
			if !o.ObjectMVCCNode.IsEmpty() {
				logutil.Warnf("[MetadataCheck] wrong object stats %v, obj %v", o.ObjectMVCCNode.String(), o.StringWithLevel(3))
			}
		} else {
			if o.ObjectMVCCNode.IsEmpty() {
				logutil.Warnf("[MetadataCheck] wrong object stats %v, obj %v", o.ObjectMVCCNode.String(), o.StringWithLevel(3))
			}
		}
		checkMVCCNode(&o.CreateNode, State_ApplyCommit, o)
		checkMVCCNode(&o.DeleteNode, State_Empty, o)
	case ObjectState_Delete_Active:
		if o.CreatedAt.Equal(&txnif.UncommitTS) || o.CreatedAt.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", o.CreatedAt.ToString(), o.StringWithLevel(3))
		}
		if !o.DeletedAt.Equal(&txnif.UncommitTS) {
			logutil.Warnf("[MetadataCheck] wrong delete ts %v, obj %v", o.DeletedAt.ToString(), o.StringWithLevel(3))
		}
		if !o.IsAppendable() {
			if o.ObjectMVCCNode.IsEmpty() {
				logutil.Warnf("[MetadataCheck] wrong object stats %v, obj %v", o.ObjectMVCCNode.String(), o.StringWithLevel(3))
			}
		}
		checkMVCCNode(&o.CreateNode, State_ApplyCommit, o)
		checkMVCCNode(&o.DeleteNode, State_Active, o)
	case ObjectState_Delete_PrepareCommit:
		if o.CreatedAt.Equal(&txnif.UncommitTS) || o.CreatedAt.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", o.CreatedAt.ToString(), o.StringWithLevel(3))
		}
		if !o.DeletedAt.Equal(&txnif.UncommitTS) {
			logutil.Warnf("[MetadataCheck] wrong delete ts %v, obj %v", o.DeletedAt.ToString(), o.StringWithLevel(3))
		}
		if !o.IsAppendable() {
			if o.ObjectMVCCNode.IsEmpty() {
				logutil.Warnf("[MetadataCheck] wrong object stats %v, obj %v", o.ObjectMVCCNode.String(), o.StringWithLevel(3))
			}
		}
		checkMVCCNode(&o.CreateNode, State_ApplyCommit, o)
		checkMVCCNode(&o.DeleteNode, State_PrepareCommit, o)
	case ObjectState_Delete_ApplyCommit:
		if o.CreatedAt.Equal(&txnif.UncommitTS) || o.CreatedAt.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong create ts %v, obj %v", o.CreatedAt.ToString(), o.StringWithLevel(3))
		}
		if o.DeletedAt.Equal(&txnif.UncommitTS) || o.DeletedAt.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong delete ts %v, obj %v", o.DeletedAt.ToString(), o.StringWithLevel(3))
		}
		if o.DeletedAt.Less(&o.CreatedAt) {
			logutil.Warnf("[MetadataCheck] wrong delete ts %v, obj %v", o.DeletedAt.ToString(), o.StringWithLevel(3))
		}
		if o.ObjectMVCCNode.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong object stats %v, obj %v", o.ObjectMVCCNode.String(), o.StringWithLevel(3))
		}
		checkMVCCNode(&o.CreateNode, State_ApplyCommit, o)
		checkMVCCNode(&o.DeleteNode, State_ApplyCommit, o)
	}
	return nil
}

const (
	State_Active = iota
	State_PrepareCommit
	State_ApplyCommit
	State_Empty
)

func checkMVCCNode(node *txnbase.TxnMVCCNode, state int, o *ObjectEntry) {
	if node == nil {
		logutil.Warnf("[MetadataCheck] empty mvcc node, obj %v", o.StringWithLevel(3))
	}
	switch state {
	case State_Empty:
		if !node.Start.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong start ts %v, obj %v", node.Start.ToString(), o.StringWithLevel(3))
		}
		if !node.Prepare.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong prepare ts %v, obj %v", node.Prepare.ToString(), o.StringWithLevel(3))
		}
		if !node.End.IsEmpty() {
			logutil.Warnf("[MetadataCheck] wrong end ts %v, obj %v", node.End.ToString(), o.StringWithLevel(3))
		}
		if node.Txn != nil {
			logutil.Warnf("[MetadataCheck] txn existed, obj %v", o.StringWithLevel(3))
		}
	case State_Active:
		if node.Start.IsEmpty() || node.Start.Equal(&txnif.UncommitTS) {
			logutil.Warnf("[MetadataCheck] wrong start ts %v, obj %v", node.Start.ToString(), o.StringWithLevel(3))
		}
		if !node.Prepare.Equal(&txnif.UncommitTS) {
			logutil.Warnf("[MetadataCheck] wrong prepare ts %v, obj %v", node.Prepare.ToString(), o.StringWithLevel(3))
		}
		if !node.End.Equal(&txnif.UncommitTS) {
			logutil.Warnf("[MetadataCheck] wrong end ts %v, obj %v", node.End.ToString(), o.StringWithLevel(3))
		}
		if node.Txn == nil {
			logutil.Warnf("[MetadataCheck] txn is nil, obj %v", o.StringWithLevel(3))
		}
	case State_PrepareCommit:
		if node.Start.IsEmpty() || node.Start.Equal(&txnif.UncommitTS) {
			logutil.Warnf("[MetadataCheck] wrong start ts %v, obj %v", node.Start.ToString(), o.StringWithLevel(3))
		}
		if node.Prepare.IsEmpty() || node.Prepare.Equal(&txnif.UncommitTS) {
			logutil.Warnf("[MetadataCheck] wrong prepare ts %v, obj %v", node.Prepare.ToString(), o.StringWithLevel(3))
		}
		if !node.End.Equal(&txnif.UncommitTS) {
			logutil.Warnf("[MetadataCheck] wrong end ts %v, obj %v", node.End.ToString(), o.StringWithLevel(3))
		}
		if node.Txn == nil {
			logutil.Warnf("[MetadataCheck] txn is nil, obj %v", o.StringWithLevel(3))
		}
	case State_ApplyCommit:
		if node.Start.IsEmpty() || node.Start.Equal(&txnif.UncommitTS) {
			logutil.Warnf("[MetadataCheck] wrong start ts %v, obj %v", node.Start.ToString(), o.StringWithLevel(3))
		}
		if node.Prepare.IsEmpty() || node.Prepare.Equal(&txnif.UncommitTS) {
			logutil.Warnf("[MetadataCheck] wrong prepare ts %v, obj %v", node.Prepare.ToString(), o.StringWithLevel(3))
		}
		if node.End.IsEmpty() || node.End.Equal(&txnif.UncommitTS) {
			logutil.Warnf("[MetadataCheck] wrong end ts %v, obj %v", node.End.ToString(), o.StringWithLevel(3))
		}
		if node.Txn != nil {
			logutil.Warnf("[MetadataCheck] txn exists, obj %v", o.StringWithLevel(3))
		}
	default:
		panic(fmt.Sprintf("invalid state %v", state))
	}
}
