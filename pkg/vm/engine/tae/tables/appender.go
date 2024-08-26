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

package tables

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type objectAppender struct {
	obj         *aobject
	placeholder uint32
	rows        uint32
}

func newAppender(aobj *aobject) *objectAppender {
	appender := new(objectAppender)
	appender.obj = aobj
	rows, _ := aobj.Rows()
	appender.rows = uint32(rows)
	return appender
}

func (appender *objectAppender) GetMeta() any {
	return appender.obj.meta.Load()
}

func (appender *objectAppender) LockFreeze() {
	appender.obj.freezelock.Lock()
}
func (appender *objectAppender) UnlockFreeze() {
	appender.obj.freezelock.Unlock()
}
func (appender *objectAppender) CheckFreeze() bool {
	return appender.obj.frozen.Load()
}

func (appender *objectAppender) GetID() *common.ID {
	return appender.obj.meta.Load().AsCommonID()
}

func (appender *objectAppender) IsAppendable() bool {
	return appender.rows+appender.placeholder < appender.obj.meta.Load().GetSchema().BlockMaxRows
}

func (appender *objectAppender) Close() {
	appender.obj.Unref()
}

func (appender *objectAppender) IsSameColumns(other any) bool {
	n := appender.obj.PinNode()
	defer n.Unref()
	return n.MustMNode().writeSchema.IsSameColumns(other.(*catalog.Schema))
}

func (appender *objectAppender) PrepareAppend(
	isMergeCompact bool,
	rows uint32,
	txn txnif.AsyncTxn) (node txnif.AppendNode, created bool, n uint32, err error) {
	left := appender.obj.meta.Load().GetSchema().BlockMaxRows - appender.rows - appender.placeholder
	if left == 0 {
		// n = rows
		return
	}
	if rows > left {
		n = left
	} else {
		n = rows
	}
	appender.obj.Lock()
	defer appender.obj.Unlock()
	node, created = appender.obj.appendMVCC.AddAppendNodeLocked(
		txn,
		appender.rows+appender.placeholder,
		appender.placeholder+appender.rows+n)
	if isMergeCompact {
		node.SetIsMergeCompact()
	}
	appender.placeholder += n
	return
}
func (appender *objectAppender) ReplayAppend(
	bat *containers.Batch,
	txn txnif.AsyncTxn) (from int, err error) {
	if from, err = appender.ApplyAppend(bat, txn); err != nil {
		return
	}
	// TODO: Remove ReplayAppend
	appender.obj.meta.Load().GetTable().AddRows(uint64(bat.Length()))
	return
}
func (appender *objectAppender) ApplyAppend(
	bat *containers.Batch,
	txn txnif.AsyncTxn) (from int, err error) {
	n := appender.obj.PinNode()
	defer n.Unref()

	node := n.MustMNode()
	appender.obj.Lock()
	defer appender.obj.Unlock()
	from, err = node.ApplyAppendLocked(bat, txn)

	schema := node.writeSchema
	for _, colDef := range schema.ColDefs {
		if colDef.IsPhyAddr() {
			continue
		}
		if colDef.IsRealPrimary() && !schema.IsSecondaryIndexTable() {
			if err = node.pkIndex.BatchUpsert(bat.Vecs[colDef.Idx].GetDownstreamVector(), from); err != nil {
				panic(err)
			}
		}
	}
	return
}
