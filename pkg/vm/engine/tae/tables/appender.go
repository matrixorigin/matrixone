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

type blockAppender struct {
	blk         *ablock
	placeholder uint32
	rows        uint32
}

func newAppender(ablk *ablock) *blockAppender {
	appender := new(blockAppender)
	appender.blk = ablk
	appender.rows = uint32(ablk.Rows())
	return appender
}

func (appender *blockAppender) GetMeta() any {
	return appender.blk.meta
}

func (appender *blockAppender) LockFreeze() {
	appender.blk.freezelock.Lock()
}
func (appender *blockAppender) UnlockFreeze() {
	appender.blk.freezelock.Unlock()
}
func (appender *blockAppender) CheckFreeze() bool {
	return appender.blk.frozen.Load()
}

func (appender *blockAppender) GetID() *common.ID {
	return appender.blk.meta.AsCommonID()
}

func (appender *blockAppender) IsAppendable() bool {
	return appender.rows+appender.placeholder < appender.blk.meta.GetSchema().BlockMaxRows
}

func (appender *blockAppender) Close() {
	appender.blk.Unref()
}

func (appender *blockAppender) IsSameColumns(other any) bool {
	n := appender.blk.PinNode()
	defer n.Unref()
	return n.MustMNode().writeSchema.IsSameColumns(other.(*catalog.Schema))
}

func (appender *blockAppender) PrepareAppend(
	rows uint32,
	txn txnif.AsyncTxn) (node txnif.AppendNode, created bool, n uint32, err error) {
	left := appender.blk.meta.GetSchema().BlockMaxRows - appender.rows - appender.placeholder
	if left == 0 {
		// n = rows
		return
	}
	if rows > left {
		n = left
	} else {
		n = rows
	}
	appender.blk.Lock()
	defer appender.blk.Unlock()
	node, created = appender.blk.mvcc.AddAppendNodeLocked(
		txn,
		appender.rows+appender.placeholder,
		appender.placeholder+appender.rows+n)
	appender.placeholder += n
	return
}
func (appender *blockAppender) ReplayAppend(
	bat *containers.Batch,
	txn txnif.AsyncTxn) (from int, err error) {
	if from, err = appender.ApplyAppend(bat, txn); err != nil {
		return
	}
	// TODO: Remove ReplayAppend
	appender.blk.meta.GetObject().GetTable().AddRows(uint64(bat.Length()))
	return
}
func (appender *blockAppender) ApplyAppend(
	bat *containers.Batch,
	txn txnif.AsyncTxn) (from int, err error) {
	n := appender.blk.PinNode()
	defer n.Unref()

	node := n.MustMNode()
	appender.blk.Lock()
	defer appender.blk.Unlock()
	from, err = node.ApplyAppend(bat, txn)

	schema := node.writeSchema
	for _, colDef := range schema.ColDefs {
		if colDef.IsPhyAddr() {
			continue
		}
		if colDef.IsRealPrimary() {
			if err = node.pkIndex.BatchUpsert(bat.Vecs[colDef.Idx].GetDownstreamVector(), from); err != nil {
				panic(err)
			}
		}
	}
	return
}
