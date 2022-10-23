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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type blockAppender struct {
	node        *appendableNode
	placeholder uint32
	rows        uint32
}

func newAppender(node *appendableNode) *blockAppender {
	appender := new(blockAppender)
	appender.node = node
	appender.rows = node.Rows()
	return appender
}

func (appender *blockAppender) GetMeta() any {
	return appender.node.block.meta
}

func (appender *blockAppender) GetID() *common.ID {
	return appender.node.block.meta.AsCommonID()
}

func (appender *blockAppender) IsAppendable() bool {
	return appender.rows+appender.placeholder < appender.node.block.meta.GetSchema().BlockMaxRows
}

func (appender *blockAppender) Close() {
	appender.node.block.Unref()
}
func (appender *blockAppender) PrepareAppend(
	rows uint32,
	txn txnif.AsyncTxn) (node txnif.AppendNode, created bool, n uint32, err error) {
	left := appender.node.block.meta.GetSchema().BlockMaxRows - appender.rows - appender.placeholder
	if left == 0 {
		// n = rows
		return
	}
	if rows > left {
		n = left
	} else {
		n = rows
	}
	appender.placeholder += n
	appender.node.block.mvcc.Lock()
	defer appender.node.block.mvcc.Unlock()
	node, created = appender.node.block.mvcc.AddAppendNodeLocked(
		txn,
		appender.rows,
		appender.placeholder+appender.rows)
	return
}
func (appender *blockAppender) ReplayAppend(
	bat *containers.Batch,
	txn txnif.AsyncTxn) (from int, err error) {
	if from, err = appender.ApplyAppend(bat, txn); err != nil {
		return
	}
	// TODO: Remove ReplayAppend
	appender.node.block.meta.GetSegment().GetTable().AddRows(uint64(bat.Length()))
	return
}
func (appender *blockAppender) ApplyAppend(
	bat *containers.Batch,
	txn txnif.AsyncTxn) (from int, err error) {
	appender.node.block.mvcc.Lock()
	defer appender.node.block.mvcc.Unlock()
	from, err = appender.node.ApplyAppend(bat, txn)

	schema := appender.node.block.meta.GetSchema()
	keysCtx := new(index.KeysCtx)
	keysCtx.Count = bat.Length()
	for _, colDef := range schema.ColDefs {
		if colDef.IsPhyAddr() {
			continue
		}
		keysCtx.Keys = bat.Vecs[colDef.Idx]
		if err = appender.node.block.indexes[colDef.Idx].BatchUpsert(keysCtx, from); err != nil {
			panic(err)
		}
	}
	return
}
