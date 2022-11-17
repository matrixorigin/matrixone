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

func (appender *blockAppender) GetID() *common.ID {
	return appender.blk.meta.AsCommonID()
}

func (appender *blockAppender) IsAppendable() bool {
	return appender.rows+appender.placeholder < appender.blk.meta.GetSchema().BlockMaxRows
}

func (appender *blockAppender) Close() {
	appender.blk.Unref()
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
	appender.placeholder += n
	appender.blk.Lock()
	defer appender.blk.Unlock()
	node, created = appender.blk.mvcc.AddAppendNodeLocked(
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
	appender.blk.meta.GetSegment().GetTable().AddRows(uint64(bat.Length()))
	return
}
func (appender *blockAppender) ApplyAppend(
	bat *containers.Batch,
	txn txnif.AsyncTxn) (from int, err error) {
	node := appender.blk.PinMemoryNode()
	defer node.Close()
	appender.blk.Lock()
	defer appender.blk.Unlock()
	from, err = node.Item().ApplyAppend(bat, txn)

	schema := appender.blk.meta.GetSchema()
	keysCtx := new(index.KeysCtx)
	keysCtx.Count = bat.Length()
	for _, colDef := range schema.ColDefs {
		if colDef.IsPhyAddr() {
			continue
		}
		keysCtx.Keys = bat.Vecs[colDef.Idx]
		if err = node.Item().indexes[colDef.Idx].BatchUpsert(keysCtx, from); err != nil {
			panic(err)
		}
	}
	return
}
