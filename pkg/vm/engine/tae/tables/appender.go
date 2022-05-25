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
	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type blockAppender struct {
	node        *appendableNode
	placeholder uint32
	rows        uint32
}

func newAppender(node *appendableNode) *blockAppender {
	appender := new(blockAppender)
	appender.node = node
	appender.rows = node.Rows(nil, true)
	return appender
}

func (appender *blockAppender) Close() error {
	return nil
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

func (appender *blockAppender) PrepareAppend(rows uint32) (n uint32, err error) {
	left := appender.node.block.meta.GetSchema().BlockMaxRows - appender.rows - appender.placeholder
	if left == 0 {
		return
	}
	if rows > left {
		n = left
	} else {
		n = rows
	}
	appender.placeholder += n
	return
}
func (appender *blockAppender) OnReplayAppendNode(maxrow uint32) {
	appender.node.block.mvcc.AddAppendNodeLocked(nil, maxrow)
}
func (appender *blockAppender) OnReplayInsertNode(bat *gbat.Batch, offset, length uint32, txn txnif.AsyncTxn) (node txnif.AppendNode, from uint32, err error) {
	err = appender.node.Expand(0, func() error {
		var err error
		from, err = appender.node.ApplyAppend(bat, offset, length, txn)
		return err
	})

	pks := bat.Vecs[appender.node.block.meta.GetSchema().PrimaryKey]
	// logutil.Infof("Append into %d: %s", appender.node.meta.GetID(), pks.String())
	_, _, err = appender.node.block.index.BatchInsert(pks, offset, length, from, false)
	if err != nil {
		panic(err)
	}

	return
}
func (appender *blockAppender) ApplyAppend(bat *gbat.Batch, offset, length uint32, txn txnif.AsyncTxn) (node txnif.AppendNode, from uint32, err error) {
	err = appender.node.DoWithPin(func() (err error) {
		writeLock := appender.node.block.mvcc.GetExclusiveLock()
		defer writeLock.Unlock()
		err = appender.node.Expand(0, func() error {
			var err error
			from, err = appender.node.ApplyAppend(bat, offset, length, txn)
			return err
		})

		pks := bat.Vecs[appender.node.block.meta.GetSchema().PrimaryKey]
		// logutil.Infof("Append into %s: %s", appender.node.block.meta.Repr(), pks.String())
		postions, rows, err := appender.node.block.index.BatchInsert(pks, offset, length, from, false)
		if err != nil {
			panic(err)
		}
		if postions != nil {
			ts := txn.GetCommitTS()
			posArr := postions.ToArray()
			rowArr := rows.ToArray()
			for i := 0; i < len(posArr); i++ {
				key := compute.GetValue(pks, posArr[i])
				if err = appender.node.block.delIndex.Upsert(key, rowArr[i], ts); err != nil {
					panic(err)
				}
			}
		}
		node = appender.node.block.mvcc.AddAppendNodeLocked(txn, appender.node.rows)
		return
	})
	return
}
