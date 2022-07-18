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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
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

func (appender *blockAppender) GetMeta() any {
	return appender.node.block.meta
}

func (appender *blockAppender) GetID() *common.ID {
	return appender.node.block.meta.AsCommonID()
}

func (appender *blockAppender) IsAppendable() bool {
	return appender.rows+appender.placeholder < appender.node.block.meta.GetSchema().BlockMaxRows
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
func (appender *blockAppender) ReplayAppend(bat *containers.Batch) (err error) {
	err = appender.node.DoWithPin(func() (err error) {
		var from int
		err = appender.node.Expand(0, func() error {
			var err error
			from, err = appender.node.ApplyAppend(bat, nil)
			return err
		})
		schema := appender.node.block.meta.GetSchema()
		if schema.HasPK() {
			keysCtx := new(index.KeysCtx)
			if schema.IsSinglePK() {
				keysCtx.Keys = bat.Vecs[appender.node.block.meta.GetSchema().GetSingleSortKeyIdx()]
			} else {
				cols := appender.node.block.GetSortColumns(schema, bat)
				keysCtx.Keys = model.EncodeCompoundColumn(cols...)
				defer keysCtx.Keys.Close()
			}
			keysCtx.Start = 0
			keysCtx.Count = bat.Length()
			// logutil.Infof("Append into %d: %s", appender.node.meta.GetID(), pks.String())
			_, err = appender.node.block.index.BatchUpsert(keysCtx, from, 0)
			if err != nil {
				panic(err)
			}
		}
		appender.node.block.meta.GetSegment().GetTable().AddRows(uint64(bat.Length()))

		return
	})
	return
}
func (appender *blockAppender) ApplyAppend(
	bat *containers.Batch,
	txn txnif.AsyncTxn) (from int, err error) {
	err = appender.node.DoWithPin(func() (err error) {
		appender.node.block.mvcc.Lock()
		defer appender.node.block.mvcc.Unlock()
		err = appender.node.Expand(0, func() error {
			var err error
			from, err = appender.node.ApplyAppend(bat, txn)
			return err
		})

		schema := appender.node.block.meta.GetSchema()
		if schema.HasPK() {
			keysCtx := new(index.KeysCtx)

			if schema.IsSinglePK() {
				keysCtx.Keys = bat.Vecs[appender.node.block.meta.GetSchema().GetSingleSortKeyIdx()]
			} else {
				cols := appender.node.block.GetSortColumns(schema, bat)
				keysCtx.Keys = model.EncodeCompoundColumn(cols...)
				defer keysCtx.Keys.Close()
			}
			keysCtx.Start = 0
			keysCtx.Count = bat.Length()
			// logutil.Infof("Append into %s: %s", appender.node.block.meta.Repr(), keysCtx.Keys.String())
			_, err = appender.node.block.index.BatchUpsert(keysCtx, from, txn.GetStartTS())
			if err != nil {
				panic(err)
			}
		}
		return
	})
	return
}
