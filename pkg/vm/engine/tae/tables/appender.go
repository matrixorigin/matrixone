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
	"context"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type objectAppender struct {
	obj *aobject
}

func newAppender(aobj *aobject) *objectAppender {
	appender := new(objectAppender)
	appender.obj = aobj
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
	return appender.obj.reserved.Load() < appender.obj.meta.Load().GetSchema().Extra.BlockMaxRows
}

func (appender *objectAppender) Close() {
	appender.obj.Unref()
}

func (appender *objectAppender) PPString() string {
	return appender.obj.PPString(common.PPL1, 0, "", 0)
}

func (appender *objectAppender) IsSameColumns(other any) bool {
	n := appender.obj.PinNode()
	defer n.Unref()
	return n.MustMNode().writeSchema.IsSameColumns(other.(*catalog.Schema))
}

func (appender *objectAppender) PrepareAppend(
	ctx context.Context,
	isMergeCompact bool,
	rows uint32,
	txn txnif.AsyncTxn) (node txnif.AppendNode, created bool, n uint32, err error) {
	// GetSchema takes the table read lock. Do not acquire it while holding
	// appendMVCC's write lock: catalog publication can hold the table lock
	// while waiting for an ObjectList scan that reads appendMVCC.
	blockMaxRows := appender.obj.meta.Load().GetSchema().Extra.BlockMaxRows
	appender.obj.appendMVCC.LockForAppend()
	sealed := false
	defer func() {
		appender.obj.appendMVCC.UnlockForAppend()
		if sealed {
			appender.obj.appendMVCC.NotifyFinalizedMax()
		}
	}()
	if err = ctx.Err(); err != nil {
		return
	}
	start := appender.obj.reserved.Load()
	if appender.obj.frozen.Load() {
		return
	}
	left := blockMaxRows - start
	if left == 0 {
		appender.obj.frozen.Store(true)
		appender.obj.appendMVCC.SealLocked()
		sealed = true
		return
	}
	if rows > left {
		n = left
	} else {
		n = rows
	}
	node, created = appender.obj.appendMVCC.AddAppendNodeLocked(
		txn,
		start,
		start+n)
	if isMergeCompact {
		node.SetIsMergeCompact()
	}
	reserved := start + n
	appender.obj.reserved.Store(reserved)
	if reserved == blockMaxRows {
		// Seal under the same MVCC write lock as the final AppendNode
		// insertion. This transaction is still active, so max-commit
		// publication waits for its commit or rollback.
		appender.obj.frozen.Store(true)
		appender.obj.appendMVCC.MarkSealedLocked()
		sealed = true
	}
	return
}

func (appender *objectAppender) ReserveAppend(offset, rows uint32) {
	n := appender.obj.PinNode()
	defer n.Unref()
	appender.obj.appendMVCC.LockForAppend()
	defer appender.obj.appendMVCC.UnlockForAppend()
	// The rows are already allocated to this append: a reservation that
	// cannot be materialized leaves the node inconsistent.
	if err := n.MustMNode().ReserveRowsLocked(offset + rows); err != nil {
		panic(err)
	}
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
	appender.obj.appendMVCC.LockForAppend()
	defer appender.obj.appendMVCC.UnlockForAppend()
	if from, err = node.ApplyAppendLocked(bat, nil); err != nil {
		return
	}

	if key, _ := node.appendPrimaryKey(bat, node.writeSchema); key != nil {
		if err = node.pkIndex.BatchUpsert(key, from); err != nil {
			panic(err)
		}
	}
	return
}

func (appender *objectAppender) ApplyAppendAt(
	bat *containers.Batch,
	offset uint32,
	writeSchema any,
) (err error) {
	n := appender.obj.PinNode()
	defer n.Unref()
	node := n.MustMNode()
	appender.obj.appendMVCC.LockForAppend()
	defer appender.obj.appendMVCC.UnlockForAppend()
	source := node.writeSchema
	if writeSchema != nil {
		source = writeSchema.(*catalog.Schema)
	}
	var key containers.Vector
	if err = node.ApplyAppendAtLocked(bat, offset, source, func() error {
		var pkIdx int
		key, pkIdx = node.appendPrimaryKey(bat, source)
		if key != nil {
			return node.removeOverwrittenPKsLocked(key.Length(), offset, pkIdx)
		}
		return nil
	}); err != nil {
		return err
	}
	if key != nil {
		if err = node.pkIndex.BatchUpsert(key, int(offset)); err != nil {
			panic(err)
		}
	}
	return nil
}

func (node *memoryNode) appendPrimaryKey(bat *containers.Batch, source *catalog.Schema) (containers.Vector, int) {
	schema := node.writeSchema
	if !schema.HasPK() || schema.IsSecondaryIndexTable() {
		return nil, 0
	}
	pk := schema.GetSingleSortKey()
	if idx, ok := source.SeqnumMap[pk.SeqNum]; ok && pk.IsRealPrimary() {
		return bat.GetVectorByName(source.ColDefs[idx].Name), pk.Idx
	}
	return nil, 0
}

func (node *memoryNode) removeOverwrittenPKsLocked(rows int, offset uint32, pkIdx int) error {
	oldVec := node.data.Vecs[pkIdx]
	end := min(int(offset)+rows, oldVec.Length())
	for row := int(offset); row < end; row++ {
		if oldVec.IsNull(uint64(row)) {
			continue
		}
		if err := node.pkIndex.DeleteAt(oldVec.GetValue(row), uint32(row)); err != nil {
			return err
		}
	}
	return nil
}
