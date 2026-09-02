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
	"slices"

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
	isMergeCompact bool,
	rows uint32,
	txn txnif.AsyncTxn) (node txnif.AppendNode, created bool, n uint32, err error) {
	appender.obj.appendMVCC.LockForAppend()
	defer appender.obj.appendMVCC.UnlockForAppend()
	start := appender.obj.reserved.Load()
	left := appender.obj.meta.Load().GetSchema().Extra.BlockMaxRows - start
	if left == 0 {
		// n = rows
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
	appender.obj.reserved.Store(start + n)
	return
}

func (appender *objectAppender) ReserveAppend(offset, rows uint32) {
	n := appender.obj.PinNode()
	defer n.Unref()
	appender.obj.appendMVCC.LockForAppend()
	defer appender.obj.appendMVCC.UnlockForAppend()
	n.MustMNode().ReserveRowsLocked(offset + rows)
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
	from, err = node.ApplyAppendLocked(bat)

	schema := node.writeSchema
	for _, colDef := range schema.ColDefs {
		if colDef.IsPhyAddr() {
			continue
		}
		if colDef.IsRealPrimary() && !schema.IsSecondaryIndexTable() {
			if err = node.pkIndex.BatchUpsert(
				bat.Vecs[colDef.Idx].GetDownstreamVector(), from); err != nil {
				panic(err)
			}
		}
	}
	return
}

func (appender *objectAppender) ApplyAppendAt(
	bat *containers.Batch,
	offset uint32,
	txn txnif.AsyncTxn,
) (err error) {
	n := appender.obj.PinNode()
	defer n.Unref()
	node := n.MustMNode()
	appender.obj.appendMVCC.LockForAppend()
	defer appender.obj.appendMVCC.UnlockForAppend()
	schema := node.writeSchema
	oldRows, _ := node.Rows()
	for _, colDef := range schema.ColDefs {
		if !colDef.IsRealPrimary() || schema.IsSecondaryIndexTable() {
			continue
		}
		srcPos := slices.Index(bat.Attrs, colDef.Name)
		if srcPos < 0 {
			continue
		}
		// A replayed append may be the first payload for an object.  In that
		// case the memory node has no data vectors yet, or this column has not
		// been materialized.  There are no old rows to remove from the PK index
		// in either case.
		if oldRows == 0 || node.data == nil || colDef.Idx >= len(node.data.Vecs) || node.data.Vecs[colDef.Idx] == nil {
			continue
		}
		oldVec := node.data.Vecs[colDef.Idx]
		for row := 0; row < bat.Length() && int(offset)+row < int(oldRows); row++ {
			physicalRow := int(offset) + row
			if oldVec.IsNull(physicalRow) {
				continue
			}
			if err = node.pkIndex.DeleteAt(oldVec.Get(physicalRow), uint32(physicalRow)); err != nil {
				return err
			}
		}
	}
	if err = node.ApplyAppendAtLocked(bat, offset); err != nil {
		return err
	}
	for _, colDef := range schema.ColDefs {
		if colDef.IsPhyAddr() {
			continue
		}
		if colDef.IsRealPrimary() && !schema.IsSecondaryIndexTable() {
			srcPos := slices.Index(bat.Attrs, colDef.Name)
			if srcPos < 0 {
				continue
			}
			if err = node.pkIndex.BatchUpsert(
				bat.Vecs[srcPos].GetDownstreamVector(), int(offset)); err != nil {
				panic(err)
			}
		}
	}
	return nil
}
