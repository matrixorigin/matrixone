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

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/indexwrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
)

var _ NodeT = (*memoryNode)(nil)

type memoryNode struct {
	common.RefHelper
	block       *baseBlock
	writeSchema *catalog.Schema
	data        *containers.Batch

	//index for primary key : Art tree + ZoneMap.
	pkIndex *indexwrapper.MutIndex
}

func newMemoryNode(block *baseBlock) *memoryNode {
	impl := new(memoryNode)
	impl.block = block

	// Get the lastest schema, it will not be modified, so just keep the pointer
	schema := block.meta.GetSchema()
	impl.writeSchema = schema
	// impl.data = containers.BuildBatchWithPool(
	// 	schema.AllNames(), schema.AllTypes(), 0, block.rt.VectorPool.Memtable,
	// )
	impl.initPKIndex(schema)
	impl.OnZeroCB = impl.close
	return impl
}

func (node *memoryNode) mustData() *containers.Batch {
	if node.data != nil {
		return node.data
	}
	schema := node.writeSchema
	opts := containers.Options{
		Allocator: common.MutMemAllocator,
	}
	node.data = containers.BuildBatch(
		schema.AllNames(), schema.AllTypes(), opts,
	)
	return node.data
}

func (node *memoryNode) initPKIndex(schema *catalog.Schema) {
	if !schema.HasPK() {
		return
	}
	pkDef := schema.GetSingleSortKey()
	node.pkIndex = indexwrapper.NewMutIndex(pkDef.Type)
}

func (node *memoryNode) close() {
	mvcc := node.block.mvcc
	logutil.Debugf("Releasing Memorynode BLK-%s", node.block.meta.ID.String())
	if node.data != nil {
		node.data.Close()
		node.data = nil
	}
	if node.pkIndex != nil {
		node.pkIndex.Close()
		node.pkIndex = nil
	}
	node.block = nil
	mvcc.ReleaseAppends()
}

func (node *memoryNode) IsPersisted() bool { return false }

func (node *memoryNode) doBatchDedup(
	ctx context.Context,
	keys containers.Vector,
	keysZM index.ZM,
	skipFn func(row uint32) error,
	bf objectio.BloomFilter,
) (sels *roaring.Bitmap, err error) {
	return node.pkIndex.BatchDedup(ctx, keys.GetDownstreamVector(), keysZM, skipFn, bf)
}

func (node *memoryNode) ContainsKey(ctx context.Context, key any) (ok bool, err error) {
	if err = node.pkIndex.Dedup(ctx, key, nil); err != nil {
		return
	}
	if !moerr.IsMoErrCode(err, moerr.OkExpectedPossibleDup) {
		return
	}
	ok = true
	err = nil
	return
}

func (node *memoryNode) GetValueByRow(readSchema *catalog.Schema, row, col int) (v any, isNull bool) {
	idx, ok := node.writeSchema.SeqnumMap[readSchema.ColDefs[col].SeqNum]
	if !ok {
		// TODO(aptend): use default value
		return nil, true
	}
	data := node.mustData()
	vec := data.Vecs[idx]
	return vec.Get(row), vec.IsNull(row)
}

func (node *memoryNode) Foreach(
	readSchema *catalog.Schema,
	colIdx int,
	op func(v any, isNull bool, row int) error,
	sels []uint32,
	mp *mpool.MPool,
) error {
	if node.data == nil {
		return nil
	}
	idx, ok := node.writeSchema.SeqnumMap[readSchema.ColDefs[colIdx].SeqNum]
	if !ok {
		v := containers.NewConstNullVector(readSchema.ColDefs[colIdx].Type, int(node.data.Length()), mp)
		for _, row := range sels {
			val := v.Get(int(row))
			isNull := v.IsNull(int(row))
			err := op(val, isNull, int(row))
			if err != nil {
				return err
			}
		}
		return nil
	}
	for _, row := range sels {
		val := node.data.Vecs[idx].Get(int(row))
		isNull := node.data.Vecs[idx].IsNull(int(row))
		err := op(val, isNull, int(row))
		if err != nil {
			return err
		}
	}
	return nil
}

func (node *memoryNode) GetRowsByKey(key any) (rows []uint32, err error) {
	return node.pkIndex.GetActiveRow(key)
}

func (node *memoryNode) Rows() uint32 {
	if node.data == nil {
		return 0
	}
	return uint32(node.data.Length())
}

func (node *memoryNode) EstimateMemSize() int {
	if node.data == nil {
		return 0
	}
	return node.data.ApproxSize()
}

func (node *memoryNode) GetColumnDataWindow(
	readSchema *catalog.Schema,
	from uint32,
	to uint32,
	col int,
	mp *mpool.MPool,
) (vec containers.Vector, err error) {
	idx, ok := node.writeSchema.SeqnumMap[readSchema.ColDefs[col].SeqNum]
	if !ok {
		return containers.NewConstNullVector(readSchema.ColDefs[col].Type, int(to-from), mp), nil
	}
	if node.data == nil {
		vec = containers.MakeVector(node.writeSchema.AllTypes()[idx], mp)
		return
	}
	data := node.data.Vecs[idx]
	vec = data.CloneWindowWithPool(int(from), int(to-from), node.block.rt.VectorPool.Transient)
	// vec = data.CloneWindow(int(from), int(to-from), common.MutMemAllocator)
	return
}

func (node *memoryNode) GetDataWindowOnWriteSchema(
	from, to uint32, mp *mpool.MPool,
) (bat *containers.BatchWithVersion, err error) {
	if node.data == nil {
		schema := node.writeSchema
		opts := containers.Options{
			Allocator: mp,
		}
		inner := containers.BuildBatch(
			schema.AllNames(), schema.AllTypes(), opts,
		)
		return &containers.BatchWithVersion{
			Version:    node.writeSchema.Version,
			NextSeqnum: uint16(node.writeSchema.Extra.NextColSeqnum),
			Seqnums:    node.writeSchema.AllSeqnums(),
			Batch:      inner,
		}, nil
	}
	inner := node.data.CloneWindowWithPool(int(from), int(to-from), node.block.rt.VectorPool.Transient)
	// inner := node.data.CloneWindow(int(from), int(to-from), common.MutMemAllocator)
	bat = &containers.BatchWithVersion{
		Version:    node.writeSchema.Version,
		NextSeqnum: uint16(node.writeSchema.Extra.NextColSeqnum),
		Seqnums:    node.writeSchema.AllSeqnums(),
		Batch:      inner,
	}
	return
}

func (node *memoryNode) GetDataWindow(
	readSchema *catalog.Schema,
	colIdxes []int,
	from, to uint32,
	mp *mpool.MPool,
) (bat *containers.Batch, err error) {
	if node.data == nil {
		schema := node.writeSchema
		opts := containers.Options{
			Allocator: mp,
		}
		bat = containers.BuildBatch(
			schema.AllNames(), schema.AllTypes(), opts,
		)
		return
	}

	// manually clone data
	bat = containers.NewBatchWithCapacity(len(colIdxes))
	if node.data.Deletes != nil {
		bat.Deletes = bat.WindowDeletes(int(from), int(to-from), false)
	}
	for _, colIdx := range colIdxes {
		colDef := readSchema.ColDefs[colIdx]
		idx, ok := node.writeSchema.SeqnumMap[colDef.SeqNum]
		var vec containers.Vector
		if !ok {
			vec = containers.NewConstNullVector(colDef.Type, int(to-from), mp)
		} else {
			vec = node.data.Vecs[idx].CloneWindowWithPool(int(from), int(to-from), node.block.rt.VectorPool.Transient)
		}
		bat.AddVector(colDef.Name, vec)
	}
	return
}

func (node *memoryNode) PrepareAppend(rows uint32) (n uint32, err error) {
	var length uint32
	if node.data == nil {
		length = 0
	} else {
		length = uint32(node.data.Length())
	}

	left := node.writeSchema.BlockMaxRows - length

	if left == 0 {
		err = moerr.NewInternalErrorNoCtx("not appendable")
		return
	}
	if rows > left {
		n = left
	} else {
		n = rows
	}
	return
}

func (node *memoryNode) FillPhyAddrColumn(startRow, length uint32) (err error) {
	var col *vector.Vector
	if col, err = objectio.ConstructRowidColumn(
		&node.block.meta.ID,
		startRow,
		length,
		common.MutMemAllocator,
	); err != nil {
		return
	}
	err = node.mustData().Vecs[node.writeSchema.PhyAddrKey.Idx].ExtendVec(col)
	col.Free(common.MutMemAllocator)
	return
}

func (node *memoryNode) ApplyAppend(
	bat *containers.Batch,
	txn txnif.AsyncTxn) (from int, err error) {
	schema := node.writeSchema
	from = int(node.mustData().Length())
	for srcPos, attr := range bat.Attrs {
		def := schema.ColDefs[schema.GetColIdx(attr)]
		destVec := node.data.Vecs[def.Idx]
		destVec.Extend(bat.Vecs[srcPos])
	}
	return
}

func (node *memoryNode) GetRowByFilter(
	ctx context.Context,
	txn txnif.TxnReader,
	filter *handle.Filter,
	mp *mpool.MPool,
) (row uint32, err error) {
	node.block.RLock()
	defer node.block.RUnlock()
	rows, err := node.GetRowsByKey(filter.Val)
	if err != nil && !moerr.IsMoErrCode(err, moerr.ErrNotFound) {
		return
	}

	waitFn := func(n *updates.AppendNode) {
		txn := n.Txn
		if txn != nil {
			node.block.RUnlock()
			txn.GetTxnState(true)
			node.block.RLock()
		}
	}
	if anyWaitable := node.block.mvcc.CollectUncommittedANodesPreparedBefore(
		txn.GetStartTS(),
		waitFn); anyWaitable {
		rows, err = node.GetRowsByKey(filter.Val)
		if err != nil {
			return
		}
	}

	for i := len(rows) - 1; i >= 0; i-- {
		row = rows[i]
		appendnode := node.block.mvcc.GetAppendNodeByRow(row)
		needWait, waitTxn := appendnode.NeedWaitCommitting(txn.GetStartTS())
		if needWait {
			node.block.RUnlock()
			waitTxn.GetTxnState(true)
			node.block.RLock()
		}
		if appendnode.IsAborted() || !appendnode.IsVisible(txn) {
			continue
		}
		var deleted bool
		deleted, err = node.block.mvcc.IsDeletedLocked(row, txn, node.block.mvcc.RWMutex)
		if err != nil {
			return
		}
		if !deleted {
			return
		}
	}
	return 0, moerr.NewNotFoundNoCtx()
}

func (node *memoryNode) BatchDedup(
	ctx context.Context,
	txn txnif.TxnReader,
	isCommitting bool,
	keys containers.Vector,
	keysZM index.ZM,
	rowmask *roaring.Bitmap,
	bf objectio.BloomFilter,
) (err error) {
	var dupRow uint32
	node.block.RLock()
	defer node.block.RUnlock()
	_, err = node.doBatchDedup(
		ctx,
		keys,
		keysZM,
		node.checkConflictAndDupClosure(txn, isCommitting, &dupRow, rowmask),
		bf)

	// definitely no duplicate
	if err == nil || !moerr.IsMoErrCode(err, moerr.OkExpectedDup) {
		return
	}
	def := node.writeSchema.GetSingleSortKey()
	v, isNull := node.GetValueByRow(node.writeSchema, int(dupRow), def.Idx)
	entry := common.TypeStringValue(*keys.GetType(), v, isNull)
	return moerr.NewDuplicateEntryNoCtx(entry, def.Name)
}

func (node *memoryNode) checkConflictAndDupClosure(
	txn txnif.TxnReader,
	isCommitting bool,
	dupRow *uint32,
	rowmask *roaring.Bitmap,
) func(row uint32) error {
	return func(row uint32) (err error) {
		if rowmask != nil && rowmask.Contains(row) {
			return nil
		}
		appendnode := node.block.mvcc.GetAppendNodeByRow(row)
		var visible bool
		if visible, err = node.checkConflictAandVisibility(
			appendnode,
			isCommitting,
			txn); err != nil {
			return
		}
		if appendnode.IsAborted() || !visible {
			return nil
		}
		deleteNode := node.block.mvcc.GetDeleteNodeByRow(row)
		if deleteNode == nil {
			*dupRow = row
			return moerr.GetOkExpectedDup()
		}

		if visible, err = node.checkConflictAandVisibility(
			deleteNode,
			isCommitting,
			txn); err != nil {
			return
		}
		if deleteNode.IsAborted() || !visible {
			return moerr.GetOkExpectedDup()
		}
		return nil
	}
}

func (node *memoryNode) checkConflictAandVisibility(
	n txnif.BaseMVCCNode,
	isCommitting bool,
	txn txnif.TxnReader,
) (visible bool, err error) {
	// if isCommitting check all nodes commit before txn.CommitTS(PrepareTS)
	// if not isCommitting check nodes commit before txn.StartTS
	if isCommitting {
		needWait := n.IsCommitting()
		if needWait {
			txn := n.GetTxn()
			node.block.mvcc.RUnlock()
			txn.GetTxnState(true)
			node.block.mvcc.RLock()
		}
	} else {
		needWait, txn := n.NeedWaitCommitting(txn.GetStartTS())
		if needWait {
			node.block.mvcc.RUnlock()
			txn.GetTxnState(true)
			node.block.mvcc.RLock()
		}
	}
	if err = n.CheckConflict(txn); err != nil {
		return
	}
	if isCommitting {
		visible = n.IsCommitted()
	} else {
		visible = n.IsVisible(txn)
	}
	return
}

func (node *memoryNode) CollectAppendInRange(
	start, end types.TS, withAborted bool, mp *mpool.MPool,
) (batWithVer *containers.BatchWithVersion, err error) {
	node.block.RLock()
	minRow, maxRow, commitTSVec, abortVec, abortedMap :=
		node.block.mvcc.CollectAppendLocked(start, end, mp)
	batWithVer, err = node.GetDataWindowOnWriteSchema(minRow, maxRow, mp)
	if err != nil {
		node.block.RUnlock()
		return nil, err
	}
	node.block.RUnlock()

	batWithVer.Seqnums = append(batWithVer.Seqnums, objectio.SEQNUM_COMMITTS)
	batWithVer.AddVector(catalog.AttrCommitTs, commitTSVec)
	if withAborted {
		batWithVer.Seqnums = append(batWithVer.Seqnums, objectio.SEQNUM_ABORT)
		batWithVer.AddVector(catalog.AttrAborted, abortVec)
	} else {
		abortVec.Close()
		batWithVer.Deletes = abortedMap
		batWithVer.Compact()
	}

	return
}

// Note: With PinNode Context
func (node *memoryNode) resolveInMemoryColumnDatas(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	colIdxes []int,
	skipDeletes bool,
	mp *mpool.MPool,
) (view *containers.BlockView, err error) {
	node.block.RLock()
	defer node.block.RUnlock()
	maxRow, visible, deSels, err := node.block.mvcc.GetVisibleRowLocked(ctx, txn)
	if !visible || err != nil {
		// blk.RUnlock()
		return
	}
	data, err := node.GetDataWindow(readSchema, colIdxes, 0, maxRow, mp)
	if err != nil {
		return
	}
	view = containers.NewBlockView()
	for i, colIdx := range colIdxes {
		view.SetData(colIdx, data.Vecs[i])
	}
	if skipDeletes {
		return
	}

	err = node.block.FillInMemoryDeletesLocked(txn, view.BaseView, node.block.RWMutex)
	if err != nil {
		return
	}
	if !deSels.IsEmpty() {
		if view.DeleteMask != nil {
			view.DeleteMask.Or(deSels)
		} else {
			view.DeleteMask = deSels
		}
	}
	return
}

// Note: With PinNode Context
func (node *memoryNode) resolveInMemoryColumnData(
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	col int,
	skipDeletes bool,
	mp *mpool.MPool,
) (view *containers.ColumnView, err error) {
	node.block.RLock()
	defer node.block.RUnlock()
	maxRow, visible, deSels, err := node.block.mvcc.GetVisibleRowLocked(context.TODO(), txn)
	if !visible || err != nil {
		return
	}

	view = containers.NewColumnView(col)
	var data containers.Vector
	if data, err = node.GetColumnDataWindow(
		readSchema,
		0,
		maxRow,
		col,
		mp,
	); err != nil {
		return
	}
	view.SetData(data)
	if skipDeletes {
		return
	}

	err = node.block.FillInMemoryDeletesLocked(txn, view.BaseView, node.block.RWMutex)
	if err != nil {
		return
	}
	if deSels != nil && !deSels.IsEmpty() {
		if view.DeleteMask != nil {
			view.DeleteMask.Or(deSels)
		} else {
			view.DeleteMask = deSels
		}
	}

	return
}

// With PinNode Context
func (node *memoryNode) getInMemoryValue(
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	row, col int,
	mp *mpool.MPool,
) (v any, isNull bool, err error) {
	node.block.RLock()
	deleted, err := node.block.mvcc.IsDeletedLocked(uint32(row), txn, node.block.RWMutex)
	node.block.RUnlock()
	if err != nil {
		return
	}
	if deleted {
		err = moerr.NewNotFoundNoCtx()
		return
	}
	view, err := node.resolveInMemoryColumnData(txn, readSchema, col, true, mp)
	if err != nil {
		return
	}
	defer view.Close()
	v, isNull = view.GetValue(row)
	return
}

func (node *memoryNode) allRowsCommittedBefore(ts types.TS) bool {
	node.block.RLock()
	defer node.block.RUnlock()
	return node.block.mvcc.AllAppendsCommittedBefore(ts)
}
