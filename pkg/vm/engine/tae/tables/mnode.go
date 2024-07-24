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
	"fmt"

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

type memoryNode struct {
	object      *aobject
	writeSchema *catalog.Schema
	data        *containers.Batch
	appendMVCC  *updates.AppendMVCCHandle
	offset      uint16

	//index for primary key : Art tree + ZoneMap.
	pkIndex *indexwrapper.MutIndex
}

func newMemoryNode(object *aobject, offset uint16) *memoryNode {
	impl := new(memoryNode)
	impl.object = object
	impl.offset = offset

	// Get the lastest schema, it will not be modified, so just keep the pointer
	schema := object.meta.Load().GetSchemaLocked()
	impl.writeSchema = schema
	impl.appendMVCC = updates.NewAppendMVCCHandle(object.meta.Load(), object.RWMutex, offset)
	impl.appendMVCC.SetAppendListener(object.OnApplyAppend)
	// impl.data = containers.BuildBatchWithPool(
	// 	schema.AllNames(), schema.AllTypes(), 0, object.rt.VectorPool.Memtable,
	// )
	impl.initPKIndex(schema)
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
	mvcc := node.appendMVCC
	logutil.Debugf("Releasing Memorynode BLK-%s", node.object.meta.Load().ID().String())
	if node.data != nil {
		node.data.Close()
		node.data = nil
	}
	if node.pkIndex != nil {
		node.pkIndex.Close()
		node.pkIndex = nil
	}
	node.object = nil
	mvcc.ReleaseAppends()
}

func (node *memoryNode) IsPersisted() bool { return false }

func (node *memoryNode) doBatchDedup(
	ctx context.Context,
	keys containers.Vector,
	keysZM index.ZM,
	skipFn func(row uint32) error,
	bf objectio.BloomFilter,
	txn txnif.TxnReader,
) (sels *roaring.Bitmap, err error) {
	needWait, txnToWait := node.appendMVCC.NeedWaitCommittingLocked(txn)
	if needWait {
		node.object.RUnlock()
		txnToWait.GetTxnState(true)
		node.object.RLock()
	}
	return node.pkIndex.BatchDedup(ctx, keys.GetDownstreamVector(), keysZM, skipFn, bf)
}

func (node *memoryNode) ContainsKey(ctx context.Context, key any, _ uint32) (ok bool, err error) {
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
	blkID uint16,
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

func (node *memoryNode) Rows() (uint32, error) {
	if node.data == nil {
		return 0, nil
	}
	return uint32(node.data.Length()), nil
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
	vec = data.CloneWindowWithPool(int(from), int(to-from), node.object.rt.VectorPool.Transient)
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
	inner := node.data.CloneWindowWithPool(int(from), int(to-from), node.object.rt.VectorPool.Transient)
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
			vec = node.data.Vecs[idx].CloneWindowWithPool(int(from), int(to-from), node.object.rt.VectorPool.Transient)
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
		objectio.NewBlockidWithObjectID(node.object.meta.Load().ID(), node.offset),
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
) (blkID uint16, row uint32, err error) {
	rows, err := node.GetRowsByKey(filter.Val)
	if err != nil && !moerr.IsMoErrCode(err, moerr.ErrNotFound) {
		return
	}

	waitFn := func(n *updates.AppendNode) {
		txn := n.Txn
		if txn != nil {
			node.object.RUnlock()
			txn.GetTxnState(true)
			node.object.RLock()
		}
	}
	if anyWaitable := node.appendMVCC.CollectUncommittedANodesPreparedBefore(
		txn.GetStartTS(),
		waitFn); anyWaitable {
		rows, err = node.GetRowsByKey(filter.Val)
		if err != nil {
			return
		}
	}

	for i := len(rows) - 1; i >= 0; i-- {
		row = rows[i]
		appendnode := node.appendMVCC.GetAppendNodeByRow(row)
		needWait, waitTxn := appendnode.NeedWaitCommitting(txn.GetStartTS())
		if needWait {
			node.object.RUnlock()
			waitTxn.GetTxnState(true)
			node.object.RLock()
		}
		if appendnode.IsAborted() || !appendnode.IsVisible(txn) {
			continue
		}
		objMVCC := node.object.tryGetMVCC()
		if objMVCC == nil {
			return
		}
		var deleted bool
		deleted, err = objMVCC.IsDeletedLocked(row, txn, node.offset)
		if err != nil {
			return
		}
		if !deleted {
			return
		}
	}
	return 0, 0, moerr.NewNotFoundNoCtx()
}

func (node *memoryNode) BatchDedupLocked(
	ctx context.Context,
	txn txnif.TxnReader,
	isCommitting bool,
	keys containers.Vector,
	keysZM index.ZM,
	rowmask *roaring.Bitmap,
	bf objectio.BloomFilter,
) (err error) {
	var dupRow uint32
	_, err = node.doBatchDedup(
		ctx,
		keys,
		keysZM,
		node.checkConflictAndDupClosure(txn, isCommitting, &dupRow, rowmask),
		bf,
		txn)

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
		appendnode := node.appendMVCC.GetAppendNodeByRow(row)
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
		objMVCC := node.object.tryGetMVCC()
		if objMVCC == nil {
			*dupRow = row
			return moerr.GetOkExpectedDup()
		}
		mvcc := objMVCC.TryGetDeleteChain(node.offset)
		if mvcc == nil {
			*dupRow = row
			return moerr.GetOkExpectedDup()
		}
		deleteNode := mvcc.GetDeleteNodeByRow(row)
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
			node.object.RUnlock()
			txn.GetTxnState(true)
			node.object.RLock()
		}
	} else {
		needWait, txn := n.NeedWaitCommitting(txn.GetStartTS())
		if needWait {
			node.object.RUnlock()
			txn.GetTxnState(true)
			node.object.RLock()
		}
	}
	if err = n.CheckConflict(txn); err != nil {
		return
	}
	if isCommitting {
		visible = n.IsCommitted() || n.IsSameTxn(txn)
	} else {
		visible = n.IsVisible(txn)
	}
	return
}

func (node *memoryNode) CollectAppendInRange(
	start, end types.TS, withAborted bool, mp *mpool.MPool,
) (batWithVer *containers.BatchWithVersion, err error) {
	minRow, maxRow, commitTSVec, abortVec, abortedMap :=
		node.appendMVCC.CollectAppendLocked(start, end, mp)
	if commitTSVec == nil || abortVec == nil {
		return nil, nil
	}
	batWithVer, err = node.GetDataWindowOnWriteSchema(minRow, maxRow, mp)
	if err != nil {
		return nil, err
	}

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
) (view *containers.Batch, err error) {
	node.object.RLock()
	defer node.object.RUnlock()
	maxRow, visible, deSels, err := node.appendMVCC.GetVisibleRowLocked(ctx, txn)
	if !visible || err != nil {
		// blk.RUnlock()
		return
	}
	data, err := node.GetDataWindow(readSchema, colIdxes, 0, maxRow, mp)
	if err != nil {
		return
	}
	view = containers.NewBatch()
	for i, colIdx := range colIdxes {
		view.AddVector(readSchema.ColDefs[colIdx].Name, data.Vecs[i])
	}
	if skipDeletes {
		return
	}

	err = node.object.fillInMemoryDeletesLocked(txn, node.offset, &view.Deletes, node.object.RWMutex)
	if err != nil {
		return
	}
	if !deSels.IsEmpty() {
		if view.Deletes != nil {
			view.Deletes.Or(deSels)
		} else {
			view.Deletes = deSels
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
) (view *containers.Batch, err error) {
	node.object.RLock()
	defer node.object.RUnlock()
	maxRow, visible, deSels, err := node.appendMVCC.GetVisibleRowLocked(context.TODO(), txn)
	if !visible || err != nil {
		return
	}

	view = containers.NewBatch()
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
	view.AddVector(readSchema.ColDefs[col].Name, data)
	if skipDeletes {
		return
	}

	err = node.object.fillInMemoryDeletesLocked(txn, node.offset, &view.Deletes, node.object.RWMutex)
	if err != nil {
		return
	}
	if deSels != nil && !deSels.IsEmpty() {
		if view.Deletes != nil {
			view.Deletes.Or(deSels)
		} else {
			view.Deletes = deSels
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
	node.object.RLock()
	deleted := false
	objMVCC := node.object.tryGetMVCC()
	if objMVCC != nil {
		mvcc := objMVCC.TryGetDeleteChain(node.offset)
		if mvcc != nil {
			deleted, err = mvcc.IsDeletedLocked(uint32(row), txn)
		}
	}
	node.object.RUnlock()
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
	isNull = view.Vecs[0].IsNull(row)
	if !isNull {
		v = view.Vecs[0].Get(row)
	}
	return
}

func (node *memoryNode) allRowsCommittedBefore(ts types.TS) bool {
	node.object.RLock()
	defer node.object.RUnlock()
	return node.appendMVCC.AllAppendsCommittedBefore(ts)
}

var _ NodeT = (*objectMemoryNode)(nil)

type objectMemoryNode struct {
	obj *aobject
	common.RefHelper
	blkMemoryNodes []*memoryNode
	writeSchema    *catalog.Schema
}

func newObjectMemoryNode(obj *aobject) *objectMemoryNode {
	impl := &objectMemoryNode{
		blkMemoryNodes: make([]*memoryNode, 0),
		writeSchema:    obj.meta.Load().GetSchemaLocked(),
		obj:            obj,
	}
	impl.OnZeroCB = impl.close
	return impl
}
func (node *objectMemoryNode) close() {
	for _, n := range node.blkMemoryNodes {
		n.close()
	}
}
func (node *objectMemoryNode) getLastNode() *memoryNode {
	return node.blkMemoryNodes[len(node.blkMemoryNodes)-1]
}
func (node *objectMemoryNode) getMemoryNode(blkID uint16) *memoryNode {
	return node.blkMemoryNodes[blkID]
}
func (node *objectMemoryNode) IsPersisted() bool { return false }
func (node *objectMemoryNode) getOrCreateNode(blkID uint16) *memoryNode {
	if len(node.blkMemoryNodes) > int(blkID) {
		return node.blkMemoryNodes[blkID]
	}
	if len(node.blkMemoryNodes) == int(blkID) {
		return node.registerNodeLocked()
	}
	panic(fmt.Sprintf("invalid blkID, current blk count %d, blkID %d", len(node.blkMemoryNodes), blkID))
}
func (node *objectMemoryNode) PrepareAppend(rows uint32) (n uint32, err error) {
	return node.getLastNode().PrepareAppend(rows)
}
func (node *objectMemoryNode) ApplyAppend(
	bat *containers.Batch,
	txn txnif.AsyncTxn,
	blkOffset uint16,
) (from int, err error) {
	return node.getOrCreateNode(blkOffset).ApplyAppend(bat, txn)
}
func (node *objectMemoryNode) GetDataWindow(
	blkID uint16, readSchema *catalog.Schema, colIdxes []int, from, to uint32, mp *mpool.MPool,
) (bat *containers.Batch, err error) {
	return node.getMemoryNode(blkID).GetDataWindow(readSchema, colIdxes, from, to, mp)
}

//	func (node *objectMemoryNode) GetValueByRow(blkID uint16, readSchema *catalog.Schema, row, col int) (v any, isNull bool) {
//		return node.getMemoryNode(blkID).GetValueByRow(readSchema, row, col)
//	}
//
//	func (node *objectMemoryNode) GetRowsByKey(blkID uint16, key any) (rows []uint32, err error) {
//		return node.getMemoryNode(blkID).GetRowsByKey(key)
//	}
func (node *objectMemoryNode) registerNodeLocked() *memoryNode {
	if !node.checkBlockCountLocked() {
		panic("logic error")
	}
	blkNode := newMemoryNode(node.obj, uint16(len(node.blkMemoryNodes)))
	node.blkMemoryNodes = append(node.blkMemoryNodes, blkNode)
	return blkNode
}
func (node *objectMemoryNode) checkBlockCountLocked() bool {
	// return len(node.blkMemoryNodes) < int(node.writeSchema.ObjectMaxBlocks)
	size := 0
	for _, blkNode := range node.blkMemoryNodes {
		if blkNode.data != nil {
			size += blkNode.data.ApproxSize()
		}
	}
	return size < node.writeSchema.AObjectMaxSize
}

// Only check block count.
// Check rows in appender.
func (node *objectMemoryNode) IsAppendable() bool {
	node.obj.RLock()
	defer node.obj.RUnlock()
	return node.checkBlockCountLocked()
}
func (node *objectMemoryNode) BatchDedup(
	ctx context.Context,
	txn txnif.TxnReader,
	isCommitting bool,
	keys containers.Vector,
	keysZM index.ZM,
	rowmask *roaring.Bitmap,
	startBLKID uint16,
	bf objectio.BloomFilter,
) (err error) {
	node.obj.RLock()
	defer node.obj.RUnlock()
	for i := int(startBLKID); i < len(node.blkMemoryNodes); i++ {
		mnode := node.blkMemoryNodes[i]
		err = mnode.BatchDedupLocked(ctx, txn, isCommitting, keys, keysZM, rowmask, bf)
		if err != nil {
			return
		}
	}
	return
}
func (node *objectMemoryNode) ContainsKey(ctx context.Context, key any, blkID uint32) (ok bool, err error) {
	panic("todo")
}

func (node *objectMemoryNode) Rows() (uint32, error) {
	total := uint32(0)
	for _, node := range node.blkMemoryNodes {
		row, err := node.Rows()
		if err != nil {
			return 0, err
		}
		total += row
	}
	return total, nil
}

func (node *objectMemoryNode) GetRowByFilter(ctx context.Context, txn txnif.TxnReader, filter *handle.Filter, mp *mpool.MPool) (bid uint16, row uint32, err error) {
	node.obj.RLock()
	defer node.obj.RUnlock()
	for blkOffset, node := range node.blkMemoryNodes {
		_, row, err = node.GetRowByFilter(ctx, txn, filter, mp)
		if moerr.IsMoErrCode(err, moerr.ErrNotFound) {
			continue
		}
		if err != nil {
			return
		}
		bid = uint16(blkOffset)
		return
	}
	return 0, 0, moerr.NewNotFoundNoCtx()
}
func (node *objectMemoryNode) CollectAppendInRange(
	start, end types.TS, withAborted bool, mp *mpool.MPool,
) (batWithVer *containers.BatchWithVersion, err error) {
	node.obj.RLock()
	defer node.obj.RUnlock()
	for _, blkNode := range node.blkMemoryNodes {
		bat, err := blkNode.CollectAppendInRange(start, end, withAborted, mp)
		if err != nil {
			return nil, err
		}
		if batWithVer == nil {
			batWithVer = bat
		} else {
			batWithVer.Batch.Extend(bat.Batch)
			bat.Batch.Close()
		}
	}
	return
}
func (node *objectMemoryNode) CollectAppendInRangeWithBlockID(
	blkOffset uint16, start, end types.TS, withAborted bool, mp *mpool.MPool,
) (batWithVer *containers.BatchWithVersion, err error) {
	return node.getMemoryNode(blkOffset).CollectAppendInRange(start, end, withAborted, mp)
}
func (node *objectMemoryNode) EstimateMemSize() int {
	size := 0
	for _, blkNode := range node.blkMemoryNodes {
		size += blkNode.EstimateMemSize()
	}
	return size
}
func (node *objectMemoryNode) getInMemoryValue(
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	blkID uint16,
	row, col int,
	mp *mpool.MPool,
) (v any, isNull bool, err error) {
	return node.getMemoryNode(blkID).getInMemoryValue(txn, readSchema, row, col, mp)
}
func (node *objectMemoryNode) resolveInMemoryColumnData(
	blkID uint16,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	col int,
	skipDeletes bool,
	mp *mpool.MPool,
) (view *containers.Batch, err error) {
	return node.getMemoryNode(blkID).resolveInMemoryColumnData(txn, readSchema, col, skipDeletes, mp)
}
func (node *objectMemoryNode) allRowsCommittedBefore(ts types.TS) bool {
	return node.getLastNode().allRowsCommittedBefore(ts)
}
func (node *objectMemoryNode) resolveInMemoryColumnDatas(
	ctx context.Context,
	blkID uint16,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	colIdxes []int,
	skipDeletes bool,
	mp *mpool.MPool,
) (view *containers.Batch, err error) {
	return node.getMemoryNode(blkID).resolveInMemoryColumnDatas(ctx, txn, readSchema, colIdxes, skipDeletes, mp)
}
func (node *objectMemoryNode) getwrteSchema() *catalog.Schema {
	return node.writeSchema
}
func (node *objectMemoryNode) blockCnt() int {
	return len(node.blkMemoryNodes)
}
