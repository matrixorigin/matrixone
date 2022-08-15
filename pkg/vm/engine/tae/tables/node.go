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
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type appendableNode struct {
	*buffer.Node
	file    file.Block
	block   *dataBlock
	data    *containers.Batch
	rows    uint32
	mgr     base.INodeManager
	flushTS atomic.Value
	//mu      struct {
	//	sync.RWMutex
	//	flushTS types.TS
	//}

	// ckpTs     uint64 // unused
	exception *atomic.Value
}

func newNode(mgr base.INodeManager, block *dataBlock, file file.Block) *appendableNode {
	flushTS, err := file.ReadTS()
	if err != nil {
		panic(err)
	}
	impl := new(appendableNode)
	impl.exception = new(atomic.Value)
	id := block.meta.AsCommonID()
	impl.Node = buffer.NewNode(impl, mgr, *id, uint64(catalog.EstimateBlockSize(block.meta, block.meta.GetSchema().BlockMaxRows)))
	impl.UnloadFunc = impl.OnUnload
	impl.LoadFunc = impl.OnLoad
	impl.UnloadableFunc = impl.CheckUnloadable
	impl.file = file
	impl.mgr = mgr
	impl.block = block
	impl.flushTS.Store(flushTS)
	impl.rows = file.ReadRows()
	mgr.RegisterNode(impl)
	return impl
}

func (node *appendableNode) TryPin() (base.INodeHandle, error) {
	return node.mgr.TryPin(node.Node, time.Second)
}

func (node *appendableNode) DoWithPin(do func() error) (err error) {
	h, err := node.TryPin()
	if err != nil {
		return
	}
	defer h.Close()
	err = do()
	return
}

func (node *appendableNode) Rows(txn txnif.AsyncTxn, coarse bool) uint32 {
	if coarse {
		node.block.mvcc.RLock()
		defer node.block.mvcc.RUnlock()
		return node.rows
	}
	// TODO: fine row count
	// 1. Load txn ts zonemap
	// 2. Calculate fine row count
	return 0
}

func (node *appendableNode) CheckUnloadable() bool {
	return !node.block.mvcc.HasActiveAppendNode()
}

func (node *appendableNode) GetDataCopy(maxRow uint32) (columns *containers.Batch, err error) {
	if exception := node.exception.Load(); exception != nil {
		err = exception.(error)
		return
	}
	node.block.RLock()
	columns = node.data.CloneWindow(0, int(maxRow), containers.DefaultAllocator)
	node.block.RUnlock()
	return
}

func (node *appendableNode) GetColumnDataCopy(
	maxRow uint32,
	colIdx int,
	buffer *bytes.Buffer) (vec containers.Vector, err error) {
	if exception := node.exception.Load(); exception != nil {
		err = exception.(error)
		return
	}
	node.block.RLock()
	if buffer != nil {
		win := node.data.Vecs[colIdx]
		if maxRow < uint32(node.data.Vecs[colIdx].Length()) {
			win = win.Window(0, int(maxRow))
		}
		vec = containers.CloneWithBuffer(win, buffer, containers.DefaultAllocator)
	} else {
		vec = node.data.Vecs[colIdx].CloneWindow(0, int(maxRow), containers.DefaultAllocator)
	}
	// logutil.Infof("src-length: %d, to copy-[0->%d]: %s", node.data.Vecs[colIdx].Length(), maxRow, node.block.meta.String())
	node.block.RUnlock()
	return
}

func (node *appendableNode) SetBlockMaxflushTS(ts types.TS) {
	node.flushTS.Store(ts)
}

func (node *appendableNode) GetBlockMaxflushTS() types.TS {
	return node.flushTS.Load().(types.TS)
}

func (node *appendableNode) OnLoad() {
	if exception := node.exception.Load(); exception != nil {
		logutil.Error("[Exception]", common.ExceptionField(exception))
		return
	}
	var err error
	schema := node.block.meta.GetSchema()
	opts := new(containers.Options)
	opts.Capacity = int(schema.BlockMaxRows)
	opts.Allocator = ImmutMemAllocator
	if node.data, err = node.file.LoadBatch(
		schema.AllTypes(),
		schema.AllNames(),
		schema.AllNullables(),
		opts); err != nil {
		node.exception.Store(err)
	}
	if node.data.Length() != int(node.rows) {
		logutil.Fatalf("Load %d rows but %d expected: %s", node.data.Length(), node.rows, node.block.meta.String())
	}
}

func (node *appendableNode) flushData(ts types.TS, colsData *containers.Batch, opCtx Operation) (err error) {
	if exception := node.exception.Load(); exception != nil {
		logutil.Error("[Exception]", common.ExceptionField(exception))
		err = exception.(error)
		return
	}
	mvcc := node.block.mvcc
	if node.GetBlockMaxflushTS().Equal(ts) {
		err = data.ErrStaleRequest
		logutil.Info("[Done]", common.TimestampField(ts),
			common.OperationField(opCtx.OpName()),
			common.ErrorField(err),
			common.ReasonField("already flushed"),
			common.OperandField(node.block.meta.AsCommonID().String()))
		return
	}
	masks := make(map[uint16]*roaring.Bitmap)
	vals := make(map[uint16]map[uint32]any)
	mvcc.RLock()
	for i := range node.block.meta.GetSchema().ColDefs {
		chain := mvcc.GetColumnChain(uint16(i))

		chain.RLock()
		updateMask, updateVals, err := chain.CollectUpdatesLocked(ts)
		chain.RUnlock()
		if err != nil {
			break
		}
		if updateMask != nil {
			masks[uint16(i)] = updateMask
			vals[uint16(i)] = updateVals
		}
	}
	if err != nil {
		mvcc.RUnlock()
		return
	}
	deleteChain := mvcc.GetDeleteChain()
	n, err := deleteChain.CollectDeletesLocked(ts, false, mvcc.RWMutex)
	mvcc.RUnlock()
	if err != nil {
		return
	}
	dnode := n.(*updates.DeleteNode)
	logutil.Info("[Running]", common.OperationField(opCtx.OpName()),
		common.OperandField(node.block.meta.AsCommonID().String()),
		common.TimestampField(ts))
	var deletes *roaring.Bitmap
	if dnode != nil {
		deletes = dnode.GetDeleteMaskLocked()
	}
	if node.block.scheduler == nil {
		err = node.block.ABlkFlushDataClosure(ts, colsData, masks, vals, deletes)()
		return
	}
	scope := node.block.meta.AsCommonID()
	task, err := node.block.scheduler.ScheduleScopedFn(
		tasks.WaitableCtx,
		tasks.IOTask,
		scope,
		node.block.ABlkFlushDataClosure(ts, colsData, masks, vals, deletes))
	if err != nil {
		return
	}
	err = task.WaitDone()
	return
}

func (node *appendableNode) OnUnload() {
	if exception := node.exception.Load(); exception != nil {
		logutil.Errorf("%v", exception)
		return
	}
	ts := node.block.mvcc.LoadMaxVisible()
	needCkp := true
	if err := node.flushData(ts, node.data, new(unloadOp)); err != nil {
		needCkp = false
		if err == data.ErrStaleRequest {
			// err = nil
		} else {
			logutil.Warnf("%s: %v", node.block.meta.String(), err)
			node.exception.Store(err)
		}
	}
	node.data.Close()
	node.data = nil
	if needCkp {
		_, _ = node.block.scheduler.ScheduleScopedFn(nil, tasks.CheckpointTask, node.block.meta.AsCommonID(), node.block.CheckpointWALClosure(ts))
	}
}

func (node *appendableNode) Close() (err error) {
	if exception := node.exception.Load(); exception != nil {
		logutil.Warnf("%v", exception)
		err = exception.(error)
		return
	}
	node.Node.Close()
	if exception := node.exception.Load(); exception != nil {
		logutil.Warnf("%v", exception)
		err = exception.(error)
		return
	}
	if node.data != nil {
		node.data.Close()
		node.data = nil
	}
	return
}

func (node *appendableNode) PrepareAppend(rows uint32) (n uint32, err error) {
	if exception := node.exception.Load(); exception != nil {
		logutil.Errorf("%v", exception)
		err = exception.(error)
		return
	}
	left := node.block.meta.GetSchema().BlockMaxRows - node.rows
	if left == 0 {
		err = data.ErrNotAppendable
		return
	}
	if rows > left {
		n = left
	} else {
		n = rows
	}
	return
}

func (node *appendableNode) FillPhyAddrColumn(startRow, length uint32) (err error) {
	col, err := model.PreparePhyAddrData(catalog.PhyAddrColumnType, node.block.prefix, startRow, length)
	if err != nil {
		return
	}
	defer col.Close()
	vec := node.data.Vecs[node.block.meta.GetSchema().PhyAddrKey.Idx]
	node.block.Lock()
	vec.Extend(col)
	node.block.Unlock()
	return
}

func (node *appendableNode) ApplyAppend(bat *containers.Batch, txn txnif.AsyncTxn) (from int, err error) {
	if exception := node.exception.Load(); exception != nil {
		logutil.Errorf("%v", exception)
		err = exception.(error)
		return
	}
	schema := node.block.meta.GetSchema()
	from = int(node.rows)
	for srcPos, attr := range bat.Attrs {
		def := schema.ColDefs[schema.GetColIdx(attr)]
		if def.IsPhyAddr() {
			continue
		}
		destVec := node.data.Vecs[def.Idx]
		node.block.Lock()
		destVec.Extend(bat.Vecs[srcPos])
		node.block.Unlock()
	}
	if err = node.FillPhyAddrColumn(uint32(from), uint32(bat.Length())); err != nil {
		return
	}
	node.rows += uint32(bat.Length())
	return
}
