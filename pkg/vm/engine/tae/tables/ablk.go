package tables

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
)

type ablock struct {
	baseBlock
	storage struct {
		mu    sync.RWMutex
		mnode *common.PinnedItem[*memoryNode]
		pnode *common.PinnedItem[*persistedNode]
	}
}

func (blk *ablock) Rows() int {
	mnode, pnode := blk.PinNode()
	if mnode != nil {
		defer mnode.Close()
		blk.RLock()
		defer blk.RUnlock()
		return int(mnode.Item().Rows())
	} else if pnode != nil {
		defer pnode.Close()
		return int(pnode.Item().Rows())
	}
	panic(moerr.NewInternalError(fmt.Sprintf("bad block %s", blk.meta.String())))
}

func (blk *ablock) PinNode() (
	mnode *common.PinnedItem[*memoryNode],
	pnode *common.PinnedItem[*persistedNode]) {
	blk.storage.mu.RLock()
	defer blk.storage.mu.RUnlock()
	if blk.storage.mnode != nil {
		mnode = blk.storage.mnode.Item().Pin()
	} else if blk.storage.pnode != nil {
		pnode = blk.storage.pnode.Item().Pin()
	}
	return
}

func (blk *ablock) GetColumnData(
	from uint32,
	to uint32,
	colIdx int,
	buffer *bytes.Buffer) (vec containers.Vector, err error) {
	mnode, pnode := blk.PinNode()
	if mnode != nil {
		defer mnode.Close()
		blk.RLock()
		defer blk.RUnlock()
		return mnode.Item().GetColumnDataWindow(from, to, colIdx, buffer)
	} else if pnode != nil {
		defer pnode.Close()
		return pnode.Item().GetColumnDataWindow(from, to, colIdx, buffer)
	}
	panic(moerr.NewInternalError(fmt.Sprintf("bad block %s", blk.meta.String())))
}

func (blk *ablock) GetColumnDataById(
	txn txnif.AsyncTxn,
	colIdx int,
	buffer *bytes.Buffer) (view *model.ColumnView, err error) {
	return blk.resolveColumnData(
		txn.GetStartTS(),
		colIdx,
		buffer,
		false)
}

func (blk *ablock) resolveColumnData(
	ts types.TS,
	colIdx int,
	buffer *bytes.Buffer,
	skipDeletes bool) (view *model.ColumnView, err error) {
	mnode, pnode := blk.PinNode()

	if mnode != nil {
		defer mnode.Close()
		return blk.resolveInMemoryColumnData(
			mnode.Item(),
			ts,
			colIdx,
			buffer,
			skipDeletes)
	} else if pnode != nil {
		defer pnode.Close()
		return blk.resolvePersistedColumnData(
			pnode.Item(),
			ts,
			colIdx,
			buffer,
			skipDeletes,
		)
	}
	panic(moerr.NewInternalError(fmt.Sprintf("bad block %s", blk.meta.String())))
}

func (blk *ablock) resolvePersistedColumnData(
	pnode *persistedNode,
	ts types.TS,
	colIdx int,
	buffer *bytes.Buffer,
	skipDeletes bool) (view *model.ColumnView, err error) {
	view = model.NewColumnView(ts, colIdx)
	vec, err := blk.LoadPersistedColumnData(colIdx, buffer)
	if err != nil {
		return
	}
	view.SetData(vec)

	if skipDeletes {
		return
	}

	defer func() {
		if err != nil {
			view.Close()
		}
	}()

	err = blk.FillPersistedDeletes(view)
	return
}

// Note: With PinNode Context
func (blk *ablock) resolveInMemoryColumnData(
	mnode *memoryNode,
	ts types.TS,
	colIdx int,
	buffer *bytes.Buffer,
	skipDeletes bool) (view *model.ColumnView, err error) {
	blk.RLock()
	maxRow, visible, deSels, err := blk.mvcc.GetVisibleRowLocked(ts)
	if !visible || err != nil {
		blk.RUnlock()
		return
	}

	view = model.NewColumnView(ts, colIdx)
	var data containers.Vector
	data, err = mnode.GetColumnDataWindow(
		0,
		maxRow,
		colIdx,
		buffer)
	if err != nil {
		blk.RUnlock()
		return
	}
	view.SetData(data)
	if skipDeletes {
		blk.RUnlock()
		return
	}

	err = blk.FillInMemoryDeletesLocked(view, blk.RWMutex)
	blk.RUnlock()
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

func (blk *ablock) GetValue(
	txn txnif.AsyncTxn,
	row, col int) (v any, err error) {
	ts := txn.GetStartTS()
	mnode, pnode := blk.PinNode()
	if mnode != nil {
		defer mnode.Close()
		return blk.getInMemoryValue(mnode.Item(), ts, row, col)
	} else {
		defer pnode.Close()
		return blk.getPersistedValue(pnode.Item(), ts, row, col)
	}
	panic(moerr.NewInternalError(fmt.Sprintf("bad block %s", blk.meta.String())))
}

func (blk *ablock) getPersistedValue(
	pnode *persistedNode,
	ts types.TS,
	row, col int) (v any, err error) {
	view := model.NewColumnView(ts, col)
	if err = blk.FillPersistedDeletes(view); err != nil {
		return
	}
	if view.DeleteMask != nil && view.DeleteMask.ContainsInt(row) {
		err = moerr.NewNotFound()
		return
	}
	view2, err := blk.resolvePersistedColumnData(pnode, ts, col, nil, true)
	if err != nil {
		return
	}
	defer view2.Close()
	v = view2.GetValue(row)
	return
}

// With PinNode Context
func (blk *ablock) getInMemoryValue(
	mnode *memoryNode,
	ts types.TS,
	row, col int) (v any, err error) {
	blk.RLock()
	defer blk.RUnlock()
	deleted, err := blk.mvcc.IsDeletedLocked(uint32(row), ts, blk.RWMutex)
	if err != nil {
		return
	}
	if deleted {
		err = moerr.NewNotFound()
		return
	}
	view, err := blk.resolveInMemoryColumnData(mnode, ts, col, nil, true)
	if err != nil {
		return
	}
	defer view.Close()
	v = view.GetValue(row)
	return
}

func (blk *ablock) GetByFilter(
	txn txnif.AsyncTxn,
	filter *handle.Filter) (offset uint32, err error) {
	if filter.Op != handle.FilterEq {
		panic("logic error")
	}
	if blk.meta.GetSchema().SortKey == nil {
		_, _, offset = model.DecodePhyAddrKeyFromValue(filter.Val)
		return
	}
	ts := txn.GetStartTS()

	mnode, pnode := blk.PinNode()
	if mnode != nil {
		defer mnode.Close()
		return blk.getInMemoryRowByFilter(mnode.Item(), ts, filter)
	} else if pnode != nil {
		defer pnode.Close()
		return blk.getPersistedRowByFilter(pnode.Item(), ts, filter)
	}

	panic(moerr.NewInternalError(fmt.Sprintf("bad block %s", blk.meta.String())))
}

func (blk *ablock) getPersistedRowByFilter(
	pnode *persistedNode,
	ts types.TS,
	filter *handle.Filter) (row uint32, err error) {
	return
}

// With PinNode Context
func (blk *ablock) getInMemoryRowByFilter(
	mnode *memoryNode,
	ts types.TS,
	filter *handle.Filter) (row uint32, err error) {
	blk.RLock()
	defer blk.RUnlock()
	rows, err := mnode.GetRowsByKey(filter.Val)
	if err != nil && !moerr.IsMoErrCode(err, moerr.ErrNotFound) {
		return
	}

	waitFn := func(n *updates.AppendNode) {
		txn := n.Txn
		if txn != nil {
			blk.mvcc.RUnlock()
			txn.GetTxnState(true)
			blk.mvcc.RLock()
		}
	}
	if anyWaitable := blk.mvcc.CollectUncommittedANodesPreparedBefore(
		ts,
		waitFn); anyWaitable {
		rows, err = mnode.GetRowsByKey(filter.Val)
		if err != nil {
			return
		}
	}

	for i := len(rows) - 1; i >= 0; i-- {
		row = rows[i]
		appendnode := blk.mvcc.GetAppendNodeByRow(row)
		needWait, txn := appendnode.NeedWaitCommitting(ts)
		if needWait {
			blk.mvcc.RUnlock()
			txn.GetTxnState(true)
			blk.mvcc.RLock()
		}
		if appendnode.IsAborted() || !appendnode.IsVisible(ts) {
			continue
		}
		var deleted bool
		deleted, err = blk.mvcc.IsDeletedLocked(row, ts, blk.mvcc.RWMutex)
		if err != nil {
			return
		}
		if !deleted {
			return
		}
	}
	return 0, moerr.NewNotFound()
}
