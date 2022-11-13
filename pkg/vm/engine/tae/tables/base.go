package tables

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
)

type baseBlock struct {
	*sync.RWMutex
	bufMgr  base.INodeManager
	fs      *objectio.ObjectFS
	meta    *catalog.BlockEntry
	mvcc    *updates.MVCCHandle
	storage struct {
		mu    sync.RWMutex
		mnode *common.PinnedItem[*memoryNode]
		pnode *common.PinnedItem[*persistedNode]
	}
}

func (blk *baseBlock) PinNode() (
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

func (blk *baseBlock) GetColumnData(
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

func (blk *baseBlock) Rows() int {
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

func (blk *baseBlock) GetMeta() any                 { return blk.meta }
func (blk *baseBlock) GetBufMgr() base.INodeManager { return blk.bufMgr }
func (blk *baseBlock) GetID() *common.ID            { return blk.meta.AsCommonID() }

func (blk *baseBlock) FillInMemoryDeletesLocked(
	view *model.ColumnView,
	rwlocker *sync.RWMutex) (err error) {
	chain := blk.mvcc.GetDeleteChain()
	n, err := chain.CollectDeletesLocked(view.Ts, false, rwlocker)
	if err != nil {
		return
	}
	dnode := n.(*updates.DeleteNode)
	if dnode != nil {
		if view.DeleteMask == nil {
			view.DeleteMask = dnode.GetDeleteMaskLocked()
		} else {
			view.DeleteMask.Or(dnode.GetDeleteMaskLocked())
		}
	}
	return
}

func (blk *baseBlock) LoadPersistedCommitTS() (vec containers.Vector, err error) {
	if !blk.meta.IsAppendable() {
		return
	}
	location := blk.meta.GetMetaLoc()
	if location == "" {
		return
	}
	reader, err := blockio.NewReader(context.Background(), blk.fs, location)
	if err != nil {
		return
	}
	meta, err := reader.ReadMeta(nil)
	if err != nil {
		return
	}
	bat, err := reader.LoadBlkColumnsByMetaAndIdx(
		[]types.Type{types.T_TS.ToType()},
		[]string{catalog.AttrCommitTs},
		[]bool{false},
		meta,
		len(blk.meta.GetSchema().NameIndex),
	)
	if err != nil {
		return
	}
	vec = bat.Vecs[0]
	return
}

func (blk *baseBlock) LoadPersistedColumnData(
	colIdx int,
	buffer *bytes.Buffer,
) (vec containers.Vector, err error) {
	def := blk.meta.GetSchema().ColDefs[colIdx]
	location := blk.meta.GetMetaLoc()
	return LoadPersistedColumnData(
		blk.bufMgr,
		blk.fs,
		blk.meta.AsCommonID(),
		def,
		location,
		buffer)
}

func (blk *baseBlock) LoadPersistedDeletes() (bat *containers.Batch, err error) {
	location := blk.meta.GetDeltaLoc()
	if location == "" {
		return
	}
	return LoadPersistedDeletes(
		blk.bufMgr,
		blk.fs,
		location)
}

func (blk *baseBlock) FillPersistedDeletes(
	view *model.ColumnView) (err error) {
	deletes, err := blk.LoadPersistedDeletes()
	if deletes == nil || err != nil {
		return nil
	}
	for i := 0; i < deletes.Length(); i++ {
		abort := deletes.Vecs[2].Get(i).(bool)
		if abort {
			continue
		}
		commitTS := deletes.Vecs[1].Get(i).(types.TS)
		if commitTS.Greater(view.Ts) {
			continue
		}
		rowid := deletes.Vecs[0].Get(i).(types.Rowid)
		_, _, row := model.DecodePhyAddrKey(rowid)
		if view.DeleteMask == nil {
			view.DeleteMask = roaring.NewBitmap()
		}
		view.DeleteMask.Add(row)
	}
	return nil
}

func (blk *baseBlock) DeletesInfo() string {
	blk.RLock()
	defer blk.RUnlock()
	return blk.mvcc.GetDeleteChain().StringLocked()
}

func (blk *baseBlock) RangeDelete(
	txn txnif.AsyncTxn,
	start, end uint32,
	dt handle.DeleteType) (node txnif.DeleteNode, err error) {
	blk.Lock()
	defer blk.Unlock()
	if err = blk.mvcc.CheckNotDeleted(start, end, txn.GetStartTS()); err != nil {
		return
	}
	node = blk.mvcc.CreateDeleteNode(txn, dt)
	node.RangeDeleteLocked(start, end)
	return
}

func (blk *baseBlock) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s | [Rows=%d]", blk.meta.PPString(level, depth, prefix), blk.Rows())
	if level >= common.PPL1 {
		blk.RLock()
		s2 := blk.mvcc.StringLocked()
		blk.RUnlock()
		if s2 != "" {
			s = fmt.Sprintf("%s\n%s", s, s2)
		}
	}
	return s
}
