package txnimpl

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
)

type blocksCompactionCtx struct {
	contexts map[common.ID]*blockCompactionCtx
}

func newBlocksCompactionCtx() *blocksCompactionCtx {
	return &blocksCompactionCtx{
		contexts: make(map[common.ID]*blockCompactionCtx),
	}
}

type blockCompactionCtx struct {
	txn  txnif.AsyncTxn
	from handle.Block
	to   handle.Block
}

func (blocks *blocksCompactionCtx) GetCtx(id *common.ID) *blockCompactionCtx {
	return blocks.contexts[*id]
}

func (blocks *blocksCompactionCtx) AddCtx(txn txnif.AsyncTxn, from, to handle.Block) {
	id := from.Fingerprint()
	blocks.contexts[*id] = &blockCompactionCtx{
		txn:  txn,
		from: from,
		to:   to,
	}
}

func (block *blockCompactionCtx) PrepareCommit() (err error) {
	dataBlock := block.from.GetMeta().(*catalog.BlockEntry).GetBlockData()
	v := dataBlock.CollectChangesInRange(block.txn.GetStartTS(), block.txn.GetCommitTS())
	view := v.(*updates.BlockView)
	if view == nil {
		return
	}
	deletes := view.DeleteMask
	for colIdx, mask := range view.UpdateMasks {
		vals := view.UpdateVals[colIdx]
		view.UpdateMasks[colIdx], view.UpdateVals[colIdx], view.DeleteMask = compute.ShuffleByDeletes(mask, vals, deletes)
		for row, v := range view.UpdateVals[colIdx] {
			if err = block.to.Update(row, colIdx, v); err != nil {
				return
			}
		}
	}
	if len(view.UpdateMasks) == 0 {
		_, _, view.DeleteMask = compute.ShuffleByDeletes(nil, nil, view.DeleteMask)
	}
	if view.DeleteMask != nil {
		it := view.DeleteMask.Iterator()
		for it.HasNext() {
			row := it.Next()
			if err = block.to.RangeDelete(row, row); err != nil {
				return
			}
		}
	}
	return
}
