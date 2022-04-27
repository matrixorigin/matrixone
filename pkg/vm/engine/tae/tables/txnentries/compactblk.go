package txnentries

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
)

type compactBlockEntry struct {
	sync.RWMutex
	txn  txnif.AsyncTxn
	from handle.Block
	to   handle.Block
}

func NewCompactBlockEntry(txn txnif.AsyncTxn, from, to handle.Block) *compactBlockEntry {
	return &compactBlockEntry{
		txn:  txn,
		from: from,
		to:   to,
	}
}

func (node *compactBlockEntry) PrepareRollback() (err error) {
	// TODO: remove block file? (should be scheduled and executed async)
	return
}
func (node *compactBlockEntry) ApplyRollback() (err error) { return }
func (node *compactBlockEntry) ApplyCommit() (err error)   { return }
func (node *compactBlockEntry) MakeCommand(csn uint32) (cmd txnif.TxnCmd, err error) {
	// TODO:
	// 1. make command
	return
}

func (node *compactBlockEntry) PrepareCommit() (err error) {
	dataBlock := node.from.GetMeta().(*catalog.BlockEntry).GetBlockData()
	v := dataBlock.CollectChangesInRange(node.txn.GetStartTS(), node.txn.GetCommitTS())
	view := v.(*updates.BlockView)
	if view == nil {
		return
	}
	deletes := view.DeleteMask
	for colIdx, mask := range view.UpdateMasks {
		vals := view.UpdateVals[colIdx]
		view.UpdateMasks[colIdx], view.UpdateVals[colIdx], view.DeleteMask = compute.ShuffleByDeletes(mask, vals, deletes)
		for row, v := range view.UpdateVals[colIdx] {
			if err = node.to.Update(row, colIdx, v); err != nil {
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
			if err = node.to.RangeDelete(row, row); err != nil {
				return
			}
		}
	}
	return
}
