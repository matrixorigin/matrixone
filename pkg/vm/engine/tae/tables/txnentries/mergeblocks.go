package txnentries

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
)

type mergeBlocksEntry struct {
	sync.RWMutex
	txn      txnif.AsyncTxn
	merged   []handle.Block
	created  []handle.Block
	mapping  []uint32
	fromAddr []uint32
	toAddr   []uint32
}

func (entry *mergeBlocksEntry) PrepareRollback() (err error) {
	// TODO: remove block file? (should be scheduled and executed async)
	return
}
func (entry *mergeBlocksEntry) ApplyRollback() (err error) { return }
func (entry *mergeBlocksEntry) ApplyCommit() (err error)   { return }
func (entry *mergeBlocksEntry) MakeCommand(csn uint32) (cmd txnif.TxnCmd, err error) {
	// TODO:
	// 1. make command
	return
}

func (entry *mergeBlocksEntry) resolveAddr(fromPos int, fromOffset uint32) (toPos int, toOffset uint32) {
	totalFromOffset := entry.fromAddr[fromPos] + fromOffset
	totalToOffset := entry.mapping[totalFromOffset]
	left, right := 0, len(entry.toAddr)-1
	for left <= right {
		toPos = (left + right) / 2
		if entry.toAddr[toPos] < totalToOffset {
			left = toPos + 1
		} else if entry.toAddr[toPos] > totalToOffset {
			right = toPos - 1
		} else {
			break
		}
	}
	if toPos == 0 && entry.toAddr[toPos] < totalToOffset {
		toPos = toPos + 1
	}
	toOffset = entry.toAddr[toPos]
	return
}

func (entry *mergeBlocksEntry) PrepareCommit() (err error) {
	for fromPos, merged := range entry.merged {
		dataBlock := merged.GetMeta().(*catalog.BlockEntry).GetBlockData()
		v := dataBlock.CollectChangesInRange(entry.txn.GetStartTS(), entry.txn.GetCommitTS())
		view := v.(*updates.BlockView)
		if view == nil {
			continue
		}
		deletes := view.DeleteMask
		for colIdx, mask := range view.UpdateMasks {
			vals := view.UpdateVals[colIdx]
			view.UpdateMasks[colIdx], view.UpdateVals[colIdx], view.DeleteMask = compute.ShuffleByDeletes(mask, vals, deletes)
			for row, v := range view.UpdateVals[colIdx] {
				toPos, toRow := entry.resolveAddr(fromPos, row)

				if err = entry.created[toPos].Update(toRow, colIdx, v); err != nil {
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
				toPos, toRow := entry.resolveAddr(fromPos, row)
				if err = entry.created[toPos].RangeDelete(toRow, toRow); err != nil {
					return
				}
			}
		}
	}
	return
}
