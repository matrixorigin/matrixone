// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtail

import (
	"runtime"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

// Reader queries transactions prepared between from and to.
// Dirty-table results can include extra tables from compacted boundary blocks.
type Reader struct {
	from, to types.TS
	table    *TxnTable
}

func mergeSummary(tree *model.Tree, summary *summary) {
	for record := range summary.tids {
		if _, ok := tree.Tables[record.ID]; !ok {
			tree.Tables[record.ID] = model.NewTableTree(record.DbID, record.ID)
		}
	}
}

// GetDirty merges dirty tables into a tree and returns the number of scanned
// transactions. After block compaction, both may include entries outside the
// requested range; retained in-range dirty tables must not be omitted.
func (r *Reader) GetDirty() (tree *model.Tree, count int) {
	tree = model.NewTree()
	iterateOnCompact := false

	op := func(row RowT) (moveOn bool) {
		memo := row.GetMemo()
		if memo == nil {
			// current block is compacted during iterating
			// retrieve the memo from blk summary, only once.
			iterateOnCompact = true
		} else if memo.HasAnyTableDataChanges() {
			row.GetTxnState(true)
			tree.Merge(memo.GetDirty())
		}
		count++
		return true
	}

	postBlkOp := func(blk BlockT) {
		if !iterateOnCompact {
			return
		}
		summary := blk.summary.Load()
		// wait TryCompact to finish
		for summary == nil {
			runtime.Gosched()
			summary = blk.summary.Load()
		}
		mergeSummary(tree, summary)
		iterateOnCompact = false
	}

	skipBlkOp := func(blk BlockT) (moveOn bool) {
		summary := blk.summary.Load()
		if summary == nil {
			return false
		}
		mergeSummary(tree, summary)
		count += len(blk.rows)
		return true
	}
	r.table.ForeachRowInBetween(r.from, r.to, skipBlkOp, postBlkOp, op)
	return
}

func (r *Reader) IsDirtyOnTable(DbID, id uint64) bool {
	// TODO: optimize if needed
	tree, _ := r.GetDirty()
	return tree.HasTable(id)
}

// TODO: optimize
func (r *Reader) GetMaxLSN() (maxLsn uint64) {
	r.table.ForeachRowInBetween(
		r.from,
		r.to,
		nil,
		nil,
		func(row RowT) (moveOn bool) {
			lsn := row.GetLSN()
			if lsn > maxLsn {
				maxLsn = lsn
			}
			return true
		})
	return
}

// ForEachDirtyTable visits table identities in the reader's inclusive range.
// A compacted block can contribute extra candidates at either boundary. It
// charges the shared work budget before visiting each block, row, and identity.
func (r *Reader) ForEachDirtyTable(charge func() error, visit func(model.TableRecord) error) (err error) {
	consume := func() bool {
		err = charge()
		return err == nil
	}
	visitRecord := func(record model.TableRecord) bool {
		if !consume() {
			return false
		}
		err = visit(record)
		return err == nil
	}

	if !consume() {
		return err
	}
	snapshot := r.table.Snapshot()
	pivot := &txnBlock{bornTS: r.from}
	snapshot.Descend(pivot, func(blk BlockT) bool {
		if consume() {
			pivot.bornTS = blk.bornTS
		}
		return false
	})
	snapshot.Ascend(pivot, func(blk BlockT) bool {
		if err != nil || !consume() || blk.bornTS.GT(&r.to) {
			return false
		}
		summary := blk.summary.Load()
		blk.RLock()
		rows := blk.rows[:len(blk.rows)]
		blk.RUnlock()
		if summary != nil {
			if len(rows) == 0 {
				err = moerr.NewInternalErrorNoCtx("scoped changed-table history has an empty compacted block")
				return false
			}
			if !consume() {
				return false
			}
			first := rows[0].GetPrepareTS()
			if !consume() {
				return false
			}
			last := rows[len(rows)-1].GetPrepareTS()
			if first.IsEmpty() || last.LT(&first) {
				err = moerr.NewInternalErrorNoCtx("scoped changed-table history has an invalid compacted block range")
				return false
			}
			if first.GT(&r.to) {
				return false
			}
			if last.LT(&r.from) {
				return true
			}
			for record := range summary.tids {
				if !visitRecord(record) {
					return false
				}
			}
			return last.LE(&r.to)
		}
		for _, row := range rows {
			if !consume() {
				return false
			}
			ts := row.GetPrepareTS()
			if ts.IsEmpty() {
				err = moerr.NewInternalErrorNoCtx("scoped changed-table history has an empty transaction timestamp")
				return false
			}
			if ts.GT(&r.to) {
				return false
			}
			if ts.LT(&r.from) {
				continue
			}
			switch row.GetTxnState(false) {
			case txnif.TxnStateCommitted:
			case txnif.TxnStateRollbacked:
				continue
			default:
				err = moerr.NewInternalErrorNoCtx("scoped changed-table history contains an in-flight transaction")
				return false
			}
			memo := row.GetMemo()
			if memo == nil || memo.Tree == nil {
				err = moerr.NewInternalErrorNoCtx("scoped changed-table history changed during compaction")
				return false
			}
			for _, record := range memo.Tables {
				if record == nil {
					err = moerr.NewInternalErrorNoCtx("scoped changed-table history contains an invalid table")
					return false
				}
				if !visitRecord(*record) {
					return false
				}
			}
		}
		return true
	})
	return
}
