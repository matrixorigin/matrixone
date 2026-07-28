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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

// Reader is a snapshot of all txn prepared between from and to.
// Dirty tables/objects/blocks can be queried based on those txn
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

// Merge all dirty table/object/block into one dirty tree
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
