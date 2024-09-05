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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

// Reader is a snapshot of all txn prepared between from and to.
// Dirty tables/objects/blocks can be queried based on those txn
type Reader struct {
	from, to types.TS
	table    *TxnTable
}

// Merge all dirty table/object/block into one dirty tree
func (r *Reader) GetDirty() (tree *model.Tree, count int) {
	tree = model.NewTree()
	op := func(row RowT) (moveOn bool) {
		if memo := row.GetMemo(); memo.HasAnyTableDataChanges() {
			row.GetTxnState(true)
			tree.Merge(memo.GetDirty())
		}
		count++
		return true
	}
	r.table.ForeachRowInBetween(r.from, r.to, nil, op)
	return
}

// HasCatalogChanges returns true if any txn in the reader modified the Catalog
func (r *Reader) HasCatalogChanges() bool {
	changed := false
	op := func(row RowT) (moveOn bool) {
		if row.GetMemo().HasCatalogChanges() {
			changed = true
			return false
		}
		return true
	}
	skipFn := func(blk BlockT) bool {
		summary := blk.summary.Load()
		return summary != nil && !summary.hasCatalogChanges
	}
	r.table.ForeachRowInBetween(r.from, r.to, skipFn, op)
	return changed
}

func (r *Reader) IsCommitted() bool {
	committed := true
	r.table.ForeachRowInBetween(r.from, r.to, nil, func(row RowT) (goNext bool) {
		state := row.GetTxnState(false)
		if state != txnif.TxnStateCommitted && state != txnif.TxnStateRollbacked {
			committed = false
			return false
		}
		return true
	})
	return committed
}

// Merge all dirty table/object/block of **a table** into one tree
func (r *Reader) GetDirtyByTable(
	dbID, id uint64,
) (tree *model.TableTree) {
	tree = model.NewTableTree(dbID, id)
	op := func(row RowT) (moveOn bool) {
		if memo := row.GetMemo(); memo.HasTableDataChanges(id) {
			tree.Merge(memo.GetDirtyTableByID(id))
		}
		return true
	}
	r.table.ForeachRowInBetween(r.from, r.to, nil, op)
	return
}

// TODO: optimize
func (r *Reader) GetMaxLSN() (maxLsn uint64) {
	r.table.ForeachRowInBetween(
		r.from,
		r.to,
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
