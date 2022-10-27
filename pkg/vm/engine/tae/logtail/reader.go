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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/tidwall/btree"
)

// a read only view of txns
type LogtailReader struct {
	start, end types.TS                 // included
	btreeView  *btree.Generic[*txnPage] // read only btree
	activeView []txnif.AsyncTxn         // read only active page
}

func (v *LogtailReader) GetDirty() (tree *common.Tree, count int) {
	tree = common.NewTree()

	readOp := func(txn txnif.AsyncTxn) (moveOn bool) {
		if memo := txn.GetMemo(); memo.HasAnyTableDataChanges() {
			tree.Merge(memo.GetDirty())
		}
		count++
		return true
	}
	v.readTxnInBetween(v.start, v.end, readOp)
	return
}

func (v *LogtailReader) GetDirtyByTable(dbID, id uint64) (tree *common.TableTree) {
	tree = common.NewTableTree(dbID, id)
	readOp := func(txn txnif.AsyncTxn) (moveOn bool) {
		if memo := txn.GetMemo(); memo.HasTableDataChanges(id) {
			tree.Merge(memo.GetDirtyTableByID(id))
		}
		return true
	}
	v.readTxnInBetween(v.start, v.end, readOp)
	return
}

func (v *LogtailReader) HasCatalogChanges() bool {
	changed := false
	readOp := func(txn txnif.AsyncTxn) (moveOn bool) {
		if txn.GetMemo().HasCatalogChanges() {
			changed = true
			return false
		}
		return true
	}
	v.readTxnInBetween(v.start, v.end, readOp)
	return changed
}

// [start, end]
func (v *LogtailReader) readTxnInBetween(start, end types.TS, readOp func(txn txnif.AsyncTxn) (moveOn bool)) {
	var pivot types.TS
	v.btreeView.Descend(&txnPage{minTs: start}, func(item *txnPage) bool { pivot = item.minTs; return false })
	stopInOldPages := false
	v.btreeView.Ascend(&txnPage{minTs: pivot}, func(page *txnPage) bool {
		var ts types.TS
		for _, txn := range page.txns {
			ts = txn.GetPrepareTS()
			// stop if not prepared or exceed request range
			if ts.IsEmpty() || ts.Greater(end) {
				stopInOldPages = true
				return false
			}
			if ts.Less(start) {
				continue
			}
			if !readOp(txn) {
				stopInOldPages = true
				return false
			}
		}
		return true
	})

	if stopInOldPages {
		return
	}

	var ts types.TS
	for _, txn := range v.activeView {
		ts = txn.GetPrepareTS()
		if ts.IsEmpty() || ts.Greater(end) {
			return
		}
		if ts.Less(start) {
			continue
		}
		if !readOp(txn) {
			return
		}
	}
}
