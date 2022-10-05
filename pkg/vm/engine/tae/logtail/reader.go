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
	"github.com/tidwall/btree"
)

// DirtySegs has determined iter order
type DirtySegs struct {
	Segs []dirtySeg
}

type dirtySeg struct {
	Sig  uint64
	Blks []uint64
}

// a read only view of txns
type LogtailReader struct {
	start, end types.TS                 // included
	tid        uint64                   // table id
	btreeView  *btree.Generic[*txnPage] // read only btree
	activeView []txnif.AsyncTxn         // read only active page
}

func (v *LogtailReader) GetDirty() DirtySegs {
	// if tree[segID] is nil, it means that is just a seg operation
	tree := make(map[uint64]map[uint64]struct{}, 0)
	var blkSet map[uint64]struct{}
	var exist bool
	f := func(txn txnif.AsyncTxn) (moveOn bool) {
		if txn.GetStore().HasTableDataChanges(v.tid) {
			pointSet := txn.GetStore().GetTableDirtyPoints(v.tid)
			for dirty := range pointSet {
				if dirty.BlkID == 0 {
					// a segment operation
					if _, exist = tree[dirty.SegID]; !exist {
						tree[dirty.SegID] = nil
					}
				} else {
					// merge the dirty block
					blkSet, exist = tree[dirty.SegID]
					if !exist || blkSet == nil {
						blkSet = make(map[uint64]struct{})
						tree[dirty.SegID] = blkSet
					}
					blkSet[dirty.BlkID] = struct{}{}
				}
			}
		}
		return true
	}
	v.scanTxnBetween(v.start, v.end, f)

	segs := make([]dirtySeg, 0, len(tree))
	for sig, blkSet := range tree {
		blk := make([]uint64, 0, len(blkSet))
		for b := range blkSet {
			blk = append(blk, b)
		}
		segs = append(segs, dirtySeg{Sig: sig, Blks: blk})
	}
	return DirtySegs{Segs: segs}
}

func (v *LogtailReader) HasCatalogChanges() bool {
	changed := false
	f := func(txn txnif.AsyncTxn) (moveOn bool) {
		if txn.GetStore().HasCatalogChanges() {
			changed = true
			return false
		}
		return true
	}
	v.scanTxnBetween(v.start, v.end, f)
	return changed
}

func (v *LogtailReader) scanTxnBetween(start, end types.TS, f func(txn txnif.AsyncTxn) (moveOn bool)) {
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
			if !f(txn) {
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
		if !f(txn) {
			return
		}
	}
}
