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
package txnbase

import (
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/tidwall/btree"
)

type txnPage struct {
	minTs types.TS
	txns  []txnif.AsyncTxn
}

func cmpTxnPage(a, b *txnPage) bool { return a.minTs.Less(b.minTs) }

type LogtailMgr struct {
	noopCommitListener
	pageSize   int32             // for test
	minTs      types.TS          // the lower bound of active page
	tsAlloc    *types.TsAlloctor // share same clock with txnMgr
	activeSize *int32
	// activePage is a fixed size array, has fixed memory address during its whole lifetime
	activePage []txnif.AsyncTxn
	// Lock is used to protect pages. there are two cases to hold lock
	// 1. activePage is full and moves to pages
	// 2. prune txn because of checkpoint TODO
	// Not RwLock because read the btree by copying a read only view, without holding read lock
	sync.Mutex
	pages *btree.Generic[*txnPage]
}

func NewLogtailMgr(pageSize int32, clock clock.Clock) *LogtailMgr {
	tsAlloc := types.NewTsAlloctor(clock)
	minTs := tsAlloc.Alloc()

	return &LogtailMgr{
		pageSize:   pageSize,
		minTs:      minTs,
		tsAlloc:    tsAlloc,
		activeSize: new(int32),
		activePage: make([]txnif.AsyncTxn, pageSize),
		pages:      btree.NewGenericOptions(cmpTxnPage, btree.Options{NoLocks: true}),
	}
}

// LogtailMgr as a commit listener
func (l *LogtailMgr) OnEndPrePrepare(op *OpTxn) { l.AddTxn(op.Txn) }

// AddTxn happens in a queue, it is safe to assume there is no concurrent AddTxn now.
func (l *LogtailMgr) AddTxn(txn txnif.AsyncTxn) {
	size := atomic.LoadInt32(l.activeSize)
	l.activePage[size] = txn
	newsize := atomic.AddInt32(l.activeSize, 1)

	if newsize == l.pageSize {
		l.Lock()
		defer l.Unlock()
		txns := l.activePage
		minTs := txns[0].GetPrepareTS()
		if minTs.IsEmpty() {
			minTs = l.minTs
		}
		newPage := &txnPage{
			minTs: minTs,
			txns:  txns,
		}
		l.pages.Set(newPage)

		l.minTs = l.tsAlloc.Alloc()
		l.activePage = make([]txnif.AsyncTxn, l.pageSize)
		atomic.StoreInt32(l.activeSize, 0)
	}
}

// GetLogtailView returns a read only view of txns at call time.
// this is cheap operation, the returned view can be accessed without any locks
func (l *LogtailMgr) GetLogtailView(start, end types.TS, tid uint64) *LogtailView {
	l.Lock()
	size := atomic.LoadInt32(l.activeSize)
	activeView := l.activePage[:size]
	btreeView := l.pages.Copy()
	l.Unlock()
	return &LogtailView{
		start:      start,
		end:        end,
		tid:        tid,
		btreeView:  btreeView,
		activeView: activeView,
	}
}

// a read only view of txns
type LogtailView struct {
	start, end types.TS                 // included
	tid        uint64                   // table id
	btreeView  *btree.Generic[*txnPage] // read only btree
	activeView []txnif.AsyncTxn         // read only active page
}

func (v *LogtailView) GetDirtyPoints() Dirties {
	// if tree[segID] is nil, it means that is just a seg operation
	tree := make(map[uint64]map[uint64]struct{}, 0)
	var blkSet map[uint64]struct{}
	var exist bool
	f := func(txn txnif.AsyncTxn) (moveOn bool) {
		store := txn.GetStore()
		if store.HasTableDataChanges(v.tid) {
			pointSet := store.GetTableDirtyPoints(v.tid)
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
	return Dirties{Segs: segs}
}

// Dirties has determined iter order
type Dirties struct {
	Segs []dirtySeg
}

type dirtySeg struct {
	Sig  uint64
	Blks []uint64
}

func (v *LogtailView) HasCatalogChanges() bool {
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

func (v *LogtailView) scanTxnBetween(start, end types.TS, f func(txn txnif.AsyncTxn) (moveOn bool)) {
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
