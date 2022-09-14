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
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

const (
	constPageSize = 1024
	// TODO: fix cycle import because of catalog.SystemTable_DB_ID and catalog.SystemBlock_Table_ID
	constSysTblDBID  = 1
	constSysTblTblID = 2
)

type txnPage struct {
	minTs, maxTs types.TS
	txns         []txnif.AsyncTxn
}

type LogtailMgr struct {
	PageSize   int32 // for test
	activeSize *int32
	// activePage is a fixed size array, has fixed memory address during its whole lifetime
	activePage []txnif.AsyncTxn
	// rwlock is used to protect pages. there are two cases to hold write lock
	// 1. activePage is full and move to pages
	// 2. prune txn because of checkpoint TODO
	rwlock sync.RWMutex
	pages  []txnPage
}

// bisect return i that make f(ele in arr[:i]) false and f(ele in arr[i:]) true.
// i is in closed range [0, len(arr)]
func bisect[T any](arr []T, f func(a T) bool) int {
	lo, hi := 0, len(arr)-1
	var mid int
	for lo <= hi {
		mid = lo + (hi-lo)/2
		if f(arr[mid]) {
			hi -= 1
		} else {
			lo += 1
		}
	}
	return lo
}

func bisect_ref[T any](arr []T, f func(a *T) bool) int {
	lo, hi := 0, len(arr)-1
	var mid int
	for lo <= hi {
		mid = lo + (hi-lo)/2
		if f(&arr[mid]) {
			hi -= 1
		} else {
			lo += 1
		}
	}
	return lo
}

func NewLogtailMgr(pageSize int32) *LogtailMgr {
	return &LogtailMgr{
		PageSize:   pageSize,
		activeSize: new(int32),
		activePage: make([]txnif.AsyncTxn, pageSize),
		pages:      make([]txnPage, 0),
	}
}

// AddTxn happens in a queue, it is safe to assume there is no concurrent AddTxn now.
func (l *LogtailMgr) AddTxn(txn txnif.AsyncTxn) {
	size := atomic.LoadInt32(l.activeSize)
	l.activePage[size] = txn
	newsize := atomic.AddInt32(l.activeSize, 1)

	if newsize == l.PageSize {
		l.rwlock.Lock()
		defer l.rwlock.Unlock()
		txns := l.activePage
		l.activePage = make([]txnif.AsyncTxn, l.PageSize)
		atomic.StoreInt32(l.activeSize, 0)
		newPage := txnPage{
			minTs: txns[0].GetPrepareTS(),
			maxTs: txns[l.PageSize-1].GetPrepareTS(),
			txns:  txns,
		}
		l.pages = append(l.pages, newPage)
	}
}

func (l *LogtailMgr) SyncLogtails(req *api.SyncLogTailReq) (api.SyncLogTailResp, error) {
	if tid := req.Table.TbId; tid == constSysTblDBID || tid == constSysTblTblID {
		return l.syncCatalogLogtails(req)
	}
	return l.syncDataLogtails(req)
}

func (l *LogtailMgr) syncDataLogtails(req *api.SyncLogTailReq) (api.SyncLogTailResp, error) {
	start := types.BuildTS(req.CnHave.PhysicalTime, req.CnHave.LogicalTime)
	end := types.BuildTS(req.CnWant.PhysicalTime, req.CnWant.LogicalTime)
	_ /* dirtyBlks */ = l.CollectDirtyTree(start, end, req.Table.TbId)
	// TODO: return scanDataLogtail(dbID, tableID, dirtyBlks)
	return api.SyncLogTailResp{}, nil
}

func (l *LogtailMgr) syncCatalogLogtails(req *api.SyncLogTailReq) (api.SyncLogTailResp, error) {
	start := types.BuildTS(req.CnHave.PhysicalTime, req.CnHave.LogicalTime)
	end := types.BuildTS(req.CnWant.PhysicalTime, req.CnWant.LogicalTime)
	_ /* changed */ = l.CollectCatalogChange(start, end)
	// TODO: return scanCatalogLogtail()
	return api.SyncLogTailResp{}, nil
}

// scanTxnBetween happens concurrently, interleaves with AddTxn
func (l *LogtailMgr) scanTxnBetween(start, end types.TS, f func(txn txnif.AsyncTxn) (moveOn bool)) {
	l.rwlock.RLock()
	defer l.rwlock.RUnlock()

	// i is start page idx, where the start ts lands in
	i := bisect_ref(l.pages, func(page *txnPage) bool { return page.maxTs.GreaterEq(start) })
	if i < len(l.pages) {
		txns := l.pages[i].txns
		// j is the first txn whose prepareTS >= start ts
		// no need to check j outbound because i is determined
		j := bisect(txns, func(txn txnif.AsyncTxn) bool { return txn.GetPrepareTS().GreaterEq(start) })
		for _, txn := range txns[j:] {
			if txn.GetPrepareTS().Greater(end) {
				return
			}
			if !f(txn) {
				return
			}
		}

	}
	// scan other pages
	if i+1 < len(l.pages) {
		for _, page := range l.pages[i+1:] {
			for _, txn := range page.txns {
				if txn.GetPrepareTS().Greater(end) {
					return
				}
				if !f(txn) {
					return
				}
			}
		}
	}

	// end ts is not found in pages, has to check active page
	// getting active page readonly view will not block AddTxn
	size := atomic.LoadInt32(l.activeSize)
	view := l.activePage[:size]
	viewStart := 0
	if len(l.pages) == 0 {
		viewStart = bisect(view, func(txn txnif.AsyncTxn) bool { return txn.GetPrepareTS().GreaterEq(start) })
		if viewStart >= int(size) { // start ts is too big
			return
		}
	}
	for _, txn := range view[viewStart:] {
		if txn.GetPrepareTS().Greater(end) {
			return
		}
		if !f(txn) {
			return
		}
	}
}

func (l *LogtailMgr) CollectDirtyTree(start, end types.TS, tableID uint64) map[uint64]map[uint64]struct{} {
	// if tree[segID] is nil, it means that is just a seg operation
	tree := make(map[uint64]map[uint64]struct{}, 0)
	var blkSet map[uint64]struct{}
	var exist bool
	f := func(txn txnif.AsyncTxn) (moveOn bool) {
		store := txn.GetStore()
		if store.IsTableDataChanged(tableID) {
			pointSet := store.GetTableDirtyPoints(tableID)
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

	l.scanTxnBetween(start, end, f)

	return tree
}

func (l *LogtailMgr) CollectCatalogChange(start, end types.TS) bool {
	changed := false
	f := func(txn txnif.AsyncTxn) (moveOn bool) {
		if txn.GetStore().IsCatalogChanged() {
			changed = true
			return false
		}
		return true
	}
	l.scanTxnBetween(start, end, f)
	return changed
}
