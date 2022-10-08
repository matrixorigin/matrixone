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
	"sync"
	"sync/atomic"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/tidwall/btree"
)

type txnPage struct {
	minTs types.TS
	txns  []txnif.AsyncTxn
}

func cmpTxnPage(a, b *txnPage) bool { return a.minTs.Less(b.minTs) }

type LogtailMgr struct {
	txnbase.NoopCommitListener
	pageSize int32             // for test
	minTs    types.TS          // the lower bound of active page
	tsAlloc  *types.TsAlloctor // share same clock with txnMgr

	// TODO: move the active page to btree, simplify the iteration of pages
	activeSize *int32
	// activePage is a fixed size array, has fixed memory address during its whole lifetime
	activePage []txnif.AsyncTxn
	// Lock is used to protect pages. there are three cases to hold lock
	// 1. activePage is full and moves to pages
	// 2. prune txn because of checkpoint TODO
	// 3. copy btree
	// Not RwLock because a copied btree can be read without holding read lock
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
func (l *LogtailMgr) OnEndPrePrepare(op *txnbase.OpTxn) { l.AddTxn(op.Txn) }

// Notes:
// 1. AddTxn happens in a queue, it is safe to assume there is no concurrent AddTxn now.
// 2. the added txn has no prepareTS because it happens in OnEndPrePrepare, so it is safe to alloc ts to be minTs
func (l *LogtailMgr) AddTxn(txn txnif.AsyncTxn) {
	size := atomic.LoadInt32(l.activeSize)
	l.activePage[size] = txn
	newsize := atomic.AddInt32(l.activeSize, 1)

	if newsize == l.pageSize {
		// alloc ts without lock
		prevMinTs := l.minTs
		l.minTs = l.tsAlloc.Alloc()
		l.Lock()
		defer l.Unlock()
		newPage := &txnPage{
			minTs: prevMinTs,
			txns:  l.activePage,
		}
		l.pages.Set(newPage)

		l.activePage = make([]txnif.AsyncTxn, l.pageSize)
		atomic.StoreInt32(l.activeSize, 0)
	}
}

// GetReader returns a read only reader of txns at call time.
// this is cheap operation, the returned reader can be accessed without any locks
func (l *LogtailMgr) GetReader(start, end types.TS) *LogtailReader {
	l.Lock()
	size := atomic.LoadInt32(l.activeSize)
	activeView := l.activePage[:size]
	btreeView := l.pages.Copy()
	l.Unlock()
	return &LogtailReader{
		start:      start,
		end:        end,
		btreeView:  btreeView,
		activeView: activeView,
	}
}

func (l *LogtailMgr) DecideScope(tableID uint64) Scope {
	var scope Scope
	switch tableID {
	case pkgcatalog.MO_DATABASE_ID:
		scope = ScopeDatabases
	case pkgcatalog.MO_TABLES_ID:
		scope = ScopeTables
	case pkgcatalog.MO_COLUMNS_ID:
		scope = ScopeColumns
	default:
		scope = ScopeUserTables
	}
	return scope
}

func (l *LogtailMgr) GetTableOperator(start, end types.TS,
	catalog *catalog.Catalog,
	dbID, tableID uint64,
	scope Scope,
	visitor catalog.Processor) *BoundTableOperator {

	reader := l.GetReader(start, end)
	return NewBoundTableOperator(catalog, reader, scope, dbID, tableID, visitor)
}
