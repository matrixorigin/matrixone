// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txnimpl

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

func readWriteConfilictCheck(entry catalog.BaseEntry, ts types.TS) (err error) {
	entry.RLock()
	defer entry.RUnlock()
	needWait, txnToWait := entry.GetLatestNodeLocked().NeedWaitCommitting(ts)
	if needWait {
		entry.RUnlock()
		txnToWait.GetTxnState(true)
		entry.RLock()
	}
	if entry.DeleteBefore(ts) {
		err = moerr.NewTxnRWConflict()
	}
	return
}

type warChecker struct {
	*common.Tree
	txn     txnif.AsyncTxn
	catalog *catalog.Catalog
	visitor common.TreeVisitor
}

func newWarChecker(txn txnif.AsyncTxn, c *catalog.Catalog) *warChecker {
	checker := &warChecker{
		Tree:    common.NewTree(),
		txn:     txn,
		catalog: c,
	}
	visitor := new(common.BaseTreeVisitor)
	visitor.BlockFn = checker.visitBlock
	checker.visitor = visitor
	return checker
}

func (checker *warChecker) check() (err error) {
	return checker.Visit(checker.visitor)
}

func (checker *warChecker) visitBlock(
	dbID, tableID, segmentID, blockID uint64) (err error) {
	db, err := checker.catalog.GetDatabaseByID(dbID)
	if err != nil {
		panic(err)
	}
	table, err := db.GetTableEntryByID(tableID)
	if err != nil {
		panic(err)
	}
	segment, err := table.GetSegmentByID(segmentID)
	if err != nil {
		panic(err)
	}
	block, err := segment.GetBlockEntryByID(blockID)
	if err != nil {
		panic(err)
	}

	entry := block.MetaBaseEntry
	return readWriteConfilictCheck(entry, checker.txn.GetCommitTS())
}
