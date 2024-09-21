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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

var ErrRWConflict = moerr.NewTxnRWConflictNoCtx()

func readWriteConfilictCheck(entry *catalog.ObjectEntry, ts types.TS) (err error) {
	lastNode := entry.GetLatestNode()
	needWait, txnToWait := lastNode.GetLastMVCCNode().NeedWaitCommitting(ts)
	// TODO:
	// I don't think we need to wait here any more. `block` and `Object` are
	// local metadata and never be involved in a 2PC txn. So a prepared `block`
	// will never be rollbacked
	if needWait {
		txnToWait.GetTxnState(true)
		lastNode = entry.GetLatestNode()
	}
	if lastNode.DeleteBefore(ts) {
		err = ErrRWConflict
	}
	return
}

type warChecker struct {
	txn     txnif.AsyncTxn
	catalog *catalog.Catalog
	//conflictSet is a set of objs which had been deleted and committed.
	conflictSet map[types.Objectid]bool
	readSet     map[types.Objectid]*catalog.ObjectEntry
	cache       map[types.Objectid]*catalog.ObjectEntry
}

func newWarChecker(txn txnif.AsyncTxn, c *catalog.Catalog) *warChecker {
	checker := &warChecker{
		txn:         txn,
		catalog:     c,
		conflictSet: make(map[types.Objectid]bool),
		readSet:     make(map[types.Objectid]*catalog.ObjectEntry),
		cache:       make(map[types.Objectid]*catalog.ObjectEntry),
	}
	return checker
}

func (checker *warChecker) CacheGet(
	dbID uint64,
	tableID uint64,
	ObjectID *types.Objectid,
	isTombstone bool) (object *catalog.ObjectEntry, err error) {
	object = checker.cacheGet(ObjectID)
	if object != nil {
		return
	}
	db, err := checker.catalog.GetDatabaseByID(dbID)
	if err != nil {
		return
	}
	table, err := db.GetTableEntryByID(tableID)
	if err != nil {
		return
	}
	object, err = table.GetObjectByID(ObjectID, isTombstone)
	if err != nil {
		return
	}
	checker.Cache(object)
	return
}

func (checker *warChecker) InsertByID(
	dbID uint64,
	tableID uint64,
	objectID *types.Objectid,
	isTombstone bool,
) {
	obj, err := checker.CacheGet(dbID, tableID, objectID, isTombstone)
	if err != nil {
		panic(err)
	}
	checker.Insert(obj)
}

func (checker *warChecker) cacheGet(id *objectio.ObjectId) *catalog.ObjectEntry {
	return checker.cache[*id]
}
func (checker *warChecker) Cache(obj *catalog.ObjectEntry) {
	checker.cache[*obj.ID()] = obj
}

func (checker *warChecker) Insert(obj *catalog.ObjectEntry) {
	checker.Cache(obj)
	if checker.HasConflict(*obj.ID()) {
		panic(fmt.Sprintf("cannot add conflicted %s into readset", obj.String()))
	}
	checker.readSet[*obj.ID()] = obj
}

func (checker *warChecker) checkOne(id *common.ID, ts types.TS) (err error) {
	// defer func() {
	// 	logutil.Infof("checkOne blk=%s ts=%s err=%v", id.BlockString(), ts.ToString(), err)
	// }()
	if checker.HasConflict(*id.ObjectID()) {
		err = ErrRWConflict
		return
	}
	entry := checker.readSet[*id.ObjectID()]
	if entry == nil {
		return
	}
	return readWriteConfilictCheck(entry, ts)
}

func (checker *warChecker) checkAll(ts types.TS) (err error) {
	for _, obj := range checker.readSet {
		if err = readWriteConfilictCheck(obj, ts); err != nil {
			return
		}
	}
	return
}

func (checker *warChecker) Delete(id *common.ID) {
	checker.conflictSet[*id.ObjectID()] = true
	delete(checker.readSet, *id.ObjectID())
}

func (checker *warChecker) HasConflict(id types.Objectid) (y bool) {
	_, y = checker.conflictSet[id]
	return
}
