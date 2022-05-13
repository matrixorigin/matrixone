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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type txnDBIt struct {
	*sync.RWMutex
	txn    txnif.AsyncTxn
	linkIt *common.LinkIt
	curr   *catalog.DBEntry
}

func newDBIt(txn txnif.AsyncTxn, c *catalog.Catalog) *txnDBIt {
	it := &txnDBIt{
		RWMutex: c.RWMutex,
		txn:     txn,
		linkIt:  c.MakeDBIt(true),
	}
	for it.linkIt.Valid() {
		curr := it.linkIt.Get().GetPayload().(*catalog.DBEntry)
		curr.RLock()
		if curr.TxnCanRead(it.txn, curr.RWMutex) {
			curr.RUnlock()
			it.curr = curr
			break
		}
		curr.RUnlock()
		it.linkIt.Next()
	}
	return it
}

func (it *txnDBIt) Close() error { return nil }
func (it *txnDBIt) Valid() bool  { return it.linkIt.Valid() }

func (it *txnDBIt) Next() {
	valid := true
	for {
		it.linkIt.Next()
		node := it.linkIt.Get()
		if node == nil {
			it.curr = nil
			break
		}
		entry := node.GetPayload().(*catalog.DBEntry)
		entry.RLock()
		valid = entry.TxnCanRead(it.txn, entry.RWMutex)
		entry.RUnlock()
		if valid {
			it.curr = entry
			break
		}
	}
}

func (it *txnDBIt) GetCurr() *catalog.DBEntry { return it.curr }

type txnDatabase struct {
	*txnbase.TxnDatabase
	entry *catalog.DBEntry
}

func newDatabase(txn txnif.AsyncTxn, meta *catalog.DBEntry) *txnDatabase {
	db := &txnDatabase{
		TxnDatabase: &txnbase.TxnDatabase{
			Txn: txn,
		},
		entry: meta,
	}
	return db

}
func (db *txnDatabase) GetID() uint64   { return db.entry.GetID() }
func (db *txnDatabase) GetName() string { return db.entry.GetName() }
func (db *txnDatabase) String() string  { return db.entry.String() }

func (db *txnDatabase) CreateRelation(def interface{}) (rel handle.Relation, err error) {
	// schema := def.(*catalog.Schema)
	// meta, err := db.entry.CreateTableEntry(schema, db.Txn)
	// if err != nil {
	// 	return
	// }
	// db.Txn.GetStore().AddTxnEntry(TxnEntryCretaeTable, meta)
	// rel = newRelation(db.Txn, meta)
	return db.Txn.GetStore().CreateRelation(def)
}

func (db *txnDatabase) DropRelationByName(name string) (rel handle.Relation, err error) {
	return db.Txn.GetStore().DropRelationByName(name)
}

func (db *txnDatabase) GetRelationByName(name string) (rel handle.Relation, err error) {
	return db.Txn.GetStore().GetRelationByName(name)
}

func (db *txnDatabase) MakeRelationIt() (it handle.RelationIt) {
	return newRelationIt(db.Txn, db.entry)
}

func (db *txnDatabase) RelationCnt() int64                  { return 0 }
func (db *txnDatabase) Relations() (rels []handle.Relation) { return }
func (db *txnDatabase) Close() error                        { return nil }
func (db *txnDatabase) GetMeta() interface{}                { return db.entry }
