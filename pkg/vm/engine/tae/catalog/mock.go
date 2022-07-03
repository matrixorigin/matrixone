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

package catalog

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

func MockTxnFactory(catalog *Catalog) txnbase.TxnFactory {
	return func(mgr *txnbase.TxnManager, store txnif.TxnStore, id uint64, ts uint64, info []byte) txnif.AsyncTxn {
		txn := new(mockTxn)
		txn.Txn = txnbase.NewTxn(mgr, store, id, ts, info)
		txn.catalog = catalog
		return txn
	}
}

func MockTxnStoreFactory(catalog *Catalog) txnbase.TxnStoreFactory {
	return func() txnif.TxnStore {
		store := new(mockTxnStore)
		store.catalog = catalog
		store.entries = make(map[txnif.TxnEntry]bool)
		return store
	}

}

type mockTxnStore struct {
	txn txnif.TxnReader
	txnbase.NoopTxnStore
	catalog *Catalog
	entries map[txnif.TxnEntry]bool
}

func (store *mockTxnStore) AddTxnEntry(et txnif.TxnEntryType, entry txnif.TxnEntry) {
	store.entries[entry] = true
}

func (store *mockTxnStore) BindTxn(txn txnif.AsyncTxn) {
	store.txn = txn
}

func (store *mockTxnStore) PrepareCommit() error {
	for e := range store.entries {
		err := e.PrepareCommit()
		if err != nil {
			return err
		}
	}
	return nil
}

func (store *mockTxnStore) ApplyCommit() error {
	for e := range store.entries {
		err := e.ApplyCommit(nil)
		if err != nil {
			return err
		}
	}
	return nil
}

type mockDBHandle struct {
	*txnbase.TxnDatabase
	catalog *Catalog
	entry   *DBEntry
}

type mockSegIt struct {
	sync.RWMutex
}

type mockTableHandle struct {
	*txnbase.TxnRelation
	catalog *Catalog
	entry   *TableEntry
}

func (h *mockTableHandle) GetDB() (handle.Database, error) {
	return h.Txn.GetStore().GetDatabase(h.GetMeta().(*TableEntry).GetDB().GetName())
}

func newMockDBHandle(catalog *Catalog, txn txnif.AsyncTxn, entry *DBEntry) *mockDBHandle {
	return &mockDBHandle{
		TxnDatabase: &txnbase.TxnDatabase{
			Txn: txn,
		},
		catalog: catalog,
		entry:   entry,
	}
}

func newMockTableHandle(catalog *Catalog, txn txnif.AsyncTxn, entry *TableEntry) *mockTableHandle {
	return &mockTableHandle{
		TxnRelation: &txnbase.TxnRelation{
			Txn: txn,
		},
		catalog: catalog,
		entry:   entry,
	}
}

func (it *mockSegIt) GetError() error            { return nil }
func (it *mockSegIt) Valid() bool                { return false }
func (it *mockSegIt) Next()                      {}
func (it *mockSegIt) Close() error               { return nil }
func (it *mockSegIt) GetSegment() handle.Segment { return nil }

func (h *mockDBHandle) CreateRelation(def any) (rel handle.Relation, err error) {
	schema := def.(*Schema)
	tbl, err := h.entry.CreateTableEntry(schema, h.Txn, nil)
	if err != nil {
		return nil, err
	}
	h.Txn.GetStore().AddTxnEntry(0, tbl)
	rel = newMockTableHandle(h.catalog, h.Txn, tbl)
	return
}

func (h *mockDBHandle) TruncateByName(name string) (rel handle.Relation, err error) {
	panic("not implemented")
}

func (h *mockDBHandle) DropRelationByName(name string) (rel handle.Relation, err error) {
	entry, err := h.entry.DropTableEntry(name, h.Txn)
	if err != nil {
		return nil, err
	}
	h.Txn.GetStore().AddTxnEntry(0, entry)
	rel = newMockTableHandle(h.catalog, h.Txn, entry)
	return
}

func (h *mockDBHandle) String() string {
	return h.entry.String()
}

func (h *mockDBHandle) GetRelationByName(name string) (rel handle.Relation, err error) {
	entry, err := h.entry.GetTableEntry(name, h.Txn)
	if err != nil {
		return nil, err
	}
	return newMockTableHandle(h.catalog, h.Txn, entry), nil
}

func (h *mockTableHandle) MakeSegmentIt() (it handle.SegmentIt) {
	return new(mockSegIt)
}

func (h *mockTableHandle) String() string {
	return h.entry.String()
}

type mockTxn struct {
	*txnbase.Txn
	catalog *Catalog
}

func (txn *mockTxn) CreateDatabase(name string) (handle.Database, error) {
	entry, err := txn.catalog.CreateDBEntry(name, txn)
	if err != nil {
		return nil, err
	}
	txn.Store.AddTxnEntry(0, entry)
	h := newMockDBHandle(txn.catalog, txn, entry)
	return h, nil
}

func (txn *mockTxn) GetDatabase(name string) (handle.Database, error) {
	entry, err := txn.catalog.GetDBEntry(name, txn)
	if err != nil {
		return nil, err
	}
	return newMockDBHandle(txn.catalog, txn, entry), nil
}

func (txn *mockTxn) DropDatabase(name string) (handle.Database, error) {
	entry, err := txn.catalog.DropDBEntry(name, txn)
	if err != nil {
		return nil, err
	}
	txn.Store.AddTxnEntry(0, entry)
	return newMockDBHandle(txn.catalog, txn, entry), nil
}

func MockBatch(schema *Schema, rows int) *containers.Batch {
	if schema.IsSingleSortKey() {
		sortKey := schema.GetSingleSortKey()
		return containers.MockBatchWithAttrs(schema.Types(), schema.Attrs(), schema.Nullables(), rows, sortKey.Idx, nil)
	} else if schema.IsCompoundSortKey() {
		return containers.MockBatchWithAttrs(schema.Types(), schema.Attrs(), schema.Nullables(), rows, schema.HiddenKey.Idx, nil)
	} else {
		return containers.MockBatchWithAttrs(schema.Types(), schema.Attrs(), schema.Nullables(), rows, schema.HiddenKey.Idx, nil)
	}
}
