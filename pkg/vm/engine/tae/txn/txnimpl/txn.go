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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type txnImpl struct {
	*txnbase.Txn
	catalog *catalog.Catalog
}

var TxnFactory = func(catalog *catalog.Catalog) txnbase.TxnFactory {
	return func(mgr *txnbase.TxnManager, store txnif.TxnStore, txnId []byte,
		start types.TS, info []byte) txnif.AsyncTxn {
		return newTxnImpl(catalog, mgr, store, txnId, start, info)
	}
}

func newTxnImpl(catalog *catalog.Catalog, mgr *txnbase.TxnManager, store txnif.TxnStore,
	txnId []byte, start types.TS, info []byte) *txnImpl {
	impl := &txnImpl{
		Txn:     txnbase.NewTxn(mgr, store, txnId, start, info),
		catalog: catalog,
	}
	return impl
}

func (txn *txnImpl) CreateDatabase(name string) (db handle.Database, err error) {
	return txn.Store.CreateDatabase(name)
}

func (txn *txnImpl) CreateDatabaseWithID(name string, id uint64) (db handle.Database, err error) {
	return txn.Store.CreateDatabaseWithID(name, id)
}

func (txn *txnImpl) DropDatabase(name string) (db handle.Database, err error) {
	return txn.Store.DropDatabase(name)
}

func (txn *txnImpl) UnsafeGetDatabase(id uint64) (db handle.Database, err error) {
	return txn.Store.UnsafeGetDatabase(id)
}

func (txn *txnImpl) UnsafeGetRelation(dbId, id uint64) (rel handle.Relation, err error) {
	return txn.Store.UnsafeGetRelation(dbId, id)
}

func (txn *txnImpl) GetDatabase(name string) (db handle.Database, err error) {
	return txn.Store.GetDatabase(name)
}

func (txn *txnImpl) DatabaseNames() (names []string) {
	return txn.Store.DatabaseNames()
}

func (txn *txnImpl) LogTxnEntry(dbId, tableId uint64, entry txnif.TxnEntry, readed []*common.ID) (err error) {
	return txn.Store.LogTxnEntry(dbId, tableId, entry, readed)
}

func (txn *txnImpl) LogTxnState(sync bool) (logEntry entry.Entry, err error) {
	return txn.Store.LogTxnState(sync)
}
