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
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
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
		start, snapshot types.TS) txnif.AsyncTxn {
		return newTxnImpl(catalog, mgr, store, txnId, start, snapshot)
	}
}

func newTxnImpl(catalog *catalog.Catalog, mgr *txnbase.TxnManager, store txnif.TxnStore,
	txnId []byte, start, snapshot types.TS) *txnImpl {
	impl := &txnImpl{
		Txn:     txnbase.NewTxn(mgr, store, txnId, start, snapshot),
		catalog: catalog,
	}
	return impl
}

func (txn *txnImpl) CreateDatabase(name, createSql, datTyp string) (db handle.Database, err error) {
	return txn.Store.CreateDatabase(name, createSql, datTyp)
}

func (txn *txnImpl) CreateDatabaseWithCtx(ctx context.Context,
	name, createSql, datTyp string, id uint64) (db handle.Database, err error) {
	err = txn.bindCtxInfo(ctx)
	if err != nil {
		return nil, err
	}
	return txn.Store.CreateDatabaseWithID(ctx, name, createSql, datTyp, id)
}

func (txn *txnImpl) DropDatabase(name string) (db handle.Database, err error) {
	return txn.Store.DropDatabase(name)
}

func (txn *txnImpl) DropDatabaseByID(id uint64) (db handle.Database, err error) {
	return txn.Store.DropDatabaseByID(id)
}

func (txn *txnImpl) UnsafeGetDatabase(id uint64) (db handle.Database, err error) {
	return txn.Store.UnsafeGetDatabase(id)
}

func (txn *txnImpl) UnsafeGetRelation(dbId, id uint64) (rel handle.Relation, err error) {
	return txn.Store.UnsafeGetRelation(dbId, id)
}

func (txn *txnImpl) bindCtxInfo(ctx context.Context) (err error) {
	var tid uint32
	if ctx == nil {
		return
	}
	tid, err = defines.GetAccountId(ctx)
	if err != nil {
		return
	}

	uid := defines.GetUserId(ctx)
	rid := defines.GetRoleId(ctx)
	txn.BindAccessInfo(tid, uid, rid)
	return err
}
func (txn *txnImpl) GetDatabaseWithCtx(ctx context.Context, name string) (db handle.Database, err error) {
	err = txn.bindCtxInfo(ctx)
	if err != nil {
		return nil, err
	}
	return txn.Store.GetDatabase(name)
}
func (txn *txnImpl) GetDatabase(name string) (db handle.Database, err error) {
	return txn.Store.GetDatabase(name)
}

func (txn *txnImpl) GetDatabaseByID(id uint64) (db handle.Database, err error) {
	return txn.Store.GetDatabaseByID(id)
}

func (txn *txnImpl) DatabaseNames() (names []string) {
	return txn.Store.DatabaseNames()
}

func (txn *txnImpl) LogTxnEntry(dbId, tableId uint64, entry txnif.TxnEntry, readedObject, readedTombstone []*common.ID) (err error) {
	return txn.Store.LogTxnEntry(dbId, tableId, entry, readedObject, readedTombstone)
}

func (txn *txnImpl) LogTxnState(sync bool) (logEntry entry.Entry, err error) {
	return txn.Store.LogTxnState(sync)
}

func makeWorkspaceVector(typ types.Type) containers.Vector {
	return containers.MakeVector(typ, common.WorkspaceAllocator)
}
