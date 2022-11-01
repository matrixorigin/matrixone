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

package moengine

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"time"

	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

var (
	_ engine.Engine = (*txnEngine)(nil)
	_ Engine        = (*txnEngine)(nil)
)

func NewEngine(impl *db.DB) *txnEngine {
	return &txnEngine{
		impl: impl,
	}
}

func (e *txnEngine) New(_ context.Context, _ client.TxnOperator) error {
	return nil
}

func (e *txnEngine) Commit(_ context.Context, _ client.TxnOperator) error {
	return nil
}

func (e *txnEngine) Rollback(_ context.Context, _ client.TxnOperator) error {
	return nil
}

func (e *txnEngine) Delete(ctx context.Context, name string, txnOp client.TxnOperator) (err error) {
	var txn txnif.AsyncTxn
	if txn, err = e.impl.GetTxnByCtx(txnOp); err != nil {
		panic(err)
	}
	txnBindAccessInfoFromCtx(txn, ctx)
	_, err = txn.DropDatabase(name)
	return
}

func (e *txnEngine) DropDatabase(ctx context.Context, name string, txnHandle Txn) (err error) {
	var txn txnif.AsyncTxn
	if txn, err = e.impl.GetTxn(txnHandle.GetID()); err != nil {
		panic(err)
	}
	txnBindAccessInfoFromCtx(txn, ctx)
	_, err = txn.DropDatabase(name)
	return
}

func (e *txnEngine) DropDatabaseByID(ctx context.Context, id uint64, txnHandle Txn) (err error) {
	var txn txnif.AsyncTxn
	if txn, err = e.impl.GetTxn(txnHandle.GetID()); err != nil {
		panic(err)
	}
	_, err = txn.DropDatabaseByID(id)
	return
}

func (e *txnEngine) Create(ctx context.Context, name string, txnOp client.TxnOperator) (err error) {
	var txn txnif.AsyncTxn
	if txn, err = e.impl.GetTxnByCtx(txnOp); err != nil {
		panic(err)
	}
	txnBindAccessInfoFromCtx(txn, ctx)
	_, err = txn.CreateDatabase(name)
	return
}

func (e *txnEngine) CreateDatabase(ctx context.Context, name string, txnHandle Txn) (err error) {
	var txn txnif.AsyncTxn
	if txn, err = e.impl.GetTxn(txnHandle.GetID()); err != nil {
		panic(err)
	}
	txnBindAccessInfoFromCtx(txn, ctx)
	_, err = txn.CreateDatabase(name)
	return
}

func (e *txnEngine) CreateDatabaseWithID(ctx context.Context,
	name string, id uint64, txnHandle Txn) (err error) {
	var txn txnif.AsyncTxn
	if txn, err = e.impl.GetTxn(txnHandle.GetID()); err != nil {
		panic(err)
	}
	txnBindAccessInfoFromCtx(txn, ctx)
	_, err = txn.CreateDatabaseWithID(name, id)
	return
}

func (e *txnEngine) Databases(ctx context.Context, txnOp client.TxnOperator) ([]string, error) {
	var err error
	var txn txnif.AsyncTxn

	if txn, err = e.impl.GetTxnByCtx(txnOp); err != nil {
		panic(err)
	}
	txnBindAccessInfoFromCtx(txn, ctx)
	return txn.DatabaseNames(), nil
}

func (e *txnEngine) DatabaseNames(ctx context.Context, txnHandle Txn) ([]string, error) {
	var err error
	var txn txnif.AsyncTxn

	if txn, err = e.impl.GetTxn(txnHandle.GetID()); err != nil {
		panic(err)
	}
	txnBindAccessInfoFromCtx(txn, ctx)
	return txn.DatabaseNames(), nil
}

func (e *txnEngine) Database(ctx context.Context, name string, txnOp client.TxnOperator) (engine.Database, error) {
	var err error
	var txn txnif.AsyncTxn

	if txn, err = e.impl.GetTxnByCtx(txnOp); err != nil {
		panic(err)
	}
	txnBindAccessInfoFromCtx(txn, ctx)
	h, err := txn.GetDatabase(name)
	if err != nil {
		return nil, err
	}
	db := newDatabase(h)
	return db, nil
}

func (e *txnEngine) GetDatabase(ctx context.Context, name string, txnHandle Txn) (Database, error) {
	var err error
	var txn txnif.AsyncTxn

	if txn, err = e.impl.GetTxn(txnHandle.GetID()); err != nil {
		panic(err)
	}
	txnBindAccessInfoFromCtx(txn, ctx)
	h, err := txn.GetDatabase(name)
	if err != nil {
		return nil, err
	}
	db := newDatabase(h)
	return db, nil
}

func (e *txnEngine) GetDatabaseByID(_ context.Context, id uint64, txnHandle Txn) (Database, error) {
	var err error
	var txn txnif.AsyncTxn

	if txn, err = e.impl.GetTxn(txnHandle.GetID()); err != nil {
		panic(err)
	}
	h, err := txn.GetDatabaseByID(id)
	if err != nil {
		return nil, err
	}
	db := newDatabase(h)
	return db, nil
}

func (e *txnEngine) GetTAE(_ context.Context) *db.DB {
	return e.impl
}
func (e *txnEngine) FlushTable(ctx context.Context, tenantID uint32, databaseId, tableId uint64, ts types.TS) error {
	return e.impl.FlushTable(tenantID, databaseId, tableId, ts)
}

func (e *txnEngine) Nodes() (engine.Nodes, error) {
	return nil, nil
}

func (e *txnEngine) StartTxn(info []byte) (txn Txn, err error) {
	return e.impl.StartTxn(info)
}

func (e *txnEngine) GetTxnByID(id []byte) (txn Txn, err error) {
	return e.impl.GetTxn(string(id))
}

func (e *txnEngine) GetOrCreateTxnWithMeta(info []byte, id []byte, ts types.TS) (txn Txn, err error) {
	return e.impl.GetOrCreateTxnWithMeta(info, id, ts)
}

func (e *txnEngine) Hints() (h engine.Hints) {
	h.CommitOrRollbackTimeout = time.Minute
	return
}

func (e *txnEngine) Close() (err error) {
	return e.impl.Close()
}

func (e *txnEngine) Destroy() (err error) {
	panic(moerr.NewNYI("Pls implement me!"))
}
