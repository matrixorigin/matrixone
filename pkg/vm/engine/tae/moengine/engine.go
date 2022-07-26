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

	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

var (
	_ engine.Engine = (*txnEngine)(nil)
)

func NewEngine(impl *db.DB) *txnEngine {
	return &txnEngine{
		impl: impl,
	}
}

func (e *txnEngine) Delete(_ context.Context, name string, ctx client.TxnOperator) (err error) {
	var txn txnif.AsyncTxn
	if txn, err = e.impl.GetTxnByCtx(ctx.(engine.Snapshot)); err != nil {
		panic(err)
	}
	_, err = txn.DropDatabase(name)
	return
}

func (e *txnEngine) Create(_ context.Context, name string, ctx client.TxnOperator) (err error) {
	var txn txnif.AsyncTxn
	if txn, err = e.impl.GetTxnByCtx(ctx.(engine.Snapshot)); err != nil {
		panic(err)
	}
	_, err = txn.CreateDatabase(name)
	return
}

func (e *txnEngine) Databases(_ context.Context, ctx client.TxnOperator) ([]string, error) {
	var err error
	var txn txnif.AsyncTxn

	if txn, err = e.impl.GetTxnByCtx(ctx.(engine.Snapshot)); err != nil {
		panic(err)
	}
	return txn.DatabaseNames(), nil
}

func (e *txnEngine) Database(_ context.Context, name string, ctx client.TxnOperator) (engine.Database, error) {
	var err error
	var txn txnif.AsyncTxn

	if txn, err = e.impl.GetTxnByCtx(ctx.(engine.Snapshot)); err != nil {
		panic(err)
	}
	h, err := txn.GetDatabase(name)
	if err != nil {
		return nil, err
	}
	db := newDatabase(h)
	return db, nil
}

func (e *txnEngine) Nodes() engine.Nodes {
	return nil
}

func (e *txnEngine) StartTxn(info []byte) (txn Txn, err error) {
	return e.impl.StartTxn(info)
}
