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
	"runtime"

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

func (e *txnEngine) Delete(_ uint64, name string, ctx engine.Snapshot) (err error) {
	var txn txnif.AsyncTxn
	if txn, err = e.impl.GetTxnByCtx(ctx); err != nil {
		panic(err)
	}
	_, err = txn.DropDatabase(name)
	return
}

func (e *txnEngine) Create(_ uint64, name string, _ int, ctx engine.Snapshot) (err error) {
	var txn txnif.AsyncTxn
	if txn, err = e.impl.GetTxnByCtx(ctx); err != nil {
		panic(err)
	}
	_, err = txn.CreateDatabase(name)
	return
}

func (e *txnEngine) Databases(ctx engine.Snapshot) (dbs []string) {
	var err error
	var txn txnif.AsyncTxn
	if txn, err = e.impl.GetTxnByCtx(ctx); err != nil {
		panic(err)
	}
	return txn.DatabaseNames()
}

func (e *txnEngine) Database(name string, ctx engine.Snapshot) (db engine.Database, err error) {
	var txn txnif.AsyncTxn
	if txn, err = e.impl.GetTxnByCtx(ctx); err != nil {
		panic(err)
	}
	h, err := txn.GetDatabase(name)
	if err != nil {
		return nil, err
	}
	db = newDatabase(h)
	return db, err
}

func (e *txnEngine) Node(ip string, _ engine.Snapshot) *engine.NodeInfo {
	return &engine.NodeInfo{Mcpu: runtime.NumCPU()}
}

func (e *txnEngine) StartTxn(info []byte) (txn Txn, err error) {
	txn = e.impl.StartTxn(info)
	return
}
