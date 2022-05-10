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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

var (
	_ engine.Engine = (*txnEngine)(nil)
)

func NewEngine(txn txnif.AsyncTxn) *txnEngine {
	return &txnEngine{
		txn: txn,
	}
}

func (e *txnEngine) Delete(_ uint64, name string) (err error) {
	_, err = e.txn.DropDatabase(name)
	return
}

func (e *txnEngine) Create(_ uint64, name string, _ int) (err error) {
	_, err = e.txn.CreateDatabase(name)
	return
}

func (e *txnEngine) Databases() (dbs []string) {
	return e.txn.DatabaseNames()
}

func (e *txnEngine) Database(name string) (db engine.Database, err error) {
	h, err := e.txn.GetDatabase(name)
	if err != nil {
		return nil, err
	}
	db = newDatabase(h)
	return db, err
}

func (e *txnEngine) Node(ip string) *engine.NodeInfo {
	return &engine.NodeInfo{Mcpu: runtime.NumCPU()}
}
