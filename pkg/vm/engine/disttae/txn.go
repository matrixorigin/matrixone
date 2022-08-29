// Copyright 2022 Matrix Origin
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

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func NewManager(cli client.TxnClient) *Manager {
	return &Manager{
		cli:  cli,
		txns: make(map[string]*Transaction),
	}
}

func (m *Manager) New(opts ...client.TxnOption) (client.TxnOperator, error) {
	op, err := m.cli.New(opts...)
	if err != nil {
		return nil, err
	}
	txn := &Transaction{
		mgr:      m,
		readOnly: false,
		meta:     op.Txn(),
		operator: op,
	}
	defer m.AddTransaction(txn)
	return txn, nil
}

func (m *Manager) NewWithSnapshot(snapshot []byte) (client.TxnOperator, error) {
	op, err := m.cli.NewWithSnapshot(snapshot)
	if err != nil {
		return nil, err
	}
	txn := &Transaction{
		mgr:      m,
		readOnly: false,
		meta:     op.Txn(),
		operator: op,
	}
	defer m.AddTransaction(txn)
	return txn, nil
}

func (m *Manager) AddTransaction(txn *Transaction) {
	m.Lock()
	defer m.Unlock()
	m.txns[string(txn.meta.ID)] = txn
}

func (m *Manager) DelTransaction(txn *Transaction) {
	m.Lock()
	defer m.Unlock()
	delete(m.txns, string(txn.meta.ID))
}

// hasConflict used to detect if a transaction on a cn is in conflict,
// currently an empty implementation, assuming all transactions on a cn are conflict free
func (m *Manager) HasConflict(txn *Transaction) bool {
	return false
}

// detecting whether a transaction is a read-only transaction
func (txn *Transaction) ReadOnly() bool {
	return txn.readOnly
}

// Write used to write data to the transaction buffer
// insert/delete/update all use this api
func (txn *Transaction) WriteBatch(bat *batch.Batch) error {
	txn.readOnly = true
	return nil
}

// WriteFile used to add a s3 file information to the transaction buffer
// insert/delete/update all use this api
func (txn *Transaction) WriteFile(file string) error {
	txn.readOnly = true
	return nil
}

// NewReader used to read data from transaction
func (txn *Transaction) NewReader(ctx context.Context, num int, expr *plan.Expr) []engine.Reader {
	return nil
}

func (txn *Transaction) Txn() txn.TxnMeta {
	return txn.operator.Txn()
}

func (txn *Transaction) Snapshot() ([]byte, error) {
	return txn.operator.Snapshot()
}

func (txn *Transaction) ApplySnapshot(data []byte) error {
	return txn.operator.ApplySnapshot(data)
}

func (txn *Transaction) Read(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	return txn.operator.Read(ctx, ops)
}

func (txn *Transaction) Write(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	return txn.operator.Write(ctx, ops)
}

func (txn *Transaction) WriteAndCommit(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	defer txn.mgr.DelTransaction(txn)
	return txn.operator.WriteAndCommit(ctx, ops)
}

func (txn *Transaction) Commit(ctx context.Context) error {
	defer txn.mgr.DelTransaction(txn)
	return txn.operator.Commit(ctx)
}

func (txn *Transaction) Rollback(ctx context.Context) error {
	defer txn.mgr.DelTransaction(txn)
	return txn.operator.Rollback(ctx)
}
