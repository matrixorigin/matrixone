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
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
)

type wrappedEngine struct {
	engine TxnEngine
}

func EngineToTxnClient(engine TxnEngine) client.TxnClient {
	return &wrappedEngine{
		engine: engine,
	}
}

var _ client.TxnClient = new(wrappedEngine)

func (w *wrappedEngine) New(
	ctx context.Context,
	ts timestamp.Timestamp,
	options ...client.TxnOption) (client.TxnOperator, error) {
	tx, err := w.engine.StartTxn(nil)
	if err != nil {
		panic(err)
	}
	return &wrappedTx{
		tx: tx,
	}, nil
}

func (w *wrappedEngine) NewWithSnapshot(snapshot []byte) (client.TxnOperator, error) {
	txn := w.engine.(*txnEngine).impl.TxnMgr.GetTxnByCtx(snapshot)
	if txn == nil {
		return nil, moerr.NewMissingTxnNoCtx()
	}
	return TxnToTxnOperator(txn), nil
}

func (w *wrappedEngine) Close() error {
	return w.engine.Close()
}

func (*wrappedEngine) MinTimestamp() timestamp.Timestamp {
	return timestamp.Timestamp{}
}

type wrappedTx struct {
	tx Txn
}

func TxnToTxnOperator(tx Txn) client.TxnOperator {
	return &wrappedTx{
		tx: tx,
	}
}

var _ client.TxnOperator = new(wrappedTx)

func (w *wrappedTx) ApplySnapshot(data []byte) error {
	panic("should not call")
}

func (w *wrappedTx) Commit(ctx context.Context) error {
	return w.tx.Commit()
}

func (*wrappedTx) Read(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	panic("should not call")
}

func (w *wrappedTx) Rollback(ctx context.Context) error {
	return w.tx.Rollback()
}

func (w *wrappedTx) Snapshot() ([]byte, error) {
	return w.tx.GetCtx(), nil
}

func (*wrappedTx) Write(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	panic("should not call")
}

func (*wrappedTx) WriteAndCommit(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	panic("should not call")
}

func (w *wrappedTx) Txn() txn.TxnMeta {
	return txn.TxnMeta{
		ID: w.tx.GetCtx(),
	}
}

func (w *wrappedTx) AddLockTable(lock.LockTable) error {
	panic("should not call")
}

func (w *wrappedTx) UpdateSnapshot(ts timestamp.Timestamp) error {
	panic("should not call")
}
