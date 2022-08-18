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

package frontend

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
)

type wrappedTAEEngine struct {
	engine moengine.TxnEngine
}

var _ TxnClient = new(wrappedTAEEngine)

func (w *wrappedTAEEngine) New(options ...TxnOption) TxnOperator {
	tx, err := w.engine.StartTxn(nil)
	if err != nil {
		panic(err)
	}
	return &wrappedTAETx{
		tx: tx,
	}
}

func (w *wrappedTAEEngine) NewWithSnapshot(snapshot []byte) (TxnOperator, error) {
	tx, err := w.engine.StartTxn(snapshot)
	if err != nil {
		return nil, err
	}
	return &wrappedTAETx{
		tx: tx,
	}, nil
}

type wrappedTAETx struct {
	tx moengine.Txn
}

var _ TxnOperator = new(wrappedTAETx)

func (w *wrappedTAETx) ApplySnapshot(data []byte) error {
	panic("should not call")
}

func (w *wrappedTAETx) Commit(ctx context.Context) error {
	return w.tx.Commit()
}

func (*wrappedTAETx) Read(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	panic("should not call")
}

func (w *wrappedTAETx) Rollback(ctx context.Context) error {
	return w.tx.Rollback()
}

func (*wrappedTAETx) Snapshot() ([]byte, error) {
	panic("should not call")
}

func (*wrappedTAETx) Write(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	panic("should not call")
}

func (*wrappedTAETx) WriteAndCommit(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	panic("should not call")
}

func (w *wrappedTAETx) AsEngineMethodArgument() client.TxnOperator {
	return engine.Snapshot(w.tx.GetCtx())
}

func (w *wrappedTAETx) ToBytes() []byte {
	return w.tx.GetCtx()
}
