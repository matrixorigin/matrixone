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

package memoryengine

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type BindedEngine struct {
	txnOp  client.TxnOperator
	engine *Engine
}

func (e *Engine) Bind(txnOp client.TxnOperator) *BindedEngine {
	return &BindedEngine{
		txnOp:  txnOp,
		engine: e,
	}
}

var _ engine.Engine = new(BindedEngine)

func (b *BindedEngine) Commit(ctx context.Context, _ client.TxnOperator) error {
	return b.engine.Commit(ctx, b.txnOp)
}

func (b *BindedEngine) Create(ctx context.Context, databaseName string, _ client.TxnOperator) error {
	return b.engine.Create(ctx, databaseName, b.txnOp)
}

func (b *BindedEngine) Database(ctx context.Context, databaseName string, _ client.TxnOperator) (engine.Database, error) {
	return b.engine.Database(ctx, databaseName, b.txnOp)
}

func (b *BindedEngine) Databases(ctx context.Context, _ client.TxnOperator) (databaseNames []string, err error) {
	return b.engine.Databases(ctx, b.txnOp)
}

func (b *BindedEngine) Delete(ctx context.Context, databaseName string, _ client.TxnOperator) error {
	return b.engine.Delete(ctx, databaseName, b.txnOp)
}

func (b *BindedEngine) Hints() engine.Hints {
	return b.engine.Hints()
}

func (b *BindedEngine) NewBlockReader(_ context.Context, _ int, _ timestamp.Timestamp,
	_ *plan.Expr, _ [][]byte, _ *plan.TableDef) ([]engine.Reader, error) {
	return nil, nil
}

func (b *BindedEngine) New(ctx context.Context, _ client.TxnOperator) error {
	return b.engine.New(ctx, b.txnOp)
}

func (b *BindedEngine) Nodes(isInternal bool, tenant string, cnLabel map[string]string) (cnNodes engine.Nodes, err error) {
	return b.engine.Nodes(isInternal, tenant, cnLabel)
}

func (b *BindedEngine) Rollback(ctx context.Context, _ client.TxnOperator) error {
	return b.engine.Rollback(ctx, b.txnOp)
}

func (b *BindedEngine) GetNameById(ctx context.Context, op client.TxnOperator, tableId uint64) (dbName string, tblName string, err error) {
	return b.engine.GetNameById(ctx, op, tableId)
}

func (b *BindedEngine) GetRelationById(ctx context.Context, op client.TxnOperator, tableId uint64) (dbName string, tblName string, rel engine.Relation, err error) {
	return b.engine.GetRelationById(ctx, op, tableId)
}

func (b *BindedEngine) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	return b.engine.AllocateIDByKey(ctx, key)
}
