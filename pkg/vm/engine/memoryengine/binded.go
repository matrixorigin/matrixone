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

	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
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

func (b *BindedEngine) LatestLogtailAppliedTime() timestamp.Timestamp {
	return b.engine.LatestLogtailAppliedTime()
}

func (b *BindedEngine) HasTempEngine() bool {
	return b.engine.HasTempEngine()
}

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

func (b *BindedEngine) BuildBlockReaders(
	ctx context.Context,
	proc any,
	ts timestamp.Timestamp,
	expr *plan.Expr,
	def *plan.TableDef,
	relData engine.RelData,
	num int) ([]engine.Reader, error) {
	return nil, nil
}

func (b *BindedEngine) New(ctx context.Context, _ client.TxnOperator) error {
	return b.engine.New(ctx, b.txnOp)
}

func (b *BindedEngine) Nodes(isInternal bool, tenant string, username string, cnLabel map[string]string) (cnNodes engine.Nodes, err error) {
	return b.engine.Nodes(isInternal, tenant, username, cnLabel)
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

func (b *BindedEngine) TryToSubscribeTable(ctx context.Context, dbID, tbID uint64, dbName, tblName string) error {
	return b.engine.TryToSubscribeTable(ctx, dbID, tbID, dbName, tblName)
}

func (b *BindedEngine) UnsubscribeTable(ctx context.Context, dbID, tbID uint64) error {
	return b.engine.UnsubscribeTable(ctx, dbID, tbID)
}

func (b *BindedEngine) PrefetchTableMeta(ctx context.Context, key pb.StatsInfoKey) bool {
	return b.engine.PrefetchTableMeta(ctx, key)
}
func (b *BindedEngine) Stats(ctx context.Context, key pb.StatsInfoKey, sync bool) *pb.StatsInfo {
	return b.engine.Stats(ctx, key, sync)
}

func (b *BindedEngine) GetMessageCenter() any {
	return nil
}

func (b *BindedEngine) GetService() string {
	return b.engine.GetService()
}
