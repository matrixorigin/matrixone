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

package engine

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

const TEMPORARY_DBNAME = "%!%mo_temp_db"

func GetTempTableName(DbName string, TblName string) string {
	return strings.ReplaceAll(DbName, ".", "DOT") + "." + strings.ReplaceAll(TblName, ".", "DOT")
}

func (e *EntireEngine) New(ctx context.Context, op client.TxnOperator) error {
	return e.Engine.New(ctx, op)
}

func (e *EntireEngine) Commit(ctx context.Context, op client.TxnOperator) error {
	return e.Engine.Commit(ctx, op)
}

func (e *EntireEngine) Rollback(ctx context.Context, op client.TxnOperator) error {
	return e.Engine.Rollback(ctx, op)
}

func (e *EntireEngine) Delete(ctx context.Context, databaseName string, op client.TxnOperator) error {
	return e.Engine.Delete(ctx, databaseName, op)
}

func (e *EntireEngine) Create(ctx context.Context, databaseName string, op client.TxnOperator) error {
	return e.Engine.Create(ctx, databaseName, op)
}

func (e *EntireEngine) Databases(ctx context.Context, op client.TxnOperator) (databaseNames []string, err error) {
	return e.Engine.Databases(ctx, op)
}

func (e *EntireEngine) Database(ctx context.Context, databaseName string, op client.TxnOperator) (Database, error) {
	if databaseName == TEMPORARY_DBNAME {
		if e.TempEngine != nil {
			return e.TempEngine.Database(ctx, TEMPORARY_DBNAME, op.(*client.EntireTxnOperator).GetTemp())
		} else {
			return nil, moerr.NewInternalError("temporary engine not init yet")
		}
	}
	return e.Engine.Database(ctx, databaseName, op)
}

func (e *EntireEngine) Nodes() (cnNodes Nodes, err error) {
	return e.Engine.Nodes()
}

func (e *EntireEngine) Hints() Hints {
	return e.Engine.Hints()
}

func (e *EntireEngine) NewBlockReader(ctx context.Context, num int, ts timestamp.Timestamp,
	expr *plan.Expr, ranges [][]byte, tblDef *plan.TableDef) ([]Reader, error) {
		return e.Engine.NewBlockReader(ctx, num, ts, expr, ranges, tblDef)
	}
