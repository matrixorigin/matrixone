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
	"errors"

	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

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
	if databaseName == "temp-db" {
		if e.TempEngine != nil {
			return e.TempEngine.Database(ctx, "temp-db", op)
		} else {
			return nil, errors.New("temporary engine not init yet")
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
