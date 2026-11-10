// Copyright 2026 Matrix Origin
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

package compile

import (
	"context"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

type privateAlterCopyCreateKey struct{}
type authorizedPrivateAlterCopyCreateKey struct{}

type privateAlterCopyCreate struct {
	owner    client.TxnOperator
	account  uint32
	database string
	table    string
	sql      string
	claimed  atomic.Bool
	closed   atomic.Bool
}

func (c *Compile) withPrivateAlterCopyCreate(database, table, sql string, create func() error) error {
	account, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	r := &privateAlterCopyCreate{owner: c.proc.GetTxnOperator(), account: account, database: database, table: table, sql: sql}
	old := c.proc.Ctx
	c.proc.Ctx = context.WithValue(old, privateAlterCopyCreateKey{}, r)
	defer func() { r.closed.Store(true); c.proc.Ctx = old }()
	return create()
}

// Match the actual internal SQL, not Compile's redacted diagnostic text. A
// nested child statement must never inherit authority from its parent CREATE.
func authorizePrivateAlterCopyCreate(ctx context.Context, owner client.TxnOperator, sql string) (context.Context, error) {
	r, _ := ctx.Value(privateAlterCopyCreateKey{}).(*privateAlterCopyCreate)
	prior, _ := ctx.Value(authorizedPrivateAlterCopyCreateKey{}).(*privateAlterCopyCreate)
	if r == nil && prior == nil {
		return ctx, nil
	}
	ctx = context.WithValue(ctx, authorizedPrivateAlterCopyCreateKey{}, (*privateAlterCopyCreate)(nil))
	if r == nil || sql != r.sql {
		return ctx, nil
	}
	if r.closed.Load() || owner == nil || owner != r.owner {
		return ctx, moerr.NewInternalErrorNoCtx("private ALTER COPY CREATE owner changed")
	}
	if !r.claimed.CompareAndSwap(false, true) {
		return ctx, moerr.NewInternalErrorNoCtx("private ALTER COPY CREATE authority already used")
	}
	return context.WithValue(ctx, authorizedPrivateAlterCopyCreateKey{}, r), nil
}

func (c *Compile) validatePrivateAlterCopyCreate(q *plan.CreateTable) (bool, error) {
	r, _ := c.proc.Ctx.Value(authorizedPrivateAlterCopyCreateKey{}).(*privateAlterCopyCreate)
	if r == nil {
		return false, nil
	}
	if q == nil {
		return false, moerr.NewInternalError(c.proc.Ctx, "missing private ALTER COPY CREATE")
	}
	account, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return false, err
	}
	database := q.GetDatabase()
	if database == "" {
		database = c.db
	}
	if r.closed.Load() || r.owner != c.proc.GetTxnOperator() || r.account != account ||
		database != r.database || q.GetTableDef() == nil || q.GetTableDef().GetName() != r.table ||
		q.Temporary || q.Replace || q.IfNotExists || q.CreateAsSelectSql != "" {
		return false, moerr.NewInternalError(c.proc.Ctx, "CREATE exceeds private ALTER COPY preparation")
	}
	return true, nil
}
