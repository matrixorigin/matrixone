// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// Task storage uses authenticated MySQL connections, so a context flag in the
// task service would not reach its CN. Require the in-memory service identity
// here and restrict the allowance to single-table task polling/heartbeat/cleanup.
func taskControlMemoryStatement(ctx context.Context, ses *Session, stmt tree.Statement) bool {
	tenant := ses.GetTenantInfo()
	if tenant == nil || !tenant.IsSysTenant() {
		return false
	}
	if special, _, _ := isSpecialUser(tenant.GetUser()); !special {
		return false
	}
	if prepare, ok := stmt.(*tree.PrepareStmt); ok {
		stmt = prepare.Stmt
	}
	stmt, database, err := effectiveStatementForTxn(ctx, ses, stmt)
	if err != nil {
		return false
	}
	if database == "" {
		database = ses.GetDatabaseName()
	}
	return isTaskControlMemoryStatement(stmt, database)
}

func isTaskControlMemoryStatement(stmt tree.Statement, database string) bool {
	var tables tree.TableExprs
	switch s := stmt.(type) {
	case *tree.Select:
		if s.With != nil || s.Ep != nil || len(s.IntoVars) != 0 {
			return false
		}
		clause, ok := s.Select.(*tree.SelectClause)
		if !ok || clause.From == nil || clause.IntoExport != nil {
			return false
		}
		tables = clause.From.Tables
	case *tree.Update:
		if s.With != nil || s.From != nil || s.MultiTable {
			return false
		}
		tables = s.Tables
	case *tree.Delete:
		if s.With != nil || len(s.Tables) != 1 {
			return false
		}
		tables = s.Tables
		if len(s.TableRefs) != 0 {
			return false
		}
	default:
		return false
	}
	if len(tables) != 1 {
		return false
	}
	var expr tree.TableExpr = tables[0]
	for {
		switch t := expr.(type) {
		case *tree.AliasedTableExpr:
			expr = t.Expr
		case *tree.JoinTableExpr:
			if t.Right != nil {
				return false
			}
			expr = t.Left
		case *tree.TableName:
			if t.ExplicitSchema {
				database = string(t.SchemaName)
			}
			if !strings.EqualFold(database, "mo_task") {
				return false
			}
			switch strings.ToLower(string(t.ObjectName)) {
			case "sys_async_task", "sys_cron_task", "sys_daemon_task":
				return true
			}
			return false
		default:
			return false
		}
	}
}
