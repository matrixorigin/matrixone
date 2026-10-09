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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func crc32PreparedMigrationSafe(ctx context.Context, ses *Session, st *PrepareStmt) bool {
	if st == nil || st.PreparePlan == nil {
		return false
	}
	err := plan.VisitExpressionsInOwner(st.PreparePlan, func(root *plan.Expr) error {
		return plan.VisitExprTree(root, func(expr *plan.Expr) error {
			if err := context.Cause(ctx); err != nil {
				return err
			}
			fn := expr.GetF()
			if fn == nil || fn.Func == nil || int32(fn.Func.Obj>>32) != function.CRC32 {
				return nil
			}
			if int32(fn.Func.Obj) != plan.CRC32LegacyOverload || len(fn.Args) != 1 || fn.Args[0] == nil ||
				types.T(fn.Args[0].Typ.Id) == types.T_json || types.T(fn.Args[0].Typ.Id) == types.T_any {
				return moerr.GetOkExpectedNotSafeToStartTransfer()
			}
			return nil
		})
	})
	if err != nil || (st.PrepareStmt != nil && !crc32ReplayStatementSafe(st.PrepareStmt)) {
		return false
	}
	// SQL survives optimizer pruning; the saved AST also sees mandatory rewrite
	// policy applied before planning. Require both where both are available.
	mode := sessionSQLModeForParser(ses)
	if st.sqlModeFlagsSet {
		mode = st.schedulingSQLMode
	}
	return crc32ReplaySQLSafe(ctx, st.Sql, mode, true)
}

func checkCRC32PrepareMigration(ctx context.Context, ses *Session, req *query.MigrateConnToRequest) error {
	if len(req.PrepareStmts) == 0 {
		return nil
	}
	mode := sessionSQLModeForParser(ses)
	// PREPARE runs after restoring the typed source sql_mode. Decode that value
	// without installing any session state; do not parse under a different mode.
	seenMode := false
	for _, variable := range req.SystemVariables {
		if variable == nil || !strings.EqualFold(variable.Name, "sql_mode") {
			continue
		}
		if !req.SystemVariablesExported || seenMode || variable.NextTransaction {
			return moerr.GetOkExpectedNotSafeToStartTransfer()
		}
		value, err := decodeUserDefinedVarValue(ctx, variable.Value)
		if err != nil {
			return moerr.GetOkExpectedNotSafeToStartTransfer()
		}
		var ok bool
		mode, ok = value.(string)
		if !ok {
			return moerr.GetOkExpectedNotSafeToStartTransfer()
		}
		mode = mysql.SessionSQLModeForParser(mode)
		seenMode = true
	}
	for _, st := range req.PrepareStmts {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if st == nil || !crc32ReplaySQLSafe(ctx, st.SQL, mode, true) {
			return moerr.GetOkExpectedNotSafeToStartTransfer()
		}
		if !req.SystemVariablesExported && len(req.SetVarStmts) != 0 {
			// Legacy SET replay may change sql_mode through an evaluated value.
			// Do not execute that SQL to discover the mode: require the statement
			// to be safe under every combination of the parser's six flags.
			flags := []string{"ANSI_QUOTES", "PIPES_AS_CONCAT", "NO_BACKSLASH_ESCAPES", "REAL_AS_FLOAT", "HIGH_NOT_PRECEDENCE", "IGNORE_SPACE"}
			for mask := 0; mask < 1<<len(flags); mask++ {
				if err := context.Cause(ctx); err != nil {
					return err
				}
				var modes []string
				for bit, flag := range flags {
					if mask&(1<<bit) != 0 {
						modes = append(modes, flag)
					}
				}
				if !crc32ReplaySQLSafe(ctx, st.SQL, strings.Join(modes, ","), true) {
					return moerr.GetOkExpectedNotSafeToStartTransfer()
				}
			}
		}
	}
	return nil
}

func crc32ReplaySQLSafe(ctx context.Context, sql, mode string, unwrapPrepare bool) bool {
	stmts, err := mysql.ParseWithSQLMode(ctx, sql, 1, mode)
	if err != nil {
		return false
	}
	defer func() {
		for _, stmt := range stmts {
			// PrepareStmt.Free resets only its wrapper. This temporary parse,
			// unlike a session-owned PREPARE, also owns the inner statement.
			if prepare, ok := stmt.(*tree.PrepareStmt); ok && prepare.Stmt != nil {
				prepare.Stmt.Free()
			}
			stmt.Free()
		}
	}()
	if len(stmts) != 1 {
		return false
	}
	if unwrapPrepare {
		switch st := stmts[0].(type) {
		case *tree.PrepareStmt:
			return crc32ReplayStatementSafe(st.Stmt)
		case *tree.PrepareString:
			return crc32ReplaySQLSafe(ctx, st.Sql, mode, false)
		}
	}
	return crc32ReplayStatementSafe(stmts[0])
}

// Without bound-plan transport even a CRC-free SELECT from a view can hide a
// legacy CRC32 expression. Only a closed scalar SELECT has a local proof here.
// Unknown statement/expression/function forms stay on the original CN until
// DEALLOCATE; this is deliberately an availability restriction, not a claim
// that every rejected statement contains CRC32. No catalog lookup or SQL runs.
func crc32ReplayStatementSafe(stmt tree.Statement) bool {
	sel, ok := stmt.(*tree.Select)
	if !ok || sel == nil || sel.IsPerform || sel.With != nil || sel.RewriteOption != nil ||
		sel.TimeWindow != nil || sel.RankOption != nil || sel.Limit != nil || len(sel.OrderBy) != 0 ||
		sel.Ep != nil || len(sel.IntoVars) != 0 || sel.SelectLockInfo != nil {
		return false
	}
	clause, ok := sel.Select.(*tree.SelectClause)
	if !ok || clause == nil || clause.Where != nil || clause.GroupBy != nil || clause.Having != nil ||
		len(clause.Windows) != 0 || clause.IntoExport != nil || len(clause.IntoVars) != 0 || len(clause.Exprs) == 0 {
		return false
	}
	if clause.From != nil && len(clause.From.Tables) != 0 {
		// The parser represents an omitted FROM with one empty TableName.
		if len(clause.From.Tables) != 1 {
			return false
		}
		alias, ok := clause.From.Tables[0].(*tree.AliasedTableExpr)
		if !ok || alias == nil {
			return false
		}
		table, ok := alias.Expr.(*tree.TableName)
		if !ok || table == nil || table.ObjectName != "" || table.ExplicitSchema || table.ExplicitCatalog {
			return false
		}
	}
	for _, expr := range clause.Exprs {
		if !crc32ReplayExprSafe(expr.Expr, 0) {
			return false
		}
	}
	return true
}

func crc32ReplayExprSafe(expr tree.Expr, depth int) bool {
	if depth >= 64 {
		return false
	}
	switch e := expr.(type) {
	case *tree.NumVal:
		return e != nil
	case *tree.ParamExpr:
		return e != nil
	case *tree.VarExpr:
		return e != nil && e.Expr == nil // a value, not SQL containing an expression
	case *tree.ParenExpr:
		return e != nil && crc32ReplayExprSafe(e.Expr, depth+1)
	case *tree.UnaryExpr:
		return e != nil && crc32ReplayExprSafe(e.Expr, depth+1)
	case *tree.BinaryExpr:
		return e != nil && crc32ReplayExprSafe(e.Left, depth+1) && crc32ReplayExprSafe(e.Right, depth+1)
	case *tree.ComparisonExpr:
		return e != nil && crc32ReplayExprSafe(e.Left, depth+1) && crc32ReplayExprSafe(e.Right, depth+1) &&
			(e.Escape == nil || crc32ReplayExprSafe(e.Escape, depth+1))
	case *tree.AndExpr:
		return e != nil && crc32ReplayExprSafe(e.Left, depth+1) && crc32ReplayExprSafe(e.Right, depth+1)
	case *tree.OrExpr:
		return e != nil && crc32ReplayExprSafe(e.Left, depth+1) && crc32ReplayExprSafe(e.Right, depth+1)
	case *tree.XorExpr:
		return e != nil && crc32ReplayExprSafe(e.Left, depth+1) && crc32ReplayExprSafe(e.Right, depth+1)
	case *tree.NotExpr:
		return e != nil && crc32ReplayExprSafe(e.Expr, depth+1)
	case *tree.IsNullExpr:
		return e != nil && crc32ReplayExprSafe(e.Expr, depth+1)
	case *tree.IsNotNullExpr:
		return e != nil && crc32ReplayExprSafe(e.Expr, depth+1)
	case *tree.RangeCond:
		return e != nil && crc32ReplayExprSafe(e.Left, depth+1) &&
			crc32ReplayExprSafe(e.From, depth+1) && crc32ReplayExprSafe(e.To, depth+1)
	case *tree.CaseExpr:
		if e == nil || len(e.Whens) == 0 || (e.Expr != nil && !crc32ReplayExprSafe(e.Expr, depth+1)) ||
			(e.Else != nil && !crc32ReplayExprSafe(e.Else, depth+1)) {
			return false
		}
		// Inspect every branch, including ones the optimizer may later prune.
		for _, when := range e.Whens {
			if when == nil || !crc32ReplayExprSafe(when.Cond, depth+1) || !crc32ReplayExprSafe(when.Val, depth+1) {
				return false
			}
		}
		return true
	case *tree.CastExpr:
		return e != nil && crc32ReplayExprSafe(e.Expr, depth+1)
	case *tree.FuncExpr:
		if e == nil || e.FuncName == nil || e.IsGeneric || e.WindowSpec != nil || len(e.OrderBy) != 0 || e.Type != tree.FUNC_TYPE_DEFAULT {
			return false
		}
		name, ok := e.Func.FunctionReference.(*tree.UnresolvedName)
		if !ok || name.NumParts != 1 {
			return false
		}
		switch strings.ToLower(e.FuncName.Origin()) {
		case "crc32":
			if len(e.Exprs) != 1 {
				return false
			}
			literal, ok := e.Exprs[0].(*tree.NumVal)
			if !ok || literal == nil {
				return false // JSON casts, columns, variables and ANY parameters
			}
			switch literal.ValType {
			case tree.P_char, tree.P_bool, tree.P_int64, tree.P_uint64, tree.P_float64, tree.P_decimal:
				return true // both historical and new binders keep overload zero
			default:
				return false
			}
		case "char_length", "length", "charset", "concat", "coalesce", "ifnull":
			for _, arg := range e.Exprs {
				if !crc32ReplayExprSafe(arg, depth+1) {
					return false
				}
			}
			return true
		}
	}
	return false
}
