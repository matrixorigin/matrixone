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

package frontend

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"go.starlark.net/syntax"
)

type cloneStarlarkEdit struct {
	start       int
	end         int
	replacement string
}

type cloneStarlarkBodyRewriter struct {
	ctx                 context.Context
	body                string
	sqlMode             string
	srcDBName           string
	dstDBName           string
	lowerCaseTableNames int64
	edits               []cloneStarlarkEdit
}

func rewriteCloneStarlarkProcedureBody(
	ctx context.Context,
	body string,
	sqlMode string,
	srcDBName string,
	dstDBName string,
	lowerCaseTableNames int64,
) (string, error) {
	if srcDBName == "" || srcDBName == dstDBName {
		return body, nil
	}
	file, err := matrixoneStarlarkFileOptions.Parse("", body, syntax.RetainComments)
	if err != nil {
		return "", moerr.NewInvalidInputf(ctx, "cannot parse cloned Starlark routine: %v", err)
	}

	rewriter := cloneStarlarkBodyRewriter{
		ctx:                 ctx,
		body:                body,
		sqlMode:             sqlMode,
		srcDBName:           srcDBName,
		dstDBName:           dstDBName,
		lowerCaseTableNames: lowerCaseTableNames,
	}
	if err = rewriter.walkStmts(file.Stmts); err != nil {
		return "", err
	}
	if len(rewriter.edits) == 0 {
		return body, nil
	}

	sort.Slice(rewriter.edits, func(i, j int) bool {
		return rewriter.edits[i].start > rewriter.edits[j].start
	})
	rewritten := body
	for _, edit := range rewriter.edits {
		rewritten = rewritten[:edit.start] + edit.replacement + rewritten[edit.end:]
	}
	return rewritten, nil
}

func (r *cloneStarlarkBodyRewriter) walkStmts(stmts []syntax.Stmt) error {
	for _, stmt := range stmts {
		if err := r.walkStmt(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (r *cloneStarlarkBodyRewriter) walkStmt(stmt syntax.Stmt) error {
	switch stmt := stmt.(type) {
	case *syntax.ExprStmt:
		return r.walkExpr(stmt.X, nil)
	case *syntax.BranchStmt:
		return nil
	case *syntax.IfStmt:
		if err := r.walkExpr(stmt.Cond, nil); err != nil {
			return err
		}
		if err := r.walkStmts(stmt.True); err != nil {
			return err
		}
		return r.walkStmts(stmt.False)
	case *syntax.WhileStmt:
		if err := r.walkExpr(stmt.Cond, nil); err != nil {
			return err
		}
		return r.walkStmts(stmt.Body)
	case *syntax.AssignStmt:
		return r.walkExpr(stmt.RHS, nil)
	case *syntax.DefStmt:
		for _, param := range stmt.Params {
			if err := r.walkParam(param); err != nil {
				return err
			}
		}
		return r.walkStmts(stmt.Body)
	case *syntax.ForStmt:
		if err := r.walkExpr(stmt.X, nil); err != nil {
			return err
		}
		return r.walkStmts(stmt.Body)
	case *syntax.ReturnStmt:
		return r.walkExpr(stmt.Result, nil)
	case *syntax.LoadStmt:
		return nil
	default:
		return moerr.NewNotSupportedf(
			r.ctx,
			"cannot safely clone Starlark routine: unsupported statement %T",
			stmt,
		)
	}
}

func (r *cloneStarlarkBodyRewriter) walkExpr(expr syntax.Expr, parentCall *syntax.CallExpr) error {
	if expr == nil {
		return nil
	}
	if dot, ok := expr.(*syntax.DotExpr); ok && isStarlarkMoSQL(dot) {
		if parentCall == nil || parentCall.Fn != expr {
			return r.unsupportedMoSQL(expr, "must be used as a direct call")
		}
	}

	switch expr := expr.(type) {
	case *syntax.Ident:
		if expr.Name == "mo" {
			return r.unsupportedMoSQL(expr, "module may only be accessed through a direct member call")
		}
		return nil
	case *syntax.Literal:
		return nil
	case *syntax.ListExpr:
		return r.walkExprs(expr.List, nil)
	case *syntax.TupleExpr:
		return r.walkExprs(expr.List, nil)
	case *syntax.ParenExpr:
		return r.walkExpr(expr.X, nil)
	case *syntax.CondExpr:
		if err := r.walkExpr(expr.Cond, nil); err != nil {
			return err
		}
		if err := r.walkExpr(expr.True, nil); err != nil {
			return err
		}
		return r.walkExpr(expr.False, nil)
	case *syntax.IndexExpr:
		if err := r.walkExpr(expr.X, nil); err != nil {
			return err
		}
		return r.walkExpr(expr.Y, nil)
	case *syntax.DictEntry:
		if err := r.walkExpr(expr.Key, nil); err != nil {
			return err
		}
		return r.walkExpr(expr.Value, nil)
	case *syntax.SliceExpr:
		if err := r.walkExpr(expr.X, nil); err != nil {
			return err
		}
		if err := r.walkExpr(expr.Lo, nil); err != nil {
			return err
		}
		if err := r.walkExpr(expr.Hi, nil); err != nil {
			return err
		}
		return r.walkExpr(expr.Step, nil)
	case *syntax.Comprehension:
		if err := r.walkExpr(expr.Body, nil); err != nil {
			return err
		}
		for _, clause := range expr.Clauses {
			switch clause := clause.(type) {
			case *syntax.IfClause:
				if err := r.walkExpr(clause.Cond, nil); err != nil {
					return err
				}
			case *syntax.ForClause:
				if err := r.walkExpr(clause.Vars, nil); err != nil {
					return err
				}
				if err := r.walkExpr(clause.X, nil); err != nil {
					return err
				}
			}
		}
		return nil
	case *syntax.DictExpr:
		return r.walkExprs(expr.List, nil)
	case *syntax.UnaryExpr:
		return r.walkExpr(expr.X, nil)
	case *syntax.BinaryExpr:
		if err := r.walkExpr(expr.X, nil); err != nil {
			return err
		}
		return r.walkExpr(expr.Y, nil)
	case *syntax.DotExpr:
		if ident, ok := expr.X.(*syntax.Ident); ok && ident.Name == "mo" {
			return nil
		}
		return r.walkExpr(expr.X, nil)
	case *syntax.CallExpr:
		if isStarlarkMoSQLCall(expr) {
			if err := r.rewriteMoSQLCall(expr); err != nil {
				return err
			}
		}
		if err := r.walkExpr(expr.Fn, expr); err != nil {
			return err
		}
		for _, arg := range expr.Args {
			if err := r.walkExpr(arg, expr); err != nil {
				return err
			}
		}
		return nil
	case *syntax.LambdaExpr:
		for _, param := range expr.Params {
			if err := r.walkParam(param); err != nil {
				return err
			}
		}
		return r.walkExpr(expr.Body, nil)
	default:
		return moerr.NewNotSupportedf(
			r.ctx,
			"cannot safely clone Starlark routine: unsupported expression %T",
			expr,
		)
	}
}

func (r *cloneStarlarkBodyRewriter) walkParam(param syntax.Expr) error {
	switch param := param.(type) {
	case *syntax.Ident:
		return nil
	case *syntax.BinaryExpr:
		return r.walkExpr(param.Y, nil)
	case *syntax.UnaryExpr:
		return nil
	default:
		return r.walkExpr(param, nil)
	}
}

func (r *cloneStarlarkBodyRewriter) walkExprs(exprs []syntax.Expr, parentCall *syntax.CallExpr) error {
	for _, expr := range exprs {
		if err := r.walkExpr(expr, parentCall); err != nil {
			return err
		}
	}
	return nil
}

func (r *cloneStarlarkBodyRewriter) rewriteMoSQLCall(call *syntax.CallExpr) error {
	if len(call.Args) != 1 {
		return r.unsupportedMoSQL(call, "requires exactly one argument")
	}
	literal, ok := call.Args[0].(*syntax.Literal)
	if !ok || literal.Token != syntax.STRING {
		return r.unsupportedMoSQL(call, "requires a string literal")
	}
	query, ok := literal.Value.(string)
	if !ok {
		return r.unsupportedMoSQL(call, "requires a string literal")
	}
	rewritten, err := rewriteCloneSQLRoutineBody(
		r.ctx,
		query,
		r.sqlMode,
		r.srcDBName,
		r.dstDBName,
		r.lowerCaseTableNames,
	)
	if err != nil {
		return moerr.NewNotSupportedf(
			r.ctx,
			"cannot safely rewrite SQL passed to mo.sql in cloned Starlark routine at line %d: %v",
			syntax.Start(literal).Line,
			err,
		)
	}
	if rewritten == query {
		return nil
	}
	start, err := cloneStarlarkByteOffset(r.body, syntax.Start(literal))
	if err != nil {
		return err
	}
	end, err := cloneStarlarkByteOffset(r.body, syntax.End(literal))
	if err != nil {
		return err
	}
	r.edits = append(r.edits, cloneStarlarkEdit{
		start:       start,
		end:         end,
		replacement: strconv.Quote(rewritten),
	})
	return nil
}

func (r *cloneStarlarkBodyRewriter) unsupportedMoSQL(node syntax.Node, detail string) error {
	return moerr.NewNotSupportedf(
		r.ctx,
		"cannot safely clone Starlark routine: mo.sql at line %d %s",
		syntax.Start(node).Line,
		detail,
	)
}

func isStarlarkMoSQL(expr syntax.Expr) bool {
	dot, ok := expr.(*syntax.DotExpr)
	if !ok {
		return false
	}
	module, ok := dot.X.(*syntax.Ident)
	return ok && module.Name == "mo" && dot.Name.Name == "sql"
}

func isStarlarkMoSQLCall(call *syntax.CallExpr) bool {
	return call != nil && isStarlarkMoSQL(call.Fn)
}

func cloneStarlarkByteOffset(src string, pos syntax.Position) (int, error) {
	line := int32(1)
	offset := 0
	for line < pos.Line {
		next := strings.IndexByte(src[offset:], '\n')
		if next < 0 {
			return 0, moerr.NewInternalErrorNoCtxf("invalid Starlark source position at line %d", pos.Line)
		}
		offset += next + 1
		line++
	}
	for col := int32(1); col < pos.Col; col++ {
		if offset >= len(src) {
			return 0, moerr.NewInternalErrorNoCtxf("invalid Starlark source position at line %d column %d", pos.Line, pos.Col)
		}
		_, size := utf8.DecodeRuneInString(src[offset:])
		offset += size
	}
	return offset, nil
}
