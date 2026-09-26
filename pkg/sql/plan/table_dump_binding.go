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

package plan

import (
	"context"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

const (
	tableDumpMaxExpressionText  = 1 << 20
	tableDumpMaxExpressionNodes = 16_384
	tableDumpMaxCandidateVisits = 1_000_000
)

type tableDumpBindingBudget struct {
	text, nodes, visits int
}

type tableDumpExpressionVisitor struct {
	nodes       int
	hasDivision bool
	hasSpecial  bool
}

func (v *tableDumpExpressionVisitor) Enter(e tree.Expr) (tree.Expr, bool) {
	v.nodes++
	if binary, ok := e.(*tree.BinaryExpr); ok && binary.Op == tree.DIV {
		v.hasDivision = true
	}
	if fn, ok := e.(*tree.FuncExpr); ok {
		switch numericAstFunctionName(fn) {
		case "format", "makedate", "maketime":
			v.hasSpecial = true
		}
	}
	return e, false
}

func (v *tableDumpExpressionVisitor) Exit(e tree.Expr) (tree.Expr, bool) {
	return e, true
}

type tableDumpBindContext struct {
	CompilerContext
	ctx       context.Context
	increment int64
}

func (c tableDumpBindContext) GetContext() context.Context { return c.ctx }

func (c tableDumpBindContext) ResolveVariable(name string, system, global bool) (any, error) {
	if strings.EqualFold(name, "div_precision_increment") && system && !global {
		return c.increment, nil
	}
	return c.CompilerContext.ResolveVariable(name, system, global)
}

type tableDumpExpression struct {
	kind, name, origin string
	col                *planpb.ColDef
	check              *planpb.CheckDef
	expr               *planpb.Expr
}

func tableDumpExpressions(def *planpb.TableDef) []tableDumpExpression {
	var result []tableDumpExpression
	for _, col := range def.Cols {
		if col == nil {
			continue
		}
		if col.Default != nil && col.Default.Expr != nil && strings.Contains(col.Default.OriginString, "/") {
			result = append(result, tableDumpExpression{"default", col.Name, col.Default.OriginString, col, nil, col.Default.Expr})
		}
		if col.OnUpdate != nil && col.OnUpdate.Expr != nil && strings.Contains(col.OnUpdate.OriginString, "/") {
			result = append(result, tableDumpExpression{"on_update", col.Name, col.OnUpdate.OriginString, col, nil, col.OnUpdate.Expr})
		}
		if col.GeneratedCol != nil && col.GeneratedCol.Expr != nil && strings.Contains(col.GeneratedCol.OriginString, "/") {
			result = append(result, tableDumpExpression{"generated", col.Name, col.GeneratedCol.OriginString, col, nil, col.GeneratedCol.Expr})
		}
	}
	for _, check := range def.Checks {
		if check != nil && check.Check != nil && strings.Contains(check.OriginSql, "/") {
			result = append(result, tableDumpExpression{"check", check.Name, check.OriginSql, nil, check, check.Check})
		}
	}
	return result
}

func tableDumpParserModes() []string {
	flags := []string{"NO_BACKSLASH_ESCAPES", "ANSI_QUOTES", "PIPES_AS_CONCAT"}
	modes := make([]string, 0, 8)
	for mask := 0; mask < 8; mask++ {
		var parts []string
		for i, flag := range flags {
			if mask&(1<<i) != 0 {
				parts = append(parts, flag)
			}
		}
		modes = append(modes, strings.Join(parts, ","))
	}
	return modes
}

func parseTableDumpExpression(ctx context.Context, origin, mode string) (tree.Statement, tree.Expr, *tableDumpExpressionVisitor, error) {
	stmt, err := parsers.ParseOneWithSQLMode(ctx, dialect.MYSQL, "select "+origin, 1, mode)
	if err != nil {
		return nil, nil, nil, err
	}
	selectStmt, ok := stmt.(*tree.Select)
	if !ok {
		stmt.Free()
		return nil, nil, nil, moerr.NewInvalidInput(ctx, "invalid persisted table expression")
	}
	clause, ok := selectStmt.Select.(*tree.SelectClause)
	if !ok || len(clause.Exprs) != 1 {
		stmt.Free()
		return nil, nil, nil, moerr.NewInvalidInput(ctx, "invalid persisted table expression")
	}
	expr := clause.Exprs[0].Expr
	visitor := &tableDumpExpressionVisitor{}
	if _, ok = expr.Accept(visitor); !ok {
		stmt.Free()
		return nil, nil, nil, moerr.NewInvalidInput(ctx, "invalid persisted table expression")
	}
	return stmt, expr, visitor, nil
}

func bindTableDumpExpression(ctx CompilerContext, def *planpb.TableDef, item tableDumpExpression, ast tree.Expr, increment int64, legacy bool) (*planpb.Expr, error) {
	bindCtx := function.WithDivPrecisionIncrement(ctx.GetContext(), int32(increment))
	if legacy {
		bindCtx = function.WithLegacySpecialConsumers(bindCtx)
	}
	proc := ctx.GetProcess()
	if proc == nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "table dump expression binding requires a process")
	}
	switch item.kind {
	case "default":
		col := &tree.ColumnTableDef{
			Name: tree.NewUnresolvedColName(item.name),
			Attributes: []tree.ColumnAttribute{
				&tree.AttributeNull{Is: item.col.Default.NullAbility},
				&tree.AttributeDefault{Expr: ast},
			},
		}
		bound, err := buildDefaultExprWithColumns(bindCtx, col, item.col.Typ, proc, def.Cols)
		if err != nil {
			return nil, err
		}
		return bound.Expr, nil
	case "on_update":
		col := &tree.ColumnTableDef{
			Name:       tree.NewUnresolvedColName(item.name),
			Attributes: []tree.ColumnAttribute{&tree.AttributeOnUpdate{Expr: ast}},
		}
		bound, err := buildOnUpdate(bindCtx, col, item.col.Typ, proc)
		if err != nil {
			return nil, err
		}
		return bound.Expr, nil
	case "generated":
		col := &tree.ColumnTableDef{
			Name:       tree.NewUnresolvedColName(item.name),
			Attributes: []tree.ColumnAttribute{&tree.AttributeGeneratedAlways{Expr: ast, Stored: item.col.GeneratedCol.IsStored}},
		}
		cols := make([]*planpb.ColDef, 0, len(def.Cols))
		for _, candidate := range def.Cols {
			if candidate != nil && !candidate.Hidden {
				cols = append(cols, candidate)
			}
		}
		bound, err := buildGeneratedExpr(bindCtx, col, item.col.Typ, cols, proc)
		if err != nil {
			return nil, err
		}
		if exprReferencesColumn(bound.Expr, item.name, cols) {
			return nil, moerr.NewInvalidInputf(ctx.GetContext(), "generated column '%s' cannot refer to itself", item.name)
		}
		return bound.Expr, nil
	case "check":
		wrapper := tableDumpBindContext{CompilerContext: ctx, ctx: bindCtx, increment: increment}
		scratch := *def
		scratch.Checks = nil
		if err := appendCheckDef(wrapper, &scratch, item.name, ast, -1); err != nil {
			return nil, err
		}
		if len(scratch.Checks) != 1 {
			return nil, moerr.NewInternalError(ctx.GetContext(), "check expression binding did not produce one check")
		}
		return scratch.Checks[0].Check, nil
	default:
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "unknown persisted table expression kind")
	}
}

func sameTableDumpBoundExpression(a, b *planpb.Expr) bool {
	if a == nil || b == nil {
		return a == b
	}
	left := proto.Clone(a).(*planpb.Expr)
	right := proto.Clone(b).(*planpb.Expr)
	var normalize func(*planpb.Expr)
	normalize = func(expr *planpb.Expr) {
		if expr == nil {
			return
		}
		// Type.Table is catalog lineage. Rebinding a persisted expression
		// against its current table fills it in, while older stored trees do
		// not carry it. It does not affect overload or value semantics.
		expr.Typ.Table = ""
		if f := expr.GetF(); f != nil {
			for _, arg := range f.Args {
				normalize(arg)
			}
		}
		if list := expr.GetList(); list != nil {
			for _, entry := range list.List {
				normalize(entry)
			}
		}
		if lit := expr.GetLit(); lit != nil {
			normalize(lit.Src)
		}
	}
	normalize(left)
	normalize(right)
	return proto.Equal(left, right)
}

// AnalyzeTableDumpBindings proves that every supplied bound expression is
// reachable from its SQL origin through ordinary DDL binding. It reports
// whether any expression's value or bound type depends on the division setting.
// A nil supplied definition classifies a v1 target without trusting a payload.
func AnalyzeTableDumpBindings(ctx CompilerContext, target, supplied *planpb.TableDef) (bool, error) {
	if ctx == nil || target == nil {
		return false, moerr.NewInvalidInputNoCtx("table dump binding context is unavailable")
	}
	items := tableDumpExpressions(target)
	var sourceItems []tableDumpExpression
	if supplied != nil {
		sourceItems = tableDumpExpressions(supplied)
		if len(sourceItems) != len(items) {
			return false, moerr.NewInvalidInputNoCtx("table dump expression count does not match target")
		}
	}
	budget := &tableDumpBindingBudget{}
	var sensitive bool
	for i, item := range items {
		if supplied != nil && (sourceItems[i].kind != item.kind || !strings.EqualFold(sourceItems[i].name, item.name) || sourceItems[i].origin != item.origin) {
			return false, moerr.NewInvalidInputNoCtx("table dump expression declaration does not match target")
		}
		if len(item.origin) == 0 {
			return false, moerr.NewInvalidInputNoCtx("persisted table expression has no SQL origin")
		}
		// div_precision_increment only affects exact division. Avoid planning
		// ordinary defaults and checks on the common v1 path.
		if !strings.Contains(item.origin, "/") {
			continue
		}
		budget.text += len(item.origin)
		if budget.text > tableDumpMaxExpressionText {
			return false, moerr.NewInvalidInputNoCtx("table dump expression text exceeds limit")
		}
		var matched bool
		var parsed bool
		for _, mode := range tableDumpParserModes() {
			stmt, ast, visit, err := parseTableDumpExpression(ctx.GetContext(), item.origin, mode)
			if err != nil {
				continue
			}
			parsed = true
			budget.nodes += visit.nodes
			if budget.nodes > tableDumpMaxExpressionNodes {
				stmt.Free()
				return false, moerr.NewInvalidInputNoCtx("table dump expression AST exceeds limit")
			}
			maxIncrement := int64(0)
			if visit.hasDivision {
				maxIncrement = 30
			}
			var first, legacyFirst *planpb.Expr
			var matchedLegacy bool
			want := item.expr
			if supplied != nil {
				want = sourceItems[i].expr
			}
			for increment := int64(0); increment <= maxIncrement; increment++ {
				budget.visits += visit.nodes
				if budget.visits > tableDumpMaxCandidateVisits {
					stmt.Free()
					return false, moerr.NewInvalidInputNoCtx("table dump expression binding exceeds limit")
				}
				candidate, err := bindTableDumpExpression(ctx, target, item, ast, increment, false)
				if err == nil {
					if first == nil {
						first = candidate
					} else if visit.hasDivision && !sameTableDumpBoundExpression(first, candidate) {
						sensitive = true
					}
					matched = matched || sameTableDumpBoundExpression(want, candidate)
				}
				if visit.hasSpecial && (!matched || matchedLegacy) {
					budget.visits += visit.nodes
					if budget.visits > tableDumpMaxCandidateVisits {
						stmt.Free()
						return false, moerr.NewInvalidInputNoCtx("table dump expression binding exceeds limit")
					}
					legacy, err := bindTableDumpExpression(ctx, target, item, ast, increment, true)
					if err == nil {
						if legacyFirst == nil {
							legacyFirst = legacy
						} else if visit.hasDivision && !sameTableDumpBoundExpression(legacyFirst, legacy) {
							sensitive = true
						}
						if sameTableDumpBoundExpression(want, legacy) {
							matched = true
							matchedLegacy = true
						}
					}
				}
				if matched && sensitive {
					break
				}
			}
			stmt.Free()
			if matched {
				break
			}
		}
		if !parsed || !matched {
			return false, moerr.NewInvalidInputNoCtxf("table dump cannot verify bound %s expression %s", item.kind, item.name)
		}
	}
	return sensitive, nil
}
