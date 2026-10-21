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
	"math"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
	nodes              int
	hasDivision        bool
	hasIntegerDivision bool
	hasSpecial         bool
	hasTimestampAdd    bool
}

func (v *tableDumpExpressionVisitor) Enter(e tree.Expr) (tree.Expr, bool) {
	v.nodes++
	if binary, ok := e.(*tree.BinaryExpr); ok {
		v.hasDivision = v.hasDivision || binary.Op == tree.DIV
		v.hasIntegerDivision = v.hasIntegerDivision || binary.Op == tree.INTEGER_DIV
	}
	if fn, ok := e.(*tree.FuncExpr); ok {
		switch numericAstFunctionName(fn) {
		case "format", "makedate", "maketime":
			v.hasSpecial = true
		case "timestampadd":
			v.hasTimestampAdd = true
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
	proc      *process.Process
}

func (c tableDumpBindContext) GetContext() context.Context { return c.ctx }

func (c tableDumpBindContext) GetProcess() *process.Process {
	if c.proc != nil {
		return c.proc
	}
	return c.CompilerContext.GetProcess()
}

// Rebinding changes expression policy, not ownership of catalog execution.
func (c tableDumpBindContext) getInternalSQLExecutor(proc *process.Process) (executor.SQLExecutor, bool) {
	if provider, ok := c.CompilerContext.(interface {
		getInternalSQLExecutor(*process.Process) (executor.SQLExecutor, bool)
	}); ok {
		return provider.getInternalSQLExecutor(proc)
	}
	return nil, false
}

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
		if col.Default != nil && col.Default.Expr != nil {
			result = append(result, tableDumpExpression{"default", col.Name, col.Default.OriginString, col, nil, col.Default.Expr})
		}
		if col.OnUpdate != nil && col.OnUpdate.Expr != nil {
			result = append(result, tableDumpExpression{"on_update", col.Name, col.OnUpdate.OriginString, col, nil, col.OnUpdate.Expr})
		}
		if col.GeneratedCol != nil && col.GeneratedCol.Expr != nil {
			result = append(result, tableDumpExpression{"generated", col.Name, col.GeneratedCol.OriginString, col, nil, col.GeneratedCol.Expr})
		}
	}
	for _, check := range def.Checks {
		if check != nil && check.Check != nil {
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

type tableDumpBindingMode uint8

const (
	tableDumpBindingCurrent tableDumpBindingMode = iota
	tableDumpBindingLegacyTimestampAdd
	tableDumpBindingLegacyDecimalIntegerDiv
	tableDumpBindingLegacySpecial
	tableDumpBindingImplicitTimestamp
	tableDumpBindingLegacyZeroTemporalDefault
)

// A flag selects one coherent pre-migration binding, never per-node metadata.
const tableDumpBindingPreTemporalNullability tableDumpBindingMode = 1 << 7

type tableDumpDefaultSession struct {
	timeZone, locale, folded bool
}

func bindTableDumpExpression(ctx CompilerContext, def *planpb.TableDef, item tableDumpExpression, ast tree.Expr, increment int64, mode tableDumpBindingMode, session *tableDumpDefaultSession) (*planpb.Expr, error) {
	bindCtx := function.WithDivPrecisionIncrement(ctx.GetContext(), int32(increment))
	baseMode := mode &^ tableDumpBindingPreTemporalNullability
	if mode&tableDumpBindingPreTemporalNullability != 0 {
		bindCtx = function.WithLegacyTemporalNullability(bindCtx)
	}
	switch baseMode {
	case tableDumpBindingLegacyTimestampAdd:
		bindCtx = function.WithLegacyTimestampAddSecond(bindCtx)
	case tableDumpBindingLegacyDecimalIntegerDiv:
		bindCtx = function.WithLegacyDecimalIntegerDiv(bindCtx)
	case tableDumpBindingLegacySpecial:
		bindCtx = function.WithLegacySpecialConsumers(bindCtx)
	}
	proc := ctx.GetProcess()
	if proc == nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "table dump expression binding requires a process")
	}
	if baseMode == tableDumpBindingImplicitTimestamp {
		// Old implicit defaults and ON UPDATE clauses saved CURRENT_TIMESTAMP()
		// as their origin, but used the column precision and this distinct DDL
		// owner. Reproduce that whole tree, including its assignment cast.
		return buildImplicitCurrentTimestampExpr(item.col.Typ, proc)
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
		purpose := defaultExprForPersistedVerification
		if baseMode == tableDumpBindingLegacyZeroTemporalDefault {
			purpose = defaultExprForPersistedLegacyZero
		}
		var unfolded *planpb.Expr
		bound, err := buildDefaultExprWithColumnsObserved(bindCtx, col, item.col.Typ, proc, def.Cols, purpose, func(source, assigned *planpb.Expr) {
			// Observe before assignment/folding can fail in the reading zone.
			// Inspect the resolved source, not the assignment cast's target type:
			// a plain NULL/zero TIMESTAMP needs no timezone reconstruction.
			tz, locale := tableDumpDefaultDependencies(source)
			session.timeZone = session.timeZone || tz
			session.locale = session.locale || locale
			unfolded = assigned
		})
		if err != nil {
			return nil, err
		}
		if (session.timeZone || session.locale) && !sameTableDumpBoundExpression(unfolded, bound.Expr) {
			session.folded = true
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

// tableDumpBoundExpressionNeedsNormalization follows exactly the edges that
// the compatibility normalization below changes. Other expression variants
// must retain their exact protobuf comparison semantics.
func tableDumpBoundExpressionNeedsNormalization(expr *planpb.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.Typ.Table != "" || (expr.Typ.Id == int32(types.T_time) && expr.Typ.Width != 0 && expr.Typ.Width == expr.Typ.Scale) {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if tableDumpBoundExpressionNeedsNormalization(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, entry := range list.List {
			if tableDumpBoundExpressionNeedsNormalization(entry) {
				return true
			}
		}
	}
	if lit := expr.GetLit(); lit != nil {
		return tableDumpBoundExpressionNeedsNormalization(lit.Src)
	}
	return false
}

func sameTableDumpBoundExpression(a, b *planpb.Expr) bool {
	if a == nil || b == nil {
		return a == b
	}
	// Canonical trees need no compatibility rewrite. Avoid copying both
	// complete trees for each speculative binding in the witness search.
	if !tableDumpBoundExpressionNeedsNormalization(a) && !tableDumpBoundExpressionNeedsNormalization(b) {
		return proto.Equal(a, b)
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
		// Older TIME trees carry Scale but leave Width unset. New binding
		// mirrors Scale into Width for DDL metadata; the stored Scale is the
		// execution precision and must still match exactly.
		if expr.Typ.Id == int32(types.T_time) && expr.Typ.Width == expr.Typ.Scale {
			expr.Typ.Width = 0
		}
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

func tableDumpImplicitTimestamp(item tableDumpExpression, ast tree.Expr) bool {
	if item.col == nil || item.col.Typ.Id != int32(types.T_timestamp) ||
		(item.kind != "default" && item.kind != "on_update") {
		return false
	}
	if item.kind == "default" && item.col.Default.NullAbility {
		return false
	}
	fn, ok := unwrapParenExpr(ast).(*tree.FuncExpr)
	return ok && numericAstFunctionName(fn) == "current_timestamp" && len(fn.Exprs) == 0
}

func tableDumpTimestampLiteral(expr *planpb.Expr) (int64, bool) {
	if expr == nil || expr.Typ.Id != int32(types.T_timestamp) {
		return 0, false
	}
	lit := expr.GetLit()
	if lit == nil || lit.Isnull {
		return 0, false
	}
	value, ok := lit.Value.(*planpb.Literal_Timestampval)
	if !ok || value.Timestampval == int64(types.ZeroTimestamp) {
		return 0, false
	}
	return value.Timestampval, true
}

// Inspect resolved types before constant folding. Implicit TIMESTAMP casts can
// occur anywhere in a default, including beneath a non-temporal result. The
// lexical conversions below read the session zone without a TIMESTAMP input;
// UUID's deterministic datetime overload does so at its epoch conversion too.
// Localized name/format consumers read lc_time_names independently of the zone.
func tableDumpDefaultDependencies(expr *planpb.Expr) (zone, locale bool) {
	if expr == nil {
		return false, false
	}
	zone = expr.Typ.Id == int32(types.T_timestamp)
	merge := func(child *planpb.Expr) {
		childZone, childLocale := tableDumpDefaultDependencies(child)
		zone, locale = zone || childZone, locale || childLocale
	}
	if fn := expr.GetF(); fn != nil {
		switch fn.Func.ObjName {
		case "from_unixtime", "unix_timestamp", "microsecond":
			zone = true
		case "monthname", "dayname", "date_format":
			locale = true
		case "uuid", "uuid_v1", "uuid_v6", "uuid_v7":
			for _, arg := range fn.Args {
				if arg.Typ.Id == int32(types.T_datetime) {
					zone = true
				}
			}
		}
		for _, arg := range fn.Args {
			merge(arg)
		}
	}
	if list := expr.GetList(); list != nil {
		for _, entry := range list.List {
			merge(entry)
		}
	}
	if literal := expr.GetLit(); literal != nil {
		merge(literal.Src)
	}
	return zone, locale
}

func matchTableDumpTimestampDefault(want, candidate *planpb.Expr, zone *time.Location, bind func(*time.Location) (*planpb.Expr, error)) bool {
	utc := candidate
	if zone != time.UTC || utc == nil {
		var err error
		utc, err = bind(time.UTC)
		if err != nil {
			return false
		}
	}
	utcValue, utcOK := tableDumpTimestampLiteral(utc)
	storedValue, storedOK := tableDumpTimestampLiteral(want)
	if !utcOK || !storedOK {
		return false
	}
	// A probe in UTC may cross a TIMESTAMP boundary that the creation offset
	// avoided. Check subtraction itself rather than rejecting that probe.
	if (storedValue < 0 && utcValue > math.MaxInt64+storedValue) ||
		(storedValue > 0 && utcValue < math.MinInt64+storedValue) {
		return false
	}
	// A legal fixed offset proves origin reachability, not payload authenticity.
	// The full
	// rebind still verifies the expression's type, precision and every field.
	delta := utcValue - storedValue
	const minute = 60 * types.MicroSecsPerSec
	if delta%minute != 0 || delta <= -14*60*minute || delta > 14*60*minute {
		return false
	}
	tz := time.FixedZone("table dump binding", int(delta/types.MicroSecsPerSec))
	rebound, err := bind(tz)
	return err == nil && sameTableDumpBoundExpression(want, rebound)
}

// AnalyzeTableDumpBindings proves that every supplied bound expression is
// reachable from its SQL origin through ordinary DDL binding. It reports
// whether a binding varies across supported migration eras or division precision.
// A nil supplied definition classifies a v1 target without trusting a payload.
func AnalyzeTableDumpBindings(ctx CompilerContext, target, supplied *planpb.TableDef) (bool, error) {
	return analyzeTableDumpBindingsWithCandidateVisitLimit(ctx, target, supplied, tableDumpMaxCandidateVisits)
}

func analyzeTableDumpBindingsWithCandidateVisitLimit(ctx CompilerContext, target, supplied *planpb.TableDef, candidateVisitLimit int) (bool, error) {
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
	if len(items) == 0 {
		return false, nil
	}
	parent := ctx.GetProcess()
	if parent == nil {
		return false, moerr.NewInternalError(ctx.GetContext(), "table dump expression binding requires a process")
	}
	// Candidate evaluation owns its session configuration and diagnostics. It
	// must not change the backup session or publish speculative warnings.
	proc := parent.NewViewBindingProcess(ctx.GetContext())
	defer proc.Free()
	proc.WarningSink = &process.WarningProbe{}
	proc.Base.UnixTime = parent.Base.UnixTime
	proc.GetSessionInfo().DefaultWeekFormat = parent.GetSessionInfo().DefaultWeekFormat
	proc.GetSessionInfo().DefaultWeekFormatSet = parent.GetSessionInfo().DefaultWeekFormatSet
	proc.GetSessionInfo().LCTimeNames = parent.GetSessionInfo().LCTimeNames
	locale := function.TemporalLocaleNameForProcess(proc)
	resolve := proc.GetResolveVariableFunc()
	proc.SetResolveVariableFunc(func(name string, system, global bool) (any, error) {
		if system && !global && strings.EqualFold(name, "lc_time_names") {
			return proc.GetSessionInfo().LCTimeNames, nil
		}
		if resolve != nil {
			return resolve(name, system, global)
		}
		return nil, nil
	})
	zone := proc.GetSessionInfo().TimeZone
	ctx = tableDumpBindContext{CompilerContext: ctx, ctx: ctx.GetContext(), proc: proc}
	budget := &tableDumpBindingBudget{}
	timeZones := newTableDumpTimeZones(time.LoadLocation)
	var sensitive bool
	for i, item := range items {
		if err := ctx.GetContext().Err(); err != nil {
			return false, err
		}
		if supplied != nil && (sourceItems[i].kind != item.kind || !strings.EqualFold(sourceItems[i].name, item.name) || sourceItems[i].origin != item.origin) {
			return false, moerr.NewInvalidInputNoCtx("table dump expression declaration does not match target")
		}
		if len(item.origin) == 0 {
			return false, moerr.NewInvalidInputNoCtx("persisted table expression has no SQL origin")
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
			// Each migration era must reproduce the whole tree; no partial matching
			// or semantic normalization is permitted for supplied expressions.
			modes := []tableDumpBindingMode{tableDumpBindingCurrent}
			zeroTemporal := false
			if item.kind == "default" {
				_, zeroTemporal = zeroTemporalDefaultLiteral(unwrapParenExpr(ast), item.col.Typ)
			}
			if item.kind == "default" && zeroTemporal {
				// Integer-zero origins are literals, disjoint from function eras.
				modes = append(modes, tableDumpBindingLegacyZeroTemporalDefault)
			} else if tableDumpImplicitTimestamp(item, ast) {
				modes = append(modes, tableDumpBindingImplicitTimestamp)
			} else {
				// The prior PR can contain fractional intervals with pre-8dd
				// nullability. Published main after 8dd also needs the independent
				// pre-fractional/current-nullability era. Compare complete trees.
				modes = append(modes, tableDumpBindingCurrent|tableDumpBindingPreTemporalNullability)
				if visit.hasTimestampAdd {
					modes = append(modes, tableDumpBindingLegacyTimestampAdd,
						tableDumpBindingLegacyTimestampAdd|tableDumpBindingPreTemporalNullability)
				}
				if visit.hasIntegerDivision {
					modes = append(modes, tableDumpBindingLegacyDecimalIntegerDiv|tableDumpBindingPreTemporalNullability)
				}
				if visit.hasSpecial {
					modes = append(modes, tableDumpBindingLegacySpecial|tableDumpBindingPreTemporalNullability)
				}
			}
			// All session witnesses share the ordinary cancellation and total
			// candidate-node budget. Only complete rebound trees can match.
			var fatal error
			var dependency tableDumpDefaultSession
			bind := func(increment int64, mode tableDumpBindingMode, tz *time.Location, lc string) (*planpb.Expr, error) {
				if fatal = ctx.GetContext().Err(); fatal != nil {
					return nil, fatal
				}
				budget.visits += visit.nodes
				if budget.visits > candidateVisitLimit {
					fatal = moerr.NewInvalidInputNoCtx("table dump expression binding exceeds limit")
					return nil, fatal
				}
				proc.GetSessionInfo().TimeZone = tz
				proc.GetSessionInfo().LCTimeNames = lc
				return bindTableDumpExpression(ctx, target, item, ast, increment, mode, &dependency)
			}
			var first *planpb.Expr
			want := item.expr
			if supplied != nil {
				want = sourceItems[i].expr
			}
			// Give every cheap precision/era witness priority over a wrong
			// precision's exhaustive session search. Retain flags, not trees.
			type sessionCandidate struct {
				increment      int64
				mode           tableDumpBindingMode
				dependency     tableDumpDefaultSession
				folded, stored bool
			}
			var sessions []sessionCandidate
			for increment := int64(0); increment <= maxIncrement; increment++ {
				if err := ctx.GetContext().Err(); err != nil {
					stmt.Free()
					return false, err
				}
				for _, mode := range modes {
					dependency = tableDumpDefaultSession{}
					candidate, err := bind(increment, mode, zone, locale)
					if fatal != nil {
						stmt.Free()
						return false, fatal
					}
					if err != nil {
						// An unavailable era (for example, before protocol promotion)
						// cannot prove that the successful bindings are independent.
						// Still require an exact successful match before capture.
						sensitive = true
					} else if first == nil {
						first = candidate
					} else if !sameTableDumpBoundExpression(first, candidate) {
						sensitive = true
					}
					matched = matched || sameTableDumpBoundExpression(want, candidate)

					if item.kind == "default" {
						_, folded := tableDumpTimestampLiteral(candidate)
						_, stored := tableDumpTimestampLiteral(want)
						if folded || stored || dependency.folded || err != nil && (dependency.timeZone || dependency.locale) {
							sensitive = true
							sessions = append(sessions, sessionCandidate{increment, mode, dependency, folded, stored})
						}
					}
				}
			}
			localesFor := func(needsLocale bool) []string {
				locales := []string{locale}
				if needsLocale {
					for _, other := range function.TemporalLocaleNames() {
						if other != locale {
							locales = append(locales, other)
						}
					}
				}
				return locales
			}
			// Locale-only witnesses are cheap too: try all precisions before
			// allowing any wrong precision to consume the timezone budget.
			for _, search := range sessions {
				if matched {
					break
				}
				dependency = search.dependency
				for _, other := range localesFor(search.dependency.locale)[1:] {
					rebound, _ := bind(search.increment, search.mode, zone, other)
					if fatal != nil {
						stmt.Free()
						return false, fatal
					}
					if sameTableDumpBoundExpression(want, rebound) {
						matched = true
						break
					}
				}
			}
			for _, search := range sessions {
				if matched {
					break
				}
				increment, mode := search.increment, search.mode
				dependency = search.dependency
				folded, stored := search.folded, search.stored
				locales := localesFor(search.dependency.locale)
				if search.dependency.timeZone || folded || stored {
					for _, lc := range locales {
						if matched || fatal != nil {
							break
						}
						if folded || stored {
							matched = matchTableDumpTimestampDefault(want, nil, zone, func(tz *time.Location) (*planpb.Expr, error) {
								return bind(increment, mode, tz, lc)
							})
						}
						if matched || fatal != nil {
							break
						}
						for tz := range timeZones {
							rebound, _ := bind(increment, mode, tz, lc)
							if fatal != nil {
								break
							}
							if sameTableDumpBoundExpression(want, rebound) {
								matched = true
								break
							}
						}
					}
				}
				if fatal != nil {
					stmt.Free()
					return false, fatal
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
