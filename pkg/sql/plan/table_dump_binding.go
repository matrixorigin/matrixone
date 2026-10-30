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
	"errors"
	"iter"
	"math"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
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
	sessionHints        []tableDumpSessionHint
	probeMemo           legacyFloatProbeMemo
	probeTimeZones      func(func(*time.Location) bool)
}

type tableDumpExpressionVisitor struct {
	nodes                 int
	hasColumn             bool
	hasDivision           bool
	hasIntegerDivision    bool
	hasSpecial            bool
	hasTimestampAdd       bool
	hasUnixTimestamp      bool
	hasJSON               bool
	hasLegacyJSONValue    bool
	hasIncompleteTemporal bool
	hasTimeString         bool
}

func (v *tableDumpExpressionVisitor) Enter(e tree.Expr) (tree.Expr, bool) {
	v.nodes++
	if _, ok := e.(*tree.UnresolvedName); ok {
		v.hasColumn = true
	}
	if binary, ok := e.(*tree.BinaryExpr); ok {
		v.hasDivision = v.hasDivision || binary.Op == tree.DIV
		v.hasIntegerDivision = v.hasIntegerDivision || binary.Op == tree.INTEGER_DIV
	}
	if fn, ok := e.(*tree.FuncExpr); ok {
		switch numericAstFunctionName(fn) {
		case "format", "makedate", "maketime":
			v.hasSpecial = true
		case "time":
			v.hasTimeString = true
		case "timestampadd":
			v.hasTimestampAdd = true
		case "unix_timestamp":
			v.hasUnixTimestamp = true
		case "json_length", "json_set", "json_insert", "json_replace":
			v.hasJSON = true
		case "json_value":
			if fn.JsonValue == nil || !jsonValueUsesContract(fn.JsonValue) {
				v.hasLegacyJSONValue = true
			}
		case "date_format", "datediff", "to_days", "to_seconds", "dayofweek", "week":
			v.hasIncompleteTemporal = true
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
	for item := range persistedTableExpressions(def) {
		result = append(result, item)
	}
	return result
}

// Resolution usually has no legacy expression. Yield owners lazily so that
// migration can filter/cancel without allocating a slice for every default.
// DUMP collects the same ordered traversal when it needs indexed declarations.
func persistedTableExpressions(def *planpb.TableDef) iter.Seq[tableDumpExpression] {
	return func(yield func(tableDumpExpression) bool) {
		for _, col := range def.Cols {
			if col == nil {
				continue
			}
			if col.Default != nil && col.Default.Expr != nil && !yield(tableDumpExpression{"default", col.Name, col.Default.OriginString, col, nil, col.Default.Expr}) {
				return
			}
			if col.OnUpdate != nil && col.OnUpdate.Expr != nil && !yield(tableDumpExpression{"on_update", col.Name, col.OnUpdate.OriginString, col, nil, col.OnUpdate.Expr}) {
				return
			}
			if col.GeneratedCol != nil && col.GeneratedCol.Expr != nil && !yield(tableDumpExpression{"generated", col.Name, col.GeneratedCol.OriginString, col, nil, col.GeneratedCol.Expr}) {
				return
			}
		}
		for _, check := range def.Checks {
			if check != nil && check.Check != nil && !yield(tableDumpExpression{"check", check.Name, check.OriginSql, nil, check, check.Check}) {
				return
			}
		}
	}
}

func tableDumpParserModes() []string {
	return tableDumpParserModesForFlags(0b111)
}

// A parser mode only needs to be considered when the origin contains syntax
// whose meaning that mode changes. Omitting an irrelevant mode is equivalent
// to the default parse, while retaining every relevant combination preserves
// the complete migration witness set.
func tableDumpParserModesForOrigin(origin string) []string {
	var relevant int
	if strings.ContainsRune(origin, '\\') {
		relevant |= 1 << 0 // NO_BACKSLASH_ESCAPES
	}
	if strings.ContainsRune(origin, '"') {
		relevant |= 1 << 1 // ANSI_QUOTES
	}
	if strings.Contains(origin, "||") {
		relevant |= 1 << 2 // PIPES_AS_CONCAT
	}
	return tableDumpParserModesForFlags(relevant)
}

func tableDumpParserModesForFlags(relevant int) []string {
	flags := []string{"NO_BACKSLASH_ESCAPES", "ANSI_QUOTES", "PIPES_AS_CONCAT"}
	modes := make([]string, 0, 1<<len(flags))
	for mask := 0; mask < 8; mask++ {
		if mask&^relevant != 0 {
			continue
		}
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

type tableDumpBindingMode uint32

const (
	tableDumpBindingCurrent tableDumpBindingMode = iota
	tableDumpBindingLegacyTimestampAdd
	tableDumpBindingLegacyDecimalIntegerDiv
	tableDumpBindingLegacySpecial
	tableDumpBindingImplicitTimestamp
	tableDumpBindingLegacyZeroTemporalDefault
)

// A flag selects one coherent pre-migration binding, never per-node metadata.
const (
	tableDumpBindingPreTemporalNullability   tableDumpBindingMode = 1 << 7
	tableDumpBindingPreUnixTimestampDatetime tableDumpBindingMode = 1 << 8
	tableDumpBindingPreJSON                  tableDumpBindingMode = 1 << 6
	tableDumpBindingPreIncompleteTemporal    tableDumpBindingMode = 1 << 10
	tableDumpBindingDDLColumnScope           tableDumpBindingMode = 1 << 9
	tableDumpBindingPreIntegerDecimal        tableDumpBindingMode = 1 << 15
	tableDumpBindingPreDecimalPrecision      tableDumpBindingMode = 1 << 16
	tableDumpBindingPreTimeNullability       tableDumpBindingMode = 1 << 17
)

const tableDumpBindingLegacyFloat tableDumpBindingMode = 1 << 11
const tableDumpBindingLegacyFloatFormatting tableDumpBindingMode = 1 << 12
const tableDumpBindingFrozenFloatDefaults tableDumpBindingMode = 1 << 13
const tableDumpBindingPreJSONFolding tableDumpBindingMode = 1 << 14

type tableDumpDefaultSession struct {
	timeZone, locale, folded     bool
	observed, incompleteTemporal bool
	unfolded                     *planpb.Expr
	budget                       *tableDumpBindingBudget
	captureUnfolded              bool
	floatFormatting              bool
	opaqueJSONFolding            bool
}

// Inspect the unfolded source: folding removes the registry and width witness.
// An unaffected default needs no duplicate historical timezone/locale search.
func tableDumpIncompleteTemporalBinding(expr *planpb.Expr) bool {
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	if err != nil || features.IncompleteTemporalStrings {
		return true
	}
	var changedWidth func(*planpb.Expr) bool
	changedWidth = func(current *planpb.Expr) bool {
		if current == nil {
			return false
		}
		if fn := current.GetF(); fn != nil {
			if fn.Func != nil && strings.EqualFold(fn.Func.ObjName, "date_format") && len(fn.Args) == 2 {
				if format := fn.Args[1].GetLit(); format != nil && !format.Isnull {
					if value, ok := format.Value.(*planpb.Literal_Sval); ok &&
						mysqlDateFormatWidthForCalendar("date_format", value.Sval, true) !=
							mysqlDateFormatWidthForCalendar("date_format", value.Sval, false) {
						return true
					}
				}
			}
			for _, arg := range fn.Args {
				if changedWidth(arg) {
					return true
				}
			}
		}
		if list := current.GetList(); list != nil {
			for _, item := range list.List {
				if changedWidth(item) {
					return true
				}
			}
		}
		return false
	}
	return changedWidth(expr)
}

// Only decimal coercions owned by numeric operators changed in this migration.
// In particular, a folded ordinary integer default must not pay for redundant
// historical candidates or exhaust the existing candidate-work budget.
func tableDumpHasIntegerDecimalCoercion(expr *planpb.Expr) bool {
	if fn := expr.GetF(); fn != nil && fn.Func != nil {
		if catalogDecimalOperator(fn.Func.ObjName, len(fn.Args)) {
			for _, arg := range fn.Args {
				if source := catalogImplicitDecimalSource(arg); source != nil &&
					(makeTypeByPlan2Type(source.Typ).IsIntOrUint() || types.T(source.Typ.Id) == types.T_bit) {
					return true
				}
			}
		}
		for _, arg := range fn.Args {
			if tableDumpHasIntegerDecimalCoercion(arg) {
				return true
			}
		}
	}
	if lit := expr.GetLit(); lit != nil {
		return tableDumpHasIntegerDecimalCoercion(lit.Src)
	}
	return false
}

// Decimal repair can replace the old integer -> decimal cast with an exact
// integer operand. Only a separately bound float DEFAULT will use this hint to
// reconstruct that historical era; the hint never proves a payload valid.
func tableDumpHasIntegerNumericOperand(expr *planpb.Expr) bool {
	if fn := expr.GetF(); fn != nil && fn.Func != nil {
		if catalogDecimalOperator(fn.Func.ObjName, len(fn.Args)) {
			for _, arg := range fn.Args {
				if arg != nil && (makeTypeByPlan2Type(arg.Typ).IsIntOrUint() || types.T(arg.Typ.Id) == types.T_bit) {
					return true
				}
			}
		}
		for _, arg := range fn.Args {
			if tableDumpHasIntegerNumericOperand(arg) {
				return true
			}
		}
	}
	if lit := expr.GetLit(); lit != nil {
		return tableDumpHasIntegerNumericOperand(lit.Src)
	}
	return false
}

// Decimal-only trees need their own precision era even without an integer
// cast. Ordinary integer defaults must not acquire extra candidate searches.
func tableDumpHasLegacyDecimalPrecision(expr *planpb.Expr) bool {
	if fn := expr.GetF(); fn != nil && fn.Func != nil {
		switch fn.Func.ObjName {
		case "+", "-", "*":
			if len(fn.Args) == 2 && ((fn.Args[0] != nil && types.T(fn.Args[0].Typ.Id).IsDecimal()) ||
				(fn.Args[1] != nil && types.T(fn.Args[1].Typ.Id).IsDecimal())) {
				return true
			}
		case "round", "ceil", "ceiling", "floor":
			if len(fn.Args) >= 1 && len(fn.Args) <= 2 && fn.Args[0] != nil && types.T(fn.Args[0].Typ.Id).IsDecimal() {
				return true
			}
		}
		for _, arg := range fn.Args {
			if tableDumpHasLegacyDecimalPrecision(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, entry := range list.List {
			if tableDumpHasLegacyDecimalPrecision(entry) {
				return true
			}
		}
	}
	if lit := expr.GetLit(); lit != nil {
		return tableDumpHasLegacyDecimalPrecision(lit.Src)
	}
	return false
}

// tableDumpHasTimeStringConversion reports a stored tree with a TIME()
// string conversion whose published binding metadata changed with the
// row-local NULL synthesis contract; only such trees need the intermediate
// post-8dd candidate era.
func tableDumpHasTimeStringConversion(expr *planpb.Expr) bool {
	if expr == nil {
		return false
	}
	if f := expr.GetF(); f != nil {
		if fid, oid := function.DecodeOverloadID(f.Func.Obj); fid == function.TIME && oid >= 5 && oid <= 8 {
			return true
		}
		for _, arg := range f.Args {
			if tableDumpHasTimeStringConversion(arg) {
				return true
			}
		}
	}
	if entry := expr.GetList(); entry != nil {
		for _, arg := range entry.List {
			if tableDumpHasTimeStringConversion(arg) {
				return true
			}
		}
	}
	if lit := expr.GetLit(); lit != nil {
		return tableDumpHasTimeStringConversion(lit.Src)
	}
	return false
}

// Float compatibility uses the same complete-era binder as DUMP/LOAD.
func bindFloatTableDumpExpression(ctx CompilerContext, def *planpb.TableDef, item tableDumpExpression, ast tree.Expr, increment int64, legacy bool, legacyFloat ...bool) (*planpb.Expr, error) {
	mode := tableDumpBindingCurrent
	if legacy {
		mode = tableDumpBindingLegacySpecial
	}
	if len(legacyFloat) > 0 && legacyFloat[0] {
		mode |= tableDumpBindingLegacyFloat
	}
	return bindTableDumpExpression(ctx, def, item, ast, increment, mode, &tableDumpDefaultSession{})
}

func bindTableDumpExpression(ctx CompilerContext, def *planpb.TableDef, item tableDumpExpression, ast tree.Expr, increment int64, mode tableDumpBindingMode, session *tableDumpDefaultSession) (*planpb.Expr, error) {
	bindCtx := function.WithDivPrecisionIncrement(ctx.GetContext(), int32(increment))
	baseMode := mode &^ (tableDumpBindingPreTemporalNullability | tableDumpBindingPreJSON | tableDumpBindingPreUnixTimestampDatetime | tableDumpBindingPreIntegerDecimal | tableDumpBindingPreDecimalPrecision | tableDumpBindingPreIncompleteTemporal | tableDumpBindingDDLColumnScope | tableDumpBindingLegacyFloat | tableDumpBindingLegacyFloatFormatting | tableDumpBindingFrozenFloatDefaults | tableDumpBindingPreJSONFolding | tableDumpBindingPreTimeNullability)
	if mode&tableDumpBindingPreDecimalPrecision != 0 {
		bindCtx = function.WithLegacyDecimalIntermediatePrecision(bindCtx)
	}
	if mode&tableDumpBindingPreIntegerDecimal != 0 {
		bindCtx = function.WithLegacyIntegerDecimalCoercions(bindCtx)
	}
	if mode&tableDumpBindingPreJSONFolding != 0 {
		bindCtx = context.WithValue(bindCtx, historicalFloatDefaultFoldingKey{}, true)
	}
	if mode&tableDumpBindingLegacyFloatFormatting != 0 {
		bindCtx = function.WithLegacyFloatStringFormatting(bindCtx)
	}
	if mode&tableDumpBindingLegacyFloat != 0 {
		bindCtx = function.WithLegacyFloatStringBounds(bindCtx)
	}
	if mode&tableDumpBindingPreTemporalNullability != 0 {
		bindCtx = function.WithLegacyTemporalNullability(bindCtx)
	}
	if mode&tableDumpBindingPreTimeNullability != 0 {
		bindCtx = function.WithLegacyTimeNullability(bindCtx)
	}
	if mode&tableDumpBindingPreUnixTimestampDatetime != 0 {
		bindCtx = function.WithLegacyUnixTimestampDatetime(bindCtx)
	}
	if mode&tableDumpBindingPreIncompleteTemporal != 0 {
		bindCtx = function.WithLegacyIncompleteTemporalStrings(bindCtx)
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
	if mode&tableDumpBindingPreJSON != 0 {
		// Constant folding executes with proc.Ctx. This process is private to
		// origin verification; restore its context before the next candidate.
		bindCtx = function.WithLegacyJSONBinding(bindCtx)
		previous := proc.Ctx
		proc.Ctx = bindCtx
		defer func() { proc.Ctx = previous }()
	}
	if baseMode == tableDumpBindingImplicitTimestamp {
		// Old implicit defaults and ON UPDATE clauses saved CURRENT_TIMESTAMP()
		// as their origin, but used the column precision and this distinct DDL
		// owner. Reproduce that whole tree, including its assignment cast.
		return buildImplicitCurrentTimestampExpr(item.col.Typ, proc)
	}
	switch item.kind {
	case "default":
		if mode&tableDumpBindingFrozenFloatDefaults != 0 {
			if session.budget == nil {
				return nil, moerr.NewInternalError(bindCtx, "missing frozen default proof budget")
			}
			currentSession := &tableDumpDefaultSession{budget: session.budget, captureUnfolded: true}
			_, err := bindTableDumpExpression(ctx, def, item, ast, increment, mode&^tableDumpBindingFrozenFloatDefaults, currentSession)
			if err != nil {
				return nil, err
			}
			oldSession := &tableDumpDefaultSession{budget: session.budget, captureUnfolded: true}
			old, err := bindTableDumpExpression(ctx, def, item, ast, increment, (mode&^tableDumpBindingFrozenFloatDefaults)|tableDumpBindingLegacyFloat, oldSession)
			if err != nil {
				return nil, err
			}
			proofProc := floatStringProofProcess(bindCtx, proc)
			currentSource := currentSession.unfolded
			var assignment *planpb.Expr
			if oldSession.unfolded != nil && isSameColumnType(oldSession.unfolded.Typ, item.col.Typ) {
				// Widening can newly require a declared assignment boundary.
				// Align only its value child with the old root, then retain
				// the complete current wrapper and its strict width policy.
				if fn := currentSource.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "cast_strict" && !fn.SyntaxExplicitCast && len(fn.Args) == 2 && fn.Args[1] != nil && fn.Args[1].GetT() != nil &&
					isSameColumnType(currentSource.Typ, item.col.Typ) && proto.Equal(&fn.Args[1].Typ, &currentSource.Typ) && !isSameColumnType(fn.Args[0].Typ, item.col.Typ) &&
					(oldSession.unfolded.GetF() == nil || oldSession.unfolded.GetF().Func.Obj != fn.Func.Obj) {
					if err = chargeFloatStringRebind(currentSource, session.budget); err != nil {
						return nil, err
					}
					witness, bindErr := makePlan2AssignmentCastExpr(bindCtx, DeepCopyExpr(fn.Args[0]), item.col.Typ)
					if bindErr != nil || !sameTableDumpBoundExpression(witness, currentSource) {
						return nil, errLegacyFloatStringRebind
					}
					assignment = currentSource
					currentSource = fn.Args[0]
				}
			}
			frozen, _, err := freezeVerifiedFloatDefault(bindCtx, old, oldSession.unfolded, currentSource, proofProc, session.budget)
			if err != nil {
				return nil, err
			}
			if assignment != nil {
				if err = chargeFloatStringRebind(assignment, session.budget); err != nil {
					return nil, err
				}
				frozen, err = makePlan2AssignmentCastExpr(bindCtx, frozen, item.col.Typ)
				if err != nil {
					return nil, err
				}
			}
			if err = chargeFloatStringRebind(frozen, session.budget); err != nil {
				return nil, err
			}
			frozen, err = ConstantFold(batch.EmptyForConstFoldBatch, frozen, proofProc, false, true)
			if !session.captureUnfolded {
				currentSession.unfolded = nil
				currentSession.captureUnfolded = false
			}
			*session = *currentSession
			return frozen, err
		}
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
		bound, err := buildDefaultExprWithColumnsObserved(bindCtx, col, item.col.Typ, floatStringProofProcess(bindCtx, proc), def.Cols, purpose, func(source, assigned *planpb.Expr) {
			// Observe before assignment/folding can fail in the reading zone.
			// Inspect the resolved source, not the assignment cast's target type:
			// a plain NULL/zero TIMESTAMP needs no timezone reconstruction.
			session.observed = true
			session.incompleteTemporal = tableDumpIncompleteTemporalBinding(source)
			if features, err := planpb.RequiredRemoteExpressionFeatures(assigned); err == nil {
				session.opaqueJSONFolding = features.JSONOpaqueTextContracts
			}
			tz, locale := tableDumpDefaultDependencies(source)
			session.timeZone = session.timeZone || tz
			session.locale = session.locale || locale
			unfolded = assigned
			session.floatFormatting = session.floatFormatting || hasFloatDefaultFormatting(source) || hasFloatDefaultFormatting(assigned)
		})
		if err != nil {
			return nil, err
		}
		if (session.timeZone || session.locale) && !sameTableDumpBoundExpression(unfolded, bound.Expr) {
			session.folded = true
		}
		if session.captureUnfolded {
			session.unfolded = unfolded
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
	// Preserve the allocation-free protobuf path for canonical trees.
	if !tableDumpBoundExpressionNeedsNormalization(a) && !tableDumpBoundExpressionNeedsNormalization(b) {
		return proto.Equal(a, b)
	}
	return sameProjectedBoundExpression(a, b, boundExpressionNormalized)
}

// Historical bare JSON_VALUE defaults can be folded to a non-null string
// literal even though rebinding the nullable function keeps NotNullable false.
// Compare that result only during the legacy float-format proof and retain all
// other expression and type fields exactly.
func sameLegacyJSONValueFoldedText(a, b *planpb.Expr) bool {
	if a == nil || b == nil || types.T(a.Typ.Id) != types.T_varchar || types.T(b.Typ.Id) != types.T_varchar {
		return false
	}
	leftLit, rightLit := a.GetLit(), b.GetLit()
	if leftLit == nil || rightLit == nil || leftLit.Isnull || rightLit.Isnull {
		return false
	}
	if _, ok := leftLit.Value.(*planpb.Literal_Sval); !ok {
		return false
	}
	if _, ok := rightLit.Value.(*planpb.Literal_Sval); !ok {
		return false
	}
	left, right := *a, *b
	left.Typ.NotNullable, right.Typ.NotNullable = false, false
	return sameTableDumpBoundExpression(&left, &right)
}

func sameTableDumpBoundType(a, b planpb.Type) bool {
	return sameTableDumpBoundExpression(&planpb.Expr{Typ: a}, &planpb.Expr{Typ: b})
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

// newTableDumpProofProcess isolates speculative session settings and warnings.
// The caller owns and frees the returned process.
func newTableDumpProofProcess(ctx CompilerContext) *process.Process {
	parent := ctx.GetProcess()
	proc := parent.NewViewBindingProcess(ctx.GetContext())
	proc.WarningSink = &process.WarningProbe{}
	proc.Base.UnixTime = parent.Base.UnixTime
	proc.GetSessionInfo().DefaultWeekFormat = parent.GetSessionInfo().DefaultWeekFormat
	proc.GetSessionInfo().DefaultWeekFormatSet = parent.GetSessionInfo().DefaultWeekFormatSet
	proc.GetSessionInfo().LCTimeNames = function.TemporalLocaleNameForProcess(parent)
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
	return proc
}

type tableDumpSessionCandidate struct {
	increment      int64
	mode           tableDumpBindingMode
	dependency     tableDumpDefaultSession
	folded, stored bool
}

// Try all cheap locale witnesses before any exhaustive timezone search. The bind
// callback owns cancellation/budget checks and returns only fatal proof errors.
func matchTableDumpSessions(want *planpb.Expr, zone *time.Location, locale string,
	timeZones func(func(*time.Location) bool), sessions []tableDumpSessionCandidate,
	bind func(tableDumpSessionCandidate, *time.Location, string) (*planpb.Expr, error),
	match func(tableDumpSessionCandidate, *planpb.Expr) bool) (bool, error) {
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
	for _, search := range sessions {
		for _, other := range localesFor(search.dependency.locale)[1:] {
			rebound, err := bind(search, zone, other)
			if err != nil {
				return false, err
			}
			if match(search, rebound) {
				return true, nil
			}
		}
	}
	for _, search := range sessions {
		if !search.dependency.timeZone && !search.folded && !search.stored {
			continue
		}
		for _, lc := range localesFor(search.dependency.locale) {
			if search.folded || search.stored {
				var fatal error
				matched := matchTableDumpTimestampDefault(want, nil, zone, func(tz *time.Location) (*planpb.Expr, error) {
					if fatal != nil {
						return nil, fatal
					}
					var result *planpb.Expr
					result, fatal = bind(search, tz, lc)
					return result, fatal
				})
				if fatal != nil {
					return false, fatal
				}
				if matched {
					return true, nil
				}
			}
			for tz := range timeZones {
				rebound, err := bind(search, tz, lc)
				if err != nil {
					return false, err
				}
				if match(search, rebound) {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

// tableDumpDDLColumnScope recreates CREATE's pre-catalog column declarations.
// The returned witness owns its changed columns; persisted declarations stay exact.
func tableDumpDDLColumnScope(target *planpb.TableDef) *planpb.TableDef {
	var ddlScope *planpb.TableDef
	for i, col := range target.Cols {
		if col == nil || !col.Typ.NotNullable {
			continue
		}
		if ddlScope == nil {
			scope := *target
			scope.Cols = append([]*planpb.ColDef(nil), target.Cols...)
			ddlScope = &scope
		}
		copy := *col
		copy.Typ.NotNullable = false
		ddlScope.Cols[i] = &copy
	}
	return ddlScope
}

func tableDumpBindingScope(target, ddlScope *planpb.TableDef, item tableDumpExpression, mode tableDumpBindingMode) (*planpb.TableDef, tableDumpExpression, tableDumpBindingMode) {
	if mode&tableDumpBindingDDLColumnScope == 0 {
		return target, item, mode
	}
	mode &^= tableDumpBindingDDLColumnScope
	if item.col != nil {
		col := *item.col
		col.Typ.NotNullable = false
		item.col = &col
	}
	return ddlScope, item, mode
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
	// CREATE binds against declared column types before the catalog adds
	// NotNullable from Default.NullAbility. ALTER can bind against the catalog
	// scope instead. Keep both complete witnesses; never discard nullability
	// from supplied expressions or change the target's stored declarations.
	ddlScope := tableDumpDDLColumnScope(target)
	parent := ctx.GetProcess()
	if parent == nil {
		return false, moerr.NewInternalError(ctx.GetContext(), "table dump expression binding requires a process")
	}
	proc := newTableDumpProofProcess(ctx)
	defer proc.Free()
	locale := function.TemporalLocaleNameForProcess(proc)
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
		integerDecimal := tableDumpHasIntegerDecimalCoercion(item.expr)
		decimalPrecision := tableDumpHasLegacyDecimalPrecision(item.expr)
		normalizedDecimal := item.kind == "default" && tableDumpHasIntegerNumericOperand(item.expr)
		if supplied != nil {
			integerDecimal = integerDecimal || tableDumpHasIntegerDecimalCoercion(sourceItems[i].expr)
			decimalPrecision = decimalPrecision || tableDumpHasLegacyDecimalPrecision(sourceItems[i].expr)
			normalizedDecimal = normalizedDecimal || item.kind == "default" && tableDumpHasIntegerNumericOperand(sourceItems[i].expr)
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
		for _, mode := range tableDumpParserModesForOrigin(item.origin) {
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
			sawTimeEra := false
			appendTimeEraCandidates := func() {
				modes = append(modes, tableDumpBindingCurrent|tableDumpBindingPreTimeNullability)
				if visit.hasTimestampAdd {
					modes = append(modes, tableDumpBindingLegacyTimestampAdd|tableDumpBindingPreTimeNullability)
				}
				if visit.hasIntegerDivision {
					modes = append(modes, tableDumpBindingLegacyDecimalIntegerDiv|tableDumpBindingPreTimeNullability)
				}
				if visit.hasSpecial {
					modes = append(modes, tableDumpBindingLegacySpecial|tableDumpBindingPreTimeNullability)
				}
			}
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
				if visit.hasTimeString || tableDumpHasTimeStringConversion(item.expr) {
					// Constant folding may replace the TIME() string
					// conversion with a literal that preserves its
					// nullability metadata, so applicability comes from the
					// SQL origin or the stored tree. The intermediate
					// post-8dd era kept TIME() NOT NULL while DATE
					// conversions were already nullable.
					appendTimeEraCandidates()
					sawTimeEra = true
				}
			}
			if visit.hasJSON {
				// The JSON correction postdates the existing migration eras.
				for i, mode := range modes {
					if mode != tableDumpBindingCurrent {
						modes[i] = mode | tableDumpBindingPreJSON
					}
				}
				modes = append(modes, tableDumpBindingCurrent|tableDumpBindingPreJSON)
				if sawTimeEra {
					// The post-JSON window that predates the TIME() change
					// keeps current JSON semantics: the legacy rewrite above
					// must not consume the only intermediate-era candidate.
					appendTimeEraCandidates()
				}
			}
			if visit.hasUnixTimestamp {
				// Rebind complete pre-overload trees, including their implicit
				// DATETIME -> TIMESTAMP cast, under each applicable earlier era.
				oldModes := append([]tableDumpBindingMode(nil), modes...)
				for _, mode := range oldModes {
					modes = append(modes, mode|tableDumpBindingPreUnixTimestampDatetime)
				}
			}
			if visit.hasIncompleteTemporal {
				// Each candidate reconstructs the whole registry era; exact tree
				// equality below still rejects mixed or forged physical metadata.
				baseModes := append([]tableDumpBindingMode(nil), modes...)
				for _, mode := range baseModes {
					modes = append(modes, mode|tableDumpBindingPreIncompleteTemporal)
				}
			}
			if ddlScope != nil && visit.hasColumn {
				for _, mode := range append([]tableDumpBindingMode(nil), modes...) {
					modes = append(modes, mode|tableDumpBindingDDLColumnScope)
				}
			}
			if decimalPrecision || integerDecimal {
				baseModes := append([]tableDumpBindingMode(nil), modes...)
				if decimalPrecision {
					for _, mode := range baseModes {
						modes = append(modes, mode|tableDumpBindingPreDecimalPrecision)
					}
				}
				if integerDecimal {
					// Compose complete eras, without redundant precision/integer
					// combinations: the integer era already predates precision.
					for _, mode := range baseModes {
						modes = append(modes, mode|tableDumpBindingPreIntegerDecimal)
					}
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
				if mode&tableDumpBindingFrozenFloatDefaults != 0 {
					budget.visits += visit.nodes
				}
				dependency.budget = budget
				if budget.visits > candidateVisitLimit {
					fatal = moerr.NewInvalidInputNoCtx("table dump expression binding exceeds limit")
					return nil, fatal
				}
				proc.GetSessionInfo().TimeZone = tz
				proc.GetSessionInfo().LCTimeNames = lc
				scope, declaration, bindingMode := tableDumpBindingScope(target, ddlScope, item, mode)
				result, err := bindTableDumpExpression(ctx, scope, declaration, ast, increment, bindingMode, &dependency)
				if fatal = ctx.GetContext().Err(); fatal != nil {
					return nil, fatal
				}
				if budget.visits > candidateVisitLimit {
					fatal = moerr.NewInvalidInputNoCtx("table dump expression binding exceeds limit")
					return nil, fatal
				}
				var proofLimit *floatStringProofBudgetError
				if errors.As(err, &proofLimit) {
					fatal = err
				}
				return result, err
			}
			var first *planpb.Expr
			want := item.expr
			if supplied != nil {
				want = sourceItems[i].expr
			}
			matchCandidate := func(mode tableDumpBindingMode, candidate *planpb.Expr) (accepted bool, matchErr error) {
				defer func() {
					if accepted && matchErr == nil && item.kind == "default" {
						budget.rememberSession(proc.GetSessionInfo().TimeZone, proc.GetSessionInfo().LCTimeNames)
					}
				}()
				if sameTableDumpBoundExpression(want, candidate) ||
					mode&tableDumpBindingLegacyFloat != 0 && sameHistoricalFloatBoundExpression(want, candidate) ||
					item.kind == "default" && visit.hasLegacyJSONValue && mode&tableDumpBindingLegacyFloatFormatting != 0 && sameLegacyJSONValueFoldedText(want, candidate) {
					return true, nil
				}
				if !integerDecimal || mode&tableDumpBindingFrozenFloatDefaults == 0 || candidate == nil {
					return false, nil
				}
				// ALTER can persist the execution-owned float -> decimal repair.
				// Reconstruct that complete state from a trusted SQL-bound tree;
				// never normalize the supplied payload into validity. Charge each
				// subtree before the normalizer's possible ancestor deep copies.
				if err := planpb.VisitExprTree(candidate, func(expr *planpb.Expr) error {
					return chargeFloatStringRebind(expr, budget)
				}); err != nil {
					return false, err
				}
				if budget.visits > candidateVisitLimit {
					return false, moerr.NewInvalidInputNoCtx("table dump expression binding exceeds limit")
				}
				normalized, changed, err := normalizeCatalogDecimalExpr(ctx.GetContext(), candidate)
				if err != nil {
					return false, ctx.GetContext().Err()
				}
				return changed && sameTableDumpBoundExpression(want, normalized), nil
			}
			eras := append([]tableDumpBindingMode(nil), modes...)
			if visit.hasLegacyJSONValue {
				for _, era := range eras {
					modes = append(modes, era|tableDumpBindingLegacyFloatFormatting)
				}
			}
			if hasLegacyFloatStringCast(want) {
				for _, era := range eras {
					modes = append(modes, era|tableDumpBindingLegacyFloat)
				}
			}
			floatCandidatesAdded := false
			foldingCandidatesAdded := false
			// Give every cheap precision/era witness priority over a wrong
			// precision's exhaustive session search. Retain flags, not trees.
			var sessions []tableDumpSessionCandidate
			var probes []*legacyFloatSessionProbe
			probeKeys := make(map[string]int)
			for increment := int64(0); increment <= maxIncrement; increment++ {
				if err := ctx.GetContext().Err(); err != nil {
					stmt.Free()
					return false, err
				}
				needsIncompleteEra := true
				for modeIndex := 0; modeIndex < len(modes); modeIndex++ {
					mode := modes[modeIndex]
					if item.kind == "default" && mode&tableDumpBindingPreIncompleteTemporal != 0 && !needsIncompleteEra {
						continue
					}
					dependency = tableDumpDefaultSession{captureUnfolded: item.kind == "default"}
					candidate, err := bind(increment, mode, zone, locale)
					if normalizedDecimal && dependency.floatFormatting && !integerDecimal {
						// A persisted ALTER may already have removed the old casts.
						// Expand once, only after the independent SQL binding confirms
						// the float DEFAULT dependency, before taking folding eras.
						integerDecimal = true
						for _, era := range append([]tableDumpBindingMode(nil), eras...) {
							if era&tableDumpBindingPreDecimalPrecision != 0 {
								continue
							}
							eras = append(eras, era|tableDumpBindingPreIntegerDecimal)
						}
						for _, era := range append([]tableDumpBindingMode(nil), modes...) {
							if era&tableDumpBindingPreDecimalPrecision != 0 {
								continue
							}
							modes = append(modes, era|tableDumpBindingPreIntegerDecimal)
						}
					}
					if mode == tableDumpBindingCurrent && dependency.observed {
						needsIncompleteEra = dependency.incompleteTemporal
					}
					if item.kind == "default" && dependency.opaqueJSONFolding && !foldingCandidatesAdded {
						// Resolved contracts also catch CAST(JSON AS CHAR) and JSON
						// column consumers whose SQL contains no json_* function.
						for _, era := range append([]tableDumpBindingMode(nil), eras...) {
							eras = append(eras, era|tableDumpBindingPreJSONFolding)
						}
						for _, era := range append([]tableDumpBindingMode(nil), modes...) {
							modes = append(modes, era|tableDumpBindingPreJSONFolding)
						}
						foldingCandidatesAdded = true
					}
					if item.kind == "default" && dependency.floatFormatting && !floatCandidatesAdded {
						// Inspect resolved pre-fold types, since a fully folded
						// stored literal no longer carries its conversion. Do
						// not multiply unrelated arithmetic/temporal witnesses.
						for _, era := range eras {
							modes = append(modes, era|tableDumpBindingLegacyFloatFormatting,
								era|tableDumpBindingLegacyFloatFormatting|tableDumpBindingLegacyFloat,
								era|tableDumpBindingFrozenFloatDefaults,
								era|tableDumpBindingFrozenFloatDefaults|tableDumpBindingLegacyFloatFormatting)
						}
						floatCandidatesAdded = true
					}
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
					if !matched {
						var matchErr error
						matched, matchErr = matchCandidate(mode, candidate)
						if matchErr != nil {
							stmt.Free()
							return false, matchErr
						}
					}
					if !matched && item.kind == "default" && (dependency.timeZone || dependency.locale) {
						readingDependency := dependency
						var hintErr error
						matched, hintErr = budget.matchSessionHints(zone, locale,
							func(tz *time.Location, lc string) (*planpb.Expr, error) {
								dependency = tableDumpDefaultSession{}
								bound, _ := bind(increment, mode, tz, lc)
								return bound, fatal
							}, func(bound *planpb.Expr) (bool, error) { return matchCandidate(mode, bound) })
						dependency = readingDependency
						proc.GetSessionInfo().TimeZone, proc.GetSessionInfo().LCTimeNames = zone, locale
						if hintErr != nil {
							stmt.Free()
							return false, hintErr
						}
						if matched {
							sensitive = true
						}
					}

					if item.kind == "default" {
						_, folded := tableDumpTimestampLiteral(candidate)
						_, stored := tableDumpTimestampLiteral(want)
						// A successful binding with a different result type cannot
						// match under another timezone or locale: those witnesses only
						// change folded values. Keep failed binds, since another session
						// may make the expression bindable there.
						typeCanMatch := err != nil || candidate == nil || want == nil ||
							sameTableDumpBoundType(candidate.Typ, want.Typ)
						if typeCanMatch && (folded || stored || dependency.folded || err != nil && (dependency.timeZone || dependency.locale)) {
							sensitive = true
							flags := tableDumpDefaultSession{timeZone: dependency.timeZone, locale: dependency.locale, folded: dependency.folded}
							search := tableDumpSessionCandidate{increment, mode, flags, folded, stored}
							sessions = append(sessions, search)
							if !matched && dependency.floatFormatting {
								if err := addLegacyFloatSessionProbe(ctx, want, candidate, dependency.unfolded, search, &probes, probeKeys, budget); err != nil {
									stmt.Free()
									return false, err
								}
								if budget.visits > candidateVisitLimit {
									stmt.Free()
									return false, moerr.NewInvalidInputNoCtx("table dump expression binding exceeds limit")
								}
							}
						}
					}
				}
			}
			if !matched {
				bindSession := func(search tableDumpSessionCandidate, tz *time.Location, lc string) (*planpb.Expr, error) {
					if fatal != nil {
						return nil, fatal
					}
					dependency = search.dependency
					result, _ := bind(search.increment, search.mode, tz, lc)
					return result, fatal
				}
				matchSession := func(search tableDumpSessionCandidate, rebound *planpb.Expr) bool {
					var result bool
					result, fatal = matchCandidate(search.mode, rebound)
					return result
				}
				matched, err = matchLegacyFloatSessionProbes(ctx, zone, locale, timeZones, probes, budget, candidateVisitLimit, bindSession,
					func(search tableDumpSessionCandidate, rebound *planpb.Expr) (bool, error) {
						return matchCandidate(search.mode, rebound)
					})
				if !matched && err == nil && fatal == nil {
					matched, err = matchTableDumpSessions(want, zone, locale, timeZones, sessions, bindSession, matchSession)
				}
				if err == nil {
					err = fatal
				}
				if err != nil {
					stmt.Free()
					return false, err
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
