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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"slices"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// MigrateLegacyFloatStringTableDef repairs execution-owned catalog expressions.
// SQL-origin proof distinguishes implicit casts from explicit CHAR(32), including
// catalogs written before explicit-cast provenance was recorded. Catalog storage
// and folded DEFAULT values are never rewritten.
func MigrateLegacyFloatStringTableDef(ctx CompilerContext, def *planpb.TableDef) error {
	if ctx == nil || def == nil {
		return nil
	}
	if err := ctx.GetContext().Err(); err != nil {
		return err
	}
	var budget *tableDumpBindingBudget
	var proofProc *process.Process
	defer func() {
		if proofProc != nil {
			proofProc.Free()
		}
	}()
	for item := range persistedTableExpressions(def) {
		if err := ctx.GetContext().Err(); err != nil {
			return err
		}
		if !hasLegacyFloatStringCast(item.expr) {
			continue
		}
		if budget == nil {
			budget = &tableDumpBindingBudget{}
			if ctx.GetProcess() == nil {
				return moerr.NewInternalError(ctx.GetContext(), "catalog expression binding requires a process")
			}
			proofProc = newTableDumpProofProcess(ctx)
			ctx = tableDumpBindContext{CompilerContext: ctx, ctx: ctx.GetContext(), proc: proofProc}
		}
		rebound, err := rebindLegacyFloatStringExpression(ctx, def, item, budget)
		if err != nil {
			return err
		}
		if err := ctx.GetContext().Err(); err != nil {
			return err
		}
		if rebound == nil {
			continue
		}
		if item.check != nil {
			def.Checks = slices.Clone(def.Checks)
			for i, check := range def.Checks {
				if check == item.check {
					owned := *check
					owned.Check = rebound
					def.Checks[i] = &owned
					break
				}
			}
		} else {
			def.Cols = slices.Clone(def.Cols)
			for i, col := range def.Cols {
				// An earlier DEFAULT repair may already have detached this same
				// column. Merge ON UPDATE into the latest owned wrapper by name.
				if col == nil || col.Name != item.name {
					continue
				}
				owned := *col
				switch item.kind {
				case "generated":
					generated := *col.GeneratedCol
					generated.Expr = rebound
					owned.GeneratedCol = &generated
				case "default":
					defaultExpr := *col.Default
					defaultExpr.Expr = rebound
					owned.Default = &defaultExpr
				case "on_update":
					onUpdate := *col.OnUpdate
					onUpdate.Expr = rebound
					owned.OnUpdate = &onUpdate
				}
				def.Cols[i] = &owned
				break
			}
		}
	}
	return ctx.GetContext().Err()
}

func hasLegacyFloatStringCast(expr *planpb.Expr) bool {
	if expr == nil || expr.GetLit() != nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		if isLegacyFloatStringCast(expr) {
			return true
		}
		for _, arg := range fn.Args {
			if hasLegacyFloatStringCast(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, entry := range list.List {
			if hasLegacyFloatStringCast(entry) {
				return true
			}
		}
	}
	return false
}

func isLegacyFloatStringCast(expr *planpb.Expr) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 2 || fn.Args[0] == nil || fn.Args[1] == nil {
		return false
	}
	id, overload := function.DecodeOverloadID(fn.Func.Obj)
	return id == function.CAST && overload == 0 &&
		expr.Typ.Id == int32(types.T_varchar) && expr.Typ.Width == 32 &&
		fn.Args[0].Typ.Id == int32(types.T_float64) &&
		fn.Args[1].GetT() != nil && fn.Args[1].Typ.Id == expr.Typ.Id && fn.Args[1].Typ.Width == 32
}

// Historical byte-string binders omitted implicit coercibility. Reconstruct only
// that bounded candidate era; explicit and native collation provenance stays exact.
func omitHistoricalFloatCoercibility(stored, candidate *planpb.Expr) {
	a, b := &stored.Typ, &candidate.Typ
	if !types.T(a.Id).IsMySQLString() || !types.T(b.Id).IsMySQLString() ||
		a.Charset != b.Charset ||
		(a.Charset != uint32(types.CharsetLegacy) && a.Charset != uint32(types.CharsetBinary) &&
			a.Charset != uint32(types.CharsetUTF8MB4Bin) && a.Charset != uint32(types.CharsetUTF8)) ||
		a.CollationVersion != 0 || b.CollationVersion != 0 ||
		a.CollationMergeConflict || b.CollationMergeConflict ||
		a.CollationCoercibilitySet || a.CollationCoercibility != 0 ||
		!b.CollationCoercibilitySet || b.CollationCoercibility == 0 || b.CollationCoercibility > 6 {
		return
	}
	b.CollationCoercibility, b.CollationCoercibilitySet = 0, false
}

func sameHistoricalFloatBoundExpression(stored, candidate *planpb.Expr) bool {
	right := proto.Clone(candidate).(*planpb.Expr)
	var project func(*planpb.Expr, *planpb.Expr)
	project = func(a, b *planpb.Expr) {
		if a == nil || b == nil {
			return
		}
		omitHistoricalFloatCoercibility(a, b)
		if x, y := a.GetF(), b.GetF(); x != nil && y != nil {
			for i := 0; i < len(x.Args) && i < len(y.Args); i++ {
				project(x.Args[i], y.Args[i])
			}
		}
		if x, y := a.GetList(), b.GetList(); x != nil && y != nil {
			for i := 0; i < len(x.List) && i < len(y.List); i++ {
				project(x.List[i], y.List[i])
			}
		}
		if x, y := a.GetLit(), b.GetLit(); x != nil && y != nil {
			project(x.Src, y.Src)
		}
	}
	project(stored, right)
	return sameTableDumpBoundExpression(stored, right)
}

// SQL-origin proof also recovers missing explicit-cast syntax markers from the
// AST before rewriting. DUMP uses only the coercibility-only historical candidate
// and continues to require exact syntax provenance.
func sameLegacyFloatOrigin(stored, candidate *planpb.Expr) bool {
	right := proto.Clone(candidate).(*planpb.Expr)
	var recoverMissingMarkers func(*planpb.Expr, *planpb.Expr)
	recoverMissingMarkers = func(old, bound *planpb.Expr) {
		if old == nil || bound == nil {
			return
		}
		omitHistoricalFloatCoercibility(old, bound)
		if a, b := old.GetF(), bound.GetF(); a != nil && b != nil && b.Func != nil {
			id, overload := function.DecodeOverloadID(b.Func.Obj)
			if id == function.CAST && overload == 0 && !a.SyntaxExplicitCast {
				b.SyntaxExplicitCast = false
			}
			for i := 0; i < len(a.Args) && i < len(b.Args); i++ {
				recoverMissingMarkers(a.Args[i], b.Args[i])
			}
		}
		if a, b := old.GetList(), bound.GetList(); a != nil && b != nil {
			for i := 0; i < len(a.List) && i < len(b.List); i++ {
				recoverMissingMarkers(a.List[i], b.List[i])
			}
		}
		if a, b := old.GetLit(), bound.GetLit(); a != nil && b != nil {
			recoverMissingMarkers(a.Src, b.Src)
		}
	}
	recoverMissingMarkers(stored, right)
	return sameTableDumpBoundExpression(stored, right)
}

func rebindLegacyFloatStringExpression(ctx CompilerContext, def *planpb.TableDef, item tableDumpExpression, budget *tableDumpBindingBudget) (*planpb.Expr, error) {
	budget.text += len(item.origin)
	if budget.text > tableDumpMaxExpressionText {
		return nil, moerr.NewInvalidInputNoCtx("catalog expression text exceeds limit")
	}
	ddlScope := tableDumpDDLColumnScope(def)
	integerDecimal := tableDumpHasIntegerDecimalCoercion(item.expr)
	for _, parserMode := range tableDumpParserModesForOrigin(item.origin) {
		if err := ctx.GetContext().Err(); err != nil {
			return nil, err
		}
		stmt, ast, visit, err := parseTableDumpExpression(ctx.GetContext(), item.origin, parserMode)
		if err != nil {
			continue
		}
		result, err := func() (*planpb.Expr, error) {
			defer stmt.Free()
			budget.nodes += visit.nodes
			if budget.nodes > tableDumpMaxExpressionNodes {
				return nil, moerr.NewInvalidInputNoCtx("catalog expression AST exceeds limit")
			}
			proc := ctx.GetProcess()
			zone, locale := proc.GetSessionInfo().TimeZone, function.TemporalLocaleNameForProcess(proc)
			originalLocale := proc.GetSessionInfo().LCTimeNames
			defer func() { proc.GetSessionInfo().TimeZone = zone; proc.GetSessionInfo().LCTimeNames = originalLocale }()
			var matched *planpb.Expr
			var witness tableDumpSessionCandidate
			var dependency tableDumpDefaultSession
			bind := func(search tableDumpSessionCandidate, tz *time.Location, lc string) (*planpb.Expr, error) {
				if err := ctx.GetContext().Err(); err != nil {
					return nil, err
				}
				budget.visits += visit.nodes
				if budget.visits > tableDumpMaxCandidateVisits {
					return nil, moerr.NewInvalidInputNoCtx("catalog expression binding exceeds limit")
				}
				proc.GetSessionInfo().TimeZone, proc.GetSessionInfo().LCTimeNames = tz, lc
				dependency = tableDumpDefaultSession{budget: budget, captureUnfolded: item.kind == "default"}
				scope, declaration, bindingMode := tableDumpBindingScope(def, ddlScope, item, search.mode)
				bound, bindErr := bindTableDumpExpression(ctx, scope, declaration, ast, search.increment, bindingMode, &dependency)
				var proofLimit *floatStringProofBudgetError
				if errors.As(bindErr, &proofLimit) {
					return nil, bindErr
				}
				if err := ctx.GetContext().Err(); err != nil {
					return nil, err
				}
				return bound, nil
			}
			match := func(search tableDumpSessionCandidate, bound *planpb.Expr) bool {
				if bound != nil && sameLegacyFloatOrigin(item.expr, bound) {
					matched, witness = bound, search
					return true
				}
				return false
			}
			finish := func(search tableDumpSessionCandidate, old *planpb.Expr) (*planpb.Expr, error) {
				scope, declaration, bindingMode := tableDumpBindingScope(def, ddlScope, item, search.mode)
				return finishLegacyFloatStringExpression(ctx, scope, declaration, ast, search.increment, bindingMode, old, visit.nodes, budget)
			}
			var sessions []tableDumpSessionCandidate
			var probes []*legacyFloatSessionProbe
			probeKeys := make(map[string]int)
			maxIncrement := int64(0)
			if visit.hasDivision {
				maxIncrement = 30
			}
			for increment := int64(0); increment <= maxIncrement; increment++ {
				modes := []tableDumpBindingMode{tableDumpBindingLegacyFloat}
				if visit.hasSpecial {
					modes = append(modes, tableDumpBindingLegacyFloat|tableDumpBindingLegacySpecial)
				}
				if ddlScope != nil && visit.hasColumn {
					for _, mode := range append([]tableDumpBindingMode(nil), modes...) {
						modes = append(modes, mode|tableDumpBindingDDLColumnScope)
					}
				}
				if integerDecimal {
					for _, mode := range append([]tableDumpBindingMode(nil), modes...) {
						modes = append(modes, mode|tableDumpBindingPreIntegerDecimal)
					}
				}
				for _, mode := range modes {
					formats := []tableDumpBindingMode{mode}
					if item.kind == "default" {
						formats = append(formats, mode|tableDumpBindingLegacyFloatFormatting)
					}
					foldingEraAdded := false
					for modeIndex := 0; modeIndex < len(formats); modeIndex++ {
						mode := formats[modeIndex]
						search := tableDumpSessionCandidate{increment: increment, mode: mode}
						bound, err := bind(search, zone, locale)
						if err != nil {
							return nil, err
						}
						if item.kind == "default" && dependency.opaqueJSONFolding && !foldingEraAdded {
							for _, era := range append([]tableDumpBindingMode(nil), formats...) {
								formats = append(formats, era|tableDumpBindingPreJSONFolding)
							}
							foldingEraAdded = true
						}
						if match(search, bound) {
							return finish(search, matched)
						}
						if item.kind == "default" && (dependency.folded || bound == nil && (dependency.timeZone || dependency.locale)) {
							possible, err := legacyFloatSessionCandidateMayMatch(ctx.GetContext(), item.expr, bound, dependency.unfolded, budget)
							if err != nil {
								return nil, err
							}
							if !possible {
								continue
							}
							search.dependency = tableDumpDefaultSession{timeZone: dependency.timeZone, locale: dependency.locale, folded: dependency.folded}
							sessions = append(sessions, search)
							if err := addLegacyFloatSessionProbe(ctx, item.expr, bound, dependency.unfolded, search, &probes, probeKeys, budget); err != nil {
								return nil, err
							}
						}
					}
				}
			}
			found, err := matchLegacyFloatSessionProbes(ctx, zone, locale, newTableDumpTimeZones(time.LoadLocation), probes, budget, tableDumpMaxCandidateVisits, bind,
				func(search tableDumpSessionCandidate, bound *planpb.Expr) (bool, error) {
					return match(search, bound), nil
				})
			if found || err != nil {
				if err != nil {
					return nil, err
				}
				return finish(witness, matched)
			}
			found, err = matchLegacyFloatSessions(zone, locale, newLegacyFloatTimeZones(time.LoadLocation), sessions, bind, match)
			if err != nil || !found {
				return nil, err
			}
			return finish(witness, matched)
		}()
		if result != nil || err != nil {
			return result, err
		}
	}
	return nil, ctx.GetContext().Err()
}

// Opaque folded constants can make several precision/era candidates equally
// plausible. Give every candidate a probe in each zone before advancing, rather
// than letting an impossible early candidate exhaust the complete zone inventory.
// The existing bind callback still owns cancellation and the shared work limit.
func matchLegacyFloatSessions(zone *time.Location, locale string, timeZones func(func(*time.Location) bool),
	sessions []tableDumpSessionCandidate,
	bind func(tableDumpSessionCandidate, *time.Location, string) (*planpb.Expr, error),
	match func(tableDumpSessionCandidate, *planpb.Expr) bool) (bool, error) {
	if len(sessions) == 0 {
		return false, nil
	}
	locales := []string{locale}
	for _, other := range function.TemporalLocaleNames() {
		if other != locale {
			locales = append(locales, other)
		}
	}
	hasTimeZone := false
	for _, search := range sessions {
		hasTimeZone = hasTimeZone || search.dependency.timeZone
	}
	probe := func(tz *time.Location, otherZone bool) (bool, error) {
		for _, search := range sessions {
			if otherZone && !search.dependency.timeZone {
				continue
			}
			for _, lc := range locales {
				if lc != locale && !search.dependency.locale || !otherZone && lc == locale {
					continue // The reading session was already bound by every candidate.
				}
				bound, err := bind(search, tz, lc)
				if err != nil {
					return false, err
				}
				if match(search, bound) {
					return true, nil
				}
			}
		}
		return false, nil
	}
	if found, err := probe(zone, false); found || err != nil {
		return found, err
	}
	if hasTimeZone {
		for tz := range timeZones {
			if tz == zone {
				continue
			}
			if found, err := probe(tz, true); found || err != nil {
				return found, err
			}
		}
	}
	return false, nil
}

// legacyFloatSessionCandidateMayMatch only rejects impossible session probes.
// A session-independent source subtree cannot acquire a different precision,
// type or folded value under another timezone/locale. Ambiguous alignment is
// retained for the full origin proof; this check never authorizes a repair.
func legacyFloatSessionCandidateMayMatch(ctx context.Context, stored, folded, source *planpb.Expr, budget *tableDumpBindingBudget) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if stored == nil || folded == nil || source == nil {
		return true, nil
	}
	if err := chargeFloatStringRebind(source, budget); err != nil {
		return false, err
	}
	// Session folding can hide an independent precision witness inside one
	// literal. Its bound type shape survives folding and cannot be repaired by
	// another timezone/locale, even when the three trees no longer align below
	// this node. Leave value and provenance checks to the complete origin proof.
	if stored.Typ.Id != folded.Typ.Id || stored.Typ.Width != folded.Typ.Width || stored.Typ.Scale != folded.Typ.Scale {
		return false, nil
	}
	zone, locale := tableDumpDefaultDependencies(source)
	if !zone && !locale {
		for _, expr := range []*planpb.Expr{stored, folded} {
			if err := chargeFloatStringRebind(expr, budget); err != nil {
				return false, err
			}
		}
		return sameLegacyFloatOrigin(stored, folded), nil
	}
	if a, b, c := stored.GetF(), folded.GetF(), source.GetF(); a != nil && b != nil && c != nil &&
		a.Func != nil && b.Func != nil && c.Func != nil && a.Func.Obj == b.Func.Obj && b.Func.Obj == c.Func.Obj &&
		len(a.Args) == len(b.Args) && len(b.Args) == len(c.Args) {
		for i := range a.Args {
			possible, err := legacyFloatSessionCandidateMayMatch(ctx, a.Args[i], b.Args[i], c.Args[i], budget)
			if err != nil || !possible {
				return possible, err
			}
		}
	}
	if a, b, c := stored.GetList(), folded.GetList(), source.GetList(); a != nil && b != nil && c != nil &&
		len(a.List) == len(b.List) && len(b.List) == len(c.List) {
		for i := range a.List {
			possible, err := legacyFloatSessionCandidateMayMatch(ctx, a.List[i], b.List[i], c.List[i], budget)
			if err != nil || !possible {
				return possible, err
			}
		}
	}
	return true, ctx.Err()
}

func finishLegacyFloatStringExpression(ctx CompilerContext, def *planpb.TableDef, item tableDumpExpression, ast tree.Expr, increment int64, mode tableDumpBindingMode, old *planpb.Expr, nodes int, budget *tableDumpBindingBudget) (*planpb.Expr, error) {
	proofCtx := ctx
	if mode&tableDumpBindingPreJSONFolding != 0 {
		proofCtx = tableDumpBindContext{CompilerContext: ctx, ctx: context.WithValue(ctx.GetContext(), historicalFloatDefaultFoldingKey{}, true), proc: ctx.GetProcess()}
	}
	legacySpecial := mode&tableDumpBindingLegacySpecial != 0
	if mode&tableDumpBindingLegacyFloatFormatting != 0 {
		proofCtx = tableDumpBindContext{CompilerContext: ctx, ctx: function.WithLegacyFloatStringFormatting(proofCtx.GetContext()), proc: ctx.GetProcess()}
	}
	expected := DeepCopyExpr(old)
	rebindCtx := function.WithDivPrecisionIncrement(proofCtx.GetContext(), int32(increment))
	if mode&tableDumpBindingPreIntegerDecimal != 0 {
		rebindCtx = function.WithLegacyIntegerDecimalCoercions(rebindCtx)
	}
	if legacySpecial {
		rebindCtx = function.WithLegacySpecialConsumers(rebindCtx)
	}
	changed, err := widenVerifiedFloatStringExpr(rebindCtx, expected, budget)
	if err != nil {
		if errors.Is(err, errLegacyFloatStringRebind) {
			return nil, nil
		}
		return nil, err
	}
	if !changed {
		return nil, nil
	}
	// Widening can newly require an assignment wrapper when the old
	// root exactly matched the declared column type. Existing wrappers
	// keep their declared type and must not be duplicated.
	if (item.kind == "generated" || item.kind == "default") && isSameColumnType(old.Typ, item.col.Typ) &&
		!isSameColumnType(expected.Typ, item.col.Typ) {
		expected, err = makePlan2AssignmentCastExpr(rebindCtx, expected, item.col.Typ)
		if err != nil {
			return nil, err
		}
	}
	if item.kind == "default" {
		// DEFAULT binding folds constant coercions inside row-dependent
		// expressions too. Replay that owner after reconciling casts;
		// already-folded literal roots never enter this migration.
		if err := chargeFloatStringRebind(expected, budget); err != nil {
			return nil, err
		}
		expected, err = ConstantFold(batch.EmptyForConstFoldBatch, expected, floatStringProofProcess(rebindCtx, ctx.GetProcess()), false, true)
		if err != nil {
			return nil, err
		}
	}
	budget.visits += nodes
	if budget.visits > tableDumpMaxCandidateVisits {
		return nil, moerr.NewInvalidInputNoCtx("catalog expression binding exceeds limit")
	}
	mode &= tableDumpBindingPreJSONFolding | tableDumpBindingPreIntegerDecimal
	if legacySpecial {
		mode |= tableDumpBindingLegacySpecial
	}
	if item.kind == "default" {
		mode |= tableDumpBindingFrozenFloatDefaults
		budget.visits += nodes
		if budget.visits > tableDumpMaxCandidateVisits {
			return nil, moerr.NewInvalidInputNoCtx("catalog expression binding exceeds limit")
		}
	}
	current, err := bindTableDumpExpression(proofCtx, def, item, ast, increment, mode, &tableDumpDefaultSession{budget: budget})
	if err != nil {
		var limit *floatStringProofBudgetError
		if ctx.GetContext().Err() != nil {
			return nil, ctx.GetContext().Err()
		}
		if errors.As(err, &limit) {
			return nil, err
		}
		return nil, nil
	}
	if !sameTableDumpBoundExpression(expected, current) {
		return nil, nil
	}
	if err := ctx.GetContext().Err(); err != nil {
		return nil, err
	}
	return current, nil
}

// An unsupported local reconstruction rejects the whole candidate, including
// repairs already made to other siblings. It is not a catalog execution error.
var errLegacyFloatStringRebind = moerr.NewInternalError(moerr.NoReportContext(), "cannot reconstruct legacy float string function")

func rebindFloatStringFunction(ctx context.Context, original *planpb.Expr, args []*planpb.Expr) (*planpb.Expr, error) {
	fn := original.GetF()
	// SQL-origin proof authorizes persisted internal argument layouts, while
	// public SQL keeps using its checked argument-count contract.
	if id, _ := function.DecodeOverloadID(fn.Func.Obj); id == function.FORMAT {
		ctx = function.WithLegacySpecialConsumers(ctx)
		// Catalog FORMAT authors VARCHAR(max) arguments even for existing
		// strings. Recreate that declaration before overload resolution;
		// ordinary typed binding narrows casts and loses this owner contract.
		varchar := planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
		for i, arg := range args {
			var err error
			args[i], err = appendCastBeforeExpr(ctx, arg, varchar)
			if err != nil {
				return nil, err
			}
		}
	}
	bound, err := bindFuncExprImplByPlanExpr(ctx, nil, fn.Func.ObjName, args, true, nil, nil, true)
	if err != nil {
		return nil, err
	}
	if id, _ := function.DecodeOverloadID(fn.Func.Obj); id == function.FORMAT {
		if err := preservePersistedFormatCompatibility(ctx, bound); err != nil {
			return nil, err
		}
	}
	// IFNULL's AST lowering owns this contract in addition to ordinary CASE
	// overload binding. Historical SQL-origin proof made this marker trusted.
	if original.PreparedNumeric != nil && original.PreparedNumeric.IfnullCommonValue {
		if fn.Func.ObjName != "case" || len(args) != 3 || bound.GetF() == nil {
			return nil, errLegacyFloatStringRebind
		}
		applyIfNullCommonValueContract(bound, bound.GetF().Args)
	}
	return bound, nil
}

// A one-element safe IN list lowers to equality, so its list-owned cast may
// survive in an '=' descendant of an expanded IN. Recover that binding owner
// only when it exactly reconstructs the historical node; keep the same owner
// for the current binding rather than widening comparison metadata by hand.
func rebindFloatStringInComparison(ctx context.Context, original *planpb.Expr, args []*planpb.Expr) (*planpb.Expr, error) {
	if len(args) != 2 {
		return nil, errLegacyFloatStringRebind
	}
	var name string
	switch original.GetF().Func.ObjName {
	case "=":
		name = "in"
	case "!=":
		name = "not_in"
	default:
		return nil, errLegacyFloatStringRebind
	}
	list := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_tuple)}, Expr: &planpb.Expr_List{List: &planpb.ExprList{List: args[1:]}}}
	return bindFuncExprImplByPlanExpr(ctx, nil, name, []*planpb.Expr{args[0], list}, true, nil, nil, true)
}

// chargeFloatStringRebind bounds subtree copies and binder passes, including
// nested functions whose reconstruction costs O(nodes * depth).
func chargeFloatStringRebind(expr *planpb.Expr, budget *tableDumpBindingBudget) error {
	if expr == nil {
		return nil
	}
	budget.visits++
	if budget.visits > tableDumpMaxCandidateVisits {
		return &floatStringProofBudgetError{moerr.NewInvalidInputNoCtx("catalog expression binding exceeds limit")}
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if err := chargeFloatStringRebind(arg, budget); err != nil {
				return err
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, arg := range list.List {
			if err := chargeFloatStringRebind(arg, budget); err != nil {
				return err
			}
		}
	}
	if lit := expr.GetLit(); lit != nil {
		return chargeFloatStringRebind(lit.Src, budget)
	}
	return nil
}

// Only implicit float conversions authorize a reconstruction. An unaffected
// sibling keeps its persisted overload, casts and internal argument layout.
// Every scan shares the candidate budget, including repeated ancestor scans.
func affectedFloatStringExpr(ctx context.Context, expr *planpb.Expr, budget *tableDumpBindingBudget) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if expr == nil || expr.GetLit() != nil {
		return false, nil
	}
	budget.visits++
	if budget.visits > tableDumpMaxCandidateVisits {
		return false, moerr.NewInvalidInputNoCtx("catalog expression binding exceeds limit")
	}
	if fn := expr.GetF(); fn != nil {
		if isLegacyFloatStringCast(expr) && !fn.SyntaxExplicitCast {
			return true, nil
		}
		for _, arg := range fn.Args {
			if affected, err := affectedFloatStringExpr(ctx, arg, budget); affected || err != nil {
				return affected, err
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, arg := range list.List {
			if affected, err := affectedFloatStringExpr(ctx, arg, budget); affected || err != nil {
				return affected, err
			}
		}
	}
	return false, nil
}

// Peel only a single implicit VARCHAR conversion owned by the current binder
// operation. User casts and value-producing functions remain boundaries.
func peelFloatStringArgument(arg *planpb.Expr) (*planpb.Expr, bool) {
	cast := arg.GetF()
	if cast == nil || cast.Func == nil || cast.SyntaxExplicitCast || len(cast.Args) != 2 ||
		cast.Args[0] == nil || cast.Args[1] == nil || cast.Args[1].GetT() == nil ||
		arg.Typ.Id != int32(types.T_varchar) || !proto.Equal(&arg.Typ, &cast.Args[1].Typ) {
		return arg, false
	}
	id, overload := function.DecodeOverloadID(cast.Func.Obj)
	if id != function.CAST || overload != 0 {
		return arg, false
	}
	return cast.Args[0], isLegacyFloatStringCast(arg)
}

// Reconcile affected functions through the binder, including sibling implicit
// casts and argument-sensitive metadata. Exact historical reconstruction proves
// ownership of peeled casts; the encoded overload and final SQL-bound tree must
// still match. Declared casts remain boundaries around repaired children.
func widenVerifiedFloatStringExpr(ctx context.Context, expr *planpb.Expr, budget *tableDumpBindingBudget) (bool, error) {
	if affected, err := affectedFloatStringExpr(ctx, expr, budget); !affected || err != nil {
		return false, err
	}
	fn := expr.GetF()
	if fn == nil {
		changed := false
		if list := expr.GetList(); list != nil {
			for _, entry := range list.List {
				child, err := widenVerifiedFloatStringExpr(ctx, entry, budget)
				if err != nil {
					return false, err
				}
				changed = changed || child
			}
		}
		return changed, nil
	}
	id, _ := function.DecodeOverloadID(fn.Func.Obj)
	if id == function.CAST || fn.Func.ObjName == "cast_assign" || fn.Func.ObjName == "cast_strict" || fn.Func.ObjName == "cast_ignore" {
		changed := false
		for _, arg := range fn.Args {
			child, err := widenVerifiedFloatStringExpr(ctx, arg, budget)
			if err != nil {
				return false, err
			}
			changed = changed || child
		}
		if isLegacyFloatStringCast(expr) && !fn.SyntaxExplicitCast {
			expr.Typ.Width = 34
			fn.Args[1].Typ.Width = 34
			return true, nil
		}
		return changed, nil
	}
	if err := chargeFloatStringRebind(expr, budget); err != nil {
		return false, err
	}
	args := DeepCopyExprList(fn.Args)
	changed := false
	for i, arg := range args {
		var legacy bool
		args[i], legacy = peelFloatStringArgument(arg)
		changed = changed || legacy
	}
	// IN owns the conversions of its immediate RHS elements. Leaving those
	// wrappers behind changes its safe-list partition after the left side widens.
	// Exact historical reconstruction below proves each peeled cast's owner.
	if (id == function.IN || id == function.NOT_IN) && len(args) == 2 {
		if list := args[1].GetList(); list != nil {
			budget.visits += len(list.List)
			if budget.visits > tableDumpMaxCandidateVisits {
				return false, moerr.NewInvalidInputNoCtx("catalog expression binding exceeds limit")
			}
			for i, entry := range list.List {
				if err := ctx.Err(); err != nil {
					return false, err
				}
				var legacy bool
				list.List[i], legacy = peelFloatStringArgument(entry)
				changed = changed || legacy
			}
		}
	}

	if id == function.FORMAT {
		if err := chargeFloatStringRebind(expr, budget); err != nil {
			return false, err
		}
	}
	old, err := rebindFloatStringFunction(function.WithLegacyFloatStringBounds(ctx), expr, DeepCopyExprList(args))
	inComparison := false
	if err != nil || !sameTableDumpBoundExpression(expr, old) {
		if len(args) != 2 || (fn.Func.ObjName != "=" && fn.Func.ObjName != "!=") {
			return false, errLegacyFloatStringRebind
		}
		if err := chargeFloatStringRebind(expr, budget); err != nil {
			return false, err
		}
		old, err = rebindFloatStringInComparison(function.WithLegacyFloatStringBounds(ctx), expr, DeepCopyExprList(args))
		if err != nil || !sameTableDumpBoundExpression(expr, old) {
			return false, errLegacyFloatStringRebind
		}
		inComparison = true
	}
	for _, arg := range args {
		child, err := widenVerifiedFloatStringExpr(ctx, arg, budget)
		if err != nil {
			return false, err
		}
		changed = changed || child
	}
	if !changed {
		return false, nil
	}
	for _, arg := range args {
		if err := chargeFloatStringRebind(arg, budget); err != nil {
			return false, err
		}
	}
	if id == function.FORMAT {
		for _, arg := range args {
			if err := chargeFloatStringRebind(arg, budget); err != nil {
				return false, err
			}
		}
	}
	var current *planpb.Expr
	if inComparison {
		current, err = rebindFloatStringInComparison(ctx, expr, args)
	} else {
		current, err = rebindFloatStringFunction(ctx, expr, args)
	}
	if err != nil || current.GetF() == nil || current.GetF().Func == nil || current.GetF().Func.Obj != fn.Func.Obj {
		return false, errLegacyFloatStringRebind
	}
	expr.Typ = current.Typ
	expr.Expr = current.Expr
	return true, nil
}

// floatStringProofProcess changes only the borrowed evaluator's context and
// diagnostics. No context mode is installed on the caller or persisted tree;
// ordinary execution always uses current formatting. The child owns no Base.
// Historical folding is a private catalog proof era, independent of float
// notation and cast width. Live expressions retain current admission identities.
type historicalFloatDefaultFoldingKey struct{}

func historicalFloatDefaultFolding(ctx context.Context) bool {
	legacy, _ := ctx.Value(historicalFloatDefaultFoldingKey{}).(bool)
	return legacy
}

func floatStringProofProcess(ctx context.Context, proc *process.Process) *process.Process {
	child := proc.NewNoContextChildProc(0)
	child.Ctx = proc.Ctx
	if function.LegacyFloatStringFormatting(ctx) {
		child.Ctx = function.WithLegacyFloatStringFormatting(child.Ctx)
	}
	if historicalFloatDefaultFolding(ctx) {
		child.Ctx = context.WithValue(child.Ctx, historicalFloatDefaultFoldingKey{}, true)
	}
	child.WarningSink = &process.WarningProbe{}
	return child
}

// Preserve the public error while distinguishing fatal proof limits from an unavailable binder era.
type floatStringProofBudgetError struct{ error }

func (e *floatStringProofBudgetError) Unwrap() error { return e.error }
