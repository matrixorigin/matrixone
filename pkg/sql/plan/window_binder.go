// Copyright 2022 Matrix Origin
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
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type windowFuncExprBinder interface {
	BindExpr(tree.Expr, int32, bool) (*plan.Expr, error)
	bindFuncExprImplByAstExpr(string, []tree.Expr, int32) (*plan.Expr, error)
	bindPreparedNumericFuncExpr(string, []tree.Expr, int32) (*plan.Expr, error)
	bindPreparedWindowFrameBound(tree.Expr, *plan.Type) (*plan.Expr, error)
	makeFrameConstValue(tree.Expr, *plan.Type) (*plan.Expr, error)
	GetContext() context.Context
}

const maxWindowsPerQueryBlock = 127

func validateQueryBlockWindowCount(ctx context.Context, clause *tree.SelectClause, orderBy tree.OrderBy) error {
	count := len(clause.Windows)
	countExpr := func(expr tree.Expr) {
		if expr == nil || count > maxWindowsPerQueryBlock {
			return
		}
		walkGroupingSetOrderByExpr(expr, func(candidate tree.Expr) bool {
			if _, subquery := candidate.(*tree.Subquery); subquery {
				return false
			}
			if function, ok := candidate.(*tree.FuncExpr); ok && function.WindowSpec != nil {
				// OVER name reuses a named window. OVER (...) defines an
				// additional implicit window, including OVER (name ...).
				if function.WindowSpec.RefName == nil || !function.WindowSpec.ReferencedOnly {
					count++
				}
			}
			return count <= maxWindowsPerQueryBlock
		})
	}

	for _, selectExpr := range clause.Exprs {
		countExpr(selectExpr.Expr)
	}
	if clause.Where != nil {
		countExpr(clause.Where.Expr)
	}
	if clause.GroupBy != nil {
		for _, group := range clause.GroupBy.GroupByExprsList {
			for _, expr := range group {
				countExpr(expr)
			}
		}
	}
	if clause.Having != nil {
		countExpr(clause.Having.Expr)
	}
	for _, order := range orderBy {
		if order != nil {
			countExpr(order.Expr)
		}
	}

	if count > maxWindowsPerQueryBlock {
		return moerr.NewErrTooManyWindows(ctx, count, maxWindowsPerQueryBlock)
	}
	return nil
}

func windowExprAstKey(astExpr tree.Expr) string {
	funcExpr, ok := astExpr.(*tree.FuncExpr)
	if !ok || funcExpr.WindowSpec == nil || funcExpr.WindowSpec.Frame == nil || funcExpr.WindowSpec.HasFrame {
		return semanticAstKey(astExpr)
	}

	funcExprCopy := *funcExpr
	windowSpecCopy := *funcExpr.WindowSpec
	windowSpecCopy.HasFrame = true
	funcExprCopy.WindowSpec = &windowSpecCopy
	return semanticAstKey(&funcExprCopy)
}

func semanticAstKey(astExpr tree.Expr) string {
	return semanticNodeKey(astExpr)
}

func semanticNodeKey(node tree.NodeFormatter) string {
	display := tree.String(node, dialect.MYSQL)
	identity := tree.StringWithOpts(node, dialect.MYSQL, tree.WithParamExprOffset())
	if identity == display {
		return display
	}
	return identity + "\x00" + display
}

func semanticAstDisplayName(key string) string {
	if separator := strings.LastIndexByte(key, 0); separator >= 0 {
		return key[separator+1:]
	}
	return key
}

func windowFuncAstName(astExpr *tree.FuncExpr) string {
	if astExpr.FuncName != nil {
		return astExpr.FuncName.Origin()
	}
	if funcRef, ok := astExpr.Func.FunctionReference.(*tree.UnresolvedName); ok {
		return funcRef.ColName()
	}
	return "unknown"
}

func findNestedWindowFuncNameInExprs(exprs ...tree.Expr) (string, bool) {
	for _, expr := range exprs {
		if name, ok := findNestedWindowFuncName(expr); ok {
			return name, true
		}
	}
	return "", false
}

func findNestedWindowFuncNameInOrderBy(orderBy tree.OrderBy) (string, bool) {
	for _, order := range orderBy {
		if order == nil {
			continue
		}
		if name, ok := findNestedWindowFuncName(order.Expr); ok {
			return name, true
		}
	}
	return "", false
}

func findNestedWindowFuncName(expr tree.Expr) (string, bool) {
	switch e := expr.(type) {
	case nil:
		return "", false
	case *tree.FuncExpr:
		if e.WindowSpec != nil {
			return windowFuncAstName(e), true
		}
		if name, ok := findNestedWindowFuncNameInExprs(e.Exprs...); ok {
			return name, true
		}
		return findNestedWindowFuncNameInOrderBy(e.OrderBy)
	case *tree.BinaryExpr:
		return findNestedWindowFuncNameInExprs(e.Left, e.Right)
	case *tree.UnaryExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.ComparisonExpr:
		return findNestedWindowFuncNameInExprs(e.Left, e.Right, e.Escape)
	case *tree.AndExpr:
		return findNestedWindowFuncNameInExprs(e.Left, e.Right)
	case *tree.XorExpr:
		return findNestedWindowFuncNameInExprs(e.Left, e.Right)
	case *tree.OrExpr:
		return findNestedWindowFuncNameInExprs(e.Left, e.Right)
	case *tree.NotExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.IsNullExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.IsNotNullExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.IsUnknownExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.IsNotUnknownExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.IsTrueExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.IsNotTrueExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.IsFalseExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.IsNotFalseExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.ParenExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.CastExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.BitCastExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.Tuple:
		return findNestedWindowFuncNameInExprs(e.Exprs...)
	case *tree.RangeCond:
		return findNestedWindowFuncNameInExprs(e.Left, e.From, e.To)
	case *tree.CaseExpr:
		if name, ok := findNestedWindowFuncName(e.Expr); ok {
			return name, true
		}
		for _, when := range e.Whens {
			if when == nil {
				continue
			}
			if name, ok := findNestedWindowFuncNameInExprs(when.Cond, when.Val); ok {
				return name, true
			}
		}
		return findNestedWindowFuncName(e.Else)
	case *tree.IntervalExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.DefaultVal:
		return findNestedWindowFuncName(e.Expr)
	case *tree.SerialExtractExpr:
		return findNestedWindowFuncNameInExprs(e.SerialExpr, e.IndexExpr)
	case *tree.Subquery:
		return "", false
	default:
		return "", false
	}
}

func rejectNestedWindowFunc(ctx context.Context, expr tree.Expr) error {
	if name, ok := findNestedWindowFuncName(expr); ok {
		return moerr.NewSyntaxErrorf(ctx, "You cannot use the window function '%s' in this context", name)
	}
	return nil
}

func validateWindowFuncNoNested(ctx context.Context, astExpr *tree.FuncExpr) error {
	for _, arg := range astExpr.Exprs {
		if err := rejectNestedWindowFunc(ctx, arg); err != nil {
			return err
		}
	}
	if name, ok := findNestedWindowFuncNameInOrderBy(astExpr.OrderBy); ok {
		return moerr.NewSyntaxErrorf(ctx, "You cannot use the window function '%s' in this context", name)
	}

	ws := astExpr.WindowSpec
	if ws == nil {
		return nil
	}
	for _, group := range ws.PartitionBy {
		if err := rejectNestedWindowFunc(ctx, group); err != nil {
			return err
		}
	}
	if name, ok := findNestedWindowFuncNameInOrderBy(ws.OrderBy); ok {
		return moerr.NewSyntaxErrorf(ctx, "You cannot use the window function '%s' in this context", name)
	}
	if ws.Frame != nil {
		if ws.Frame.Start != nil {
			if err := rejectNestedWindowFunc(ctx, ws.Frame.Start.Expr); err != nil {
				return err
			}
		}
		if ws.Frame.End != nil {
			if err := rejectNestedWindowFunc(ctx, ws.Frame.End.Expr); err != nil {
				return err
			}
		}
	}
	return nil
}

func rejectWindowResultDependency(ctx context.Context, expr *plan.Expr, windowTag int32) error {
	if HasTag(expr, windowTag) {
		return moerr.NewSyntaxError(ctx, "You cannot use a window function result in another window function in the same query block")
	}
	return nil
}

func cloneWindowSpec(spec *tree.WindowSpec) *tree.WindowSpec {
	if spec == nil {
		return nil
	}
	cloned := *spec
	cloned.PartitionBy = cloneTreeExprs(spec.PartitionBy)
	if len(spec.OrderBy) > 0 {
		cloned.OrderBy = make(tree.OrderBy, len(spec.OrderBy))
		for i, order := range spec.OrderBy {
			if order == nil {
				continue
			}
			orderCopy := *order
			orderCopy.Expr = cloneTreeExpr(order.Expr)
			cloned.OrderBy[i] = &orderCopy
		}
	}
	if spec.Frame != nil {
		frameCopy := *spec.Frame
		if spec.Frame.Start != nil {
			startCopy := *spec.Frame.Start
			startCopy.Expr = cloneTreeExpr(spec.Frame.Start.Expr)
			frameCopy.Start = &startCopy
		}
		if spec.Frame.End != nil {
			endCopy := *spec.Frame.End
			endCopy.Expr = cloneTreeExpr(spec.Frame.End.Expr)
			frameCopy.End = &endCopy
		}
		cloned.Frame = &frameCopy
	}
	return &cloned
}

func inheritWindowSpec(
	ctx context.Context,
	base, local *tree.WindowSpec,
	childName, baseName string,
) (*tree.WindowSpec, error) {
	if len(local.PartitionBy) > 0 {
		return nil, moerr.NewWindowNoChildPartitioning(ctx)
	}
	if base.HasFrame {
		return nil, moerr.NewWindowNoInheritFrame(ctx, baseName)
	}
	if len(base.OrderBy) > 0 && len(local.OrderBy) > 0 {
		return nil, moerr.NewWindowNoRedefineOrderBy(ctx, childName, baseName)
	}

	merged := cloneWindowSpec(base)
	if len(local.OrderBy) > 0 {
		merged.OrderBy = cloneWindowSpec(local).OrderBy
	}
	if local.HasFrame {
		localCopy := cloneWindowSpec(local)
		merged.HasFrame = true
		merged.Frame = localCopy.Frame
	} else {
		// The parser materializes an implicit frame before named-window
		// inheritance is resolved. Drop that provisional frame here so it can
		// be rebuilt from the final inherited ORDER BY clause.
		merged.HasFrame = false
		merged.Frame = nil
	}
	merged.RefName = nil
	merged.ReferencedOnly = false
	return merged, nil
}

func resolveNamedWindowDefinitions(ctx context.Context, definitions tree.WindowDefinitions) (map[string]*tree.WindowSpec, error) {
	raw := make(map[string]*tree.WindowDefinition, len(definitions))
	for _, definition := range definitions {
		if definition == nil || definition.Name == nil || definition.Spec == nil {
			return nil, moerr.NewSyntaxError(ctx, "Invalid named window definition")
		}
		name := definition.Name.Compare()
		if _, exists := raw[name]; exists {
			return nil, moerr.NewWindowDuplicateName(ctx, definition.Name.Origin())
		}
		raw[name] = definition
	}

	resolved := make(map[string]*tree.WindowSpec, len(raw))
	state := make(map[string]uint8, len(raw))
	var resolve func(string, string) (*tree.WindowSpec, error)
	resolve = func(name, origin string) (*tree.WindowSpec, error) {
		if spec, ok := resolved[name]; ok {
			return spec, nil
		}
		definition, ok := raw[name]
		if !ok {
			return nil, moerr.NewWindowNoSuchWindow(ctx, origin)
		}
		if state[name] == 1 {
			return nil, moerr.NewWindowCircularityInWindowGraph(ctx)
		}
		state[name] = 1
		local := cloneWindowSpec(definition.Spec)
		var spec *tree.WindowSpec
		if local.RefName == nil {
			spec = local
			spec.ReferencedOnly = false
		} else {
			baseName := local.RefName.Compare()
			base, err := resolve(baseName, local.RefName.Origin())
			if err != nil {
				return nil, err
			}
			spec, err = inheritWindowSpec(
				ctx, base, local, definition.Name.Origin(), local.RefName.Origin(),
			)
			if err != nil {
				return nil, err
			}
		}
		state[name] = 2
		resolved[name] = spec
		return spec, nil
	}

	for _, definition := range definitions {
		if _, err := resolve(definition.Name.Compare(), definition.Name.Origin()); err != nil {
			return nil, err
		}
	}
	return resolved, nil
}

func resolveWindowSpecReference(
	ctx context.Context,
	spec *tree.WindowSpec,
	namedWindows map[string]*tree.WindowSpec,
) (*tree.WindowSpec, error) {
	local := cloneWindowSpec(spec)
	if local == nil || local.RefName == nil {
		return local, nil
	}
	baseName := local.RefName.Compare()
	base, ok := namedWindows[baseName]
	if !ok {
		return nil, moerr.NewWindowNoSuchWindow(ctx, local.RefName.Origin())
	}
	if local.ReferencedOnly {
		resolved := cloneWindowSpec(base)
		resolved.RefName = nil
		resolved.ReferencedOnly = false
		return resolved, nil
	}
	return inheritWindowSpec(ctx, base, local, "<unnamed window>", local.RefName.Origin())
}

func ensureDefaultWindowFrame(spec *tree.WindowSpec) {
	if spec == nil || spec.Frame != nil {
		return
	}
	spec.HasFrame = false
	spec.Frame = &tree.FrameClause{
		Type:  tree.Range,
		Start: &tree.FrameBound{Type: tree.Preceding, UnBounded: true},
	}
	if len(spec.OrderBy) == 0 {
		spec.Frame.End = &tree.FrameBound{Type: tree.Following, UnBounded: true}
	} else {
		spec.Frame.End = &tree.FrameBound{Type: tree.CurrentRow}
	}
}

func expandNamedWindowReferences(
	ctx context.Context,
	clause *tree.SelectClause,
	orderBy tree.OrderBy,
) (*tree.SelectClause, tree.OrderBy, error) {
	if len(clause.Windows) == 0 {
		return clause, orderBy, nil
	}
	namedWindows, err := resolveNamedWindowDefinitions(ctx, clause.Windows)
	if err != nil {
		return nil, nil, err
	}

	clonedClause := cloneTreeValue(
		reflect.ValueOf(clause), make(map[treeClonePointer]reflect.Value),
	).Interface().(*tree.SelectClause)
	for _, definition := range clonedClause.Windows {
		if definition == nil || definition.Name == nil {
			continue
		}
		definition.Spec = cloneWindowSpec(namedWindows[definition.Name.Compare()])
	}
	var clonedOrderBy tree.OrderBy
	if orderBy != nil {
		clonedOrderBy = make(tree.OrderBy, len(orderBy))
		for i, order := range orderBy {
			if order == nil {
				continue
			}
			orderCopy := *order
			orderCopy.Expr = cloneTreeExpr(order.Expr)
			clonedOrderBy[i] = &orderCopy
		}
	}

	var expandErr error
	expand := func(expr tree.Expr) bool {
		if _, subquery := expr.(*tree.Subquery); subquery {
			return false
		}
		function, ok := expr.(*tree.FuncExpr)
		if !ok || function.WindowSpec == nil || function.WindowSpec.RefName == nil {
			return true
		}
		function.WindowSpec, expandErr = resolveWindowSpecReference(ctx, function.WindowSpec, namedWindows)
		return expandErr == nil
	}
	walk := func(expr tree.Expr) {
		if expr != nil && expandErr == nil {
			walkGroupingSetOrderByExpr(expr, expand)
		}
	}
	for _, selectExpr := range clonedClause.Exprs {
		walk(selectExpr.Expr)
	}
	if clonedClause.Where != nil {
		walk(clonedClause.Where.Expr)
	}
	if clonedClause.GroupBy != nil {
		for _, group := range clonedClause.GroupBy.GroupByExprsList {
			for _, expr := range group {
				walk(expr)
			}
		}
	}
	if clonedClause.Having != nil {
		walk(clonedClause.Having.Expr)
	}
	for _, order := range clonedOrderBy {
		if order != nil {
			walk(order.Expr)
		}
	}
	if expandErr != nil {
		return nil, nil, expandErr
	}
	return clonedClause, clonedOrderBy, nil
}

func cloneWindowValidationMap[K comparable, V any](source map[K]V) map[K]V {
	if source == nil {
		return nil
	}
	cloned := make(map[K]V, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

func cloneBindContextForWindowValidation(ctx *BindContext) *BindContext {
	cloned := *ctx
	cloned.queryBlockOwner = &cloned
	cloned.groupByAst = cloneWindowValidationMap(ctx.groupByAst)
	cloned.groupByCanonicalAst = cloneWindowValidationMap(ctx.groupByCanonicalAst)
	cloned.groupByParamAst = cloneWindowValidationMap(ctx.groupByParamAst)
	cloned.aggregateByAst = cloneWindowValidationMap(ctx.aggregateByAst)
	cloned.sampleByAst = cloneWindowValidationMap(ctx.sampleByAst)
	cloned.windowByAst = cloneWindowValidationMap(ctx.windowByAst)
	cloned.projectByExpr = cloneWindowValidationMap(ctx.projectByExpr)
	cloned.timeByAst = cloneWindowValidationMap(ctx.timeByAst)
	cloned.projectColByAst = cloneWindowValidationMap(ctx.projectColByAst)
	cloned.flattenedVolatileExprs = cloneWindowValidationMap(ctx.flattenedVolatileExprs)
	cloned.groups = append([]*plan.Expr(nil), ctx.groups...)
	cloned.aggregates = append([]*plan.Expr(nil), ctx.aggregates...)
	cloned.projects = append([]*plan.Expr(nil), ctx.projects...)
	cloned.results = append([]*plan.Expr(nil), ctx.results...)
	cloned.windows = append([]*plan.Expr(nil), ctx.windows...)
	cloned.times = append([]*plan.Expr(nil), ctx.times...)
	cloned.timeAsts = append([]tree.Expr(nil), ctx.timeAsts...)
	cloned.views = append([]string(nil), ctx.views...)
	return &cloned
}

type windowValidationCTERefSnapshot struct {
	ref          *CTERef
	isRecursive  bool
	occurrences  []cteOccurrence
	hasNestedRef bool
	hasNestedUse bool
}

type windowValidationContextSnapshot struct {
	ctx   *BindContext
	views []string
}

// snapshotWindowValidationCTEState keeps the validation-only builder from
// publishing its node ids through the mutable CTE metadata shared by the real
// query builder. CTE declaration contexts are included because binding a CTE
// can also record view dependencies on the declaration root.
func snapshotWindowValidationCTEState(ctx *BindContext) func() {
	cteSnapshots := make(map[*CTERef]windowValidationCTERefSnapshot)
	contextSnapshots := make(map[*BindContext]windowValidationContextSnapshot)

	var collectContext func(*BindContext)
	collectCTE := func(ref *CTERef) {
		if ref == nil {
			return
		}
		if _, exists := cteSnapshots[ref]; exists {
			return
		}
		cteSnapshots[ref] = windowValidationCTERefSnapshot{
			ref:          ref,
			isRecursive:  ref.isRecursive,
			occurrences:  append([]cteOccurrence(nil), ref.occurrences...),
			hasNestedRef: ref.hasNestedRef,
			hasNestedUse: ref.hasNestedUse,
		}
		collectContext(ref.declarationCtx)
	}
	collectContext = func(current *BindContext) {
		if current == nil {
			return
		}
		if _, exists := contextSnapshots[current]; exists {
			return
		}
		contextSnapshots[current] = windowValidationContextSnapshot{
			ctx:   current,
			views: append([]string(nil), current.views...),
		}
		for _, ref := range current.cteByName {
			collectCTE(ref)
		}
		for _, ref := range current.boundCtes {
			collectCTE(ref)
		}
		collectCTE(current.cteState.cte)
		collectContext(current.parent)
	}
	collectContext(ctx)

	return func() {
		for _, snapshot := range cteSnapshots {
			snapshot.ref.isRecursive = snapshot.isRecursive
			snapshot.ref.occurrences = snapshot.occurrences
			snapshot.ref.hasNestedRef = snapshot.hasNestedRef
			snapshot.ref.hasNestedUse = snapshot.hasNestedUse
		}
		for _, snapshot := range contextSnapshots {
			snapshot.ctx.views = snapshot.views
		}
	}
}

func validateNamedWindowDefinitions(builder *QueryBuilder, ctx *BindContext, definitions tree.WindowDefinitions) error {
	if len(definitions) == 0 {
		return nil
	}

	validationBuilder := NewQueryBuilder(plan.Query_SELECT, builder.compCtx, builder.isPrepareStatement, true)
	validationBuilder.nextBindTag = builder.nextBindTag
	validationCtx := cloneBindContextForWindowValidation(ctx)
	restoreCTEState := snapshotWindowValidationCTEState(validationCtx)
	defer restoreCTEState()
	havingBinder := NewHavingBinder(validationBuilder, validationCtx)
	projectionBinder := NewProjectionBinder(validationBuilder, validationCtx, havingBinder)
	validationCtx.binder = projectionBinder

	for _, definition := range definitions {
		if definition == nil || definition.Spec == nil {
			continue
		}
		windowSpec := cloneWindowSpec(definition.Spec)
		ensureDefaultWindowFrame(windowSpec)
		validationExpr := &tree.FuncExpr{WindowSpec: windowSpec}
		if err := validateWindowFuncNoNested(builder.GetContext(), validationExpr); err != nil {
			return err
		}
		if _, err := bindWindowSpec(
			projectionBinder, validationCtx, "", windowSpec, 0, true, false,
		); err != nil {
			return err
		}
	}
	if err := mergeWindowValidationDependencies(builder, validationBuilder); err != nil {
		return err
	}
	appendWindowValidationPrivilegeScans(builder, validationBuilder)
	if builder.isPrepareStatement {
		seen := make(map[int]struct{})
		for _, metadata := range builder.qry.Params {
			if param := metadata.GetP(); param != nil {
				seen[int(param.Pos)] = struct{}{}
			}
		}
		collect := func(expr tree.Expr) {
			walkGroupingSetOrderByExpr(expr, func(candidate tree.Expr) bool {
				param, ok := candidate.(*tree.ParamExpr)
				if !ok {
					return true
				}
				if _, exists := seen[param.Offset]; exists {
					return false
				}
				seen[param.Offset] = struct{}{}
				builder.qry.Params = append(builder.qry.Params, &plan.Expr{
					Typ:  plan.Type{Id: int32(types.T_any)},
					Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: int32(param.Offset)}},
				})
				return false
			})
		}
		for _, definition := range definitions {
			if definition == nil || definition.Spec == nil {
				continue
			}
			for _, expr := range definition.Spec.PartitionBy {
				collect(expr)
			}
			for _, order := range definition.Spec.OrderBy {
				if order != nil {
					collect(order.Expr)
				}
			}
			if definition.Spec.Frame != nil {
				if definition.Spec.Frame.Start != nil {
					collect(definition.Spec.Frame.Start.Expr)
				}
				if definition.Spec.Frame.End != nil {
					collect(definition.Spec.Frame.End.Expr)
				}
			}
		}
	}
	return nil
}

// appendWindowValidationPrivilegeScans makes every validation-only relation
// recognized by the frontend authorization walk visible without connecting it
// to a query step. They are compact ownership metadata, never executable plan
// roots: keeping a complete TableDef here would make a legal 127-window query
// retain a copy of each wide schema it happens to validate.
func appendWindowValidationPrivilegeScans(builder, validationBuilder *QueryBuilder) {
	seen := make(map[string]struct{})
	for _, node := range validationBuilder.qry.Nodes {
		carrier := windowValidationPrivilegeCarrier(node)
		if carrier == nil {
			continue
		}
		key := windowValidationPrivilegeCarrierKey(carrier)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		builder.windowValidationScans = append(builder.windowValidationScans, carrier)
	}
}

// windowValidationPrivilegeCarrier mirrors the node classes inspected by
// extractPrivilegeTipsFromPlan. Keep this closure in sync with that frontend
// contract rather than treating TABLE_SCAN as a proxy for every relation that
// semantic binding can resolve.
func windowValidationPrivilegeCarrier(node *plan.Node) *plan.Node {
	if node == nil || node.ObjRef == nil {
		return nil
	}

	carrier := &plan.Node{
		NodeType:     node.NodeType,
		ObjRef:       DeepCopyObjectRef(node.ObjRef),
		ParentObjRef: DeepCopyObjectRef(node.ParentObjRef),
		ScanSnapshot: DeepCopySnapshot(node.ScanSnapshot),
		OriginViews:  append([]string(nil), node.OriginViews...),
		DirectView:   node.DirectView,
	}
	if node.TableDef != nil {
		// Authorization only needs the table type to identify cluster tables.
		carrier.TableDef = &plan.TableDef{TableType: node.TableDef.TableType}
	}

	switch node.NodeType {
	case plan.Node_TABLE_SCAN:
		return carrier
	case plan.Node_EXTERNAL_SCAN:
		if node.ExternScan == nil || node.ExternScan.Type != int32(plan.ExternType_MONGODB_TB) {
			return nil
		}
		// The frontend recognizes MongoDB scans from this discriminator alone.
		carrier.ExternScan = &plan.ExternScan{Type: node.ExternScan.Type}
		return carrier
	case plan.Node_FUNCTION_SCAN:
		if node.TableDef == nil || node.TableDef.TblFunc == nil || node.TableDef.TblFunc.Name != "table_changes" {
			return nil
		}
		// isPrivilegeBearingTableScan identifies this function by name.
		carrier.TableDef = &plan.TableDef{
			TableType: node.TableDef.TableType,
			TblFunc:   &plan.TableFunction{Name: "table_changes"},
		}
		return carrier
	default:
		return nil
	}
}

// windowValidationPrivilegeCarrierKey preserves distinct view and snapshot
// authorization contexts while coalescing repeated references to one relation.
func windowValidationPrivilegeCarrierKey(node *plan.Node) string {
	return fmt.Sprintf("%d/%v/%v/%v/%s/%s", node.NodeType, node.ObjRef,
		node.ParentObjRef, node.ScanSnapshot, strings.Join(node.OriginViews, "\x00"), node.DirectView)
}

// mergeWindowValidationDependencies retains the catalog closure discovered
// while binding declarations that are not used by an executable window
// function. Those declarations still undergo semantic binding, so their table,
// view, and plugin-index dependencies must participate in privilege checks and
// prepared-plan invalidation without adding executable nodes to the query.
func mergeWindowValidationDependencies(builder, validationBuilder *QueryBuilder) error {
	dependencyRule := NewGetParamRule()
	for _, node := range validationBuilder.qry.Nodes {
		if node != nil {
			dependencyRule.MatchNode(node)
		}
	}

	dependencies := append([]*plan.ObjectRef(nil), validationBuilder.qry.CatalogDependencies...)
	dependencies = appendPrepareSchemas(dependencies, dependencyRule.schemas...)
	for _, dependency := range dependencyRule.indexDependencies {
		objRef, tableDef, err := builder.compCtx.ResolveIndexTableByRef(
			dependency.baseRef, dependency.tableName, dependency.snapshot)
		if err != nil {
			return err
		}
		if objRef == nil || tableDef == nil {
			return moerr.NewInternalErrorf(
				builder.GetContext(), "resolved index table %q without catalog metadata", dependency.tableName)
		}
		dependencies = appendPrepareSchemas(
			dependencies, prepareSchemaRefWithSnapshot(objRef, tableDef, dependency.snapshot))
	}
	builder.qry.CatalogDependencies = appendPrepareSchemas(builder.qry.CatalogDependencies, dependencies...)
	return nil
}

func bindWindowSpec(
	b windowFuncExprBinder,
	ctx *BindContext,
	funcName string,
	ws *tree.WindowSpec,
	depth int32,
	isRoot bool,
	consumerSpecific bool,
) (*plan.WindowSpec, error) {
	w := &plan.WindowSpec{}

	if consumerSpecific && function.GetFunctionIgnoresWindowFrameByName(funcName) && !ws.HasFrame {
		ws.Frame = &tree.FrameClause{Type: tree.Rows}
		ws.Frame.Start = &tree.FrameBound{Type: tree.Preceding, UnBounded: true}
		ws.Frame.End = &tree.FrameBound{Type: tree.Following, UnBounded: true}
	}

	for _, group := range ws.PartitionBy {
		expr, err := b.BindExpr(group, depth, isRoot)
		if err != nil {
			return nil, err
		}
		if err = rejectStandaloneIntervalExpr(b.GetContext(), expr, "window PARTITION BY"); err != nil {
			return nil, err
		}
		if err = rejectWindowResultDependency(b.GetContext(), expr, ctx.windowTag); err != nil {
			return nil, err
		}
		// Partition membership is an equality boundary.  Normalize only the
		// key expression so value-returning window functions still expose the
		// original padded representation.
		expr, err = appendPadSpaceWindowKeyCastIfNeeded(b.GetContext(), expr)
		if err != nil {
			return nil, err
		}
		w.PartitionBy = append(w.PartitionBy, expr)
	}

	if ws.OrderBy != nil {
		w.OrderBy = make([]*plan.OrderBySpec, 0, len(ws.OrderBy))
		for _, order := range ws.OrderBy {
			expr, err := b.BindExpr(order.Expr, depth, isRoot)
			if err != nil {
				return nil, err
			}
			if err = rejectStandaloneIntervalOrderExpr(b.GetContext(), expr); err != nil {
				return nil, err
			}
			if err = rejectWindowResultDependency(b.GetContext(), expr, ctx.windowTag); err != nil {
				return nil, err
			}

			// Keep enum/set window ordering aligned with definition order. The
			// originating block can use the raw storage value; a pure display
			// column crossing a query boundary uses its planner provenance.
			if isEnumOrSetDisplayValueExpr(expr) {
				fn := expr.GetF()
				if len(fn.Args) == 2 && isEnumOrSetPlanType(&fn.Args[1].Typ) {
					expr = fn.Args[1]
				}
			} else if storageType := ctx.mysqlSpecialOrderTypeForExpr(expr); storageType != nil {
				expr, err = makeMySQLSpecialOrderKey(b.GetContext(), expr, storageType)
				if err != nil {
					return nil, err
				}
			}
			// Window peer groups and rank ordering use this expression as a key.
			// Apply the same semantic key normalization after any storage-order
			// rewrite, without touching the window function result itself.
			expr, err = appendPadSpaceWindowKeyCastIfNeeded(b.GetContext(), expr)
			if err != nil {
				return nil, err
			}

			orderBy := &plan.OrderBySpec{
				Expr: expr,
				Flag: plan.OrderBySpec_INTERNAL,
			}

			switch order.Direction {
			case tree.Ascending:
				orderBy.Flag |= plan.OrderBySpec_ASC
			case tree.Descending:
				orderBy.Flag |= plan.OrderBySpec_DESC
			}

			switch order.NullsPosition {
			case tree.NullsFirst:
				orderBy.Flag |= plan.OrderBySpec_NULLS_FIRST
			case tree.NullsLast:
				orderBy.Flag |= plan.OrderBySpec_NULLS_LAST
			}

			w.OrderBy = append(w.OrderBy, orderBy)
		}
	}

	switch ws.Frame.Start.Type {
	case tree.Following:
		if ws.Frame.Start.UnBounded {
			return nil, moerr.NewParseError(b.GetContext(), "Window '<unnamed window>': frame start cannot be UNBOUNDED FOLLOWING.")
		}
		if ws.Frame.End.Type == tree.Preceding || ws.Frame.End.Type == tree.CurrentRow {
			return nil, newWindowFrameIllegalError(b.GetContext())
		}
	case tree.CurrentRow:
		if ws.Frame.End.Type == tree.Preceding {
			return nil, newWindowFrameIllegalError(b.GetContext())
		}
	}

	if ws.Frame.End.Type == tree.Preceding && ws.Frame.End.UnBounded {
		return nil, moerr.NewParseError(b.GetContext(), "Window '<unnamed window>': frame end cannot be UNBOUNDED PRECEDING.")
	}

	w.Frame = &plan.FrameClause{
		Type: plan.FrameClause_FrameType(ws.Frame.Type),
		Start: &plan.FrameBound{
			Type:      plan.FrameBound_BoundType(ws.Frame.Start.Type),
			UnBounded: ws.Frame.Start.UnBounded,
		},
		End: &plan.FrameBound{
			Type:      plan.FrameBound_BoundType(ws.Frame.End.Type),
			UnBounded: ws.Frame.End.UnBounded,
		},
	}
	var typ *plan.Type
	switch ws.Frame.Type {
	case tree.Rows:
		typ = &plan.Type{Id: int32(types.T_uint64)}
	case tree.Range:
		if len(w.OrderBy) != 1 && isNRange(ws.Frame) {
			return nil, moerr.NewParseError(b.GetContext(), "Window '<unnamed window>' with RANGE N PRECEDING/FOLLOWING frame requires exactly one ORDER BY expression, of numeric or temporal type")
		}
		if len(w.OrderBy) == 0 {
			break
		}
		typ = &w.OrderBy[0].Expr.Typ
		t := types.Type{Oid: types.T(typ.Id)}
		if consumerSpecific && !function.GetFunctionIsWinOrderFunByName(funcName) && isNRange(ws.Frame) && !t.IsNumericOrTemporal() {
			return nil, moerr.NewParseError(b.GetContext(), "Window '<unnamed window>' with RANGE frame requires ORDER BY expression of numeric or temporal type")
		}
	case tree.Groups:
		return nil, moerr.NewNYI(b.GetContext(), "GROUPS in WINDOW FUNCTION condition")
	}
	if isPreparedWindowIntervalBound(ws.Frame.Start.Expr) || isPreparedWindowIntervalBound(ws.Frame.End.Expr) {
		return nil, moerr.NewNotSupported(b.GetContext(), "prepared parameter markers in interval window frames")
	}
	if ws.Frame.Type == tree.Range &&
		(isWindowFrameParam(ws.Frame.Start.Expr) || isWindowFrameParam(ws.Frame.End.Expr)) &&
		(typ == nil || !types.Type{Oid: types.T(typ.Id)}.IsNumeric()) {
		return nil, moerr.NewParseError(b.GetContext(), "Window '<unnamed window>' with a parameterized RANGE frame requires a numeric ORDER BY expression")
	}
	var err error
	if ws.Frame.Start.Expr != nil {
		if isWindowFrameParam(ws.Frame.Start.Expr) {
			w.Frame.Start.Val, err = b.bindPreparedWindowFrameBound(ws.Frame.Start.Expr, typ)
		} else {
			w.Frame.Start.Val, err = b.makeFrameConstValue(ws.Frame.Start.Expr, typ)
		}
		if err != nil {
			return nil, err
		}
		if err = rejectWindowResultDependency(b.GetContext(), w.Frame.Start.Val, ctx.windowTag); err != nil {
			return nil, err
		}
	}
	if ws.Frame.End.Expr != nil {
		if isWindowFrameParam(ws.Frame.End.Expr) {
			w.Frame.End.Val, err = b.bindPreparedWindowFrameBound(ws.Frame.End.Expr, typ)
		} else {
			w.Frame.End.Val, err = b.makeFrameConstValue(ws.Frame.End.Expr, typ)
		}
		if err != nil {
			return nil, err
		}
		if err = rejectWindowResultDependency(b.GetContext(), w.Frame.End.Val, ctx.windowTag); err != nil {
			return nil, err
		}
	}

	return w, nil
}

func bindWindowFuncExpr(b windowFuncExprBinder, ctx *BindContext, funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	if astExpr.Type == tree.FUNC_TYPE_DISTINCT {
		return nil, moerr.NewNYI(b.GetContext(), "DISTINCT in window function")
	}

	if err := validateCountArgs(b.GetContext(), funcName, astExpr); err != nil {
		return nil, err
	}
	ws := cloneWindowSpec(astExpr.WindowSpec)
	if ws == nil {
		return nil, moerr.NewSyntaxErrorf(b.GetContext(), "Window function '%s' requires an OVER clause", funcName)
	}
	if ws.RefName != nil {
		return nil, moerr.NewWindowNoSuchWindow(b.GetContext(), ws.RefName.Origin())
	}
	ensureDefaultWindowFrame(ws)
	resolvedAstExpr := *astExpr
	resolvedAstExpr.WindowSpec = ws
	if err := validateWindowFuncNoNested(b.GetContext(), &resolvedAstExpr); err != nil {
		return nil, err
	}
	if len(astExpr.OrderBy) > 0 {
		return nil, moerr.NewNYI(b.GetContext(), "function-local ORDER BY in window function")
	}

	astStr := windowExprAstKey(&resolvedAstExpr)

	// window function
	windowFunc, err := b.bindPreparedNumericFuncExpr(funcName, astExpr.Exprs, depth)
	if err != nil {
		return nil, err
	}
	if err = rejectWindowResultDependency(b.GetContext(), windowFunc, ctx.windowTag); err != nil {
		return nil, err
	}
	w, err := bindWindowSpec(b, ctx, funcName, ws, depth, isRoot, true)
	if err != nil {
		return nil, err
	}
	w.WindowFunc = windowFunc
	w.Name = funcName

	if colPos, ok := ctx.windowByAst[astStr]; ok {
		return buildWindowColRefExpr(ctx, ctx.windows[colPos].Typ, colPos), nil
	}

	colPos := int32(len(ctx.windows))
	ctx.windows = append(ctx.windows, &plan.Expr{
		Typ:  w.WindowFunc.Typ,
		Expr: &plan.Expr_W{W: w},
	})
	ctx.windowByAst[astStr] = colPos

	return buildWindowColRefExpr(ctx, w.WindowFunc.Typ, colPos), nil
}

func isWindowFrameParam(expr tree.Expr) bool {
	_, ok := expr.(*tree.ParamExpr)
	return ok
}

func isPreparedWindowIntervalBound(expr tree.Expr) bool {
	interval, ok := expr.(*tree.FuncExpr)
	if !ok {
		return false
	}
	return hasWindowFrameParam(interval)
}

func hasWindowFrameParam(expr tree.Expr) bool {
	switch expr := expr.(type) {
	case *tree.ParamExpr:
		return true
	case *tree.BinaryExpr:
		return hasWindowFrameParam(expr.Left) || hasWindowFrameParam(expr.Right)
	case *tree.UnaryExpr:
		return hasWindowFrameParam(expr.Expr)
	case *tree.ComparisonExpr:
		return hasWindowFrameParam(expr.Left) ||
			hasWindowFrameParam(expr.Right) ||
			hasWindowFrameParam(expr.Escape)
	case *tree.AndExpr:
		return hasWindowFrameParam(expr.Left) || hasWindowFrameParam(expr.Right)
	case *tree.XorExpr:
		return hasWindowFrameParam(expr.Left) || hasWindowFrameParam(expr.Right)
	case *tree.OrExpr:
		return hasWindowFrameParam(expr.Left) || hasWindowFrameParam(expr.Right)
	case *tree.NotExpr:
		return hasWindowFrameParam(expr.Expr)
	case *tree.IsNullExpr:
		return hasWindowFrameParam(expr.Expr)
	case *tree.IsNotNullExpr:
		return hasWindowFrameParam(expr.Expr)
	case *tree.IsUnknownExpr:
		return hasWindowFrameParam(expr.Expr)
	case *tree.IsNotUnknownExpr:
		return hasWindowFrameParam(expr.Expr)
	case *tree.IsTrueExpr:
		return hasWindowFrameParam(expr.Expr)
	case *tree.IsNotTrueExpr:
		return hasWindowFrameParam(expr.Expr)
	case *tree.IsFalseExpr:
		return hasWindowFrameParam(expr.Expr)
	case *tree.IsNotFalseExpr:
		return hasWindowFrameParam(expr.Expr)
	case *tree.Subquery:
		// A frame bound cannot be folded through a subquery safely.
		return true
	case *tree.FuncExpr:
		return hasWindowFrameParamInExprs(expr.Exprs) || hasWindowFrameParamInOrderBy(expr.OrderBy)
	case *tree.ExprList:
		return hasWindowFrameParamInExprs(expr.Exprs)
	case *tree.ParenExpr:
		return hasWindowFrameParam(expr.Expr)
	case *tree.CastExpr:
		return hasWindowFrameParam(expr.Expr)
	case *tree.BitCastExpr:
		return hasWindowFrameParam(expr.Expr)
	case *tree.Tuple:
		return hasWindowFrameParamInExprs(expr.Exprs)
	case *tree.RangeCond:
		return hasWindowFrameParam(expr.Left) ||
			hasWindowFrameParam(expr.From) ||
			hasWindowFrameParam(expr.To)
	case *tree.CaseExpr:
		if hasWindowFrameParam(expr.Expr) || hasWindowFrameParam(expr.Else) {
			return true
		}
		for _, when := range expr.Whens {
			if when != nil && (hasWindowFrameParam(when.Cond) || hasWindowFrameParam(when.Val)) {
				return true
			}
		}
	case *tree.IntervalExpr:
		return hasWindowFrameParam(expr.Expr)
	case *tree.DefaultVal:
		return hasWindowFrameParam(expr.Expr)
	case *tree.VarExpr:
		return hasWindowFrameParam(expr.Expr)
	case *tree.SerialExtractExpr:
		return hasWindowFrameParam(expr.SerialExpr) || hasWindowFrameParam(expr.IndexExpr)
	case *tree.FullTextMatchExpr:
		return hasWindowFrameParam(expr.Pattern)
	}
	return false
}

func hasWindowFrameParamInExprs(exprs tree.Exprs) bool {
	for _, expr := range exprs {
		if hasWindowFrameParam(expr) {
			return true
		}
	}
	return false
}

func hasWindowFrameParamInOrderBy(orderBy tree.OrderBy) bool {
	for _, order := range orderBy {
		if order != nil && hasWindowFrameParam(order.Expr) {
			return true
		}
	}
	return false
}

func (b *baseBinder) bindPreparedWindowFrameBound(expr tree.Expr, typ *plan.Type) (*plan.Expr, error) {
	if b.builder == nil || !b.builder.isPrepareStatement {
		return nil, moerr.NewInvalidInput(b.GetContext(), "only prepare statement can use ? expr")
	}
	if typ == nil {
		return nil, moerr.NewInvalidInput(b.GetContext(), "window frame bound parameter requires a target type")
	}
	bound, err := b.impl.BindExpr(expr, 0, true)
	if err != nil {
		return nil, err
	}
	return appendCastBeforeExpr(b.GetContext(), bound, *typ)
}

func buildWindowColRefExpr(ctx *BindContext, typ plan.Type, colPos int32) *plan.Expr {
	return &plan.Expr{
		Typ: typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: ctx.windowTag,
				ColPos: colPos,
			},
		},
	}
}

func makeWindowFrameConstValue(
	baseBindExpr func(tree.Expr, int32, bool) (*Expr, error),
	proc *process.Process,
	bindCtx context.Context,
	expr tree.Expr,
	typ *plan.Type,
) (*plan.Expr, error) {
	e, err := baseBindExpr(expr, 0, true)
	if err != nil {
		return nil, err
	}
	if e.Typ.Id == int32(types.T_interval) {
		return resetWindowIntervalExpr(bindCtx, proc, e)
	}
	if typ == nil {
		return e, nil
	}
	e, err = appendCastBeforeExpr(bindCtx, e, *typ)
	if err != nil {
		return nil, err
	}

	executor, err := colexec.NewExpressionExecutor(proc, e)
	if err != nil {
		return nil, err
	}
	defer executor.Free()
	vec, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return nil, err
	}
	c := rule.GetConstantValue(vec, false, 0)

	return &plan.Expr{
		Typ:  *typ,
		Expr: &plan.Expr_Lit{Lit: c},
	}, nil
}

func resetWindowIntervalExpr(bindCtx context.Context, proc *process.Process, e *Expr) (*Expr, error) {
	e1 := e.Expr.(*plan.Expr_List).List.List[0]
	e2 := e.Expr.(*plan.Expr_List).List.List[1]

	intervalTypeStr := e2.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Sval).Sval
	intervalType, err := types.IntervalTypeOf(intervalTypeStr)
	if err != nil {
		return nil, err
	}

	if e1.Typ.Id == int32(types.T_varchar) || e1.Typ.Id == int32(types.T_char) {
		s := e1.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Sval).Sval
		returnNum, returnType, err := types.NormalizeInterval(s, intervalType)
		if err != nil {
			returnNum = math.MaxInt64
			returnType = intervalType
		}

		return setWindowIntervalValue(bindCtx, e, returnNum, returnType)
	}

	isTimeUnit := intervalType == types.Second || intervalType == types.Minute ||
		intervalType == types.Hour || intervalType == types.Day
	isDecimalOrFloat := e1.Typ.Id == int32(types.T_decimal64) ||
		e1.Typ.Id == int32(types.T_decimal128) || e1.Typ.Id == int32(types.T_float32) ||
		e1.Typ.Id == int32(types.T_float64)
	lit := e1.GetLit()
	if isTimeUnit && isDecimalOrFloat && lit != nil && !lit.Isnull {
		var floatVal float64
		var hasValue bool

		if dval, ok := lit.Value.(*plan.Literal_Dval); ok {
			floatVal = dval.Dval
			hasValue = true
		} else if fval, ok := lit.Value.(*plan.Literal_Fval); ok {
			floatVal = float64(fval.Fval)
			hasValue = true
		} else if d64val, ok := lit.Value.(*plan.Literal_Decimal64Val); ok {
			d64 := types.Decimal64(d64val.Decimal64Val.A)
			scale := e1.Typ.Scale
			if scale < 0 {
				scale = 0
			}
			floatVal = types.Decimal64ToFloat64(d64, scale)
			hasValue = true
		} else if d128val, ok := lit.Value.(*plan.Literal_Decimal128Val); ok {
			d128 := types.Decimal128{B0_63: uint64(d128val.Decimal128Val.A), B64_127: uint64(d128val.Decimal128Val.B)}
			scale := e1.Typ.Scale
			if scale < 0 {
				scale = 0
			}
			floatVal = types.Decimal128ToFloat64(d128, scale)
			hasValue = true
		}

		if hasValue {
			if floatVal < 0 {
				return nil, newWindowFrameIllegalError(bindCtx)
			}
			var finalValue int64
			switch intervalType {
			case types.Second:
				finalValue = int64(math.Round(floatVal * float64(types.MicroSecsPerSec)))
			case types.Minute:
				finalValue = int64(math.Round(floatVal * float64(types.MicroSecsPerSec*types.SecsPerMinute)))
			case types.Hour:
				finalValue = int64(math.Round(floatVal * float64(types.MicroSecsPerSec*types.SecsPerHour)))
			case types.Day:
				finalValue = int64(math.Round(floatVal * float64(types.MicroSecsPerSec*types.SecsPerDay)))
			}
			return setWindowIntervalValue(bindCtx, e, finalValue, types.MicroSecond)
		}
	}

	typ := &plan.Type{Id: int32(types.T_int64)}
	numberExpr, err := appendCastBeforeExpr(bindCtx, e1, *typ)
	if err != nil {
		return nil, err
	}

	executor, err := colexec.NewExpressionExecutor(proc, numberExpr)
	if err != nil {
		return nil, err
	}
	defer executor.Free()
	vec, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return nil, err
	}
	c := rule.GetConstantValue(vec, false, 0)

	var finalValue int64
	if c.Isnull {
		finalValue = math.MaxInt64
	} else if ival, ok := c.Value.(*plan.Literal_I64Val); ok {
		finalValue = ival.I64Val
	} else {
		return nil, moerr.NewInvalidInput(bindCtx, "invalid interval value")
	}

	return setWindowIntervalValue(bindCtx, e, finalValue, intervalType)
}

func setWindowIntervalValue(
	bindCtx context.Context,
	e *Expr,
	value int64,
	intervalType types.IntervalType,
) (*Expr, error) {
	if value < 0 {
		return nil, newWindowFrameIllegalError(bindCtx)
	}

	e.Expr.(*plan.Expr_List).List.List[0] = makePlan2Int64ConstExprWithType(value)
	e.Expr.(*plan.Expr_List).List.List[1] = makePlan2Int64ConstExprWithType(int64(intervalType))
	return e, nil
}

func newWindowFrameIllegalError(ctx context.Context) error {
	return moerr.NewParseError(ctx, "Window '<unnamed window>': frame start or end is negative, NULL or of non-integral type")
}
