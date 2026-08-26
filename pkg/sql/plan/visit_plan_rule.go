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
	"bytes"
	"context"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var (
	_ VisitPlanRule = &GetParamRule{}
	_ VisitPlanRule = &ResetParamOrderRule{}
	_ VisitPlanRule = &ResetParamRefRule{}
)

type GetParamRule struct {
	params            map[int]int
	mapTypes          map[int]int32
	paramTypes        []int32
	schemas           []*plan.ObjectRef
	indexDependencies []prepareIndexDependency
	exprMemo          map[*plan.Expr]*plan.Expr
}

type prepareIndexDependency struct {
	baseRef   *plan.ObjectRef
	snapshot  *Snapshot
	tableName string
}

func applyRuleToWindowSpec(rule VisitPlanRule, window *plan.WindowSpec) error {
	if window == nil {
		return nil
	}
	apply := func(expr **plan.Expr) error {
		if *expr == nil {
			return nil
		}
		var err error
		*expr, err = rule.ApplyExpr(*expr)
		return err
	}
	var err error
	if err = apply(&window.WindowFunc); err != nil {
		return err
	}
	for i := range window.PartitionBy {
		if err = apply(&window.PartitionBy[i]); err != nil {
			return err
		}
	}
	for i := range window.OrderBy {
		if window.OrderBy[i] != nil {
			if err = apply(&window.OrderBy[i].Expr); err != nil {
				return err
			}
		}
	}
	if window.Frame != nil {
		if window.Frame.Start != nil {
			if err = apply(&window.Frame.Start.Val); err != nil {
				return err
			}
		}
		if window.Frame.End != nil {
			if err = apply(&window.Frame.End.Val); err != nil {
				return err
			}
		}
	}
	return nil
}

func NewGetParamRule() *GetParamRule {
	return &GetParamRule{
		params:   make(map[int]int),
		mapTypes: make(map[int]int32),
	}
}

func (rule *GetParamRule) MatchNode(node *Node) bool {
	if node.NodeType == plan.Node_TABLE_SCAN ||
		node.NodeType == plan.Node_EXTERNAL_SCAN ||
		node.NodeType == plan.Node_INSERT {
		if node.ObjRef != nil && node.TableDef != nil {
			rule.schemas = append(rule.schemas, prepareSchemaRefWithSnapshot(
				node.ObjRef, node.TableDef, node.ScanSnapshot))
		}
		if node.NodeType == plan.Node_TABLE_SCAN && node.ObjRef != nil && node.TableDef != nil {
			for _, indexDef := range node.TableDef.Indexes {
				if indexDef != nil && indexplugin.IsPluginAlgo(indexDef.IndexAlgo) && indexDef.IndexTableName != "" {
					rule.indexDependencies = append(rule.indexDependencies, prepareIndexDependency{
						baseRef:   node.ObjRef,
						snapshot:  node.ScanSnapshot,
						tableName: indexDef.IndexTableName,
					})
				}
			}
		}
	} else if node.NodeType == plan.Node_MULTI_UPDATE {
		for _, updateCtx := range node.UpdateCtxList {
			rule.schemas = append(rule.schemas, prepareSchemaRef(updateCtx.ObjRef, updateCtx.TableDef))
		}
	}
	return false
}

// recordPreparedPluginDependencies preserves the catalog dependency closure of
// a plugin-index rewrite. Some rewrites can replace the owning TABLE_SCAN with
// a FUNCTION_SCAN, so ResetPreparePlan cannot recover these objects by walking
// the final plan alone.
func (builder *QueryBuilder) recordPreparedPluginDependencies(scanNode *Node) error {
	if !builder.isPrepareStatement || scanNode == nil || scanNode.ObjRef == nil || scanNode.TableDef == nil {
		return nil
	}

	dependencies := []*plan.ObjectRef{
		prepareSchemaRefWithSnapshot(scanNode.ObjRef, scanNode.TableDef, scanNode.ScanSnapshot),
	}
	for _, indexDef := range scanNode.TableDef.Indexes {
		if indexDef == nil || !indexplugin.IsPluginAlgo(indexDef.IndexAlgo) || indexDef.IndexTableName == "" {
			continue
		}
		objRef, tableDef, err := builder.compCtx.ResolveIndexTableByRef(
			scanNode.ObjRef, indexDef.IndexTableName, scanNode.ScanSnapshot)
		if err != nil {
			return err
		}
		if objRef == nil || tableDef == nil {
			return moerr.NewInternalErrorf(
				builder.GetContext(), "resolved index table %q without catalog metadata", indexDef.IndexTableName)
		}
		dependencies = append(dependencies,
			prepareSchemaRefWithSnapshot(objRef, tableDef, scanNode.ScanSnapshot))
	}

	builder.qry.CatalogDependencies = appendPrepareSchemas(
		builder.qry.CatalogDependencies, dependencies...)
	return nil
}

func (rule *GetParamRule) IsApplyExpr() bool {
	return true
}

func (rule *GetParamRule) ApplyNode(node *Node) error {
	return nil
}

func (rule *GetParamRule) ApplyExpr(e *plan.Expr) (*plan.Expr, error) {
	if e == nil {
		return nil, nil
	}
	if rewritten, ok := rule.exprMemo[e]; ok {
		return rewritten, nil
	}
	rewritten, err := rule.applyExpr(e)
	if err != nil {
		return nil, err
	}
	if rule.exprMemo == nil {
		rule.exprMemo = make(map[*plan.Expr]*plan.Expr)
	}
	rule.exprMemo[e] = rewritten
	return rewritten, nil
}

func (rule *GetParamRule) applyExpr(e *plan.Expr) (*plan.Expr, error) {
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_F:
		for i := range exprImpl.F.Args {
			exprImpl.F.Args[i], _ = rule.ApplyExpr(exprImpl.F.Args[i])
		}
		return e, nil
	case *plan.Expr_W:
		return applyWindowExpr(e, rule.ApplyExpr)
	case *plan.Expr_P:
		pos := int(exprImpl.P.Pos)
		rule.params[pos] = 0
		/*
			if e.Typ.Id == int32(types.T_any) && e.Typ.NotNullable {
				// is not null, use string
				rule.mapTypes[pos] = int32(types.T_varchar)
			} else {
				rule.mapTypes[pos] = e.Typ.Id
			}
		*/
		return e, nil
	case *plan.Expr_List:
		for i := range exprImpl.List.List {
			exprImpl.List.List[i], _ = rule.ApplyExpr(exprImpl.List.List[i])
		}
		return e, nil
	default:
		return e, nil
	}
}

func (rule *GetParamRule) SetParamOrder() {
	argPos := []int{}
	for pos := range rule.params {
		argPos = append(argPos, pos)
	}
	sort.Ints(argPos)
	rule.paramTypes = make([]int32, len(argPos))

	for idx, pos := range argPos {
		rule.params[pos] = idx
		rule.paramTypes[idx] = rule.mapTypes[pos]
	}
}

// ---------------------------

type ResetParamOrderRule struct {
	params   map[int]int
	exprMemo map[*plan.Expr]*plan.Expr
}

func NewResetParamOrderRule(params map[int]int) *ResetParamOrderRule {
	return &ResetParamOrderRule{
		params: params,
	}
}

func (rule *ResetParamOrderRule) MatchNode(_ *Node) bool {
	return false
}

func (rule *ResetParamOrderRule) IsApplyExpr() bool {
	return true
}

func (rule *ResetParamOrderRule) ApplyNode(node *Node) error {
	return nil
}

func (rule *ResetParamOrderRule) ApplyExpr(e *plan.Expr) (*plan.Expr, error) {
	if e == nil {
		return nil, nil
	}
	if rewritten, ok := rule.exprMemo[e]; ok {
		return rewritten, nil
	}
	rewritten, err := rule.applyExpr(e)
	if err != nil {
		return nil, err
	}
	if rule.exprMemo == nil {
		rule.exprMemo = make(map[*plan.Expr]*plan.Expr)
	}
	rule.exprMemo[e] = rewritten
	return rewritten, nil
}

func (rule *ResetParamOrderRule) applyExpr(e *plan.Expr) (*plan.Expr, error) {
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_F:
		for i := range exprImpl.F.Args {
			exprImpl.F.Args[i], _ = rule.ApplyExpr(exprImpl.F.Args[i])
		}
		return e, nil
	case *plan.Expr_W:
		return applyWindowExpr(e, rule.ApplyExpr)
	case *plan.Expr_P:
		exprImpl.P.Pos = int32(rule.params[int(exprImpl.P.Pos)])
		return e, nil
	case *plan.Expr_List:
		for i := range exprImpl.List.List {
			exprImpl.List.List[i], _ = rule.ApplyExpr(exprImpl.List.List[i])
		}
		return e, nil
	default:
		return e, nil
	}
}

// ---------------------------

type subqueryRootRule struct {
	pending []int32
}

func newSubqueryRootRule() *subqueryRootRule {
	return &subqueryRootRule{}
}

func (rule *subqueryRootRule) MatchNode(_ *Node) bool {
	return false
}

func (rule *subqueryRootRule) IsApplyExpr() bool {
	return true
}

func (rule *subqueryRootRule) ApplyNode(_ *Node) error {
	return nil
}

func (rule *subqueryRootRule) ApplyExpr(e *plan.Expr) (*plan.Expr, error) {
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_F:
		for i := range exprImpl.F.Args {
			exprImpl.F.Args[i], _ = rule.ApplyExpr(exprImpl.F.Args[i])
		}
	case *plan.Expr_List:
		for i := range exprImpl.List.List {
			exprImpl.List.List[i], _ = rule.ApplyExpr(exprImpl.List.List[i])
		}
	case *plan.Expr_W:
		if err := applyRuleToWindowSpec(rule, exprImpl.W); err != nil {
			return nil, err
		}
	case *plan.Expr_Sub:
		rule.pending = append(rule.pending, exprImpl.Sub.NodeId)
	}
	return e, nil
}

// ---------------------------

type decrementParamOrdinalRule struct {
	seen map[*plan.ParamRef]struct{}
}

func (rule *decrementParamOrdinalRule) MatchNode(_ *Node) bool {
	return false
}

func (rule *decrementParamOrdinalRule) IsApplyExpr() bool {
	return true
}

func (rule *decrementParamOrdinalRule) ApplyNode(_ *Node) error {
	return nil
}

func (rule *decrementParamOrdinalRule) ApplyExpr(e *plan.Expr) (*plan.Expr, error) {
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_F:
		for i := range exprImpl.F.Args {
			var err error
			exprImpl.F.Args[i], err = rule.ApplyExpr(exprImpl.F.Args[i])
			if err != nil {
				return nil, err
			}
		}
	case *plan.Expr_List:
		for i := range exprImpl.List.List {
			var err error
			exprImpl.List.List[i], err = rule.ApplyExpr(exprImpl.List.List[i])
			if err != nil {
				return nil, err
			}
		}
	case *plan.Expr_W:
		if err := applyRuleToWindowSpec(rule, exprImpl.W); err != nil {
			return nil, err
		}
	case *plan.Expr_P:
		if _, ok := rule.seen[exprImpl.P]; ok {
			return e, nil
		}
		rule.seen[exprImpl.P] = struct{}{}
		if exprImpl.P.Pos <= 0 {
			return nil, moerr.NewInternalErrorNoCtx("prepared parameter ordinal is not one-based")
		}
		exprImpl.P.Pos--
	}
	return e, nil
}

// ---------------------------

type ResetParamRefRule struct {
	ctx                  context.Context
	params               []*Expr
	paramKinds           []vector.PrepareParamKind
	exprMemo             map[*plan.Expr]*plan.Expr
	validateFunctionArgs func(string, []*Expr) error
}

func NewResetParamRefRule(ctx context.Context, params []*Expr) *ResetParamRefRule {
	return &ResetParamRefRule{
		ctx:    ctx,
		params: params,
	}
}

// SetParamKinds supplies the protocol type category observed for each
// prepared value.  The ordinary replacement path intentionally keeps
// parameters as text literals, but deferred numeric overloads can use this
// metadata to specialize an exact integer value without changing the typing
// of unrelated prepared expressions.
func (rule *ResetParamRefRule) SetParamKinds(kinds []vector.PrepareParamKind) {
	rule.paramKinds = kinds
}

func (rule *ResetParamRefRule) MatchNode(_ *Node) bool {
	return false
}

func (rule *ResetParamRefRule) IsApplyExpr() bool {
	return true
}

func (rule *ResetParamRefRule) ApplyNode(node *Node) error {
	return nil
}

func (rule *ResetParamRefRule) ApplyExpr(e *plan.Expr) (*plan.Expr, error) {
	if e == nil {
		return nil, nil
	}
	if rewritten, ok := rule.exprMemo[e]; ok {
		return rewritten, nil
	}
	rewritten, err := rule.applyExpr(e)
	if err != nil {
		return nil, err
	}
	if rule.exprMemo == nil {
		rule.exprMemo = make(map[*plan.Expr]*plan.Expr)
	}
	rule.exprMemo[e] = rewritten
	return rewritten, nil
}

func (rule *ResetParamRefRule) applyExpr(e *plan.Expr) (*plan.Expr, error) {
	var err error
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_F:
		originalArgs := append([]*plan.Expr(nil), exprImpl.F.Args...)
		var originalAbsArg *plan.Expr
		var hasPreparedAbsParam bool
		if strings.EqualFold(exprImpl.F.Func.GetObjName(), "abs") && len(originalArgs) == 1 {
			originalAbsArg = DeepCopyExpr(originalArgs[0])
			_, hasPreparedAbsParam = preparedNumericValueParamPosition(originalAbsArg)
		}
		if rule.validateFunctionArgs != nil {
			if err := rule.validateFunctionArgs(exprImpl.F.Func.GetObjName(), exprImpl.F.Args); err != nil {
				return nil, err
			}
		}
		needResetFunction := false
		for i, arg := range exprImpl.F.Args {
			if _, ok := arg.Expr.(*plan.Expr_P); ok {
				needResetFunction = true
			}
			exprImpl.F.Args[i], err = rule.ApplyExpr(arg)
			if err != nil {
				return nil, err
			}
		}

		// The prepare-time ABS(?) plan uses a DOUBLE cast so both integer and
		// fractional executions are accepted.  A binary integer value must not
		// pass through that cast: DOUBLE cannot represent all BIGINT values.
		// Rebind only the direct numeric-value path (not a CASE condition or
		// another control-flow parameter) and leave all other functions on their
		// existing replacement path.
		if hasPreparedAbsParam {
			_, fallbackOK := preparedNumericFallbackSource(exprImpl.F.Args[0])
			allInteger := rule.allIntegerParamRefs(originalAbsArg)
			if fallbackOK && allInteger {
				source, ok := preparedNumericFallbackSource(originalAbsArg)
				if !ok {
					return e, nil
				}
				reboundSource, changed, bindErr := rule.rebindPreparedIntegerExpr(source)
				if bindErr != nil {
					return nil, bindErr
				}
				if !changed {
					return e, nil
				}
				rewritten, bindErr := BindFuncExprImplByPlanExpr(rule.ctx, "abs", []*plan.Expr{reboundSource})
				if bindErr != nil {
					return nil, bindErr
				}
				if rewrittenFn := rewritten.GetF(); rewrittenFn != nil {
					rewrittenFn.AggConfig = bytes.Clone(exprImpl.F.AggConfig)
					rewrittenFn.AggConfigType = exprImpl.F.AggConfigType
				}
				return rewritten, nil
			}
		}

		// reset function
		if needResetFunction {
			rewritten, err := BindFuncExprImplByPlanExpr(
				rule.ctx,
				exprImpl.F.Func.GetObjName(),
				exprImpl.F.Args,
			)
			if err != nil {
				return nil, err
			}
			rewrittenFn := rewritten.GetF()
			if rewrittenFn != nil {
				rewrittenFn.AggConfig = bytes.Clone(exprImpl.F.AggConfig)
				rewrittenFn.AggConfigType = exprImpl.F.AggConfigType
			}
			rewritten.AuxId = e.AuxId
			return rewritten, nil
		}
		return e, nil
	case *plan.Expr_W:
		return applyWindowExpr(e, rule.ApplyExpr)
	case *plan.Expr_P:
		if int(exprImpl.P.Pos) >= len(rule.params) {
			return nil, moerr.NewInternalErrorf(context.TODO(), "get prepare params error, index %d not exists", int(exprImpl.P.Pos))
		}
		return &plan.Expr{
			Typ:  e.Typ,
			Expr: rule.params[int(exprImpl.P.Pos)].Expr,
		}, nil
	case *plan.Expr_List:
		for i, arg := range exprImpl.List.List {
			exprImpl.List.List[i], err = rule.ApplyExpr(arg)
			if err != nil {
				return nil, err
			}
		}
		return e, nil
	default:
		return e, nil
	}
}

func preparedNumericValueParamPosition(expr *plan.Expr) (int32, bool) {
	if expr == nil {
		return 0, false
	}
	if param := expr.GetP(); param != nil {
		return param.Pos, true
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return 0, false
	}
	name := strings.ToLower(fn.Func.GetObjName())
	if indexes, ok := numericFunctionResultArgs(name, len(fn.Args)); ok {
		for _, index := range indexes {
			if index >= 0 && index < len(fn.Args) {
				if pos, found := preparedNumericValueParamPosition(fn.Args[index]); found {
					return pos, true
				}
			}
		}
		return 0, false
	}
	if name == "case" {
		for index, arg := range fn.Args {
			if !numericFunctionArgKeepsContext(name, index, len(fn.Args)) {
				continue
			}
			if pos, ok := preparedNumericValueParamPosition(arg); ok {
				return pos, true
			}
		}
		return 0, false
	}
	for _, arg := range fn.Args {
		if pos, ok := preparedNumericValueParamPosition(arg); ok {
			return pos, true
		}
	}
	return 0, false
}

func (rule *ResetParamRefRule) allIntegerParamRefs(expr *plan.Expr) bool {
	positions := make(map[int32]struct{})
	collectNumericValueParamPositions(expr, positions)
	if len(positions) == 0 {
		return false
	}
	for pos := range positions {
		if pos < 0 || int(pos) >= len(rule.paramKinds) ||
			rule.paramKinds[pos] != vector.PrepareParamInteger {
			return false
		}
	}
	return true
}

func collectNumericValueParamPositions(expr *plan.Expr, positions map[int32]struct{}) {
	if expr == nil {
		return
	}
	if param := expr.GetP(); param != nil {
		positions[param.Pos] = struct{}{}
		return
	}
	if fn := expr.GetF(); fn != nil {
		name := strings.ToLower(fn.Func.GetObjName())
		if indexes, ok := numericFunctionResultArgs(name, len(fn.Args)); ok {
			for _, index := range indexes {
				if index >= 0 && index < len(fn.Args) {
					collectNumericValueParamPositions(fn.Args[index], positions)
				}
			}
			return
		}
		if name == "case" {
			for index, arg := range fn.Args {
				if numericFunctionArgKeepsContext(name, index, len(fn.Args)) {
					collectNumericValueParamPositions(arg, positions)
				}
			}
			return
		}
		for _, arg := range fn.Args {
			collectNumericValueParamPositions(arg, positions)
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			collectNumericValueParamPositions(item, positions)
		}
	}
}

func (rule *ResetParamRefRule) rebindPreparedIntegerExpr(expr *plan.Expr) (*plan.Expr, bool, error) {
	if expr == nil {
		return nil, false, nil
	}
	if param := expr.GetP(); param != nil {
		typed, ok := rule.typedIntegerParamExpr(param.Pos)
		if !ok {
			return expr, false, nil
		}
		return typed, true, nil
	}
	if fn := expr.GetF(); fn != nil {
		args := make([]*plan.Expr, len(fn.Args))
		changed := false
		for i, arg := range fn.Args {
			rebound, argChanged, err := rule.rebindPreparedIntegerExpr(arg)
			if err != nil {
				return nil, false, err
			}
			args[i] = rebound
			changed = changed || argChanged
		}
		if !changed {
			return expr, false, nil
		}
		rebound, err := BindFuncExprImplByPlanExpr(rule.ctx, fn.Func.GetObjName(), args)
		if err != nil {
			return nil, false, err
		}
		if reboundFn := rebound.GetF(); reboundFn != nil {
			reboundFn.AggConfig = bytes.Clone(fn.AggConfig)
			reboundFn.AggConfigType = fn.AggConfigType
		}
		return rebound, true, nil
	}
	if list := expr.GetList(); list != nil {
		copied := DeepCopyExpr(expr)
		changed := false
		for i, item := range copied.GetList().List {
			rebound, itemChanged, err := rule.rebindPreparedIntegerExpr(item)
			if err != nil {
				return nil, false, err
			}
			copied.GetList().List[i] = rebound
			changed = changed || itemChanged
		}
		return copied, changed, nil
	}
	return expr, false, nil
}

func preparedNumericFallbackSource(expr *plan.Expr) (*plan.Expr, bool) {
	if !isPreparedNumericFallbackExpr(expr) {
		return nil, false
	}
	if fn := expr.GetF(); fn != nil && fn.Func != nil &&
		strings.EqualFold(fn.Func.GetObjName(), "cast") && len(fn.Args) > 0 {
		return fn.Args[0], true
	}
	return expr, true
}

func (rule *ResetParamRefRule) typedIntegerParamExpr(pos int32) (*plan.Expr, bool) {
	if pos < 0 || int(pos) >= len(rule.params) || int(pos) >= len(rule.paramKinds) ||
		rule.paramKinds[pos] != vector.PrepareParamInteger {
		return nil, false
	}
	lit := rule.params[pos].GetLit()
	if lit == nil || lit.GetIsnull() {
		return nil, false
	}
	sval := lit.GetSval()
	if sval == "" {
		return nil, false
	}
	if strings.HasPrefix(sval, "-") {
		value, err := strconv.ParseInt(sval, 10, 64)
		if err != nil {
			return nil, false
		}
		return makePlan2Int64ConstExprWithType(value), true
	}
	value, err := strconv.ParseUint(sval, 10, 64)
	if err != nil {
		return nil, false
	}
	if value <= uint64(math.MaxInt64) {
		return makePlan2Int64ConstExprWithType(int64(value)), true
	}
	return makePlan2Uint64ConstExprWithType(value), true
}

func applyWindowExpr(e *plan.Expr, apply func(*plan.Expr) (*plan.Expr, error)) (*plan.Expr, error) {
	w := e.GetW()
	if w == nil {
		return e, nil
	}

	var err error
	if w.WindowFunc != nil {
		w.WindowFunc, err = apply(w.WindowFunc)
		if err != nil {
			return nil, err
		}
	}
	for i := range w.PartitionBy {
		w.PartitionBy[i], err = apply(w.PartitionBy[i])
		if err != nil {
			return nil, err
		}
	}
	for i := range w.OrderBy {
		if w.OrderBy[i] == nil || w.OrderBy[i].Expr == nil {
			continue
		}
		w.OrderBy[i].Expr, err = apply(w.OrderBy[i].Expr)
		if err != nil {
			return nil, err
		}
	}
	if w.Frame != nil {
		if w.Frame.Start != nil && w.Frame.Start.Val != nil {
			w.Frame.Start.Val, err = apply(w.Frame.Start.Val)
			if err != nil {
				return nil, err
			}
		}
		if w.Frame.End != nil && w.Frame.End.Val != nil {
			w.Frame.End.Val, err = apply(w.Frame.End.Val)
			if err != nil {
				return nil, err
			}
		}
	}
	return e, nil
}
