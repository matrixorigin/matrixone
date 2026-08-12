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
	"sort"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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
				if indexplugin.IsPluginAlgo(indexDef.IndexAlgo) && indexDef.IndexTableName != "" {
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
		if !indexplugin.IsPluginAlgo(indexDef.IndexAlgo) || indexDef.IndexTableName == "" {
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
	ctx                         context.Context
	params                      []*Expr
	exprMemo                    map[*plan.Expr]*plan.Expr
	exactDecimalComparisonsOnly bool
	realDecimalGroups           map[int32]bool
}

func NewResetParamRefRule(ctx context.Context, params []*Expr) *ResetParamRefRule {
	return &ResetParamRefRule{
		ctx:    ctx,
		params: params,
	}
}

func NewResetExactDecimalComparisonParamRule(
	ctx context.Context, params []*Expr, realDecimalGroups map[int32]bool,
) *ResetParamRefRule {
	return &ResetParamRefRule{
		ctx:                         ctx,
		params:                      params,
		exactDecimalComparisonsOnly: true,
		realDecimalGroups:           realDecimalGroups,
	}
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
	if rule.exactDecimalComparisonsOnly && e.ExactDecimalParam {
		replacement, ok, err := rule.preparedDecimalComparisonValue(e)
		if err != nil {
			return nil, err
		}
		if ok {
			return replacement, nil
		}
	}
	var err error
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_F:
		needResetFunction := false
		comparisonNeedsReal := rule.realDecimalGroups[e.ExactDecimalGroup]
		preparedInGroup, preparedInNeedsReal := preparedDecimalInCommonDomain(exprImpl.F, rule.params)
		if isDecimalComparisonOperator(exprImpl.F.Func.GetObjName()) {
			for i, arg := range exprImpl.F.Args {
				replacement, ok, err := rule.preparedDecimalComparisonValue(arg)
				if err != nil {
					return nil, err
				}
				if ok {
					exprImpl.F.Args[i] = replacement
					needResetFunction = true
					comparisonNeedsReal = comparisonNeedsReal || preparedDecimalGroupNeedsReal(replacement)
				}
			}
		}
		for i, arg := range exprImpl.F.Args {
			if _, ok := arg.Expr.(*plan.Expr_P); ok && !rule.exactDecimalComparisonsOnly {
				needResetFunction = true
			}
			exprImpl.F.Args[i], err = rule.ApplyExpr(arg)
			if err != nil {
				return nil, err
			}
		}
		if preparedInGroup {
			return rebindPreparedDecimalInGroup(rule.ctx, e, preparedInNeedsReal)
		}
		if comparisonNeedsReal {
			return rebindPreparedDecimalGroupAsReal(rule.ctx, e)
		}
		if rule.exactDecimalComparisonsOnly && e.ExactDecimalParam &&
			(exprImpl.F.Func.GetObjName() == "and" || exprImpl.F.Func.GetObjName() == "or") &&
			preparedDecimalGroupNeedsReal(e) {
			return rebindPreparedDecimalGroupAsReal(rule.ctx, e)
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
			rewritten.ExactDecimalGroup = e.ExactDecimalGroup
			return rewritten, nil
		}
		return e, nil
	case *plan.Expr_W:
		return applyWindowExpr(e, rule.ApplyExpr)
	case *plan.Expr_P:
		if rule.exactDecimalComparisonsOnly {
			return e, nil
		}
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

func (rule *ResetParamRefRule) preparedDecimalComparisonValue(expr *plan.Expr) (*plan.Expr, bool, error) {
	cast, ok := preparedDecimalComparisonCast(expr)
	if !ok {
		return nil, false, nil
	}

	paramRef := cast.Args[0].GetP()
	if int(paramRef.Pos) >= len(rule.params) {
		return nil, false, moerr.NewInternalErrorf(
			context.TODO(),
			"get prepare params error, index %d not exists",
			int(paramRef.Pos),
		)
	}
	raw := DeepCopyExpr(rule.params[int(paramRef.Pos)])
	literal := raw.GetLit()
	if literal == nil || literal.Isnull || literal.IsBin {
		return nil, false, nil
	}
	stringValue, ok := literal.Value.(*plan.Literal_Sval)
	if !ok {
		var numericValue string
		switch value := literal.Value.(type) {
		case *plan.Literal_I64Val:
			numericValue = strconv.FormatInt(value.I64Val, 10)
		case *plan.Literal_U64Val:
			numericValue = strconv.FormatUint(value.U64Val, 10)
		case *plan.Literal_Bval:
			if value.Bval {
				numericValue = "1"
			} else {
				numericValue = "0"
			}
		default:
			// FLOAT/DOUBLE retain the protocol REAL domain. The enclosing
			// comparison, and any marked BETWEEN/IN group, will be rebound.
			raw.ExactDecimalParam = true
			return raw, true, nil
		}
		exact, exactOK, err := makePlan2ExactDecimalStringExprWithType(rule.ctx, numericValue)
		if err != nil {
			return nil, false, err
		}
		if exactOK {
			exact.ExactDecimalParam = true
			return exact, true, nil
		}
		raw.ExactDecimalParam = true
		return raw, true, nil
	}
	numericValue, ok := function.GetNumericStringPrefix(stringValue.Sval)
	if !ok {
		raw.ExactDecimalParam = true
		return raw, true, nil
	}
	exact, ok, err := makePlan2ExactDecimalStringExprWithType(rule.ctx, numericValue)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		raw.ExactDecimalParam = true
		return raw, true, nil
	}
	exact.ExactDecimalParam = true
	return exact, true, nil
}

func rebindPreparedDecimalInGroup(ctx context.Context, expr *plan.Expr, real bool) (*plan.Expr, error) {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 2 || fn.Args[1].GetList() == nil {
		return expr, nil
	}
	args := DeepCopyExprList(fn.Args)
	if real {
		floatType := types.T_float64.ToType()
		target := makePlan2Type(&floatType)
		var err error
		args[0], err = appendCastBeforeExpr(ctx, args[0], target)
		if err != nil {
			return nil, err
		}
		for i, item := range args[1].GetList().List {
			item.ExactDecimalParam = false
			item.ExactDecimalGroup = 0
			args[1].GetList().List[i], err = appendCastBeforeExpr(ctx, item, target)
			if err != nil {
				return nil, err
			}
		}
	}
	rebound, err := BindFuncExprImplByPlanExpr(ctx, fn.Func.GetObjName(), args)
	if err == nil {
		rebound.ExactDecimalGroup = expr.ExactDecimalGroup
	}
	return rebound, err
}

func preparedDecimalInCommonDomain(fn *plan.Function, params []*Expr) (bool, bool) {
	if fn == nil || fn.Func == nil || (fn.Func.GetObjName() != "in" && fn.Func.GetObjName() != "not_in") ||
		len(fn.Args) != 2 || fn.Args[1].GetList() == nil {
		return false, false
	}
	count := 0
	real := false
	for _, item := range fn.Args[1].GetList().List {
		cast, ok := preparedDecimalComparisonCast(item)
		if !ok {
			continue
		}
		count++
		pos := int(cast.Args[0].GetP().Pos)
		if pos >= 0 && pos < len(params) && preparedDecimalParamRequiresReal(params[pos]) {
			real = true
		}
	}
	return count > 1, real
}

func preparedDecimalGroupNeedsReal(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.ExactDecimalParam {
		typ := types.T(expr.Typ.Id)
		if typ == types.T_float32 || typ == types.T_float64 || typ.IsMySQLString() {
			return true
		}
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if preparedDecimalGroupNeedsReal(arg) {
				return true
			}
		}
	}
	return false
}

func rebindPreparedDecimalGroupAsReal(ctx context.Context, expr *plan.Expr) (*plan.Expr, error) {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return expr, nil
	}
	name := fn.Func.GetObjName()
	if isDecimalComparisonOperator(name) {
		floatType := types.T_float64.ToType()
		target := makePlan2Type(&floatType)
		args := make([]*plan.Expr, len(fn.Args))
		for i, arg := range fn.Args {
			var err error
			args[i], err = appendCastBeforeExpr(ctx, arg, target)
			if err != nil {
				return nil, err
			}
		}
		rebound, err := BindFuncExprImplByPlanExpr(ctx, name, args)
		if err == nil {
			rebound.ExactDecimalGroup = expr.ExactDecimalGroup
		}
		return rebound, err
	}
	for i, arg := range fn.Args {
		var err error
		fn.Args[i], err = rebindPreparedDecimalGroupAsReal(ctx, arg)
		if err != nil {
			return nil, err
		}
	}
	rebound, err := BindFuncExprImplByPlanExpr(ctx, name, fn.Args)
	if err != nil {
		return nil, err
	}
	rebound.ExactDecimalParam = expr.ExactDecimalParam
	rebound.ExactDecimalGroup = expr.ExactDecimalGroup
	return rebound, nil
}

type findPreparedDecimalGroupDomainsRule struct {
	params     []*Expr
	realGroups map[int32]bool
}

func (rule *findPreparedDecimalGroupDomainsRule) MatchNode(_ *Node) bool { return false }
func (rule *findPreparedDecimalGroupDomainsRule) IsApplyExpr() bool      { return true }
func (rule *findPreparedDecimalGroupDomainsRule) ApplyNode(_ *Node) error {
	return nil
}

func (rule *findPreparedDecimalGroupDomainsRule) ApplyExpr(expr *plan.Expr) (*plan.Expr, error) {
	if expr == nil {
		return nil, nil
	}
	if group := expr.ExactDecimalGroup; group != 0 && preparedDecimalGroupHasRealParam(expr, rule.params) {
		rule.realGroups[group] = true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if _, err := rule.ApplyExpr(arg); err != nil {
				return nil, err
			}
		}
	} else if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if _, err := rule.ApplyExpr(item); err != nil {
				return nil, err
			}
		}
	}
	return expr, nil
}

func preparedDecimalGroupHasRealParam(expr *plan.Expr, params []*Expr) bool {
	if cast, ok := preparedDecimalComparisonCast(expr); ok {
		pos := int(cast.Args[0].GetP().Pos)
		if pos >= 0 && pos < len(params) {
			return preparedDecimalParamRequiresReal(params[pos])
		}
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if preparedDecimalGroupHasRealParam(arg, params) {
				return true
			}
		}
	} else if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if preparedDecimalGroupHasRealParam(item, params) {
				return true
			}
		}
	}
	return false
}

func preparedDecimalParamRequiresReal(param *Expr) bool {
	if param == nil {
		return false
	}
	typ := types.T(param.Typ.Id)
	if typ == types.T_float32 || typ == types.T_float64 {
		return true
	}
	if literal := param.GetLit(); literal != nil && literal.IsBin {
		return true
	}
	if typ.IsMySQLString() {
		literal := param.GetLit()
		if literal == nil || literal.Isnull {
			return false
		}
		_, ok := function.GetNumericStringPrefix(literal.GetSval())
		return !ok
	}
	return false
}

func preparedDecimalComparisonCast(expr *plan.Expr) (*plan.Function, bool) {
	if expr == nil || !types.T(expr.Typ.Id).IsDecimal() {
		return nil, false
	}
	cast := expr.GetF()
	if cast == nil || cast.Func == nil || cast.Func.GetObjName() != "cast" || len(cast.Args) != 2 {
		return nil, false
	}
	_, overload := function.DecodeOverloadID(cast.Func.GetObj())
	if overload != 0 || !isDirectDynamicParam(cast.Args[0]) || !isCharacterStringType(cast.Args[0].Typ.Id) {
		return nil, false
	}
	return cast, true
}

type findDecimalComparisonParamRule struct {
	found bool
}

func (rule *findDecimalComparisonParamRule) MatchNode(_ *Node) bool { return false }
func (rule *findDecimalComparisonParamRule) IsApplyExpr() bool      { return true }
func (rule *findDecimalComparisonParamRule) ApplyNode(_ *Node) error {
	return nil
}

func (rule *findDecimalComparisonParamRule) ApplyExpr(expr *plan.Expr) (*plan.Expr, error) {
	if rule.found || expr == nil {
		return expr, nil
	}
	if expr.ExactDecimalParam {
		rule.found = true
		return expr, nil
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_F:
		fn := impl.F
		if fn == nil {
			return expr, nil
		}
		if fn.Func != nil && isDecimalComparisonOperator(fn.Func.GetObjName()) {
			for _, arg := range fn.Args {
				if _, ok := preparedDecimalComparisonCast(arg); ok {
					rule.found = true
					return expr, nil
				}
			}
			if planExprHasParamRef(expr) && planExprHasDecimalType(expr) {
				rule.found = true
				return expr, nil
			}
		}
		for _, arg := range fn.Args {
			if _, err := rule.ApplyExpr(arg); err != nil {
				return nil, err
			}
			if rule.found {
				break
			}
		}
	case *plan.Expr_W:
		if _, err := applyWindowExpr(expr, rule.ApplyExpr); err != nil {
			return nil, err
		}
	case *plan.Expr_List:
		for _, item := range impl.List.List {
			if _, err := rule.ApplyExpr(item); err != nil {
				return nil, err
			}
			if rule.found {
				break
			}
		}
	}
	return expr, nil
}

func planExprHasParamRef(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.GetP() != nil {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if planExprHasParamRef(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if planExprHasParamRef(item) {
				return true
			}
		}
	}
	return false
}

func planExprHasDecimalType(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if types.T(expr.Typ.Id).IsDecimal() {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if planExprHasDecimalType(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if planExprHasDecimalType(item) {
				return true
			}
		}
	}
	return false
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
