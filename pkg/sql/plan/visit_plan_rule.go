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
	"errors"
	"sort"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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
	ctx               context.Context
	params            []*Expr
	preserveParamRefs bool
	exprMemo          map[*plan.Expr]*plan.Expr
}

type preparedTypeLineageRule struct {
	ctx   context.Context
	query *plan.Query
	types map[[2]int32]plan.Type
}

func (rule *preparedTypeLineageRule) MatchNode(*Node) bool  { return false }
func (rule *preparedTypeLineageRule) IsApplyExpr() bool     { return true }
func (rule *preparedTypeLineageRule) ApplyNode(*Node) error { return nil }
func (rule *preparedTypeLineageRule) ApplyExpr(e *plan.Expr) (*plan.Expr, error) {
	if e == nil {
		return nil, nil
	}
	switch impl := e.Expr.(type) {
	case *plan.Expr_Col:
		if typ, ok := rule.types[[2]int32{impl.Col.RelPos, impl.Col.ColPos}]; ok {
			e.Typ = typ
		}
		return e, nil
	case *plan.Expr_Sub:
		if rule.query == nil || impl.Sub == nil || impl.Sub.Typ != plan.SubqueryRef_SCALAR ||
			impl.Sub.NodeId < 0 || int(impl.Sub.NodeId) >= len(rule.query.Nodes) {
			return e, nil
		}
		root := rule.query.Nodes[impl.Sub.NodeId]
		if root != nil && impl.Sub.RowSize == 1 && len(root.ProjectList) == 1 {
			e.Typ = root.ProjectList[0].Typ
		}
		return e, nil
	case *plan.Expr_F:
		changed := false
		for i, arg := range impl.F.Args {
			old := arg.Typ
			rewritten, err := rule.ApplyExpr(arg)
			if err != nil {
				return nil, err
			}
			impl.F.Args[i] = rewritten
			changed = changed || old.Id != rewritten.Typ.Id || old.Width != rewritten.Typ.Width ||
				old.Scale != rewritten.Typ.Scale || old.NotNullable != rewritten.Typ.NotNullable
		}
		if !changed {
			return e, nil
		}
		// The prepare-time dynamic DECIMAL domain also coerces siblings that do
		// not themselves reference the producer. Restore those generated casts
		// before selecting the consumer overload for the new lineage generation.
		for i, arg := range impl.F.Args {
			restored, err := restorePreparedNumericLiteralType(rule.ctx, arg)
			if err != nil {
				return nil, err
			}
			impl.F.Args[i] = restored
		}
		rewritten, err := BindFuncExprImplByPlanExpr(rule.ctx, impl.F.Func.GetObjName(), impl.F.Args)
		if err != nil {
			return nil, err
		}
		if fn := rewritten.GetF(); fn != nil {
			fn.AggConfig = bytes.Clone(impl.F.AggConfig)
			fn.AggConfigType = impl.F.AggConfigType
		}
		return rewritten, nil
	case *plan.Expr_List:
		for i, item := range impl.List.List {
			rewritten, err := rule.ApplyExpr(item)
			if err != nil {
				return nil, err
			}
			impl.List.List[i] = rewritten
		}
	case *plan.Expr_W:
		return applyWindowExpr(e, rule.ApplyExpr)
	}
	return e, nil
}

func NewResetParamRefRule(ctx context.Context, params []*Expr) *ResetParamRefRule {
	return &ResetParamRefRule{
		ctx:    ctx,
		params: params,
	}
}

// NewSpecializeParamRefRule uses the current parameter values only to resolve
// runtime types. It deliberately keeps ParamRef expressions in the resulting
// plan so a type-specialized compile can be reused with later values of the
// same shape.
func NewSpecializeParamRefRule(ctx context.Context, params []*Expr) *ResetParamRefRule {
	return &ResetParamRefRule{
		ctx:               ctx,
		params:            params,
		preserveParamRefs: true,
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
	var err error
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_F:
		needResetFunction := false
		dynamicParamPos := int32(-1)
		var dynamicParamExpr *plan.Expr
		dynamicNumericArgs := make([]bool, len(exprImpl.F.Args))
		if isPreparedDynamicNumericCast(e) &&
			len(exprImpl.F.Args) > 0 && exprImpl.F.Args[0].GetP() != nil {
			dynamicParamPos = exprImpl.F.Args[0].GetP().Pos
			dynamicParamExpr = exprImpl.F.Args[0]
		}
		for i, arg := range exprImpl.F.Args {
			_, directParam := arg.Expr.(*plan.Expr_P)
			dynamicNumericParam := containsPreparedDynamicNumericParam(arg)
			dynamicNumericArgs[i] = dynamicNumericParam
			rewrittenArg, rewriteErr := rule.ApplyExpr(arg)
			if rewriteErr != nil {
				return nil, rewriteErr
			}
			// Any child replacement invalidates the parent's selected overload.
			// Generated exact-sibling casts do not contain ParamRefs themselves,
			// so restricting this to dynamicNumericParam leaves their parent tree
			// frozen at the prepare-time DECIMAL256 shape.
			if directParam || rewrittenArg != arg {
				needResetFunction = true
			}
			exprImpl.F.Args[i] = rewrittenArg
		}
		if needResetFunction {
			for i, arg := range exprImpl.F.Args {
				if !dynamicNumericArgs[i] {
					exprImpl.F.Args[i], err = restorePreparedNumericLiteralType(rule.ctx, arg)
					if err != nil {
						return nil, err
					}
				}
			}
		}
		if isPreparedDynamicNumericCast(e) {
			if dynamicParamPos >= 0 && int(dynamicParamPos) < len(rule.params) {
				param := rule.params[dynamicParamPos]
				dynamicParamExpr.Typ.Table = ""
				dynamicParamExpr.Typ.NotNullable = param.Typ.NotNullable
				value := param.GetLit().GetSval()
				runtimeType := types.T(param.Typ.Id)
				switch runtimeType {
				case types.T_bool:
					// Keep the parameter value and source type intact at the
					// frontend boundary. Numeric consumers alone interpret BOOL
					// as MySQL integer 0/1 through their generated cast.
					intType := types.T_int64.ToType()
					return makePlan2CastExpr(rule.ctx, dynamicParamExpr,
						makePlan2Type(&intType))
				case types.T_float32:
					parsed, parseErr := parsePreparedFloat(value, 32)
					if parseErr != nil {
						return nil, parseErr
					}
					return makePlan2CastExpr(rule.ctx, dynamicParamExpr,
						MakePlan2Float32ConstExprWithType(float32(parsed)).Typ)
				case types.T_float64:
					parsed, parseErr := parsePreparedFloat(value, 64)
					if parseErr != nil {
						return nil, parseErr
					}
					return makePlan2CastExpr(rule.ctx, dynamicParamExpr,
						MakePlan2Float64ConstExprWithType(parsed).Typ)
				}
				if runtimeType.IsMySQLString() {
					parsed, parseErr := planfunction.ParseStringToFloatForNumericExpression(value)
					if parseErr != nil {
						return nil, parseErr
					}
					return makePlan2CastExpr(rule.ctx, dynamicParamExpr,
						MakePlan2Float64ConstExprWithType(parsed).Typ)
				}
				if runtimeType.IsInteger() || runtimeType == types.T_bit {
					typed, makeErr := makePreparedIntegerExpr(value, runtimeType)
					if makeErr != nil {
						return nil, makeErr
					}
					return makePlan2CastExpr(rule.ctx, dynamicParamExpr, typed.Typ)
				}
				if runtimeType.IsDecimal() {
					typed, makeErr := makePlan2DecimalExprWithType(rule.ctx, value, param.GetLit().GetIsBin())
					if makeErr != nil {
						return nil, makeErr
					}
					return makePlan2CastExpr(rule.ctx, dynamicParamExpr, typed.Typ)
				}
			}
			if dynamicParamPos < 0 {
				// The prepare-time dynamic DECIMAL domain also coerces the
				// parameter's sibling operands. Restore those operands before
				// rebinding the enclosing function for the current runtime type.
				return restorePreparedNumericLiteralType(rule.ctx, e)
			}
			value := exprImpl.F.Args[0]
			if literal := value.GetLit(); literal != nil {
				var decimal string
				switch literalValue := literal.Value.(type) {
				case *plan.Literal_Sval:
					decimal = literalValue.Sval
				case *plan.Literal_I64Val:
					decimal = strconv.FormatInt(literalValue.I64Val, 10)
				case *plan.Literal_U64Val:
					decimal = strconv.FormatUint(literalValue.U64Val, 10)
				}
				if decimal != "" {
					typed, makeErr := makePlan2DecimalExprWithType(rule.ctx, decimal, literal.GetIsBin())
					if makeErr != nil {
						return nil, makeErr
					}
					return makePlan2CastExpr(rule.ctx, dynamicParamExpr, typed.Typ)
				}
			}
			return value, nil
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
			return rewritten, nil
		}
		return e, nil
	case *plan.Expr_W:
		return applyWindowExpr(e, rule.ApplyExpr)
	case *plan.Expr_P:
		if int(exprImpl.P.Pos) >= len(rule.params) {
			return nil, moerr.NewInternalErrorf(context.TODO(), "get prepare params error, index %d not exists", int(exprImpl.P.Pos))
		}
		if rule.preserveParamRefs {
			return e, nil
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

func restorePreparedNumericLiteralType(ctx context.Context, expr *plan.Expr) (*plan.Expr, error) {
	restored, _, err := restorePreparedNumericExactType(ctx, expr)
	return restored, err
}

func restorePreparedNumericExactType(ctx context.Context, expr *plan.Expr) (*plan.Expr, bool, error) {
	if expr == nil {
		return expr, false, nil
	}
	if fn := expr.GetF(); fn != nil && fn.Func.GetObjName() == "cast" && len(fn.Args) > 0 &&
		(isPreparedDynamicNumericCast(expr) ||
			(types.T(expr.Typ.Id).IsDecimal() && expr.Typ.Width > 65)) {
		_, overload := planfunction.DecodeOverloadID(fn.Func.GetObj())
		if overload == 0 {
			restored, _, err := restorePreparedNumericExactType(ctx, fn.Args[0])
			if err != nil {
				return nil, false, err
			}
			if lit := restored.GetLit(); lit != nil {
				if value, ok := lit.Value.(*plan.Literal_Sval); ok {
					if decimal, err := makePlan2DecimalExprWithType(
						ctx, value.Sval, lit.GetIsBin()); err == nil {
						return decimal, true, nil
					}
				}
			}
			return restored, true, nil
		}
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func.GetObjName() == "cast" {
			// An unmarked CAST is explicit SQL and forms a type boundary. Its
			// declared target must not be inferred again from the surrounding
			// prepared arithmetic domain.
			return expr, false, nil
		}
		changed := isPreparedDynamicNumericType(expr.Typ) ||
			(types.T(expr.Typ.Id).IsDecimal() && expr.Typ.Width > 65)
		args := make([]*plan.Expr, len(fn.Args))
		for i, arg := range fn.Args {
			restored, childChanged, err := restorePreparedNumericExactType(ctx, arg)
			if err != nil {
				return nil, false, err
			}
			args[i] = restored
			changed = changed || childChanged
		}
		if !changed {
			return expr, false, nil
		}
		name := fn.Func.GetObjName()
		if planfunction.GetFunctionIsAggregateByName(name) || planfunction.GetFunctionIsWinFunByName(name) {
			return expr, false, nil
		}
		// Exact siblings can be arbitrary function trees (for example ABS(1),
		// MOD(3, 2), COALESCE(1, 0), or (-1 + 0)). Rebind every parent bottom-up:
		// the explicit changed bit propagates a removed generated cast through the
		// tree without rebinding unrelated aggregate, window, or CAST nodes.
		rebound, err := BindFuncExprImplByPlanExpr(ctx, name, args)
		if err != nil {
			return nil, false, err
		}
		if reboundFn := rebound.GetF(); reboundFn != nil {
			reboundFn.AggConfig = bytes.Clone(fn.AggConfig)
			reboundFn.AggConfigType = fn.AggConfigType
		}
		return rebound, true, nil
	}
	if !isPreparedDynamicNumericType(expr.Typ) {
		return expr, false, nil
	}
	literal := expr.GetLit()
	if literal == nil {
		return expr, false, nil
	}
	switch value := literal.Value.(type) {
	case *plan.Literal_I64Val:
		return makePlan2Int64ConstExprWithType(value.I64Val), true, nil
	case *plan.Literal_U64Val:
		return makePlan2Uint64ConstExprWithType(value.U64Val), true, nil
	default:
		return expr, false, nil
	}
}

func parsePreparedFloat(value string, bitSize int) (float64, error) {
	parsed, err := strconv.ParseFloat(value, bitSize)
	if err == nil || (errors.Is(err, strconv.ErrRange) && parsed == 0) {
		return parsed, nil
	}
	return 0, err
}

func makePreparedIntegerExpr(value string, typ types.T) (*plan.Expr, error) {
	switch typ {
	case types.T_int8:
		parsed, err := strconv.ParseInt(value, 10, 8)
		return MakePlan2Int8ConstExprWithType(int8(parsed)), err
	case types.T_int16:
		parsed, err := strconv.ParseInt(value, 10, 16)
		return MakePlan2Int16ConstExprWithType(int16(parsed)), err
	case types.T_int32:
		parsed, err := strconv.ParseInt(value, 10, 32)
		return MakePlan2Int32ConstExprWithType(int32(parsed)), err
	case types.T_int64:
		parsed, err := strconv.ParseInt(value, 10, 64)
		return MakePlan2Int64ConstExprWithType(parsed), err
	case types.T_uint8:
		parsed, err := strconv.ParseUint(value, 10, 8)
		return MakePlan2Uint8ConstExprWithType(uint8(parsed)), err
	case types.T_uint16:
		parsed, err := strconv.ParseUint(value, 10, 16)
		return MakePlan2Uint16ConstExprWithType(uint16(parsed)), err
	case types.T_uint32:
		parsed, err := strconv.ParseUint(value, 10, 32)
		return MakePlan2Uint32ConstExprWithType(uint32(parsed)), err
	case types.T_uint64, types.T_bit:
		parsed, err := strconv.ParseUint(value, 10, 64)
		return MakePlan2Uint64ConstExprWithType(parsed), err
	default:
		return nil, moerr.NewInternalErrorNoCtxf("unsupported prepared integer type %s", typ)
	}
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
		e.Typ = w.WindowFunc.Typ
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
