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
	"reflect"
	"sort"
	"strings"

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
	exprMemo             map[*plan.Expr]*plan.Expr
	validateFunctionArgs func(string, []*Expr) error
	// specialized is set only when execute-time rebinding changes a function
	// overload/result type. Literal replacement alone is not enough to require
	// rebuilding a cached prepared compile.
	specialized bool
	// inferTextParamPositions records only the COM_STMT text parameters that may
	// carry numeric payloads.  Keep this per parameter: enabling inference for
	// every text marker in a mixed statement would reinterpret an ordinary
	// string predicate merely because another marker came from COM_STMT.
	inferTextParamPositions map[int]bool
	// inferTextParamTypes retains the explicit TEXT runtime-type compatibility
	// path used by FillValuesOfParamsInPlan callers.  COM_STMT values use the
	// per-position map above instead of this broad fallback.
	inferTextParamTypes bool
}

func NewResetParamRefRule(ctx context.Context, params []*Expr) *ResetParamRefRule {
	return &ResetParamRefRule{
		ctx:    ctx,
		params: params,
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
		if rule.validateFunctionArgs != nil {
			if err := rule.validateFunctionArgs(exprImpl.F.Func.GetObjName(), exprImpl.F.Args); err != nil {
				return nil, err
			}
		}
		originalTyp := e.Typ
		originalFuncObj := int64(0)
		originalArgTypes := make([]plan.Type, len(exprImpl.F.Args))
		if exprImpl.F.Func != nil {
			originalFuncObj = exprImpl.F.Func.Obj
		}
		for i, arg := range exprImpl.F.Args {
			if arg != nil {
				originalArgTypes[i] = arg.Typ
			}
		}
		needResetFunction := false
		compareArgTypes := false
		boundArgs := make([]*plan.Expr, len(exprImpl.F.Args))
		binaryTextParamArgs := make(map[int]struct{})
		functionName := ""
		if exprImpl.F.Func != nil {
			functionName = strings.ToLower(exprImpl.F.Func.GetObjName())
		}
		for i, arg := range exprImpl.F.Args {
			originalArgTyp := plan.Type{}
			originalArgFuncObj := int64(0)
			if arg != nil {
				originalArgTyp = arg.Typ
				originalArgFuncObj = preparedExprFunctionObj(arg)
			}
			implicitParamCast := isImplicitPreparedParamCast(arg)
			paramPos, hasParamPos := implicitPreparedParamPosition(arg)
			if _, ok := arg.Expr.(*plan.Expr_P); ok && exprImpl.F.Func.GetObjName() != "cast" {
				needResetFunction = true
				compareArgTypes = true
			}
			if implicitParamCast {
				// The prepare-time TEXT marker may have been wrapped in an
				// implicit numeric cast selected by overload resolution.  The
				// cast is provisional; the execute-time value must participate in
				// resolving the outer function again.
				needResetFunction = true
			}
			rewrittenArg, applyErr := rule.ApplyExpr(arg)
			err = applyErr
			if err != nil {
				return nil, err
			}
			exprImpl.F.Args[i] = rewrittenArg
			boundArgs[i] = rewrittenArg
			if preparedExprBindingChanged(originalArgTyp, originalArgFuncObj, rewrittenArg) {
				// A nested typed function may have changed overload/result domain
				// after its parameter was rebound.  The enclosing function was
				// bound against the old child domain and must be resolved again.
				needResetFunction = true
				compareArgTypes = true
			}
			if implicitParamCast {
				// Keep decimal casts: decimal arithmetic requires every operand to
				// be materialized as a decimal vector, even when the protocol value
				// was encoded as an integer. For casts to other numeric domains, use
				// the execute-time type so functions such as ABS can specialize a
				// decimal parameter instead of retaining a prepare-time BIGINT cast.
				inferText := rule.inferTextParamTypes ||
					(hasParamPos && rule.inferTextParamPositions[paramPos])
				if unwrapped, ok := unwrapImplicitPreparedParamCast(rule.ctx, rewrittenArg, inferText); ok {
					boundArgs[i] = unwrapped
					compareArgTypes = true
				}
			}
			if arg.GetP() != nil && arg.GetP().Pos >= 0 &&
				rule.inferTextParamPositions[int(arg.GetP().Pos)] &&
				rewrittenArg.Typ.Id == int32(types.T_text) {
				binaryTextParamArgs[i] = struct{}{}
			}
		}
		// A comparison with at least one numeric binary parameter follows MySQL's
		// numeric coercion rules even when its other parameter was sent as
		// VAR_STRING.  The prepare-time binder sees TEXT/TEXT and would otherwise
		// insert an INT cast around values such as "1.00", which fails at execute
		// time.  Infer only these per-position COM_STMT text arguments; direct text
		// projections and ordinary string predicates keep their TEXT domain.
		if isPreparedNumericComparison(functionName) && len(binaryTextParamArgs) > 0 {
			hasNumericArg := false
			for i, arg := range boundArgs {
				if _, ok := binaryTextParamArgs[i]; ok {
					continue
				}
				if arg != nil && types.T(arg.Typ.Id).ToType().IsNumeric() {
					hasNumericArg = true
					break
				}
			}
			if hasNumericArg {
				for i := range binaryTextParamArgs {
					literal := boundArgs[i].GetLit()
					if literal == nil {
						continue
					}
					runtimeType, ok := PreparedRuntimeTypeFromString(literal.GetSval())
					if !ok || !runtimeType.IsNumeric() {
						continue
					}
					inferred, inferErr := preparedRuntimeParamExpr(rule.ctx, literal.GetSval(), literal.IsBin, runtimeType)
					if inferErr != nil {
						continue
					}
					boundArgs[i] = inferred
					needResetFunction = true
					compareArgTypes = true
				}
			}
		}

		// reset function
		if needResetFunction {
			rewritten, err := BindFuncExprImplByPlanExpr(
				rule.ctx,
				exprImpl.F.Func.GetObjName(),
				boundArgs,
			)
			if err != nil {
				return nil, err
			}
			rewrittenFn := rewritten.GetF()
			if rewrittenFn != nil {
				rewrittenFn.AggConfig = bytes.Clone(exprImpl.F.AggConfig)
				rewrittenFn.AggConfigType = exprImpl.F.AggConfigType
			}
			if functionBindingChanged(originalTyp, originalFuncObj, originalArgTypes, rewritten, compareArgTypes) {
				rule.specialized = true
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
		param := rule.params[int(exprImpl.P.Pos)]
		typ := e.Typ
		// Most prepared parameters are intentionally replaced as TEXT to retain
		// the historical SQL-EXECUTE behavior.  Binary protocol executions can
		// carry an explicit numeric domain, represented by a non-text type on the
		// replacement expression; preserve that domain for direct projections and
		// for the function rebinding performed by the parent expression.
		if param != nil && param.Typ.Id != int32(types.T_text) {
			typ = param.Typ
		}
		return &plan.Expr{
			Typ:  typ,
			Expr: param.Expr,
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

func preparedExprFunctionObj(expr *plan.Expr) int64 {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return 0
	}
	return fn.Func.Obj
}

func preparedExprBindingChanged(originalTyp plan.Type, originalFuncObj int64, rewritten *plan.Expr) bool {
	if rewritten == nil || !reflect.DeepEqual(rewritten.Typ, originalTyp) {
		return true
	}
	return preparedExprFunctionObj(rewritten) != originalFuncObj
}

func isPreparedNumericComparison(name string) bool {
	switch name {
	case "=", "<=>", "!=", "<>", "<", "<=", ">", ">=":
		return true
	default:
		return false
	}
}

func functionBindingChanged(
	originalTyp plan.Type,
	originalFuncObj int64,
	originalArgTypes []plan.Type,
	rewritten *plan.Expr,
	compareArgTypes bool,
) bool {
	if rewritten == nil || rewritten.GetF() == nil {
		return true
	}
	if !reflect.DeepEqual(rewritten.Typ, originalTyp) || rewritten.GetF().Func == nil || rewritten.GetF().Func.Obj != originalFuncObj {
		return true
	}
	if !compareArgTypes {
		return false
	}
	if len(rewritten.GetF().Args) != len(originalArgTypes) {
		return true
	}
	for i, arg := range rewritten.GetF().Args {
		if arg == nil || !reflect.DeepEqual(arg.Typ, originalArgTypes[i]) {
			return true
		}
	}
	return false
}

// isImplicitPreparedParamCast identifies the cast inserted by overload
// resolution around a parameter marker. Explicit CAST(? AS ...) uses a
// separate cast overload and must remain authoritative.
func isImplicitPreparedParamCast(expr *plan.Expr) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.GetObjName() != "cast" || len(fn.Args) == 0 || fn.Args[0].GetP() == nil {
		return false
	}
	_, overload := planfunction.DecodeOverloadID(fn.Func.GetObj())
	return overload == 0
}

func implicitPreparedParamPosition(expr *plan.Expr) (int, bool) {
	fn := expr.GetF()
	if fn == nil || len(fn.Args) == 0 {
		return 0, false
	}
	param := fn.Args[0].GetP()
	if param == nil || param.Pos < 0 {
		return 0, false
	}
	return int(param.Pos), true
}

// unwrapImplicitPreparedParamCast strips a provisional overload cast only when
// the execute-time value has a numeric type that can safely drive rebinding.
// Decimal and YEAR casts are retained because their executors require the
// target physical representation for arithmetic and index serialization.
func unwrapImplicitPreparedParamCast(ctx context.Context, rewritten *plan.Expr, inferText bool) (*plan.Expr, bool) {
	fn := rewritten.GetF()
	if fn == nil || len(fn.Args) == 0 {
		return nil, false
	}
	arg := fn.Args[0]
	if arg.Typ.Id == int32(types.T_text) {
		if !inferText {
			return nil, false
		}
		literal := arg.GetLit()
		if literal == nil {
			return nil, false
		}
		typ, ok := PreparedRuntimeTypeFromString(literal.GetSval())
		if !ok {
			return nil, false
		}
		bound, err := preparedRuntimeParamExpr(ctx, literal.GetSval(), literal.IsBin, typ)
		if err != nil {
			return nil, false
		}
		arg = bound
	}
	argType := types.New(types.T(arg.Typ.Id), arg.Typ.Width, arg.Typ.Scale)
	if !argType.IsNumeric() {
		return nil, false
	}
	targetType := types.New(types.T(rewritten.Typ.Id), rewritten.Typ.Width, rewritten.Typ.Scale)
	if !targetType.IsNumeric() {
		return nil, false
	}
	if targetType.IsDecimal() || targetType.Oid == types.T_year {
		return nil, false
	}
	return arg, true
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
