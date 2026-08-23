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
	"strconv"
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
	// numericPrefixParamPositions is populated only after the deployment-wide
	// protocol reaches the version that understands Charset=255 numeric-prefix
	// casts. The map remains per-position to keep unrelated text parameters in
	// their ordinary string domains.
	numericPrefixParamPositions map[int]bool
	numericPrefixParamKinds     map[int]types.StringConversionKind
	// numericPrefixDependent records rewritten expressions whose value domain
	// was selected from an execute-time numeric-prefix parameter. The dependency
	// propagates through binder-inserted casts so enclosing consumers can remove
	// provisional prepare-time coercions and bind against the runtime domain.
	numericPrefixDependent      map[*plan.Expr]bool
	serializedDecimalParamTypes map[*plan.Expr]types.Type
}

// PreparedPlanNeedsNumericPrefixSpecialization reports whether a prepared plan
// contains an eligible parameter in a decimal-aware common-type context. It is
// a read-only O(plan expressions) eligibility check used before DeepCopyPlan on
// SQL EXECUTE; COM_STMT keeps its broader runtime-type specialization path.
func PreparedPlanNeedsNumericPrefixSpecialization(preparePlan *Plan, paramVals []any) bool {
	if preparePlan == nil || len(paramVals) == 0 {
		return false
	}
	positions := make(map[int]types.StringConversionKind)
	for i, value := range paramVals {
		param, ok := value.(ParamValue)
		if !ok || !param.EnableNumericPrefix {
			continue
		}
		positions[i] = param.PrepareParamKind
	}
	if len(positions) == 0 {
		return false
	}

	required := false
	_ = plan.VisitExpressionsInOwner(preparePlan, func(expr *plan.Expr) error {
		if !required {
			required = preparedExprNeedsNumericPrefixSpecialization(expr, positions)
		}
		return nil
	})
	return required
}

func preparedExprNeedsNumericPrefixSpecialization(
	expr *plan.Expr,
	positions map[int]types.StringConversionKind,
) bool {
	if expr == nil {
		return false
	}
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		if isPreparedPrefixFilter(exprImpl.F.Func.GetObjName()) &&
			preparedExprHasRuntimeDecimalParam(expr, positions) {
			// Secondary-index planning exposes only its serialized prefix predicate
			// to this owner scan. Admit the plan so the runtime DECIMAL parameter can
			// be materialized in the prefix key's prepared target type.
			return true
		}
		// SQL EXECUTE admits either a static DECIMAL peer or a runtime DECIMAL
		// parameter paired with an exact numeric operand. Keep approximate FLOAT
		// operands outside this path. COM_STMT bypasses this eligibility scan and
		// still uses runtime kinds while performing the actual rewrite.
		if preparedNumericPrefixPositionContext(
			exprImpl.F.Func.GetObjName(), exprImpl.F.Args, positions) {
			return true
		}
		for _, arg := range exprImpl.F.Args {
			if preparedExprNeedsNumericPrefixSpecialization(arg, positions) {
				return true
			}
		}
	case *plan.Expr_List:
		for _, item := range exprImpl.List.List {
			if preparedExprNeedsNumericPrefixSpecialization(item, positions) {
				return true
			}
		}
	case *plan.Expr_Lit:
		return preparedExprNeedsNumericPrefixSpecialization(exprImpl.Lit.Src, positions)
	case *plan.Expr_Sub:
		return preparedExprNeedsNumericPrefixSpecialization(exprImpl.Sub.Child, positions)
	case *plan.Expr_W:
		window := exprImpl.W
		if preparedExprNeedsNumericPrefixSpecialization(window.WindowFunc, positions) {
			return true
		}
		for _, item := range window.PartitionBy {
			if preparedExprNeedsNumericPrefixSpecialization(item, positions) {
				return true
			}
		}
		for _, order := range window.OrderBy {
			if order != nil && preparedExprNeedsNumericPrefixSpecialization(order.Expr, positions) {
				return true
			}
		}
		if window.Frame != nil {
			if window.Frame.Start != nil && preparedExprNeedsNumericPrefixSpecialization(
				window.Frame.Start.Val, positions) {
				return true
			}
			if window.Frame.End != nil && preparedExprNeedsNumericPrefixSpecialization(
				window.Frame.End.Val, positions) {
				return true
			}
		}
	}
	return false
}

func (rule *ResetParamRefRule) markSerializedDecimalParamTypes(expr *plan.Expr) {
	if expr == nil {
		return
	}
	if isImplicitPreparedParamCast(expr) {
		fn := expr.GetF()
		if len(fn.Args) > 0 {
			paramExpr := fn.Args[0]
			if param := paramExpr.GetP(); param != nil && param.Pos >= 0 {
				pos := int(param.Pos)
				if rule.numericPrefixParamKinds[pos] == types.StringConversionDecimal {
					if rule.serializedDecimalParamTypes == nil {
						rule.serializedDecimalParamTypes = make(map[*plan.Expr]types.Type)
					}
					rule.serializedDecimalParamTypes[paramExpr] = makeTypeByPlan2Expr(expr)
				}
			}
		}
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			rule.markSerializedDecimalParamTypes(arg)
		}
	}
}

func exactIntegerDecimalText(value string) (string, bool) {
	integer, fraction, found := strings.Cut(strings.TrimSpace(value), ".")
	if !found {
		return integer, integer != ""
	}
	if integer == "" || strings.Trim(fraction, "0") != "" {
		return "", false
	}
	return integer, true
}

func hasRuntimeDecimalPrefixFilter(preparePlan *Plan, paramVals []any) bool {
	query := preparePlan.GetQuery()
	if query == nil {
		return false
	}
	positions := make(map[int]types.StringConversionKind)
	for i, value := range paramVals {
		param, ok := value.(ParamValue)
		if ok && param.EnableNumericPrefix && param.PrepareParamKind == types.StringConversionDecimal {
			positions[i] = param.PrepareParamKind
		}
	}
	if len(positions) == 0 {
		return false
	}
	for _, node := range query.Nodes {
		if node == nil {
			continue
		}
		for _, filters := range [][]*plan.Expr{node.FilterList, node.BlockFilterList} {
			for _, filter := range filters {
				fn := filter.GetF()
				if fn != nil && isPreparedPrefixFilter(fn.Func.GetObjName()) &&
					preparedExprHasRuntimeDecimalParam(filter, positions) {
					return true
				}
			}
		}
	}
	return false
}

func isPreparedPrefixFilter(name string) bool {
	switch name {
	case "prefix_eq", "prefix_in", "prefix_between", "prefix_in_range":
		return true
	default:
		return false
	}
}

func preparedExprHasRuntimeDecimalParam(
	expr *plan.Expr,
	positions map[int]types.StringConversionKind,
) bool {
	if pos, ok := preparedParamPosition(expr); ok && positions[pos] == types.StringConversionDecimal {
		return true
	}
	fn := expr.GetF()
	if fn != nil {
		for _, arg := range fn.Args {
			if preparedExprHasRuntimeDecimalParam(arg, positions) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if preparedExprHasRuntimeDecimalParam(item, positions) {
				return true
			}
		}
	}
	return false
}

func preparedNumericPrefixPositionContext(
	name string,
	args []*plan.Expr,
	positions map[int]types.StringConversionKind,
) bool {
	switch name {
	case "coalesce", "greatest", "least", "=", "<=>", "!=", "<>", "<", "<=", ">", ">=", "between", "in_range", "in", "not_in":
	default:
		return false
	}

	hasEligibleParam := false
	hasRuntimeDecimal := false
	hasDecimalPeer := false
	hasFloatPeer := false
	hasCommonValueBoundary := false
	numericArgCount := len(args)
	if name == "in_range" && numericArgCount > 3 {
		numericArgCount = 3
	}
	for i, arg := range args {
		if i >= numericArgCount || arg == nil {
			continue
		}
		pos, directEligible := preparedParamPosition(arg)
		kind, eligiblePosition := positions[pos]
		directEligible = directEligible && eligiblePosition
		source := unwrapPreparedImplicitCast(arg, directEligible)
		if directEligible {
			hasEligibleParam = true
			hasRuntimeDecimal = hasRuntimeDecimal || kind == types.StringConversionDecimal
			continue
		}
		if source == nil {
			continue
		}
		if list := source.GetList(); list != nil {
			for _, item := range list.List {
				itemPos, itemEligible := preparedParamPosition(item)
				kind, eligiblePosition := positions[itemPos]
				itemEligible = itemEligible && eligiblePosition
				item = unwrapPreparedImplicitCast(item, itemEligible)
				if itemEligible {
					hasEligibleParam = true
					hasRuntimeDecimal = hasRuntimeDecimal || kind == types.StringConversionDecimal
				} else if item != nil {
					hasDecimalPeer = hasDecimalPeer || types.T(item.Typ.Id).IsDecimal()
					hasFloatPeer = hasFloatPeer || preparedStaticFloatOperand(item)
				}
			}
			continue
		}
		if types.T(source.Typ.Id).IsDecimal() {
			hasDecimalPeer = true
		}
		if preparedStaticFloatOperand(source) {
			hasFloatPeer = true
		}
		if isPreparedCommonValueFunction(name) &&
			!preparedNumericCommonOperandType(types.T(source.Typ.Id)) {
			hasCommonValueBoundary = true
		}
	}
	return hasEligibleParam && (hasDecimalPeer || hasRuntimeDecimal) && !hasFloatPeer && !hasCommonValueBoundary
}

func preparedStaticFloatOperand(expr *plan.Expr) bool {
	if expr == nil || !types.T(expr.Typ.Id).IsFloat() {
		return false
	}
	// Function result types can be provisional products of prepare-time TEXT
	// binding. Only leaf FLOAT values establish an approximate-domain boundary.
	return expr.GetCol() != nil || expr.GetLit() != nil
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

func (rule *ResetParamRefRule) markNumericPrefixDependent(exprs ...*plan.Expr) {
	if rule.numericPrefixDependent == nil {
		rule.numericPrefixDependent = make(map[*plan.Expr]bool)
	}
	for _, expr := range exprs {
		if expr != nil {
			rule.numericPrefixDependent[expr] = true
		}
	}
}

func (rule *ResetParamRefRule) isNumericPrefixDependent(expr *plan.Expr) bool {
	return expr != nil && rule.numericPrefixDependent[expr]
}

func (rule *ResetParamRefRule) applyExpr(e *plan.Expr) (*plan.Expr, error) {
	var err error
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_F:
		if isPreparedPrefixFilter(exprImpl.F.Func.GetObjName()) {
			rule.markSerializedDecimalParamTypes(e)
		}
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
		numericPrefixDependent := false
		boundArgs := make([]*plan.Expr, len(exprImpl.F.Args))
		numericPrefixArgs := make([]bool, len(exprImpl.F.Args))
		numericPrefixKinds := make([]types.StringConversionKind, len(exprImpl.F.Args))
		numericPrefixListArgs := make([][]bool, len(exprImpl.F.Args))
		numericPrefixListKinds := make([][]types.StringConversionKind, len(exprImpl.F.Args))
		for i, arg := range exprImpl.F.Args {
			originalArgTyp := plan.Type{}
			originalArgFuncObj := int64(0)
			if arg != nil {
				originalArgTyp = arg.Typ
				originalArgFuncObj = preparedExprFunctionObj(arg)
			}
			implicitParamCast := isImplicitPreparedParamCast(arg)
			paramPos, hasParamPos := 0, false
			if implicitParamCast {
				paramPos, hasParamPos = implicitPreparedParamPosition(arg)
			}
			if directParam := arg.GetP(); directParam != nil {
				paramPos, hasParamPos = int(directParam.Pos), directParam.Pos >= 0
			}
			if hasParamPos && rule.numericPrefixParamPositions[paramPos] {
				numericPrefixArgs[i] = true
				numericPrefixKinds[i] = rule.numericPrefixParamKinds[paramPos]
			}
			if list := arg.GetList(); list != nil {
				numericPrefixListArgs[i] = make([]bool, len(list.List))
				numericPrefixListKinds[i] = make([]types.StringConversionKind, len(list.List))
				for itemIndex, item := range list.List {
					if itemPos, ok := preparedParamPosition(item); ok && rule.numericPrefixParamPositions[itemPos] {
						numericPrefixListArgs[i][itemIndex] = true
						numericPrefixListKinds[i][itemIndex] = rule.numericPrefixParamKinds[itemPos]
					}
				}
			}
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
			if rule.isNumericPrefixDependent(rewrittenArg) {
				numericPrefixDependent = true
				if unwrapped, changed := unwrapNumericPrefixDependentImplicitCast(rewrittenArg); changed {
					boundArgs[i] = unwrapped
					needResetFunction = true
					compareArgTypes = true
				}
			}
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
		}
		if numericPrefixDependent {
			for i, arg := range boundArgs {
				// A nested runtime common-type result invalidates provisional
				// prepare-time casts on every sibling, not only on the dependent
				// child. Rebind the enclosing consumer from numeric source domains
				// so a DECIMAL peer is not left behind a FLOAT cast selected while
				// the parameter marker was still TEXT.
				unwrapped, changed := unwrapNumericPrefixDependentImplicitCast(arg)
				if changed {
					boundArgs[i] = unwrapped
					needResetFunction = true
					compareArgTypes = true
				}
			}
		}
		if contextualArgs, changed, contextualErr := rule.preparedNumericPrefixArgs(
			exprImpl.F.Func.GetObjName(), boundArgs,
			numericPrefixArgs, numericPrefixKinds, numericPrefixListArgs, numericPrefixListKinds,
		); contextualErr != nil {
			return nil, contextualErr
		} else if changed {
			boundArgs = contextualArgs
			needResetFunction = true
			compareArgTypes = true
			numericPrefixDependent = true
			// A comparison can retain the same overload and outer argument
			// types while its parameter is replaced by a numeric-prefix cast.
			// The execution must still use this rewritten plan copy instead of
			// falling back to the cached prepare-time template.
			rule.specialized = true
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
			if numericPrefixDependent && !isExplicitPreparedCast(e) {
				rule.markNumericPrefixDependent(e, rewritten)
			}
			return rewritten, nil
		}
		if numericPrefixDependent && !isExplicitPreparedCast(e) {
			rule.markNumericPrefixDependent(e)
		}
		return e, nil
	case *plan.Expr_W:
		rewritten, err := applyWindowExpr(e, rule.ApplyExpr)
		if err == nil && windowHasNumericPrefixDependency(rewritten.GetW(), rule.isNumericPrefixDependent) {
			rule.markNumericPrefixDependent(e, rewritten)
		}
		return rewritten, err
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
		rewritten := &plan.Expr{
			Typ:  typ,
			Expr: param.Expr,
		}
		if target, ok := rule.serializedDecimalParamTypes[e]; ok {
			if integerText, exact := exactIntegerDecimalText(rewritten.GetLit().GetSval()); exact {
				return preparedRuntimeParamExpr(rule.ctx, integerText, false, target)
			}
		}
		return rewritten, nil
	case *plan.Expr_List:
		dependent := false
		for i, arg := range exprImpl.List.List {
			exprImpl.List.List[i], err = rule.ApplyExpr(arg)
			if err != nil {
				return nil, err
			}
			dependent = dependent || rule.isNumericPrefixDependent(exprImpl.List.List[i])
		}
		if dependent {
			rule.markNumericPrefixDependent(e)
		}
		return e, nil
	default:
		return e, nil
	}
}

func (rule *ResetParamRefRule) preparedNumericPrefixArgs(
	name string,
	args []*plan.Expr,
	prefixArgs []bool,
	prefixKinds []types.StringConversionKind,
	prefixListArgs [][]bool,
	prefixListKinds [][]types.StringConversionKind,
) ([]*plan.Expr, bool, error) {
	if !preparedNumericPrefixContext(
		name, args, prefixArgs, prefixKinds, prefixListArgs, prefixListKinds,
	) {
		return args, false, nil
	}

	numericArgCount := len(args)
	if name == "in_range" && numericArgCount > 3 {
		numericArgCount = 3
	}
	sources := make([]*plan.Expr, len(args))
	for i, arg := range args {
		eligibleArg := i < len(prefixArgs) && prefixArgs[i]
		sources[i] = unwrapPreparedImplicitCast(arg, eligibleArg)
	}

	changed := false
	for i := 0; i < numericArgCount; i++ {
		if sources[i] == nil {
			continue
		}
		if list := sources[i].GetList(); list != nil {
			for itemIndex, item := range list.List {
				originalItem := item
				eligible := i < len(prefixListArgs) && itemIndex < len(prefixListArgs[i]) &&
					prefixListArgs[i][itemIndex]
				item = unwrapPreparedImplicitCast(item, eligible)
				if !eligible {
					list.List[itemIndex] = item
					changed = changed || item != originalItem
					continue
				}
				kind := types.StringConversionString
				if i < len(prefixListKinds) && itemIndex < len(prefixListKinds[i]) {
					kind = prefixListKinds[i][itemIndex]
				}
				cast, castChanged, err := rule.preparedNumericPrefixCast(item, kind)
				if err != nil {
					return nil, false, err
				}
				list.List[itemIndex] = cast
				changed = changed || castChanged || item != originalItem
			}
			continue
		}
		if i >= len(prefixArgs) || !prefixArgs[i] || sources[i] == nil ||
			!types.T(sources[i].Typ.Id).IsMySQLString() {
			continue
		}
		kind := types.StringConversionString
		if i < len(prefixKinds) {
			kind = prefixKinds[i]
		}
		cast, castChanged, err := rule.preparedNumericPrefixCast(sources[i], kind)
		if err != nil {
			return nil, false, err
		}
		sources[i] = cast
		changed = changed || castChanged
	}
	normalized, commonTypeChanged, err := rule.normalizePreparedNumericCommonArgs(name, sources)
	if err != nil {
		return nil, false, err
	}
	sources = normalized
	changed = changed || commonTypeChanged
	if !changed {
		return args, false, nil
	}
	return sources, true, nil
}

func preparedNumericPrefixContext(
	name string,
	args []*plan.Expr,
	prefixArgs []bool,
	prefixKinds []types.StringConversionKind,
	prefixListArgs [][]bool,
	prefixListKinds [][]types.StringConversionKind,
) bool {
	switch name {
	case "coalesce", "greatest", "least", "=", "<=>", "!=", "<>", "<", "<=", ">", ">=", "between", "in_range", "in", "not_in":
	default:
		return false
	}

	sources := make([]*plan.Expr, len(args))
	hasEligibleParam := false
	hasDecimalPeer := false
	hasCommonValueBoundary := false
	numericArgCount := len(args)
	if name == "in_range" && numericArgCount > 3 {
		numericArgCount = 3
	}
	for i, arg := range args {
		eligibleArg := i < len(prefixArgs) && prefixArgs[i]
		sources[i] = unwrapPreparedImplicitCast(arg, eligibleArg)
		if i >= numericArgCount {
			continue
		}
		if eligibleArg {
			hasEligibleParam = true
			if (sources[i] != nil && types.T(sources[i].Typ.Id).IsDecimal()) ||
				(i < len(prefixKinds) && prefixKinds[i] == types.StringConversionDecimal) {
				hasDecimalPeer = true
			}
			continue
		}
		if sources[i] == nil {
			continue
		}
		if list := sources[i].GetList(); list != nil {
			for itemIndex, item := range list.List {
				eligible := i < len(prefixListArgs) && itemIndex < len(prefixListArgs[i]) &&
					prefixListArgs[i][itemIndex]
				item = unwrapPreparedImplicitCast(item, eligible)
				if eligible {
					hasEligibleParam = true
					if (item != nil && types.T(item.Typ.Id).IsDecimal()) ||
						(i < len(prefixListKinds) && itemIndex < len(prefixListKinds[i]) &&
							prefixListKinds[i][itemIndex] == types.StringConversionDecimal) {
						hasDecimalPeer = true
					}
				} else if item != nil && types.T(item.Typ.Id).IsDecimal() {
					hasDecimalPeer = true
				}
			}
			continue
		}
		if types.T(sources[i].Typ.Id).IsDecimal() {
			hasDecimalPeer = true
		}
		if isPreparedCommonValueFunction(name) &&
			!preparedNumericCommonOperandType(types.T(sources[i].Typ.Id)) {
			hasCommonValueBoundary = true
		}
	}
	if !hasEligibleParam || !hasDecimalPeer || hasCommonValueBoundary {
		return false
	}
	return true
}

func isPreparedCommonValueFunction(name string) bool {
	return name == "coalesce" || name == "greatest" || name == "least"
}

func preparedNumericCommonOperandType(oid types.T) bool {
	return oid == types.T_any || oid == types.T_bool || oid == types.T_bit || oid == types.T_year ||
		oid.IsInteger() || oid.IsFloat() || oid.IsDecimal()
}

func (rule *ResetParamRefRule) normalizePreparedNumericCommonArgs(
	name string,
	args []*plan.Expr,
) ([]*plan.Expr, bool, error) {
	numericArgCount := len(args)
	if name == "in_range" && numericArgCount > 3 {
		numericArgCount = 3
	}
	operands := make([]*plan.Expr, 0, numericArgCount)
	for i := 0; i < numericArgCount; i++ {
		if args[i] == nil {
			return args, false, nil
		}
		if list := args[i].GetList(); list != nil {
			operands = append(operands, list.List...)
			continue
		}
		operands = append(operands, args[i])
	}
	target, ok := preparedNumericCommonType(operands)
	if !ok {
		return args, false, nil
	}

	changed := false
	for i := 0; i < numericArgCount; i++ {
		if list := args[i].GetList(); list != nil {
			for itemIndex, item := range list.List {
				cast, castChanged, err := castPreparedNumericCommonExpr(rule.ctx, item, target)
				if err != nil {
					return nil, false, err
				}
				list.List[itemIndex] = cast
				changed = changed || castChanged
			}
			continue
		}
		cast, castChanged, err := castPreparedNumericCommonExpr(rule.ctx, args[i], target)
		if err != nil {
			return nil, false, err
		}
		args[i] = cast
		changed = changed || castChanged
	}
	return args, changed, nil
}

func preparedNumericCommonType(operands []*plan.Expr) (types.Type, bool) {
	hasDecimal := false
	hasFloat := false
	maxIntegralWidth := int32(0)
	maxScale := int32(0)
	for _, operand := range operands {
		if operand == nil {
			return types.Type{}, false
		}
		typ := makeTypeByPlan2Expr(operand)
		switch {
		case typ.Oid == types.T_any:
			continue
		case typ.Oid.IsFloat():
			hasFloat = true
		case typ.Oid.IsDecimal():
			hasDecimal = true
			width := typ.Width
			if width <= 0 {
				width = typ.Oid.ToType().Width
			}
			scale := typ.Scale
			if scale < 0 {
				scale = 0
			}
			maxIntegralWidth = max(maxIntegralWidth, max(width-scale, int32(0)))
			maxScale = max(maxScale, scale)
		case typ.Oid.IsInteger(), typ.Oid == types.T_bit, typ.Oid == types.T_bool, typ.Oid == types.T_year:
			maxIntegralWidth = max(maxIntegralWidth, preparedIntegerIntegralWidth(typ.Oid))
		default:
			return types.Type{}, false
		}
	}
	if !hasDecimal {
		return types.Type{}, false
	}
	if hasFloat {
		return types.T_float64.ToType(), true
	}
	width := maxIntegralWidth + maxScale
	if width < 1 {
		width = 1
	}
	switch {
	case width <= types.T_decimal64.ToType().Width:
		return types.New(types.T_decimal64, width, maxScale), true
	case width <= types.T_decimal128.ToType().Width:
		return types.New(types.T_decimal128, width, maxScale), true
	case width <= types.T_decimal256.ToType().Width:
		return types.New(types.T_decimal256, width, maxScale), true
	default:
		return types.T_float64.ToType(), true
	}
}

func preparedIntegerIntegralWidth(oid types.T) int32 {
	switch oid {
	case types.T_bool:
		return 1
	case types.T_bit, types.T_uint64:
		return 20
	case types.T_year:
		return 4
	case types.T_int8, types.T_uint8:
		return 3
	case types.T_int16, types.T_uint16:
		return 5
	case types.T_int32, types.T_uint32:
		return 10
	case types.T_int64:
		return 19
	default:
		return 0
	}
}

func castPreparedNumericCommonExpr(
	ctx context.Context,
	expr *plan.Expr,
	target types.Type,
) (*plan.Expr, bool, error) {
	if expr == nil {
		return nil, false, nil
	}
	source := makeTypeByPlan2Expr(expr)
	if source.Oid == types.T_bool {
		bridge := types.T_uint8.ToType()
		var err error
		expr, err = makePlan2CastExpr(ctx, expr, makePlan2Type(&bridge))
		if err != nil {
			return nil, false, err
		}
	}
	before := expr.Typ
	cast, err := makePlan2CastExpr(ctx, expr, makePlan2Type(&target))
	if err != nil {
		return nil, false, err
	}
	return cast, cast != expr || !reflect.DeepEqual(before, cast.Typ), nil
}

func (rule *ResetParamRefRule) preparedNumericPrefixCast(
	expr *plan.Expr,
	kind types.StringConversionKind,
) (*plan.Expr, bool, error) {
	if expr == nil {
		return expr, false, nil
	}
	literal := expr.GetLit()
	if types.T(expr.Typ.Id) == types.T_bool && literal != nil && !literal.Isnull {
		value := uint8(0)
		if literal.GetBval() {
			value = 1
		}
		return makePlan2Uint8ConstExprWithType(value), true, nil
	}
	if !types.T(expr.Typ.Id).IsMySQLString() {
		return expr, false, nil
	}
	if literal == nil {
		return expr, false, nil
	}
	if !literal.Isnull {
		var runtimeType types.Type
		switch kind {
		case types.StringConversionBoolean:
			value, err := strconv.ParseBool(strings.TrimSpace(literal.GetSval()))
			if err == nil {
				if value {
					return makePlan2Uint8ConstExprWithType(1), true, nil
				}
				return makePlan2Uint8ConstExprWithType(0), true, nil
			}
		case types.StringConversionInteger:
			if inferred, ok := PreparedRuntimeTypeFromString(literal.GetSval()); ok && inferred.Oid.IsInteger() {
				runtimeType = inferred
			}
		case types.StringConversionFloat:
			runtimeType = types.T_float64.ToType()
		case types.StringConversionDecimal:
			runtimeType = PreparedNumericPrefixTypeFromString(literal.GetSval())
		}
		if runtimeType.Oid != types.T_any {
			materialized, err := preparedRuntimeParamExpr(rule.ctx, literal.GetSval(), literal.IsBin, runtimeType)
			return materialized, err == nil, err
		}
	}
	target := types.New(types.T_decimal64, 1, 0)
	if !literal.Isnull {
		target = PreparedNumericPrefixTypeFromString(literal.GetSval())
	}
	if target.IsDecimal() {
		target.Charset = 255
	}
	cast, err := makePlan2CastExpr(rule.ctx, expr, makePlan2Type(&target))
	return cast, err == nil, err
}

func preparedParamPosition(expr *plan.Expr) (int, bool) {
	if expr == nil {
		return 0, false
	}
	if param := expr.GetP(); param != nil && param.Pos >= 0 {
		return int(param.Pos), true
	}
	if !isImplicitPreparedParamCast(expr) {
		return 0, false
	}
	return implicitPreparedParamPosition(expr)
}

func unwrapPreparedImplicitCast(expr *plan.Expr, eligible bool) *plan.Expr {
	for expr != nil {
		fn := expr.GetF()
		if fn == nil || fn.Func == nil || fn.Func.GetObjName() != "cast" || len(fn.Args) == 0 {
			return expr
		}
		_, overload := planfunction.DecodeOverloadID(fn.Func.GetObj())
		if overload != 0 {
			return expr
		}
		if !eligible {
			source := makeTypeByPlan2Expr(fn.Args[0])
			// Prepare-time binding can provisionally coerce a numeric peer to
			// TEXT or FLOAT because the marker is still TEXT. Recover the peer's
			// semantic source domain before deriving the execute-time common type.
			// Preserve the opposite string-to-numeric direction: decimal literals
			// are represented by exactly such an implicit cast.
			if !preparedNumericCommonOperandType(source.Oid) {
				return expr
			}
		}
		expr = fn.Args[0]
	}
	return nil
}

func unwrapNumericPrefixDependentImplicitCast(expr *plan.Expr) (*plan.Expr, bool) {
	current := expr
	changed := false
	for current != nil {
		fn := current.GetF()
		if fn == nil || fn.Func == nil || fn.Func.GetObjName() != "cast" || len(fn.Args) == 0 {
			break
		}
		_, overload := planfunction.DecodeOverloadID(fn.Func.GetObj())
		if overload != 0 {
			break
		}
		target := makeTypeByPlan2Expr(current)
		source := makeTypeByPlan2Expr(fn.Args[0])
		// Dependency propagation only invalidates the provisional coercions
		// selected while a numeric common-type child was still TEXT. Physical
		// casts such as YEAR and DECIMAL remain part of the executor/index ABI.
		if !source.IsNumeric() ||
			(!target.Oid.IsFloat() && !target.Oid.IsMySQLString()) {
			break
		}
		current = fn.Args[0]
		changed = true
	}
	return current, changed
}

func isExplicitPreparedCast(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.GetObjName() != "cast" {
		return false
	}
	_, overload := planfunction.DecodeOverloadID(fn.Func.GetObj())
	return overload != 0
}

func windowHasNumericPrefixDependency(
	window *plan.WindowSpec,
	dependent func(*plan.Expr) bool,
) bool {
	if window == nil {
		return false
	}
	if dependent(window.WindowFunc) {
		return true
	}
	for _, expr := range window.PartitionBy {
		if dependent(expr) {
			return true
		}
	}
	for _, order := range window.OrderBy {
		if order != nil && dependent(order.Expr) {
			return true
		}
	}
	if window.Frame != nil {
		if window.Frame.Start != nil && dependent(window.Frame.Start.Val) {
			return true
		}
		if window.Frame.End != nil && dependent(window.Frame.End.Val) {
			return true
		}
	}
	return false
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
	_, ok := implicitPreparedParam(expr)
	return ok
}

func implicitPreparedParamPosition(expr *plan.Expr) (int, bool) {
	param, ok := implicitPreparedParam(expr)
	if !ok {
		return 0, false
	}
	return int(param.Pos), true
}

// implicitPreparedParam follows only binder-inserted cast overloads. IN list
// normalization can legitimately stack several of them around a marker; an
// explicit CAST in any layer remains a hard boundary.
func implicitPreparedParam(expr *plan.Expr) (*plan.ParamRef, bool) {
	current := expr
	seenCast := false
	for current != nil {
		if param := current.GetP(); param != nil {
			return param, seenCast && param.Pos >= 0
		}
		fn := current.GetF()
		if fn == nil || fn.Func == nil || fn.Func.GetObjName() != "cast" || len(fn.Args) == 0 {
			return nil, false
		}
		_, overload := planfunction.DecodeOverloadID(fn.Func.GetObj())
		if overload != 0 {
			return nil, false
		}
		seenCast = true
		current = fn.Args[0]
	}
	return nil, false
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
