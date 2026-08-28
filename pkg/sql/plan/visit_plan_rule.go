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
	"fmt"
	"math"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
	if metadata := e.GetPreparedNumeric(); metadata.GetFallback() {
		if mapped, ok := rule.params[int(metadata.ParamPos)]; ok {
			metadata.ParamPos = int32(mapped)
		}
	}
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
	seen         map[*plan.ParamRef]struct{}
	seenFallback map[*plan.Expr]struct{}
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
	if metadata := e.GetPreparedNumeric(); metadata.GetFallback() {
		if rule.seenFallback == nil {
			rule.seenFallback = make(map[*plan.Expr]struct{})
		}
		if _, ok := rule.seenFallback[e]; !ok {
			rule.seenFallback[e] = struct{}{}
			if metadata.ParamPos > 0 {
				metadata.ParamPos--
			}
		}
	}
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
	ctx      context.Context
	params   []*Expr
	exprMemo map[*plan.Expr]*plan.Expr
	// preserveRoots contains DML write expressions whose outer shape must
	// remain stable while nested parameters are rebound.  The write operator
	// consumes these expressions positionally; rebuilding the outer function
	// can change its assignment-cast contract even when the predicate needs a
	// different execute-time overload.
	preserveRoots        map[*plan.Expr]struct{}
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
	// numericComparisonTextParamPositions identifies COM_STMT text markers
	// whose surrounding comparison has a numeric domain. Replace these leaves
	// with an engine DOUBLE cast before rebinding any enclosing function so
	// nested expressions, IN, and BETWEEN share MySQL numeric-string semantics.
	numericComparisonTextParamPositions map[int]bool
	// numericComparisonTextFallbackExprs records expressions that must remain in
	// the common DOUBLE comparison domain. A LOCK_OP key expression must retain
	// the primary-key physical type, so these expressions are replaced there by
	// a typed NULL while the scan filter performs the conversion and selects the
	// rows that the normal row-lock path must lock.
	numericComparisonTextFallbackExprs map[*Expr]struct{}
	// numericPrefixParamPositions is populated only after the deployment-wide
	// protocol reaches the version that understands Charset=255 numeric-prefix
	// casts. The map remains per-position to keep unrelated text parameters in
	// their ordinary string domains.
	numericPrefixParamPositions map[int]bool
	numericPrefixParamKinds     map[int]types.StringConversionKind
	// sqlExecuteNumericParams carries the logical source value of SQL EXECUTE
	// user variables. It is consulted only by arithmetic consumers; comparison
	// domains continue to use the dedicated common-type paths above.
	sqlExecuteNumericParams []*plan.Expr
	// numericPrefixDependent records rewritten expressions whose value domain
	// was selected from an execute-time numeric-prefix parameter. The dependency
	// propagates through binder-inserted casts so enclosing consumers can remove
	// provisional prepare-time coercions and bind against the runtime domain.
	numericPrefixDependent      map[*plan.Expr]bool
	serializedDecimalParamTypes map[*plan.Expr]types.Type
	// preparedPlan is used only to synchronize a flattened scalar-subquery
	// ColRef with the rebound type of its inner projection.  The explicit
	// source node/column metadata on Expr avoids relying on AuxId or expression
	// pointer identity after a plan copy.
	preparedPlan *Plan
	// paramKinds is populated by the execute-time replacement path.  It is
	// deliberately kept on the rule rather than inferred from Expr.Typ: a
	// prepared marker is TEXT at prepare time while COM_STMT carries the
	// protocol's actual numeric category.
	paramKinds  []vector.PrepareParamKind
	paramValues []any
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

func (rule *ResetParamRefRule) setPreparedPlan(preparePlan *Plan) {
	rule.preparedPlan = preparePlan
}

// SetParamKinds is used by the plan-level replacement tests and by callers
// that already decoded the protocol metadata.  Production replacement passes
// the richer ParamValue slice through SetParamValues below; keeping the kind
// setter makes the rule useful for the small, plan-only helpers as well.
func (rule *ResetParamRefRule) SetParamKinds(kinds []vector.PrepareParamKind) {
	rule.paramKinds = append(rule.paramKinds[:0], kinds...)
}

func (rule *ResetParamRefRule) SetParamValues(values []any) {
	rule.paramValues = append(rule.paramValues[:0], values...)
	if len(rule.paramKinds) == 0 {
		rule.paramKinds = make([]vector.PrepareParamKind, len(values))
	}
	for i, value := range values {
		if i >= len(rule.paramKinds) {
			break
		}
		if param, ok := value.(ParamValue); ok {
			rule.paramKinds[i] = param.PrepareParamKind
		}
	}
}

func (rule *ResetParamRefRule) runtimeParamValue(pos int) (any, vector.PrepareParamKind, bool) {
	if pos < 0 {
		return nil, vector.PrepareParamNone, false
	}
	if pos < len(rule.paramValues) {
		value := rule.paramValues[pos]
		if param, ok := value.(ParamValue); ok {
			return param.Value, param.PrepareParamKind, true
		}
		if value != nil {
			kind := vector.PrepareParamNone
			if pos < len(rule.paramKinds) {
				kind = rule.paramKinds[pos]
			}
			return value, kind, true
		}
	}
	if pos >= len(rule.params) || rule.params[pos] == nil {
		return nil, vector.PrepareParamNone, false
	}
	param := rule.params[pos]
	kind := vector.PrepareParamNone
	if pos < len(rule.paramKinds) {
		kind = rule.paramKinds[pos]
	}
	if lit := param.GetLit(); lit != nil {
		if lit.GetIsnull() {
			return nil, kind, true
		}
		// Read the oneof directly. Getter methods cannot distinguish a literal
		// value of zero from an unset field, which made the old fallback silently
		// reject ABS(0) and other zero-valued parameters.
		switch value := lit.Value.(type) {
		case *plan.Literal_Sval:
			return value.Sval, kind, value.Sval != ""
		case *plan.Literal_I8Val:
			return int8(value.I8Val), kind, true
		case *plan.Literal_I16Val:
			return int16(value.I16Val), kind, true
		case *plan.Literal_I32Val:
			return value.I32Val, kind, true
		case *plan.Literal_I64Val:
			return value.I64Val, kind, true
		case *plan.Literal_U8Val:
			return uint8(value.U8Val), kind, true
		case *plan.Literal_U16Val:
			return uint16(value.U16Val), kind, true
		case *plan.Literal_U32Val:
			return value.U32Val, kind, true
		case *plan.Literal_U64Val:
			return value.U64Val, kind, true
		case *plan.Literal_Fval:
			return value.Fval, kind, true
		case *plan.Literal_Dval:
			return value.Dval, kind, true
		case *plan.Literal_Bval:
			return value.Bval, kind, true
		case *plan.Literal_Decimal64Val, *plan.Literal_Decimal128Val:
			// Decimal literals created by the plan binder carry their exact value
			// in the oneof, but runtime tests/protocol values are represented by
			// their textual source. The type is still available from Expr.Typ.
			if sval := lit.GetSval(); sval != "" {
				return sval, kind, true
			}
		}
	}
	return nil, kind, false
}

func (rule *ResetParamRefRule) runtimeParamType(pos int) (types.Type, bool) {
	value, kind, ok := rule.runtimeParamValue(pos)
	if !ok || value == nil {
		return types.Type{}, false
	}
	if pos < len(rule.paramValues) {
		if param, ok := rule.paramValues[pos].(ParamValue); ok && param.HasRuntimeType {
			return param.RuntimeType, true
		}
	}
	switch kind {
	case vector.PrepareParamInteger:
		if typ, ok := PreparedRuntimeTypeFromString(strings.TrimSpace(fmt.Sprint(value))); ok && typ.Oid.IsInteger() {
			return typ, true
		}
	case vector.PrepareParamDecimal:
		if typ, ok := PreparedRuntimeTypeFromString(strings.TrimSpace(fmt.Sprint(value))); ok && typ.IsDecimal() {
			return typ, true
		}
	case vector.PrepareParamFloat:
		return types.T_float64.ToType(), true
	case vector.PrepareParamBoolean:
		return types.T_bool.ToType(), true
	default:
		if typ, ok := PreparedRuntimeTypeFromString(strings.TrimSpace(fmt.Sprint(value))); ok {
			return typ, true
		}
	}
	return types.Type{}, false
}

// typedIntegerParamExpr materializes the exact integer representation of a
// protocol value.  It intentionally refuses non-integer categories so an
// invalid/fractional value keeps the ordinary fallback semantics.
func (rule *ResetParamRefRule) typedIntegerParamExpr(pos int32) (*Expr, bool) {
	value, kind, ok := rule.runtimeParamValue(int(pos))
	if !ok || value == nil {
		return nil, false
	}
	if kind != vector.PrepareParamInteger && kind != vector.PrepareParamNone {
		return nil, false
	}
	typ, ok := rule.runtimeParamType(int(pos))
	if !ok || !typ.Oid.IsInteger() {
		return nil, false
	}
	isBin := false
	if int(pos) < len(rule.paramValues) {
		if param, ok := rule.paramValues[pos].(ParamValue); ok {
			isBin = param.IsBin
		}
	}
	bound, err := preparedRuntimeParamExpr(rule.ctx, value, isBin, typ)
	if err != nil {
		return nil, false
	}
	rule.retainRuntimeParamRef(int(pos), bound)
	return bound, true
}

func (rule *ResetParamRefRule) typedDecimalParamExpr(pos int32) (*Expr, bool, error) {
	value, kind, ok := rule.runtimeParamValue(int(pos))
	if !ok || value == nil || (kind != vector.PrepareParamDecimal && kind != vector.PrepareParamNone) {
		return nil, false, nil
	}
	typ, ok := rule.runtimeParamType(int(pos))
	if !ok || !typ.IsDecimal() {
		return nil, false, nil
	}
	isBin := false
	if int(pos) < len(rule.paramValues) {
		if param, ok := rule.paramValues[pos].(ParamValue); ok {
			isBin = param.IsBin
		}
	}
	bound, err := preparedRuntimeParamExpr(rule.ctx, value, isBin, typ)
	if err != nil {
		return nil, false, err
	}
	rule.retainRuntimeParamRef(int(pos), bound)
	return bound, true, nil
}

func (rule *ResetParamRefRule) typedRuntimeParamExpr(pos int) (*Expr, bool, error) {
	if bound, ok := rule.typedIntegerParamExpr(int32(pos)); ok {
		return bound, true, nil
	}
	if bound, ok, err := rule.typedDecimalParamExpr(int32(pos)); err != nil || ok {
		return bound, ok, err
	}
	value, kind, ok := rule.runtimeParamValue(pos)
	if !ok || value == nil {
		return nil, false, nil
	}
	typ, typOK := rule.runtimeParamType(pos)
	if !typOK {
		return nil, false, nil
	}
	if kind != vector.PrepareParamFloat && typ.Oid != types.T_float64 && typ.Oid != types.T_float32 {
		return nil, false, nil
	}
	isBin := false
	if pos < len(rule.paramValues) {
		if param, ok := rule.paramValues[pos].(ParamValue); ok {
			isBin = param.IsBin
		}
	}
	bound, err := preparedRuntimeParamExpr(rule.ctx, value, isBin, typ)
	if err != nil {
		return nil, false, err
	}
	rule.retainRuntimeParamRef(pos, bound)
	return bound, true, nil
}

// retainRuntimeParamRef keeps a specialized literal tied to its execution
// parameter when the resulting plan is placed in the bounded runtime cache.
// Decimal256 uses a text-to-decimal cast because the plan literal protocol has
// no Decimal256 oneof; attach the source to that inner text literal so restore
// leaves the cast target intact.  Other numeric literals can carry the source
// directly and restore to an equivalent typed cast.
func (rule *ResetParamRefRule) retainRuntimeParamRef(pos int, expr *Expr) {
	if expr == nil || pos < 0 || pos >= len(rule.paramValues) {
		return
	}
	param, ok := rule.paramValues[pos].(ParamValue)
	if !ok || !param.RetainParamRef {
		return
	}
	var target *Expr
	if lit := expr.GetLit(); lit != nil {
		target = expr
	} else if fn := expr.GetF(); fn != nil && fn.Func != nil &&
		strings.EqualFold(fn.Func.GetObjName(), "cast") && len(fn.Args) > 0 {
		if lit := fn.Args[0].GetLit(); lit != nil {
			target = fn.Args[0]
		}
	}
	if target == nil || target.GetLit() == nil {
		return
	}
	sourceType := target.Typ
	target.GetLit().Src = &plan.Expr{
		Typ:  sourceType,
		Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: int32(pos)}},
	}
}

func (rule *ResetParamRefRule) allIntegerParamRefs(expr *plan.Expr) bool {
	positions := make(map[int32]struct{})
	collectNumericValueParamPositions(expr, positions)
	if len(positions) == 0 {
		return false
	}
	for pos := range positions {
		if _, ok := rule.typedIntegerParamExpr(pos); !ok {
			return false
		}
	}
	return true
}

func (rule *ResetParamRefRule) allDecimalParamRefs(expr *plan.Expr) bool {
	positions := make(map[int32]struct{})
	collectNumericValueParamPositions(expr, positions)
	if len(positions) == 0 {
		return false
	}
	for pos := range positions {
		bound, ok, err := rule.typedDecimalParamExpr(pos)
		if err != nil || !ok || bound == nil {
			return false
		}
	}
	return true
}

// preparedNumericValueParamPositions returns every marker that contributes a
// value to a numeric result expression. Control-flow conditions are not value
// operands: a CASE/IF marker can choose between BIGINT branches without
// changing the domain of the value consumed by ABS. A flattened scalar
// subquery no longer contains its ParamRefs, so its explicit source marker is
// the only safe fallback in that shape.
func preparedNumericValueParamPositions(expr *plan.Expr) map[int32]struct{} {
	positions := make(map[int32]struct{})
	collectNumericValueParamPositions(expr, positions)
	collectFlattenedPreparedNumericSourcePositions(expr, positions)
	return positions
}

func collectFlattenedPreparedNumericSourcePositions(expr *plan.Expr, positions map[int32]struct{}) {
	if expr == nil {
		return
	}
	metadata := expr.GetPreparedNumeric()
	if expr.GetCol() != nil && metadata.GetFallbackSource() && metadata.GetParamPos() >= 0 {
		positions[metadata.GetParamPos()] = struct{}{}
		return
	}
	if fn := expr.GetF(); fn != nil && fn.Func != nil {
		name := strings.ToLower(fn.Func.GetObjName())
		if indexes, ok := numericFunctionResultArgs(name, len(fn.Args)); ok {
			for _, index := range indexes {
				if index >= 0 && index < len(fn.Args) {
					collectFlattenedPreparedNumericSourcePositions(fn.Args[index], positions)
				}
			}
			return
		}
		if name == "case" {
			for index, arg := range fn.Args {
				if numericFunctionArgKeepsContext(name, index, len(fn.Args)) {
					collectFlattenedPreparedNumericSourcePositions(arg, positions)
				}
			}
			return
		}
		for _, arg := range fn.Args {
			collectFlattenedPreparedNumericSourcePositions(arg, positions)
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			collectFlattenedPreparedNumericSourcePositions(item, positions)
		}
	}
	if sub := expr.GetSub(); sub != nil {
		collectFlattenedPreparedNumericSourcePositions(sub.Child, positions)
	}
}

func collectNumericValueParamPositions(expr *plan.Expr, positions map[int32]struct{}) {
	if expr == nil {
		return
	}
	if param := expr.GetP(); param != nil && param.Pos >= 0 {
		positions[param.Pos] = struct{}{}
		return
	}
	if fn := expr.GetF(); fn != nil && fn.Func != nil {
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
	if sub := expr.GetSub(); sub != nil {
		collectNumericValueParamPositions(sub.Child, positions)
	}
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

func (rule *ResetParamRefRule) rebindPreparedNumericExpr(
	expr *plan.Expr,
	positions map[int32]struct{},
) (*Expr, bool, error) {
	if expr == nil {
		return nil, false, nil
	}
	if param := expr.GetP(); param != nil {
		if _, ok := positions[param.Pos]; !ok {
			return expr, false, nil
		}
		bound, ok, err := rule.typedRuntimeParamExpr(int(param.Pos))
		return bound, ok, err
	}
	if isImplicitPreparedParamCast(expr) {
		if param, ok := implicitPreparedParam(expr); ok {
			if _, selected := positions[param.Pos]; !selected {
				return expr, false, nil
			}
			bound, changed, err := rule.rebindPreparedNumericExpr(expr.GetF().Args[0], positions)
			return bound, changed, err
		}
	}
	if sub := expr.GetSub(); sub != nil {
		child, changed, err := rule.rebindPreparedNumericExpr(sub.Child, positions)
		if err != nil || !changed {
			return expr, false, err
		}
		copy := DeepCopyExpr(expr)
		copy.GetSub().Child = child
		copy.Typ = child.Typ
		return copy, true, nil
	}
	if list := expr.GetList(); list != nil {
		copy := DeepCopyExpr(expr)
		changed := false
		for i, item := range list.List {
			bound, itemChanged, err := rule.rebindPreparedNumericExpr(item, positions)
			if err != nil {
				return nil, false, err
			}
			copy.GetList().List[i] = bound
			changed = changed || itemChanged
		}
		return copy, changed, nil
	}
	if fn := expr.GetF(); fn != nil {
		// A provisional cast is not an explicit user cast.  Removing it before
		// rebuilding the enclosing expression is what prevents ABS(? + 0) from
		// reintroducing the prepare-time DOUBLE round trip.
		copy := DeepCopyExpr(expr)
		changed := false
		for i, arg := range fn.Args {
			bound, argChanged, err := rule.rebindPreparedNumericExpr(arg, positions)
			if err != nil {
				return nil, false, err
			}
			copy.GetF().Args[i] = bound
			changed = changed || argChanged
		}
		if !changed {
			return expr, false, nil
		}
		// A numeric expression under the prepare-time ABS fallback is initially
		// bound against DOUBLE because the marker is TEXT.  Rebinding an integer
		// packet must also remove integral DOUBLE literals introduced by that
		// provisional context (for example the `0` in ABS(? + 0)); otherwise the
		// enclosing arithmetic function remains on its lossy DOUBLE overload.
		runtimeDomain := rule.preparedNumericRuntimeDomain(positions)
		if runtimeDomain == preparedNumericRuntimeInteger {
			for i, arg := range copy.GetF().Args {
				if integral, integralOK := provisionalIntegralFloatLiteral(arg); integralOK {
					copy.GetF().Args[i] = integral
				}
			}
		} else if runtimeDomain == preparedNumericRuntimeDecimal {
			for i, arg := range copy.GetF().Args {
				if decimal, decimalOK, decimalErr := provisionalDecimalFloatLiteral(rule.ctx, arg); decimalErr != nil {
					return nil, false, decimalErr
				} else if decimalOK {
					copy.GetF().Args[i] = decimal
				}
			}
		}
		name := fn.Func.GetObjName()
		if name == "cast" && isImplicitPreparedParamCast(expr) {
			return copy.GetF().Args[0], true, nil
		}
		bound, err := BindFuncExprImplByPlanExpr(rule.ctx, name, copy.GetF().Args)
		if err != nil {
			return nil, false, err
		}
		if boundFn := bound.GetF(); boundFn != nil {
			boundFn.AggConfig = bytes.Clone(fn.AggConfig)
			boundFn.AggConfigType = fn.AggConfigType
		}
		return bound, true, nil
	}
	// Flattened scalar subqueries can expose the deferred source as a ColRef.
	// Keep that reference intact when its explicit source identity is present;
	// the inner projection is rebound separately and the enclosing consumer only
	// needs its refreshed type.  Replacing the column with the raw parameter
	// would drop scalar-subquery filtering, LIMIT, and empty-result semantics.
	metadata := expr.GetPreparedNumeric()
	if expr.GetCol() != nil && metadata.GetFallbackSource() {
		return expr, false, nil
	}
	if metadata.GetFallback() && metadata.GetParamPos() >= 0 {
		if _, selected := positions[metadata.GetParamPos()]; !selected {
			return expr, false, nil
		}
		if bound, ok, err := rule.typedRuntimeParamExpr(int(metadata.GetParamPos())); err != nil || ok {
			return bound, ok, err
		}
	}
	return expr, false, nil
}

type preparedNumericRuntimeDomain uint8

const (
	preparedNumericRuntimeUnknown preparedNumericRuntimeDomain = iota
	preparedNumericRuntimeInteger
	preparedNumericRuntimeDecimal
	preparedNumericRuntimeFloat
)

func (rule *ResetParamRefRule) preparedNumericRuntimeDomain(
	positions map[int32]struct{},
) preparedNumericRuntimeDomain {
	domain := preparedNumericRuntimeInteger
	if len(positions) == 0 {
		return preparedNumericRuntimeUnknown
	}
	for pos := range positions {
		runtimeType, ok := rule.runtimeParamType(int(pos))
		if !ok {
			return preparedNumericRuntimeUnknown
		}
		switch {
		case runtimeType.Oid.IsFloat():
			return preparedNumericRuntimeFloat
		case runtimeType.Oid.IsDecimal():
			domain = preparedNumericRuntimeDecimal
		case runtimeType.Oid.IsInteger():
		default:
			return preparedNumericRuntimeUnknown
		}
	}
	return domain
}

func provisionalIntegralFloatLiteral(expr *plan.Expr) (*Expr, bool) {
	if expr == nil || (expr.Typ.Id != int32(types.T_float32) && expr.Typ.Id != int32(types.T_float64)) {
		return nil, false
	}
	lit := expr.GetLit()
	if lit == nil || lit.Isnull {
		return nil, false
	}
	var value float64
	switch valueImpl := lit.Value.(type) {
	case *plan.Literal_Dval:
		value = valueImpl.Dval
	case *plan.Literal_Fval:
		value = float64(valueImpl.Fval)
	default:
		return nil, false
	}
	if math.IsNaN(value) || math.IsInf(value, 0) || math.Trunc(value) != value ||
		value < math.MinInt64 || value > math.MaxInt64 {
		return nil, false
	}
	return makePlan2Int64ConstExprWithType(int64(value)), true
}

func provisionalDecimalFloatLiteral(ctx context.Context, expr *plan.Expr) (*Expr, bool, error) {
	if expr == nil || (expr.Typ.Id != int32(types.T_float32) && expr.Typ.Id != int32(types.T_float64)) {
		return nil, false, nil
	}
	lit := expr.GetLit()
	if lit == nil || lit.Isnull {
		return nil, false, nil
	}
	var value string
	switch valueImpl := lit.Value.(type) {
	case *plan.Literal_Dval:
		if math.IsNaN(valueImpl.Dval) || math.IsInf(valueImpl.Dval, 0) {
			return nil, false, nil
		}
		value = strconv.FormatFloat(valueImpl.Dval, 'g', -1, 64)
	case *plan.Literal_Fval:
		if math.IsNaN(float64(valueImpl.Fval)) || math.IsInf(float64(valueImpl.Fval), 0) {
			return nil, false, nil
		}
		value = strconv.FormatFloat(float64(valueImpl.Fval), 'g', -1, 32)
	default:
		return nil, false, nil
	}
	typ, ok := PreparedRuntimeTypeFromString(value)
	if !ok || !typ.IsDecimal() {
		// A provisional Dval for an integer literal such as `0` has no decimal
		// point, so the generic runtime inference reports INT64.  Within a
		// DECIMAL expression it is still an exact decimal operand; derive the
		// bounded decimal representation from the same textual prefix helper.
		typ = PreparedNumericPrefixTypeFromString(value)
		if !typ.IsDecimal() {
			return nil, false, nil
		}
	}
	converted, err := preparedRuntimeParamExpr(ctx, value, lit.IsBin, typ)
	if err != nil {
		return nil, false, err
	}
	return converted, true, nil
}

func (rule *ResetParamRefRule) rebindPreparedIntegerExpr(expr *plan.Expr) (*Expr, bool, error) {
	positions := preparedNumericValueParamPositions(expr)
	if len(positions) == 0 {
		return expr, false, nil
	}
	return rule.rebindPreparedNumericExpr(expr, positions)
}

func (rule *ResetParamRefRule) rebindPreparedDecimalExpr(expr *plan.Expr) (*Expr, bool, error) {
	positions := preparedNumericValueParamPositions(expr)
	if len(positions) == 0 {
		return expr, false, nil
	}
	return rule.rebindPreparedNumericExpr(expr, positions)
}

func (rule *ResetParamRefRule) preparedNumericSourceType(expr *plan.Expr) (plan.Type, bool) {
	metadata := expr.GetPreparedNumeric()
	if !metadata.GetFallbackSource() || rule.preparedPlan == nil {
		return plan.Type{}, false
	}
	query := rule.preparedPlan.GetQuery()
	if query == nil {
		return plan.Type{}, false
	}
	nodeID := metadata.GetFallbackSourceNodeId()
	colPos := metadata.GetFallbackSourceColPos()
	if nodeID < 0 || int(nodeID) >= len(query.Nodes) || colPos < 0 {
		return plan.Type{}, false
	}
	node := query.Nodes[nodeID]
	if node == nil || int(colPos) >= len(node.ProjectList) || node.ProjectList[colPos] == nil {
		return plan.Type{}, false
	}
	return node.ProjectList[colPos].Typ, true
}

func (rule *ResetParamRefRule) refreshPreparedNumericSource(expr *plan.Expr) (*Expr, bool, error) {
	if expr == nil {
		return nil, false, nil
	}
	if expr.GetCol() != nil && expr.GetPreparedNumeric().GetFallbackSource() {
		if typ, ok := rule.preparedNumericSourceType(expr); ok && !reflect.DeepEqual(expr.Typ, typ) {
			copy := DeepCopyExpr(expr)
			copy.Typ = typ
			return copy, true, nil
		}
		return expr, false, nil
	}
	if fn := expr.GetF(); fn != nil {
		copy := DeepCopyExpr(expr)
		changed := false
		for i, arg := range fn.Args {
			refreshed, argChanged, err := rule.refreshPreparedNumericSource(arg)
			if err != nil {
				return nil, false, err
			}
			copy.GetF().Args[i] = refreshed
			changed = changed || argChanged
		}
		if !changed {
			return expr, false, nil
		}
		bound, err := BindFuncExprImplByPlanExpr(rule.ctx, fn.Func.GetObjName(), copy.GetF().Args)
		if err != nil {
			return nil, false, err
		}
		if boundFn := bound.GetF(); boundFn != nil {
			boundFn.AggConfig = bytes.Clone(fn.AggConfig)
			boundFn.AggConfigType = fn.AggConfigType
		}
		return bound, true, nil
	}
	if list := expr.GetList(); list != nil {
		copy := DeepCopyExpr(expr)
		changed := false
		for i, item := range list.List {
			refreshed, itemChanged, err := rule.refreshPreparedNumericSource(item)
			if err != nil {
				return nil, false, err
			}
			copy.GetList().List[i] = refreshed
			changed = changed || itemChanged
		}
		return copy, changed, nil
	}
	if sub := expr.GetSub(); sub != nil && sub.Child != nil {
		refreshed, changed, err := rule.refreshPreparedNumericSource(sub.Child)
		if err != nil || !changed {
			return expr, false, err
		}
		copy := DeepCopyExpr(expr)
		copy.GetSub().Child = refreshed
		copy.Typ = refreshed.Typ
		return copy, true, nil
	}
	return expr, false, nil
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
	// A scalar subquery may be flattened to a ColRef while its parameter stays
	// in the inner PROJECT list.  The binder marks both the outer fallback and
	// that source projection.  Snapshot a non-column marker before recursively
	// replacing its children so the complete source expression (ROUND(?),
	// ? + 0, etc.) can be rebound without dropping the subquery semantics.
	var fallbackSource *plan.Expr
	if e.GetPreparedNumeric().GetFallback() && e.GetCol() == nil && e.GetSub() == nil {
		fallbackSource = DeepCopyExpr(e)
	}
	var rewritten *plan.Expr
	var err error
	if _, preserve := rule.preserveRoots[e]; preserve {
		rewritten, err = rule.applyExprPreservingRoot(e)
	} else {
		rewritten, err = rule.applyExpr(e)
	}
	if err != nil {
		return nil, err
	}
	if fallbackSource != nil {
		if source, ok := preparedNumericFallbackSource(fallbackSource); ok {
			positions := preparedNumericValueParamPositions(fallbackSource)
			if len(positions) > 0 {
				bound, changed, bindErr := rule.rebindPreparedNumericExpr(source, positions)
				if bindErr != nil {
					return nil, bindErr
				}
				if changed {
					rewritten = bound
					rule.specialized = true
				}
			}
		}
	}
	if rule.exprMemo == nil {
		rule.exprMemo = make(map[*plan.Expr]*plan.Expr)
	}
	rule.exprMemo[e] = rewritten
	return rewritten, nil
}

// PreserveAssignmentCast reports whether VisitPlan must leave the assignment
// cast around this expression untouched.  It is intentionally a small,
// optional rule hook so ordinary expression visitors retain their existing
// behavior.
func (rule *ResetParamRefRule) PreserveAssignmentCast(e *plan.Expr) bool {
	_, ok := rule.preserveRoots[e]
	return ok
}

// NormalizePreparedLockRows keeps a DOUBLE-domain text comparison out of the
// lock executor's typed primary-key fetch path. The corresponding scan filter
// remains responsible for MySQL conversion and row selection; NULL disables
// only this unsafe parameter-derived pre-lock key.
func (rule *ResetParamRefRule) NormalizePreparedLockRows(rewritten *Expr, target plan.Type) *Expr {
	if _, ok := rule.numericComparisonTextFallbackExprs[rewritten]; !ok {
		return rewritten
	}
	target.NotNullable = false
	return &Expr{
		Typ: target,
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Isnull: true,
		}},
	}
}

// applyExprPreservingRoot replaces parameters below a DML write expression,
// but keeps the root function (and its result type) intact.  A bare parameter
// root is left parameterized so the normal ParamExpressionExecutor supplies
// the value using the prepare-time assignment domain.
func (rule *ResetParamRefRule) applyExprPreservingRoot(e *plan.Expr) (*plan.Expr, error) {
	if e == nil {
		return nil, nil
	}
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_P:
		return e, nil
	case *plan.Expr_F:
		if exprImpl.F == nil {
			return e, nil
		}
		if rule.validateFunctionArgs != nil {
			if err := rule.validateFunctionArgs(exprImpl.F.Func.GetObjName(), exprImpl.F.Args); err != nil {
				return nil, err
			}
		}
		for i, arg := range exprImpl.F.Args {
			rewritten, err := rule.ApplyExpr(arg)
			if err != nil {
				return nil, err
			}
			exprImpl.F.Args[i] = rewritten
		}
		return e, nil
	case *plan.Expr_W:
		return applyWindowExpr(e, rule.ApplyExpr)
	case *plan.Expr_List:
		if exprImpl.List == nil {
			return e, nil
		}
		for i, arg := range exprImpl.List.List {
			rewritten, err := rule.ApplyExpr(arg)
			if err != nil {
				return nil, err
			}
			exprImpl.List.List[i] = rewritten
		}
		return e, nil
	default:
		return e, nil
	}
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
		functionName := ""
		if exprImpl.F.Func != nil {
			functionName = exprImpl.F.Func.GetObjName()
		}
		isAbs := strings.EqualFold(functionName, "abs") && len(exprImpl.F.Args) == 1
		var originalAbsArg *plan.Expr
		var hasPreparedAbsValue bool
		if isAbs {
			// Keep an immutable copy of the marker-bearing argument. Recursive
			// replacement can rebuild CASE/IF/scalar-subquery nodes and discard
			// the explicit fallback metadata; the copy is the provenance source
			// for the final ABS overload decision.
			originalAbsArg = DeepCopyExpr(exprImpl.F.Args[0])
			hasPreparedAbsValue = isPreparedNumericFallbackExpr(originalAbsArg) &&
				len(preparedNumericValueParamPositions(originalAbsArg)) > 0
		}
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
		numericComparisonFallback := false
		boundArgs := make([]*plan.Expr, len(exprImpl.F.Args))
		functionName = strings.ToLower(functionName)
		// An implicit cast around a COM_STMT text marker is provisional.  For a
		// numeric comparison, however, the column/literal side owns the
		// comparison domain and must remain indexable.  Replace the provisional
		// cast with the explicit MySQL numeric-prefix cast to that same target
		// type, instead of stripping it and rebinding the comparison as DOUBLE
		// (which would cast the column and can make an indexed predicate fail).
		implicitComparisonCast := functionName == "cast" && isImplicitPreparedParamCast(e)
		implicitComparisonCastPos := -1
		if implicitComparisonCast {
			if pos, ok := implicitPreparedParamPosition(e); ok {
				implicitComparisonCastPos = pos
			}
		}
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
			useSQLExecuteNumericSource := hasParamPos &&
				preparedFunctionArgUsesSQLExecuteNumericSource(
					e, functionName, i, len(exprImpl.F.Args)) &&
				paramPos < len(rule.sqlExecuteNumericParams) &&
				rule.sqlExecuteNumericParams[paramPos] != nil
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
			var rewrittenArg *plan.Expr
			if useSQLExecuteNumericSource {
				// The prepare-time implicit cast is provisional. Materialize the
				// SQL user variable's current source domain before descending into
				// that cast; evaluating it first can reject a valid DECIMAL value
				// using the overload selected for the initial TEXT marker.
				source := rule.sqlExecuteNumericParams[paramPos]
				rewrittenArg = &plan.Expr{Typ: source.Typ, Expr: source.Expr}
			} else {
				var applyErr error
				rewrittenArg, applyErr = rule.ApplyExpr(arg)
				err = applyErr
				if err != nil {
					return nil, err
				}
			}
			exprImpl.F.Args[i] = rewrittenArg
			boundArgs[i] = rewrittenArg
			if useSQLExecuteNumericSource {
				needResetFunction = true
				compareArgTypes = true
				// SourceType already represents the SQL value's numeric contract.
				// Do not also reinterpret the same argument through the text-prefix
				// specialization selected for comparisons and common-value peers.
				numericPrefixArgs[i] = false
			}
			if preparedExprContainsNumericComparisonFallback(
				rewrittenArg, rule.numericComparisonTextFallbackExprs,
			) {
				numericComparisonFallback = true
			}
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
				// Keep the original comparison-domain cast for text parameters. The
				// implicit cast node itself is rewritten to the explicit prefix cast
				// below; unwrapping it here would make the binder promote the column
				// side to DOUBLE and lose indexability.
				if !(isPreparedNumericComparison(functionName) && hasParamPos &&
					rule.numericComparisonTextParamPositions[paramPos]) &&
					(!isPreparedNumericComparison(functionName) || inferText) {
					if unwrapped, ok := unwrapImplicitPreparedParamCast(rule.ctx, rewrittenArg, inferText); ok {
						boundArgs[i] = unwrapped
						compareArgTypes = true
					}
				}
			}
		}
		if implicitComparisonCast && implicitComparisonCastPos >= 0 &&
			implicitComparisonCastPos < len(rule.params) &&
			rule.numericComparisonTextParamPositions[implicitComparisonCastPos] &&
			rule.params[implicitComparisonCastPos] != nil {
			if literal := rule.params[implicitComparisonCastPos].GetLit(); literal != nil {
				exact, ok, exactErr := preparedComparisonExactIntegerExpr(
					rule.ctx, literal.GetSval(), originalTyp)
				if exactErr != nil {
					return nil, exactErr
				}
				if ok {
					if literal.Src != nil {
						attachPreparedRuntimeParamSource(exact, literal.Src)
					}
					rule.specialized = true
					return exact, nil
				}
			}
			if literal := rule.params[implicitComparisonCastPos].GetLit(); literal != nil &&
				preparedComparisonTextNeedsDoubleFallback(literal.GetSval(), originalTyp) {
				// Keep the comparison in DOUBLE space when narrowing the converted
				// text into the column domain would change MySQL's numeric comparison
				// result. Ordinary exactly representable integer prefixes still use
				// the column-domain cast and keep the indexed column side untouched.
				numericType := preparedNumericComparisonTextType()
				fallback, castErr := makePlan2CastExpr(
					rule.ctx,
					rule.params[implicitComparisonCastPos],
					makePlan2Type(&numericType),
				)
				if castErr != nil {
					return nil, castErr
				}
				if rule.numericComparisonTextFallbackExprs == nil {
					rule.numericComparisonTextFallbackExprs = make(map[*Expr]struct{})
				}
				rule.numericComparisonTextFallbackExprs[fallback] = struct{}{}
				rule.specialized = true
				return fallback, nil
			}
			numericType := preparedNumericComparisonTextType()
			numeric, castErr := makePlan2CastExpr(
				rule.ctx,
				rule.params[implicitComparisonCastPos],
				makePlan2Type(&numericType),
			)
			if castErr != nil {
				return nil, castErr
			}
			explicit, castErr := appendExplicitCastBeforeExpr(
				rule.ctx,
				numeric,
				originalTyp,
			)
			if castErr != nil {
				return nil, castErr
			}
			rule.specialized = true
			return explicit, nil
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
		if numericComparisonFallback && isPreparedNumericComparisonContext(functionName) {
			var castErr error
			boundArgs, castErr = castPreparedComparisonArgsToDouble(rule.ctx, functionName, boundArgs)
			if castErr != nil {
				return nil, castErr
			}
			needResetFunction = true
			compareArgTypes = true
		}

		if isAbs && hasPreparedAbsValue {
			// A flattened scalar subquery leaves the ABS argument as a column
			// reference.  Its inner projection has already been rebound above;
			// refresh the reference type and rebind ABS, but keep the reference so
			// empty/multi-row scalar-subquery semantics remain intact.
			if originalAbsArg.GetPreparedNumeric().GetFallbackSource() {
				refreshed, changed, refreshErr := rule.refreshPreparedNumericSource(boundArgs[0])
				if refreshErr != nil {
					return nil, refreshErr
				}
				if changed {
					rewritten, bindErr := BindFuncExprImplByPlanExpr(
						rule.ctx, functionName, []*Expr{refreshed})
					if bindErr != nil {
						return nil, bindErr
					}
					if rewrittenFn := rewritten.GetF(); rewrittenFn != nil {
						rewrittenFn.AggConfig = bytes.Clone(exprImpl.F.AggConfig)
						rewrittenFn.AggConfigType = exprImpl.F.AggConfigType
					}
					rule.specialized = true
					return rewritten, nil
				}
			}
			source, sourceOK := preparedNumericFallbackSource(originalAbsArg)
			positions := preparedNumericValueParamPositions(originalAbsArg)
			if sourceOK && len(positions) > 0 {
				rebound, changed, reboundErr := rule.rebindPreparedNumericExpr(source, positions)
				if reboundErr != nil {
					return nil, reboundErr
				}
				if changed {
					rewritten, bindErr := BindFuncExprImplByPlanExpr(
						rule.ctx, functionName, []*Expr{rebound})
					if bindErr != nil {
						return nil, bindErr
					}
					if rewrittenFn := rewritten.GetF(); rewrittenFn != nil {
						rewrittenFn.AggConfig = bytes.Clone(exprImpl.F.AggConfig)
						rewrittenFn.AggConfigType = exprImpl.F.AggConfigType
					}
					rule.specialized = true
					return rewritten, nil
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
		position := int(exprImpl.P.Pos)
		param := rule.params[position]
		if rule.numericComparisonTextParamPositions[position] &&
			param != nil && param.Typ.Id == int32(types.T_text) && param.GetLit() != nil {
			runtimeType := preparedNumericComparisonTextType()
			return makePlan2CastExpr(rule.ctx, param, makePlan2Type(&runtimeType))
		}
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
	case *plan.Expr_Sub:
		if exprImpl.Sub == nil || exprImpl.Sub.Child == nil {
			return e, nil
		}
		child, childErr := rule.ApplyExpr(exprImpl.Sub.Child)
		if childErr != nil {
			return nil, childErr
		}
		if child != exprImpl.Sub.Child {
			exprImpl.Sub.Child = child
			e.Typ = child.Typ
			rule.specialized = true
		}
		return e, nil
	default:
		return e, nil
	}
}

// preparedComparisonExactIntegerExpr keeps an exact integral text prefix in
// the comparison peer's integer domain. Routing it through DOUBLE first loses
// adjacent BIGINT/BIT values above 2^53. Fractional and out-of-range values are
// intentionally rejected here so the caller retains MySQL's approximate
// numeric-comparison fallback for those cases.
func preparedComparisonExactIntegerExpr(
	ctx context.Context,
	value string,
	target plan.Type,
) (*plan.Expr, bool, error) {
	prefix, ok := planfunction.GetNumericStringPrefix(value)
	if !ok {
		return nil, false, nil
	}
	exact, ok := new(big.Rat).SetString(prefix)
	if !ok || !exact.IsInt() {
		return nil, false, nil
	}
	integer := exact.Num()
	targetType := makeTypeByPlan2Type(target)
	bits := 0
	signed := false
	switch targetType.Oid {
	case types.T_int8:
		bits, signed = 8, true
	case types.T_int16:
		bits, signed = 16, true
	case types.T_int32:
		bits, signed = 32, true
	case types.T_int64:
		bits, signed = 64, true
	case types.T_uint8:
		bits = 8
	case types.T_uint16:
		bits = 16
	case types.T_uint32:
		bits = 32
	case types.T_uint64:
		bits = 64
	case types.T_bit:
		bits = int(targetType.Width)
		if bits <= 0 || bits > 64 {
			bits = 64
		}
	default:
		return nil, false, nil
	}
	if signed {
		if !integer.IsInt64() {
			return nil, false, nil
		}
		parsed := integer.Int64()
		if bits < 64 && (parsed < -(int64(1)<<(bits-1)) || parsed > (int64(1)<<(bits-1))-1) {
			return nil, false, nil
		}
	} else {
		if !integer.IsUint64() || bits < 64 && integer.BitLen() > bits {
			return nil, false, nil
		}
	}
	expr, err := preparedRuntimeParamExpr(ctx, integer.String(), false, targetType)
	return expr, err == nil, err
}

func preparedComparisonTextNeedsDoubleFallback(value string, target plan.Type) bool {
	prefix, ok := planfunction.GetNumericStringPrefix(value)
	if !ok {
		// MySQL converts every string operand of a numeric comparison through its
		// numeric (DOUBLE) prefix.  A string with no numeric prefix therefore
		// becomes zero with a truncation warning; routing it through the
		// prepare-time integer/DECIMAL cast would instead raise an error or use a
		// different rounding domain.
		return true
	}
	numeric, err := strconv.ParseFloat(prefix, 64)
	if errors.Is(err, strconv.ErrRange) {
		return true
	}
	if err != nil || math.IsNaN(numeric) || math.IsInf(numeric, 0) {
		return true
	}

	// An integral column can keep its index only when the text value is an
	// exactly representable value in that column's domain.  Converting a
	// fractional DOUBLE through an integer cast would round it and change
	// MySQL's numeric-comparison result (for example, 1 = '0.9'). Values outside
	// the target range likewise need the common DOUBLE comparison domain instead
	// of an overflowing integer cast.
	switch types.T(target.Id) {
	case types.T_int8:
		return math.Trunc(numeric) != numeric || numeric < math.MinInt8 || numeric > math.MaxInt8 ||
			preparedComparisonTextLosesDoublePrecision(prefix, numeric)
	case types.T_int16:
		return math.Trunc(numeric) != numeric || numeric < math.MinInt16 || numeric > math.MaxInt16 ||
			preparedComparisonTextLosesDoublePrecision(prefix, numeric)
	case types.T_int32:
		return math.Trunc(numeric) != numeric || numeric < math.MinInt32 || numeric > math.MaxInt32 ||
			preparedComparisonTextLosesDoublePrecision(prefix, numeric)
	case types.T_int64:
		return math.Trunc(numeric) != numeric || numeric < -math.Exp2(63) || numeric >= math.Exp2(63) ||
			preparedComparisonTextLosesDoublePrecision(prefix, numeric)
	case types.T_uint8:
		return math.Trunc(numeric) != numeric || numeric < 0 || numeric > math.MaxUint8 ||
			preparedComparisonTextLosesDoublePrecision(prefix, numeric)
	case types.T_uint16:
		return math.Trunc(numeric) != numeric || numeric < 0 || numeric > math.MaxUint16 ||
			preparedComparisonTextLosesDoublePrecision(prefix, numeric)
	case types.T_uint32:
		return math.Trunc(numeric) != numeric || numeric < 0 || numeric > math.MaxUint32 ||
			preparedComparisonTextLosesDoublePrecision(prefix, numeric)
	case types.T_uint64:
		return math.Trunc(numeric) != numeric || numeric < 0 || numeric >= math.Exp2(64) ||
			preparedComparisonTextLosesDoublePrecision(prefix, numeric)
	case types.T_decimal64, types.T_decimal128, types.T_decimal256:
		// MySQL compares a DECIMAL value with a string in the approximate DOUBLE
		// domain. Casting the converted text back to DECIMAL can change the value
		// first (for example, 9007199254740993 becomes 9007199254740992).
		return true
	default:
		return false
	}
}

// castPreparedComparisonArgsToDouble keeps a comparison that contains a
// text-to-DOUBLE fallback in one common numeric domain.  Rebinding only the
// marker is insufficient: the function binder may otherwise promote the
// DOUBLE marker back through a DECIMAL/integer envelope.  IN-family functions
// carry their values in a plan list, so cast list items individually while
// preserving the list shape.
func castPreparedComparisonArgsToDouble(
	ctx context.Context,
	name string,
	args []*plan.Expr,
) ([]*plan.Expr, error) {
	numericType := makePlan2Type(&types.Type{Oid: types.T_float64})
	for i, arg := range args {
		if arg == nil {
			continue
		}
		if (name == "in" || name == "not_in" || name == "partition_in") && i == 1 {
			if list := arg.GetList(); list != nil {
				for j, item := range list.List {
					if item == nil {
						continue
					}
					converted, err := makePlan2CastExpr(ctx, item, numericType)
					if err != nil {
						return nil, err
					}
					list.List[j] = converted
				}
				// IN operators dispatch from the list expression's type. Keep it in
				// the same DOUBLE domain as its materialized items so the binder
				// selects the matching implementation instead of an integer operator
				// that would assert the vector type at execution.
				args[i].Typ = numericType
				continue
			}
		}
		converted, err := makePlan2CastExpr(ctx, arg, numericType)
		if err != nil {
			return nil, err
		}
		args[i] = converted
	}
	return args, nil
}

// preparedExprContainsNumericComparisonFallback reports whether an expression
// contains a marker that must stay in the MySQL text-to-DOUBLE comparison
// domain.  IN/NOT IN keep their candidates in a List expression, so looking
// up only the list node would miss a fallback marker nested in one of its
// items and leave the enclosing operator bound to an integer implementation.
func preparedExprContainsNumericComparisonFallback(
	expr *plan.Expr,
	fallbacks map[*Expr]struct{},
) bool {
	if expr == nil {
		return false
	}
	if _, ok := fallbacks[expr]; ok {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if preparedExprContainsNumericComparisonFallback(arg, fallbacks) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if preparedExprContainsNumericComparisonFallback(item, fallbacks) {
				return true
			}
		}
	}
	return false
}

// preparedComparisonTextLosesDoublePrecision reports whether converting the
// original numeric prefix to the runtime DOUBLE changed its value. Comparing
// only the truncated integer parts misses fractional prefixes that round to an
// integral DOUBLE (for example, 9007199254740992.5). Keep the original prefix
// as an exact rational so both integer and fractional precision loss is
// detected before the value is narrowed into an integral column domain.
func preparedComparisonTextLosesDoublePrecision(prefix string, numeric float64) bool {
	exact, ok := new(big.Rat).SetString(prefix)
	if !ok {
		return false
	}
	runtime, accuracy := new(big.Float).SetFloat64(numeric).Rat(nil)
	return accuracy != big.Exact || exact.Cmp(runtime) != 0
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

func preparedFunctionArgUsesSQLExecuteNumericSource(
	parent *plan.Expr,
	name string,
	argIndex int,
	argCount int,
) bool {
	if parent == nil || !makeTypeByPlan2Expr(parent).IsNumeric() {
		return false
	}
	if !isNumericContextFunction(name) && !supportsGenericNumericFunctionContext(name) {
		return false
	}
	return !numericFunctionHasSelectiveContext(name) ||
		numericFunctionArgKeepsContext(name, argIndex, argCount)
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

func isPreparedNumericComparisonContext(name string) bool {
	if isPreparedNumericComparison(name) {
		return true
	}
	switch name {
	case "between", "not_between", "in", "not_in", "partition_in":
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

// RestorePreparedRuntimeParamRefs converts runtime literals carrying ParamRef
// provenance back to typed parameter references after overload specialization.
// The surrounding casts/functions remain specialized, while the resulting plan
// can safely be compiled once and reused with different values in the same
// semantic category.
func RestorePreparedRuntimeParamRefs(ctx context.Context, preparePlan *Plan) error {
	if preparePlan == nil || preparePlan.GetQuery() == nil {
		return nil
	}
	return NewVisitPlan(preparePlan, []VisitPlanRule{restorePreparedRuntimeParamRefRule{ctx: ctx}}).Visit(ctx)
}

type restorePreparedRuntimeParamRefRule struct{ ctx context.Context }

func (restorePreparedRuntimeParamRefRule) MatchNode(*Node) bool  { return false }
func (restorePreparedRuntimeParamRefRule) IsApplyExpr() bool     { return true }
func (restorePreparedRuntimeParamRefRule) ApplyNode(*Node) error { return nil }
func (restore restorePreparedRuntimeParamRefRule) ApplyExpr(expr *Expr) (*Expr, error) {
	err := plan.VisitExprTree(expr, func(candidate *plan.Expr) error {
		lit := candidate.GetLit()
		if lit == nil || lit.Src == nil || lit.Src.GetP() == nil {
			return nil
		}
		restored := DeepCopyExpr(lit.Src)
		if candidate.Typ.Id != int32(types.T_text) {
			// Process stores COM_STMT payloads in a TEXT vector. Keep the
			// parameter source physical type honest and retain the runtime domain
			// as an explicit cast in the reusable specialized plan.
			restored.Typ = plan.Type{Id: int32(types.T_text)}
			target := types.New(types.T(candidate.Typ.Id), candidate.Typ.Width, candidate.Typ.Scale)
			cast, err := makePlan2CastExpr(restore.ctx, restored, makePlan2Type(&target))
			if err != nil {
				return err
			}
			restored = cast
		}
		*candidate = *restored
		return nil
	})
	return expr, err
}
