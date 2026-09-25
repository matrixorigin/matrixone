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
	case *plan.Expr_Sub:
		if exprImpl.Sub != nil {
			exprImpl.Sub.Child, _ = rule.ApplyExpr(exprImpl.Sub.Child)
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
	if metadata := e.GetPreparedNumeric(); metadata != nil {
		if metadata.GetFallback() {
			if mapped, ok := rule.params[int(metadata.ParamPos)]; ok {
				metadata.ParamPos = int32(mapped)
			}
		}
		if metadata.StringDomainSource != nil {
			var err error
			metadata.StringDomainSource, err = rule.ApplyExpr(metadata.StringDomainSource)
			if err != nil {
				return nil, err
			}
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
	case *plan.Expr_Sub:
		if exprImpl.Sub != nil {
			exprImpl.Sub.Child, _ = rule.ApplyExpr(exprImpl.Sub.Child)
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
	err := plan.VisitExprTree(e, func(expr *plan.Expr) error {
		if sub := expr.GetSub(); sub != nil {
			rule.pending = append(rule.pending, sub.NodeId)
		}
		return nil
	})
	return e, err
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
	case *plan.Expr_Sub:
		if exprImpl.Sub != nil && exprImpl.Sub.Child != nil {
			var err error
			exprImpl.Sub.Child, err = rule.ApplyExpr(exprImpl.Sub.Child)
			if err != nil {
				return nil, err
			}
		}
	}
	return e, nil
}

// ---------------------------

type ResetParamRefRule struct {
	ctx      context.Context
	params   []*Expr
	exprMemo map[*plan.Expr]*plan.Expr
	// Runtime value sources reached through integer-consumer lineage, including
	// bare markers and producers behind PROJECT, set, aggregate or window nodes.
	integerSourceRoots    map[*plan.Expr]struct{}
	integerPredicateRoots map[*plan.Expr]map[int32]struct{}
	// preserveRoots contains DML write expressions whose outer shape must
	// remain stable while nested parameters are rebound.  The write operator
	// consumes these expressions positionally; rebuilding the outer function
	// can change its assignment-cast contract even when the predicate needs a
	// different execute-time overload.
	preserveRoots        map[*plan.Expr]struct{}
	validateFunctionArgs func(string, []*Expr) error
	// specialized is set when execute-time rebinding changes the cached plan's
	// execution semantics, including value-only rewrites whose overload and
	// result type remain stable.
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
	// user variables. String-backed sources retain their separate MySQL numeric-
	// prefix domain and must not own a result/common-value domain.
	sqlExecuteNumericParams      []*plan.Expr
	sqlExecuteStringBackedParams []bool
	// numericPrefixDependent records rewritten expressions whose value domain
	// was selected from an execute-time numeric-prefix parameter. The dependency
	// propagates through binder-inserted casts so enclosing consumers can remove
	// provisional prepare-time coercions and bind against the runtime domain.
	numericPrefixDependent map[*plan.Expr]bool
	// sqlExecuteNumericDependent tracks expressions whose numeric result domain
	// was selected from an execute-time source, including deferred numeric
	// fallbacks. Unlike a direct marker, a dependent child must propagate
	// through enclosing numeric consumers so their provisional prepare-time
	// envelopes are rebound too.
	sqlExecuteNumericDependent  map[*plan.Expr]bool
	serializedDecimalParamTypes map[*plan.Expr]types.Type
	// preparedPlan locates existing numeric scalar-subquery fallback sources.
	// String-domain lineage is self-contained in sparse expression metadata and
	// does not add a plan-graph walk to prepared execution.
	preparedPlan *Plan
	// exactProjectedParam records only transparent output-to-marker identity
	// before ParamRefs are materialized. Domain summaries are not value sources.
	exactProjectedParam  map[preparedSetOperationNullKey]int32
	projectedParamDomain map[preparedSetOperationNullKey]int32
	setArmParam          map[preparedSetOperationInputKey]int32
	// preparedTemporalNullEnvelopeExprs marks runtime-only casts that restore a
	// strict NULL TIME-arithmetic envelope. Deferred numeric consumers must keep
	// these casts intact instead of pairing their target-type argument with the
	// original arithmetic children during a later fallback rebind.
	preparedTemporalNullEnvelopeExprs map[*plan.Expr]struct{}
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
		explicitCast := isExplicitPreparedCast(expr)
		if !explicitCast {
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
	if isImplicitPreparedParamCast(expr) {
		pos, ok := implicitPreparedParamPosition(expr)
		if !ok || positions[pos] != types.StringConversionDecimal {
			return false
		}
		// A serialized prefix filter may also contain parameters whose temporal
		// target type was fixed by the binder.  Such a cast is a semantic boundary,
		// not evidence that a string packet's leading digits should select the
		// execute-time DECIMAL domain.
		return preparedParamCastAllowsNumericPrefix(expr)
	}
	if param := expr.GetP(); param != nil && param.Pos >= 0 &&
		positions[int(param.Pos)] == types.StringConversionDecimal {
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
		directEligible = directEligible && eligiblePosition && preparedParamCastAllowsNumericPrefix(arg)
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
				itemEligible = itemEligible && eligiblePosition && preparedParamCastAllowsNumericPrefix(item)
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

func preparedParamCastAllowsNumericPrefix(expr *plan.Expr) bool {
	if !isImplicitPreparedParamCast(expr) {
		return true
	}
	target := types.T(expr.Typ.Id)
	return !target.ToType().IsTemporal() || target == types.T_year
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
	rule.integerSourceRoots = make(map[*plan.Expr]struct{})
	rule.integerPredicateRoots = make(map[*plan.Expr]map[int32]struct{})
	query := preparePlan.GetQuery()
	if query == nil {
		return
	}
	positions := make(map[int32]struct{})
	var checkedProjectedParam map[preparedSetOperationNullKey]struct{}
	for nodeID, node := range query.Nodes {
		if node == nil {
			continue
		}
		if isPreparedSetOperationNode(node.NodeType) {
			for branchIdx, childID := range node.Children {
				if childID < 0 || int(childID) >= len(query.Nodes) {
					continue
				}
				child := query.Nodes[childID]
				if child == nil {
					continue
				}
				for colPos, output := range child.ProjectList {
					if colPos >= len(node.ProjectList) || node.ProjectList[colPos] == nil {
						continue
					}
					source := unwrapPreparedSetOperationCoercion(query, childID, colPos,
						node.ProjectList[colPos].Typ, output)
					if pos, ok := preparedProjectedParamPosition(query, child, source,
						make(map[preparedSetOperationNullKey]bool), false); ok {
						if rule.setArmParam == nil {
							rule.setArmParam = make(map[preparedSetOperationInputKey]int32)
						}
						rule.setArmParam[preparedSetOperationInputKey{
							node: node, branchIdx: branchIdx, colPos: colPos,
						}] = pos
					}
				}
			}
		}
		registerProjectedParam := func(col *plan.ColRef) {
			if col == nil || col.RelPos < 0 || col.ColPos < 0 || int(col.RelPos) >= len(node.Children) {
				return
			}
			key := preparedSetOperationNullKey{nodeID: node.Children[col.RelPos], colPos: col.ColPos}
			if _, checked := checkedProjectedParam[key]; checked {
				return
			}
			if checkedProjectedParam == nil {
				checkedProjectedParam = make(map[preparedSetOperationNullKey]struct{})
			}
			checkedProjectedParam[key] = struct{}{}
			if pos, exact := preparedProjectedOutputParamPosition(query, key.nodeID, key.colPos,
				make(map[preparedSetOperationNullKey]bool), false); exact {
				if rule.exactProjectedParam == nil {
					rule.exactProjectedParam = make(map[preparedSetOperationNullKey]int32)
				}
				if rule.projectedParamDomain == nil {
					rule.projectedParamDomain = make(map[preparedSetOperationNullKey]int32)
				}
				rule.exactProjectedParam[key] = pos
				rule.projectedParamDomain[key] = pos
			} else if pos, domain := preparedProjectedOutputParamPosition(query, key.nodeID, key.colPos,
				make(map[preparedSetOperationNullKey]bool), true); domain {
				if rule.projectedParamDomain == nil {
					rule.projectedParamDomain = make(map[preparedSetOperationNullKey]int32)
				}
				rule.projectedParamDomain[key] = pos
			}
		}
		_ = plan.VisitExpressionsInOwner(node, func(root *plan.Expr) error {
			return plan.VisitExprTree(root, func(expr *plan.Expr) error {
				if fn := expr.GetF(); fn != nil {
					if expr.GetPreparedNumeric().GetProvisionalResultCast() && len(fn.Args) > 0 {
						registerProjectedParam(fn.Args[0].GetCol())
					}
					if fn.Func != nil {
						for i, arg := range fn.Args {
							if preparedProjectedValueOperand(fn.Func.GetObjName(), i, len(fn.Args)) {
								registerProjectedParam(arg.GetCol())
							}
						}
					}
				}
				for _, source := range integerArgumentSources(expr) {
					collectPreparedIntegerArgumentParamPositions(query, int32(nodeID), source,
						positions, make(map[[2]int32]struct{}), rule.integerSourceRoots)
				}
				return nil
			})
		})
	}
	// A numeric peer in NULLIF establishes a separate comparison domain. Its
	// provisional casts need rebinding for numeric EXECUTE values, but TEXT
	// comparisons must not inherit the integer result source contract.
	for _, node := range query.Nodes {
		if node == nil {
			continue
		}
		_ = plan.VisitExpressionsInOwner(node, func(root *plan.Expr) error {
			return plan.VisitExprTree(root, func(expr *plan.Expr) error {
				fn := expr.GetF()
				if fn == nil || fn.Func == nil || fn.Func.ObjName != "case" || len(fn.Args) != 3 ||
					!preparedNullValueExpr(fn.Args[1]) || !preparedComparisonHasNumericLiteral(fn.Args[0]) {
					return nil
				}
				if _, source := rule.integerSourceRoots[expr]; !source {
					return nil
				}
				positions := preparedNumericValueParamPositions(fn.Args[0])
				if len(positions) == 0 || !preparedComparisonUsesResultMarker(positions, fn.Args[2]) {
					return nil
				}
				return plan.VisitExprTree(fn.Args[0], func(predicate *plan.Expr) error {
					if predicate.GetF() != nil || predicate.GetP() != nil {
						rule.integerSourceRoots[predicate] = struct{}{}
						rule.integerPredicateRoots[predicate] = positions
					}
					return nil
				})
			})
		})
	}
	// BIT_COUNT owns its marker's text-vs-numeric dispatch. Keep the complete
	// function as an integer source, but let its descendants pass through the
	// ordinary BIT_COUNT fallback path instead of forcing protocol TEXT.
	for _, node := range query.Nodes {
		if node == nil {
			continue
		}
		_ = plan.VisitExpressionsInOwner(node, func(root *plan.Expr) error {
			return plan.VisitExprTree(root, func(expr *plan.Expr) error {
				fn := expr.GetF()
				if fn == nil || fn.Func == nil || !strings.EqualFold(fn.Func.ObjName, "bit_count") {
					return nil
				}
				for _, arg := range fn.Args {
					_ = plan.VisitExprTree(arg, func(descendant *plan.Expr) error {
						delete(rule.integerSourceRoots, descendant)
						delete(rule.integerPredicateRoots, descendant)
						return nil
					})
				}
				return nil
			})
		})
	}
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

func (rule *ResetParamRefRule) preparedGeometrySRIDParamExpr(pos int, sourceIsNull bool) (*Expr, bool, error) {
	value, _, ok := rule.runtimeParamValue(pos)
	if !ok {
		return nil, false, nil
	}
	if sourceIsNull {
		// Geometry evaluators short-circuit a NULL source before validating a
		// scalar SRID. Materialize a typed NULL here as well, otherwise the
		// binder would reject an otherwise NULL result for an invalid SRID.
		typ := types.T_int64.ToType()
		return &Expr{
			Typ:  makePlan2Type(&typ),
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}},
		}, true, nil
	}
	if pos >= 0 && pos < len(rule.paramValues) {
		if param, isParam := rule.paramValues[pos].(ParamValue); isParam &&
			value == nil && param.MaterializedValue != "" {
			value = param.MaterializedValue
		}
	}
	srid, isNull, err := geometrySRIDRuntimeValue(value)
	if err != nil {
		return nil, false, err
	}
	typ := types.T_int64.ToType()
	literal := &plan.Literal{Isnull: isNull}
	if !isNull {
		literal.Value = &plan.Literal_I64Val{I64Val: int64(srid)}
	}
	return &Expr{
		Typ:  makePlan2Type(&typ),
		Expr: &plan.Expr_Lit{Lit: literal},
	}, true, nil
}

func (rule *ResetParamRefRule) preparedBitCountUsesNumericRuntime(expr *plan.Expr) bool {
	for pos := range preparedNumericValueParamPositions(expr) {
		if pos >= 0 && int(pos) < len(rule.paramValues) {
			if PreparedParamValueHasNumericRuntime(rule.paramValues[pos]) {
				return true
			}
		}
	}
	return false
}

func (rule *ResetParamRefRule) runtimeParamType(pos int) (types.Type, bool) {
	value, kind, ok := rule.runtimeParamValue(pos)
	if !ok {
		return types.Type{}, false
	}
	if pos < len(rule.paramValues) {
		if param, ok := rule.paramValues[pos].(ParamValue); ok {
			// SQL EXECUTE values are transported through a text vector, so their
			// logical source type must drive overload rebinding without becoming
			// the visible type of a bare result marker.  An explicit RuntimeType
			// (for protocol values or a latched specialization) remains authoritative;
			// a SQL source is only a fallback when no such type is present.
			if param.HasRuntimeType {
				return param.RuntimeType, true
			}
			if !param.IsBinaryProtocol && param.HasSourceType &&
				(param.SourceType.IsNumeric() || param.SourceType.Oid == types.T_bool ||
					param.SourceType.Oid == types.T_year) {
				return param.SourceType, true
			}
		}
	}
	if value == nil {
		return types.Type{}, false
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

func (rule *ResetParamRefRule) hasNumericRuntimeParam(pos int) bool {
	_, kind, ok := rule.runtimeParamValue(pos)
	if !ok {
		return false
	}
	switch kind {
	case vector.PrepareParamInteger, vector.PrepareParamDecimal,
		vector.PrepareParamFloat, vector.PrepareParamBoolean:
		return true
	}
	if pos < 0 || pos >= len(rule.paramValues) {
		return false
	}
	param, ok := rule.paramValues[pos].(ParamValue)
	if !ok {
		return false
	}
	if param.HasSourceType {
		return param.SourceType.IsNumeric() || param.SourceType.Oid == types.T_bool ||
			param.SourceType.Oid == types.T_year
	}
	return param.HasRuntimeType && (param.RuntimeType.IsNumeric() ||
		param.RuntimeType.Oid == types.T_bool || param.RuntimeType.Oid == types.T_year)
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
	if value, _, ok := rule.runtimeParamValue(pos); ok && value == nil {
		// A NULL still has a source domain (or is genuinely ANY). Rebinding
		// the enclosing numeric expression from that source lets its overload
		// be selected as if the same value had appeared in the direct query.
		return rule.preparedRuntimeSourceExpr(pos, false)
	}
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
		if !typ.Oid.IsMySQLString() {
			return nil, false, nil
		}
		raw := fmt.Sprint(value)
		if pos < len(rule.paramValues) {
			if param, ok := rule.paramValues[pos].(ParamValue); ok {
				raw = preparedParamValueText(param)
			}
		}
		inferred, inferredOK := PreparedRuntimeTypeFromString(strings.TrimSpace(raw))
		if !inferredOK {
			return nil, false, nil
		}
		isBin := false
		if pos < len(rule.paramValues) {
			if param, ok := rule.paramValues[pos].(ParamValue); ok {
				isBin = param.IsBin
			}
		}
		bound, err := preparedRuntimeParamExpr(rule.ctx, raw, isBin, inferred)
		if err != nil {
			return nil, false, err
		}
		rule.retainRuntimeParamRef(pos, bound)
		return bound, true, nil
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
	return rule.rebindPreparedNumericExprWithBound(expr, expr, positions)
}

// rebindPreparedNumericExprWithBound carries two views of a deferred numeric
// expression: expr is the immutable prepare-time provenance, while bound is
// the occurrence already materialized for this EXECUTE.  A runtime value may
// refine the numeric envelope, but an unsupported value (for example "foo")
// must not resurrect the prepare-time cast or raw transport text.  Returning
// the bound occurrence with changed=false lets the enclosing numeric context
// apply its own conversion rules.
func (rule *ResetParamRefRule) rebindPreparedNumericExprWithBound(
	expr, bound *plan.Expr,
	positions map[int32]struct{},
) (*Expr, bool, error) {
	if expr == nil {
		return bound, false, nil
	}
	if bound != nil {
		if _, marked := rule.preparedTemporalNullEnvelopeExprs[bound]; marked {
			return bound, false, nil
		}
	}
	if param := expr.GetP(); param != nil {
		if _, ok := positions[param.Pos]; !ok {
			if bound != nil {
				return bound, false, nil
			}
			return expr, false, nil
		}
		typedBound, ok, err := rule.typedRuntimeParamExpr(int(param.Pos))
		if err != nil || ok {
			return typedBound, ok, err
		}
		// A NULL or an otherwise non-numeric runtime value may not have a
		// typed numeric replacement. Keep the already materialized execution
		// occurrence and report no refinement so a surrounding comparison can
		// still apply its own numeric-prefix conversion.
		if bound != nil {
			return bound, false, nil
		}
		return expr, false, nil
	}
	if isImplicitPreparedParamCast(expr) {
		if param, ok := implicitPreparedParam(expr); ok {
			if _, selected := positions[param.Pos]; !selected {
				if bound != nil {
					return bound, false, nil
				}
				return expr, false, nil
			}
			boundChild := bound
			if bound != nil {
				if boundFn := bound.GetF(); boundFn != nil && len(boundFn.Args) > 0 {
					boundChild = boundFn.Args[0]
				}
			}
			return rule.rebindPreparedNumericExprWithBound(
				expr.GetF().Args[0], boundChild, positions)
		}
	}
	if sub := expr.GetSub(); sub != nil {
		boundChild := sub.Child
		if bound != nil {
			if boundSub := bound.GetSub(); boundSub != nil {
				boundChild = boundSub.Child
			}
		}
		child, changed, err := rule.rebindPreparedNumericExprWithBound(
			sub.Child, boundChild, positions)
		if err != nil {
			return nil, false, err
		}
		if !changed {
			if bound != nil {
				return bound, false, nil
			}
			return expr, false, nil
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
			boundItem := item
			if bound != nil {
				if boundList := bound.GetList(); boundList != nil && i < len(boundList.List) {
					boundItem = boundList.List[i]
				}
			}
			itemBound, itemChanged, err := rule.rebindPreparedNumericExprWithBound(
				item, boundItem, positions)
			if err != nil {
				return nil, false, err
			}
			if itemBound == nil {
				itemBound = boundItem
			}
			if itemBound == nil {
				itemBound = item
			}
			copy.GetList().List[i] = itemBound
			changed = changed || itemChanged
		}
		if !changed && bound != nil {
			return bound, false, nil
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
			boundArg := arg
			if bound != nil {
				if boundFn := bound.GetF(); boundFn != nil && i < len(boundFn.Args) {
					boundArg = boundFn.Args[i]
				}
			}
			argBound, argChanged, err := rule.rebindPreparedNumericExprWithBound(
				arg, boundArg, positions)
			if err != nil {
				return nil, false, err
			}
			if argBound == nil {
				argBound = boundArg
			}
			if argBound == nil {
				argBound = arg
			}
			copy.GetF().Args[i] = argBound
			changed = changed || argChanged
		}
		if !changed {
			if bound != nil {
				return bound, false, nil
			}
			return expr, false, nil
		}
		if isExplicitPreparedCast(expr) {
			bound, err := rebindExplicitPreparedCast(rule.ctx, expr, copy.GetF().Args)
			return bound, true, err
		}
		// Recover only peers whose source explicitly proves that FLOAT was a
		// prepare-time envelope. Source-less scientific literals and explicit
		// FLOAT casts are semantic FLOAT boundaries and must remain unchanged.
		for i, arg := range copy.GetF().Args {
			if source, sourceOK := provisionalNumericPeerSource(arg); sourceOK {
				copy.GetF().Args[i] = source
			}
		}
		name := fn.Func.GetObjName()
		if name == "cast" && isImplicitPreparedParamCast(expr) {
			return copy.GetF().Args[0], true, nil
		}
		restorePreparedIntegerArithmeticOperands(name, copy.GetF().Args)
		if isPreparedTemporalIntegerArithmetic(name) {
			if temporalPeer, ok := preparedTemporalNumericPeerFromArgs(fn.Args); ok {
				// Reconstruct the same TIME boundary as the main rebind path.
				// The recursive fallback removes provisional marker casts, so
				// coerce the completed integer operand before choosing an overload.
				for i, original := range fn.Args {
					coerced, _, err := rule.coercePreparedTemporalIntegerOperand(
						original, copy.GetF().Args[i], temporalPeer,
						len(preparedNumericValueParamPositions(original)) > 0)
					if err != nil {
						return nil, false, err
					}
					copy.GetF().Args[i] = coerced
				}
			}
		}
		bound, err := BindFuncExprImplByPlanExpr(rule.ctx, name, copy.GetF().Args)
		if err != nil {
			return nil, false, err
		}
		preserveReboundFunctionMetadata(fn, bound.GetF())
		return bound, true, nil
	}
	// Flattened scalar subqueries can expose the deferred source as a ColRef.
	// Keep that reference intact when its explicit source identity is present;
	// the inner projection is rebound separately and the enclosing consumer only
	// needs its refreshed type.  Replacing the column with the raw parameter
	// would drop scalar-subquery filtering, LIMIT, and empty-result semantics.
	metadata := expr.GetPreparedNumeric()
	if expr.GetCol() != nil && metadata.GetFallbackSource() {
		if bound != nil {
			return bound, false, nil
		}
		return expr, false, nil
	}
	if metadata.GetFallback() && metadata.GetParamPos() >= 0 {
		if _, selected := positions[metadata.GetParamPos()]; !selected {
			if bound != nil {
				return bound, false, nil
			}
			return expr, false, nil
		}
		if bound, ok, err := rule.typedRuntimeParamExpr(int(metadata.GetParamPos())); err != nil || ok {
			return bound, ok, err
		}
	}
	if bound != nil {
		return bound, false, nil
	}
	return expr, false, nil
}

// A user CAST fixes the target domain and conversion semantics. Revalidate its
// current source type, but do not resolve it as a new implicit CAST or elide a
// now-redundant boundary before the enclosing arithmetic is rebound.
func rebindExplicitPreparedCast(ctx context.Context, original *Expr, args []*Expr) (*Expr, error) {
	if len(args) != 2 || args[0] == nil || args[1] == nil {
		return nil, moerr.NewInternalError(ctx, "invalid prepared CAST arguments")
	}
	_, overload := planfunction.DecodeOverloadID(original.GetF().Func.Obj)
	_, err := planfunction.GetFunctionByNameWithOverload(ctx, "cast", []types.Type{
		makeTypeByPlan2Expr(args[0]), makeTypeByPlan2Expr(args[1]),
	}, overload)
	if err != nil {
		return nil, err
	}
	bound := DeepCopyExpr(original)
	bound.GetF().Args = args
	return bound, nil
}

// provisionalNumericPeerSource restores a semantic numeric peer that PREPARE
// temporarily converted to TEXT to coexist with an unresolved marker. Unlike
// provisionalExactNumericSource, FLOAT is valid here: it remains a deliberate
// semantic boundary and is restored only after a numeric runtime source proves
// that the enclosing common-type function must be rebound numerically.
func provisionalNumericPeerSource(expr *plan.Expr) (*Expr, bool) {
	if source, ok := provisionalExactNumericSource(expr); ok {
		return source, true
	}
	if expr == nil || !expr.GetPreparedNumeric().GetProvisionalResultPeer() {
		return nil, false
	}
	literal := expr.GetLit()
	if literal == nil || literal.Src == nil {
		return nil, false
	}
	source := literal.Src
	if fn := source.GetF(); fn != nil && fn.Func != nil && fn.Func.GetObjName() == "cast" &&
		!fn.GetSyntaxExplicitCast() && len(fn.Args) > 0 && types.T(source.Typ.Id).IsMySQLString() {
		_, overload := planfunction.DecodeOverloadID(fn.Func.Obj)
		if overload == 0 {
			source = fn.Args[0]
		}
	}
	if source != nil && preparedNumericCommonOperandType(makeTypeByPlan2Expr(source).Oid) {
		return DeepCopyExpr(source), true
	}
	return nil, false
}

func provisionalExactNumericSource(expr *plan.Expr) (*Expr, bool) {
	if expr == nil {
		return nil, false
	}
	target := types.T(expr.Typ.Id)
	if !target.IsFloat() && !target.IsMySQLString() {
		return nil, false
	}
	lit := expr.GetLit()
	if lit != nil && lit.Src != nil {
		if source, ok := provisionalExactNumericSource(lit.Src); ok {
			return source, true
		}
		sourceType := makeTypeByPlan2Expr(lit.Src)
		if preparedNumericCommonOperandType(sourceType.Oid) && !sourceType.Oid.IsFloat() {
			return DeepCopyExpr(lit.Src), true
		}
	}
	if fn := expr.GetF(); fn != nil && fn.Func != nil && fn.Func.GetObjName() == "cast" && len(fn.Args) > 0 {
		_, overload := planfunction.DecodeOverloadID(fn.Func.GetObj())
		provisionalCast := (target.IsMySQLString() && !fn.GetSyntaxExplicitCast()) ||
			(target.IsFloat() && overload == 0)
		sourceType := makeTypeByPlan2Expr(fn.Args[0])
		if provisionalCast && preparedNumericCommonOperandType(sourceType.Oid) && !sourceType.Oid.IsFloat() {
			return DeepCopyExpr(fn.Args[0]), true
		}
	}
	return nil, false
}

// Integer widening casts inserted for the PREPARE-time overload are not source
// domains. Re-resolve arithmetic from those sources when a parameter changes;
// otherwise a BIT peer widened to UINT64 acquires checked unsigned semantics
// that the equivalent directly bound BIT expression does not have.
func restorePreparedIntegerArithmeticOperands(name string, args []*Expr) {
	if name != "+" && name != "-" && name != "*" {
		return
	}
	for i, arg := range args {
		for arg != nil && types.T(arg.Typ.Id).IsInteger() {
			fn := arg.GetF()
			if fn == nil || fn.Func == nil || fn.Func.ObjName != "cast" || fn.SyntaxExplicitCast || len(fn.Args) == 0 || fn.Args[0] == nil {
				break
			}
			_, overload := planfunction.DecodeOverloadID(fn.Func.Obj)
			source := types.T(fn.Args[0].Typ.Id)
			if overload != 0 || (!source.IsInteger() && source != types.T_bit) {
				break
			}
			arg = fn.Args[0]
			args[i] = arg
		}
	}
}

func preparedTemporalNumericPeer(expr *Expr) (types.Type, bool) {
	if expr == nil {
		return types.Type{}, false
	}
	if metadata := expr.GetPreparedNumeric(); metadata.GetProvisionalResultPeer() &&
		metadata.GetProvisionalResultPeerTypeId() == int32(types.T_time) {
		return makeTypeByPlan2Expr(expr), true
	}
	if literal := expr.GetLit(); literal != nil && literal.Src != nil &&
		types.T(literal.Src.Typ.Id) == types.T_time {
		return makeTypeByPlan2Expr(expr), true
	}
	if fn := expr.GetF(); fn != nil && fn.Func != nil &&
		strings.EqualFold(fn.Func.GetObjName(), "cast") && !isExplicitPreparedCast(expr) && len(fn.Args) > 0 &&
		types.T(fn.Args[0].Typ.Id) == types.T_time {
		return makeTypeByPlan2Expr(expr), true
	}
	return types.Type{}, false
}

func preparedTemporalNumericPeerFromArgs(args []*Expr) (types.Type, bool) {
	for _, arg := range args {
		peerType, ok := preparedTemporalNumericPeer(arg)
		if ok && peerType.Oid.IsDecimal() {
			return peerType, true
		}
	}
	return types.Type{}, false
}

func isPreparedTemporalIntegerArithmetic(name string) bool {
	switch name {
	case "+", "-", "%", "mod":
		return true
	default:
		return false
	}
}

// coercePreparedTemporalIntegerOperand applies the TIME numeric coercion at
// the boundary where the completed operand meets TIME. Rebinding individual
// parameter leaves would move the TIME scale into an inner integer operation
// and can change both its overflow behavior and its result. Keep the inner
// expression in its execute-time integer domain, then cast the whole integer
// result to the TIME peer's DECIMAL64 domain with scale zero.
func (rule *ResetParamRefRule) coercePreparedTemporalIntegerOperand(
	original, bound *Expr,
	temporalPeer types.Type,
	hasNumericValueParam bool,
) (*Expr, bool, error) {
	if original == nil || bound == nil ||
		!hasNumericValueParam ||
		!types.T(bound.Typ.Id).IsInteger() || !temporalPeer.Oid.IsDecimal() {
		return bound, false, nil
	}
	temporalPeer.Scale = 0
	coerced, err := appendCastBeforeExpr(rule.ctx, bound, makePlan2Type(&temporalPeer))
	return coerced, true, err
}

// preparedTemporalStrictNullNestedArithmetic reports whether expr is a nested
// arithmetic expression whose result is forced NULL by the current execution
// parameters. Keep this separate from the integer boundary coercion: a NULL
// marker has no execute-time numeric domain, so the prepared decimal envelope
// is the only stable result metadata for a strict nested operation.
func (rule *ResetParamRefRule) preparedTemporalStrictNullNestedArithmetic(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil && fn.Func != nil {
		name := strings.ToLower(fn.Func.GetObjName())
		if name == "cast" && len(fn.Args) > 0 {
			return rule.preparedTemporalStrictNullNestedArithmetic(fn.Args[0])
		}
		if !isPreparedTemporalIntegerArithmetic(name) {
			return false
		}
		for _, arg := range fn.Args {
			if rule.preparedTemporalStrictNullValue(arg) {
				return true
			}
		}
	}
	return false
}

func (rule *ResetParamRefRule) preparedTemporalStrictNullValue(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if param := expr.GetP(); param != nil && param.Pos >= 0 {
		return rule.preparedRuntimeParamIsNull(int(param.Pos))
	}
	if literal := expr.GetLit(); literal != nil {
		return literal.GetIsnull()
	}
	if fn := expr.GetF(); fn != nil && fn.Func != nil {
		name := strings.ToLower(fn.Func.GetObjName())
		if name == "cast" && len(fn.Args) > 0 {
			return rule.preparedTemporalStrictNullValue(fn.Args[0])
		}
		if isPreparedTemporalIntegerArithmetic(name) {
			for _, arg := range fn.Args {
				if rule.preparedTemporalStrictNullValue(arg) {
					return true
				}
			}
		}
	}
	return false
}

func (rule *ResetParamRefRule) preparedRuntimeParamSelected(pos int) bool {
	if pos < 0 {
		return false
	}
	// A nil params slice is used by direct helper tests that provide runtime
	// values through SetParamValues. The execute-time plan replacement path
	// always supplies a slot for every marker; an absent slot there means the
	// position was deliberately left unselected.
	if rule.params == nil {
		return true
	}
	return pos < len(rule.params) && rule.params[pos] != nil
}

func (rule *ResetParamRefRule) preparedRuntimeParamIsNull(pos int) bool {
	if pos < 0 {
		return false
	}
	// A position-scoped specialization leaves unselected entries absent from
	// rule.params while its parallel ParamValue slot is the zero value. That
	// zero value is not an execute-time NULL and must not participate in the
	// unresolved envelope inference.
	if !rule.preparedRuntimeParamSelected(pos) {
		return false
	}
	if pos < len(rule.paramValues) {
		if param, ok := rule.paramValues[pos].(ParamValue); ok {
			return preparedParamValueIsNull(param)
		}
	}
	value, _, ok := rule.runtimeParamValue(pos)
	return ok && value == nil
}

// inferPreparedTemporalNullExpr derives one operand type for the unresolved
// NULL envelope without changing the executable expression. A domainless NULL
// uses the complete DECIMAL128 operand domain for inference only; a concrete
// marker uses its current execute-time numeric type. Provisional casts are
// transparent, while explicit casts and non-arithmetic control-flow functions
// remain semantic boundaries.
func (rule *ResetParamRefRule) inferPreparedTemporalNullExpr(
	expr *plan.Expr,
) (types.Type, bool, error) {
	if expr == nil {
		return types.Type{}, false, nil
	}
	if param := expr.GetP(); param != nil && param.Pos >= 0 {
		if !rule.preparedRuntimeParamSelected(int(param.Pos)) {
			return types.Type{}, false, nil
		}
		if rule.preparedRuntimeParamIsNull(int(param.Pos)) {
			return types.New(types.T_decimal128, 38, 0), true, nil
		}
		typ, ok := rule.runtimeParamType(int(param.Pos))
		if !ok || !typ.IsNumeric() {
			return types.Type{}, false, nil
		}
		return typ, true, nil
	}
	if literal := expr.GetLit(); literal != nil {
		if literal.GetIsnull() {
			// Constant folding can erase an explicit CAST(NULL AS ...)
			// wrapper while retaining the literal's declared type. Only a
			// T_any NULL is domainless; a typed NULL must keep its explicit
			// integer/decimal boundary during envelope inference.
			typedNull := makeTypeByPlan2Expr(expr)
			if typedNull.Oid != types.T_any {
				return typedNull, true, nil
			}
			return types.New(types.T_decimal128, 38, 0), true, nil
		}
		return makeTypeByPlan2Expr(expr), true, nil
	}
	if len(preparedNumericValueParamPositions(expr)) == 0 &&
		!rule.preparedTemporalStrictNullValue(expr) {
		return makeTypeByPlan2Expr(expr), true, nil
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return types.Type{}, false, nil
	}
	name := strings.ToLower(fn.Func.GetObjName())
	if name == "cast" && len(fn.Args) > 0 {
		if isExplicitPreparedCast(expr) {
			return makeTypeByPlan2Expr(expr), true, nil
		}
		// Binder-inserted casts around a marker or a marker-bearing arithmetic
		// subtree are provisional overload artifacts. Infer from their source;
		// keeping this cast would reintroduce the DECIMAL64 PREPARE hint.
		return rule.inferPreparedTemporalNullExpr(fn.Args[0])
	}
	if !isPreparedTemporalIntegerArithmetic(name) {
		// In particular, do not cross COALESCE/IFNULL/CASE: those functions can
		// turn a NULL marker into a concrete value before TIME arithmetic sees it.
		return types.Type{}, false, nil
	}
	argTypes := make([]types.Type, len(fn.Args))
	for i, arg := range fn.Args {
		typ, ok, err := rule.inferPreparedTemporalNullExpr(arg)
		if err != nil || !ok {
			return types.Type{}, false, err
		}
		argTypes[i] = typ
	}
	resolved, err := planfunction.GetFunctionByName(rule.ctx, name, argTypes)
	if err != nil {
		return types.Type{}, false, nil
	}
	return resolved.GetReturnType(), true, nil
}

// preparedTemporalNullEnvelopeType resolves the guarded outer operation from
// its original operands. originalTyp is intentionally only a scale fallback:
// it is the narrow PREPARE-time hint, not the NULL execution envelope.
func (rule *ResetParamRefRule) preparedTemporalNullEnvelopeType(
	functionName string,
	temporalPeer types.Type,
	originalTyp plan.Type,
	originalArgs []*Expr,
) (plan.Type, bool, error) {
	argTypes := make([]types.Type, len(originalArgs))
	for i, arg := range originalArgs {
		if peer, ok := preparedTemporalNumericPeer(arg); ok && peer.Oid.IsDecimal() {
			argTypes[i] = peer
			continue
		}
		typ, ok, err := rule.inferPreparedTemporalNullExpr(arg)
		if err != nil || !ok {
			return plan.Type{}, false, err
		}
		// The executable TIME boundary first coerces a completed integer
		// operand into the TIME peer's DECIMAL64 domain. Reproduce that boundary
		// in the type-only inference as well; resolving DECIMAL64 + INT directly
		// would widen an explicit CAST(... AS SIGNED/UNSIGNED) to DECIMAL128.
		if types.T(typ.Oid).IsInteger() {
			integerPeer := temporalPeer
			integerPeer.Scale = 0
			typ = integerPeer
		}
		argTypes[i] = typ
	}
	resolved, err := planfunction.GetFunctionByName(rule.ctx, functionName, argTypes)
	if err != nil {
		return plan.Type{}, false, nil
	}
	target := resolved.GetReturnType()
	if target.Scale < originalTyp.Scale {
		target.Scale = originalTyp.Scale
	}
	if target.Scale < temporalPeer.Scale {
		target.Scale = temporalPeer.Scale
	}
	return makePlan2Type(&target), true, nil
}

// restorePreparedTemporalNullEnvelope keeps the result metadata selected when
// a prepared TIME arithmetic expression had no runtime numeric category. It
// is deliberately limited to strict nested arithmetic: COALESCE/IFNULL/CASE
// can turn a NULL marker into a value and must continue through normal
// execute-time type inference.
func (rule *ResetParamRefRule) restorePreparedTemporalNullEnvelope(
	functionName string,
	originalTyp plan.Type,
	originalArgs []*Expr,
	rewritten *Expr,
) (*Expr, bool, error) {
	if rewritten == nil || !isPreparedTemporalIntegerArithmetic(functionName) ||
		!types.T(originalTyp.Id).IsDecimal() {
		return rewritten, false, nil
	}
	temporalPeer, ok := preparedTemporalNumericPeerFromArgs(originalArgs)
	if !ok {
		return rewritten, false, nil
	}
	strictNull := false
	for _, arg := range originalArgs {
		if rule.preparedTemporalStrictNullNestedArithmetic(arg) {
			strictNull = true
			break
		}
	}
	if !strictNull {
		return rewritten, false, nil
	}
	target, ok, inferErr := rule.preparedTemporalNullEnvelopeType(
		functionName, temporalPeer, originalTyp, originalArgs)
	if inferErr != nil {
		return rewritten, false, inferErr
	}
	if !ok || (types.T(rewritten.Typ.Id) == types.T(target.Id) &&
		rewritten.Typ.Width == target.Width &&
		rewritten.Typ.Scale == target.Scale &&
		rewritten.Typ.Charset == target.Charset &&
		rewritten.Typ.NotNullable == target.NotNullable) {
		return rewritten, false, nil
	}
	// A strict nested NULL is nullable even if the prepare-time hint was
	// marked non-nullable by the provisional overload.
	target.NotNullable = false
	restored, err := appendCastBeforeExpr(rule.ctx, rewritten, target)
	if err == nil && restored != nil {
		restored.Typ.NotNullable = false
		if cast := restored.GetF(); cast != nil && len(cast.Args) > 1 && cast.Args[1] != nil {
			cast.Args[1].Typ.NotNullable = false
		}
		if rule.preparedTemporalNullEnvelopeExprs == nil {
			rule.preparedTemporalNullEnvelopeExprs = make(map[*plan.Expr]struct{})
		}
		rule.preparedTemporalNullEnvelopeExprs[restored] = struct{}{}
	}
	return restored, true, err
}

func (rule *ResetParamRefRule) rebindPreparedIntegerExpr(expr *plan.Expr) (*Expr, bool, error) {
	positions := preparedNumericValueParamPositions(expr)
	if len(positions) == 0 {
		return expr, false, nil
	}
	return rule.rebindPreparedNumericExprWithBound(expr, expr, positions)
}

func (rule *ResetParamRefRule) rebindPreparedDecimalExpr(expr *plan.Expr) (*Expr, bool, error) {
	positions := preparedNumericValueParamPositions(expr)
	if len(positions) == 0 {
		return expr, false, nil
	}
	return rule.rebindPreparedNumericExprWithBound(expr, expr, positions)
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
			// The producer owns the value domain, but an outer join can add
			// NULLs at this occurrence even for a non-nullable aggregate.
			copy.Typ.NotNullable = typ.NotNullable && expr.Typ.NotNullable
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
		preserveReboundFunctionMetadata(fn, bound.GetF())
		// Rebinding a child must not erase the enclosing expression's
		// provenance (notably an IFNULL common-value boundary).
		bound.PreparedNumeric = copyPreparedNumericMetadata(expr.PreparedNumeric)
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
	_, source := rule.integerSourceRoots[e]
	if positions, predicate := rule.integerPredicateRoots[e]; predicate {
		// All markers participating in this separate comparison must have a
		// numeric execution domain. One numeric result cannot coerce a TEXT
		// comparison peer into an integer value source.
		source = len(positions) > 0
		for pos := range positions {
			if pos < 0 || int(pos) >= len(rule.paramValues) ||
				!PreparedParamValueHasNumericRuntime(rule.paramValues[pos]) {
				source = false
				break
			}
		}
	}
	if source {
		// This occurrence already uses the actual runtime domain. A second
		// numeric-fallback pass would infer a number from real TEXT again.
		fallbackSource = nil
		rewritten, err = rule.integerArgumentRuntimeSource(e)
		rule.specialized = true
	} else if hasSourceDependentIntegerArguments(e) {
		fallbackSource = nil
		rewritten, err = rule.rebindSourceDependentIntegerArguments(e)
	} else if isIntegerArgumentCast(e) {
		rewritten, err = rule.rebindIntegerArgumentCast(e)
	} else if _, preserve := rule.preserveRoots[e]; preserve {
		rewritten, err = rule.applyExprPreservingRoot(e)
	} else if _, directAssignment := directIntegerAssignmentParam(e); directAssignment {
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
				bound, changed, bindErr := rule.rebindPreparedNumericExprWithBound(
					source, rewritten, positions)
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

// NormalizePreparedLockRows preserves the lock executor ABI: LockRows is read
// through the physical accessor selected by PrimaryColTyp, so execute-time
// specialization must never leave a differently typed vector here.
func (rule *ResetParamRefRule) NormalizePreparedLockRows(rewritten *Expr, target plan.Type) (*Expr, error) {
	if _, ok := rule.numericComparisonTextFallbackExprs[rewritten]; ok {
		// The scan filter remains responsible for MySQL's DOUBLE comparison.
		// A typed NULL disables only the unsafe parameter-derived pre-lock key.
		return &Expr{
			Typ: target,
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Isnull: true,
			}},
		}, nil
	}
	if reflect.DeepEqual(rewritten.Typ, target) {
		return rewritten, nil
	}
	normalized, err := makePlan2CastExpr(rule.ctx, rewritten, target)
	if err != nil {
		return nil, err
	}
	return normalized, nil
}

// applyExprPreservingRoot replaces parameters below a DML write expression,
// but keeps the root function (and its result type) intact.  A bare parameter
// root is left parameterized so the normal ParamExpressionExecutor supplies
// the value using the prepare-time assignment domain.
func (rule *ResetParamRefRule) applyExprPreservingRoot(e *plan.Expr) (*plan.Expr, error) {
	if e == nil {
		return nil, nil
	}
	// The writer's integer type stays fixed, but a directly assigned numeric
	// parameter must not be interpreted as TEXT merely because of transport.
	// Do not reinterpret string parameters or descend through user expressions.
	if pos, ok := directIntegerAssignmentParam(e); ok {
		if pos < len(rule.paramValues) {
			if param, ok := rule.paramValues[pos].(ParamValue); ok &&
				((param.IsBinaryProtocol && (!param.HasRuntimeType || param.RuntimeType.Oid.IsMySQLString())) ||
					(param.HasSourceType && param.SourceType.Oid.IsMySQLString())) {
				return e, nil
			}
		}
		if typ, known := rule.runtimeParamType(pos); known && typ.IsNumeric() {
			if source, bound, err := rule.typedRuntimeParamExpr(pos); err != nil {
				return nil, err
			} else if bound {
				rule.specialized = true
				return forceAssignmentCastExprWithName(rule.ctx, source, e.Typ, e.GetF().Func.GetObjName())
			}
		}
		// A textual marker remains parameterized: generic parameter replacement
		// may infer numeric-prefix types for other consumers of the same marker.
		return e, nil
	}
	// A sibling direct assignment marker must keep its runtime vector metadata
	// (notably PrepareParamKind for BIT). Replacing it with a TEXT literal while
	// another assignment triggers specialization changes numeric values to bytes.
	if fn := e.GetF(); fn != nil && len(fn.Args) == 2 && !types.T(e.Typ.Id).IsInteger() {
		switch fn.Func.GetObjName() {
		case "cast", "cast_strict", "cast_assign", "cast_ignore":
			if param := fn.Args[0].GetP(); param != nil {
				position := int(param.Pos)
				if position >= 0 && position < len(rule.paramValues) {
					if value, ok := rule.paramValues[position].(ParamValue); ok &&
						(value.PrepareParamKind != vector.PrepareParamNone ||
							(value.HasSourceType && value.SourceType.IsNumeric()) ||
							(value.HasRuntimeType && value.RuntimeType.IsNumeric())) {
						return e, nil
					}
				}
				bound, err := rule.ApplyExpr(fn.Args[0])
				if err != nil {
					return nil, err
				}
				rule.retainRuntimeParamRef(int(param.Pos), bound)
				fn.Args[0] = bound
				return e, nil
			}
		}
	}
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_P:
		return e, nil
	case *plan.Expr_F:
		if exprImpl.F == nil {
			return e, nil
		}
		if exprImpl.F.Func != nil && strings.EqualFold(exprImpl.F.Func.GetObjName(), "inet_ntoa") &&
			len(exprImpl.F.Args) == 1 {
			// INET_NTOA owns the source-domain contract for a direct marker. A
			// matching VARCHAR assignment cast may make this function a preserved
			// DML write root, but preserving the root must not bypass the same
			// execute-time JSON/temporal rebinding used by SELECT. Keep the
			// destination-facing result type stable after rebinding.
			_, direct := preparedParamPosition(exprImpl.F.Args[0])
			if !direct {
				_, direct = preparedResultParamPosition(exprImpl.F.Args[0], "inet_ntoa")
			}
			if direct {
				originalTyp := e.Typ
				rewritten, err := rule.applyExpr(e)
				if err != nil {
					return nil, err
				}
				if rewritten != nil {
					rewritten.Typ = originalTyp
				}
				return rewritten, nil
			}
		}
		if isPreparedGeometrySRIDFunction(exprImpl.F.Func.GetObjName()) &&
			len(exprImpl.F.Args) >= 2 {
			if _, ok := preparedParamPosition(exprImpl.F.Args[len(exprImpl.F.Args)-1]); ok {
				// A geometry SRID setter/constructor owns value-dependent
				// result metadata. It cannot remain a preserved write root or
				// the destination cast would see the prepare-time undefined SRID.
				return rule.applyExpr(e)
			}
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
		if strings.EqualFold(exprImpl.F.Func.GetObjName(), moGeometryCastToSubtypeFun) {
			if err := validateGeometryAssignmentSRID(rule.ctx, e, e.Typ); err != nil {
				return nil, err
			}
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

func (rule *ResetParamRefRule) markSQLExecuteNumericDependent(exprs ...*plan.Expr) {
	if rule.sqlExecuteNumericDependent == nil {
		rule.sqlExecuteNumericDependent = make(map[*plan.Expr]bool)
	}
	for _, expr := range exprs {
		if expr != nil {
			rule.sqlExecuteNumericDependent[expr] = true
		}
	}
}

func (rule *ResetParamRefRule) isSQLExecuteNumericDependent(expr *plan.Expr) bool {
	return expr != nil && rule.sqlExecuteNumericDependent[expr]
}

// preparedExecutionParamType returns the current parameter type used only for
// execute-time regexp compatibility and result-domain rebinding. COM_STMT
// string packets deliberately keep a text-shaped transport type, so their
// binary domain must come from IsBinaryString rather than RuntimeType.
func (rule *ResetParamRefRule) preparedExecutionParamType(
	expr *plan.Expr,
	pos int,
) (typ types.Type, dynamic, domainless bool, err error) {
	if pos < 0 || pos >= len(rule.paramValues) {
		return types.Type{}, false, false,
			moerr.NewInternalErrorf(rule.ctx, "get prepare params error, index %d not exists", pos)
	}
	value := rule.paramValues[pos]
	if param, ok := value.(ParamValue); ok {
		if param.Value == nil {
			// A NULL execution does not erase the marker's PREPARE-time type.
			// This differs from a bare NULL literal, which has no domain. MySQL
			// therefore still rejects a prepared text marker paired with a fixed
			// binary regexp operand when the current marker value is NULL.
			preparedType := makeTypeByPlan2Expr(expr)
			return preparedType, true, preparedType.Oid == types.T_any, nil
		}
		if param.IsBinaryString ||
			(param.HasSourceType && types.StaticStringDomain(param.SourceType) == types.StringDomainBinary) ||
			(!param.HasSourceType && !param.IsBinaryProtocol && param.IsBin) {
			return types.T_varbinary.ToType(), true, false, nil
		}
		if param.HasSourceType {
			return param.SourceType, true, false, nil
		}
		if param.HasRuntimeType {
			return param.RuntimeType, true, false, nil
		}
		if param.IsBinaryProtocol {
			return types.T_text.ToType(), true, false, nil
		}
	}
	if value == nil {
		preparedType := makeTypeByPlan2Expr(expr)
		return preparedType, true, preparedType.Oid == types.T_any, nil
	}
	if pos < len(rule.params) && rule.params[pos] != nil {
		return makeTypeByPlan2Expr(rule.params[pos]), true, false, nil
	}
	return types.T_text.ToType(), true, false, nil
}

// preparedExecutionExprType evaluates only the type/domain transfer of a
// parameter-dependent expression. It does not fold values or mutate the cached
// plan. Explicit casts terminate the transfer; fixed-output functions keep
// their prepared type; domain-preserving functions are resolved from current
// child types through the normal function registry.
func (rule *ResetParamRefRule) preparedExecutionExprType(
	expr *plan.Expr,
) (typ types.Type, dynamic, domainless bool, err error) {
	if expr == nil {
		return types.Type{}, false, false, nil
	}
	preparedType := makeTypeByPlan2Expr(expr)
	if isExplicitPreparedCast(expr) {
		return preparedType, false, false, nil
	}
	if isImplicitPreparedParamCast(expr) {
		fn := expr.GetF()
		if fn != nil && len(fn.Args) > 0 {
			return rule.preparedExecutionExprType(fn.Args[0])
		}
	}

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_P:
		return rule.preparedExecutionParamType(expr, int(exprImpl.P.Pos))
	case *plan.Expr_Lit:
		if exprImpl.Lit != nil && exprImpl.Lit.Src != nil &&
			preparedExprContainsParam(exprImpl.Lit.Src) {
			return rule.preparedExecutionExprType(exprImpl.Lit.Src)
		}
		return preparedType, false, exprImpl.Lit != nil && exprImpl.Lit.GetIsnull() &&
			exprImpl.Lit.GetStringSource() == 0, nil
	case *plan.Expr_Sub:
		if exprImpl.Sub != nil {
			return rule.preparedExecutionExprType(exprImpl.Sub.Child)
		}
		return preparedType, false, false, nil
	case *plan.Expr_Col:
		if source := expr.GetPreparedNumeric().GetStringDomainSource(); source != nil {
			return rule.preparedExecutionExprType(source)
		}
		return preparedType, false, false, nil
	case *plan.Expr_F:
		if exprImpl.F == nil || exprImpl.F.Func == nil ||
			!preparedFunctionStringDomainDependsOnRuntimeParam(expr) {
			return preparedType, false, false, nil
		}
		functionName := strings.ToLower(exprImpl.F.Func.GetObjName())
		argTypes := make([]types.Type, len(exprImpl.F.Args))
		var stringDomainModes []planfunction.StringDomainCheckMode
		stringOperands := preparedRegexpCompatibilityStringOperandCount(functionName, len(exprImpl.F.Args))
		if stringOperands > 0 {
			stringDomainModes = make([]planfunction.StringDomainCheckMode, len(exprImpl.F.Args))
		}
		for i, arg := range exprImpl.F.Args {
			currentType, _, childDomainless, childErr := rule.preparedExecutionExprType(arg)
			if childErr != nil {
				return types.Type{}, false, false, childErr
			}
			if currentType.Oid == types.T_any && !childDomainless {
				currentType = makeTypeByPlan2Expr(arg)
			}
			if preparedType, ok := rule.preparedSQLExecuteTextFunctionParamType(functionName, arg); ok {
				currentType = preparedType
				childDomainless = false
			}
			argTypes[i] = currentType
			if i < stringOperands {
				if childDomainless {
					stringDomainModes[i] = planfunction.StringDomainCheckDomainless
				} else if _, directMarker := preparedParamPosition(arg); directMarker {
					stringDomainModes[i] = planfunction.StringDomainCheckParamMarker
				}
			}
		}
		var resolved planfunction.FuncGetResult
		var resolveErr error
		if stringDomainModes != nil {
			resolved, resolveErr = planfunction.GetFunctionByNameWithStringDomainCheckModes(
				rule.ctx, exprImpl.F.Func.GetObjName(), argTypes, stringDomainModes)
		} else {
			resolved, resolveErr = planfunction.GetFunctionByName(
				rule.ctx, exprImpl.F.Func.GetObjName(), argTypes)
		}
		if resolveErr != nil {
			return types.Type{}, false, false, resolveErr
		}
		// A function result keeps its declared domain even when this row is
		// NULL. Only a bare, untyped NULL literal is domainless; treating a
		// REGEXP_SUBSTR/REPLACE result as domainless would hide a mismatch in
		// an enclosing regexp call.
		return resolved.GetReturnType(), true, false, nil
	default:
		return preparedType, false, false, nil
	}
}

func (rule *ResetParamRefRule) resolvePreparedRegexpStringDomainCheckModes(
	name string,
	boundArgs, originalArgs []*plan.Expr,
) ([]planfunction.StringDomainCheckMode, error) {
	stringOperands := preparedRegexpCompatibilityStringOperandCount(name, len(boundArgs))
	if stringOperands == 0 {
		return nil, nil
	}
	modes := make([]planfunction.StringDomainCheckMode, len(boundArgs))
	for i := 0; i < stringOperands && i < len(originalArgs); i++ {
		currentType, dynamic, currentDomainless, err :=
			rule.preparedExecutionExprType(originalArgs[i])
		if err != nil {
			return nil, err
		}
		if currentDomainless {
			modes[i] = planfunction.StringDomainCheckDomainless
			continue
		}
		if _, directMarker := preparedParamPosition(originalArgs[i]); directMarker {
			modes[i] = planfunction.StringDomainCheckParamMarker
		}
		if !dynamic {
			continue
		}
		if boundArgs[i] == nil {
			continue
		}
		currentArg := *boundArgs[i]
		currentArg.Typ = makePlan2Type(&currentType)
		boundArgs[i] = &currentArg
	}
	return modes, nil
}

func (rule *ResetParamRefRule) applyExpr(e *plan.Expr) (*plan.Expr, error) {
	var err error
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_F:
		var originalTemporalExpr *Expr
		if exprImpl.F.Func != nil {
			switch strings.ToLower(exprImpl.F.Func.GetObjName()) {
			case "date_add", "date_sub", "str_to_date", "to_date", "addtime", "subtime", "timediff":
				originalTemporalExpr = DeepCopyExpr(e)
			}
		}
		functionName := ""
		if exprImpl.F.Func != nil {
			functionName = exprImpl.F.Func.GetObjName()
		}
		if strings.EqualFold(functionName, "bit_count") && len(exprImpl.F.Args) == 1 &&
			isPreparedNumericFallbackExpr(exprImpl.F.Args[0]) &&
			!rule.preparedBitCountUsesNumericRuntime(exprImpl.F.Args[0]) {
			// An unresolved BIT_COUNT marker has a binary-string default in
			// MySQL. Text/BLOB protocol values therefore keep the prepared
			// VARBINARY cast; only an actual numeric runtime domain reparses the
			// function and selects a numeric overload.
			return e, nil
		}
		isDeferredNumeric := isPreparedNumericFallbackFunctionCall(functionName, len(exprImpl.F.Args))
		var originalDeferredNumericArg *plan.Expr
		var hasPreparedDeferredNumericValue bool
		if isDeferredNumeric {
			// Keep an immutable copy of the marker-bearing argument. Recursive
			// replacement can rebuild CASE/IF/scalar-subquery nodes and discard
			// the explicit fallback metadata; the copy is the provenance source
			// for the final ABS/SIGN overload decision.
			originalDeferredNumericArg = DeepCopyExpr(exprImpl.F.Args[0])
			hasPreparedDeferredNumericValue = isPreparedNumericFallbackExpr(originalDeferredNumericArg) &&
				len(preparedNumericValueParamPositions(originalDeferredNumericArg)) > 0
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
		originalArgs := make([]*plan.Expr, len(exprImpl.F.Args))
		if exprImpl.F.Func != nil {
			originalFuncObj = exprImpl.F.Func.Obj
		}
		for i, arg := range exprImpl.F.Args {
			if arg != nil {
				originalArgTypes[i] = arg.Typ
				originalArgs[i] = DeepCopyExpr(arg)
			}
		}
		functionName = strings.ToLower(functionName)
		var temporalPeer types.Type
		hasTemporalPeer := false
		if isPreparedTemporalIntegerArithmetic(functionName) {
			temporalPeer, hasTemporalPeer = preparedTemporalNumericPeerFromArgs(originalArgs)
		}
		// A regexp check deferred at PREPARE must run at every EXECUTE even
		// when a nested dynamic result happens to resolve back to the same type.
		// Type equality alone cannot prove that all current sibling domains are
		// compatible.
		regexpDomainsDeferred := preparedRegexpStringDomainCheckModes(functionName, originalArgs) != nil
		needResetFunction := regexpDomainsDeferred
		compareArgTypes := regexpDomainsDeferred
		geometrySRIDParamPos := -1
		if isPreparedGeometrySRIDFunction(functionName) && len(originalArgs) >= 2 {
			positions := preparedGeometrySRIDParamPositionsInExpr(originalArgs[len(originalArgs)-1])
			if len(positions) == 1 {
				position := int(positions[0])
				geometrySRIDParamPos = position
				// The SRID is part of the result type metadata, so the function
				// must be rebound even when the parameter's scalar type stays
				// int64 across executions.
				needResetFunction = true
				compareArgTypes = true
			}
		}
		numericPrefixDependent := false
		sqlExecuteNumericSourceDependent := false
		sqlExecuteNumericNestedDependent := false
		commonValueSourceChanged := false
		numericComparisonFallback := false
		boundArgs := make([]*plan.Expr, len(exprImpl.F.Args))
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
		sqlExecuteNumericSourceArgs := make([]bool, len(exprImpl.F.Args))
		numericPrefixKinds := make([]types.StringConversionKind, len(exprImpl.F.Args))
		numericPrefixListArgs := make([][]bool, len(exprImpl.F.Args))
		numericPrefixListKinds := make([][]types.StringConversionKind, len(exprImpl.F.Args))
		var sharedControlReturnType *plan.Type
		for i, arg := range exprImpl.F.Args {
			originalArgTyp := plan.Type{}
			originalArgFuncObj := int64(0)
			if arg != nil {
				originalArgTyp = arg.Typ
				originalArgFuncObj = preparedExprFunctionObj(arg)
			}
			implicitParamCast := isImplicitPreparedParamCast(arg)
			provisionalCommonValueCast := false
			commonValueResultArg := (isPreparedCommonValueFunction(functionName) || functionName == "if" ||
				functionName == "ifnull" || functionName == "case") &&
				preparedSQLExecuteNumericResultValueArg(functionName, i, len(exprImpl.F.Args))
			if commonValueResultArg &&
				originalArgs[i] != nil && originalArgs[i].GetPreparedNumeric().GetProvisionalResultCast() {
				if cast := arg.GetF(); cast != nil && cast.Func != nil &&
					cast.Func.GetObjName() == "cast" && len(cast.Args) == 2 && cast.Args[0] != nil &&
					!cast.GetSyntaxExplicitCast() && cast.Args[0].GetCol() == nil {
					// A relational source is refreshed after its producer. Keep its
					// annotated envelope until that pass can resolve the column.
					_, overload := planfunction.DecodeOverloadID(cast.Func.GetObj())
					provisionalCommonValueCast = overload == 0
				}
			}
			bitwiseParamCast := isPreparedBitwiseOperator(functionName) &&
				isPreparedBitwiseParamCast(arg)
			paramPos, hasParamPos := preparedParamPosition(arg)
			bareCommonValueNull := commonValueResultArg && hasParamPos &&
				arg.GetP() != nil && rule.preparedRuntimeParamIsNull(paramPos)
			var preparedBitwiseSource *plan.Expr
			var hasPreparedBitwiseSource bool
			var nestedPreparedBitwiseSource *plan.Expr
			hasPreparedNumericValue := hasTemporalPeer &&
				len(preparedNumericValueParamPositions(originalArgs[i])) > 0
			if hasParamPos && isPreparedBitwiseAggregate(functionName) && i == 0 &&
				isBitwiseAggregatePrivateCast(arg) {
				preparedBitwiseSource, hasPreparedBitwiseSource, err =
					rule.preparedRuntimeSourceExpr(paramPos, false)
				if err != nil {
					return nil, err
				}
			} else if !hasParamPos && isPreparedBitwiseAggregate(functionName) && i == 0 &&
				isBitwiseAggregatePrivateCast(arg) && len(preparedNumericValueParamPositions(arg.GetF().Args[0])) > 0 {
				// The aggregate's private conversion owns the final input contract.
				// Rebinding it as an ordinary CAST while a nested expression (for
				// example COALESCE(?, 0)) changes type can replace CAST4 with CAST0
				// and silently change tie rounding. Rebind the nested source first,
				// then let the aggregate binder select the execute-time conversion.
				nestedPreparedBitwiseSource = arg.GetF().Args[0]
			}
			if !hasParamPos && preparedFunctionArgUsesSQLExecuteNumericSource(
				e, functionName, i, len(exprImpl.F.Args)) {
				paramPos, hasParamPos = preparedResultParamPosition(arg, functionName)
			}
			// The prepare-time cast around a marker is only an overload hint.
			// FIELD and the variadic extrema compare the complete runtime tuple;
			// a SQL user variable's source domain must reach that comparison before
			// any numeric-prefix or provisional cast can reinterpret it.
			// A bare marker already typed by its consumer (for example the uint64
			// LIMIT inside a generated vector-index overfetch budget) is not a
			// provisional comparison operand; keep that consumer's domain.
			variadicSource := false
			if hasParamPos && !isExplicitPreparedCast(arg) &&
				(implicitParamCast || types.T(arg.Typ.Id).IsMySQLString() || types.T(arg.Typ.Id) == types.T_any) &&
				(functionName == "field" || functionName == "greatest" || functionName == "least") &&
				paramPos < len(rule.paramValues) {
				if param, ok := rule.paramValues[paramPos].(ParamValue); ok &&
					((param.Value == nil && (param.IsBinaryProtocol || param.HasSourceType || param.HasRuntimeType)) ||
						(!param.IsBinaryProtocol && param.HasSourceType && param.SourceType.Oid != types.T_any) ||
						(functionName == "field" && param.IsBinaryProtocol && param.HasRuntimeType &&
							param.RuntimeType.Oid != types.T_any && preparedNumericCommonOperandType(param.RuntimeType.Oid))) {
					variadicSource = true
				}
			}
			var preparedInetNtoaSource *plan.Expr
			var hasPreparedInetNtoaSource bool
			if hasParamPos && functionName == "inet_ntoa" {
				preparedInetNtoaSource, hasPreparedInetNtoaSource, err =
					rule.preparedInetNtoaSourceExpr(paramPos)
				if err != nil {
					return nil, err
				}
			}
			var preparedCharSource *plan.Expr
			var hasPreparedCharSource bool
			if hasParamPos && strings.EqualFold(functionName, "char") {
				preparedCharSource, hasPreparedCharSource, err = rule.preparedCharSourceExpr(paramPos)
				if err != nil {
					return nil, err
				}
			}
			// A non-numeric SQL string variable has no approximate numeric source
			// expression. CHAR still owns the SQL EXECUTE argument domain: it must
			// receive the original string so builtInChar can apply MySQL's
			// non-numeric-prefix -> 0 rule, instead of evaluating the provisional
			// prepare-time TEXT-to-INT cast (which would return an error).
			charStringSourceFallback := hasParamPos &&
				strings.EqualFold(functionName, "char") &&
				paramPos >= 0 && paramPos < len(rule.sqlExecuteNumericParams) &&
				rule.sqlExecuteNumericParams[paramPos] == nil &&
				paramPos < len(rule.sqlExecuteStringBackedParams) &&
				rule.sqlExecuteStringBackedParams[paramPos] &&
				paramPos < len(rule.params) && rule.params[paramPos] != nil
			useSQLExecuteNumericSource := hasParamPos &&
				preparedFunctionArgUsesSQLExecuteNumericSource(
					e, functionName, i, len(exprImpl.F.Args)) &&
				(hasPreparedBitwiseSource || hasPreparedCharSource || hasPreparedInetNtoaSource || charStringSourceFallback || (paramPos >= 0 &&
					paramPos < len(rule.sqlExecuteNumericParams) &&
					rule.sqlExecuteNumericParams[paramPos] != nil)) &&
				preparedSQLExecuteNumericSourceOwnsResultDomain(
					functionName, paramPos, rule.sqlExecuteStringBackedParams)
			sharedControlParam := false
			if paramPos >= 0 && preparedSQLExecuteNumericResultValueArg(functionName, i, len(exprImpl.F.Args)) {
				for j, sibling := range originalArgs {
					if !preparedSQLExecuteNumericResultValueArg(functionName, j, len(exprImpl.F.Args)) &&
						exprContainsPreparedPosition(sibling, paramPos) {
						sharedControlParam = true
						break
					}
				}
			}
			prefixEligibleOccurrence := !(sharedControlParam &&
				paramPos < len(rule.sqlExecuteStringBackedParams) && rule.sqlExecuteStringBackedParams[paramPos])
			// Common results preserve concrete SQL strings, including typed NULL.
			// Prefix eligibility belongs to transport-only text and numeric consumers.
			if hasParamPos && isPreparedCommonValueFunction(functionName) && paramPos < len(rule.paramValues) {
				if param, ok := rule.paramValues[paramPos].(ParamValue); ok && !param.IsBinaryProtocol &&
					param.HasSourceType && param.SourceType.Oid.IsMySQLString() {
					prefixEligibleOccurrence = false
				}
			}
			if hasParamPos && rule.numericPrefixParamPositions[paramPos] && prefixEligibleOccurrence && !variadicSource {
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
			if implicitParamCast || bitwiseParamCast {
				// The prepare-time parameter may have been wrapped in a provisional
				// cast selected by overload resolution. The execute-time value must
				// participate in resolving the outer function again.
				needResetFunction = true
			}
			var rewrittenArg *plan.Expr
			if nestedPreparedBitwiseSource != nil {
				var applyErr error
				rewrittenArg, applyErr = rule.ApplyExpr(DeepCopyExpr(nestedPreparedBitwiseSource))
				if applyErr != nil {
					return nil, applyErr
				}
				needResetFunction = true
				compareArgTypes = true
				rule.specialized = true
			} else if hasParamPos && !isExplicitPreparedCast(arg) &&
				((functionName == "json_arrayagg" && i == 0) ||
					(functionName == "json_objectagg" && i == 1)) {
				// JSON aggregate values preserve the source SQL domain. The
				// prepared TEXT marker (and its implicit cast) is only a
				// placeholder, not a request to stringify DECIMAL or JSON.
				source, known, sourceErr := rule.preparedRuntimeSourceExpr(paramPos, true)
				if sourceErr != nil {
					return nil, sourceErr
				}
				if known {
					rewrittenArg = source
					needResetFunction = true
					compareArgTypes = true
					rule.specialized = true
				} else {
					rewrittenArg, err = rule.ApplyExpr(arg)
					if err != nil {
						return nil, err
					}
				}
			} else if variadicSource {
				var sourceOK bool
				rewrittenArg, sourceOK, err = rule.preparedRuntimeSourceExpr(paramPos, false)
				if err != nil {
					return nil, err
				}
				if !sourceOK {
					return nil, moerr.NewInternalErrorNoCtx("missing prepared variadic source type")
				}
				needResetFunction = true
				compareArgTypes = true
				rule.specialized = true
				if (functionName == "greatest" || functionName == "least" || functionName == "field") &&
					preparedNumericCommonOperandType(types.T(rewrittenArg.Typ.Id)) &&
					types.T(rewrittenArg.Typ.Id) != types.T_any {
					// A numeric runtime marker also invalidates a peer literal's
					// prepare-time TEXT envelope. Restore that peer only when the
					// complete tuple has no actual string operand.
					sqlExecuteNumericSourceDependent = true
					sqlExecuteNumericSourceArgs[i] = true
				}
			} else if useSQLExecuteNumericSource {
				sqlExecuteNumericSourceDependent = true
				sqlExecuteNumericSourceArgs[i] = true
				// The prepare-time implicit cast is provisional. Materialize the
				// SQL user variable's current source domain before descending into
				// that cast; evaluating it first can reject a valid DECIMAL value
				// using the overload selected for the initial TEXT marker.
				source := preparedBitwiseSource
				if source == nil {
					source = preparedCharSource
				}
				if source == nil {
					source = preparedInetNtoaSource
				}
				if source == nil && paramPos >= 0 && paramPos < len(rule.sqlExecuteNumericParams) {
					source = rule.sqlExecuteNumericParams[paramPos]
				}
				if source == nil {
					// Only CHAR takes this branch. Keep the original TEXT/BINARY
					// source so its function-specific prefix parser can produce 0
					// for an invalid numeric string without raising a cast error.
					source = DeepCopyExpr(rule.params[paramPos])
				}
				if functionName == "hex" && rule.sqlExecuteStringBackedParams[paramPos] {
					// The shared numeric-source builder wraps a numeric-prefix SQL
					// string in an explicit FLOAT64 cast. HEX instead owns a string
					// operand as bytes, so restore the typed source below that wrapper.
					if cast := source.GetF(); cast != nil && cast.GetFunc().GetObjName() == "cast" &&
						len(cast.GetArgs()) > 0 {
						source = cast.GetArgs()[0]
					}
				}
				rewrittenArg = DeepCopyExpr(source)
			} else if provisionalCommonValueCast || bareCommonValueNull {
				commonValueSourceChanged = true
				// The outer cast was selected while the result operand was an
				// unresolved TEXT marker. Rebind its source for this EXECUTE so a
				// nested numeric function can supply its actual result domain.
				source := arg
				if provisionalCommonValueCast {
					source = arg.GetF().Args[0]
				}
				if marker := source.GetP(); marker != nil {
					var known bool
					rewrittenArg, known, err = rule.preparedRuntimeSourceExpr(int(marker.Pos), false)
					if err != nil {
						return nil, err
					}
					if !known {
						rewrittenArg, err = rule.ApplyExpr(source)
					}
				} else {
					rewrittenArg, err = rule.ApplyExpr(source)
				}
				if err != nil {
					return nil, err
				}
				needResetFunction = true
				compareArgTypes = true
				rule.specialized = true
			} else {
				var applyErr error
				disablePrefix := sharedControlParam && paramPos >= 0 &&
					paramPos < len(rule.numericPrefixParamPositions) && rule.numericPrefixParamPositions[paramPos] &&
					paramPos < len(rule.sqlExecuteStringBackedParams) && rule.sqlExecuteStringBackedParams[paramPos]
				if disablePrefix && paramPos < len(rule.params) && paramPos < len(rule.paramValues) &&
					rule.params[paramPos] != nil {
					// NULLIF-style rewrites share one marker between comparison and
					// return roles. The comparison consumes the numeric prefix, while
					// the return occurrence must materialize the original SQL value.
					rewrittenArg = DeepCopyExpr(rule.params[paramPos])
					rewrittenArg.Typ = arg.Typ
					returnPlanType := preparedRuntimeResultOccurrenceType(
						rule.paramValues[paramPos], arg.Typ)
					rewrittenArg.Typ = returnPlanType
					sharedControlReturnType = &returnPlanType
				} else {
					rewrittenArg, applyErr = rule.ApplyExpr(arg)
				}
				err = applyErr
				if err != nil {
					return nil, err
				}
			}
			if preparedType, textContext := rule.preparedSQLExecuteTextFunctionParamType(
				functionName, originalArgs[i]); textContext {
				textArg, known, textErr := rule.preparedSQLExecuteTextFunctionArg(paramPos, preparedType)
				if textErr != nil {
					return nil, textErr
				}
				if known {
					rewrittenArg = textArg
					needResetFunction = true
					compareArgTypes = true
					rule.specialized = true
				}
			}
			if geometrySRIDParamPos >= 0 && i == len(exprImpl.F.Args)-1 &&
				hasParamPos && paramPos == geometrySRIDParamPos {
				sourceIsNull := len(boundArgs) > 0 && geometrySRIDSourceIsStaticNull(boundArgs[0])
				geometryArg, known, geometryErr := rule.preparedGeometrySRIDParamExpr(paramPos, sourceIsNull)
				if geometryErr != nil {
					return nil, geometryErr
				}
				if known {
					// Do not retain the provisional cast around the marker. The
					// normalized int64 literal makes the geometry binder apply the
					// current SRID to Width, including NULL -> undefined metadata.
					rewrittenArg = geometryArg
					needResetFunction = true
					compareArgTypes = true
					rule.specialized = true
				}
			}
			// ST_DISTANCE has a legacy third-argument SRID overload and a
			// MySQL-compatible length-unit overload. A marker is provisionally
			// wrapped as BIGINT during PREPARE, so a string-backed EXECUTE value
			// must be rebound from the marker's text expression rather than from
			// that stale implicit cast. Numeric SQL variables keep the explicit
			// SRID overload and its existing conversion contract.
			if hasParamPos && isPreparedSpatialUnitFunction(functionName, i) {
				if preparedParamValueUsesStringDomain(rule.paramValues[paramPos]) {
					if cast := rewrittenArg.GetF(); cast != nil &&
						cast.Func != nil && cast.Func.GetObjName() == "cast" &&
						isImplicitPreparedParamCast(rewrittenArg) && len(cast.Args) > 0 {
						rewrittenArg = cast.Args[0]
					}
					// TEXT-to-VARCHAR is otherwise costed as a conversion to the
					// legacy BIGINT overload. Normalize the runtime string marker to
					// VARCHAR so the unit overload is selected unambiguously while
					// preserving the original string payload.
					stringType := types.T_varchar.ToType()
					rewrittenArg.Typ = makePlan2Type(&stringType)
				} else if param, ok := rule.paramValues[paramPos].(ParamValue); ok &&
					param.Value != nil && param.PrepareParamKind != vector.PrepareParamNone {
					// SQL EXECUTE still materializes the value through a text
					// transport literal. Recreate the assignment-time numeric
					// literal before rebinding, otherwise the already-selected unit
					// overload would see the textual spelling of an SRID (for
					// example, "4326") and fail as an unknown length unit.
					numeric, numericOK, numericErr := rule.typedRuntimeParamExpr(paramPos)
					if numericErr != nil {
						return nil, numericErr
					}
					if numericOK {
						rewrittenArg = numeric
					}
				}
				boundArgs[i] = rewrittenArg
				needResetFunction = true
				compareArgTypes = true
			}
			exprImpl.F.Args[i] = rewrittenArg
			boundArgs[i] = rewrittenArg
			if useSQLExecuteNumericSource {
				needResetFunction = true
				compareArgTypes = true
				// The execute-time source may change only an argument literal while
				// the selected overload and result type remain stable (for example,
				// the precision argument of `ROUND(decimal, ?)`).  The copied plan still
				// contains a different value and must be installed for this execute;
				// functionBindingChanged cannot observe that value-only change.
				rule.specialized = true
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
			if !implicitParamCast && hasPreparedNumericValue {
				coerced, coercedChanged, coercedErr := rule.coercePreparedTemporalIntegerOperand(
					originalArgs[i], rewrittenArg, temporalPeer, hasPreparedNumericValue)
				if coercedErr != nil {
					return nil, coercedErr
				}
				if coercedChanged {
					rewrittenArg = coerced
					exprImpl.F.Args[i] = rewrittenArg
					boundArgs[i] = rewrittenArg
					needResetFunction = true
					compareArgTypes = true
					rule.specialized = true
				}
			}
			if rule.isNumericPrefixDependent(rewrittenArg) {
				if !preparedSQLExecuteNumericResultConsumer(functionName) ||
					preparedSQLExecuteNumericResultValueArg(functionName, i, len(exprImpl.F.Args)) {
					numericPrefixDependent = true
				}
				if unwrapped, changed := unwrapNumericPrefixDependentImplicitCast(rewrittenArg); changed {
					boundArgs[i] = unwrapped
					needResetFunction = true
					compareArgTypes = true
				}
			}
			if rule.isSQLExecuteNumericDependent(rewrittenArg) &&
				((functionName == "cast" && !isExplicitPreparedCast(e)) ||
					isPreparedNumericComparisonContext(functionName) ||
					isPreparedCommonValueFunction(functionName) || functionName == "field" ||
					preparedFunctionArgUsesSQLExecuteNumericSource(e, functionName, i, len(exprImpl.F.Args))) {
				sqlExecuteNumericSourceDependent = true
				sqlExecuteNumericNestedDependent = true
				sqlExecuteNumericSourceArgs[i] = true
			}
			if preparedExprBindingChanged(originalArgTyp, originalArgFuncObj, rewrittenArg) {
				// A nested typed function may have changed overload/result domain
				// after its parameter was rebound.  The enclosing function was
				// bound against the old child domain and must be resolved again.
				needResetFunction = true
				compareArgTypes = true
			}
			if bitwiseParamCast {
				if unwrapped, ok := unwrapImplicitPreparedBinaryParamCast(rewrittenArg); ok {
					boundArgs[i] = unwrapped
					compareArgTypes = true
				}
			}
			if implicitParamCast {
				// Numeric arithmetic uses the prepare-time cast only to make a
				// TEXT marker resolvable. Once an execute-time numeric source is
				// available, rebind from that source instead of narrowing it to the
				// provisional decimal hint used for a TIME operand. Explicit CAST
				// expressions do not enter this branch because implicitParamCast
				// excludes them.
				if isNumericContextFunction(functionName) && hasParamPos &&
					rule.hasNumericRuntimeParam(paramPos) {
					typed, typedOK, typedErr := rule.typedRuntimeParamExpr(paramPos)
					if typedErr != nil {
						return nil, typedErr
					}
					if typedOK {
						if hasPreparedNumericValue {
							coerced, coercedChanged, coercedErr :=
								rule.coercePreparedTemporalIntegerOperand(
									originalArgs[i], typed, temporalPeer, hasPreparedNumericValue)
							if coercedErr != nil {
								return nil, coercedErr
							}
							if coercedChanged {
								typed = coerced
							}
						}
						boundArgs[i] = typed
						needResetFunction = true
						compareArgTypes = true
						rule.specialized = true
						continue
					}
				}
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
		variadicStringBoundary := false
		if functionName == "greatest" || functionName == "least" {
			for i, arg := range boundArgs {
				if arg == nil || !types.T(arg.Typ.Id).IsMySQLString() || sqlExecuteNumericSourceArgs[i] {
					continue
				}
				if _, numericPeer := provisionalNumericPeerSource(originalArgs[i]); !numericPeer {
					variadicStringBoundary = true
					break
				}
			}
		}
		// A concrete string/typed NULL also invalidates the provisional peer
		// envelope. Resolve the whole original tuple, including result metadata.
		sqlExecuteNumericPeerDependent := commonValueSourceChanged || !variadicStringBoundary && (sqlExecuteNumericNestedDependent ||
			(sqlExecuteNumericSourceDependent &&
				(functionName == "/" || functionName == "field" || preparedSQLExecuteNumericResultConsumer(functionName))))
		if numericPrefixDependent || sqlExecuteNumericPeerDependent {
			var sqlExecuteResultType plan.Type
			if sqlExecuteNumericPeerDependent {
				for i, sourceArg := range boundArgs {
					if sqlExecuteNumericSourceArgs[i] && sourceArg != nil {
						sqlExecuteResultType = sourceArg.Typ
						break
					}
				}
			}
			for i, arg := range boundArgs {
				// A nested runtime common-type result invalidates provisional
				// prepare-time casts on every sibling, not only on the dependent
				// child. Rebind the enclosing consumer from numeric source domains
				// so a DECIMAL peer is not left behind a FLOAT cast selected while
				// the parameter marker was still TEXT.
				candidate := arg
				if sqlExecuteNumericPeerDependent && !sqlExecuteNumericSourceArgs[i] && originalArgs[i] != nil {
					candidate = originalArgs[i]
					if source, ok := provisionalNumericPeerSource(candidate); ok {
						boundArgs[i] = source
						candidate = source
						needResetFunction = true
						compareArgTypes = true
					} else if literal := candidate.GetLit(); literal != nil &&
						candidate.GetPreparedNumeric().GetProvisionalResultPeer() &&
						types.T(candidate.Typ.Id).IsMySQLString() && preparedNumericCommonOperandType(types.T(sqlExecuteResultType.Id)) {
						peerType := sqlExecuteResultType
						metadata := candidate.GetPreparedNumeric()
						if metadata.GetProvisionalResultPeerTypeId() != 0 {
							peerType.Id = metadata.GetProvisionalResultPeerTypeId()
							peerType.Width = metadata.GetProvisionalResultPeerWidth()
							peerType.Scale = metadata.GetProvisionalResultPeerScale()
						}
						numericPeer, castErr := appendCastBeforeExpr(rule.ctx, DeepCopyExpr(candidate), peerType)
						if castErr != nil {
							return nil, castErr
						}
						boundArgs[i] = numericPeer
						candidate = numericPeer
						needResetFunction = true
						compareArgTypes = true
					}
				}
				unwrapped, changed := unwrapNumericPrefixDependentImplicitCast(candidate)
				if changed {
					if sqlExecuteNumericPeerDependent && !sqlExecuteNumericSourceArgs[i] {
						var applyErr error
						unwrapped, applyErr = rule.ApplyExpr(unwrapped)
						if applyErr != nil {
							return nil, applyErr
						}
					}
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

		if isDeferredNumeric && hasPreparedDeferredNumericValue {
			// A flattened scalar subquery leaves the ABS/SIGN argument as a column
			// reference.  Its inner projection has already been rebound above;
			// refresh the reference type and rebind the function, but keep the reference so
			// empty/multi-row scalar-subquery semantics remain intact.
			if originalDeferredNumericArg.GetPreparedNumeric().GetFallbackSource() {
				refreshed, changed, refreshErr := rule.refreshPreparedNumericSource(boundArgs[0])
				if refreshErr != nil {
					return nil, refreshErr
				}
				if changed {
					reboundArgs := []*Expr{refreshed}
					if functionName == "elt" {
						reboundArgs = append([]*Expr(nil), boundArgs...)
						reboundArgs[0] = refreshed
					}
					rewritten, bindErr := BindFuncExprImplByPlanExpr(
						rule.ctx, functionName, reboundArgs)
					if bindErr != nil {
						return nil, bindErr
					}
					preserveReboundFunctionMetadata(exprImpl.F, rewritten.GetF())
					rule.specialized = true
					if makeTypeByPlan2Expr(rewritten).IsNumeric() {
						rule.markSQLExecuteNumericDependent(e, rewritten)
					}
					return rewritten, nil
				}
			}
			source, sourceOK := preparedNumericFallbackSource(originalDeferredNumericArg)
			positions := preparedNumericValueParamPositions(originalDeferredNumericArg)
			if sourceOK && len(positions) > 0 {
				rebound, changed, reboundErr := rule.rebindPreparedNumericExprWithBound(
					source, boundArgs[0], positions)
				if reboundErr != nil {
					return nil, reboundErr
				}
				if changed {
					reboundArgs := []*Expr{rebound}
					if functionName == "elt" {
						reboundArgs = append([]*Expr(nil), boundArgs...)
						reboundArgs[0] = rebound
					}
					rewritten, bindErr := BindFuncExprImplByPlanExpr(
						rule.ctx, functionName, reboundArgs)
					if bindErr != nil {
						return nil, bindErr
					}
					preserveReboundFunctionMetadata(exprImpl.F, rewritten.GetF())
					rule.specialized = true
					if makeTypeByPlan2Expr(rewritten).IsNumeric() {
						rule.markSQLExecuteNumericDependent(e, rewritten)
					}
					return rewritten, nil
				}
			}
		}

		if sharedControlReturnType != nil {
			for i, resultArg := range boundArgs {
				if preparedSQLExecuteNumericResultValueArg(functionName, i, len(boundArgs)) &&
					resultArg != nil && resultArg.GetLit().GetIsnull() {
					resultArg.Typ = *sharedControlReturnType
				}
			}
		}

		// reset function
		if needResetFunction {
			restorePreparedIntegerArithmeticOperands(functionName, boundArgs)
			stringDomainModes, resolveErr := rule.resolvePreparedRegexpStringDomainCheckModes(
				functionName, boundArgs, originalArgs)
			if resolveErr != nil {
				return nil, resolveErr
			}
			var rewritten *Expr
			if isExplicitPreparedCast(e) {
				rewritten, err = rebindExplicitPreparedCast(rule.ctx, e, boundArgs)
				rule.specialized = true
			} else {
				rewritten, err = bindPreparedFuncExprImplByPlanExpr(
					rule.ctx,
					originalTemporalExpr,
					exprImpl.F.Func.GetObjName(),
					boundArgs,
					stringDomainModes,
				)
			}
			if err != nil {
				return nil, err
			}
			preserveReboundFunctionMetadata(exprImpl.F, rewritten.GetF())
			if restored, restoredChanged, restoreErr := rule.restorePreparedTemporalNullEnvelope(
				functionName, originalTyp, originalArgs, rewritten,
			); restoreErr != nil {
				return nil, restoreErr
			} else if restoredChanged {
				rewritten = restored
				rule.specialized = true
			}
			if functionBindingChanged(originalTyp, originalFuncObj, originalArgTypes, rewritten, compareArgTypes) {
				rule.specialized = true
			}
			if numericPrefixDependent && !isExplicitPreparedCast(e) {
				rule.markNumericPrefixDependent(e, rewritten)
			}
			if (sqlExecuteNumericSourceDependent || commonValueSourceChanged) && !isExplicitPreparedCast(e) {
				rule.markSQLExecuteNumericDependent(e, rewritten)
			}
			return rewritten, nil
		}
		if restored, restoredChanged, restoreErr := rule.restorePreparedTemporalNullEnvelope(
			functionName, originalTyp, originalArgs, e,
		); restoreErr != nil {
			return nil, restoreErr
		} else if restoredChanged {
			rule.specialized = true
			return restored, nil
		}
		if numericPrefixDependent && !isExplicitPreparedCast(e) {
			rule.markNumericPrefixDependent(e)
		}
		if (sqlExecuteNumericSourceDependent || commonValueSourceChanged) && !isExplicitPreparedCast(e) {
			rule.markSQLExecuteNumericDependent(e)
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
		if param == nil {
			return e, nil
		}
		if rule.numericComparisonTextParamPositions[position] &&
			param.Typ.Id == int32(types.T_text) && param.GetLit() != nil {
			runtimeType := preparedNumericComparisonTextType()
			return makePlan2CastExpr(rule.ctx, param, makePlan2Type(&runtimeType))
		}
		typ := e.Typ
		// Most prepared parameters are intentionally replaced as TEXT to retain
		// the historical SQL-EXECUTE behavior.  Binary protocol executions can
		// carry an explicit numeric domain, represented by a non-text type on the
		// replacement expression; preserve that domain for direct projections and
		// for the function rebinding performed by the parent expression.
		if param.Typ.Id != int32(types.T_text) {
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

func isPreparedSpatialUnitFunction(name string, argIndex int) bool {
	if argIndex != 2 {
		return false
	}
	switch name {
	case "st_distance", "st_frechetdistance", "st_hausdorffdistance":
		return true
	default:
		return false
	}
}

func preparedParamValueUsesStringDomain(value any) bool {
	param, ok := value.(ParamValue)
	if !ok || param.Value == nil {
		return false
	}
	// SQL EXECUTE uses the marker's string type as a transport placeholder. Its
	// assignment-time PrepareParamKind is the stronger signal: a numeric user
	// variable must keep the legacy SRID overload even when the marker itself
	// was provisionally typed VARCHAR during PREPARE.
	if !param.IsBinaryProtocol && param.PrepareParamKind != vector.PrepareParamNone {
		return false
	}
	// SQL EXECUTE transports user variables through a TEXT vector. When the
	// assignment was numeric, the transport type remains TEXT but the prepared
	// parameter kind is the authoritative numeric domain. Do not reinterpret
	// that neutral transport type as a length-unit string.
	if param.HasRuntimeType {
		return isStringBackedType(param.RuntimeType)
	}
	if param.HasSourceType {
		if param.SourceType.Oid == types.T_text && param.PrepareParamKind != vector.PrepareParamNone {
			return false
		}
		return isStringBackedType(param.SourceType)
	}
	// An explicit non-string source/runtime type is authoritative even when
	// PrepareParamKind is left at None (the SQL EXECUTE path does not attach a
	// protocol category). Do not mistake a numeric SRID marker for a unit name.
	if param.HasSourceType || param.HasRuntimeType {
		return false
	}
	if param.IsBinaryProtocol {
		return param.PrepareParamKind == vector.PrepareParamNone
	}
	return param.PrepareParamKind == vector.PrepareParamNone
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
	if strings.Trim(value, " \t\n\v\f\r") != prefix {
		// A numeric prefix with trailing non-space text must retain the engine's
		// ordinary DOUBLE conversion so it emits the existing truncation warning.
		return nil, false, nil
	}
	integerText, ok := preparedBoundedExactIntegerPrefix(prefix)
	if !ok {
		return nil, false, nil
	}
	targetType := makeTypeByPlan2Type(target)
	bits := 0
	switch targetType.Oid {
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
	if strings.HasPrefix(integerText, "-") {
		return nil, false, nil
	}
	if _, err := strconv.ParseUint(integerText, 10, bits); err != nil {
		return nil, false, nil
	}
	expr, err := preparedRuntimeParamExpr(ctx, integerText, false, targetType)
	return expr, err == nil, err
}

// preparedBoundedExactIntegerPrefix normalizes a decimal/scientific prefix to
// an integer string of at most 20 digits without constructing an arbitrary-
// precision number. The scan is linear in the supplied text and allocates only
// the bounded result, so inputs such as 1e1000000 cannot amplify memory.
func preparedBoundedExactIntegerPrefix(prefix string) (string, bool) {
	if prefix == "" {
		return "", false
	}
	i := 0
	negative := false
	if prefix[i] == '+' || prefix[i] == '-' {
		negative = prefix[i] == '-'
		i++
		if i == len(prefix) {
			return "", false
		}
	}
	mantissaEnd := len(prefix)
	for j := i; j < len(prefix); j++ {
		if prefix[j] == 'e' || prefix[j] == 'E' {
			mantissaEnd = j
			break
		}
	}
	digitCount, fractionalDigits := 0, 0
	firstNonZero, lastNonZero := -1, -1
	seenDot := false
	for j := i; j < mantissaEnd; j++ {
		switch c := prefix[j]; {
		case c >= '0' && c <= '9':
			if c != '0' {
				if firstNonZero < 0 {
					firstNonZero = digitCount
				}
				lastNonZero = digitCount
			}
			digitCount++
			if seenDot {
				fractionalDigits++
			}
		case c == '.' && !seenDot:
			seenDot = true
		default:
			return "", false
		}
	}
	if digitCount == 0 {
		return "", false
	}
	if firstNonZero < 0 {
		return "0", true
	}

	exponent := 0
	if mantissaEnd < len(prefix) {
		j := mantissaEnd + 1
		exponentNegative := false
		if j < len(prefix) && (prefix[j] == '+' || prefix[j] == '-') {
			exponentNegative = prefix[j] == '-'
			j++
		}
		if j == len(prefix) {
			return "", false
		}
		capValue := len(prefix) + 64
		for ; j < len(prefix); j++ {
			c := prefix[j]
			if c < '0' || c > '9' {
				return "", false
			}
			if exponent < capValue {
				digit := int(c - '0')
				if exponent > (capValue-digit)/10 {
					exponent = capValue
				} else {
					exponent = exponent*10 + digit
				}
			}
		}
		if exponentNegative {
			exponent = -exponent
		}
	}

	scale := fractionalDigits - exponent
	endDigit := digitCount
	appendZeros := 0
	if scale > 0 {
		if scale > digitCount-1-lastNonZero {
			return "", false
		}
		endDigit -= scale
	} else if scale < 0 {
		appendZeros = -scale
	}
	resultDigits := endDigit - firstNonZero + appendZeros
	if resultDigits <= 0 || resultDigits > 20 {
		return "", false
	}

	var normalized strings.Builder
	normalized.Grow(resultDigits + 1)
	if negative {
		normalized.WriteByte('-')
	}
	digitIndex := 0
	for j := i; j < mantissaEnd && digitIndex < endDigit; j++ {
		c := prefix[j]
		if c == '.' {
			continue
		}
		if digitIndex >= firstNonZero {
			normalized.WriteByte(c)
		}
		digitIndex++
	}
	for range appendZeros {
		normalized.WriteByte('0')
	}
	return normalized.String(), true
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
		if overload != 0 || fn.GetSyntaxExplicitCast() {
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
	if source, ok := provisionalExactNumericSource(current); ok {
		return source, true
	}
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
	// BIT_AND/BIT_OR/BIT_XOR insert CAST4 while binding a bare parameter.
	// This is an implementation cast for the aggregate's input contract, not
	// a SQL-authored cast that fixes the parameter domain. Keep the source type
	// visible to prepared-plan runtime specialization.
	if isBitwiseAggregatePrivateCast(expr) {
		return false
	}
	_, overload := planfunction.DecodeOverloadID(fn.Func.GetObj())
	return overload != 0 || fn.GetSyntaxExplicitCast()
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

func preparedSQLExecuteNumericResultConsumer(name string) bool {
	return preparedNumericResultPolymorphicFunction(name) || strings.EqualFold(name, "char") ||
		strings.EqualFold(name, "inet_ntoa") || isPreparedBitwiseAggregate(name)
}

func preparedSQLExecuteNumericResultValueArg(name string, argIndex, argCount int) bool {
	if !preparedSQLExecuteNumericResultConsumer(name) {
		return false
	}
	name = canonicalPreparedResultFunctionName(name)
	switch name {
	case "case", "if", "coalesce", "ifnull", "nullif":
		return numericFunctionArgKeepsContext(name, argIndex, argCount)
	case "sum", "avg", "min", "max", "any_value":
		return argCount == 1 && argIndex == 0
	case "bit_and", "bit_or", "bit_xor":
		return argCount == 1 && argIndex == 0
	case "first_value", "last_value", "lag", "lead", "nth_value", "max_by", "max_by_non_null":
		return argCount > 0 && argIndex == 0
	case "inet_ntoa":
		return argCount == 1 && argIndex == 0
	default: // greatest, least
		return true
	}
}

func preparedRuntimeResultOccurrenceType(value any, fallback plan.Type) plan.Type {
	param, ok := value.(ParamValue)
	if !ok {
		return fallback
	}
	var source types.Type
	switch {
	case param.HasSourceType:
		source = param.SourceType
	case param.HasRuntimeType:
		source = param.RuntimeType
	default:
		return fallback
	}
	switch source.Oid {
	case types.T_char, types.T_varchar, types.T_text, types.T_binary, types.T_varbinary, types.T_blob,
		types.T_date, types.T_time, types.T_datetime, types.T_timestamp, types.T_json:
		return makePlan2Type(&source)
	default:
		return fallback
	}
}

func preparedSQLExecuteNumericSourceOwnsResultDomain(
	name string,
	paramPos int,
	stringBacked []bool,
) bool {
	if isPreparedBitwiseAggregate(name) {
		// The binder-inserted private cast is removed before rebinding so the
		// execute-time source can select numeric-prefix or binary-string semantics.
		return true
	}
	// CHAR's prepared marker is deliberately numeric even when SQL EXECUTE
	// supplies a string-backed user variable. This is different from common
	// value consumers, where a string source remains the comparison/result
	// domain. preparedSQLExecuteNumericParamExpr performs the required MySQL
	// numeric-prefix conversion before CHAR is rebound.
	if strings.EqualFold(name, "char") {
		return true
	}
	if !preparedSQLExecuteNumericResultConsumer(name) {
		return true
	}
	return paramPos >= 0 && paramPos < len(stringBacked) && !stringBacked[paramPos]
}

func isPreparedBitwiseAggregate(name string) bool {
	switch strings.ToLower(name) {
	case "bit_and", "bit_or", "bit_xor":
		return true
	default:
		return false
	}
}

func isInetNtoaDomainSourceType(typ types.Type) bool {
	switch typ.Oid {
	case types.T_date, types.T_time, types.T_datetime, types.T_timestamp, types.T_json:
		return true
	default:
		return false
	}
}

// preparedInetNtoaSourceExpr is deliberately separate from the shared SQL
// EXECUTE numeric-source builder.  Temporal and JSON provenance is meaningful
// to INET_NTOA, but making it a generic numeric source would change unrelated
// arithmetic (for example JSON 1.6 + 0) by forcing an integer-domain cast.
func (rule *ResetParamRefRule) preparedInetNtoaSourceExpr(pos int) (*Expr, bool, error) {
	if pos < 0 || pos >= len(rule.paramValues) {
		return nil, false, nil
	}
	param, ok := rule.paramValues[pos].(ParamValue)
	if !ok || param.Value == nil {
		return nil, false, nil
	}
	sourceType := param.SourceType
	hasSourceType := param.HasSourceType
	if param.HasInetNtoaSourceType {
		sourceType = param.InetNtoaSourceType
		hasSourceType = true
	}
	if !hasSourceType || !isInetNtoaDomainSourceType(sourceType) {
		return nil, false, nil
	}
	value := param.Value
	if param.MaterializedValue != "" {
		value = param.MaterializedValue
	}
	source, err := preparedRuntimeParamExpr(
		rule.ctx, value, param.IsBin, sourceType)
	if err != nil {
		return nil, false, err
	}
	rule.retainRuntimeParamRef(pos, source)
	return source, true, nil
}

// preparedCharSourceExpr keeps CHAR's two string contracts separate at
// execute time. SQL EXECUTE and COM_STMT string values are text-backed
// transport values, but a bare prepared marker in CHAR receives numeric
// context: a complete integer/DECIMAL lexeme is materialized exactly, while a
// suffix or a non-numeric value stays on CHAR's integer-prefix parser.
// This is deliberately CHAR-specific; changing the shared string-to-DOUBLE
// path would regress ordinary arithmetic and comparison semantics.
func (rule *ResetParamRefRule) preparedCharSourceExpr(pos int) (*plan.Expr, bool, error) {
	if pos < 0 || pos >= len(rule.paramValues) || pos >= len(rule.params) {
		return nil, false, nil
	}
	param, ok := rule.paramValues[pos].(ParamValue)
	if !ok || param.Value == nil {
		return nil, false, nil
	}

	if param.HasSourceType && isStringBackedType(param.SourceType) {
		raw := preparedParamValueText(param)
		runtimeType, typeOK := PreparedCharSourceTypeFromString(raw)
		if !typeOK {
			return DeepCopyExpr(rule.params[pos]), true, nil
		}

		bound, err := preparedRuntimeParamExpr(rule.ctx, raw, param.IsBin, runtimeType)
		if err != nil {
			return nil, false, err
		}
		rule.retainRuntimeParamRef(pos, bound)
		return bound, true, nil
	}

	comStmtText := param.IsBinaryProtocol &&
		param.PrepareParamKind == vector.PrepareParamNone &&
		(param.Value != nil || param.HasRuntimeType)
	if (param.HasRuntimeType && isStringBackedType(param.RuntimeType)) || comStmtText {
		raw := preparedParamValueText(param)
		runtimeType, typeOK := PreparedCharSourceTypeFromString(raw)
		if !typeOK {
			// Invalid and suffix-bearing strings must keep CHAR's ordinary
			// numeric-prefix parser; routing them through the provisional INT64
			// cast would turn a valid prefix conversion into a strict cast error.
			return DeepCopyExpr(rule.params[pos]), true, nil
		}
		bound, err := preparedRuntimeParamExpr(rule.ctx, raw, param.IsBin, runtimeType)
		if err != nil {
			return nil, false, err
		}
		rule.retainRuntimeParamRef(pos, bound)
		return bound, true, nil
	}
	return nil, false, nil
}

// preparedRuntimeSourceExpr reconstructs an execute-time operand using its
// protocol type or SQL user-variable source type, including text and typed
// NULL. Consumers must not infer a numeric type from a textual value here.
func (rule *ResetParamRefRule) preparedRuntimeSourceExpr(pos int, preserveProtocolText bool) (*plan.Expr, bool, error) {
	if pos < 0 || pos >= len(rule.paramValues) || pos >= len(rule.params) {
		return nil, false, nil
	}
	param, ok := rule.paramValues[pos].(ParamValue)
	if !ok {
		return nil, false, nil
	}

	var sourceType types.Type
	switch {
	case param.IsBinaryProtocol && param.HasRuntimeType:
		sourceType = param.RuntimeType
	case param.IsBinaryProtocol && preserveProtocolText:
		// An absent concrete protocol domain denotes text, not an invitation
		// to infer DECIMAL from its spelling in an integer context.
		sourceType = types.T_text.ToType()
	case param.HasSourceType:
		sourceType = param.SourceType
	case param.HasRuntimeType:
		sourceType = param.RuntimeType
	case param.Value == nil && param.IsBinaryProtocol:
		sourceType = types.T_any.ToType()
	default:
		return nil, false, nil
	}
	if sourceType.Oid == types.T_any && param.Value != nil {
		return nil, false, nil
	}
	if sourceType.Oid.IsMySQLString() &&
		(param.IsBinaryString || param.IsBin || param.RuntimeStringDomain == types.RuntimeStringBinary) {
		sourceType.Charset = types.CharsetBinary
	}

	if param.Value == nil {
		// rule.params may hold a non-NULL placeholder from PREPARE. Never reuse
		// its literal payload for a runtime NULL; type the fresh NULL from the
		// execute-time metadata and leave the cached plan untouched.
		source := &plan.Expr{
			Typ: makePlan2Type(&sourceType),
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Isnull: true,
				IsBin:  param.IsBin,
				Value:  &plan.Literal_Sval{Sval: ""},
			}},
		}
		setPreparedRuntimeStringDomain(source, param.RuntimeStringDomain)
		rule.retainRuntimeParamRef(pos, source)
		return source, true, nil
	}

	value := param.Value
	if param.MaterializedValue != "" {
		value = param.MaterializedValue
	} else if bytes, ok := value.([]byte); ok {
		value = string(bytes)
	}
	source, err := preparedRuntimeParamExpr(rule.ctx, value, param.IsBin, sourceType)
	if err != nil {
		return nil, false, err
	}
	domain := param.RuntimeStringDomain
	if domain == types.RuntimeStringInherit &&
		(param.IsBinaryString || param.IsBin) {
		domain = types.RuntimeStringBinary
	}
	setPreparedRuntimeStringDomain(source, domain)
	rule.retainRuntimeParamRef(pos, source)
	return source, true, nil
}

func preparedStringMarkerType(expr *plan.Expr) types.Type {
	for expr != nil && isImplicitPreparedParamCast(expr) {
		fn := expr.GetF()
		if fn == nil || len(fn.Args) == 0 {
			break
		}
		expr = fn.Args[0]
	}
	if expr == nil {
		return types.T_text.ToType()
	}
	typ := makeTypeByPlan2Expr(expr)
	if !typ.Oid.IsMySQLString() || types.StaticStringDomain(typ) == types.StringDomainBinary ||
		(typ.Width <= 0 && typ.Oid != types.T_text) {
		return types.T_text.ToType()
	}
	typ.Charset = types.CharsetUTF8
	return typ
}

// preparedSQLExecuteTextFunctionParamType identifies only bare SQL EXECUTE
// markers passed to SOUNDEX/QUOTE whose user-variable source is binary. MySQL
// prepares these markers in a text context; preserve the payload and apply that
// context at this consumer rather than changing the variable's domain globally.
func (rule *ResetParamRefRule) preparedSQLExecuteTextFunctionParamType(
	functionName string,
	expr *plan.Expr,
) (types.Type, bool) {
	switch strings.ToLower(functionName) {
	case "soundex", "quote":
	default:
		return types.Type{}, false
	}
	pos, ok := preparedParamPosition(expr)
	if !ok || pos < 0 || pos >= len(rule.paramValues) {
		return types.Type{}, false
	}
	param, ok := rule.paramValues[pos].(ParamValue)
	if !ok || param.IsBinaryProtocol {
		return types.Type{}, false
	}
	isBinarySource := param.IsBin || param.IsBinaryString ||
		param.RuntimeStringDomain == types.RuntimeStringBinary ||
		(param.HasSourceType && types.StaticStringDomain(param.SourceType) == types.StringDomainBinary)
	if !isBinarySource {
		return types.Type{}, false
	}
	return preparedStringMarkerType(expr), true
}

// preparedSQLExecuteTextFunctionArg performs a byte-preserving cast into the
// marker's prepared text type. Unlike a global parameter-domain change, the
// cast is local to the SOUNDEX/QUOTE occurrence and leaves explicit binary
// casts and unrelated uses of the same SQL variable untouched.
func (rule *ResetParamRefRule) preparedSQLExecuteTextFunctionArg(
	pos int,
	targetType types.Type,
) (*plan.Expr, bool, error) {
	source, ok, err := rule.preparedRuntimeSourceExpr(pos, false)
	if err != nil || !ok {
		return source, ok, err
	}
	if !targetType.Oid.IsMySQLString() || types.StaticStringDomain(targetType) == types.StringDomainBinary {
		targetType = types.T_text.ToType()
	}
	targetType.Charset = types.CharsetUTF8
	textExpr, err := makePlan2CastExpr(rule.ctx, source, makePlan2Type(&targetType))
	if err != nil {
		return nil, false, err
	}
	return textExpr, true, nil
}

func isBitwiseAggregatePrivateCast(expr *plan.Expr) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || !strings.EqualFold(fn.Func.GetObjName(), "cast") ||
		len(fn.Args) == 0 || fn.GetSyntaxExplicitCast() {
		return false
	}
	_, overload := planfunction.DecodeOverloadID(fn.Func.GetObj())
	return overload == 4
}

func preparedParamValueText(param ParamValue) string {
	if param.MaterializedValue != "" {
		return param.MaterializedValue
	}
	switch value := param.Value.(type) {
	case []byte:
		return string(value)
	default:
		return fmt.Sprint(value)
	}
}

func preparedFunctionArgUsesSQLExecuteNumericSource(
	parent *plan.Expr,
	name string,
	argIndex int,
	argCount int,
) bool {
	// HEX owns the runtime domain of its sole argument: numeric source values
	// use numeric conversion, while non-numeric strings retain byte encoding.
	// The SQL EXECUTE source is materialized below without crossing an explicit
	// user CAST boundary.
	if name == "hex" {
		return argIndex == 0 && argCount == 1
	}
	// A prepared TEXT marker can make result-selecting functions bind to a
	// non-numeric envelope even though the execute-time SQL source is numeric.
	// Decide from the argument's value role before consulting that provisional
	// parent type; condition/control arguments must retain their original domain.
	if preparedSQLExecuteNumericResultConsumer(name) {
		return preparedSQLExecuteNumericResultValueArg(name, argIndex, argCount)
	}
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

func preserveReboundFunctionMetadata(original, rebound *plan.Function) {
	if original == nil || rebound == nil {
		return
	}
	rebound.AggConfig = bytes.Clone(original.AggConfig)
	rebound.AggConfigType = original.AggConfigType
	if original.Func != nil && rebound.Func != nil {
		// DISTINCT is encoded in the high bit of the function object ID, not in
		// the aggregate config. Binding from a name recreates only the base
		// overload, so keep this semantic flag when a prepared expression is
		// rebound against its execute-time source type.
		rebound.Func.Obj = int64(uint64(rebound.Func.Obj) |
			(uint64(original.Func.Obj) & uint64(planfunction.Distinct)))
	}
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

// preparedResultParamPosition recognizes provisional result-domain casts that
// overload resolution inserts around a prepared TEXT marker. The enclosing
// consumer and provisional target jointly distinguish these from authoritative
// numeric casts written by the user.
func preparedResultParamPosition(expr *plan.Expr, name string) (int, bool) {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.GetObjName() != "cast" || len(fn.Args) == 0 ||
		!expr.GetPreparedNumeric().GetProvisionalResultCast() {
		return 0, false
	}
	if !preparedSQLExecuteNumericResultConsumer(name) {
		return 0, false
	}
	param := fn.Args[0].GetP()
	if param == nil || param.Pos < 0 {
		return 0, false
	}
	return int(param.Pos), true
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
		if fn.GetSyntaxExplicitCast() {
			return nil, false
		}
		_, overload := planfunction.DecodeOverloadID(fn.Func.GetObj())
		if overload != 0 && overload != 4 {
			return nil, false
		}
		seenCast = true
		current = fn.Args[0]
	}
	return nil, false
}

func isPreparedBitwiseOperator(name string) bool {
	switch name {
	case "&", "|", "^", "<<", ">>", "unary_tilde":
		return true
	default:
		return false
	}
}

// isPreparedBitwiseParamCast recognizes a provisional cast chain around a
// prepared marker when bitwise overload resolution used either the ordinary
// cast or the comparison-cast overload. Explicit and set-operation casts stay
// semantic boundaries.
func isPreparedBitwiseParamCast(expr *plan.Expr) bool {
	current := expr
	seenCast := false
	for current != nil {
		if param := current.GetP(); param != nil {
			return seenCast && param.Pos >= 0
		}
		fn := current.GetF()
		if fn == nil || fn.Func == nil || fn.Func.GetObjName() != "cast" || len(fn.Args) == 0 ||
			fn.GetSyntaxExplicitCast() {
			return false
		}
		_, overload := planfunction.DecodeOverloadID(fn.Func.GetObj())
		if overload != 0 && overload != 2 {
			return false
		}
		seenCast = true
		current = fn.Args[0]
	}
	return false
}

// unwrapImplicitPreparedBinaryParamCast removes the prepare-time overload
// casts around a binary protocol value before rebinding a bitwise operator.
// The caller has already verified that the original expression contains only
// provisional casts, so explicit CAST remains authoritative.
func unwrapImplicitPreparedBinaryParamCast(rewritten *plan.Expr) (*plan.Expr, bool) {
	current := rewritten
	for current != nil {
		fn := current.GetF()
		if fn == nil || fn.Func == nil || fn.Func.GetObjName() != "cast" || len(fn.Args) == 0 {
			break
		}
		if fn.GetSyntaxExplicitCast() {
			return nil, false
		}
		_, overload := planfunction.DecodeOverloadID(fn.Func.GetObj())
		if overload != 0 && overload != 2 {
			return nil, false
		}
		current = fn.Args[0]
	}
	if current == nil || types.StaticStringDomain(makeTypeByPlan2Expr(current)) != types.StringDomainBinary {
		return nil, false
	}
	return current, true
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
