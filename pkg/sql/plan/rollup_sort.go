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
	"reflect"
	"strings"

	containertypes "github.com/matrixorigin/matrixone/pkg/container/types"
	plan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const sortRollupExtraOption = "sort_rollup"

// EncodeSortRollupOption marks an aggregate node whose input is ordered by its
// grouping keys and whose executor should produce ROLLUP rows in one pass.
// ExtraOptions is already carried through plan cloning and remote plan
// transport, so no protobuf change is needed for this physical hint.
func EncodeSortRollupOption() string {
	return sortRollupExtraOption
}

// IsSortRollupOption reports whether an aggregate node uses the sort rollup
// executor. Keep this tolerant of future option suffixes so adding physical
// parameters does not make older readers silently fall back to hash rollup.
func IsSortRollupOption(extra string) bool {
	return extra == sortRollupExtraOption ||
		strings.HasPrefix(extra, sortRollupExtraOption+":")
}

// sortRollupTypeEligible mirrors the executor's fail-closed whitelist. The
// physical SORT uses SQL order equality, while the aggregate's ordinary hash
// path uses the equality key codec. Types without a proven identical domain
// must stay on the legacy grouping-set rewrite instead of reaching the sort
// executor and failing after a plan has already been selected.
func sortRollupTypeEligible(oid int32) bool {
	switch containertypes.T(oid) {
	case containertypes.T_bool,
		containertypes.T_bit,
		containertypes.T_int8, containertypes.T_int16, containertypes.T_int32,
		containertypes.T_int64,
		containertypes.T_uint8, containertypes.T_uint16, containertypes.T_uint32,
		containertypes.T_uint64,
		containertypes.T_decimal64, containertypes.T_decimal128, containertypes.T_decimal256,
		containertypes.T_date, containertypes.T_time, containertypes.T_datetime,
		containertypes.T_timestamp, containertypes.T_year,
		containertypes.T_uuid, containertypes.T_enum:
		return true
	default:
		return false
	}
}

// sortRollupGroupingTypesEligible performs a planning-only binding probe. The
// fast-path decision is made before bindSelectClause because unsupported
// grouping types must fall back to the existing UNION ALL rewrite. A separate
// builder keeps the probe's nodes, tags, and name maps out of the real plan;
// any probe failure is deliberately treated as "not eligible" so the normal
// binder remains the source of user-visible errors.
func (builder *QueryBuilder) sortRollupGroupingTypesEligible(
	ctx *BindContext,
	from tree.TableExprs,
	exprs tree.Exprs,
	isRoot bool,
) bool {
	_, ok := builder.probeSortRollupInput(ctx, from, nil, exprs, isRoot)
	return ok
}

// sortRollupProbe is a private planning-only copy of the input relation. The
// copy is intentionally retained until the cost decision is complete: the
// bound grouping expressions provide both their physical types and the NDV
// lookup keys used by the estimator.
type sortRollupProbe struct {
	builder      *QueryBuilder
	ctx          *BindContext
	source       *Node
	tableDef     *TableDef
	groupExprs   []*Expr
	orderedInput bool
}

func (builder *QueryBuilder) probeSortRollupInput(
	ctx *BindContext,
	from tree.TableExprs,
	where *tree.Where,
	exprs tree.Exprs,
	isRoot bool,
) (*sortRollupProbe, bool) {
	if builder == nil || ctx == nil || len(exprs) == 0 {
		return nil, false
	}
	orderedInput := false
	if !sortRollupSimpleBaseTableSource(from) {
		if !sortRollupOrderedDerivedSource(from, exprs) {
			return nil, false
		}
		orderedInput = true
	}
	for current := ctx; current != nil; current = current.parent {
		if len(current.cteByName) > 0 || len(current.boundCtes) > 0 {
			return nil, false
		}
	}

	// BuildPlan intentionally starts the real binder with skipStats=true and
	// enables statistics only for the later optimizer pass. Automatic ROLLUP
	// selection happens during binding, so its isolated probe must explicitly
	// load the table statistics; forced eligibility probes keep the cheap
	// skip-stats behavior.
	probeSkipStats := builder.skipStats
	if builder.sortRollupMode() == 0 {
		probeSkipStats = false
	}
	probeBuilder := NewQueryBuilder(
		builder.qry.StmtType,
		builder.compCtx,
		builder.isPrepareStatement,
		probeSkipStats,
	)
	probeCtx := NewBindContext(probeBuilder, nil)
	probeCtx.defaultDatabase = ctx.defaultDatabase
	probeCtx.lower = ctx.lower
	probeCtx.snapshot = ctx.snapshot
	probeCtx.remapOption = ctx.remapOption
	// Do not share CTE binding state with the real builder. Binding a CTE can
	// update declaration/reference bookkeeping; an unsupported CTE shape should
	// simply use the normal grouping-set path rather than perturbing the real
	// query block during this probe.

	probeFrom := cloneSortRollupTableExprs(from)
	probeNodeID, err := probeBuilder.buildFrom(probeFrom, probeCtx, isRoot)
	if err != nil || probeNodeID < 0 || int(probeNodeID) >= len(probeBuilder.qry.Nodes) {
		return nil, false
	}

	// Include a cloned WHERE in the cardinality estimate when it can be bound
	// independently. A failed probe is a cost-unknown case, not a user-visible
	// binding error; the normal binder remains responsible for reporting the
	// actual SQL error and will use the legacy grouping-set path.
	if where != nil && where.Expr != nil {
		probeCtx.binder = NewWhereBinder(probeBuilder, probeCtx)
		whereExpr := cloneTreeExpr(where.Expr)
		boundFilters, bindErr := splitAndBindCondition(whereExpr, NoAlias, probeCtx)
		if bindErr != nil || len(boundFilters) == 0 {
			return nil, false
		}
		for _, filter := range boundFilters {
			unsupported := false
			existentialWalk(filter, func(expr *Expr) {
				if expr.GetSub() != nil || expr.GetCorr() != nil || expr.GetW() != nil {
					unsupported = true
				}
			})
			if unsupported {
				return nil, false
			}
		}
		// Attach the filters directly to the isolated scan. This gives the
		// probe the same selectivity calculation as a pushed-down base-table
		// predicate without running subquery flattening or mutating the real
		// query block. Unsupported/subquery predicates remain cost-unknown.
		probeBuilder.qry.Nodes[probeNodeID].FilterList = append(
			probeBuilder.qry.Nodes[probeNodeID].FilterList, boundFilters...)
		ReCalcNodeStats(probeNodeID, probeBuilder, false, true, true)
	}
	probeCtx.binder = NewWhereBinder(probeBuilder, probeCtx)
	groupBinder := NewGroupBinder(probeBuilder, probeCtx, nil, false)
	boundExprs := make([]*Expr, 0, len(exprs))
	for _, expr := range exprs {
		// qualifyColumnNames adds the source table to an unresolved name in
		// place. Deep-copy the expression before probing so the real binder sees
		// the original AST even if a future binder adds another mutation path.
		probeExpr := cloneTreeExpr(expr)
		qualified, err := probeCtx.qualifyColumnNames(probeExpr, AliasAfterColumn)
		if err != nil {
			return nil, false
		}
		bound, err := groupBinder.BindExpr(qualified, 0, true)
		if err != nil || bound == nil || !sortRollupTypeEligible(bound.Typ.Id) {
			return nil, false
		}
		boundExprs = append(boundExprs, bound)
	}

	source := probeBuilder.qry.Nodes[probeNodeID]
	if source == nil || source.Stats == nil {
		return nil, false
	}
	if orderedInput {
		// Keep the cost estimate aligned with the physical plan. The AST proof
		// admits only a narrow derived-table shape, but the isolated builder is
		// still the authority on whether that ORDER BY survived as an actual
		// complete Sort plus transparent projections.
		orderedInput = probeBuilder.sortRollupNodeInputOrdered(
			probeNodeID, boundExprs)
	}
	tableDef := source.TableDef
	if tableDef == nil {
		tableDef = sortRollupLeafTableDef(probeBuilder, probeNodeID)
	}
	return &sortRollupProbe{
		builder:      probeBuilder,
		ctx:          probeCtx,
		source:       source,
		tableDef:     tableDef,
		groupExprs:   boundExprs,
		orderedInput: orderedInput,
	}, true
}

func sortRollupSimpleBaseTableSource(from tree.TableExprs) bool {
	if len(from) != 1 {
		return false
	}
	var source tree.TableExpr = from[0]
	for {
		switch expr := source.(type) {
		case *tree.AliasedTableExpr:
			source = expr.Expr
		case *tree.ParenTableExpr:
			source = expr.Expr
		case *tree.JoinTableExpr:
			if expr.Right != nil {
				return false
			}
			source = expr.Left
		default:
			_, ok := source.(*tree.TableName)
			return ok
		}
	}
}

// sortRollupOrderedDerivedSource recognizes the one derived-table shape where
// an existing ORDER BY is a physical input property we can safely reuse. The
// inner query must expose the ordered columns directly and in the same prefix
// as ROLLUP. Expressions, aliases, DESC, and NULLS LAST are intentionally not
// accepted because translating their order/equality domains would require a
// stronger property framework than the current plan wire carries.
func sortRollupOrderedDerivedSource(from tree.TableExprs, grouping tree.Exprs) bool {
	if len(from) != 1 || len(grouping) == 0 {
		return false
	}
	source := from[0]
	for {
		switch expr := source.(type) {
		case *tree.AliasedTableExpr:
			source = expr.Expr
		case *tree.ParenTableExpr:
			source = expr.Expr
		case *tree.JoinTableExpr:
			if expr.Right != nil {
				return false
			}
			source = expr.Left
		default:
			goto unwrapped
		}
	}

unwrapped:
	derived, ok := source.(*tree.Select)
	if !ok || derived.Limit != nil || derived.RankOption != nil || len(derived.OrderBy) < len(grouping) {
		return false
	}
	clause, ok := derived.Select.(*tree.SelectClause)
	if !ok || clause.Distinct || clause.Where != nil || clause.GroupBy != nil || clause.Having != nil {
		return false
	}

	for i, groupExpr := range grouping {
		groupName, ok := sortRollupASTColumnName(groupExpr)
		if !ok {
			return false
		}
		order := derived.OrderBy[i]
		if order == nil || order.Direction == tree.Descending || order.NullsPosition == tree.NullsLast {
			return false
		}
		orderName, ok := sortRollupASTColumnName(order.Expr)
		if !ok || orderName != groupName {
			return false
		}
		if !sortRollupDerivedProjectsColumn(clause.Exprs, orderName) {
			return false
		}
	}
	return true
}

func sortRollupASTColumnName(expr tree.Expr) (string, bool) {
	name, ok := unwrapParenExpr(expr).(*tree.UnresolvedName)
	if !ok || name == nil || name.Star || name.NumParts == 0 {
		return "", false
	}
	return strings.ToLower(name.ColName()), true
}

func sortRollupDerivedProjectsColumn(exprs tree.SelectExprs, name string) bool {
	for _, expr := range exprs {
		column, ok := sortRollupASTColumnName(expr.Expr)
		if !ok || column != name {
			continue
		}
		if expr.As == nil || expr.As.Empty() || strings.EqualFold(expr.As.Compare(), name) {
			return true
		}
	}
	return false
}

func sortRollupLeafTableDef(builder *QueryBuilder, nodeID int32) *TableDef {
	if builder == nil || nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) {
		return nil
	}
	node := builder.qry.Nodes[nodeID]
	if node == nil {
		return nil
	}
	if node.TableDef != nil {
		return node.TableDef
	}
	for _, childID := range node.Children {
		if tableDef := sortRollupLeafTableDef(builder, childID); tableDef != nil {
			return tableDef
		}
	}
	return nil
}

// sortRollupNodeInputOrdered is the plan-level counterpart of the derived
// table proof. It is deliberately limited to a complete global SORT, with
// transparent FILTERs and positional identity PROJECTs; a TABLE_SCAN's
// OrderBy is local to a CN unless the compiler also constructs a MergeOrder,
// so treating it as globally ordered here would be an incorrect distributed
// optimization.
func (builder *QueryBuilder) sortRollupNodeInputOrdered(
	nodeID int32,
	groupExprs []*Expr,
) bool {
	if builder == nil || nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) || len(groupExprs) == 0 {
		return false
	}
	node := builder.qry.Nodes[nodeID]
	if node == nil || len(node.Children) != 1 {
		return false
	}
	switch node.NodeType {
	case plan.Node_SORT:
		return node.Limit == nil && node.Offset == nil && node.RankOption == nil &&
			sortRollupPlanOrderMatches(node.OrderBy, groupExprs)
	case plan.Node_FILTER:
		return builder.sortRollupNodeInputOrdered(node.Children[0], groupExprs)
	case plan.Node_PROJECT:
		// Derived-table binding may insert an identity projection between the
		// producer Sort and this aggregate. Only remove that wrapper when every
		// grouping column is forwarded positionally with the same type; a
		// reordered/computed projection must fall back to a fresh Sort.
		for _, expr := range groupExprs {
			col := expr.GetCol()
			if col == nil || col.ColPos < 0 || int(col.ColPos) >= len(node.ProjectList) {
				return false
			}
			projected := node.ProjectList[col.ColPos]
			projectedCol := projected.GetCol()
			if projectedCol == nil || projectedCol.ColPos != col.ColPos ||
				projected.Typ.Id != expr.Typ.Id ||
				projected.Typ.Width != expr.Typ.Width ||
				projected.Typ.Scale != expr.Typ.Scale {
				return false
			}
		}
		return builder.sortRollupNodeInputOrdered(node.Children[0], groupExprs)
	default:
		return false
	}
}

func sortRollupPlanOrderMatches(orderBy []*plan.OrderBySpec, groupExprs []*Expr) bool {
	if len(orderBy) < len(groupExprs) {
		return false
	}
	for i, expr := range groupExprs {
		if expr == nil || expr.GetCol() == nil {
			return false
		}
		spec := orderBy[i]
		if spec == nil || spec.Expr == nil || spec.Expr.GetCol() == nil ||
			spec.Flag&plan.OrderBySpec_DESC != 0 || spec.Flag&plan.OrderBySpec_NULLS_LAST != 0 {
			return false
		}
		left, right := expr.GetCol(), spec.Expr.GetCol()
		// Binding and projection nodes assign different relation tags to the
		// same positional column. For the candidate shapes (a single base or
		// direct derived source), position plus type is the stable identity; the
		// caller has already ruled out joins and computed/reordered projections.
		if left.ColPos != right.ColPos ||
			expr.Typ.Id != spec.Expr.Typ.Id ||
			expr.Typ.Width != spec.Expr.Typ.Width ||
			expr.Typ.Scale != spec.Expr.Typ.Scale {
			return false
		}
	}
	return true
}

func cloneSortRollupTableExprs(from tree.TableExprs) tree.TableExprs {
	if from == nil {
		return nil
	}
	cloned := cloneTreeValue(
		reflect.ValueOf(from),
		make(map[treeClonePointer]reflect.Value),
	)
	return cloned.Interface().(tree.TableExprs)
}

// sortRollupGroupingListEligible is deliberately conservative. GroupBinder
// deduplicates grouping expressions, while ROLLUP's level list is ordered and
// multiplicity-sensitive. Direct, distinct column references let the first
// sort implementation keep those two contracts aligned; all other forms stay
// on the existing grouping-set rewrite.
func sortRollupGroupingListEligible(exprs tree.Exprs, selectExprs tree.SelectExprs) bool {
	aliases := make(map[string]struct{})
	for _, selectExpr := range selectExprs {
		if selectExpr.As != nil && !selectExpr.As.Empty() {
			aliases[strings.ToLower(selectExpr.As.Compare())] = struct{}{}
		}
	}

	seen := make(map[string]struct{}, len(exprs))
	for _, expr := range exprs {
		name, ok := unwrapParenExpr(expr).(*tree.UnresolvedName)
		if !ok || name.Star || name.NumParts == 0 {
			return false
		}
		// Treat qualified and unqualified references to the same column name as
		// duplicates. This may conservatively fall back for a join containing
		// homonymous columns, which is preferable to losing a ROLLUP level.
		key := strings.ToLower(name.ColName())
		// An unresolved name can bind to a SELECT alias. The alias may be a
		// different source column or a complex expression, and GroupBinder can
		// then deduplicate it against another grouping item. Reject aliases
		// before the sort path bypasses grouping-set expansion.
		if _, ok := aliases[key]; ok {
			return false
		}
		if _, ok := seen[key]; ok {
			return false
		}
		seen[key] = struct{}{}
	}
	return len(exprs) > 0
}

// sortRollupAggregateOrderEligible keeps the sort path from changing an
// aggregate's input-order contract. Sorting by the ROLLUP keys changes the
// order within each group, while these aggregates observe that order unless
// their own executor has an explicit ordering contract. The first version
// therefore leaves all of them on the legacy path; rollup_algorithm='SORT'
// remains a plan-shape switch only for semantically safe aggregates.
func sortRollupAggregateOrderEligible(selectExprs tree.SelectExprs, extraExprs ...tree.Expr) bool {
	for _, selectExpr := range selectExprs {
		if !sortRollupAggregateOrderEligibleExpr(selectExpr.Expr) {
			return false
		}
	}
	for _, expr := range extraExprs {
		if !sortRollupAggregateOrderEligibleExpr(expr) {
			return false
		}
	}
	return true
}

func sortRollupAggregateOrderEligibleExpr(expr tree.Expr) bool {
	eligible := true
	walkGroupingSetOrderByExpr(expr, func(expr tree.Expr) bool {
		fn, ok := expr.(*tree.FuncExpr)
		if !ok {
			return true
		}
		switch sortRollupASTFunctionName(fn) {
		case "group_concat", "json_arrayagg", "json_objectagg":
			eligible = false
			return false
		default:
			return true
		}
	})
	return eligible
}

func sortRollupASTFunctionName(fn *tree.FuncExpr) string {
	if fn == nil {
		return ""
	}
	if fn.FuncName != nil && fn.FuncName.Origin() != "" {
		return strings.ToLower(fn.FuncName.Origin())
	}
	if unresolved, ok := fn.Func.FunctionReference.(*tree.UnresolvedName); ok && unresolved != nil {
		return strings.ToLower(unresolved.ColName())
	}
	return ""
}
