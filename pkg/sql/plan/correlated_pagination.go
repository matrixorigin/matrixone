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
	"math"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// postJoinPagination carries the one pagination boundary that must be applied
// after the correlated join. Its expressions are remapped while predicates are
// pulled through the right-hand projections.
type postJoinPagination struct {
	boundary int32
	outerID  *plan.Expr
	marker   *plan.Expr
	orderBy  []*plan.OrderBySpec
	offset   uint64
	crossed  bool
}

type correlatedPaginationBoundary struct {
	id    int32
	root  bool
	preds []*plan.Expr
}

// A LIMIT 0 at the semantic root makes the entire subquery empty. Do not
// cross aggregates, filters or other row-changing nodes when locating it.
func (builder *QueryBuilder) subqueryRootLimitIsZero(nodeID int32) bool {
	for {
		node := builder.qry.Nodes[nodeID]
		if node.RankOption != nil {
			return false
		}
		if node.Limit != nil {
			limit, literal := getLiteralUint64(node.Limit)
			return literal && limit == 0
		}
		if node.Offset != nil || len(node.Children) != 1 ||
			(node.NodeType != plan.Node_PROJECT && node.NodeType != plan.Node_SORT) {
			return false
		}
		nodeID = node.Children[0]
	}
}

func (builder *QueryBuilder) emptySubqueryResult(nodeID int32, ctx *BindContext, subquery *plan.SubqueryRef) (int32, *plan.Expr, error) {
	switch subquery.Typ {
	case plan.SubqueryRef_SCALAR:
		result := makePlan2NullConstExprWithType()
		result.Typ = ctx.results[0].Typ
		result.Typ.NotNullable = false
		return nodeID, result, nil
	case plan.SubqueryRef_EXISTS:
		return nodeID, constFalse, nil
	case plan.SubqueryRef_NOT_EXISTS:
		return nodeID, constTrue, nil
	case plan.SubqueryRef_IN, plan.SubqueryRef_ANY, plan.SubqueryRef_NOT_IN, plan.SubqueryRef_ALL:
		if !paginationExpressionSafe(subquery.Child) {
			return 0, nil, moerr.NewNYI(builder.GetContext(), "empty quantified subquery with an unsafe left operand")
		}
		if subquery.Typ == plan.SubqueryRef_IN || subquery.Typ == plan.SubqueryRef_ANY {
			return nodeID, constFalse, nil
		}
		return nodeID, constTrue, nil
	default:
		return 0, nil, moerr.NewNotSupportedf(builder.GetContext(), "%s subquery not supported", subquery.Typ.String())
	}
}

// COUNT's empty-input fallback applies only when the scalar aggregate row
// survives pagination. Otherwise a NULL right side can mean that LIMIT or
// OFFSET removed the row, which must remain a NULL scalar result.
func (builder *QueryBuilder) scalarPaginationProperties(nodeID int32) (mayDiscard, hasPagination bool) {
	for {
		node := builder.qry.Nodes[nodeID]
		if node.Limit != nil {
			hasPagination = true
			limit, ok := getLiteralUint64(node.Limit)
			if !ok || limit == 0 {
				return true, true
			}
		}
		if node.Offset != nil {
			hasPagination = true
			offset, ok := getLiteralUint64(node.Offset)
			if !ok || offset > 0 {
				return true, true
			}
		}
		if len(node.Children) != 1 || (node.NodeType != plan.Node_PROJECT && node.NodeType != plan.Node_SORT) {
			return false, hasPagination
		}
		nodeID = node.Children[0]
	}
}

// prepareCorrelatedPagination finds pagination crossed by predicate pull-up
// before modifying the plan. Pagination above a correlated projection or
// ordering is not safe to move using the predicate-only decorrelator. The
// returned boundary IDs authorize byte-key partitioning or retaining the
// original pagination only at that node; -1 means no such authorization.
func (builder *QueryBuilder) prepareCorrelatedPagination(
	outerID, subID int32, subCtx *BindContext, subqueryType plan.SubqueryRef_Type,
) (*postJoinPagination, int32, int32, error) {
	var hasPagination func(int32) bool
	hasPagination = func(id int32) bool {
		node := builder.qry.Nodes[id]
		if node.Limit != nil || node.Offset != nil || node.RankOption != nil {
			return true
		}
		for _, child := range node.Children {
			if hasPagination(child) {
				return true
			}
		}
		return false
	}
	if !hasPagination(subID) {
		return nil, -1, -1, nil
	}
	var boundaries []correlatedPaginationBoundary
	var visit func(int32, bool) ([]*plan.Expr, bool, bool, bool, bool)
	visit = func(id int32, root bool) ([]*plan.Expr, bool, bool, bool, bool) {
		node := builder.qry.Nodes[id]
		var preds []*plan.Expr
		otherRef, volatile, volatileExceptDistinctAgg, unsafe := false, false, false, false
		for _, child := range node.Children {
			childRoot := root && len(node.Children) == 1 && node.Limit == nil &&
				node.Offset == nil && (node.NodeType == plan.Node_PROJECT || node.NodeType == plan.Node_SORT)
			childPreds, childOther, childVolatile, childDistinctVolatile, childUnsafe := visit(child, childRoot)
			preds = append(preds, childPreds...)
			otherRef = otherRef || childOther
			volatile = volatile || childVolatile
			volatileExceptDistinctAgg = volatileExceptDistinctAgg || childDistinctVolatile
			unsafe = unsafe || childUnsafe
		}
		if len(preds) > 0 {
			switch node.NodeType {
			case plan.Node_FILTER, plan.Node_PROJECT, plan.Node_SORT:
			case plan.Node_AGG:
				// An aggregate may produce a row on empty input. EXISTS over a
				// nontrivial aggregate cannot be reduced to ordinary matches.
				if subqueryType == plan.SubqueryRef_EXISTS || subqueryType == plan.SubqueryRef_NOT_EXISTS {
					unsafe = true
				}
				// Pulling a predicate above aggregation evaluates group keys
				// for inner rows that the correlated filter would have skipped.
				for _, key := range node.GroupBy {
					if !paginationExpressionSafe(key) {
						unsafe = true
					}
				}
				for _, agg := range node.AggList {
					if !paginationAggregateSafe(agg) {
						unsafe = true
					}
				}
			default:
				unsafe = true
			}
		}
		for _, exprs := range [][]*plan.Expr{node.ProjectList, node.OnList, node.GroupBy, node.WinSpecList} {
			for _, expr := range exprs {
				otherRef = otherRef || paginationHasCorrCol(expr)
				if expr != nil {
					changes := exprMayChangeWhenRepeated(expr)
					volatile = volatile || changes
					volatileExceptDistinctAgg = volatileExceptDistinctAgg || changes
				}
			}
		}
		for _, agg := range node.AggList {
			otherRef = otherRef || paginationHasCorrCol(agg)
			if agg != nil {
				volatile = volatile || exprMayChangeWhenRepeated(agg)
				volatileExceptDistinctAgg = volatileExceptDistinctAgg || paginationAggregateMayChangeWhenRepeated(agg)
			}
		}
		for _, exprs := range [][]*plan.Expr{node.ProjectList, node.OnList} {
			for _, expr := range exprs {
				if !paginationExpressionSafe(expr) {
					unsafe = true
				}
			}
		}
		for _, order := range node.OrderBy {
			if order != nil {
				otherRef = otherRef || paginationHasCorrCol(order.Expr)
				if !paginationExpressionSafe(order.Expr) {
					unsafe = true
				}
				if order.Expr != nil {
					changes := exprMayChangeWhenRepeated(order.Expr)
					volatile = volatile || changes
					volatileExceptDistinctAgg = volatileExceptDistinctAgg || changes
				}
			}
		}
		for _, filter := range node.FilterList {
			if filter == nil {
				continue
			}
			if node.NodeType == plan.Node_FILTER && hasCorrCol(filter) {
				preds = append(preds, filter)
			} else {
				otherRef = otherRef || paginationHasCorrCol(filter)
			}
			changes := exprMayChangeWhenRepeated(filter)
			volatile = volatile || changes
			volatileExceptDistinctAgg = volatileExceptDistinctAgg || changes
			if !paginationExpressionSafe(filter) {
				unsafe = true
			}
		}
		if (node.Limit != nil || node.Offset != nil || node.RankOption != nil) && (len(preds) > 0 || otherRef) {
			boundaries = append(boundaries, correlatedPaginationBoundary{id: id, root: root, preds: preds})
		}
		return preds, otherRef, volatile, volatileExceptDistinctAgg, unsafe
	}
	_, allOtherRefs, allVolatile, allDistinctVolatile, allUnsafe := visit(subID, true)
	if len(boundaries) == 0 {
		return nil, -1, -1, nil
	}
	// If no correlated predicate crosses pagination, this decorrelator does
	// not move the boundary. Leave projection/order-only correlation to its
	// existing owner instead of changing otherwise independent plan binding.
	pulledAcross := false
	for _, boundary := range boundaries {
		pulledAcross = pulledAcross || len(boundary.preds) > 0
		if subqueryType == plan.SubqueryRef_SCALAR {
			for _, pred := range boundary.preds {
				if !allCorrColsAtDepthOne(pred) {
					return nil, -1, -1, moerr.NewNYIf(builder.GetContext(), "correlated columns in %s subquery deeper than 1 level will be supported in future version", subqueryType.String())
				}
			}
		}
	}
	if !pulledAcross {
		return nil, -1, -1, nil
	}
	if len(boundaries) != 1 || allOtherRefs {
		return nil, -1, -1, moerr.NewNYI(builder.GetContext(), "correlated pagination across nested boundaries or correlated expressions")
	}
	boundary := boundaries[0]
	node := builder.qry.Nodes[boundary.id]
	if node.RankOption != nil {
		return nil, -1, -1, moerr.NewNYI(builder.GetContext(), "correlated LIMIT with BY RANK")
	}
	outerOnly := len(boundary.preds) > 0
	nonEq := false
	for _, pred := range boundary.preds {
		outerOnly = outerOnly && !exprHasColRef(pred)
		nonEq = nonEq || containsCorrelatedNonEqComparison(pred)
	}
	if boundary.root && subqueryType == plan.SubqueryRef_SCALAR {
		// An outer-only predicate either admits the whole inner input or none.
		// Keep the existing pagination in place; a window and its INT64 bounds
		// would change both the plan and supported parameterized limits.
		if !allVolatile && outerOnly && len(subCtx.aggregates) == 0 &&
			(node.NodeType == plan.Node_PROJECT || node.NodeType == plan.Node_SORT) &&
			builder.simplePaginationInput(subID, boundary.id, true) {
			limit, literal := getLiteralUint64(node.Limit)
			limitSupported, limitStatic := paginationPreservableBound(node.Limit)
			offsetSupported, offsetStatic := paginationPreservableBound(node.Offset)
			if node.Limit != nil && limitSupported && offsetSupported && (!literal || limit > 0) &&
				(limitStatic && offsetStatic || builder.paginationDirectScanResult(subID, subCtx)) {
				return nil, -1, boundary.id, nil
			}
		}
		aggregateSafe := !allDistinctVolatile && builder.paginationRedundantAggregateLimit(subID, boundary.id, subCtx)
		if aggregateSafe {
			if outerOnly {
				return nil, -1, boundary.id, nil
			}
			if nonEq {
				// The comparison stays visible through pull-up, so the
				// existing non-equality aggregate owner will run.
				node.Limit, node.Offset = nil, nil
				return nil, -1, -1, nil
			}
		} else if nonEq && len(subCtx.aggregates) > 0 {
			if limit, literal := getLiteralUint64(node.Limit); literal && limit > 0 {
				// The non-equality owner aggregates a synthetic NULL row on
				// no match; do not fall through without proving it safe.
				return nil, -1, -1, moerr.NewNYI(builder.GetContext(), "non-equality scalar aggregate without safe NULL extension")
			}
		}
	}

	limit, literal := getLiteralUint64(node.Limit)
	if !literal {
		return nil, -1, -1, moerr.NewNYI(builder.GetContext(), "dynamic LIMIT in correlated subquery")
	}
	if limit == 0 {
		if !boundary.root {
			return nil, -1, -1, moerr.NewNYI(builder.GetContext(), "nested correlated LIMIT 0")
		}
		if node.Offset != nil {
			if _, ok := getLiteralUint64(node.Offset); !ok {
				return nil, -1, -1, moerr.NewNYI(builder.GetContext(), "correlated LIMIT 0 with a dynamic OFFSET")
			}
		}
		return nil, -1, -1, nil
	}
	if allVolatile || allUnsafe {
		return nil, -1, -1, moerr.NewNYI(builder.GetContext(), "correlated pagination with expressions or input that cannot be safely decorrelated")
	}
	offset := uint64(0)
	if node.Offset != nil {
		var ok bool
		offset, ok = getLiteralUint64(node.Offset)
		if !ok || offset > math.MaxInt64 {
			return nil, -1, -1, moerr.NewNYI(builder.GetContext(), "dynamic or oversized OFFSET in correlated subquery")
		}
	}
	keys, equality := correlatedPaginationPartitionKeys(boundary.preds)
	compatibleKeys := equality && len(keys) > 0
	for _, key := range keys {
		compatibleKeys = compatibleKeys && correlatedPaginationPartitionTypeSupported(types.T(key.Typ.Id))
	}
	// A plain scalar aggregate without GROUP BY has exactly one row before
	// pagination. A positive root LIMIT with zero OFFSET cannot remove it.
	// Drop that boundary only for the shape already handled by the existing
	// post-join aggregate projection reconstruction and predicates that
	// produce at most one aggregate group per outer row.
	if subqueryType == plan.SubqueryRef_SCALAR && boundary.id == subID && offset == 0 &&
		equality && (len(keys) == 0 || compatibleKeys) && len(subCtx.groups) == 0 && len(subCtx.aggregates) > 0 &&
		node.NodeType == plan.Node_PROJECT && len(node.Children) == 1 {
		agg := builder.qry.Nodes[node.Children[0]]
		if agg.NodeType == plan.Node_AGG && len(agg.GroupBy) == 0 &&
			agg.Limit == nil && agg.Offset == nil && agg.RankOption == nil {
			node.Limit = nil
			node.Offset = nil
			node.OrderBy = nil
			return nil, -1, -1, nil
		}
	}
	if boundary.root && offset == 0 && (subqueryType == plan.SubqueryRef_EXISTS || subqueryType == plan.SubqueryRef_NOT_EXISTS) {
		return nil, -1, -1, nil
	}
	if equality && (len(keys) == 0 || compatibleKeys) {
		return nil, -1, -1, nil
	}
	if !boundary.root || subqueryType != plan.SubqueryRef_SCALAR || limit != 1 {
		return nil, -1, -1, moerr.NewNYI(builder.GetContext(), "correlated pagination with non-equality predicates")
	}
	outerIdentity := builder.paginationOuterIdentity(outerID)
	simpleInput := builder.simplePaginationInput(subID, boundary.id, false)
	if outerIdentity == nil && simpleInput && equality && len(keys) > 0 {
		byteKey, supported := false, true
		for _, key := range keys {
			typ := types.T(key.Typ.Id)
			byteKey = byteKey || correlatedPaginationByteKeySupported(typ)
			supported = supported && (correlatedPaginationPartitionTypeSupported(typ) || correlatedPaginationByteKeySupported(typ))
		}
		if byteKey && supported {
			// Without an outer-row identity, rank the inner equality classes
			// instead. Retain the post-join path when identity is available:
			// ranking all inner keys can be slower for a selective outer input.
			return nil, boundary.id, -1, nil
		}
	}
	if outerIdentity == nil || !simpleInput {
		return nil, -1, -1, moerr.NewNYI(builder.GetContext(), "correlated pagination without a unique outer row or simple inner input")
	}
	var marker *plan.Expr
	if !builder.paginationDirectScanResult(subID, subCtx) {
		marker = builder.findRowIDColRef(subID)
		if marker == nil || types.T(marker.Typ.Id) != types.T_Rowid {
			return nil, -1, -1, moerr.NewNYI(builder.GetContext(), "correlated pagination without an inner row identity")
		}
	}
	return &postJoinPagination{boundary: boundary.id, outerID: outerIdentity, marker: marker, offset: offset}, -1, -1, nil
}

// Projection and ORDER BY can run on inner rows that no outer row matches.
// Accept only expressions known to be total and repeatable; function metadata
// alone does not describe either property for every built-in.
func paginationExpressionSafe(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch item := expr.Expr.(type) {
	case *plan.Expr_Col, *plan.Expr_Corr, *plan.Expr_Lit, *plan.Expr_P:
		return true
	case *plan.Expr_List:
		if item.List == nil {
			return false
		}
		for _, arg := range item.List.List {
			if !paginationExpressionSafe(arg) {
				return false
			}
		}
		return true
	case *plan.Expr_F:
		if item.F == nil || item.F.Func == nil {
			return false
		}
		switch item.F.Func.ObjName {
		case "cast":
			return paginationCastConversionSafe(expr) && paginationExpressionSafe(item.F.Args[0])
		case "coalesce", "ifnull", "case", "isnull", "istrue", "isfalse", "not",
			"and", "or", "=", "<=>", "<>", "!=", "<", "<=", ">", ">=":
			for _, arg := range item.F.Args {
				if !paginationExpressionSafe(arg) {
					return false
				}
			}
			return true
		}
	}
	return false
}

// Prove the conversion itself is total; callers separately prove the input.
func paginationCastConversionSafe(expr *plan.Expr) bool {
	f := expr.GetF()
	if f == nil || f.Func == nil || f.Func.ObjName != "cast" || len(f.Args) != 2 ||
		f.Args[0] == nil || f.Args[1] == nil || f.Args[1].GetT() == nil {
		return false
	}
	sourceWidth, sourceSigned, sourceOK := integerTypeWidth(types.T(f.Args[0].Typ.Id))
	targetWidth, targetSigned, targetOK := integerTypeWidth(types.T(expr.Typ.Id))
	// A decimal128 literal conversion with at least 20 integer digits can
	// represent every signed or unsigned integer input exactly.
	if sourceOK && f.Args[0].GetLit() != nil && types.T(expr.Typ.Id) == types.T_decimal128 &&
		expr.Typ.Scale >= 0 && expr.Typ.Width-expr.Typ.Scale >= 20 {
		return true
	}
	return sourceOK && targetOK && sourceSigned == targetSigned && sourceWidth <= targetWidth
}

// Evaluating an aggregate for an unrelated inner key must not create an
// error that the correlated subquery would not have seen. SUM and AVG, for
// example, can overflow and therefore are not admitted by this proof.
func paginationAggregateSafe(expr *plan.Expr) bool {
	if expr == nil || expr.GetF() == nil || expr.GetF().Func == nil {
		return false
	}
	fn := expr.GetF()
	switch fn.Func.ObjName {
	case "count", "starcount", "min", "max":
		for _, arg := range fn.Args {
			if !paginationExpressionSafe(arg) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// Direct-outer projection normalization can discard sorting or pagination.
// Only discard it when expressions beneath it are safe to move or omit.
func (builder *QueryBuilder) paginationSubtreeExpressionsSafe(nodeID int32, allowAggregate bool) bool {
	node := builder.qry.Nodes[nodeID]
	if node.NodeType == plan.Node_AGG && !allowAggregate {
		return false
	}
	for _, exprs := range [][]*plan.Expr{node.ProjectList, node.FilterList, node.OnList, node.GroupBy} {
		for _, expr := range exprs {
			if !paginationExpressionSafe(expr) {
				return false
			}
		}
	}
	for _, order := range node.OrderBy {
		if order == nil || !paginationExpressionSafe(order.Expr) {
			return false
		}
	}
	for _, child := range node.Children {
		if !builder.paginationSubtreeExpressionsSafe(child, allowAggregate) {
			return false
		}
	}
	return len(node.WinSpecList) == 0
}

// A primary key or source row ID is unique here only if no operator can
// duplicate it or hide it. Derived tables and joins need per-occurrence IDs.
func (builder *QueryBuilder) paginationOuterIdentity(nodeID int32) *plan.Expr {
	for {
		node := builder.qry.Nodes[nodeID]
		switch node.NodeType {
		case plan.Node_TABLE_SCAN:
			if node.TableDef == nil || len(node.BindingTags) != 1 {
				return nil
			}
			// A narrow integer primary key avoids carrying the physical Row_ID
			// through every joined candidate. Other keys use the physical ID.
			if pk := node.TableDef.Pkey; pk != nil && len(pk.Names) == 1 &&
				pk.Names[0] == pk.PkeyColName && !catalog.IsFakePkName(pk.PkeyColName) {
				if pos, ok := node.TableDef.Name2ColIndex[pk.PkeyColName]; ok && pos >= 0 && int(pos) < len(node.TableDef.Cols) {
					col := node.TableDef.Cols[pos]
					if col.Typ.NotNullable && correlatedPaginationPartitionTypeSupported(types.T(col.Typ.Id)) {
						return GetColExpr(col.Typ, node.BindingTags[0], pos)
					}
				}
			}
			pos, ok := node.TableDef.Name2ColIndex[catalog.Row_ID]
			if !ok || pos < 0 || int(pos) >= len(node.TableDef.Cols) || types.T(node.TableDef.Cols[pos].Typ.Id) != types.T_Rowid {
				return nil
			}
			typ := node.TableDef.Cols[pos].Typ
			typ.NotNullable = true
			return GetColExpr(typ, node.BindingTags[0], pos)
		case plan.Node_FILTER, plan.Node_SORT:
			if len(node.Children) != 1 || len(node.ProjectList) != 0 {
				return nil
			}
			nodeID = node.Children[0]
		default:
			return nil
		}
	}
}

// A direct scan column is already NULL-extended by the LEFT JOIN. Expressions
// and constants need an independent marker because optimizer projection
// rewrites can otherwise evaluate them on a synthetic unmatched row.
func (builder *QueryBuilder) paginationDirectScanResult(nodeID int32, ctx *BindContext) bool {
	if len(ctx.projects) == 0 {
		return false
	}
	col := ctx.projects[0].GetCol()
	if col == nil {
		return false
	}
	for {
		node := builder.qry.Nodes[nodeID]
		if node.NodeType == plan.Node_TABLE_SCAN {
			return node.TableDef != nil && len(node.BindingTags) == 1 && col.RelPos == node.BindingTags[0] &&
				col.ColPos >= 0 && int(col.ColPos) < len(node.TableDef.Cols)
		}
		if len(node.Children) != 1 {
			return false
		}
		nodeID = node.Children[0]
	}
}

// Only literals and the parameter conversion produced by LimitBinder are
// statement-constant bounds that can be retained without a window rewrite.
func paginationPreservableBound(expr *plan.Expr) (supported, literal bool) {
	if expr == nil {
		return true, true
	}
	if _, ok := getLiteralUint64(expr); ok {
		return true, true
	}
	if expr.GetP() != nil {
		return types.T(expr.Typ.Id) == types.T_uint64, false
	}
	f := expr.GetF()
	if f == nil || f.Func == nil || len(f.Args) != 2 || f.Args[0] == nil || f.Args[1] == nil {
		return false, false
	}
	fid, _ := function.DecodeOverloadID(f.Func.Obj)
	return fid == function.CAST && types.T(expr.Typ.Id) == types.T_uint64 &&
		f.Args[0].GetP() != nil && f.Args[1].GetT() != nil && types.T(f.Args[1].Typ.Id) == types.T_uint64, false
}

// The existing volatility metadata does not cover every expression variant.
// In particular, RANDOM_BYTES is currently not marked volatile in the registry.
func paginationPreservableExpr(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch item := expr.Expr.(type) {
	case *plan.Expr_Col:
		return item.Col != nil
	case *plan.Expr_Corr:
		return item.Corr != nil
	case *plan.Expr_Lit:
		return item.Lit != nil
	case *plan.Expr_P:
		return item.P != nil
	case *plan.Expr_T:
		return item.T != nil
	case *plan.Expr_F:
		if item.F == nil || item.F.Func == nil {
			return false
		}
		fid, _ := function.DecodeOverloadID(item.F.Func.Obj)
		if fid == function.RANDOM_BYTES {
			return false
		}
		for _, arg := range item.F.Args {
			if !paginationPreservableExpr(arg) {
				return false
			}
		}
		return true
	case *plan.Expr_List:
		if item.List == nil {
			return false
		}
		for _, arg := range item.List.List {
			if !paginationPreservableExpr(arg) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// DISTINCT is encoded in the aggregate overload ID, but is not a volatile
// function. Only the single-aggregate preservation proof uses this result;
// ordinary pagination rewrites retain the conservative volatility check.
func paginationAggregateMayChangeWhenRepeated(expr *plan.Expr) bool {
	if f := expr.GetF(); f != nil && f.Func != nil && f.Func.Obj < 0 {
		normalized := DeepCopyExpr(expr)
		normalized.GetF().Func.Obj &= function.DistinctMask
		return exprMayChangeWhenRepeated(normalized)
	}
	return exprMayChangeWhenRepeated(expr)
}

// Prove that pagination cannot remove the single implicit aggregate row and
// that the existing aggregate owner can handle a synthetic unmatched row.
func (builder *QueryBuilder) paginationRedundantAggregateLimit(subID, boundaryID int32, ctx *BindContext) bool {
	if subID != boundaryID || len(ctx.groups) != 0 || len(ctx.aggregates) != 1 || len(ctx.results) != 1 {
		return false
	}
	project := builder.qry.Nodes[subID]
	limit, literal := getLiteralUint64(project.Limit)
	if project.NodeType != plan.Node_PROJECT || !literal || limit == 0 || len(project.Children) != 1 ||
		len(project.ProjectList) != 1 || len(project.FilterList) != 0 || len(project.OrderBy) != 0 {
		return false
	}
	if project.Offset != nil {
		offset, ok := getLiteralUint64(project.Offset)
		if !ok || offset != 0 {
			return false
		}
	}
	agg := builder.qry.Nodes[project.Children[0]]
	if agg.NodeType != plan.Node_AGG || len(agg.Children) != 1 || len(agg.BindingTags) < 2 ||
		len(agg.GroupBy) != 0 || len(agg.AggList) != 1 || len(agg.FilterList) != 0 ||
		agg.Limit != nil || agg.Offset != nil || agg.RankOption != nil {
		return false
	}
	col := project.ProjectList[0].GetCol()
	if col == nil || col.RelPos != agg.BindingTags[1] || col.ColPos != 0 {
		return false
	}
	f := agg.AggList[0].GetF()
	if f == nil || f.Func == nil || len(f.Args) == 0 || !paginationPreservableExpr(agg.AggList[0]) {
		return false
	}
	fid, _ := function.DecodeOverloadID(f.Func.Obj & function.DistinctMask)
	if f.Func.Obj < 0 && fid != function.COUNT && fid != function.SUM && fid != function.AVG {
		return false
	}
	if len(f.Args) != 1 && (fid != function.COUNT || f.Func.Obj >= 0) {
		return false
	}
	inputID := agg.Children[0]
	input := builder.qry.Nodes[inputID]
	if input.Limit != nil || input.Offset != nil || input.RankOption != nil ||
		!builder.simplePaginationInput(inputID, inputID, true) {
		return false
	}
	for input.NodeType == plan.Node_FILTER && len(input.Children) == 1 {
		input = builder.qry.Nodes[input.Children[0]]
	}
	if input.NodeType != plan.Node_TABLE_SCAN || len(input.BindingTags) != 1 || input.TableDef == nil {
		return false
	}
	if fid == function.STARCOUNT {
		// The non-equality aggregate owner counts a real scan Row_ID instead
		// of counting the synthetic NULL row produced by a LEFT JOIN.
		lit := f.Args[0].GetLit()
		marker := builder.findRowIDColRef(inputID)
		return f.Func.ObjName == "starcount" && lit != nil && !lit.Isnull &&
			marker != nil && types.T(marker.Typ.Id) == types.T_Rowid
	}
	if fid != function.MIN && fid != function.MAX && fid != function.SUM &&
		fid != function.COUNT && fid != function.AVG {
		return false
	}
	hasNull := false
	for _, arg := range f.Args {
		safe, alwaysNull := paginationAggregateArgOnNull(arg, input.BindingTags[0], len(input.TableDef.Cols))
		if !safe {
			return false
		}
		hasNull = hasNull || alwaysNull
	}
	return hasNull
}

// Aggregate arguments are evaluated even for a synthetic unmatched join row.
// Prove both that evaluation is safe on that row and whether it yields NULL.
// The generic function STRICT flag is insufficient for SQL AND/OR/COALESCE.
func paginationAggregateArgOnNull(expr *plan.Expr, scanTag int32, scanCols int) (safe, alwaysNull bool) {
	if expr == nil {
		return false, false
	}
	if col := expr.GetCol(); col != nil {
		valid := col.RelPos == scanTag && col.ColPos >= 0 && int(col.ColPos) < scanCols
		return valid, valid
	}
	if lit := expr.GetLit(); lit != nil {
		return true, lit.Isnull
	}
	if expr.GetP() != nil {
		return true, false
	}
	f := expr.GetF()
	if f == nil || f.Func == nil || f.Func.Obj < 0 {
		return false, false
	}
	fid, overload := function.DecodeOverloadID(f.Func.Obj)
	args := f.Args
	switch fid {
	case function.COALESCE:
		if len(args) == 0 {
			return false, false
		}
		allNull := true
		for _, arg := range args {
			safe, alwaysNull := paginationAggregateArgOnNull(arg, scanTag, scanCols)
			if !safe {
				return false, false
			}
			allNull = allNull && alwaysNull
		}
		return true, allNull
	case function.ISNULL:
		if len(args) != 1 {
			return false, false
		}
		safe, _ := paginationAggregateArgOnNull(args[0], scanTag, scanCols)
		return safe, false
	case function.CASE:
		if len(args) < 3 || len(args)%2 == 0 {
			return false, false
		}
		allNull := true
		for i, arg := range args {
			safe, alwaysNull := paginationAggregateArgOnNull(arg, scanTag, scanCols)
			if !safe {
				return false, false
			}
			if i%2 == 1 || i == len(args)-1 {
				allNull = allNull && alwaysNull
			}
		}
		return true, allNull
	case function.ABS:
		if overload < 0 || overload > 5 || len(args) != 1 || args[0] == nil ||
			!paginationNumericType(types.T(expr.Typ.Id)) || !paginationNumericType(types.T(args[0].Typ.Id)) {
			return false, false
		}
	case function.CAST:
		if len(args) != 2 || args[1] == nil || args[1].GetT() == nil {
			return false, false
		}
		safe, alwaysNull := paginationAggregateArgOnNull(args[0], scanTag, scanCols)
		if !safe || alwaysNull {
			return safe, alwaysNull
		}
		return paginationCastConversionSafe(expr), false
	case function.PLUS, function.MINUS, function.MULTI, function.DIV, function.INTEGER_DIV, function.MOD:
		if overload != 0 || len(args) != 2 || !paginationNumericType(types.T(expr.Typ.Id)) ||
			args[0] == nil || !paginationNumericType(types.T(args[0].Typ.Id)) ||
			args[1] == nil || !paginationNumericType(types.T(args[1].Typ.Id)) {
			return false, false
		}
	default:
		return false, false
	}
	hasNull := false
	for _, arg := range args {
		safe, alwaysNull := paginationAggregateArgOnNull(arg, scanTag, scanCols)
		if !safe {
			return false, false
		}
		hasNull = hasNull || alwaysNull
	}
	// A call with no guaranteed NULL argument could still fail when evaluated.
	return hasNull, hasNull
}

func paginationNumericType(typ types.T) bool {
	return typ.IsInteger() || typ.IsFloat() || typ.IsDecimal()
}

// The post-join rewrite moves a root pagination boundary across only simple,
// deterministic projection/filter nodes. It never moves it across an inner
// aggregate, join, DISTINCT, nested LIMIT or other cardinality boundary.
func (builder *QueryBuilder) simplePaginationInput(nodeID, boundaryID int32, preserve bool) bool {
	crossed := false
	for {
		node := builder.qry.Nodes[nodeID]
		if nodeID == boundaryID {
			crossed = true
		} else if node.Limit != nil || node.Offset != nil || node.RankOption != nil {
			return false
		}
		if !crossed && node.NodeType != plan.Node_PROJECT && node.NodeType != plan.Node_SORT {
			return false
		}
		for _, exprs := range [][]*plan.Expr{node.ProjectList, node.FilterList} {
			for _, expr := range exprs {
				if expr == nil || exprMayChangeWhenRepeated(expr) || preserve && !paginationPreservableExpr(expr) {
					return false
				}
			}
		}
		for _, order := range node.OrderBy {
			if order == nil || order.Expr == nil || exprMayChangeWhenRepeated(order.Expr) || preserve && !paginationPreservableExpr(order.Expr) {
				return false
			}
		}
		switch node.NodeType {
		case plan.Node_TABLE_SCAN:
			return crossed && len(node.Children) == 0
		case plan.Node_PROJECT, plan.Node_FILTER, plan.Node_SORT:
			if len(node.Children) != 1 {
				return false
			}
			nodeID = node.Children[0]
		default:
			return false
		}
	}
}

// Expose a right-side column through the existing projections. A grouped
// scalar needs a COUNT output, which is non-NULL for every existing group.
// For LIMIT 0, any right-side column suffices because no row can reach the join.
func (builder *QueryBuilder) scalarSubqueryMarker(nodeID int32, ctx *BindContext, grouped bool) *plan.Expr {
	node := builder.qry.Nodes[nodeID]
	switch node.NodeType {
	case plan.Node_TABLE_SCAN:
		if grouped {
			return nil
		}
		return builder.findRowIDColRef(nodeID)
	case plan.Node_AGG:
		if len(node.BindingTags) < 2 || len(node.AggList) == 0 {
			return nil
		}
		if grouped {
			if node.BindingTags[1] != ctx.aggregateTag {
				return nil
			}
			for i, agg := range node.AggList {
				fn := agg.GetF()
				if fn != nil && fn.Func != nil && (fn.Func.ObjName == "count" || fn.Func.ObjName == "starcount") {
					return GetColExpr(agg.Typ, node.BindingTags[1], int32(i))
				}
			}
			return nil
		}
		return GetColExpr(node.AggList[0].Typ, node.BindingTags[1], 0)
	case plan.Node_PROJECT, plan.Node_FILTER, plan.Node_SORT, plan.Node_PARTITION, plan.Node_WINDOW:
		if len(node.Children) != 1 {
			return nil
		}
		marker := builder.scalarSubqueryMarker(node.Children[0], ctx, grouped)
		if marker == nil {
			return nil
		}
		if node.NodeType == plan.Node_PROJECT {
			if len(node.BindingTags) != 1 {
				return nil
			}
			marker = builder.pullupThroughProj(ctx, node, node.BindingTags[0], marker)
		}
		return marker
	default:
		return nil
	}
}

func (builder *QueryBuilder) nullExtendScalarValue(marker, value *plan.Expr) (*plan.Expr, error) {
	marker = DeepCopyExpr(marker)
	marker.Typ.NotNullable = false
	isNull, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "isnull", []*plan.Expr{marker})
	if err != nil {
		return nil, err
	}
	result, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "case", []*plan.Expr{
		isNull, makePlan2NullConstExprWithType(), value,
	})
	if err != nil {
		return nil, err
	}
	if !makeTypeByPlan2Expr(result).Eq(makeTypeByPlan2Expr(value)) {
		return nil, moerr.NewNYI(builder.GetContext(), "correlated pagination result type")
	}
	result.Typ.NotNullable = false
	return result, nil
}

// Apply all correlated predicates before ranking the matching rows for each
// outer row. The right-side marker distinguishes a real NULL value from the
// NULL-extended row produced by a LEFT JOIN.
func (builder *QueryBuilder) flattenPostJoinPagination(
	outerID, subID int32, subCtx *BindContext, preds []*plan.Expr, ctx *BindContext, pagination *postJoinPagination,
) (int32, *plan.Expr, error) {
	filters, joins := decreaseDepthAndDispatch(preds)
	if !pagination.crossed || len(filters) != 0 || len(subCtx.results) != 1 {
		return 0, nil, moerr.NewNYI(builder.GetContext(), "deep or incomplete correlated pagination")
	}
	value := GetColExpr(subCtx.results[0].Typ, subCtx.topTag(), 0)
	value.Typ.NotNullable = false
	if pagination.marker != nil {
		var err error
		value, err = builder.nullExtendScalarValue(pagination.marker, value)
		if err != nil {
			return 0, nil, err
		}
	}
	value.Typ.NotNullable = false
	joined := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{outerID, subID},
		JoinType: plan.Node_LEFT,
		OnList:   joins,
		SpillMem: builder.joinSpillMem,
	}, ctx)
	partition := []*plan.Expr{pagination.outerID}
	ranked, rank, err := builder.appendPaginationWindow(joined, ctx, partition, pagination.orderBy, "row_number", nil)
	if err != nil {
		return 0, nil, err
	}
	if pagination.offset > 0 {
		// The second window orders by the first one's unique row number. Two
		// independent sorts on tied user keys need not choose the same order.
		byRank := []*plan.OrderBySpec{{Expr: DeepCopyExpr(rank), Flag: plan.OrderBySpec_INTERNAL}}
		ranked, value, err = builder.appendPaginationWindow(ranked, ctx, partition, byRank, "lead", []*plan.Expr{
			value, makePlan2Int64ConstExprWithType(int64(pagination.offset)),
		})
		if err != nil {
			return 0, nil, err
		}
	}
	first, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{rank, makePlan2Int64ConstExprWithType(1)})
	if err != nil {
		return 0, nil, err
	}
	ranked = builder.appendNode(&plan.Node{
		NodeType:   plan.Node_FILTER,
		Children:   []int32{ranked},
		FilterList: []*plan.Expr{first},
	}, ctx)
	return ranked, value, nil
}

func (builder *QueryBuilder) appendPaginationWindow(
	input int32, ctx *BindContext, partition []*plan.Expr, order []*plan.OrderBySpec, name string, args []*plan.Expr,
) (int32, *plan.Expr, error) {
	tag := builder.genNewBindTag()
	partitionOrder := make([]*plan.OrderBySpec, len(partition))
	for i, key := range partition {
		partitionOrder[i] = &plan.OrderBySpec{Expr: DeepCopyExpr(key), Flag: plan.OrderBySpec_INTERNAL}
	}
	input = builder.appendNode(&plan.Node{
		NodeType: plan.Node_PARTITION, Children: []int32{input},
		OrderBy: partitionOrder, BindingTags: []int32{tag},
	}, ctx)
	fn, err := BindFuncExprImplByPlanExpr(builder.GetContext(), name, args)
	if err != nil {
		return 0, nil, err
	}
	window := &plan.Expr{Typ: fn.Typ, Expr: &plan.Expr_W{W: &plan.WindowSpec{
		WindowFunc: fn, Name: name, PartitionBy: DeepCopyExprList(partition), OrderBy: DeepCopyOrderBySpecList(order),
		Frame: &plan.FrameClause{
			Type:  plan.FrameClause_ROWS,
			Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, UnBounded: true},
			End:   &plan.FrameBound{Type: plan.FrameBound_FOLLOWING, UnBounded: true},
		},
	}}}
	input = builder.appendNode(&plan.Node{
		NodeType: plan.Node_WINDOW, Children: []int32{input}, WinSpecList: []*plan.Expr{window},
		WindowIdx: 0, BindingTags: []int32{tag},
	}, ctx)
	return input, GetColExpr(fn.Typ, tag, 0), nil
}

// rewriteCorrelatedPagination preserves the evaluation boundary of LIMIT and
// OFFSET while decorrelating an equality-correlated subquery. Before predicate
// pull-up, pagination runs once for every outer correlation key. After pull-up,
// a plain LIMIT on the right input would run once globally and silently remove
// rows belonging to other keys.
//
// Rewrite the pagination as ROW_NUMBER over the inner equality keys, then keep
// the requested row-number interval. Predicates that cannot define stable
// inner partitions are rejected instead of being executed with global limit
// semantics. In particular, a non-equality predicate may select a different
// set for two outer rows that share every equality key.
func (builder *QueryBuilder) rewriteCorrelatedPagination(
	nodeID int32,
	node *plan.Node,
	preds []*plan.Expr,
	ctx *BindContext,
	subqueryType plan.SubqueryRef_Type,
	isSubqueryRoot, allowByteKeys bool,
) (int32, error) {
	limit, literal := getLiteralUint64(node.Limit)
	if !literal {
		return 0, moerr.NewNYI(builder.GetContext(), "dynamic LIMIT in correlated subquery")
	}
	if limit == 0 {
		// An empty right input is empty for every correlation key, so the global
		// zero-row plan already has the same scalar and existential semantics.
		return nodeID, nil
	}

	offset := uint64(0)
	if node.Offset != nil {
		var ok bool
		offset, ok = getLiteralUint64(node.Offset)
		if !ok {
			return 0, moerr.NewNYI(builder.GetContext(), "dynamic OFFSET in correlated subquery")
		}
	}
	if offset > math.MaxInt64 || limit > uint64(math.MaxInt64)-offset {
		return 0, moerr.NewNYI(builder.GetContext(), "correlated LIMIT or OFFSET larger than INT64_MAX")
	}
	if node.RankOption != nil {
		return 0, moerr.NewNYI(builder.GetContext(), "correlated LIMIT with BY RANK")
	}
	if offset == 0 && isSubqueryRoot {
		switch subqueryType {
		case plan.SubqueryRef_EXISTS, plan.SubqueryRef_NOT_EXISTS:
			// A positive LIMIT and its ordering cannot change whether the
			// complete existential input is empty.  The semantic root can be
			// below transparent PROJECT/SORT wrappers, so remove the complete
			// pagination boundary before validating or copying its ORDER BY.
			node.Limit = nil
			node.Offset = nil
			if node.NodeType == plan.Node_SORT {
				if len(node.Children) != 1 {
					return 0, moerr.NewInternalError(builder.GetContext(), "correlated LIMIT sort must have one child")
				}
				return node.Children[0], nil
			}
			node.OrderBy = nil
			return nodeID, nil
		}
	}
	for _, pred := range preds {
		if !allCorrColsAtDepthOne(pred) {
			if subqueryType == plan.SubqueryRef_SCALAR {
				// Preserve the established deep-scalar rejection below, including
				// its public diagnostic, instead of partially rewriting its limit.
				return nodeID, nil
			}
			return 0, moerr.NewNYI(builder.GetContext(), "deeply correlated LIMIT")
		}
	}

	partitionBy, ok := correlatedPaginationPartitionKeys(preds)
	if !ok {
		return 0, moerr.NewNYI(builder.GetContext(), "correlated LIMIT with non-equality predicates")
	}
	if len(partitionBy) == 0 {
		// Correlated predicates depending only on the outer row do not change
		// the right-side set, so its pagination is legitimately global.
		return nodeID, nil
	}
	for _, key := range partitionBy {
		if typ := types.T(key.Typ.Id); !correlatedPaginationPartitionTypeSupported(typ) &&
			!(allowByteKeys && correlatedPaginationByteKeySupported(typ)) {
			return 0, moerr.NewNYIf(builder.GetContext(),
				"correlated LIMIT partition key type %s", makeTypeByPlan2Expr(key))
		}
	}

	inputID := nodeID
	if node.NodeType == plan.Node_SORT {
		if len(node.Children) != 1 {
			return 0, moerr.NewInternalError(builder.GetContext(), "correlated LIMIT sort must have one child")
		}
		inputID = node.Children[0]
	}
	orderBy := DeepCopyOrderBySpecList(node.OrderBy)
	node.Limit = nil
	node.Offset = nil

	windowID, rowNumberCol, err := builder.appendPaginationWindow(inputID, ctx, partitionBy, orderBy, "row_number", nil)
	if err != nil {
		return 0, err
	}
	upperBound := makePlan2Int64ConstExprWithType(int64(offset + limit))
	rowFilter, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "<=", []*plan.Expr{
		DeepCopyExpr(rowNumberCol),
		upperBound,
	})
	if err != nil {
		return 0, err
	}
	if offset > 0 {
		lowerBound := makePlan2Int64ConstExprWithType(int64(offset))
		afterOffset, bindErr := BindFuncExprImplByPlanExpr(builder.GetContext(), ">", []*plan.Expr{
			DeepCopyExpr(rowNumberCol),
			lowerBound,
		})
		if bindErr != nil {
			return 0, bindErr
		}
		rowFilter, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "and", []*plan.Expr{
			afterOffset,
			rowFilter,
		})
		if err != nil {
			return 0, err
		}
	}

	builder.qry.Nodes[windowID].FilterList = []*plan.Expr{rowFilter}
	return windowID, nil
}

// The preflight visits every node under the boundary, so projected aliases
// need no separate graph chase to find their correlated expressions.
func paginationHasCorrCol(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch item := expr.Expr.(type) {
	case *plan.Expr_Corr:
		return true
	case *plan.Expr_F:
		for _, arg := range item.F.Args {
			if paginationHasCorrCol(arg) {
				return true
			}
		}
	case *plan.Expr_List:
		for _, arg := range item.List.List {
			if paginationHasCorrCol(arg) {
				return true
			}
		}
	case *plan.Expr_W:
		if item.W == nil {
			return false
		}
		if paginationHasCorrCol(item.W.WindowFunc) {
			return true
		}
		for _, arg := range item.W.PartitionBy {
			if paginationHasCorrCol(arg) {
				return true
			}
		}
		for _, order := range item.W.OrderBy {
			if order != nil && paginationHasCorrCol(order.Expr) {
				return true
			}
		}
	}
	return false
}

func correlatedPaginationPartitionKeys(preds []*plan.Expr) ([]*plan.Expr, bool) {
	keys := make([]*plan.Expr, 0, len(preds))
	for _, pred := range preds {
		if !collectCorrelatedPaginationPartitionKeys(pred, &keys) {
			return nil, false
		}
	}
	return keys, true
}

func collectCorrelatedPaginationPartitionKeys(expr *plan.Expr, keys *[]*plan.Expr) bool {
	if expr == nil || !hasCorrCol(expr) {
		return true
	}
	// An outer-only condition either admits every row in a partition or none.
	if !exprHasColRef(expr) {
		return allCorrColsAtDepthOne(expr)
	}

	fn := expr.GetF()
	if fn != nil && fn.Func != nil && fn.Func.ObjName == "and" {
		for _, arg := range fn.Args {
			if !collectCorrelatedPaginationPartitionKeys(arg, keys) {
				return false
			}
		}
		return true
	}

	if fn != nil && fn.Func != nil &&
		(fn.Func.ObjName == "=" || fn.Func.ObjName == "<=>") && len(fn.Args) == 2 {
		leftCorr := hasCorrCol(fn.Args[0])
		rightCorr := hasCorrCol(fn.Args[1])
		if leftCorr != rightCorr {
			outerExpr := fn.Args[0]
			innerExpr := fn.Args[1]
			if rightCorr {
				outerExpr, innerExpr = innerExpr, outerExpr
			}
			// Partition identity must agree with the equality used by the JOIN.
			// Restrict this path to a direct, same-typed column comparison; casts,
			// functions and other SQL comparison domains need a separate proof.
			if outerExpr.GetCorr() == nil || outerExpr.GetCorr().Depth != 1 ||
				innerExpr.GetCol() == nil || outerExpr.Typ.Id != innerExpr.Typ.Id {
				return false
			}
			*keys = append(*keys, DeepCopyExpr(innerExpr))
			return true
		}
	}

	// A predicate depending only on outer columns does not change which inner
	// rows belong to an equality partition; the eventual join still applies it.
	return !exprHasColRef(expr) && allCorrColsAtDepthOne(expr)
}

func exprHasColRef(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch item := expr.Expr.(type) {
	case *plan.Expr_Col:
		return true
	case *plan.Expr_F:
		for _, arg := range item.F.Args {
			if exprHasColRef(arg) {
				return true
			}
		}
	case *plan.Expr_List:
		for _, arg := range item.List.List {
			if exprHasColRef(arg) {
				return true
			}
		}
	}
	return false
}

func allCorrColsAtDepthOne(expr *plan.Expr) bool {
	if expr == nil {
		return true
	}
	switch item := expr.Expr.(type) {
	case *plan.Expr_Corr:
		return item.Corr != nil && item.Corr.Depth == 1
	case *plan.Expr_F:
		for _, arg := range item.F.Args {
			if !allCorrColsAtDepthOne(arg) {
				return false
			}
		}
	case *plan.Expr_List:
		for _, arg := range item.List.List {
			if !allCorrColsAtDepthOne(arg) {
				return false
			}
		}
	}
	return true
}

// Only types whose partition identity is known to match SQL equality are
// admitted here. In particular, this branch groups JSON by its raw bytes.
func correlatedPaginationPartitionTypeSupported(typ types.T) bool {
	switch typ {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		return true
	default:
		return false
	}
}

// These direct same-type SQL equality keys and window partitions both use
// byte identity. Other variable-length types (such as JSON) need their own
// equality proof before they can be used as partition keys.
func correlatedPaginationByteKeySupported(typ types.T) bool {
	switch typ {
	case types.T_char, types.T_varchar:
		return true
	default:
		return false
	}
}
