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
	"math"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// pushDownOuterCounts replaces a grouped COUNT over a key-preserving outer join
// with fact-side counts and a projection. The catalog proves multiplicity;
// estimates only decide whether to pay for the earlier aggregation.
func (builder *QueryBuilder) pushDownOuterCounts(rootID int32) bool {
	if builder.qry.StmtType != plan.Query_SELECT || builder.isForUpdate || builder.qry.Correlation != nil ||
		(builder.optimizerHints != nil && builder.optimizerHints.aggPushDown != 0) {
		return false
	}
	owners := make(map[int32]int)
	var count func(int32)
	count = func(id int32) {
		owners[id]++
		if owners[id] == 1 {
			for _, child := range builder.qry.Nodes[id].Children {
				count(child)
			}
		}
	}
	count(rootID)
	skipped := false
	for _, root := range builder.qry.Steps {
		if root == rootID && !skipped {
			skipped = true
			continue
		}
		count(root)
	}
	outputs := make(map[[2]int32]*plan.Expr)
	var visit func(int32)
	visit = func(id int32) {
		if owners[id] != 1 {
			return
		}
		n := builder.qry.Nodes[id]
		for _, child := range n.Children {
			visit(child)
		}
		if n.NodeType == plan.Node_AGG {
			builder.pushOuterCount(n, owners, outputs)
		}
	}
	visit(rootID)
	if len(outputs) != 0 {
		builder.applyEffectlessAggRemap(rootID, outputs)
	}
	return len(outputs) != 0
}

func (builder *QueryBuilder) pushOuterCount(agg *plan.Node, owners map[int32]int, outputs map[[2]int32]*plan.Expr) {
	if len(agg.Children) != 1 || len(agg.BindingTags) != 2 || len(agg.GroupBy) != 1 || len(agg.AggList) != 1 ||
		len(agg.FilterList) != 0 || len(agg.BlockFilterList) != 0 || slices.Contains(agg.GroupingFlag, false) ||
		len(agg.PhysicalEqualityKeyList) != 0 || !scalarAggPlainNode(agg) ||
		len(builder.scalarReaggAliases[agg.NodeId]) != 0 {
		return
	}
	join := builder.qry.Nodes[agg.Children[0]]
	if owners[join.NodeId] != 1 || join.NodeType != plan.Node_JOIN || join.JoinType != plan.Node_LEFT ||
		len(join.Children) != 2 || joinNodeHasLocalSemantics(join) || join.FilterIsBarrier ||
		len(join.BlockFilterList) != 0 || !scalarAggPlainNode(join) || builder.joinOwnsConstantDiagnostic(join) {
		return
	}
	left, right := builder.qry.Nodes[join.Children[0]], builder.qry.Nodes[join.Children[1]]
	if owners[left.NodeId] != 1 || owners[right.NodeId] != 1 || !outerCountPlainScan(left) || !outerCountPlainScan(right) {
		return
	}
	pk, ok := sqlEqualityCompatiblePrimaryKeyColumnPositions(left.TableDef)
	group := agg.GroupBy[0].GetCol()
	if !ok || len(pk) != 1 || group == nil || group.RelPos != left.BindingTags[0] || group.ColPos != pk[0] ||
		!types.T(agg.GroupBy[0].Typ.Id).IsInteger() ||
		!sqlEqualityJoinUsesOneIdentityDomain(agg.GroupBy[0].Typ, left.TableDef.Cols[pk[0]].Typ) {
		return
	}
	countExpr := agg.AggList[0]
	fn := countExpr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 1 || uint64(fn.Func.Obj)&function.Distinct != 0 ||
		len(fn.AggConfig) != 0 || fn.AggConfigType != plan.AggregateConfigType_AGG_CONFIG_NONE ||
		!canonicalBoundFunction(builder.GetContext(), countExpr, "count") {
		return
	}
	countID, _ := function.DecodeOverloadID(fn.Func.Obj)
	arg := fn.Args[0].GetCol()
	if countID != function.COUNT || arg == nil || arg.RelPos != right.BindingTags[0] ||
		arg.ColPos < 0 || int(arg.ColPos) >= len(right.TableDef.Cols) ||
		types.T(countExpr.Typ.Id) != types.T_int64 || !countExpr.Typ.NotNullable {
		return
	}
	var key *plan.Expr
	var equality *plan.Expr
	var filters []*plan.Expr
	rightTags := map[int32]bool{right.BindingTags[0]: true}
	for _, pred := range splitPlanConjunctions(join.OnList) {
		l, r := getJoinCondCol(pred, left.BindingTags[0], right.BindingTags[0])
		if l != nil && r != nil {
			f := pred.GetF()
			if equality != nil || l.Col.ColPos != pk[0] || r.Col.ColPos < 0 || int(r.Col.ColPos) >= len(right.TableDef.Cols) ||
				!sqlEqualityJoinUsesOneIdentityDomain(f.Args[0].Typ, f.Args[1].Typ) ||
				!isTruncationSafePredicateExpr(pred) {
				return
			}
			equality = pred
			key = GetColExpr(right.TableDef.Cols[r.Col.ColPos].Typ, right.BindingTags[0], r.Col.ColPos)
			continue
		}
		if !containsTag(pred, right.BindingTags[0]) || !containsOnlyTags(pred, rightTags) || !outerCountPredicateTotal(pred) {
			return
		}
		filters = append(filters, DeepCopyExpr(pred))
	}
	if key == nil || !types.T(key.Typ.Id).IsInteger() ||
		!sqlEqualityJoinUsesOneIdentityDomain(key.Typ, agg.GroupBy[0].Typ) {
		return
	}
	groups := estimateNDVAfterSelection(getExprNdv(key, builder), right.Stats)
	if w := builder.getStatsInfoByCol(key.GetCol()); w != nil && w.GetStats() != nil {
		stats := w.GetStats()
		name := key.GetCol().Name
		lo, hasLo := stats.MinValMap[name]
		hi, hasHi := stats.MaxValMap[name]
		oid, hasType := stats.DataTypeMap[name]
		if hasLo && hasHi && hasType && oid == uint64(key.Typ.Id) {
			groups = outerCountIntegerSpanCap(groups, key.Typ, lo, hi)
		}
	}
	if !outerCountCostAllows(right.Stats, left.Stats, groups) {
		return
	}

	// Bind all fallible proposal expressions before publishing any nodes/edges.
	partialTags := []int32{builder.genNewBindTag(), builder.genNewBindTag()}
	projectTag := builder.genNewBindTag()
	partialKey := GetColExpr(key.Typ, partialTags[0], 0)
	newOn := DeepCopyExpr(equality)
	replaceColumnsForExprList([]*plan.Expr{newOn}, map[[2]int32]*plan.Expr{
		{key.GetCol().RelPos, key.GetCol().ColPos}: partialKey,
	})
	nullableCount := countExpr.Typ
	nullableCount.NotNullable = false
	countRef := GetColExpr(nullableCount, partialTags[1], 0)
	zero := makePlan2Int64ConstExprWithType(0)
	restored, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "coalesce", []*plan.Expr{countRef, zero})
	if err != nil || !isSameColumnType(restored.Typ, countExpr.Typ) {
		return
	}
	// Both COALESCE arguments have COUNT's exact int64 domain, and zero is
	// non-NULL. This restores the original public COUNT metadata as well.
	restored.Typ = countExpr.Typ
	ctx := builder.ctxByNode[agg.NodeId]
	factID := right.NodeId
	if len(filters) != 0 {
		factID = builder.appendNode(&plan.Node{NodeType: plan.Node_FILTER, Children: []int32{factID}, FilterList: filters}, ctx)
	}
	partialID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_AGG, Children: []int32{factID}, BindingTags: partialTags,
		GroupBy: []*plan.Expr{key}, AggList: []*plan.Expr{DeepCopyExpr(countExpr)}, SpillMem: builder.aggSpillMem,
	}, ctx)
	join.Children[1], join.OnList, join.IsRightJoin = partialID, []*plan.Expr{newOn}, false
	project := []*plan.Expr{DeepCopyExpr(agg.GroupBy[0]), restored}
	for i, tag := range agg.BindingTags {
		ref := [2]int32{tag, 0}
		outputs[ref] = GetColExpr(project[i].Typ, projectTag, int32(i))
		builder.nameByColRef[[2]int32{projectTag, int32(i)}] = builder.nameByColRef[ref]
		delete(builder.tag2NodeID, tag)
	}
	builder.tag2NodeID[projectTag] = agg.NodeId
	builder.qry.Nodes[agg.NodeId] = &plan.Node{
		NodeId: agg.NodeId, NodeType: plan.Node_PROJECT, Children: []int32{join.NodeId},
		BindingTags: []int32{projectTag}, ProjectList: project,
	}
}

func outerCountCostAllows(fact, preserved *plan.Stats, groups float64) bool {
	valid := func(n float64) bool { return n > 0 && !math.IsNaN(n) && !math.IsInf(n, 0) }
	return fact != nil && preserved != nil && valid(fact.Outcnt) && valid(preserved.Outcnt) && valid(groups) &&
		fact.Outcnt >= 4*hashmap.UnitLimit && groups <= fact.Outcnt/4 && groups <= preserved.Outcnt
}

// Statistics may overestimate NDV across overlapping objects. For small integer
// keys, exactly represented min/max values provide an independent domain bound.
// This affects profitability only; catalog proofs still establish legality.
func outerCountIntegerSpanCap(groups float64, key plan.Type, lo, hi float64) float64 {
	if !finitePositive(groups) || math.IsNaN(lo) || math.IsNaN(hi) || math.IsInf(lo, 0) || math.IsInf(hi, 0) ||
		lo > hi || math.Trunc(lo) != lo || math.Trunc(hi) != hi || lo == 0 && hi == 0 {
		// UpdateStatsInfo also uses [0,0] for an uninitialized zone map.
		// Do not treat that ambiguous sentinel as a proven singleton domain.
		return groups
	}
	var lower, upper float64
	switch types.T(key.Id) {
	case types.T_int8:
		lower, upper = math.MinInt8, math.MaxInt8
	case types.T_int16:
		lower, upper = math.MinInt16, math.MaxInt16
	case types.T_int32:
		lower, upper = math.MinInt32, math.MaxInt32
	case types.T_uint8:
		upper = math.MaxUint8
	case types.T_uint16:
		upper = math.MaxUint16
	case types.T_uint32:
		upper = math.MaxUint32
	default:
		return groups
	}
	if lo < lower || hi > upper {
		return groups
	}
	span := hi - lo + 1
	if !key.NotNullable {
		span++
	}
	return math.Min(groups, span)
}

func outerCountPlainScan(scan *plan.Node) bool {
	if scan.NodeType != plan.Node_TABLE_SCAN || len(scan.Children) != 0 || len(scan.BindingTags) != 1 ||
		scan.TableDef == nil || scan.ParentObjRef != nil || scan.IndexReaderParam != nil || scan.IndexSearchScan != nil ||
		scan.ExternScan != nil || scan.ClusterTable != nil || !scalarAggPlainNode(scan) {
		return false
	}
	// Reuse the complete local-owner audit after proving the deliberately
	// scoped LIKE extension. Do not widen unrelated optimizer totality rules.
	proof := *scan
	for _, list := range [][]*plan.Expr{scan.FilterList, scan.BlockFilterList} {
		for _, pred := range list {
			if !outerCountPredicateTotal(pred) {
				return false
			}
		}
	}
	proof.FilterList, proof.BlockFilterList = nil, nil
	return SetMembershipNodeEvaluationIsTotal(&proof)
}

func outerCountPredicateTotal(expr *plan.Expr) bool {
	if isTruncationSafePredicateExpr(expr) {
		return true
	}
	if expr == nil || types.T(expr.Typ.Id) != types.T_bool {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	overload, ok := function.GetFunctionByIdWithoutError(fn.Func.Obj)
	if !ok || overload.CannotFold() || overload.IsRealTimeRelated() {
		return false
	}
	id, _ := function.DecodeOverloadID(fn.Func.Obj)
	switch id {
	case function.NOT, function.ISTRUE:
		return len(fn.Args) == 1 && outerCountPredicateTotal(fn.Args[0])
	case function.AND, function.OR:
		return len(fn.Args) == 2 && outerCountPredicateTotal(fn.Args[0]) && outerCountPredicateTotal(fn.Args[1])
	case function.LIKE:
		if len(fn.Args) != 2 || fn.Args[0].GetCol() == nil || fn.Args[1].GetLit() == nil || fn.Args[1].GetLit().Src != nil {
			return false
		}
		source, pattern := fn.Args[0].Typ, fn.Args[1].Typ
		oid := types.T(source.Id)
		// These text matchers return a boolean (or NULL), including regexp
		// compilation refusal. Unicode-ci has a data-dependent resource-limit
		// error and explicit ESCAPE has argument validation: neither is moved.
		return (oid == types.T_char || oid == types.T_varchar) && source.Width > 0 &&
			(source.Charset == uint32(types.CharsetLegacy) || source.Charset == uint32(types.CharsetUTF8)) &&
			source.Charset == pattern.Charset && !fn.Args[1].GetLit().IsBin
	default:
		return false
	}
}
