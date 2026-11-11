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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// pushDownGroupedSums delays dimension payload construction until after a
// narrow partial aggregation. The final grouping is retained: distinct primary
// keys may still have identical dimension attributes. Estimates decide only
// profitability; total exact arguments and the join domain prove legality.
func (builder *QueryBuilder) pushDownGroupedSums(rootID int32) int32 {
	if builder.optimizerHints != nil && builder.optimizerHints.aggPushDown != 0 {
		return rootID
	}
	parents := make(map[int32]int)
	var count func(int32)
	count = func(id int32) {
		parents[id]++
		if parents[id] > 1 {
			return
		}
		for _, child := range builder.qry.Nodes[id].Children {
			count(child)
		}
	}
	count(rootID)
	skippedCurrentRoot := false
	for _, stepRoot := range builder.qry.Steps {
		if stepRoot == rootID && !skippedCurrentRoot {
			skippedCurrentRoot = true
			continue
		}
		count(stepRoot)
	}
	outputs := make(map[[2]int32]*plan.Expr)
	var visit func(int32) int32
	visit = func(id int32) int32 {
		n := builder.qry.Nodes[id]
		if parents[id] != 1 {
			return id
		}
		for i, child := range n.Children {
			n.Children[i] = visit(child)
		}
		if n.NodeType != plan.Node_AGG || len(n.Children) != 1 ||
			len(n.BindingTags) != 2 || len(n.GroupBy) == 0 || len(n.AggList) == 0 ||
			slices.Contains(n.GroupingFlag, false) || len(n.PhysicalEqualityKeyList) != 0 ||
			!scalarAggPlainNode(n) {
			return id
		}
		join := builder.qry.Nodes[n.Children[0]]
		if parents[join.NodeId] != 1 || join.NodeType != plan.Node_JOIN ||
			join.JoinType != plan.Node_INNER || len(join.Children) != 2 ||
			joinNodeHasLocalSemantics(join) || join.FilterIsBarrier || builder.joinOwnsConstantDiagnostic(join) {
			return id
		}
		for side := range join.Children {
			if result, ok := builder.pushGroupedSumSide(n, join, side, outputs); ok {
				return result
			}
		}
		return id
	}
	rootID = visit(rootID)
	if len(outputs) != 0 {
		builder.applyEffectlessAggRemap(rootID, outputs)
	}
	return rootID
}

func (builder *QueryBuilder) pushGroupedSumSide(
	agg, join *plan.Node, side int, outputs map[[2]int32]*plan.Expr,
) (int32, bool) {
	fact := builder.qry.Nodes[join.Children[side]]
	dim := builder.qry.Nodes[join.Children[1-side]]
	if dim.NodeType != plan.Node_TABLE_SCAN || len(dim.BindingTags) != 1 ||
		!scalarAggPlainNode(dim) || dim.FilterIsBarrier || fact.Stats == nil ||
		dim.Stats == nil || fact.Stats.Outcnt <= 1 || dim.Stats.Outcnt <= 0 ||
		len(builder.scalarReaggAliases[agg.NodeId]) != 0 {
		return 0, false
	}
	pk, ok := sqlEqualityCompatiblePrimaryKeyColumnPositions(dim.TableDef)
	if !ok || len(join.OnList) != len(pk) {
		return 0, false
	}
	for _, pred := range dim.FilterList {
		if !isTruncationSafePredicateExpr(pred) {
			return 0, false
		}
	}
	factTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(fact.NodeId) {
		factTags[tag] = true
	}
	keys := make([]*plan.Expr, 0, len(join.OnList)+len(agg.GroupBy))
	keyPos := make(map[[2]int32]int32)
	addKey := func(expr *plan.Expr) bool {
		col := expr.GetCol()
		if col == nil || !factTags[col.RelPos] || !types.T(expr.Typ.Id).IsInteger() {
			return false
		}
		ref := [2]int32{col.RelPos, col.ColPos}
		if _, found := keyPos[ref]; !found {
			keyPos[ref] = int32(len(keys))
			keys = append(keys, DeepCopyExpr(expr))
		}
		return true
	}
	var dimKeys []int32
	for _, on := range join.OnList {
		fn := on.GetF()
		if fn == nil || fn.Func == nil || !IsEqualFunc(fn.Func.Obj) || len(fn.Args) != 2 ||
			!sqlEqualityJoinUsesOneIdentityDomain(fn.Args[0].Typ, fn.Args[1].Typ) {
			return 0, false
		}
		f, d := fn.Args[0], fn.Args[1]
		if c := d.GetCol(); c == nil || c.RelPos != dim.BindingTags[0] {
			f, d = d, f
		}
		c := d.GetCol()
		if c == nil || c.RelPos != dim.BindingTags[0] || !addKey(f) {
			return 0, false
		}
		dimKeys = append(dimKeys, c.ColPos)
	}
	if !containsAllSQLEqualityCompatiblePKs(dimKeys, dim.TableDef) {
		return 0, false
	}
	joinKeyCount := len(keys)
	for _, group := range agg.GroupBy {
		col := group.GetCol()
		if col == nil || (col.RelPos != dim.BindingTags[0] && !addKey(group)) {
			return 0, false
		}
	}
	for _, sum := range agg.AggList {
		fn := sum.GetF()
		if fn == nil || fn.Func == nil || fn.Func.ObjName != "sum" || len(fn.Args) != 1 ||
			uint64(fn.Func.Obj)&function.Distinct != 0 || len(fn.AggConfig) != 0 ||
			fn.AggConfigType != plan.AggregateConfigType_AGG_CONFIG_NONE ||
			!canonicalBoundFunction(builder.GetContext(), sum, "sum") {
			return 0, false
		}
		arg := fn.Args[0]
		if !types.T(arg.Typ.Id).IsDecimal() || arg.Typ.Width > 38 ||
			sum.Typ.Width < arg.Typ.Width+20 {
			return 0, false
		}
		if _, total := builder.groupedSumIntegerDigits(arg, factTags); !total {
			return 0, false
		}
	}
	// The dimension's unique domain bounds the complete fact join-key tuple,
	// even when independent sampled column NDVs overestimate that domain.
	groups := estimateNDVAfterSelection(dim.Stats.Outcnt, fact.Stats)
	for _, key := range keys[joinKeyCount:] {
		ndv := getExprNdv(key, builder)
		if builder.groupedSumKeyIsConstant(fact.NodeId, key) {
			ndv = 1
		}
		if ndv <= 0 || math.IsNaN(ndv) || math.IsInf(ndv, 0) {
			return 0, false
		}
		groups *= ndv
	}
	var wideTypes, keyTypes []plan.Type
	for _, group := range agg.GroupBy {
		wideTypes = append(wideTypes, group.Typ)
	}
	for _, key := range keys {
		keyTypes = append(keyTypes, key.Typ)
	}
	wide, wideOK := MaterializedDeclaredRowSize(wideTypes)
	narrow, narrowOK := MaterializedDeclaredRowSize(keyTypes)
	if !wideOK || !narrowOK || groups <= 0 || groups >= fact.Stats.Outcnt ||
		math.IsNaN(groups) || math.IsInf(groups, 0) ||
		(fact.Stats.Outcnt-groups)*wide <= fact.Stats.Outcnt*narrow {
		return 0, false
	}

	partialTags := []int32{builder.genNewBindTag(), builder.genNewBindTag()}
	finalTags := []int32{builder.genNewBindTag(), builder.genNewBindTag()}
	projectTag := builder.genNewBindTag()
	keyRefs := make(map[[2]int32]*plan.Expr, len(keys))
	for ref, pos := range keyPos {
		keyRefs[ref] = GetColExpr(keys[pos].Typ, partialTags[0], pos)
	}
	merged := make([]*plan.Expr, len(agg.AggList))
	project := make([]*plan.Expr, 0, len(agg.GroupBy)+len(agg.AggList))
	localRefs := make(map[[2]int32]*plan.Expr)
	for i, group := range agg.GroupBy {
		project = append(project, GetColExpr(group.Typ, finalTags[0], int32(i)))
		localRefs[[2]int32{agg.BindingTags[0], int32(i)}] = project[i]
	}
	for i, sum := range agg.AggList {
		var err error
		merged[i], err = BindFuncExprImplByPlanExpr(builder.GetContext(), "sum",
			[]*plan.Expr{GetColExpr(sum.Typ, partialTags[1], int32(i))})
		if err != nil {
			return 0, false
		}
		restored, err := appendCastBeforeExpr(builder.GetContext(),
			GetColExpr(merged[i].Typ, finalTags[1], int32(i)), sum.Typ)
		if err != nil {
			return 0, false
		}
		project = append(project, restored)
		localRefs[[2]int32{agg.BindingTags[1], int32(i)}] = restored
	}
	ctx := builder.ctxByNode[agg.NodeId]
	partialID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_AGG, Children: []int32{fact.NodeId}, BindingTags: partialTags,
		GroupBy: keys, AggList: DeepCopyExprList(agg.AggList), SpillMem: builder.aggSpillMem,
	}, ctx)
	join.Children[side] = partialID
	replaceColumnsForExprList(join.OnList, keyRefs)
	replaceColumnsForExprList(agg.GroupBy, keyRefs)
	replaceColumnsForExprList(agg.FilterList, localRefs)
	for ref, restored := range localRefs {
		pos := ref[1]
		if ref[0] == agg.BindingTags[1] {
			pos = int32(len(agg.GroupBy)) + ref[1]
		}
		outputs[ref] = GetColExpr(restored.Typ, projectTag, pos)
		builder.nameByColRef[[2]int32{projectTag, pos}] = builder.nameByColRef[ref]
	}
	for i, tag := range agg.BindingTags {
		delete(builder.tag2NodeID, tag)
		builder.tag2NodeID[finalTags[i]] = agg.NodeId
	}
	agg.BindingTags, agg.AggList = finalTags, merged
	projectID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT, Children: []int32{agg.NodeId},
		BindingTags: []int32{projectTag}, ProjectList: project,
	}, ctx)
	return projectID, true
}

func (builder *QueryBuilder) groupedSumKeyIsConstant(id int32, key *plan.Expr) bool {
	n := builder.qry.Nodes[id]
	if n.NodeType == plan.Node_TABLE_SCAN {
		for _, pred := range n.FilterList {
			fn := pred.GetF()
			if fn == nil || fn.Func == nil || fn.Func.ObjName != "=" || len(fn.Args) != 2 {
				continue
			}
			for i, arg := range fn.Args {
				if col := arg.GetCol(); col != nil && col.RelPos == key.GetCol().RelPos && col.ColPos == key.GetCol().ColPos {
					if lit := fn.Args[1-i].GetLit(); lit != nil && !lit.Isnull && lit.Src == nil {
						return true
					}
				}
			}
		}
	}
	if n.NodeType == plan.Node_JOIN && n.JoinType == plan.Node_INNER {
		for _, child := range n.Children {
			if builder.groupedSumKeyIsConstant(child, key) {
				return true
			}
		}
	}
	return false
}

// Prove a strict 10^digits bound from declared domains, not sampled extrema.
// Only total exact decimal operations may move before a filtering join: an
// error-producing unmatched row must remain unobserved, just as before.
func (builder *QueryBuilder) groupedSumIntegerDigits(expr *plan.Expr, tags map[int32]bool) (int32, bool) {
	if expr == nil || expr.PreparedNumeric != nil || expr.AuxId < 0 ||
		!types.T(expr.Typ.Id).IsDecimal() || expr.Typ.Width <= 0 || expr.Typ.Width > 38 ||
		expr.Typ.Scale < 0 || expr.Typ.Scale > expr.Typ.Width {
		return 0, false
	}
	capacity := expr.Typ.Width - expr.Typ.Scale
	if col := expr.GetCol(); col != nil {
		return capacity, tags[col.RelPos]
	}
	if lit := expr.GetLit(); lit != nil {
		return capacity, lit.Src == nil
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 2 {
		return 0, false
	}
	left, ok := builder.groupedSumIntegerDigits(fn.Args[0], tags)
	if !ok {
		return 0, false
	}
	var digits int32
	switch fn.Func.ObjName {
	case "cast":
		if fn.Args[1].GetT() == nil || expr.Typ.Scale < fn.Args[0].Typ.Scale ||
			!canonicalExactAffineIntegerCast(builder.GetContext(), expr) {
			return 0, false
		}
		digits = left
	case "+", "-":
		right, total := builder.groupedSumIntegerDigits(fn.Args[1], tags)
		if !total || !canonicalBoundFunction(builder.GetContext(), expr, fn.Func.ObjName) {
			return 0, false
		}
		digits = max(left, right) + 1
	case "/":
		denom := fn.Args[1]
		lit := denom.GetLit()
		id, overload := function.DecodeOverloadID(fn.Func.Obj)
		if lit == nil || lit.Src != nil || lit.Isnull || denom.PreparedNumeric != nil || denom.Typ.Scale != 0 ||
			id != function.DIV || overload != 0 {
			return 0, false
		}
		// Preserve the already bound expression verbatim. Decimal division's
		// binder can refine precision from the original operands; rebinding its
		// widened argument casts is not idempotent and loses that refinement.
		positiveInteger := false
		switch v := lit.Value.(type) {
		case *plan.Literal_Decimal64Val:
			positiveInteger = v.Decimal64Val.A > 0
		case *plan.Literal_Decimal128Val:
			positiveInteger = v.Decimal128Val.B == 0 && v.Decimal128Val.A > 0
		}
		if !positiveInteger {
			return 0, false
		}
		digits = left + 1 // includes a possible rounding carry
	default:
		return 0, false
	}
	return digits, digits <= capacity
}
