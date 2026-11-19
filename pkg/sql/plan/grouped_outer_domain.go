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

// pruneGroupedOuterDomains derives a necessary existence condition without
// replacing the original outer joins or their final predicate. All rows of a
// surviving group have the same key, so this never filters just part of a sum.
func (builder *QueryBuilder) pruneGroupedOuterDomains(rootID int32) {
	if builder.qry.Correlation != nil || builder.isForUpdate ||
		(builder.optimizerHints != nil && builder.optimizerHints.aggPushDown != 0) {
		return
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
	var visit func(int32)
	visit = func(id int32) {
		if owners[id] != 1 {
			return
		}
		n := builder.qry.Nodes[id]
		for _, child := range n.Children {
			visit(child)
		}
		if n.NodeType == plan.Node_FILTER && len(n.Children) == 1 && !n.FilterIsBarrier {
			builder.pruneGroupedOuterFilter(n, owners)
		}
	}
	visit(rootID)
}

func plainDomainAggregate(n *plan.Node) bool {
	if n.NodeType != plan.Node_AGG || len(n.Children) != 1 || len(n.BindingTags) != 2 ||
		len(n.GroupBy) == 0 || slices.Contains(n.GroupingFlag, false) ||
		len(n.FilterList) != 0 || len(n.BlockFilterList) != 0 ||
		len(n.PhysicalEqualityKeyList) != 0 || !scalarAggPlainNode(n) {
		return false
	}
	for _, key := range n.GroupBy {
		if key.GetCol() == nil || !types.T(key.Typ.Id).IsInteger() {
			return false
		}
	}
	for _, agg := range n.AggList {
		fn := agg.GetF()
		if fn == nil || fn.Func == nil || uint64(fn.Func.Obj)&function.Distinct != 0 ||
			len(fn.AggConfig) != 0 || fn.AggConfigType != plan.AggregateConfigType_AGG_CONFIG_NONE {
			return false
		}
		functionID, _ := function.DecodeOverloadID(fn.Func.Obj)
		switch functionID {
		case function.SUM, function.MIN, function.MAX, function.COUNT, function.STARCOUNT:
		default:
			return false
		}
		for _, arg := range fn.Args {
			if !isTruncationSafeRowExpr(arg) || types.T(arg.Typ.Id).IsFloat() {
				return false
			}
		}
	}
	return true
}

func (builder *QueryBuilder) pruneGroupedOuterFilter(filter *plan.Node, owners map[int32]int) {
	var joins, rights []*plan.Node
	id := filter.Children[0]
	for {
		n := builder.qry.Nodes[id]
		if owners[id] != 1 || n.NodeType != plan.Node_JOIN || n.JoinType != plan.Node_LEFT ||
			len(n.Children) != 2 || joinNodeHasLocalSemantics(n) || n.FilterIsBarrier ||
			builder.joinOwnsConstantDiagnostic(n) {
			break
		}
		right := builder.qry.Nodes[n.Children[1]]
		if owners[right.NodeId] != 1 || !plainDomainAggregate(right) {
			return
		}
		joins, rights = append(joins, n), append(rights, right)
		id = n.Children[0]
	}
	probe := builder.qry.Nodes[id]
	if len(rights) < 2 || owners[id] != 1 || !plainDomainAggregate(probe) || !builder.outerDomainProbeIsTotal(probe) {
		return
	}
	rightTags := make(map[int32]bool)
	for _, right := range rights {
		for _, tag := range right.BindingTags {
			rightTags[tag] = true
		}
	}
	nullRejected := false
	for _, pred := range filter.FilterList {
		// Skipping groups must not skip a volatile or fallible WHERE call.
		if !outerDomainPredicateSafe(pred) {
			return
		}
		refs := make(map[[2]int32]int)
		increaseRefCnt(pred, 1, refs)
		onlyRight := len(refs) != 0
		for ref := range refs {
			onlyRight = onlyRight && rightTags[ref[0]]
		}
		if onlyRight && rejectsNull(pred, builder.compCtx.GetProcess()) {
			nullRejected = true
		}
	}
	if !nullRejected {
		return
	}
	// Normalize every arm to the same ordered tuple of preserved-side group
	// positions. Residuals or a key from another nullable join are not proofs.
	positions := make([][]int32, len(rights))
	for i, join := range joins {
		positions[i] = make([]int32, len(probe.GroupBy))
		for j := range positions[i] {
			positions[i][j] = -1
		}
		for _, on := range join.OnList {
			fn := on.GetF()
			if fn == nil || fn.Func == nil || !IsEqualFunc(fn.Func.Obj) || len(fn.Args) != 2 {
				return
			}
			left, right := fn.Args[0], fn.Args[1]
			if col := left.GetCol(); col == nil || col.RelPos != probe.BindingTags[0] {
				left, right = right, left
			}
			l, r := left.GetCol(), right.GetCol()
			if l == nil || r == nil || l.RelPos != probe.BindingTags[0] ||
				r.RelPos != rights[i].BindingTags[0] || l.ColPos < 0 || int(l.ColPos) >= len(probe.GroupBy) ||
				r.ColPos < 0 || int(r.ColPos) >= len(rights[i].GroupBy) ||
				positions[i][l.ColPos] != -1 ||
				!sqlEqualityJoinUsesOneIdentityDomain(left.Typ, right.Typ) {
				return
			}
			positions[i][l.ColPos] = r.ColPos
		}
		if slices.Contains(positions[i], -1) || len(join.OnList) != len(probe.GroupBy) {
			return
		}
	}
	// A domain may omit ANTI exclusions: that only adds false positives. Prove
	// the remaining cloned expression/scan domain before publishing any nodes.
	for _, agg := range append([]*plan.Node{probe}, rights...) {
		budget := 64
		if !builder.outerDomainInputSafe(agg.Children[0], owners, &budget) {
			return
		}
	}
	var active []int
	constantInput := builder.qry.Nodes[probe.Children[0]]
	for constantInput.NodeType == plan.Node_JOIN && constantInput.JoinType == plan.Node_ANTI {
		constantInput = builder.qry.Nodes[constantInput.Children[0]]
	}
	for i, key := range probe.GroupBy {
		if !builder.groupedSumKeyIsConstant(constantInput.NodeId, key) {
			active = append(active, i)
		}
	}
	if len(active) == 0 || !builder.outerDomainProfitable(probe, rights, active) {
		return
	}
	ctx := builder.ctxByNode[filter.NodeId]
	// Bind before publishing auxiliary nodes. Omitting constant tuple fields
	// only widens this necessary condition; the original equality stays intact.
	domainTag := builder.genNewBindTag()
	ons := make([]*plan.Expr, len(active))
	for i, pos := range active {
		key := probe.GroupBy[pos]
		var err error
		ons[i], err = BindFuncExprImplByPlanExpr(builder.GetContext(), "=",
			[]*plan.Expr{DeepCopyExpr(key), GetColExpr(key.Typ, domainTag, int32(i))})
		if err != nil {
			return
		}
	}
	var domainID, previousTag int32
	for i, right := range rights {
		tags := make(map[int32]int32)
		input := builder.cloneOuterDomainInput(right.Children[0], tags)
		keys := make([]*plan.Expr, len(active))
		for j, pos := range active {
			keys[j] = DeepCopyExpr(right.GroupBy[positions[i][pos]])
			keys[j].GetCol().RelPos = tags[keys[j].GetCol().RelPos]
		}
		tag := builder.genNewBindTag()
		arm := builder.appendNode(&plan.Node{NodeType: plan.Node_PROJECT,
			Children: []int32{input}, BindingTags: []int32{tag}, ProjectList: keys}, ctx)
		if i == 0 {
			domainID, previousTag = arm, tag
			continue
		}
		projects := make([]*plan.Expr, len(keys))
		for j, key := range keys {
			projects[j] = GetColExpr(key.Typ, previousTag, int32(j))
		}
		previousTag = domainTag
		if i != len(rights)-1 {
			previousTag = builder.genNewBindTag()
		}
		domainID = builder.appendNode(&plan.Node{NodeType: plan.Node_UNION_ALL,
			Children: []int32{domainID, arm}, BindingTags: []int32{previousTag}, ProjectList: projects}, ctx)
	}
	probe.Children[0] = builder.appendNode(&plan.Node{NodeType: plan.Node_JOIN,
		JoinType: plan.Node_SEMI, Children: []int32{probe.Children[0], domainID},
		OnList: ons, SpillMem: builder.joinSpillMem}, ctx)
}

func (builder *QueryBuilder) outerDomainProbeIsTotal(agg *plan.Node) bool {
	for _, expr := range agg.AggList {
		if builder.cteAggregateIsTotal(agg.NodeId, expr, builder.ctxByNode[agg.NodeId], make(map[[2]int32]bool)) {
			continue
		}
		fn := expr.GetF()
		if fn == nil || fn.Func == nil || fn.Func.ObjName != "sum" || len(fn.Args) != 1 {
			return false
		}
		arg, result := types.T(fn.Args[0].Typ.Id), types.T(expr.Typ.Id)
		if !arg.IsInteger() || !result.IsInteger() || arg.IsUnsignedInt() != result.IsUnsignedInt() {
			return false
		}
		bits, known := builder.outerDomainGroupRowBits(agg.Children[0], agg.GroupBy)
		if !known || bits+8*arg.TypeLen() > 8*result.TypeLen() {
			return false
		}
	}
	return true
}

// A PK's unfixed integer suffix bounds rows in one group without trusting
// cardinality estimates. Return log2 of an upper bound; reject multiplication
// by an unknown/nonunique join rather than using a sampled row count.
func (builder *QueryBuilder) outerDomainGroupRowBits(id int32, groups []*plan.Expr) (int, bool) {
	n := builder.qry.Nodes[id]
	if n.NodeType == plan.Node_FILTER && len(n.Children) == 1 {
		return builder.outerDomainGroupRowBits(n.Children[0], groups)
	}
	if n.NodeType == plan.Node_JOIN && len(n.Children) == 2 {
		if n.JoinType == plan.Node_ANTI {
			return builder.outerDomainGroupRowBits(n.Children[0], groups)
		}
		if n.JoinType == plan.Node_INNER {
			for side, child := range n.Children {
				if builder.outerDomainJoinSideIsUnique(n, 1-side) {
					if bits, known := builder.outerDomainGroupRowBits(child, groups); known {
						return bits, true
					}
				}
			}
		}
		return 0, false
	}
	if n.NodeType != plan.Node_TABLE_SCAN || len(n.BindingTags) != 1 {
		return 0, false
	}
	pk, known := sqlEqualityCompatiblePrimaryKeyColumnPositions(n.TableDef)
	if !known {
		return 0, false
	}
	bits := 0
	for _, pos := range pk {
		fixed := false
		for _, group := range groups {
			col := group.GetCol()
			fixed = fixed || (col != nil && col.RelPos == n.BindingTags[0] && col.ColPos == pos &&
				sqlEqualityJoinUsesOneIdentityDomain(group.Typ, n.TableDef.Cols[pos].Typ))
		}
		if !fixed {
			typ := types.T(n.TableDef.Cols[pos].Typ.Id)
			if !typ.IsInteger() || bits > 64-8*typ.TypeLen() {
				return 0, false
			}
			bits += 8 * typ.TypeLen()
		}
	}
	return bits, true
}

func (builder *QueryBuilder) outerDomainJoinSideIsUnique(join *plan.Node, side int) bool {
	scan := builder.qry.Nodes[join.Children[side]]
	if scan.NodeType != plan.Node_TABLE_SCAN || len(scan.BindingTags) != 1 {
		return false
	}
	pk, known := sqlEqualityCompatiblePrimaryKeyColumnPositions(scan.TableDef)
	if !known {
		return false
	}
	matched := make(map[int32]bool)
	for _, on := range join.OnList {
		fn := on.GetF()
		if fn == nil || fn.Func == nil || !IsEqualFunc(fn.Func.Obj) || len(fn.Args) != 2 ||
			!sqlEqualityJoinUsesOneIdentityDomain(fn.Args[0].Typ, fn.Args[1].Typ) {
			return false
		}
		for i, arg := range fn.Args {
			col, other := arg.GetCol(), fn.Args[1-i].GetCol()
			if col != nil && other != nil && col.RelPos == scan.BindingTags[0] && other.RelPos != col.RelPos {
				matched[col.ColPos] = true
			}
		}
	}
	for _, pos := range pk {
		if !matched[pos] {
			return false
		}
	}
	return true
}

func outerDomainPredicateSafe(expr *plan.Expr) bool {
	proof := DeepCopyExpr(expr)
	total := true
	walkLocalCTEExpr(proof, func(e *plan.Expr) {
		fn := e.GetF()
		if fn == nil || fn.Func == nil || fn.Func.ObjName != "coalesce" {
			return
		}
		if len(fn.Args) == 0 {
			total = false
			return
		}
		functionID, _ := function.DecodeOverloadID(fn.Func.Obj)
		if functionID != function.COALESCE {
			total = false
			return
		}
		for _, arg := range fn.Args {
			if !isTruncationSafePredicateValue(arg) || !isSameColumnType(arg.Typ, e.Typ) {
				total = false
			}
		}
		// The proof substitutes a typed value; execution keeps COALESCE intact.
		e.Expr = &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}
	})
	return total && isTruncationSafePredicateExpr(proof)
}

func (builder *QueryBuilder) outerDomainInputSafe(id int32, owners map[int32]int, budget *int) bool {
	*budget--
	if *budget < 0 || owners[id] != 1 {
		return false
	}
	n := builder.qry.Nodes[id]
	if !SetMembershipNodeEvaluationIsTotal(n) {
		return false
	}
	switch n.NodeType {
	case plan.Node_TABLE_SCAN:
		return len(n.Children) == 0 && len(n.BindingTags) == 1 && n.TableDef != nil
	case plan.Node_FILTER, plan.Node_PROJECT:
		if len(n.Children) != 1 {
			return false
		}
	case plan.Node_JOIN:
		if len(n.Children) != 2 || (n.JoinType != plan.Node_INNER && n.JoinType != plan.Node_ANTI) ||
			joinNodeHasLocalSemantics(n) || builder.joinOwnsConstantDiagnostic(n) {
			return false
		}
	default:
		return false
	}
	for _, child := range n.Children {
		if !builder.outerDomainInputSafe(child, owners, budget) {
			return false
		}
	}
	return true
}

func (builder *QueryBuilder) cloneOuterDomainInput(id int32, tags map[int32]int32) int32 {
	n := builder.qry.Nodes[id]
	if n.NodeType == plan.Node_JOIN && n.JoinType == plan.Node_ANTI {
		return builder.cloneOuterDomainInput(n.Children[0], tags)
	}
	clone := DeepCopyNode(n)
	for i, child := range n.Children {
		clone.Children[i] = builder.cloneOuterDomainInput(child, tags)
	}
	for i, tag := range n.BindingTags {
		tags[tag] = builder.genNewBindTag()
		clone.BindingTags[i] = tags[tag]
	}
	for _, e := range append(localCTENodeExprs(clone), clone.BlockFilterList...) {
		walkLocalCTEExpr(e, func(expr *plan.Expr) {
			if col := expr.GetCol(); col != nil {
				col.RelPos = tags[col.RelPos]
			}
		})
	}
	if clone.NodeType == plan.Node_TABLE_SCAN {
		builder.tag2Table[clone.BindingTags[0]] = clone.TableDef
		builder.addNameByColRef(clone.BindingTags[0], clone.TableDef)
	}
	return builder.appendNode(clone, builder.ctxByNode[id])
}

func (builder *QueryBuilder) outerDomainProfitable(probe *plan.Node, rights []*plan.Node, active []int) bool {
	if probe.Stats == nil || probe.Stats.Outcnt <= 0 || math.IsNaN(probe.Stats.Outcnt) || math.IsInf(probe.Stats.Outcnt, 0) {
		return false
	}
	var keys, payload []plan.Type
	var domainNDV float64 = 1
	for _, pos := range active {
		key := probe.GroupBy[pos]
		keys = append(keys, key.Typ)
		ndv := getExprNdv(key, builder)
		if ndv <= 0 || math.IsNaN(ndv) || math.IsInf(ndv, 0) {
			return false
		}
		domainNDV *= ndv
	}
	for _, key := range probe.GroupBy {
		payload = append(payload, key.Typ)
	}
	for _, agg := range probe.AggList {
		payload = append(payload, agg.Typ)
	}
	keyWidth, keyOK := MaterializedDeclaredRowSize(keys)
	stateWidth, stateOK := MaterializedDeclaredRowSize(payload)
	var rows float64
	for _, right := range rights {
		input := builder.qry.Nodes[right.Children[0]]
		if input.NodeType == plan.Node_JOIN && input.JoinType == plan.Node_ANTI {
			input = builder.qry.Nodes[input.Children[0]]
		}
		if input.Stats == nil || input.Stats.Outcnt <= 0 {
			return false
		}
		rows += input.Stats.Outcnt
	}
	coverage := math.Min(1, rows/domainNDV)
	return keyOK && stateOK && !math.IsNaN(coverage) && !math.IsInf(domainNDV, 0) &&
		coverage < 0.5 && rows*keyWidth < probe.Stats.Outcnt*(1-coverage)*stateWidth
}
