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

// restrictAggregateMembership copies a necessary positive membership from the
// candidate relation to the corresponding aggregate input key. The original
// membership and scalar consumer stay in place; no candidate bag is duplicated.
func (builder *QueryBuilder) restrictAggregateMembership(rootID int32) bool {
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
	changed := false
	var visit func(int32)
	visit = func(id int32) {
		if owners[id] != 1 {
			return
		}
		n := builder.qry.Nodes[id]
		for _, child := range n.Children {
			visit(child)
		}
		if n.NodeType != plan.Node_JOIN || len(n.Children) != 2 || builder.joinOwnsConstantDiagnostic(n) {
			return
		}
		switch n.JoinType {
		case plan.Node_LEFT, plan.Node_SINGLE:
			changed = builder.restrictMembershipSide(n, 1, owners) || changed
		case plan.Node_INNER:
			updated := builder.restrictMembershipSide(n, 1, owners)
			if !updated {
				updated = builder.restrictMembershipSide(n, 0, owners)
			}
			changed = changed || updated
		}
	}
	visit(rootID)
	return changed
}

func (builder *QueryBuilder) membershipAggregate(id int32, owners map[int32]int) (*plan.Node, []*plan.Node, bool) {
	var path []*plan.Node
	for {
		if owners[id] != 1 {
			return nil, nil, false
		}
		n := builder.qry.Nodes[id]
		if n.NodeType == plan.Node_AGG {
			if len(n.Children) != 1 || len(n.BindingTags) != 2 || len(n.GroupBy) == 0 || len(n.AggList) == 0 ||
				len(n.FilterList) != 0 || len(n.BlockFilterList) != 0 || slices.Contains(n.GroupingFlag, false) ||
				len(n.PhysicalEqualityKeyList) != 0 || !scalarAggPlainNode(n) {
				return nil, nil, false
			}
			for _, key := range n.GroupBy {
				if key.GetCol() == nil || !types.T(key.Typ.Id).IsInteger() {
					return nil, nil, false
				}
			}
			for _, agg := range n.AggList {
				if !builder.membershipAggregateTotal(agg) {
					return nil, nil, false
				}
			}
			return n, path, true
		}
		if len(n.Children) != 1 || (n.NodeType != plan.Node_PROJECT && n.NodeType != plan.Node_FILTER) ||
			!SetMembershipNodeEvaluationIsTotal(n) {
			return nil, nil, false
		}
		if n.NodeType == plan.Node_PROJECT && len(n.BindingTags) != 1 {
			return nil, nil, false
		}
		path = append(path, n)
		id = n.Children[0]
	}
}

func (builder *QueryBuilder) membershipAggregateTotal(expr *plan.Expr) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 1 || fn.Args[0].GetCol() == nil ||
		uint64(fn.Func.Obj)&function.Distinct != 0 || len(fn.AggConfig) != 0 ||
		fn.AggConfigType != plan.AggregateConfigType_AGG_CONFIG_NONE {
		return false
	}
	id, _ := function.DecodeOverloadID(fn.Func.Obj)
	if id == function.COUNT {
		return canonicalBoundFunction(builder.GetContext(), expr, "count")
	}
	if id != function.SUM || !canonicalBoundFunction(builder.GetContext(), expr, "sum") || !validDecimalPlanType(expr.Typ) {
		return false
	}
	arg := fn.Args[0].Typ
	if types.T(arg.Id).IsDecimal() {
		return validDecimalPlanType(arg) && arg.Scale == expr.Typ.Scale && arg.Width+20 <= expr.Typ.Width
	}
	digits := integerDecimalDigits(types.T(arg.Id))
	return digits > 0 && expr.Typ.Scale == 0 && digits+20 <= expr.Typ.Width
}

func membershipGroupReference(expr *plan.Expr, path []*plan.Node) *plan.ColRef {
	for _, node := range path {
		if node.NodeType != plan.Node_PROJECT {
			continue
		}
		col := expr.GetCol()
		if col == nil || col.RelPos != node.BindingTags[0] || col.ColPos < 0 || int(col.ColPos) >= len(node.ProjectList) {
			return nil
		}
		expr = node.ProjectList[col.ColPos]
	}
	return expr.GetCol()
}

// Only memberships necessarily satisfied by every surviving candidate row are
// collected. In particular, never enter a SEMI build, outer optional side, OR
// alternative, aggregate, shared producer or semantic barrier.
func (builder *QueryBuilder) candidateMemberships(id int32, owners map[int32]int, result *[]*plan.Node) {
	if owners[id] != 1 {
		return
	}
	n := builder.qry.Nodes[id]
	if n.NodeType == plan.Node_JOIN {
		if n.JoinType == plan.Node_SEMI && plainLogicalSemiJoin(n) {
			*result = append(*result, n)
			builder.candidateMemberships(n.Children[0], owners, result)
		} else if n.JoinType == plan.Node_INNER && !joinNodeHasLocalSemantics(n) &&
			SetMembershipNodeEvaluationIsTotal(n) && !builder.joinOwnsConstantDiagnostic(n) {
			for _, child := range n.Children {
				builder.candidateMemberships(child, owners, result)
			}
		}
	} else if n.NodeType == plan.Node_FILTER && len(n.Children) == 1 && SetMembershipNodeEvaluationIsTotal(n) {
		builder.candidateMemberships(n.Children[0], owners, result)
	}
}

func (builder *QueryBuilder) restrictMembershipSide(join *plan.Node, side int, owners map[int32]int) bool {
	agg, path, ok := builder.membershipAggregate(join.Children[side], owners)
	if !ok {
		return false
	}
	fact := builder.qry.Nodes[agg.Children[0]]
	// This includes idempotence and avoids competing with an already admitted
	// domain restriction. The initial rule changes only an unrestricted scan.
	if owners[fact.NodeId] != 1 || fact.NodeType != plan.Node_TABLE_SCAN || !membershipDomainScanTotal(fact) {
		return false
	}
	outerKeys := make(map[[2]int32]*plan.Expr)
	for _, pred := range splitPlanConjunctions(join.OnList) {
		fn := pred.GetF()
		if fn == nil || fn.Func == nil || !IsEqualFunc(fn.Func.Obj) || len(fn.Args) != 2 ||
			!sqlEqualityJoinUsesOneIdentityDomain(fn.Args[0].Typ, fn.Args[1].Typ) {
			continue
		}
		for argSide := range fn.Args {
			group := membershipGroupReference(fn.Args[argSide], path)
			outer := fn.Args[1-argSide].GetCol()
			if group == nil || outer == nil || group.RelPos != agg.BindingTags[0] ||
				group.ColPos < 0 || int(group.ColPos) >= len(agg.GroupBy) {
				continue
			}
			key := agg.GroupBy[group.ColPos]
			if key.GetCol().RelPos == fact.BindingTags[0] && types.T(fn.Args[1-argSide].Typ.Id).IsInteger() &&
				sqlEqualityJoinUsesOneIdentityDomain(key.Typ, fn.Args[1-argSide].Typ) {
				outerKeys[[2]int32{outer.RelPos, outer.ColPos}] = key
			}
		}
	}
	if len(outerKeys) == 0 {
		return false
	}
	var memberships []*plan.Node
	builder.candidateMemberships(join.Children[1-side], owners, &memberships)
	for _, membership := range memberships {
		if builder.copyAggregateMembership(agg, fact, membership, outerKeys, owners) {
			return true
		}
	}
	return false
}

func (builder *QueryBuilder) copyAggregateMembership(
	agg, fact, membership *plan.Node, outerKeys map[[2]int32]*plan.Expr, owners map[int32]int,
) bool {
	if len(membership.OnList) != 1 {
		return false
	}
	domain := builder.qry.Nodes[membership.Children[1]]
	if owners[domain.NodeId] != 1 || domain.NodeType != plan.Node_TABLE_SCAN || !membershipDomainScanTotal(domain) ||
		len(domain.FilterList) == 0 {
		return false
	}
	fn := membership.OnList[0].GetF()
	if fn == nil || fn.Func == nil || !IsEqualFunc(fn.Func.Obj) || len(fn.Args) != 2 ||
		!isTruncationSafePredicateExpr(membership.OnList[0]) {
		return false
	}
	var key, domainKey *plan.Expr
	for side := range fn.Args {
		col, other := fn.Args[side].GetCol(), fn.Args[1-side].GetCol()
		if col == nil || other == nil || other.RelPos != domain.BindingTags[0] {
			continue
		}
		if input := outerKeys[[2]int32{col.RelPos, col.ColPos}]; input != nil {
			key, domainKey = input, fn.Args[1-side]
		}
	}
	if key == nil || !sqlEqualityJoinUsesOneIdentityDomain(key.Typ, domainKey.Typ) ||
		!types.T(domainKey.Typ.Id).IsInteger() || domainKey.GetCol().ColPos < 0 ||
		int(domainKey.GetCol().ColPos) >= len(domain.TableDef.Cols) {
		return false
	}
	// Row selectivity does not establish that the same fraction of distinct
	// keys disappeared: a date predicate may retain nearly every part key.
	// Use the available key NDV bounded by surviving rows, without assuming
	// uniform key loss from that row predicate.
	factNDV, domainNDV := float64(-1), float64(-1)
	if fact.Stats != nil {
		factNDV = math.Min(getExprNdv(DeepCopyExpr(key), builder), fact.Stats.Outcnt)
	}
	if domain.Stats != nil {
		domainNDV = math.Min(getExprNdv(DeepCopyExpr(domainKey), builder), domain.Stats.Outcnt)
	}
	if !aggregateMembershipCostAllows(agg, fact, domain, factNDV, domainNDV, key.Typ, domainKey.Typ) {
		return false
	}
	copy := DeepCopyNode(domain)
	copy.ScanSnapshot = DeepCopySnapshot(domain.ScanSnapshot)
	builder.rebindScanNode(copy)
	newKey := GetColExpr(domainKey.Typ, copy.BindingTags[0], domainKey.GetCol().ColPos)
	on, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{DeepCopyExpr(key), newKey})
	if err != nil {
		return false
	}
	ctx := builder.ctxByNode[agg.NodeId]
	domainID := builder.appendNode(copy, ctx)
	inputID := builder.appendNode(&plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_SEMI,
		Children: []int32{fact.NodeId, domainID}, OnList: []*plan.Expr{on}, SpillMem: builder.joinSpillMem}, ctx)
	agg.Children[0] = inputID
	return true
}

func aggregateMembershipCostAllows(agg, fact, domain *plan.Node, factNDV, domainNDV float64, key, domainKey plan.Type) bool {
	if agg.Stats == nil || fact.Stats == nil || domain.Stats == nil ||
		!finitePositive(factNDV) || !finitePositive(domainNDV) || domainNDV >= factNDV ||
		!finitePositive(fact.Stats.Outcnt) || !finitePositive(agg.Stats.Outcnt) ||
		!finitePositive(domain.Stats.TableCnt) || !finitePositive(domain.Stats.Outcnt) {
		return false
	}
	groupTypes := make([]plan.Type, 0, len(agg.GroupBy))
	for _, expr := range agg.GroupBy {
		groupTypes = append(groupTypes, expr.Typ)
	}
	keyWidth, ok := MaterializedDeclaredRowSize(groupTypes)
	if !ok {
		return false
	}
	stateTypes := slices.Clone(groupTypes)
	for _, expr := range agg.AggList {
		stateTypes = append(stateTypes, expr.Typ)
	}
	stateWidth, ok := MaterializedDeclaredRowSize(stateTypes)
	if !ok {
		return false
	}
	domainWidth, ok := MaterializedDeclaredRowSize([]plan.Type{domainKey})
	if !ok || !types.T(key.Id).IsInteger() {
		return false
	}
	selectivity := domainNDV / factNDV
	// Local work estimates, not allocation admission: charge a second domain
	// scan, its hash build, and all fact membership probes. Credit reduced
	// aggregate-key traffic and resident hash/state work. Actual executors still
	// enforce their existing memory account and spill limits.
	saved := (1 - selectivity) * (fact.Stats.Outcnt*keyWidth + agg.Stats.Outcnt*math.Max(64, stateWidth+32))
	extra := domain.Stats.TableCnt*domainWidth + domain.Stats.Outcnt*math.Max(32, domainWidth+16) + fact.Stats.Outcnt*domainWidth
	return finitePositive(saved) && finitePositive(extra) && saved > extra
}

func membershipDomainScanTotal(scan *plan.Node) bool {
	if scan.NodeType != plan.Node_TABLE_SCAN || len(scan.Children) != 0 || len(scan.BindingTags) != 1 ||
		scan.TableDef == nil || scan.ParentObjRef != nil || scan.ClusterTable != nil ||
		!scalarAggPlainNode(scan) {
		return false
	}
	proof := *scan
	for _, list := range [][]*plan.Expr{scan.FilterList, scan.BlockFilterList} {
		for _, pred := range list {
			if !membershipDomainPredicateTotal(pred) {
				return false
			}
		}
	}
	proof.FilterList, proof.BlockFilterList = nil, nil
	return SetMembershipNodeEvaluationIsTotal(&proof)
}

func membershipDomainPredicateTotal(expr *plan.Expr) bool {
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
		return len(fn.Args) == 1 && membershipDomainPredicateTotal(fn.Args[0])
	case function.AND, function.OR:
		return len(fn.Args) == 2 && membershipDomainPredicateTotal(fn.Args[0]) && membershipDomainPredicateTotal(fn.Args[1])
	case function.LIKE, function.PREFIX_EQ:
		if len(fn.Args) != 2 || fn.Args[0].GetCol() == nil || !membershipLiteralPattern(fn.Args[1]) {
			return false
		}
		source, pattern := fn.Args[0].Typ, fn.Args[1].Typ
		oid := types.T(source.Id)
		return (oid == types.T_char || oid == types.T_varchar) && source.Width > 0 &&
			(source.Charset == uint32(types.CharsetLegacy) || source.Charset == uint32(types.CharsetUTF8)) &&
			source.Charset == pattern.Charset
	default:
		return false
	}
}

func membershipLiteralPattern(expr *plan.Expr) bool {
	if lit := expr.GetLit(); lit != nil {
		return lit.Src == nil && !lit.IsBin
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 2 || fn.Args[1].GetT() == nil ||
		!isSameColumnType(fn.Args[1].Typ, expr.Typ) {
		return false
	}
	id, _ := function.DecodeOverloadID(fn.Func.Obj)
	if id != function.CAST && id != function.CAST_STRICT {
		return false
	}
	source := fn.Args[0]
	lit := source.GetLit()
	if lit == nil || lit.Src != nil || lit.IsBin || expr.Typ.Width <= 0 ||
		types.T(source.Typ.Id) != types.T_varchar || types.T(expr.Typ.Id) != types.T_varchar {
		return false
	}
	value, ok := lit.Value.(*plan.Literal_Sval)
	if !ok || len(value.Sval) > int(expr.Typ.Width) {
		return false
	}
	// The binder can add a literal-only collation cast. ASCII fits both text
	// domains unchanged and cannot produce conversion/width diagnostics.
	for i := range value.Sval {
		if value.Sval[i] >= 128 {
			return false
		}
	}
	return true
}
