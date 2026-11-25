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

type groupedMeasureRegion struct {
	scans      []*plan.Node
	membership *plan.Node
	predicates []*plan.Expr
}

// reuseGroupedMeasures carries an already computed qualifying SUM through
// key-preserving dimension joins. No producer is replayed or materialized.
func (builder *QueryBuilder) reuseGroupedMeasures(rootID int32) bool {
	if builder.qry.StmtType != plan.Query_SELECT || builder.isForUpdate || builder.qry.Correlation != nil ||
		(builder.optimizerHints != nil && (builder.optimizerHints.aggPushDown != 0 || builder.optimizerHints.pushDownSemiAntiJoins != 0)) {
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
			builder.reuseGroupedMeasure(n, owners, outputs)
		}
	}
	visit(rootID)
	if len(outputs) != 0 {
		builder.applyEffectlessAggRemap(rootID, outputs)
	}
	return len(outputs) != 0
}

func groupedMeasurePlainAggregate(n *plan.Node) bool {
	return n.NodeType == plan.Node_AGG && len(n.Children) == 1 && len(n.BindingTags) == 2 &&
		len(n.GroupBy) > 0 && len(n.AggList) == 1 && !slices.Contains(n.GroupingFlag, false) &&
		len(n.PhysicalEqualityKeyList) == 0 && len(n.BlockFilterList) == 0 && scalarAggPlainNode(n)
}

// A common bag is insufficient when the omitted SUM can fail because a
// different row/partial-state order overflows. Bound every possible prefix by
// the declared coefficient domain plus 20 digits for a uint64 row count.
func (builder *QueryBuilder) groupedMeasureTotalSum(sum *plan.Expr) bool {
	fn := sum.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 1 || fn.Args[0].GetCol() == nil ||
		uint64(fn.Func.Obj)&function.Distinct != 0 || len(fn.AggConfig) != 0 ||
		fn.AggConfigType != plan.AggregateConfigType_AGG_CONFIG_NONE || !canonicalBoundFunction(builder.GetContext(), sum, "sum") {
		return false
	}
	arg := fn.Args[0].Typ
	if !validDecimalPlanType(sum.Typ) {
		return false
	}
	digits := int32(0)
	if types.T(arg.Id).IsDecimal() && validDecimalPlanType(arg) && arg.Scale == sum.Typ.Scale {
		digits = arg.Width
	} else if types.T(arg.Id).IsInteger() && sum.Typ.Scale == 0 {
		digits = integerDecimalDigits(types.T(arg.Id))
	}
	return digits > 0 && digits+20 <= sum.Typ.Width
}

func (builder *QueryBuilder) readGroupedMeasureRegion(id int32, owners map[int32]int, region *groupedMeasureRegion) bool {
	if owners[id] != 1 {
		return false
	}
	n := builder.qry.Nodes[id]
	switch n.NodeType {
	case plan.Node_TABLE_SCAN:
		if len(region.scans) >= maxSemiContainmentScans || len(n.BindingTags) != 1 || len(n.Children) != 0 ||
			n.TableDef == nil || n.ParentObjRef != nil || n.IndexReaderParam != nil ||
			!SetMembershipNodeEvaluationIsTotal(n) {
			return false
		}
		region.scans = append(region.scans, n)
		return true
	case plan.Node_JOIN:
		if n.JoinType == plan.Node_SEMI {
			if region.membership != nil || !plainLogicalSemiJoin(n) {
				return false
			}
			region.membership = n
			return builder.readGroupedMeasureRegion(n.Children[0], owners, region)
		}
		if n.JoinType != plan.Node_INNER || len(n.Children) != 2 || joinNodeHasLocalSemantics(n) ||
			!SetMembershipNodeEvaluationIsTotal(n) || builder.joinOwnsConstantDiagnostic(n) {
			return false
		}
		for _, pred := range n.OnList {
			if !groupedMeasureIntegerEquality(pred) {
				return false
			}
		}
		region.predicates = append(region.predicates, n.OnList...)
		return builder.readGroupedMeasureRegion(n.Children[0], owners, region) &&
			builder.readGroupedMeasureRegion(n.Children[1], owners, region)
	default:
		return false
	}
}

func groupedMeasureIntegerEquality(expr *plan.Expr) bool {
	fn := expr.GetF()
	return fn != nil && fn.Func != nil && IsEqualFunc(fn.Func.Obj) && len(fn.Args) == 2 &&
		fn.Args[0].GetCol() != nil && fn.Args[1].GetCol() != nil && types.T(fn.Args[0].Typ.Id).IsInteger() &&
		sqlEqualityJoinUsesOneIdentityDomain(fn.Args[0].Typ, fn.Args[1].Typ) && isTruncationSafePredicateExpr(expr)
}

func (builder *QueryBuilder) groupedMeasureProducer(id int32, owners map[int32]int) (*plan.Node, []*plan.Node, bool) {
	var path []*plan.Node
	for {
		if owners[id] != 1 {
			return nil, nil, false
		}
		n := builder.qry.Nodes[id]
		if n.NodeType == plan.Node_AGG {
			return n, path, groupedMeasurePlainAggregate(n) && len(n.GroupBy) == 1
		}
		if len(n.Children) != 1 || (n.NodeType != plan.Node_PROJECT && n.NodeType != plan.Node_FILTER) ||
			!SetMembershipNodeEvaluationIsTotal(n) {
			return nil, nil, false
		}
		if n.NodeType == plan.Node_PROJECT {
			if len(n.BindingTags) != 1 {
				return nil, nil, false
			}
			for _, expr := range n.ProjectList {
				if expr.GetCol() == nil {
					return nil, nil, false
				}
			}
		}
		path = append(path, n)
		id = n.Children[0]
	}
}

func groupedMeasureUnwrapKey(expr *plan.Expr, path []*plan.Node) *plan.ColRef {
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

func (builder *QueryBuilder) reuseGroupedMeasure(agg *plan.Node, owners map[int32]int, outputs map[[2]int32]*plan.Expr) {
	if !groupedMeasurePlainAggregate(agg) || len(agg.FilterList) != 0 || !builder.groupedMeasureTotalSum(agg.AggList[0]) ||
		len(builder.scalarReaggAliases[agg.NodeId]) != 0 {
		return
	}
	var region groupedMeasureRegion
	if !builder.readGroupedMeasureRegion(agg.Children[0], owners, &region) || region.membership == nil || len(region.scans) < 2 {
		return
	}
	semi := region.membership
	if len(semi.OnList) != 1 || !groupedMeasureIntegerEquality(semi.OnList[0]) {
		return
	}
	qualifier, path, ok := builder.groupedMeasureProducer(semi.Children[1], owners)
	if !ok || !builder.groupedMeasureTotalSum(qualifier.AggList[0]) ||
		!isSameColumnType(agg.AggList[0].Typ, qualifier.AggList[0].Typ) ||
		!areTruncationSafePredicates(qualifier.FilterList) {
		return
	}
	key := qualifier.GroupBy[0]
	keyCol := key.GetCol()
	if keyCol == nil || !types.T(key.Typ.Id).IsInteger() || !types.T(qualifier.AggList[0].Typ.Id).IsDecimal() {
		return
	}
	qualFact := builder.qry.Nodes[qualifier.Children[0]]
	arg := agg.AggList[0].GetF().Args[0].GetCol()
	qualArg := qualifier.AggList[0].GetF().Args[0].GetCol()
	if owners[qualFact.NodeId] != 1 || qualFact.NodeType != plan.Node_TABLE_SCAN || len(qualFact.BindingTags) != 1 ||
		len(qualFact.Children) != 0 || !SetMembershipNodeEvaluationIsTotal(qualFact) ||
		qualArg.RelPos != qualFact.BindingTags[0] || keyCol.RelPos != qualArg.RelPos || arg.ColPos != qualArg.ColPos ||
		!isSameColumnType(agg.AggList[0].GetF().Args[0].Typ, qualifier.AggList[0].GetF().Args[0].Typ) {
		return
	}
	var fact *plan.Node
	var dims []*plan.Node
	for _, scan := range region.scans {
		if scan.BindingTags[0] == arg.RelPos {
			fact = scan
		} else {
			dims = append(dims, scan)
		}
	}
	if fact == nil || !sameScalarAggInput(fact, qualFact) || len(dims) == 0 {
		return
	}
	fn := semi.OnList[0].GetF()
	var probe, build *plan.Expr
	for side := range fn.Args {
		ref := groupedMeasureUnwrapKey(fn.Args[side], path)
		if ref != nil && ref.RelPos == qualifier.BindingTags[0] && ref.ColPos == 0 {
			build, probe = fn.Args[side], fn.Args[1-side]
		}
	}
	if build == nil || probe.GetCol() == nil {
		return
	}
	// Keep qualification ahead of dimension materialization. Normal SEMI
	// pushdown must already have lowered this edge to its surviving probe scan;
	// converting a join-spanning SEMI earlier lets join ordering lift the filter.
	probeScan := builder.qry.Nodes[semi.Children[0]]
	if probeScan.NodeType != plan.Node_TABLE_SCAN || probeScan.NodeId == fact.NodeId ||
		probe.GetCol().RelPos != probeScan.BindingTags[0] {
		return
	}
	factKey := GetColExpr(key.Typ, fact.BindingTags[0], keyCol.ColPos)
	equalities := newSemiContainmentEqualities(region.predicates)
	if !equalities.equivalent(factKey, probe) || !groupedMeasureDimensionsUnique(dims, factKey, equalities, agg.GroupBy) ||
		!groupedMeasureCostAllows(fact.Stats, builder.qry.Nodes[semi.Children[1]].Stats, factKey.Typ, agg.AggList[0].GetF().Args[0].Typ, agg.AggList[0].Typ) {
		return
	}
	connector, factSide := builder.groupedMeasureFactConnector(agg.Children[0], fact.NodeId)
	if connector == nil || len(connector.OnList) != 1 ||
		!groupedMeasureConnectorImplied(connector.OnList[0], factKey, probe) {
		return
	}
	factPredicates := 0
	for _, predicate := range region.predicates {
		if containsTag(predicate, fact.BindingTags[0]) {
			factPredicates++
		}
	}
	if factPredicates != 1 {
		return
	}
	// Every surviving dimension row identifies exactly one fact group. Replace
	// references to its fact key with that dimension key before retiring the scan.
	refs := map[[2]int32]*plan.Expr{{fact.BindingTags[0], keyCol.ColPos}: probe}
	project := DeepCopyExprList(agg.GroupBy)
	replaceColumnsForExprList(project, refs)
	measure := GetColExpr(qualifier.AggList[0].Typ, qualifier.BindingTags[1], 0)
	for i := len(path) - 1; i >= 0; i-- {
		if path[i].NodeType == plan.Node_PROJECT {
			position := int32(len(path[i].ProjectList))
			path[i].ProjectList = append(path[i].ProjectList, measure)
			measure = GetColExpr(measure.Typ, path[i].BindingTags[0], position)
		}
	}
	measure.Typ = agg.AggList[0].Typ
	project = append(project, measure)
	// Preserve every surviving join and its predicate placement. Rebuilding a
	// Cartesian region and rewriting the retired equality to probe=probe creates
	// useless scan filters and false low-cardinality/serial execution plans.
	root := builder.pruneGroupedMeasureConnector(agg.Children[0], connector.NodeId, connector.Children[1-factSide])
	semi.JoinType, semi.IsRightJoin = plan.Node_INNER, false
	projectTag := builder.genNewBindTag()
	for i, expr := range project {
		oldTag, oldPos := agg.BindingTags[0], int32(i)
		if i == len(agg.GroupBy) {
			oldTag, oldPos = agg.BindingTags[1], 0
		}
		ref := [2]int32{oldTag, oldPos}
		outputs[ref] = GetColExpr(expr.Typ, projectTag, int32(i))
		builder.nameByColRef[[2]int32{projectTag, int32(i)}] = builder.nameByColRef[ref]
	}
	for _, tag := range agg.BindingTags {
		delete(builder.tag2NodeID, tag)
	}
	builder.tag2NodeID[projectTag] = agg.NodeId
	builder.qry.Nodes[agg.NodeId] = &plan.Node{NodeId: agg.NodeId, NodeType: plan.Node_PROJECT,
		Children: []int32{root}, BindingTags: []int32{projectTag}, ProjectList: project}
}

func (builder *QueryBuilder) groupedMeasureFactConnector(id, factID int32) (*plan.Node, int) {
	n := builder.qry.Nodes[id]
	if n.NodeType != plan.Node_JOIN {
		return nil, 0
	}
	for side, child := range n.Children {
		if n.JoinType == plan.Node_SEMI && side == 1 {
			continue
		}
		if child == factID {
			if n.JoinType == plan.Node_INNER {
				return n, side
			}
			return nil, 0
		}
		if parent, index := builder.groupedMeasureFactConnector(child, factID); parent != nil {
			return parent, index
		}
	}
	return nil, 0
}

func groupedMeasureConnectorImplied(predicate, factKey, probe *plan.Expr) bool {
	fn := predicate.GetF()
	if fn == nil || len(fn.Args) != 2 || !groupedMeasureIntegerEquality(predicate) {
		return false
	}
	same := func(a, b *plan.Expr) bool {
		x, y := a.GetCol(), b.GetCol()
		return x != nil && y != nil && x.RelPos == y.RelPos && x.ColPos == y.ColPos && sameSemiContainmentType(a.Typ, b.Typ)
	}
	return same(fn.Args[0], factKey) && same(fn.Args[1], probe) || same(fn.Args[1], factKey) && same(fn.Args[0], probe)
}

func (builder *QueryBuilder) pruneGroupedMeasureConnector(id, connectorID, siblingID int32) int32 {
	if id == connectorID {
		return siblingID
	}
	n := builder.qry.Nodes[id]
	if n.NodeType == plan.Node_JOIN {
		for side, child := range n.Children {
			if n.JoinType != plan.Node_SEMI || side == 0 {
				n.Children[side] = builder.pruneGroupedMeasureConnector(child, connectorID, siblingID)
			}
		}
	}
	return id
}

func groupedMeasureDimensionsUnique(dims []*plan.Node, key *plan.Expr, equalities *semiContainmentEqualities, groups []*plan.Expr) bool {
	known := []*plan.Expr{key}
	proven := make(map[int32]bool)
	for {
		progress := false
		for _, dim := range dims {
			if proven[dim.NodeId] {
				continue
			}
			pk, ok := sqlEqualityCompatiblePrimaryKeyColumnPositions(dim.TableDef)
			if !ok {
				return false
			}
			complete := true
			for _, pos := range pk {
				ref := GetColExpr(dim.TableDef.Cols[pos].Typ, dim.BindingTags[0], pos)
				found := false
				for _, prior := range known {
					found = found || equalities.equivalent(ref, prior)
				}
				complete = complete && found
			}
			if complete {
				proven[dim.NodeId], progress = true, true
				for pos, col := range dim.TableDef.Cols {
					known = append(known, GetColExpr(col.Typ, dim.BindingTags[0], int32(pos)))
				}
			}
		}
		if !progress {
			break
		}
	}
	if len(proven) != len(dims) {
		return false
	}
	containsKey := false
	for _, group := range groups {
		if group.GetCol() == nil {
			return false
		}
		found := false
		for _, expr := range known {
			found = found || equalities.equivalent(group, expr)
		}
		if !found {
			return false
		}
		containsKey = containsKey || equalities.equivalent(group, key)
	}
	return containsKey
}

func groupedMeasureCostAllows(fact, qualified *plan.Stats, key, arg, result plan.Type) bool {
	if fact == nil || qualified == nil || !finitePositive(fact.Outcnt) || !finitePositive(qualified.Outcnt) {
		return false
	}
	removedWidth, ok := MaterializedDeclaredRowSize([]plan.Type{key, arg})
	if !ok {
		return false
	}
	addedWidth, ok := MaterializedDeclaredRowSize([]plan.Type{key, result})
	removed, added := fact.Outcnt*removedWidth, qualified.Outcnt*addedWidth
	return ok && !math.IsInf(removed, 0) && !math.IsInf(added, 0) && added < removed
}
