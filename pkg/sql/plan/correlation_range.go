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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// For one-level candidates not claimed by the existing deep/dependent routing,
// select complete relation execution before pull-up can split aggregate groups
// or remove filters. A range predicate above an independent aggregate does not
// cross that aggregate and must not change its existing scalar join route.
func (builder *QueryBuilder) rangeScalarNeedsDependent(outerID int32, sub *plan.SubqueryRef, ctx *BindContext) bool {
	if sub.Typ != plan.SubqueryRef_SCALAR {
		return false
	}
	type shape struct{ predicate, aggregate, pagination bool }
	seen := make(map[int32]shape)
	var visit func(int32) shape
	visit = func(id int32) shape {
		if s, ok := seen[id]; ok {
			return s
		}
		if id < 0 || int(id) >= len(builder.qry.Nodes) || builder.qry.Nodes[id] == nil {
			return shape{pagination: true} // The dependent validator owns invalid references.
		}
		seen[id] = shape{} // Recursive source edges are not additional lexical predicates.
		n := builder.qry.Nodes[id]
		var s shape
		merge := func(child shape) {
			s.predicate = s.predicate || child.predicate
			s.aggregate = s.aggregate || child.aggregate
			s.pagination = s.pagination || child.pagination
		}
		for _, child := range n.Children {
			merge(visit(child))
		}
		for _, step := range n.SourceStep {
			if step < 0 || int(step) >= len(builder.qry.Steps) {
				s.pagination = true
			} else {
				merge(visit(builder.qry.Steps[step]))
			}
		}
		if n.NodeType == plan.Node_AGG && s.predicate {
			s.aggregate = true
		}
		for _, predicates := range [][]*plan.Expr{n.FilterList, n.OnList} {
			_, equality := correlatedPaginationPartitionKeys(predicates)
			s.predicate = s.predicate || !equality
		}
		if s.predicate && (n.Limit != nil || n.Offset != nil || n.RankOption != nil) {
			s.pagination = true
		}
		seen[id] = s
		return s
	}
	s := visit(sub.NodeId)
	if s.pagination && !s.aggregate {
		// Projection-owned correlation has its own normalization, including
		// MARK joins and bounded per-outer-row pagination. Preserve that
		// route and its independent admission boundaries.
		for _, expr := range builder.ctxByNode[sub.NodeId].projects {
			if hasCorrCol(expr) {
				return false
			}
		}
	}
	return s.pagination || s.aggregate && !builder.canReaggregateRangeScalar(outerID, sub, ctx)
}

// This proof inspects the original relation, not a speculative rewritten copy.
// Identity and aggregate-input admission are shared with the legacy rewrite.
func (builder *QueryBuilder) canReaggregateRangeScalar(outerID int32, sub *plan.SubqueryRef, ctx *BindContext) bool {
	// Flattening a subquery in the comparison operand composes the outer
	// relation after this proof. Keep both operands in their demand stage.
	if sub.Child != nil && hasSubquery(sub.Child) {
		return false
	}
	var spine []*plan.Node
	id := sub.NodeId
	for range len(builder.qry.Nodes) {
		n := builder.qry.Nodes[id]
		if len(n.Children) != 1 || n.Limit != nil || n.Offset != nil || n.RankOption != nil ||
			len(n.FilterList) != 0 || len(n.OrderBy) != 0 {
			return false
		}
		spine = append(spine, n)
		if n.NodeType == plan.Node_AGG {
			if len(n.GroupBy) != 0 || len(n.BindingTags) != 2 {
				return false
			}
			break
		}
		if n.NodeType != plan.Node_PROJECT || len(n.BindingTags) != 1 {
			return false
		}
		id = n.Children[0]
	}
	path := scalarAggregateProjectionFromSpine(spine, 0)
	if path == nil {
		return false
	}
	subCtx := builder.ctxByNode[sub.NodeId]
	wrapped := len(subCtx.aggregates) == 0
	if !wrapped && len(spine) > 2 || wrapped && sub.Child != nil {
		return false
	}
	if sub.Child != nil {
		for i := range subCtx.results {
			if scalarAggregateProjectionFromSpine(spine, int32(i)) == nil {
				return false
			}
		}
	}
	innerID := path.aggNode.Children[0]
	if builder.subtreeContainsAggregate(innerID) {
		return false
	}
	if _, _, err := builder.rangeScalarOuterSource(outerID, ctx, sub.Child != nil); err != nil {
		return false
	}
	marker, source, cols, err := builder.rangeScalarInnerSource(innerID)
	if err != nil {
		return false
	}
	aggregates := path.aggNode.AggList
	if wrapped {
		aggregates = aggregates[path.aggregatePos : path.aggregatePos+1]
		if !scalarWrappedAggregateArgIsNullOnNoMatch(aggregates[0]) {
			return false
		}
	}
	for _, agg := range aggregates {
		if !rangeScalarAggregateInputSupported(agg, marker.GetCol().RelPos, cols, source) {
			return false
		}
	}
	return true
}

// The single visible binding must still denote individual outer rows. Staging
// CTE identity forwarding is read-only; only the admitted rewrite publishes it.
func (builder *QueryBuilder) rangeScalarOuterSource(nodeID int32, ctx *BindContext, rowComparison bool) (*scalarCTERowSource, []scalarOuterOutput, error) {
	if len(ctx.bindings) != 1 {
		return nil, nil, moerr.NewNYI(builder.GetContext(),
			"aggregation with non equal predicate in scalar subquery referencing multiple outer tables will be supported in future version")
	}
	binding := ctx.bindings[0]
	if rowComparison && builder.findReachableRowIDColRef(nodeID) == nil {
		return nil, nil, moerr.NewNYI(builder.GetContext(),
			"non-equality correlated aggregate with outer composition cannot be safely decorrelated")
	}
	marker := builder.findPreservedOuterRowIDColRef(nodeID)
	var source *scalarCTERowSource
	if marker == nil || marker.GetCol() == nil || marker.GetCol().RelPos != binding.tag ||
		marker.GetCol().ColPos < 0 || int(marker.GetCol().ColPos) >= len(binding.cols) ||
		int(marker.GetCol().ColPos) >= len(binding.colIsHidden) || !binding.colIsHidden[marker.GetCol().ColPos] {
		source = builder.analyzeScalarCTERowSource(binding.nodeId, len(binding.cols))
		if source == nil || source.marker.GetCol().RelPos != binding.tag || !builder.preservesScalarCTEIdentity(nodeID, source) {
			return nil, nil, moerr.NewNYI(builder.GetContext(),
				"non-equality correlated aggregate with outer composition cannot be safely decorrelated")
		}
	}
	outputs, ok := builder.collectScalarOuterOutputs(nodeID, binding.tag, int32(len(binding.cols)), ctx)
	if !ok {
		return nil, nil, moerr.NewNYI(builder.GetContext(),
			"correlated aggregate on an outer input without preserved row columns")
	}
	return source, outputs, nil
}

func (builder *QueryBuilder) rangeScalarInnerSource(innerID int32) (*plan.Expr, *scalarCTERowSource, int, error) {
	marker := builder.findReachableRowIDColRef(innerID)
	var source *scalarCTERowSource
	var cols int
	if marker == nil {
		source = builder.analyzeScalarCTERowSource(innerID, -1)
		if source != nil {
			marker = DeepCopyExpr(source.marker)
			cols = len(source.columns)
		}
	} else {
		id := innerID
		for builder.qry.Nodes[id].NodeType != plan.Node_TABLE_SCAN {
			id = builder.qry.Nodes[id].Children[0]
		}
		cols = len(builder.qry.Nodes[id].TableDef.Cols)
	}
	if cols == 0 {
		return nil, nil, 0, moerr.NewNYI(builder.GetContext(),
			"non-equality correlated aggregate requires an accessible inner row marker")
	}
	return marker, source, cols, nil
}

func rangeScalarAggregateInputSupported(agg *plan.Expr, markerTag int32, cols int, source *scalarCTERowSource) bool {
	f := agg.GetF()
	if f == nil || f.Func == nil {
		return false
	}
	fid, _ := function.DecodeOverloadID(f.Func.Obj & function.DistinctMask)
	switch fid {
	case function.STARCOUNT:
		return true
	case function.COUNT, function.SUM, function.MIN, function.MAX, function.AVG:
		if len(f.Args) != 1 {
			return false
		}
	case function.GROUP_CONCAT:
		if len(f.Args) == 0 {
			return false
		}
	default:
		return false
	}
	for _, arg := range f.Args {
		switch input := arg.Expr.(type) {
		case *plan.Expr_Col:
			if input.Col == nil || input.Col.RelPos != markerTag || input.Col.ColPos < 0 ||
				int(input.Col.ColPos) >= cols || source != nil && arg.Typ.Id != source.columns[input.Col.ColPos].Typ.Id {
				return false
			}
		case *plan.Expr_Lit:
		default:
			return false
		}
	}
	return true
}
