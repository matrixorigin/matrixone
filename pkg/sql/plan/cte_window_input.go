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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// reusableWindowCTEInput leaves windows (including fallible or order-sensitive
// expressions) per consumer and tries the existing bounded reuse admission on
// their grouped input instead. It never certifies a window as total.
func (builder *QueryBuilder) reusableWindowCTEInput(
	ref *CTERef, rootID int32,
) (*CTERef, int32, map[int32]bool, bool) {
	if ref == nil || ref.isRecursive || ref.hasNestedUse || builder.boundCTEs[ref] != nil || len(ref.occurrences) < 2 {
		return nil, 0, nil, false
	}
	inputRef := *ref
	inputRef.occurrences = make([]cteOccurrence, 0, len(ref.occurrences))
	paths := make([][]int32, 0, len(ref.occurrences))
	inputs := make([]int32, 0, len(ref.occurrences))
	for _, occurrence := range ref.occurrences {
		if occurrence.isCorrelated {
			return nil, 0, nil, false
		}
		nodeID := occurrence.rootID
		var path []int32
		hasWindow := false
		for {
			node := builder.qry.Nodes[nodeID]
			if node.Limit != nil || node.Offset != nil || len(node.FilterList) != 0 || len(node.Children) != 1 {
				return nil, 0, nil, false
			}
			if node.NodeType == planpb.Node_AGG {
				if !hasWindow || len(node.BindingTags) != 2 {
					return nil, 0, nil, false
				}
				break
			}
			switch node.NodeType {
			case planpb.Node_WINDOW:
				hasWindow = true
			case planpb.Node_PROJECT, planpb.Node_PARTITION:
			default:
				return nil, 0, nil, false
			}
			path = append(path, nodeID)
			nodeID = node.Children[0]
		}
		paths = append(paths, path)
		inputs = append(inputs, nodeID)
	}

	// No expression is changed until admission succeeds. On failure restore the
	// temporary edges and discard the identity projections and their tag index.
	start := len(builder.qry.Nodes)
	originalFilters := make(map[int32]*planpb.Node)
	admitted := false
	defer func() {
		if admitted {
			return
		}
		for i, path := range paths {
			builder.qry.Nodes[path[len(path)-1]].Children[0] = inputs[i]
		}
		for id, node := range originalFilters {
			builder.qry.Nodes[id] = node
		}
		for _, node := range builder.qry.Nodes[start:] {
			for _, tag := range node.BindingTags {
				delete(builder.tag2NodeID, tag)
			}
		}
		builder.qry.Nodes = builder.qry.Nodes[:start]
		builder.ctxByNode = builder.ctxByNode[:start]
	}()
	// These value-proved casts are normally folded by optimizeFilters, after
	// CTE admission. Fold only safe predicates of this candidate input now;
	// do not broaden totality/admission for unrelated whole CTE producers.
	if !builder.foldWindowCTEInputPredicates(inputs[0], originalFilters) {
		return nil, 0, nil, false
	}
	for i, occurrence := range ref.occurrences {
		agg := builder.qry.Nodes[inputs[i]]
		input := cteOccurrence{ctx: occurrence.ctx, rootTag: builder.genNewBindTag()}
		var projects []*planpb.Expr
		for tagPos, exprs := range [][]*planpb.Expr{agg.GroupBy, agg.AggList} {
			for colPos, expr := range exprs {
				projects = append(projects, GetColExpr(expr.Typ, agg.BindingTags[tagPos], int32(colPos)))
				input.types = append(input.types, expr.Typ)
				input.headings = append(input.headings, "")
			}
		}
		input.rootID = builder.appendNode(&planpb.Node{
			NodeType: planpb.Node_PROJECT, Children: []int32{inputs[i]},
			BindingTags: []int32{input.rootTag}, ProjectList: projects,
		}, occurrence.ctx)
		inputRef.occurrences = append(inputRef.occurrences, input)
		path := paths[i]
		builder.qry.Nodes[path[len(path)-1]].Children[0] = input.rootID
	}
	producer, drain, ok := builder.reusableCTEProducer(&inputRef, rootID)
	if !ok {
		return nil, 0, nil, false
	}
	for i, path := range paths {
		agg := builder.qry.Nodes[inputs[i]]
		tag := inputRef.occurrences[i].rootTag
		rebind := func(expr *planpb.Expr) {
			col := expr.GetCol()
			if col == nil {
				return
			}
			switch col.RelPos {
			case agg.BindingTags[0]:
				col.RelPos = tag
			case agg.BindingTags[1]:
				col.RelPos = tag
				col.ColPos += int32(len(agg.GroupBy))
			}
		}
		for _, nodeID := range path {
			node := builder.qry.Nodes[nodeID]
			// Bound expressions can also be owned by BindContext. Copy before
			// rebinding so other occurrences and later proofs retain their tags.
			node.ProjectList = DeepCopyExprList(node.ProjectList)
			node.WinSpecList = DeepCopyExprList(node.WinSpecList)
			node.OrderBy = DeepCopyOrderBySpecList(node.OrderBy)
			for _, exprs := range [][]*planpb.Expr{node.ProjectList, node.WinSpecList} {
				for _, expr := range exprs {
					walkCTEWindowInputExpr(expr, rebind)
				}
			}
			for _, order := range node.OrderBy {
				walkCTEWindowInputExpr(order.Expr, rebind)
			}
		}
	}
	admitted = true
	return &inputRef, producer, drain, true
}

func (builder *QueryBuilder) foldWindowCTEInputPredicates(root int32, originals map[int32]*planpb.Node) bool {
	node := builder.qry.Nodes[root]
	for _, child := range node.Children {
		if !builder.foldWindowCTEInputPredicates(child, originals) {
			return false
		}
	}
	copy := *node
	for i, exprs := range [][]*planpb.Expr{node.FilterList, node.OnList, node.BlockFilterList} {
		if areTruncationSafePredicates(exprs) {
			continue
		}
		folded := make([]*planpb.Expr, len(exprs))
		for j, expr := range exprs {
			if !localCTEGuardedPredicateSafe(expr) {
				return false
			}
			var err error
			folded[j], err = ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(expr), builder.compCtx.GetProcess(), false, false)
			if err != nil {
				return false
			}
		}
		switch i {
		case 0:
			copy.FilterList = folded
		case 1:
			copy.OnList = folded
		case 2:
			copy.BlockFilterList = folded
		}
		originals[root] = node
		builder.qry.Nodes[root] = &copy
	}
	return true
}

func walkCTEWindowInputExpr(expr *planpb.Expr, visit func(*planpb.Expr)) {
	if expr == nil {
		return
	}
	walkLocalCTEExpr(expr, func(current *planpb.Expr) {
		visit(current)
		if window := current.GetW(); window != nil {
			walkCTEWindowInputExpr(window.WindowFunc, visit)
			for _, key := range window.PartitionBy {
				walkCTEWindowInputExpr(key, visit)
			}
			for _, order := range window.OrderBy {
				walkCTEWindowInputExpr(order.Expr, visit)
			}
			if window.Frame != nil {
				for _, bound := range []*planpb.FrameBound{window.Frame.Start, window.Frame.End} {
					if bound != nil {
						walkCTEWindowInputExpr(bound.Val, visit)
					}
				}
			}
		}
	})
}
