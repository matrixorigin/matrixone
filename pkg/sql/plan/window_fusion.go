// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// fuseWindowNodes runs after physical column remapping. This keeps the binder's
// single-output WindowIdx contract intact, including pruning and Top-N rules.
func (builder *QueryBuilder) fuseWindowNodes(nodeID int32, seen map[int32]bool) {
	if seen[nodeID] {
		return
	}
	seen[nodeID] = true
	node := builder.qry.Nodes[nodeID]
	for _, child := range node.Children {
		builder.fuseWindowNodes(child, seen)
	}
	for builder.fuseWindowNode(node) {
	}
}

func plainWindowFusionNode(node *plan.Node) bool {
	return node != nil && len(node.Children) == 1 && node.Limit == nil &&
		node.Offset == nil && node.PartitionByCount == 0 && !node.PartitionTopNWithTies
}

func fusionWindowSpec(expr *plan.Expr) bool {
	w := expr.GetW()
	if w == nil || len(w.PartitionBy) == 0 || w.WindowFunc.GetF() == nil {
		return false
	}
	// MIN/MAX on columns cannot introduce aggregate overflow or evaluate a
	// fallible expression earlier. Broader aggregate eligibility needs an
	// independent error-domain proof, not just matching window definitions.
	if !strings.EqualFold(w.Name, "min") && !strings.EqualFold(w.Name, "max") {
		return false
	}
	f := w.WindowFunc.GetF()
	if f.Func == nil || uint64(f.Func.Obj)&function.Distinct != 0 || len(f.Args) != 1 ||
		(f.Args[0].GetCol() == nil && f.Args[0].GetLit() == nil) {
		return false
	}
	if !comparisonEvaluationIsTotal(function.LESS_THAN, f.Args[0].Typ, f.Args[0].Typ) {
		return false
	}
	frame := w.Frame
	if frame == nil || frame.Type != plan.FrameClause_ROWS || frame.Start == nil || frame.End == nil ||
		frame.Start.Type != plan.FrameBound_PRECEDING || !frame.Start.UnBounded ||
		(frame.End.Type != plan.FrameBound_CURRENT_ROW &&
			!(frame.End.Type == plan.FrameBound_FOLLOWING && frame.End.UnBounded)) {
		return false
	}
	for _, expr := range w.PartitionBy {
		if expr.GetCol() == nil {
			return false
		}
	}
	for _, order := range w.OrderBy {
		if order == nil || order.Expr.GetCol() == nil {
			return false
		}
	}
	return true
}

func sameFusionWindowLayout(a, b *plan.WindowSpec) bool {
	left, right := proto.Clone(a).(*plan.WindowSpec), proto.Clone(b).(*plan.WindowSpec)
	left.Name, right.Name = "", ""
	left.WindowFunc, right.WindowFunc = nil, nil
	return proto.Equal(left, right)
}

// mapFusionExpr copies before translating so a failed candidate never mutates
// either the original window or another consumer of its child.
func mapFusionExpr(expr *plan.Expr, column func(*plan.ColRef) *plan.Expr) *plan.Expr {
	copy := DeepCopyExpr(expr)
	var rewrite func(*plan.Expr) bool
	rewrite = func(e *plan.Expr) bool {
		switch v := e.Expr.(type) {
		case *plan.Expr_Col:
			replacement := column(v.Col)
			if replacement == nil {
				return false
			}
			e.Expr = DeepCopyExpr(replacement).Expr
		case *plan.Expr_F:
			for _, arg := range v.F.Args {
				if !rewrite(arg) {
					return false
				}
			}
		case *plan.Expr_W:
			if !rewrite(v.W.WindowFunc) {
				return false
			}
			// Ordinary Window retains logical PARTITION BY references for
			// diagnostics; its child Partition owns their physical evaluation.
			// Compare those identities unchanged, just as remapColRefForExpr does.
			for _, order := range v.W.OrderBy {
				if !rewrite(order.Expr) {
					return false
				}
			}
		case *plan.Expr_Lit:
		default:
			return false
		}
		return true
	}
	if !rewrite(copy) {
		return nil
	}
	return copy
}

func (builder *QueryBuilder) fuseWindowNode(upper *plan.Node) bool {
	if upper.NodeType != plan.Node_WINDOW || !plainWindowFusionNode(upper) || len(upper.WinSpecList) == 0 {
		return false
	}
	partition := builder.qry.Nodes[upper.Children[0]]
	if partition.NodeType != plan.Node_PARTITION || !plainWindowFusionNode(partition) || len(partition.FilterList) != 0 {
		return false
	}
	lower := builder.qry.Nodes[partition.Children[0]]
	if lower.NodeType != plan.Node_WINDOW || !plainWindowFusionNode(lower) ||
		len(lower.FilterList) != 0 || len(lower.WinSpecList) == 0 || lower.SpillMem != upper.SpillMem {
		return false
	}
	input := builder.qry.Nodes[lower.Children[0]]
	if input.NodeType != plan.Node_PARTITION || !plainWindowFusionNode(input) {
		return false
	}
	inputWidth := int32(len(input.ProjectList))
	for _, expr := range lower.WinSpecList {
		if !fusionWindowSpec(expr) || !sameFusionWindowLayout(lower.WinSpecList[0].GetW(), expr.GetW()) {
			return false
		}
	}
	// Upper input -> Partition output -> lower Window's physical output.
	resolveInput := func(col *plan.ColRef) *plan.Expr {
		if col.RelPos != 0 || col.ColPos < 0 || int(col.ColPos) >= len(partition.ProjectList) {
			return nil
		}
		p := partition.ProjectList[col.ColPos].GetCol()
		if p == nil || p.RelPos != 0 || p.ColPos < 0 || int(p.ColPos) >= len(lower.ProjectList) {
			return nil
		}
		result := lower.ProjectList[p.ColPos]
		c := result.GetCol()
		if c == nil || (c.RelPos != 0 && c.RelPos != -1) {
			return nil
		}
		return result
	}
	newSpecs := append([]*plan.Expr(nil), lower.WinSpecList...)
	for _, expr := range upper.WinSpecList {
		if !fusionWindowSpec(expr) {
			return false
		}
		mapped := mapFusionExpr(expr, func(col *plan.ColRef) *plan.Expr {
			result := resolveInput(col)
			if result == nil || result.GetCol().RelPos != 0 || result.GetCol().ColPos >= inputWidth {
				return nil // dependent windows must remain separate
			}
			return result
		})
		if mapped == nil || !sameFusionWindowLayout(lower.WinSpecList[0].GetW(), mapped.GetW()) {
			return false
		}
		newSpecs = append(newSpecs, mapped)
	}
	mapOutput := func(col *plan.ColRef) *plan.Expr {
		if col.RelPos == -1 && col.ColPos >= int32(len(partition.ProjectList)) {
			if col.ColPos-int32(len(partition.ProjectList)) >= int32(len(upper.WinSpecList)) {
				return nil
			}
			copy := DeepCopyExpr(upper.WinSpecList[0])
			copy.Expr = &plan.Expr_Col{Col: &plan.ColRef{RelPos: -1,
				ColPos: inputWidth + int32(len(lower.WinSpecList)) + col.ColPos - int32(len(partition.ProjectList)), Name: col.Name}}
			return copy
		}
		return resolveInput(col)
	}
	newProjects := make([]*plan.Expr, len(upper.ProjectList))
	newFilters := make([]*plan.Expr, len(upper.FilterList))
	for i, expr := range upper.ProjectList {
		newProjects[i] = mapFusionExpr(expr, mapOutput)
		if newProjects[i] == nil {
			return false
		}
	}
	for i, expr := range upper.FilterList {
		newFilters[i] = mapFusionExpr(expr, mapOutput)
		if newFilters[i] == nil {
			return false
		}
	}
	upper.Children = append([]int32(nil), lower.Children...)
	upper.WinSpecList, upper.ProjectList, upper.FilterList = newSpecs, newProjects, newFilters
	return true
}
