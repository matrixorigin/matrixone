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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// PreparedPaginationTopInputSafe proves that replacing blocking Order with Top
// cannot suppress input diagnostics or effects when Top prunes scan blocks.
// Restrict the proof to an ordinary scan and transparent, total filter/project
// nodes. Joins, materialized producers, locks and unknown nodes keep Order.
func PreparedPaginationTopInputSafe(query *planpb.Query, sort *planpb.Node) bool {
	if query == nil || query.StmtType != planpb.Query_SELECT || sort == nil ||
		sort.NodeType != planpb.Node_SORT || len(sort.Children) != 1 ||
		sort.RankOption != nil || len(sort.OrderBy) == 0 {
		return false
	}
	for _, key := range sort.OrderBy {
		if key == nil || key.Expr == nil || key.Expr.GetCol() == nil || !countSortKeyTypeIsTotal(key.Expr.Typ) {
			return false
		}
	}
	// The graph is normally a tree. A depth bound also fails closed on a cycle
	// without allocating a visited map for each compiled pagination node.
	id := sort.Children[0]
	for depth := 0; depth < len(query.Nodes); depth++ {
		if id < 0 || int(id) >= len(query.Nodes) || query.Nodes[id] == nil {
			return false
		}
		node := query.Nodes[id]
		if node.Limit != nil || node.Offset != nil || node.RankOption != nil ||
			node.FilterIsBarrier || len(node.SourceStep) != 0 ||
			len(node.LockTargets) != 0 || node.IndexReaderParam != nil ||
			len(node.RuntimeFilterProbeList) != 0 || len(node.RuntimeFilterBuildList) != 0 ||
			len(node.OnList) != 0 || len(node.GroupBy) != 0 || len(node.AggList) != 0 ||
			len(node.WinSpecList) != 0 || node.SampleFunc != nil ||
			node.InsertCtx != nil || node.DeleteCtx != nil || node.PreInsertCtx != nil ||
			node.PreInsertUkCtx != nil || node.PreInsertSkCtx != nil || node.PostDmlCtx != nil ||
			len(node.UpdateCtxList) != 0 || len(node.OnUpdateExprs) != 0 {
			return false
		}
		for _, expr := range node.ProjectList {
			if !isTruncationSafeRowExpr(expr) || !preparedTopExprHasNoRuntimeSource(expr) {
				return false
			}
		}
		for _, filters := range [][]*planpb.Expr{node.FilterList, node.BlockFilterList} {
			for _, expr := range filters {
				if !isTruncationSafePredicateExpr(expr) || !preparedTopExprHasNoRuntimeSource(expr) {
					return false
				}
			}
		}
		switch node.NodeType {
		case planpb.Node_TABLE_SCAN:
			return node.TableDef != nil && len(node.Children) == 0
		case planpb.Node_PROJECT, planpb.Node_FILTER:
			if len(node.Children) != 1 || len(node.OrderBy) != 0 {
				return false
			}
			id = node.Children[0]
		default:
			return false
		}
	}
	return false
}

// A parameter's physical TEXT-to-source conversion may diagnose at execution;
// a literal's Src can similarly retain a runtime conversion. Neither is proved
// total by the structural predicate/row-expression proof alone.
func preparedTopExprHasNoRuntimeSource(expr *planpb.Expr) bool {
	if function.ContainsParameter(expr) {
		return false
	}
	safe := true
	_ = planpb.VisitExprTree(expr, func(part *planpb.Expr) error {
		if lit := part.GetLit(); lit != nil && lit.Src != nil {
			safe = false
		}
		return nil
	})
	return safe
}
