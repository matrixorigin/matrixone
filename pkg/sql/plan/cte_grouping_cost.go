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

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// cteConstrainedGroupingRows costs a grouped spool before ordinary filter
// pushdown. A point predicate fixes a grouping key to at most one value; an OR
// is a union of such tuple domains. Unknown predicates add no constraint.
// This only refines an estimate, never a runtime bound or a semantic rewrite.
func (builder *QueryBuilder) cteConstrainedGroupingRows(root int32) (float64, bool) {
	node := builder.qry.Nodes[root]
	for node.NodeType == planpb.Node_PROJECT && len(node.Children) == 1 {
		node = builder.qry.Nodes[node.Children[0]]
	}
	if node.NodeType != planpb.Node_AGG || len(node.Children) != 1 ||
		len(node.GroupBy) == 0 || len(node.GroupBy) > 64 {
		return 0, false
	}
	for _, ordinary := range node.GroupingFlag {
		if !ordinary {
			return 0, false
		}
	}
	keys := make(map[[2]int32]uint64, len(node.GroupBy))
	ndvs := make([]float64, 0, len(node.GroupBy))
	for _, group := range node.GroupBy {
		col := group.GetCol()
		if col == nil {
			return 0, false
		}
		key := [2]int32{col.RelPos, col.ColPos}
		if _, exists := keys[key]; exists {
			continue
		}
		ndv := getExprNdv(group, builder)
		if !finitePositive(ndv) {
			return 0, false
		}
		if !group.Typ.NotNullable {
			ndv++
		}
		keys[key] = uint64(1) << len(ndvs)
		ndvs = append(ndvs, math.Max(1, ndv))
	}
	domains := []uint64{0}
	var collect func(int32) bool
	collect = func(id int32) bool {
		n := builder.qry.Nodes[id]
		if n.NodeType != planpb.Node_TABLE_SCAN && n.NodeType != planpb.Node_FILTER &&
			(n.NodeType != planpb.Node_JOIN || n.JoinType != planpb.Node_INNER) {
			return false
		}
		for _, child := range n.Children {
			if !collect(child) {
				return false
			}
		}
		for _, exprs := range [][]*planpb.Expr{n.FilterList, n.OnList} {
			for _, expr := range exprs {
				masks := cteGroupingDomains(expr, keys)
				domains = intersectCTEGroupingDomains(domains, masks)
				if domains == nil {
					return false
				}
			}
		}
		return true
	}
	if !collect(node.Children[0]) {
		return 0, false
	}
	rows := 0.0
	for _, mask := range domains {
		product := 1.0
		for i, ndv := range ndvs {
			if mask&(uint64(1)<<i) == 0 {
				product *= ndv
			}
		}
		rows += product
	}
	return rows, finitePositive(rows)
}

func cteGroupingDomains(expr *planpb.Expr, keys map[[2]int32]uint64) []uint64 {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 2 {
		return []uint64{0}
	}
	switch fn.Func.ObjName {
	case "or":
		left, right := cteGroupingDomains(fn.Args[0], keys), cteGroupingDomains(fn.Args[1], keys)
		if left == nil || right == nil || len(left)+len(right) > 64 {
			return nil
		}
		return append(left, right...)
	case "and":
		return intersectCTEGroupingDomains(cteGroupingDomains(fn.Args[0], keys), cteGroupingDomains(fn.Args[1], keys))
	case "=":
		for i, arg := range fn.Args {
			if col := arg.GetCol(); col != nil && function.IsConstant(fn.Args[1-i], false) {
				return []uint64{keys[[2]int32{col.RelPos, col.ColPos}]}
			}
		}
	}
	return []uint64{0}
}

func intersectCTEGroupingDomains(left, right []uint64) []uint64 {
	if len(left) == 0 || len(right) == 0 || len(left)*len(right) > 64 {
		return nil
	}
	result := make([]uint64, 0, len(left)*len(right))
	for _, l := range left {
		for _, r := range right {
			result = append(result, l|r)
		}
	}
	return result
}
