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

import plan "github.com/matrixorigin/matrixone/pkg/pb/plan"

// A filtering OR of positive EXISTS branches is equivalent to a SEMI join
// against their UNION ALL. When their outer keys differ, hash on the keys all
// branches share and check each branch's remaining equalities as a residual.
// A branch number distinguishes a missing key from an actual NULL key.
func (builder *QueryBuilder) rewriteFilteringExistenceUnion(
	originalID, baseID int32, marks []*plan.Node, keys [][]existenceJoinKey,
) (int32, bool) {
	// Bound the size of the generated union row and residual expression. The
	// original MARK joins remain available for unusually wide disjunctions.
	const maxBranches, maxExtraKeys = 8, 16
	if len(keys) < 2 || len(marks) != len(keys) {
		return originalID, false
	}
	used := make([][]bool, len(keys))
	for i := range keys {
		used[i] = make([]bool, len(keys[i]))
	}
	var common [][]existenceJoinKey
	for firstIdx, first := range keys[0] {
		matches := make([]existenceJoinKey, len(keys))
		positions := make([]int, len(keys))
		matches[0], positions[0] = first, firstIdx
		found := true
		for branch := 1; branch < len(keys); branch++ {
			positions[branch] = -1
			for idx, candidate := range keys[branch] {
				if exprStructuralEqual(first.outer, candidate.outer) &&
					makeTypeByPlan2Expr(first.inner).Eq(makeTypeByPlan2Expr(candidate.inner)) {
					positions[branch] = idx
					matches[branch] = candidate
					break
				}
			}
			if positions[branch] < 0 {
				found = false
				break
			}
		}
		if found {
			common = append(common, matches)
			for branch, position := range positions {
				used[branch][position] = true
			}
		}
	}
	if len(common) == 0 {
		return originalID, false
	}

	type extraKey struct {
		branch int
		key    existenceJoinKey
	}
	var extras []extraKey
	for branch := range keys {
		for idx, key := range keys[branch] {
			if !used[branch][idx] {
				extras = append(extras, extraKey{branch: branch, key: key})
			}
		}
	}
	if len(extras) > 0 && (len(keys) > maxBranches || len(extras) > maxExtraKeys) {
		return originalID, false
	}

	branchIDs := make([]int32, len(keys))
	branchTags := make([]int32, len(keys))
	for branch, mark := range marks {
		project := make([]*plan.Expr, 0, len(common)+len(extras)+1)
		for _, matching := range common {
			project = append(project, DeepCopyExpr(matching[branch].inner))
		}
		for _, extra := range extras {
			if extra.branch == branch {
				project = append(project, DeepCopyExpr(extra.key.inner))
			} else {
				null := makePlan2NullConstExprWithType()
				null.Typ = extra.key.inner.Typ
				null.Typ.NotNullable = false
				project = append(project, null)
			}
		}
		if len(extras) > 0 {
			project = append(project, makePlan2Int32ConstExprWithType(int32(branch)))
		}
		branchTags[branch] = builder.genNewBindTag()
		branchIDs[branch] = builder.appendNode(&plan.Node{
			NodeType: plan.Node_PROJECT, Children: []int32{mark.Children[1]},
			ProjectList: project, BindingTags: []int32{branchTags[branch]},
		}, nil)
	}

	unionID, unionTag := branchIDs[0], branchTags[0]
	for branch := 1; branch < len(branchIDs); branch++ {
		left := builder.qry.Nodes[unionID].ProjectList
		right := builder.qry.Nodes[branchIDs[branch]].ProjectList
		project := make([]*plan.Expr, len(left))
		for i := range left {
			project[i] = &plan.Expr{
				Typ:  setOperationOutputType(plan.Node_UNION_ALL, left[i].Typ, right[i].Typ),
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: unionTag, ColPos: int32(i)}},
			}
		}
		unionTag = builder.genNewBindTag()
		unionID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_UNION_ALL, Children: []int32{unionID, branchIDs[branch]},
			ProjectList: project, BindingTags: []int32{unionTag},
		}, nil)
	}
	unionProject := builder.qry.Nodes[unionID].ProjectList
	unionColumn := func(index int) *plan.Expr {
		return &plan.Expr{
			Typ:  unionProject[index].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: unionTag, ColPos: int32(index)}},
		}
	}
	joinPredicates := make([]*plan.Expr, 0, len(common)+1)
	for i, matching := range common {
		key := matching[0]
		equality := DeepCopyExpr(key.equality)
		equality.GetF().Args[key.innerArgIdx] = unionColumn(i)
		equality.Typ.NotNullable = key.outer.Typ.NotNullable && unionProject[i].Typ.NotNullable
		joinPredicates = append(joinPredicates, equality)
	}

	var residual *plan.Expr
	markerIndex := len(common) + len(extras)
	for branch := 0; len(extras) > 0 && branch < len(marks); branch++ {
		condition, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
			unionColumn(markerIndex), makePlan2Int32ConstExprWithType(int32(branch)),
		})
		if err != nil {
			return originalID, false
		}
		for i, extra := range extras {
			if extra.branch != branch {
				continue
			}
			equality := DeepCopyExpr(extra.key.equality)
			equality.GetF().Args[extra.key.innerArgIdx] = unionColumn(len(common) + i)
			equality.Typ.NotNullable = false
			condition, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "and", []*plan.Expr{condition, equality})
			if err != nil {
				return originalID, false
			}
		}
		if residual == nil {
			residual = condition
		} else {
			residual, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "or", []*plan.Expr{residual, condition})
			if err != nil {
				return originalID, false
			}
		}
	}
	if residual != nil {
		joinPredicates = append(joinPredicates, residual)
	}
	return builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN, Children: []int32{baseID, unionID},
		JoinType: plan.Node_SEMI, OnList: joinPredicates, SpillMem: builder.joinSpillMem,
	}, nil), true
}
