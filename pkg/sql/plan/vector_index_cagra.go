// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// buildCagraTableFuncArgs defines the planner-side positional contract for a
// future cagra_search table function:
//   - arg 0: index table config JSON
//   - arg 1: query vector
//   - arg 2: optional filter JSON lowered from covered planner filters
func buildCagraTableFuncArgs(tblCfgStr string, vecLitArg *planpb.Expr, filterJSON string) []*planpb.Expr {
	args := []*planpb.Expr{
		{
			Typ: planpb.Type{
				Id: int32(types.T_varchar),
			},
			Expr: &planpb.Expr_Lit{
				Lit: &planpb.Literal{
					Value: &planpb.Literal_Sval{
						Sval: tblCfgStr,
					},
				},
			},
		},
		DeepCopyExpr(vecLitArg),
	}

	if filterJSON != "" {
		args = append(args, makePlan2StringConstExprWithType(filterJSON))
	}
	return args
}

// prepareCagraFilterPushdown is the planner-side handoff boundary for a future
// CAGRA runtime. It first selects filter-capable predicates using the logical
// IncludedColumns contract, then lowers only those covered predicates into the
// name-based JSON payload expected by cagra_search(cfg, vec, filter_json?).
func prepareCagraFilterPushdown(
	filters []*planpb.Expr,
	scanNode *planpb.Node,
	multiTableIndex *MultiTableIndex,
	partPos int32,
) (string, []*planpb.Expr, []*planpb.Expr, error) {
	candidateFilters, _ := splitFiltersByVectorIndexCoverage(
		filters,
		scanNode,
		getVectorIndexIncludedColumns(multiTableIndex),
		partPos,
	)

	filterJSON, pushdownFilters, _, err := lowerFiltersToCagraJSON(candidateFilters, scanNode, partPos)
	if err != nil {
		return "", nil, nil, err
	}

	pushdownCounts := make(map[*planpb.Expr]int, len(pushdownFilters))
	for _, filter := range pushdownFilters {
		pushdownCounts[filter]++
	}

	residualFilters := make([]*planpb.Expr, 0, len(filters)-len(pushdownFilters))
	for _, filter := range filters {
		if pushdownCounts[filter] > 0 {
			pushdownCounts[filter]--
			continue
		}
		residualFilters = append(residualFilters, filter)
	}

	return filterJSON, pushdownFilters, residualFilters, nil
}
