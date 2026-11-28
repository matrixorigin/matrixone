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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// appendRepeatedCandidate computes generated values and CHECK eligibility for
// one role using the last accepted row. An inactive role first selects the
// current values, so it cannot manufacture a candidate from absent inputs.
func appendRepeatedCandidate(
	builder *QueryBuilder, bindCtx *BindContext, last int32, table *planpb.TableDef,
	rowID, activePos int, current []int, names []string, values []int, isIgnore bool,
) (int32, []string, []int, int, error) {
	generated, err := affectedGeneratedColumns(table, names)
	if err != nil {
		return -1, nil, nil, 0, err
	}
	if len(generated) == 0 && len(table.Checks) == 0 {
		return last, names, values, activePos, nil
	}
	if len(names) != len(values) || len(current) != len(table.Cols) {
		return -1, nil, nil, 0, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE candidate")
	}
	input := getProjectionByLastNode(builder, last)
	base := len(input)
	output := append([]*planpb.Expr(nil), input...)
	output = append(output, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bool)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: int32(base)}}})
	pairs := make([]planpb.UpdateUniqueValue, len(names))
	candidate := append([]int(nil), current...)
	proposals := make([]int, len(values))
	for i, name := range names {
		col := table.Name2ColIndex[name]
		if values[i] < 0 || values[i] >= len(input) {
			return -1, nil, nil, 0, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE proposal")
		}
		pairs[i] = planpb.UpdateUniqueValue{Current: int32(current[col]), Proposed: int32(values[i])}
		proposals[i] = base + 1 + i
		candidate[col] = proposals[i]
		output = append(output, &planpb.Expr{Typ: table.Cols[col].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: int32(proposals[i])}}})
	}
	last = builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_UPDATE_UNIQUE, Children: []int32{last}, ProjectList: output,
		UpdateUniqueCtx: &planpb.UpdateUniqueCtx{RowId: int32(rowID), Active: int32(activePos), Values: pairs},
	}, bindCtx)
	for _, colIdx := range generated {
		col := table.Cols[colIdx]
		last, candidate[colIdx] = appendGeneratedUpdateColumn(builder, bindCtx, last, col, candidate, isIgnore)
		names = append(names, col.Name)
		proposals = append(proposals, candidate[colIdx])
	}
	if len(table.Checks) == 0 {
		return last, names, proposals, activePos, nil
	}
	if err := requireCheckConstraintProtocol(builder.GetContext(), builder.compCtx.GetProcess()); err != nil {
		return -1, nil, nil, 0, err
	}
	input = getProjectionByLastNode(builder, last)
	lookup := make([]*planpb.Expr, len(table.Cols))
	for i, col := range table.Cols {
		if col.Name != catalog.Row_ID {
			lookup[i] = input[candidate[i]]
		}
	}
	active := DeepCopyExpr(input[activePos])
	assertionCapacity := len(table.Checks)
	if isIgnore {
		assertionCapacity = 0
	}
	assertions := make([]*planpb.Expr, 0, assertionCapacity)
	for _, check := range table.Checks {
		checkExpr := substituteColRefsInExpr(DeepCopyExpr(check.Check), lookup, 0)
		pass, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "coalesce", []*planpb.Expr{checkExpr, makePlan2BoolConstExprWithType(true)})
		if err != nil {
			return -1, nil, nil, 0, err
		}
		if isIgnore {
			active, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "and", []*planpb.Expr{active, pass})
			if err != nil {
				return -1, nil, nil, 0, err
			}
			continue
		}
		absent, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "not", []*planpb.Expr{DeepCopyExpr(input[activePos])})
		if err != nil {
			return -1, nil, nil, 0, err
		}
		pass, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "or", []*planpb.Expr{absent, pass})
		if err != nil {
			return -1, nil, nil, 0, err
		}
		assert, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "_check_constraint_assert", []*planpb.Expr{
			pass, makePlan2StringConstExprWithType(fmt.Sprintf("Check constraint '%s' is violated", check.Name)),
		})
		if err != nil {
			return -1, nil, nil, 0, err
		}
		assertions = append(assertions, assert)
	}
	if !isIgnore {
		last = builder.appendNode(&planpb.Node{
			NodeType: planpb.Node_FILTER, Children: []int32{last}, FilterList: assertions,
			ProjectList: getProjectionByLastNode(builder, last),
		}, bindCtx)
		builder.markRepeatedUpdateCapacityFilter(last)
		return last, names, proposals, activePos, nil
	}
	activePos = len(input)
	input = append(input, active)
	last = builder.appendNode(&planpb.Node{NodeType: planpb.Node_PROJECT, Children: []int32{last}, ProjectList: input}, bindCtx)
	return last, names, proposals, activePos, nil
}
