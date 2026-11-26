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
	"sort"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// appendRepeatedImages applies roles in target order to one immutable old row.
// The resulting dense source is consumed by the normal table and index writer.
func appendRepeatedImages(
	builder *QueryBuilder,
	bindCtx *BindContext,
	participantStep int32,
	plans []*dmlPlanCtx,
	markers []int,
	values [][]int,
	isIgnore bool,
) (int32, error) {
	if len(plans) == 0 || len(markers) != len(plans) || len(values) != len(plans) {
		return -1, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE images")
	}
	first := plans[0]
	table := first.tableDef
	asyncColumns, err := asyncDatalinkColumns(table)
	if err != nil {
		return -1, err
	}
	asyncEligible := make(map[string]int, len(asyncColumns))
	oldWidth := len(table.Cols)
	current := make([]int, oldWidth)
	for i := range current {
		current[i] = i
	}
	updated := make(map[string]struct{})
	last := appendSinkScanNode(builder, bindCtx, participantStep)
	baseWidth := len(getProjectionByLastNode(builder, last))
	countPos := -1
	for i, up := range plans {
		if up.tableDef.TblId != table.TblId || len(up.tableDef.Cols) != oldWidth {
			return -1, moerr.NewInternalErrorNoCtx("inconsistent repeated UPDATE target")
		}
		names := make([]string, 0, len(up.updateColPosMap))
		for name := range up.updateColPosMap {
			names = append(names, name)
		}
		sort.Slice(names, func(a, b int) bool { return up.updateColPosMap[names[a]] < up.updateColPosMap[names[b]] })
		if len(names) != len(values[i]) {
			return -1, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE values")
		}
		input := getProjectionByLastNode(builder, last)
		if markers[i] < 0 || markers[i] >= len(input) {
			return -1, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE marker")
		}
		active, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "isnotnull", []*planpb.Expr{DeepCopyExpr(input[markers[i]])})
		if err != nil {
			return -1, err
		}
		activePos := len(input)
		input = append(input, active)
		last = builder.appendNode(&planpb.Node{NodeType: planpb.Node_PROJECT, Children: []int32{last}, ProjectList: input}, bindCtx)
		var proposals []int
		last, names, proposals, activePos, err = appendRepeatedCandidate(
			builder, bindCtx, last, table, first.rowIdPos, activePos, current, names, values[i], isIgnore,
		)
		if err != nil {
			return -1, err
		}
		var keys []planpb.UpdateUniqueKey
		if isIgnore {
			last, keys, err = appendRepeatedIndexAdmission(builder, bindCtx, last, first, current, names, proposals)
			if err != nil {
				return -1, err
			}
		}
		output := getProjectionByLastNode(builder, last)
		base := len(output)
		output = append(output, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bool)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: int32(base)}}})
		pairs := make([]planpb.UpdateUniqueValue, len(names))
		candidate := append([]int(nil), current...)
		for j, name := range names {
			col, ok := table.Name2ColIndex[name]
			if !ok || col < 0 || int(col) >= oldWidth || proposals[j] < 0 || proposals[j] >= base {
				return -1, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE column")
			}
			pairs[j] = planpb.UpdateUniqueValue{Current: int32(current[col]), Proposed: int32(proposals[j])}
			candidate[col] = proposals[j]
			output = append(output, &planpb.Expr{Typ: table.Cols[col].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: int32(base + 1 + j)}}})
			current[col] = base + 1 + j
			updated[name] = struct{}{}
		}
		var checks []planpb.UpdateUniqueCheck
		for col, def := range table.Cols {
			if def.Hidden {
				continue
			}
			notNull := def.Default != nil && !def.Default.NullAbility
			typ := types.T(def.Typ.Id)
			if notNull || typ == types.T_date || typ == types.T_datetime || typ == types.T_timestamp {
				checks = append(checks, planpb.UpdateUniqueCheck{Column: int32(candidate[col]), Name: def.GetOriginCaseName(), NotNull: notNull})
			}
		}
		last = builder.appendNode(&planpb.Node{
			NodeType: planpb.Node_UPDATE_UNIQUE, Children: []int32{last}, ProjectList: output,
			UpdateUniqueCtx: &planpb.UpdateUniqueCtx{RowId: int32(first.rowIdPos), Active: int32(activePos), Keys: keys, Values: pairs, Checks: checks},
		}, bindCtx)
		output = getProjectionByLastNode(builder, last)
		count, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "if", []*planpb.Expr{
			output[base], makePlan2Uint64ConstExprWithType(1), makePlan2Uint64ConstExprWithType(0),
		})
		if err != nil {
			return -1, err
		}
		if countPos >= 0 {
			count, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "+", []*planpb.Expr{output[countPos], count})
			if err != nil {
				return -1, err
			}
		}
		// Drop this role's key lookups and intermediate values before the next
		// stage. Only the frozen inputs, current row and count remain live.
		compact := append([]*planpb.Expr(nil), output[:baseWidth]...)
		for _, pos := range current {
			compact = append(compact, output[pos])
		}
		countPos = len(compact)
		compact = append(compact, count)
		nextEligible := make(map[string]int, len(asyncEligible))
		for _, name := range asyncColumns {
			previous, alreadyUpdated := asyncEligible[name]
			roleUpdates := false
			for _, updatedName := range names {
				if updatedName == name {
					roleUpdates = true
					break
				}
			}
			if !alreadyUpdated && !roleUpdates {
				continue
			}
			var eligible *planpb.Expr
			if alreadyUpdated {
				eligible = output[previous]
			}
			if roleUpdates {
				if eligible == nil {
					eligible = output[base]
				} else {
					eligible, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "or", []*planpb.Expr{eligible, output[base]})
					if err != nil {
						return -1, err
					}
				}
			}
			nextEligible[name] = len(compact)
			compact = append(compact, eligible)
		}
		asyncEligible = nextEligible
		last = builder.appendNode(&planpb.Node{
			NodeType: planpb.Node_PROJECT, Children: []int32{last}, ProjectList: compact,
		}, bindCtx)
		for j := range current {
			current[j] = baseWidth + j
		}
	}

	output := getProjectionByLastNode(builder, last)
	project := append([]*planpb.Expr(nil), output[:oldWidth]...)
	names := make([]string, 0, len(updated))
	for name := range updated {
		names = append(names, name)
	}
	sort.Strings(names)
	first.updateColPosMap = make(map[string]int, len(names))
	for _, name := range names {
		col := table.Name2ColIndex[name]
		first.updateColPosMap[name] = len(project)
		project = append(project, output[current[col]])
	}
	first.updateColLength = len(names)
	first.insertColPos = nil
	for i, col := range table.Cols {
		if col.Hidden && col.Name != catalog.FakePrimaryKeyColName {
			continue
		}
		if pos, ok := first.updateColPosMap[col.Name]; ok {
			first.insertColPos = append(first.insertColPos, pos)
		} else {
			first.insertColPos = append(first.insertColPos, i)
		}
	}
	updatePK := false
	for _, up := range plans {
		updatePK = updatePK || up.updatePkCol
	}
	if table.Pkey != nil {
		for _, name := range table.Pkey.Names {
			_, changed := first.updateColPosMap[name]
			updatePK = updatePK || changed
		}
	}
	first.updatePkCol = updatePK
	first.dedupByRowNumber = true
	// Every participant has a non-NULL RowID from the role expander.
	first.needAggFilter = false
	first.logicalAffectedRows = true
	first.asyncDatalinkAdmitted = true

	projectCountPos := len(project)
	project = append(project, output[countPos])
	for _, name := range asyncColumns {
		if pos, ok := asyncEligible[name]; ok {
			asyncEligible[name] = len(project)
			project = append(project, output[pos])
		}
	}
	last = builder.appendNode(&planpb.Node{NodeType: planpb.Node_PROJECT, Children: []int32{last}, ProjectList: project}, bindCtx)
	if isIgnore {
		present, err := BindFuncExprImplByPlanExpr(builder.GetContext(), ">", []*planpb.Expr{
			getProjectionByLastNode(builder, last)[projectCountPos], makePlan2Uint64ConstExprWithType(0),
		})
		if err != nil {
			return -1, err
		}
		last = builder.appendNode(&planpb.Node{
			NodeType: planpb.Node_FILTER, Children: []int32{last}, FilterList: []*planpb.Expr{present},
			ProjectList: getProjectionByLastNode(builder, last),
		}, bindCtx)
		builder.markRepeatedUpdateCapacityFilter(last)
	}
	for _, name := range asyncColumns {
		pos, ok := asyncEligible[name]
		if !ok {
			continue
		}
		projection := getProjectionByLastNode(builder, last)
		last, err = appendAsyncDatalinkAdmission(builder, bindCtx, table, []string{name}, last, 0, func(column string) (int32, bool) {
			position, exists := first.updateColPosMap[column]
			return int32(position), exists
		}, projection[pos])
		if err != nil {
			return -1, err
		}
		builder.qry.Nodes[last].ProjectList = projection
		builder.markRepeatedUpdateCapacityFilter(last)
	}
	sink := appendSinkNode(builder, bindCtx, last)
	builder.qry.Nodes[sink].UpdateCountCtx = &planpb.UpdateCountCtx{Column: int32(projectCountPos)}
	if builder.preserveSinkProjection == nil {
		builder.preserveSinkProjection = make(map[int32]struct{})
	}
	builder.preserveSinkProjection[sink] = struct{}{}
	return builder.appendStep(sink), nil
}
