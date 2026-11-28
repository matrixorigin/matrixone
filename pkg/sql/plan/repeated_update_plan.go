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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type repeatedUpdateTarget struct {
	roles   []int
	step    int32
	markers []int
	values  [][]int
}

// buildRepeatedUpdatePlan gives each physical table a single direct writer.
// The role source is shared, while different physical targets keep separate
// write and affected-row streams.
func buildRepeatedUpdatePlan(
	stmt *tree.Update,
	ctx CompilerContext,
	builder *QueryBuilder,
	bindCtx *BindContext,
	info *dmlTableInfo,
) (*Plan, error) {
	assignments := &repeatedUpdateAssignments{}
	match, plans, err := selectUpdateTables(builder, bindCtx, stmt, info, assignments)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, up := range plans {
			putDmlPlanCtx(up)
		}
	}()
	if len(bindCtx.aggregates) > 0 {
		return nil, moerr.NewUnsupportedDML(builder.GetContext(), "repeated-target UPDATE with a top-level aggregate")
	}
	branches, err := buildRepeatedRoleBranches(builder, bindCtx, match, plans, assignments, stmt.Ignore)
	if err != nil {
		return nil, err
	}
	if err = repeatedUpdateSafetyBoundary(builder, bindCtx, plans); err != nil {
		return nil, err
	}
	targetByID := make(map[uint64]int, len(plans))
	var targets []repeatedUpdateTarget
	for i, up := range plans {
		index, exists := targetByID[up.tableDef.TblId]
		if !exists {
			index = len(targets)
			targetByID[up.tableDef.TblId] = index
			targets = append(targets, repeatedUpdateTarget{})
		}
		targets[index].roles = append(targets[index].roles, i)
	}
	for i := range targets {
		target := &targets[i]
		roleBranches := make([]repeatedUpdateBranch, len(target.roles))
		for j, role := range target.roles {
			roleBranches[j] = branches[role]
		}
		root, tag, markers, values, err := appendRepeatedParticipants(builder, bindCtx, roleBranches)
		if err != nil {
			return nil, err
		}
		target.markers, target.values = markers, values
		sink := appendSinkNodeWithTag(builder, bindCtx, root, tag)
		builder.preserveSinkProjection[sink] = struct{}{}
		target.step = builder.appendStep(sink)
	}
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}

	for _, target := range targets {
		up := plans[target.roles[0]]
		rolePlans := make([]*dmlPlanCtx, len(target.roles))
		for i, role := range target.roles {
			rolePlans[i] = plans[role]
		}
		target.step, err = appendRepeatedImages(builder, NewBindContext(builder, nil), target.step, rolePlans, target.markers, target.values, stmt.Ignore)
		if err != nil {
			return nil, err
		}
		up.beginIdx = 0
		up.sourceStep = target.step
		// The repeated-target plan has already checked each accepted role.
		up.checkMode = skipUpdateCheck
		// Checked FK key transitions were rejected by the safety boundary.
		up.skipForeignKeyChecks = true
		// The final assignment map already includes every role's changes.
		if err = buildUpdatePlans(ctx, builder, NewBindContext(builder, nil), up, false, false); err != nil {
			return nil, err
		}
	}
	reduceSinkSinkScanNodes(query)
	builder.tempOptimizeForDML()
	reCheckifNeedLockWholeTable(builder)
	query.StmtType = planpb.Query_UPDATE
	return &Plan{Plan: &planpb.Plan_Query{Query: query}}, nil
}
