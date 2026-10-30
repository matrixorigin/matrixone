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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const virtualTimestampStageOption = "virtual-timestamp-stage"

func mayHaveVirtualTimestampRead(table *plan.TableDef) bool {
	if table == nil {
		return false
	}
	hasTimestamp, hasVirtual := false, false
	for _, col := range table.Cols {
		if col == nil {
			continue
		}
		hasTimestamp = hasTimestamp || types.T(col.Typ.Id) == types.T_timestamp
		hasVirtual = hasVirtual || col.GeneratedCol != nil && !col.GeneratedCol.IsStored
		if hasTimestamp && hasVirtual {
			return true
		}
	}
	return false
}

// Historical catalogs could contain small generated-column DAGs whose fully
// inlined expression is enormous. Compute each affected column once, in
// dependency order, and pass its result to the next projection. This path is
// used only for catalogs that exceed the current DDL admission bound.
func (builder *QueryBuilder) stageLegacyVirtualTimestampColumns(scanID int32, ctx *BindContext) (int32, error) {
	scan := builder.qry.Nodes[scanID]
	if scan == nil || scan.NodeType != plan.Node_TABLE_SCAN || scan.TableDef == nil || len(scan.BindingTags) != 1 {
		return scanID, moerr.NewInternalError(builder.GetContext(), "legacy virtual timestamp staging requires a table scan")
	}
	table := scan.TableDef
	ordered, err := builder.virtualTimestampStageOrder(table)
	if err != nil {
		return scanID, err
	}
	if len(ordered) == 0 {
		return scanID, nil
	}
	previousID, previousTag := scanID, scan.BindingTags[0]
	type stageRef struct{ tag, nodeID int32 }
	stages := make([]stageRef, 0, len(ordered))
	for _, generatedPos := range ordered {
		previousID, previousTag, err = builder.appendVirtualTimestampStage(previousID, previousTag, table, generatedPos, ctx, previousID != scanID)
		if err != nil {
			return scanID, err
		}
		stages = append(stages, stageRef{previousTag, previousID})
	}
	binding := ctx.bindingByTag[scan.BindingTags[0]]
	if binding == nil {
		return scanID, moerr.NewInternalError(builder.GetContext(), "missing legacy virtual timestamp scan binding")
	}
	// Planner provenance and the hidden physical passthrough still refer to
	// the original scan tag. Keep an internal binding for those references;
	// name resolution continues to use the public final-stage binding.
	physicalBinding := *binding
	ctx.bindingByTag[physicalBinding.tag] = &physicalBinding
	for _, stage := range stages[:len(stages)-1] {
		internal := *binding
		internal.tag = stage.tag
		internal.nodeId = stage.nodeID
		internal.virtualTimestampStaged = true
		ctx.bindingByTag[stage.tag] = &internal
	}
	binding.tag = previousTag
	binding.nodeId = previousID
	binding.virtualTimestampStaged = true
	ctx.bindingByTag[previousTag] = binding
	for pos := range table.Cols {
		builder.nameByColRef[[2]int32{previousTag, int32(pos)}] = binding.table + "." + binding.cols[pos]
	}
	return previousID, nil
}

func (builder *QueryBuilder) virtualTimestampStageOrder(table *plan.TableDef) ([]int32, error) {
	classified := make([]uint8, len(table.Cols))
	visiting := make([]uint8, len(table.Cols))
	ordered := make([]int32, 0, len(table.Cols))
	var visit func(int32) error
	visit = func(pos int32) error {
		if pos < 0 || int(pos) >= len(table.Cols) || table.Cols[pos] == nil {
			return moerr.NewInvalidInputf(builder.GetContext(), "invalid generated column ordinal %d", pos)
		}
		switch visiting[pos] {
		case 1:
			return moerr.NewInvalidInputf(builder.GetContext(), "generated column cycle at %s", table.Cols[pos].Name)
		case 2:
			return nil
		}
		if !virtualTimestampColumnState(table, pos, classified) {
			visiting[pos] = 2
			return nil
		}
		generated := table.Cols[pos].GeneratedCol
		if generated == nil || generated.Expr == nil {
			return moerr.NewInvalidInputf(builder.GetContext(), "missing generated expression for %s", table.Cols[pos].Name)
		}
		visiting[pos] = 1
		if err := plan.VisitExprTree(generated.Expr, func(expr *plan.Expr) error {
			if col := expr.GetCol(); col != nil {
				return visit(col.ColPos)
			}
			return nil
		}); err != nil {
			return err
		}
		visiting[pos] = 2
		ordered = append(ordered, pos)
		return nil
	}
	for pos := range table.Cols {
		if err := visit(int32(pos)); err != nil {
			return nil, err
		}
	}
	return ordered, nil
}

func (builder *QueryBuilder) appendVirtualTimestampStage(previousID, previousTag int32, table *plan.TableDef, generatedPos int32, ctx *BindContext, hasOldImage bool) (int32, int32, error) {
	projects := make([]*plan.Expr, 2*len(table.Cols))
	for pos, col := range table.Cols {
		projects[pos] = GetColExpr(col.Typ, previousTag, int32(pos))
		physicalPos := int32(pos)
		if hasOldImage {
			physicalPos += int32(len(table.Cols))
		}
		projects[len(table.Cols)+pos] = GetColExpr(col.Typ, previousTag, physicalPos)
	}
	expr := DeepCopyExpr(table.Cols[generatedPos].GeneratedCol.Expr)
	if err := plan.VisitExprTree(expr, func(node *plan.Expr) error {
		if col := node.GetCol(); col != nil {
			col.RelPos = previousTag
		}
		return nil
	}); err != nil {
		return 0, 0, err
	}
	projects[generatedPos] = expr
	newTag := builder.genNewBindTag()
	stageID := builder.appendNode(&plan.Node{
		NodeType:     plan.Node_PROJECT,
		ProjectList:  projects,
		Children:     []int32{previousID},
		BindingTags:  []int32{newTag},
		ExtraOptions: virtualTimestampStageOption,
	}, ctx)
	return stageID, newTag, nil
}
