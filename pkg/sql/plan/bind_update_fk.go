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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const foreignKeyNoReferencedRowAssert = "fk_no_referenced_row"

func (builder *QueryBuilder) appendUpdateForeignKeyChecks(
	bindCtx *BindContext,
	dmlCtx *DMLContext,
	lastNodeID int32,
	selectNodeTag int32,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
	enabled bool,
) (int32, int32, *plan.Node, error) {
	if !enabled {
		return lastNodeID, selectNodeTag, builder.qry.Nodes[lastNodeID], nil
	}

	var err error
	for i, tableDef := range dmlCtx.tableDefs {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}

		alias := dmlCtx.aliases[i]
		if err = builder.rejectUpdateOfReferencedParentKey(
			bindCtx,
			tableDef,
			alias,
			newColName2Idx,
		); err != nil {
			return 0, 0, nil, err
		}

		affectedFks := affectedUpdateChildFks(tableDef, alias, newColName2Idx)
		if len(affectedFks) == 0 {
			continue
		}

		fkTableDef := *tableDef
		fkTableDef.Fkeys = make([]*plan.ForeignKeyDef, len(affectedFks))
		for j, fk := range affectedFks {
			fkTableDef.Fkeys[j] = DeepCopyFkey(fk)
			if fkTableDef.Fkeys[j].ForeignTbl == 0 {
				fkTableDef.Fkeys[j].ForeignTbl = tableDef.TblId
			}
		}

		sourceNode := builder.qry.Nodes[lastNodeID]
		projLen := len(sourceNode.ProjectList)
		projectTypes := make([]plan.Type, projLen)
		for j, expr := range sourceNode.ProjectList {
			projectTypes[j] = expr.Typ
		}

		var oks []*plan.Expr
		lastNodeID, oks, err = builder.appendModernChildFkMarkOks(
			bindCtx,
			&fkTableDef,
			lastNodeID,
			selectNodeTag,
			func(colName string) int32 {
				qualifiedName := alias + "." + colName
				if pos, ok := newColName2Idx[qualifiedName]; ok {
					return pos
				}
				return oldColName2Idx[qualifiedName]
			},
		)
		if err != nil {
			return 0, 0, nil, err
		}

		assertConds := make([]*plan.Expr, len(oks))
		errExpr := makePlan2StringConstExprWithType(
			"Cannot add or update a child row: a foreign key constraint fails",
		)
		errTypeExpr := makePlan2StringConstExprWithType(foreignKeyNoReferencedRowAssert)
		for j, fk := range affectedFks {
			unchanged, buildErr := builder.buildUpdateFkUnchangedExpr(
				tableDef,
				fk,
				alias,
				selectNodeTag,
				oldColName2Idx,
				newColName2Idx,
				sourceNode,
			)
			if buildErr != nil {
				return 0, 0, nil, buildErr
			}
			ok := oks[j]
			if unchanged != nil {
				ok, err = BindFuncExprImplByPlanExpr(
					builder.GetContext(),
					"or",
					[]*plan.Expr{ok, unchanged},
				)
				if err != nil {
					return 0, 0, nil, err
				}
			}
			assertConds[j], err = BindFuncExprImplByPlanExpr(
				builder.GetContext(),
				"assert",
				[]*plan.Expr{ok, DeepCopyExpr(errExpr), DeepCopyExpr(errTypeExpr)},
			)
			if err != nil {
				return 0, 0, nil, err
			}
		}

		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:   plan.Node_FILTER,
			Children:   []int32{lastNodeID},
			FilterList: assertConds,
		}, bindCtx)

		validatedTag := builder.genNewBindTag()
		projectList := make([]*plan.Expr, projLen)
		for j := range projectList {
			projectList[j] = &plan.Expr{
				Typ: projectTypes[j],
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: selectNodeTag,
						ColPos: int32(j),
					},
				},
			}
		}
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			ProjectList: projectList,
			Children:    []int32{lastNodeID},
			BindingTags: []int32{validatedTag},
		}, bindCtx)

		selectNodeTag = validatedTag
	}

	return lastNodeID, selectNodeTag, builder.qry.Nodes[lastNodeID], nil
}

func updateMayDependOnForeignKeys(
	dmlCtx *DMLContext,
	newColName2Idx map[string]int32,
) bool {
	for i, tableDef := range dmlCtx.tableDefs {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}
		if len(affectedUpdateChildFks(tableDef, dmlCtx.aliases[i], newColName2Idx)) > 0 ||
			len(tableDef.RefChildTbls) > 0 {
			return true
		}
	}
	return false
}

func affectedUpdateChildFks(
	tableDef *plan.TableDef,
	alias string,
	newColName2Idx map[string]int32,
) []*plan.ForeignKeyDef {
	if tableDef == nil {
		return nil
	}

	colIDToName := make(map[uint64]string, len(tableDef.Cols))
	for _, col := range tableDef.Cols {
		colIDToName[col.ColId] = col.Name
	}

	affected := make([]*plan.ForeignKeyDef, 0, len(tableDef.Fkeys))
	for _, fk := range tableDef.Fkeys {
		for _, childColID := range fk.Cols {
			if _, ok := newColName2Idx[alias+"."+colIDToName[childColID]]; ok {
				affected = append(affected, fk)
				break
			}
		}
	}
	return affected
}

func (builder *QueryBuilder) rejectUpdateOfReferencedParentKey(
	bindCtx *BindContext,
	tableDef *plan.TableDef,
	alias string,
	newColName2Idx map[string]int32,
) error {
	if tableDef == nil || len(tableDef.RefChildTbls) == 0 {
		return nil
	}

	parentColIDToName := make(map[uint64]string, len(tableDef.Cols))
	for _, col := range tableDef.Cols {
		parentColIDToName[col.ColId] = col.Name
	}

	visited := make(map[uint64]struct{}, len(tableDef.RefChildTbls))
	for _, childTableID := range tableDef.RefChildTbls {
		if childTableID == 0 {
			childTableID = tableDef.TblId
		}
		if _, ok := visited[childTableID]; ok {
			continue
		}
		visited[childTableID] = struct{}{}

		_, childTableDef, err := builder.compCtx.ResolveById(childTableID, bindCtx.snapshot)
		if err != nil {
			return err
		}
		if childTableDef == nil {
			return moerr.NewInternalErrorf(
				builder.GetContext(),
				"foreign-key child table %d not found",
				childTableID,
			)
		}

		for _, fk := range childTableDef.Fkeys {
			referencesCurrentTable := fk.ForeignTbl == tableDef.TblId ||
				(fk.ForeignTbl == 0 && childTableDef.TblId == tableDef.TblId)
			if !referencesCurrentTable {
				continue
			}
			for _, parentColID := range fk.ForeignCols {
				if _, ok := newColName2Idx[alias+"."+parentColIDToName[parentColID]]; ok {
					return newLegacyUpdatePlannerRouteError(
						updateRouteReasonForeignKey,
						moerr.NewUnsupportedDML(
							builder.GetContext(),
							foreignKeyUnsupportedDMLCause,
						),
					)
				}
			}
		}
	}
	return nil
}

func (builder *QueryBuilder) buildUpdateFkUnchangedExpr(
	tableDef *plan.TableDef,
	fk *plan.ForeignKeyDef,
	alias string,
	selectNodeTag int32,
	oldColName2Idx map[string]int32,
	newColName2Idx map[string]int32,
	selectNode *plan.Node,
) (*plan.Expr, error) {
	colIDToName := make(map[uint64]string, len(tableDef.Cols))
	for _, col := range tableDef.Cols {
		colIDToName[col.ColId] = col.Name
	}

	var unchanged *plan.Expr
	for _, childColID := range fk.Cols {
		qualifiedName := alias + "." + colIDToName[childColID]
		newPos, updated := newColName2Idx[qualifiedName]
		if !updated {
			continue
		}
		oldPos := oldColName2Idx[qualifiedName]
		oldExpr := &plan.Expr{
			Typ: selectNode.ProjectList[oldPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: selectNodeTag,
					ColPos: oldPos,
				},
			},
		}
		newExpr := &plan.Expr{
			Typ: selectNode.ProjectList[newPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: selectNodeTag,
					ColPos: newPos,
				},
			},
		}
		equal, err := BindFuncExprImplByPlanExpr(
			builder.GetContext(),
			"<=>",
			[]*plan.Expr{oldExpr, newExpr},
		)
		if err != nil {
			return nil, err
		}
		if unchanged == nil {
			unchanged = equal
			continue
		}
		unchanged, err = BindFuncExprImplByPlanExpr(
			builder.GetContext(),
			"and",
			[]*plan.Expr{unchanged, equal},
		)
		if err != nil {
			return nil, err
		}
	}
	return unchanged, nil
}
