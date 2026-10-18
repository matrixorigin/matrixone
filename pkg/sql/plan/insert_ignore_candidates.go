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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// retainInsertIgnoreCandidates materializes constraint predicates exactly once.
// CHECK eligibility is consumed before allocation; FK eligibility is consumed
// by the ordered arbiter. Column positions remain stable except for the new flag.
func (builder *QueryBuilder) retainInsertIgnoreCandidates(
	bindCtx *BindContext, nodeID, inputTag int32, inputProjection []*plan.Expr, eligibility *int32,
) (int32, error) {
	node := builder.qry.Nodes[nodeID]
	if node.NodeType != plan.Node_FILTER || len(node.FilterList) == 0 {
		return 0, moerr.NewInternalError(builder.GetContext(), "missing INSERT IGNORE constraint filter")
	}
	pass := node.FilterList[0]
	for _, predicate := range node.FilterList[1:] {
		var err error
		pass, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "and", []*plan.Expr{pass, predicate})
		if err != nil {
			return 0, err
		}
	}
	width := len(inputProjection)
	projection := make([]*plan.Expr, width, width+1)
	for i, expr := range inputProjection {
		projection[i] = &plan.Expr{Typ: expr.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: inputTag, ColPos: int32(i),
		}}}
	}
	*eligibility = int32(width)
	projection = append(projection, pass)
	tag := builder.genNewBindTag()
	node.NodeType = plan.Node_PROJECT
	node.ProjectList = projection
	node.FilterList = nil
	node.FilterIsBarrier = false
	node.BindingTags = []int32{tag}
	return tag, nil
}

func hasSelfReferencingForeignKeys(table *plan.TableDef) bool {
	for _, fk := range table.Fkeys {
		if fk.ForeignTbl == 0 {
			return true
		}
	}
	return false
}

func autoIncrementForeignKey(table *plan.TableDef, fk *plan.ForeignKeyDef) bool {
	for _, col := range table.Cols {
		if col != nil && col.Typ.AutoIncr {
			for _, id := range fk.Cols {
				if id == col.ColId {
					return true
				}
			}
		}
	}
	return false
}

func independentAutoIncrementForeignKeys(table *plan.TableDef) *plan.TableDef {
	result := *table
	result.Fkeys = nil
	for _, fk := range table.Fkeys {
		if !autoIncrementForeignKey(table, fk) && fk.ForeignTbl != 0 {
			result.Fkeys = append(result.Fkeys, fk)
		}
	}
	return &result
}

func (builder *QueryBuilder) orderedAutoIncrementForeignKeys(bindCtx *BindContext, table *plan.TableDef, objRef *plan.ObjectRef, columns map[string]int32, includeAutoIncrement bool) ([]*plan.OrderedForeignKey, error) {
	if len(table.Fkeys) == 0 {
		return nil, nil
	}
	enabled, err := IsForeignKeyChecksEnabled(builder.compCtx)
	if err != nil || !enabled {
		return nil, err
	}
	var result []*plan.OrderedForeignKey
	for _, fk := range table.Fkeys {
		if fk.ForeignTbl != 0 && (!includeAutoIncrement || !autoIncrementForeignKey(table, fk)) {
			continue
		}
		ref, parent := objRef, table
		if fk.ForeignTbl != 0 {
			ref, parent, err = builder.compCtx.ResolveById(fk.ForeignTbl, bindCtx.snapshot)
			if err != nil {
				return nil, err
			}
		}
		if parent == nil || len(fk.Cols) == 0 || len(fk.Cols) != len(fk.ForeignCols) {
			return nil, moerr.NewInternalError(builder.GetContext(), "invalid ordered foreign key")
		}
		builder.qry.CatalogDependencies = appendPrepareSchemas(builder.qry.CatalogDependencies, prepareSchemaRefWithSnapshot(ref, parent, bindCtx.snapshot))
		check := &plan.OrderedForeignKey{Ref: DeepCopyObjectRef(ref), TableDef: DeepCopyTableDef(parent, true)}
		for i, id := range fk.Cols {
			childPos, parentPos := int32(-1), int32(-1)
			var childType plan.Type
			for _, col := range table.Cols {
				if col.ColId == id {
					childPos = columns[table.Name+"."+col.Name]
					childType = col.Typ
					break
				}
			}
			for pos, col := range parent.Cols {
				if col.ColId == fk.ForeignCols[i] {
					parentPos = int32(pos)
					break
				}
			}
			if childPos < 0 || parentPos < 0 {
				return nil, moerr.NewInternalError(builder.GetContext(), "missing ordered foreign-key column")
			}
			check.ChildColumns = append(check.ChildColumns, &plan.ColRef{ColPos: childPos})
			check.ParentColumns = append(check.ParentColumns, parentPos)
			if fk.ForeignTbl == 0 {
				check.SelfParentColumns = append(check.SelfParentColumns, &plan.ColRef{ColPos: columns[table.Name+"."+parent.Cols[parentPos].Name]})
			}
			predicate, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
				{Typ: parent.Cols[parentPos].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: parentPos, Name: parent.Cols[parentPos].Name}}},
				{Typ: childType, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: int32(i)}}},
			})
			if err != nil {
				return nil, err
			}
			if check.Predicate == nil {
				check.Predicate = predicate
			} else {
				check.Predicate, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "and", []*plan.Expr{check.Predicate, predicate})
				if err != nil {
					return nil, err
				}
			}
		}
		check.ParentAccess, err = builder.orderedForeignKeyAccess(bindCtx, check)
		if err != nil {
			return nil, err
		}
		result = append(result, check)
	}
	return result, nil
}
