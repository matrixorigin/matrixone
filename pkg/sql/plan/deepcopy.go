// Copyright 2022 Matrix Origin
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

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

func DeepCopyExprList(list []*Expr) []*Expr {
	newList := make([]*Expr, len(list))
	for idx, expr := range list {
		newList[idx] = DeepCopyExpr(expr)
	}
	return newList
}

func DeepCopyNode(node *plan.Node) *plan.Node {
	newNode := &Node{
		NodeType:        node.NodeType,
		NodeId:          node.NodeId,
		ExtraOptions:    node.ExtraOptions,
		Children:        make([]int32, len(node.Children)),
		JoinType:        node.JoinType,
		BindingTags:     make([]int32, len(node.BindingTags)),
		Limit:           DeepCopyExpr(node.Limit),
		Offset:          DeepCopyExpr(node.Offset),
		ProjectList:     make([]*plan.Expr, len(node.ProjectList)),
		OnList:          make([]*plan.Expr, len(node.OnList)),
		FilterList:      make([]*plan.Expr, len(node.FilterList)),
		GroupBy:         make([]*plan.Expr, len(node.GroupBy)),
		GroupingSet:     make([]*plan.Expr, len(node.GroupingSet)),
		AggList:         make([]*plan.Expr, len(node.AggList)),
		OrderBy:         make([]*plan.OrderBySpec, len(node.OrderBy)),
		DeleteTablesCtx: make([]*plan.DeleteTableCtx, len(node.DeleteTablesCtx)),
		UpdateCtxs:      make([]*plan.UpdateCtx, len(node.UpdateCtxs)),
		TableDefVec:     make([]*plan.TableDef, len(node.TableDefVec)),
	}

	copy(newNode.Children, node.Children)
	copy(newNode.BindingTags, node.BindingTags)

	for idx, expr := range node.ProjectList {
		newNode.ProjectList[idx] = DeepCopyExpr(expr)
	}

	for idx, expr := range node.OnList {
		newNode.OnList[idx] = DeepCopyExpr(expr)
	}

	for idx, expr := range node.FilterList {
		newNode.FilterList[idx] = DeepCopyExpr(expr)
	}

	for idx, expr := range node.GroupBy {
		newNode.GroupBy[idx] = DeepCopyExpr(expr)
	}

	for idx, expr := range node.GroupingSet {
		newNode.GroupingSet[idx] = DeepCopyExpr(expr)
	}

	for idx, expr := range node.AggList {
		newNode.AggList[idx] = DeepCopyExpr(expr)
	}

	for idx, orderBy := range node.OrderBy {
		newNode.OrderBy[idx] = &plan.OrderBySpec{
			Expr:      DeepCopyExpr(orderBy.Expr),
			Collation: orderBy.Collation,
			Flag:      orderBy.Flag,
		}
	}

	for idx, deleteTablesCtx := range node.DeleteTablesCtx {
		newNode.DeleteTablesCtx[idx] = &plan.DeleteTableCtx{
			DbName:       deleteTablesCtx.DbName,
			TblName:      deleteTablesCtx.TblName,
			UseDeleteKey: deleteTablesCtx.UseDeleteKey,
			CanTruncate:  deleteTablesCtx.CanTruncate,
		}
	}

	for i, updateCtx := range node.UpdateCtxs {
		newNode.UpdateCtxs[i] = &plan.UpdateCtx{
			DbName:     updateCtx.DbName,
			TblName:    updateCtx.TblName,
			PriKey:     updateCtx.PriKey,
			PriKeyIdx:  updateCtx.PriKeyIdx,
			HideKey:    updateCtx.HideKey,
			HideKeyIdx: updateCtx.HideKeyIdx,
			OrderAttrs: make([]string, len(updateCtx.OrderAttrs)),
			UpdateCols: make([]*ColDef, len(updateCtx.UpdateCols)),
			OtherAttrs: make([]string, len(updateCtx.OtherAttrs)),
		}
		for j, col := range updateCtx.UpdateCols {
			newNode.UpdateCtxs[i].UpdateCols[j] = &plan.ColDef{
				Name:    col.Name,
				Alg:     col.Alg,
				Typ:     DeepCopyTyp(col.Typ),
				Default: DeepCopyDefault(col.Default),
				Primary: col.Primary,
				Pkidx:   col.Pkidx,
			}
		}
		copy(newNode.UpdateCtxs[i].OtherAttrs, updateCtx.OtherAttrs)
		copy(newNode.UpdateCtxs[i].OrderAttrs, updateCtx.OrderAttrs)
	}

	for i, tbl := range node.TableDefVec {
		newNode.TableDefVec[i] = DeepCopyTableDef(tbl)
	}

	if node.Cost != nil {
		newNode.Cost = &plan.Cost{
			Card:    node.Cost.Card,
			Rowsize: node.Cost.Rowsize,
			Ndv:     node.Cost.Ndv,
			Start:   node.Cost.Start,
			Total:   node.Cost.Total,
		}
	}

	if node.ObjRef != nil {
		newNode.ObjRef = &plan.ObjectRef{
			Server:     node.ObjRef.Server,
			Db:         node.ObjRef.Db,
			Schema:     node.ObjRef.Schema,
			Obj:        node.ObjRef.Obj,
			ServerName: node.ObjRef.ServerName,
			DbName:     node.ObjRef.DbName,
			SchemaName: node.ObjRef.SchemaName,
			ObjName:    node.ObjRef.ObjName,
		}
	}

	if node.WinSpec != nil {
		newNode.WinSpec = &plan.WindowSpec{
			PartitionBy: make([]*plan.Expr, len(node.WinSpec.PartitionBy)),
			OrderBy:     make([]*plan.OrderBySpec, len(node.WinSpec.OrderBy)),
			Lead:        node.WinSpec.Lead,
			Lag:         node.WinSpec.Lag,
		}
		for idx, pb := range node.WinSpec.PartitionBy {
			newNode.WinSpec.PartitionBy[idx] = DeepCopyExpr(pb)
		}
		for idx, orderBy := range node.WinSpec.OrderBy {
			newNode.WinSpec.OrderBy[idx] = &plan.OrderBySpec{
				Expr:      DeepCopyExpr(orderBy.Expr),
				Collation: orderBy.Collation,
				Flag:      orderBy.Flag,
			}
		}
	}

	if node.TableDef != nil {
		newNode.TableDef = DeepCopyTableDef(node.TableDef)
	}

	if node.RowsetData != nil {
		newNode.RowsetData = &plan.RowsetData{
			Cols: make([]*plan.ColData, len(node.RowsetData.Cols)),
		}

		for idx, col := range node.RowsetData.Cols {
			newNode.RowsetData.Cols[idx] = DeepCopyColData(col)
		}

		if node.RowsetData.Schema != nil {
			newNode.RowsetData.Schema = DeepCopyTableDef(node.RowsetData.Schema)
		}
	}

	return newNode
}

func DeepCopyDefault(def *plan.Default) *plan.Default {
	if def == nil {
		return nil
	}
	return &plan.Default{
		NullAbility:  def.NullAbility,
		Expr:         DeepCopyExpr(def.Expr),
		OriginString: def.OriginString,
	}
}

func DeepCopyTyp(typ *plan.Type) *plan.Type {
	return &plan.Type{
		Id:        typ.Id,
		Nullable:  typ.Nullable,
		Width:     typ.Width,
		Precision: typ.Precision,
		Size:      typ.Size,
		Scale:     typ.Scale,
	}
}

func DeepCopyColDef(col *plan.ColDef) *plan.ColDef {
	return &plan.ColDef{
		Name:    col.Name,
		Alg:     col.Alg,
		Typ:     DeepCopyTyp(col.Typ),
		Default: DeepCopyDefault(col.Default),
		Primary: col.Primary,
		Pkidx:   col.Pkidx,
	}
}

func DeepCopyTableDef(table *plan.TableDef) *plan.TableDef {
	newTable := &plan.TableDef{
		Name:          table.Name,
		Cols:          make([]*plan.ColDef, len(table.Cols)),
		Defs:          make([]*plan.TableDef_DefType, len(table.Defs)),
		TableType:     table.TableType,
		Createsql:     table.Createsql,
		Name2ColIndex: table.Name2ColIndex,
		CompositePkey: table.CompositePkey,
	}

	for idx, col := range table.Cols {
		newTable.Cols[idx] = DeepCopyColDef(col)
	}
	// FIX ME: don't support now
	// for idx, def := range table.Defs {
	// 	newTable.Cols[idx] = &plan.TableDef_DefType{}
	// }
	return newTable
}

func DeepCopyColData(col *plan.ColData) *plan.ColData {
	newCol := &plan.ColData{
		RowCount:  col.RowCount,
		NullCount: col.NullCount,
		Nulls:     make([]bool, len(col.Nulls)),
		I32:       make([]int32, len(col.I32)),
		I64:       make([]int64, len(col.I64)),
		F32:       make([]float32, len(col.F32)),
		F64:       make([]float64, len(col.F64)),
		S:         make([]string, len(col.S)),
	}
	copy(newCol.Nulls, col.Nulls)
	copy(newCol.I32, col.I32)
	copy(newCol.I64, col.I64)
	copy(newCol.F32, col.F32)
	copy(newCol.F64, col.F64)
	copy(newCol.S, col.S)

	return newCol
}

func DeepCopyQuery(qry *plan.Query) *plan.Query {
	newQry := &plan.Query{
		StmtType: qry.StmtType,
		Steps:    qry.Steps,
		Nodes:    make([]*plan.Node, len(qry.Nodes)),
		Params:   make([]*plan.Expr, len(qry.Params)),
		Headings: qry.Headings,
	}
	for idx, param := range qry.Params {
		newQry.Params[idx] = DeepCopyExpr(param)
	}
	for idx, node := range qry.Nodes {
		newQry.Nodes[idx] = DeepCopyNode(node)
	}
	return newQry
}

func DeepCopyInsertValues(insert *plan.InsertValues) *plan.InsertValues {
	newInsert := &plan.InsertValues{
		DbName:       insert.DbName,
		TblName:      insert.TblName,
		ExplicitCols: make([]*plan.ColDef, len(insert.ExplicitCols)),
		OtherCols:    make([]*plan.ColDef, len(insert.OtherCols)),
		Columns:      make([]*plan.Column, len(insert.Columns)),
		OrderAttrs:   make([]string, len(insert.OrderAttrs)),
	}

	for idx, col := range insert.ExplicitCols {
		newInsert.ExplicitCols[idx] = DeepCopyColDef(col)
	}
	for idx, col := range insert.OtherCols {
		newInsert.OtherCols[idx] = DeepCopyColDef(col)
	}
	copy(newInsert.OrderAttrs, insert.OrderAttrs)
	for idx, column := range insert.Columns {
		newExprs := make([]*Expr, len(column.Column))
		for i, expr := range column.Column {
			newExprs[i] = DeepCopyExpr(expr)
		}
		newInsert.Columns[idx] = &plan.Column{
			Column: newExprs,
		}
	}
	return newInsert
}

func DeepCopyPlan(pl *Plan) *Plan {
	switch pl := pl.Plan.(type) {
	case *Plan_Query:
		return &Plan{
			Plan: &plan.Plan_Query{
				Query: DeepCopyQuery(pl.Query),
			},
		}
	case *plan.Plan_Ins:
		return &Plan{
			Plan: &plan.Plan_Ins{
				Ins: DeepCopyInsertValues(pl.Ins),
			},
		}
	default:
		// only support query/insert plan now
		return nil
	}
}
