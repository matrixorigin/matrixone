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

	if node.UpdateInfo != nil {
		newNode.UpdateInfo = &plan.UpdateInfo{
			PriKey:      node.UpdateInfo.PriKey,
			PriKeyIdx:   node.UpdateInfo.PriKeyIdx,
			HideKey:     node.UpdateInfo.HideKey,
			UpdateAttrs: make([]string, len(node.UpdateInfo.UpdateAttrs)),
			OtherAttrs:  make([]string, len(node.UpdateInfo.OtherAttrs)),
			AttrOrders:  make([]string, len(node.UpdateInfo.AttrOrders)),
		}
		copy(newNode.UpdateInfo.UpdateAttrs, node.UpdateInfo.UpdateAttrs)
		copy(newNode.UpdateInfo.OtherAttrs, node.UpdateInfo.OtherAttrs)
		copy(newNode.UpdateInfo.AttrOrders, node.UpdateInfo.AttrOrders)
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

func DeepCopyTableDef(table *plan.TableDef) *plan.TableDef {
	newTable := &plan.TableDef{
		Name: table.Name,
		Cols: make([]*plan.ColDef, len(table.Cols)),
		Defs: make([]*plan.TableDef_DefType, len(table.Defs)),
	}
	for idx, col := range table.Cols {
		newTable.Cols[idx] = &plan.ColDef{
			Name: col.Name,
			Alg:  col.Alg,
			Typ: &plan.Type{
				Id:        col.Typ.Id,
				Nullable:  col.Typ.Nullable,
				Width:     col.Typ.Width,
				Precision: col.Typ.Precision,
				Size:      col.Typ.Size,
				Scale:     col.Typ.Scale,
			},
			// FIX ME: Default should change to Expr
			Default: &plan.DefaultExpr{},
			Primary: col.Primary,
			Pkidx:   col.Pkidx,
		}
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
