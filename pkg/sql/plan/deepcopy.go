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

func DeepCopyOrderBy(orderBy *plan.OrderBySpec) *plan.OrderBySpec {
	if orderBy == nil {
		return nil
	}
	return &plan.OrderBySpec{
		Expr:      DeepCopyExpr(orderBy.Expr),
		Collation: orderBy.Collation,
		Flag:      orderBy.Flag,
	}
}

func DeepCopyObjectRef(ref *plan.ObjectRef) *plan.ObjectRef {
	if ref == nil {
		return nil
	}
	return &plan.ObjectRef{
		Server:     ref.Server,
		Db:         ref.Db,
		Schema:     ref.Schema,
		Obj:        ref.Obj,
		ServerName: ref.ServerName,
		DbName:     ref.DbName,
		SchemaName: ref.SchemaName,
		ObjName:    ref.ObjName,
	}
}

func DeepCopyInsertCtx(ctx *plan.InsertCtx) *plan.InsertCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &plan.InsertCtx{
		Ref:            DeepCopyObjectRef(ctx.Ref),
		OnDuplicateIdx: make([]int32, len(ctx.OnDuplicateIdx)),
		TableDef:       DeepCopyTableDef(ctx.TableDef),

		IdxIdx: make([]int32, len(ctx.IdxIdx)),
		IdxRef: make([]*plan.ObjectRef, len(ctx.IdxRef)),

		ClusterTable: DeepCopyClusterTable(ctx.ClusterTable),
	}

	copy(newCtx.OnDuplicateIdx, ctx.OnDuplicateIdx)
	copy(newCtx.IdxIdx, ctx.IdxIdx)

	for i, ref := range ctx.IdxRef {
		newCtx.IdxRef[i] = DeepCopyObjectRef(ref)
	}

	if ctx.ParentIdx != nil {
		newCtx.ParentIdx = make(map[string]int32)
		for k, v := range ctx.ParentIdx {
			newCtx.ParentIdx[k] = v
		}
	}
	if ctx.OnDuplicateExpr != nil {
		newCtx.OnDuplicateExpr = make(map[string]*Expr)
		for k, v := range ctx.OnDuplicateExpr {
			newCtx.OnDuplicateExpr[k] = DeepCopyExpr(v)
		}
	}

	return newCtx
}

func DeepCopyDeleteCtx(ctx *plan.DeleteCtx) *plan.DeleteCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &plan.DeleteCtx{
		CanTruncate:   ctx.CanTruncate,
		OnRestrictIdx: make([]int32, len(ctx.OnRestrictIdx)),
		IdxIdx:        make([]int32, len(ctx.IdxIdx)),
		OnCascadeIdx:  make([]int32, len(ctx.OnCascadeIdx)),

		Ref:    make([]*plan.ObjectRef, len(ctx.Ref)),
		IdxRef: make([]*plan.ObjectRef, len(ctx.IdxRef)),

		OnRestrictRef: make([]*plan.ObjectRef, len(ctx.OnRestrictRef)),

		OnCascadeRef: make([]*plan.ObjectRef, len(ctx.OnCascadeRef)),

		OnSetRef:       make([]*plan.ObjectRef, len(ctx.OnSetRef)),
		OnSetDef:       make([]*plan.TableDef, len(ctx.OnSetDef)),
		OnSetIdx:       make([]*plan.IdList, len(ctx.OnSetIdx)),
		OnSetUpdateCol: make([]*plan.ColPosMap, len(ctx.OnSetUpdateCol)),
	}

	copy(newCtx.OnRestrictIdx, ctx.OnRestrictIdx)
	copy(newCtx.IdxIdx, ctx.IdxIdx)
	copy(newCtx.OnCascadeIdx, ctx.OnCascadeIdx)

	for i, ref := range ctx.Ref {
		newCtx.Ref[i] = DeepCopyObjectRef(ref)
	}
	for i, ref := range ctx.IdxRef {
		newCtx.IdxRef[i] = DeepCopyObjectRef(ref)
	}
	for i, ref := range ctx.OnRestrictRef {
		newCtx.OnRestrictRef[i] = DeepCopyObjectRef(ref)
	}
	for i, ref := range ctx.OnCascadeRef {
		newCtx.OnCascadeRef[i] = DeepCopyObjectRef(ref)
	}
	for i, ref := range ctx.OnSetRef {
		newCtx.OnSetRef[i] = DeepCopyObjectRef(ref)
	}
	for i, def := range ctx.OnSetDef {
		newCtx.OnSetDef[i] = DeepCopyTableDef(def)
	}
	for i, list := range ctx.OnSetIdx {
		if list != nil {
			newCtx.OnSetIdx[i] = &plan.IdList{
				List: make([]int64, len(list.List)),
			}
			copy(newCtx.OnSetIdx[i].List, list.List)
		}
	}
	for i, m := range ctx.OnSetUpdateCol {
		newMap := make(map[string]int32)
		for k, v := range m.Map {
			newMap[k] = v
		}
		newCtx.OnSetUpdateCol[i] = &plan.ColPosMap{Map: newMap}
	}
	return newCtx
}

func DeepCopyUpdateCtx(ctx *plan.UpdateCtx) *plan.UpdateCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &plan.UpdateCtx{
		Ref:       make([]*plan.ObjectRef, len(ctx.Ref)),
		Idx:       make([]*plan.IdList, len(ctx.Idx)),
		TableDefs: make([]*plan.TableDef, len(ctx.TableDefs)),
		UpdateCol: make([]*plan.ColPosMap, len(ctx.UpdateCol)),

		IdxRef: make([]*plan.ObjectRef, len(ctx.IdxRef)),
		IdxIdx: make([]int32, len(ctx.IdxIdx)),

		OnRestrictRef: make([]*plan.ObjectRef, len(ctx.OnRestrictRef)),
		OnRestrictIdx: make([]int32, len(ctx.OnRestrictIdx)),

		OnCascadeRef:       make([]*plan.ObjectRef, len(ctx.OnCascadeRef)),
		OnCascadeIdx:       make([]*plan.IdList, len(ctx.OnCascadeIdx)),
		OnCascadeDef:       make([]*plan.TableDef, len(ctx.OnCascadeDef)),
		OnCascadeUpdateCol: make([]*plan.ColPosMap, len(ctx.OnCascadeUpdateCol)),

		OnSetRef:       make([]*plan.ObjectRef, len(ctx.OnSetRef)),
		OnSetIdx:       make([]*plan.IdList, len(ctx.OnSetIdx)),
		OnSetDef:       make([]*plan.TableDef, len(ctx.OnSetDef)),
		OnSetUpdateCol: make([]*plan.ColPosMap, len(ctx.OnSetUpdateCol)),

		ParentIdx: make([]*plan.ColPosMap, len(ctx.ParentIdx)),
	}

	for i, ref := range ctx.Ref {
		newCtx.Ref[i] = DeepCopyObjectRef(ref)
	}
	for i, def := range ctx.TableDefs {
		newCtx.TableDefs[i] = DeepCopyTableDef(def)
	}
	for i, m := range ctx.UpdateCol {
		newMap := make(map[string]int32)
		for k, v := range m.Map {
			newMap[k] = v
		}
		newCtx.UpdateCol[i] = &plan.ColPosMap{Map: newMap}
	}
	for i, list := range ctx.Idx {
		if list != nil {
			newCtx.Idx[i] = &plan.IdList{
				List: make([]int64, len(list.List)),
			}
			copy(newCtx.Idx[i].List, list.List)
		}
	}

	for i, ref := range ctx.IdxRef {
		newCtx.IdxRef[i] = DeepCopyObjectRef(ref)
	}
	copy(newCtx.IdxIdx, ctx.IdxIdx)

	for i, ref := range ctx.OnRestrictRef {
		newCtx.OnRestrictRef[i] = DeepCopyObjectRef(ref)
	}
	copy(newCtx.OnRestrictIdx, ctx.OnRestrictIdx)

	for i, ref := range ctx.OnSetRef {
		newCtx.OnSetRef[i] = DeepCopyObjectRef(ref)
	}
	for i, def := range ctx.OnSetDef {
		newCtx.OnSetDef[i] = DeepCopyTableDef(def)
	}
	for i, list := range ctx.OnSetIdx {
		if list != nil {
			newCtx.OnSetIdx[i] = &plan.IdList{
				List: make([]int64, len(list.List)),
			}
			copy(newCtx.OnSetIdx[i].List, list.List)
		}
	}
	for i, m := range ctx.OnSetUpdateCol {
		newMap := make(map[string]int32)
		for k, v := range m.Map {
			newMap[k] = v
		}
		newCtx.OnSetUpdateCol[i] = &plan.ColPosMap{Map: newMap}
	}

	for i, ref := range ctx.OnCascadeRef {
		newCtx.OnCascadeRef[i] = DeepCopyObjectRef(ref)
	}
	for i, def := range ctx.OnCascadeDef {
		newCtx.OnCascadeDef[i] = DeepCopyTableDef(def)
	}
	for i, list := range ctx.OnCascadeIdx {
		if list != nil {
			newCtx.OnCascadeIdx[i] = &plan.IdList{
				List: make([]int64, len(list.List)),
			}
			copy(newCtx.OnCascadeIdx[i].List, list.List)
		}
	}
	for i, m := range ctx.OnCascadeUpdateCol {
		newMap := make(map[string]int32)
		for k, v := range m.Map {
			newMap[k] = v
		}
		newCtx.OnCascadeUpdateCol[i] = &plan.ColPosMap{Map: newMap}
	}

	for i, m := range ctx.ParentIdx {
		newMap := make(map[string]int32)
		for k, v := range m.Map {
			newMap[k] = v
		}
		newCtx.ParentIdx[i] = &plan.ColPosMap{Map: newMap}
	}
	return newCtx
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
		DeleteCtx:       DeepCopyDeleteCtx(node.DeleteCtx),
		UpdateCtx:       DeepCopyUpdateCtx(node.UpdateCtx),
		TableDefVec:     make([]*plan.TableDef, len(node.TableDefVec)),
		TblFuncExprList: make([]*plan.Expr, len(node.TblFuncExprList)),
		ClusterTable:    DeepCopyClusterTable(node.GetClusterTable()),
		InsertCtx:       DeepCopyInsertCtx(node.InsertCtx),
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
		newNode.OrderBy[idx] = DeepCopyOrderBy(orderBy)
	}

	for i, tbl := range node.TableDefVec {
		newNode.TableDefVec[i] = DeepCopyTableDef(tbl)
	}

	if node.Stats != nil {
		newNode.Stats = &plan.Stats{
			BlockNum:    node.Stats.BlockNum,
			Rowsize:     node.Stats.Rowsize,
			HashmapSize: node.Stats.HashmapSize,
			Cost:        node.Stats.Cost,
			Outcnt:      node.Stats.Outcnt,
		}
	}

	newNode.ObjRef = DeepCopyObjectRef(node.ObjRef)

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
			newNode.WinSpec.OrderBy[idx] = DeepCopyOrderBy(orderBy)
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
	}
	for idx, expr := range node.TblFuncExprList {
		newNode.TblFuncExprList[idx] = DeepCopyExpr(expr)
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
	if typ == nil {
		return nil
	}
	return &plan.Type{
		Id:          typ.Id,
		NotNullable: typ.NotNullable,
		Width:       typ.Width,
		Size:        typ.Size,
		Scale:       typ.Scale,
		AutoIncr:    typ.AutoIncr,
	}
}

func DeepCopyColDef(col *plan.ColDef) *plan.ColDef {
	if col == nil {
		return nil
	}
	return &plan.ColDef{
		ColId:     col.ColId,
		Name:      col.Name,
		Alg:       col.Alg,
		Typ:       DeepCopyTyp(col.Typ),
		Default:   DeepCopyDefault(col.Default),
		Primary:   col.Primary,
		Pkidx:     col.Pkidx,
		Comment:   col.Comment,
		OnUpdate:  DeepCopyOnUpdate(col.OnUpdate),
		ClusterBy: col.ClusterBy,
	}
}

func DeepCopyPrimaryKeyDef(pkeyDef *plan.PrimaryKeyDef) *plan.PrimaryKeyDef {
	if pkeyDef == nil {
		return nil
	}
	def := &plan.PrimaryKeyDef{
		PkeyColName: pkeyDef.PkeyColName,
		Names:       make([]string, len(pkeyDef.Names)),
	}
	copy(def.Names, pkeyDef.Names)
	return def
}

func DeepCopyIndexDef(indexDef *plan.IndexDef) *plan.IndexDef {
	if indexDef == nil {
		return nil
	}
	newindexDef := &plan.IndexDef{
		IdxId:          indexDef.IdxId,
		IndexName:      indexDef.IndexName,
		Unique:         indexDef.Unique,
		TableExist:     indexDef.TableExist,
		IndexTableName: indexDef.IndexTableName,
		Comment:        indexDef.Comment,
	}

	newParts := make([]string, len(indexDef.Parts))
	copy(newParts, indexDef.Parts)
	newindexDef.Parts = newParts
	return newindexDef
}

func DeepCopyOnUpdate(old *plan.OnUpdate) *plan.OnUpdate {
	if old == nil {
		return nil
	}
	return &plan.OnUpdate{
		Expr:         DeepCopyExpr(old.Expr),
		OriginString: old.OriginString,
	}
}

func DeepCopyTableDef(table *plan.TableDef) *plan.TableDef {
	if table == nil {
		return nil
	}
	newTable := &plan.TableDef{
		Name:          table.Name,
		Cols:          make([]*plan.ColDef, len(table.Cols)),
		Defs:          make([]*plan.TableDef_DefType, len(table.Defs)),
		TableType:     table.TableType,
		Createsql:     table.Createsql,
		Name2ColIndex: table.Name2ColIndex,
		CompositePkey: nil,
		OriginCols:    make([]*plan.ColDef, len(table.OriginCols)),
		Indexes:       make([]*IndexDef, len(table.Indexes)),
	}

	for idx, col := range table.Cols {
		newTable.Cols[idx] = DeepCopyColDef(col)
	}

	for idx, col := range table.OriginCols {
		newTable.OriginCols[idx] = DeepCopyColDef(col)
	}

	if table.TblFunc != nil {
		newTable.TblFunc = &plan.TableFunction{
			Name:  table.TblFunc.Name,
			Param: make([]byte, len(table.TblFunc.Param)),
		}
		copy(newTable.TblFunc.Param, table.TblFunc.Param)
	}

	if table.Pkey != nil {
		newTable.Pkey = &plan.PrimaryKeyDef{
			Names: make([]string, len(table.Pkey.Names)),
		}
		copy(newTable.Pkey.Names, table.Pkey.Names)
	}

	if table.CompositePkey != nil {
		newTable.CompositePkey = DeepCopyColDef(table.CompositePkey)
	}
	if table.ClusterBy != nil {
		newTable.ClusterBy = &plan.ClusterByDef{
			Parts: make([]*plan.Expr, len(table.ClusterBy.Parts)),
			Name:  table.ClusterBy.Name,
		}
		for i, part := range table.ClusterBy.Parts {
			newTable.ClusterBy.Parts[i] = DeepCopyExpr(part)
		}
	}

	if table.ViewSql != nil {
		newTable.ViewSql = &plan.ViewDef{
			View: table.ViewSql.View,
		}
	}

	if table.Partition != nil {
		partitionDef := &plan.PartitionByDef{
			Type:                table.Partition.GetType(),
			PartitionExpression: table.Partition.GetPartitionExpression(),
			PartitionNum:        table.Partition.GetPartitionNum(),
			Partitions:          make([]*plan.PartitionItem, len(table.Partition.Partitions)),
			Algorithm:           table.Partition.GetAlgorithm(),
			IsSubPartition:      table.Partition.GetIsSubPartition(),
			PartitionMsg:        table.Partition.GetPartitionMsg(),
		}
		if table.Partition.PartitionExpr != nil {
			partitionDef.PartitionExpr = &plan.PartitionExpr{
				Expr:    DeepCopyExpr(table.Partition.PartitionExpr.Expr),
				ExprStr: table.Partition.PartitionExpr.GetExprStr(),
			}
		}

		if table.Partition.PartitionColumns != nil {
			partitionDef.PartitionColumns = &plan.PartitionColumns{
				Columns:          make([]*plan.Expr, len(table.Partition.PartitionColumns.Columns)),
				PartitionColumns: make([]string, len(table.Partition.PartitionColumns.PartitionColumns)),
			}
			for i, e := range table.Partition.PartitionColumns.Columns {
				partitionDef.PartitionColumns.Columns[i] = DeepCopyExpr(e)
			}
			copy(partitionDef.PartitionColumns.PartitionColumns, table.Partition.PartitionColumns.PartitionColumns)
		}

		for i, e := range table.Partition.Partitions {
			partitionDef.Partitions[i] = &plan.PartitionItem{
				PartitionName:   e.PartitionName,
				OrdinalPosition: e.OrdinalPosition,
				Description:     e.Description,
				Comment:         e.Comment,
				LessThan:        make([]*plan.Expr, len(e.LessThan)),
				InValues:        make([]*plan.Expr, len(e.InValues)),
			}
			for j, ee := range e.LessThan {
				partitionDef.Partitions[i].LessThan[j] = DeepCopyExpr(ee)
			}
			for j, ee := range e.InValues {
				partitionDef.Partitions[i].InValues[j] = DeepCopyExpr(ee)
			}
		}
		newTable.Partition = partitionDef
	}

	if table.Indexes != nil {
		for i, indexdef := range table.Indexes {
			newTable.Indexes[i] = DeepCopyIndexDef(indexdef)
		}
	}

	for idx, def := range table.Defs {
		switch defImpl := def.Def.(type) {
		case *plan.TableDef_DefType_Properties:
			propDef := &plan.PropertiesDef{
				Properties: make([]*plan.Property, len(defImpl.Properties.Properties)),
			}
			for i, p := range defImpl.Properties.Properties {
				propDef.Properties[i] = &plan.Property{
					Key:   p.Key,
					Value: p.Value,
				}
			}
			newTable.Defs[idx] = &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_Properties{
					Properties: propDef,
				},
			}
		}
	}

	return newTable
}

func DeepCopyColData(col *plan.ColData) *plan.ColData {
	newCol := &plan.ColData{
		Data: make([]*plan.Expr, len(col.Data)),
	}
	for i, e := range col.Data {
		newCol.Data[i] = DeepCopyExpr(e)
	}

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

func DeepCopyPlan(pl *Plan) *Plan {
	switch pl := pl.Plan.(type) {
	case *Plan_Query:
		return &Plan{
			Plan: &plan.Plan_Query{
				Query: DeepCopyQuery(pl.Query),
			},
		}

	case *plan.Plan_Ddl:
		return &Plan{
			Plan: &plan.Plan_Ddl{
				Ddl: DeepCopyDataDefinition(pl.Ddl),
			},
		}

	default:
		// only support query/insert plan now
		return nil
	}
}

func DeepCopyDataDefinition(old *plan.DataDefinition) *plan.DataDefinition {
	newDf := &plan.DataDefinition{
		DdlType: old.DdlType,
	}
	if old.Query != nil {
		newDf.Query = DeepCopyQuery(old.Query)
	}

	switch df := old.Definition.(type) {
	case *plan.DataDefinition_CreateDatabase:
		newDf.Definition = &plan.DataDefinition_CreateDatabase{
			CreateDatabase: &plan.CreateDatabase{
				IfNotExists: df.CreateDatabase.IfNotExists,
				Database:    df.CreateDatabase.Database,
			},
		}

	case *plan.DataDefinition_AlterDatabase:
		newDf.Definition = &plan.DataDefinition_AlterDatabase{
			AlterDatabase: &plan.AlterDatabase{
				IfExists: df.AlterDatabase.IfExists,
				Database: df.AlterDatabase.Database,
			},
		}

	case *plan.DataDefinition_DropDatabase:
		newDf.Definition = &plan.DataDefinition_DropDatabase{
			DropDatabase: &plan.DropDatabase{
				IfExists: df.DropDatabase.IfExists,
				Database: df.DropDatabase.Database,
			},
		}

	case *plan.DataDefinition_CreateTable:
		newDf.Definition = &plan.DataDefinition_CreateTable{
			CreateTable: &plan.CreateTable{
				IfNotExists: df.CreateTable.IfNotExists,
				Temporary:   df.CreateTable.Temporary,
				Database:    df.CreateTable.Database,
				TableDef:    DeepCopyTableDef(df.CreateTable.TableDef),
			},
		}

	case *plan.DataDefinition_AlterTable:
		newDf.Definition = &plan.DataDefinition_AlterTable{
			AlterTable: &plan.AlterTable{
				Table:    df.AlterTable.Table,
				TableDef: DeepCopyTableDef(df.AlterTable.TableDef),
			},
		}

	case *plan.DataDefinition_DropTable:
		newDf.Definition = &plan.DataDefinition_DropTable{
			DropTable: &plan.DropTable{
				IfExists:     df.DropTable.IfExists,
				Database:     df.DropTable.Database,
				Table:        df.DropTable.Table,
				ClusterTable: DeepCopyClusterTable(df.DropTable.GetClusterTable()),
			},
		}

	case *plan.DataDefinition_CreateIndex:
		newDf.Definition = &plan.DataDefinition_CreateIndex{
			CreateIndex: &plan.CreateIndex{
				Database: df.CreateIndex.Database,
				Table:    df.CreateIndex.Table,
				Index: &plan.CreateTable{
					IfNotExists: df.CreateIndex.Index.IfNotExists,
					Temporary:   df.CreateIndex.Index.Temporary,
					Database:    df.CreateIndex.Index.Database,
					TableDef:    DeepCopyTableDef(df.CreateIndex.Index.TableDef),
				},
				OriginTablePrimaryKey: df.CreateIndex.OriginTablePrimaryKey,
			},
		}

	case *plan.DataDefinition_AlterIndex:
		newDf.Definition = &plan.DataDefinition_AlterIndex{
			AlterIndex: &plan.AlterIndex{
				Index: df.AlterIndex.Index,
			},
		}

	case *plan.DataDefinition_DropIndex:
		newDf.Definition = &plan.DataDefinition_DropIndex{
			DropIndex: &plan.DropIndex{
				Database:       df.DropIndex.Database,
				Table:          df.DropIndex.Table,
				IndexName:      df.DropIndex.IndexName,
				IndexTableName: df.DropIndex.IndexTableName,
			},
		}

	case *plan.DataDefinition_TruncateTable:
		truncateTable := &plan.TruncateTable{
			Database:        df.TruncateTable.Database,
			Table:           df.TruncateTable.Table,
			ClusterTable:    DeepCopyClusterTable(df.TruncateTable.GetClusterTable()),
			IndexTableNames: make([]string, len(df.TruncateTable.IndexTableNames)),
		}
		copy(truncateTable.IndexTableNames, df.TruncateTable.IndexTableNames)
		newDf.Definition = &plan.DataDefinition_TruncateTable{
			TruncateTable: truncateTable,
		}

	case *plan.DataDefinition_ShowVariables:
		showVariables := &plan.ShowVariables{
			Global: df.ShowVariables.Global,
			Where:  make([]*plan.Expr, len(df.ShowVariables.Where)),
		}
		for i, e := range df.ShowVariables.Where {
			showVariables.Where[i] = DeepCopyExpr(e)
		}

		newDf.Definition = &plan.DataDefinition_ShowVariables{
			ShowVariables: showVariables,
		}

	case *plan.DataDefinition_LockTables:
		newDf.Definition = &plan.DataDefinition_LockTables{
			LockTables: &plan.LockTables{
				TableLocks: df.LockTables.TableLocks,
			},
		}

	case *plan.DataDefinition_UnlockTables:
		newDf.Definition = &plan.DataDefinition_UnlockTables{
			UnlockTables: &plan.UnLockTables{},
		}

	}

	return newDf
}

func DeepCopyExpr(expr *Expr) *Expr {
	if expr == nil {
		return nil
	}
	newExpr := &Expr{
		Typ: DeepCopyTyp(expr.Typ),
	}

	switch item := expr.Expr.(type) {
	case *plan.Expr_C:
		pc := &plan.Const{
			Isnull: item.C.GetIsnull(),
			Src:    item.C.Src,
		}

		switch c := item.C.Value.(type) {
		case *plan.Const_I8Val:
			pc.Value = &plan.Const_I8Val{I8Val: c.I8Val}
		case *plan.Const_I16Val:
			pc.Value = &plan.Const_I16Val{I16Val: c.I16Val}
		case *plan.Const_I32Val:
			pc.Value = &plan.Const_I32Val{I32Val: c.I32Val}
		case *plan.Const_I64Val:
			pc.Value = &plan.Const_I64Val{I64Val: c.I64Val}
		case *plan.Const_Dval:
			pc.Value = &plan.Const_Dval{Dval: c.Dval}
		case *plan.Const_Sval:
			pc.Value = &plan.Const_Sval{Sval: c.Sval}
		case *plan.Const_Bval:
			pc.Value = &plan.Const_Bval{Bval: c.Bval}
		case *plan.Const_U8Val:
			pc.Value = &plan.Const_U8Val{U8Val: c.U8Val}
		case *plan.Const_U16Val:
			pc.Value = &plan.Const_U16Val{U16Val: c.U16Val}
		case *plan.Const_U32Val:
			pc.Value = &plan.Const_U32Val{U32Val: c.U32Val}
		case *plan.Const_U64Val:
			pc.Value = &plan.Const_U64Val{U64Val: c.U64Val}
		case *plan.Const_Fval:
			pc.Value = &plan.Const_Fval{Fval: c.Fval}
		case *plan.Const_Dateval:
			pc.Value = &plan.Const_Dateval{Dateval: c.Dateval}
		case *plan.Const_Timeval:
			pc.Value = &plan.Const_Timeval{Timeval: c.Timeval}
		case *plan.Const_Datetimeval:
			pc.Value = &plan.Const_Datetimeval{Datetimeval: c.Datetimeval}
		case *plan.Const_Decimal64Val:
			pc.Value = &plan.Const_Decimal64Val{Decimal64Val: &plan.Decimal64{A: c.Decimal64Val.A}}
		case *plan.Const_Decimal128Val:
			pc.Value = &plan.Const_Decimal128Val{Decimal128Val: &plan.Decimal128{A: c.Decimal128Val.A, B: c.Decimal128Val.B}}
		case *plan.Const_Timestampval:
			pc.Value = &plan.Const_Timestampval{Timestampval: c.Timestampval}
		case *plan.Const_Jsonval:
			pc.Value = &plan.Const_Jsonval{Jsonval: c.Jsonval}
		case *plan.Const_Defaultval:
			pc.Value = &plan.Const_Defaultval{Defaultval: c.Defaultval}
		case *plan.Const_UpdateVal:
			pc.Value = &plan.Const_UpdateVal{UpdateVal: c.UpdateVal}
		}

		newExpr.Expr = &plan.Expr_C{
			C: pc,
		}

	case *plan.Expr_P:
		newExpr.Expr = &plan.Expr_P{
			P: &plan.ParamRef{
				Pos: item.P.GetPos(),
			},
		}

	case *plan.Expr_V:
		newExpr.Expr = &plan.Expr_V{
			V: &plan.VarRef{
				Name: item.V.GetName(),
			},
		}

	case *plan.Expr_Col:
		newExpr.Expr = &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: item.Col.GetRelPos(),
				ColPos: item.Col.GetColPos(),
				Name:   item.Col.GetName(),
			},
		}

	case *plan.Expr_F:
		newArgs := make([]*Expr, len(item.F.Args))
		for idx, arg := range item.F.Args {
			newArgs[idx] = DeepCopyExpr(arg)
		}
		newExpr.Expr = &plan.Expr_F{
			F: &plan.Function{
				Func: DeepCopyObjectRef(item.F.Func),
				Args: newArgs,
			},
		}

	case *plan.Expr_Sub:
		newExpr.Expr = &plan.Expr_Sub{
			Sub: &plan.SubqueryRef{
				NodeId: item.Sub.GetNodeId(),
			},
		}

	case *plan.Expr_Corr:
		newExpr.Expr = &plan.Expr_Corr{
			Corr: &plan.CorrColRef{
				ColPos: item.Corr.GetColPos(),
				RelPos: item.Corr.GetRelPos(),
				Depth:  item.Corr.GetDepth(),
			},
		}

	case *plan.Expr_T:
		newExpr.Expr = &plan.Expr_T{
			T: &plan.TargetType{
				Typ: DeepCopyTyp(item.T.Typ),
			},
		}

	case *plan.Expr_Max:
		newExpr.Expr = &plan.Expr_Max{
			Max: &plan.MaxValue{
				Value: item.Max.GetValue(),
			},
		}

	case *plan.Expr_List:
		e := &plan.ExprList{
			List: make([]*plan.Expr, len(item.List.List)),
		}
		for i, ie := range item.List.List {
			e.List[i] = DeepCopyExpr(ie)
		}
		newExpr.Expr = &plan.Expr_List{
			List: e,
		}
	}

	return newExpr
}

func DeepCopyClusterTable(cluster *plan.ClusterTable) *plan.ClusterTable {
	if cluster == nil {
		return nil
	}

	accountIds := make([]uint32, len(cluster.GetAccountIDs()))
	copy(accountIds, cluster.GetAccountIDs())
	newClusterTable := &plan.ClusterTable{
		IsClusterTable:         cluster.GetIsClusterTable(),
		AccountIDs:             accountIds,
		ColumnIndexOfAccountId: cluster.GetColumnIndexOfAccountId(),
	}
	return newClusterTable
}

func DeepCopyAnalyzeInfo(analyzeinfo *plan.AnalyzeInfo) *plan.AnalyzeInfo {
	if analyzeinfo == nil {
		return nil
	}

	return &plan.AnalyzeInfo{
		InputRows:        analyzeinfo.GetInputRows(),
		OutputRows:       analyzeinfo.GetOutputRows(),
		InputSize:        analyzeinfo.GetInputSize(),
		OutputSize:       analyzeinfo.GetOutputSize(),
		TimeConsumed:     analyzeinfo.GetTimeConsumed(),
		MemorySize:       analyzeinfo.GetMemorySize(),
		WaitTimeConsumed: analyzeinfo.GetWaitTimeConsumed(),
		DiskIO:           analyzeinfo.GetDiskIO(),
		S3IOByte:         analyzeinfo.GetS3IOByte(),
		S3IOCount:        analyzeinfo.GetS3IOCount(),
		NetworkIO:        analyzeinfo.GetNetworkIO(),
		ScanTime:         analyzeinfo.GetScanTime(),
		InsertTime:       analyzeinfo.GetInsertTime(),
	}
}
