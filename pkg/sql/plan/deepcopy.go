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
		TblFuncExprList: make([]*plan.Expr, len(node.TblFuncExprList)),
		ClusterTable:    DeepCopyClusterTable(node.GetClusterTable()),
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

	for idx, deleteTablesCtx := range node.DeleteTablesCtx {
		newNode.DeleteTablesCtx[idx] = &plan.DeleteTableCtx{
			DbName:       deleteTablesCtx.DbName,
			TblName:      deleteTablesCtx.TblName,
			UseDeleteKey: deleteTablesCtx.UseDeleteKey,
			CanTruncate:  deleteTablesCtx.CanTruncate,
			ColIndex:     deleteTablesCtx.ColIndex,
		}
	}

	for i, updateCtx := range node.UpdateCtxs {
		newNode.UpdateCtxs[i] = &plan.UpdateCtx{
			DbName:        updateCtx.DbName,
			TblName:       updateCtx.TblName,
			HideKey:       updateCtx.HideKey,
			HideKeyIdx:    updateCtx.HideKeyIdx,
			UpdateCols:    make([]*ColDef, len(updateCtx.UpdateCols)),
			OtherAttrs:    make([]string, len(updateCtx.OtherAttrs)),
			OrderAttrs:    make([]string, len(updateCtx.OrderAttrs)),
			CompositePkey: DeepCopyColDef(updateCtx.GetCompositePkey()),
		}
		for j, col := range updateCtx.UpdateCols {
			newNode.UpdateCtxs[i].UpdateCols[j] = DeepCopyColDef(col)
		}
		copy(newNode.UpdateCtxs[i].OtherAttrs, updateCtx.OtherAttrs)
		copy(newNode.UpdateCtxs[i].OrderAttrs, updateCtx.OrderAttrs)
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
		node.TblFuncExprList[idx] = DeepCopyExpr(expr)
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
		Precision:   typ.Precision,
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
	}

	for idx, col := range table.Cols {
		newTable.Cols[idx] = DeepCopyColDef(col)
	}
	if table.TblFunc != nil {
		newTable.TblFunc = &plan.TableFunction{
			Name:  table.TblFunc.Name,
			Param: make([]byte, len(table.TblFunc.Param)),
		}
		copy(newTable.TblFunc.Param, table.TblFunc.Param)
	}

	if table.CompositePkey != nil {
		newTable.CompositePkey = DeepCopyColDef(table.CompositePkey)
	}

	if table.ViewSql != nil {
		newTable.ViewSql = &plan.ViewDef{
			View: table.ViewSql.View,
		}
	}

	for idx, def := range table.Defs {
		switch defImpl := def.Def.(type) {
		case *plan.TableDef_DefType_Pk:
			pkDef := &plan.PrimaryKeyDef{
				Names: make([]string, len(defImpl.Pk.Names)),
			}
			copy(pkDef.Names, defImpl.Pk.Names)
			newTable.Defs[idx] = &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_Pk{
					Pk: pkDef,
				},
			}
		case *plan.TableDef_DefType_UIdx:
			indexDef := &plan.UniqueIndexDef{
				Fields: make([]*plan.Field, len(defImpl.UIdx.Fields)),
			}
			copy(indexDef.IndexNames, defImpl.UIdx.IndexNames)
			copy(indexDef.TableNames, defImpl.UIdx.TableNames)
			copy(indexDef.TableExists, defImpl.UIdx.TableExists)
			for idx, oldField := range defImpl.UIdx.Fields {
				newField := &plan.Field{
					Parts: make([]string, len(oldField.Parts)),
					Cols:  make([]*plan.ColDef, len(oldField.Cols)),
				}
				copy(newField.Parts, oldField.Parts)
				for i, col := range oldField.Cols {
					newField.Cols[i] = DeepCopyColDef(col)
				}
				indexDef.Fields[idx] = newField
			}
			newTable.Defs[idx] = &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_UIdx{
					UIdx: indexDef,
				},
			}
		case *plan.TableDef_DefType_SIdx:
			indexDef := &plan.SecondaryIndexDef{
				Fields: make([]*plan.Field, len(defImpl.SIdx.Fields)),
			}
			copy(indexDef.IndexNames, defImpl.SIdx.IndexNames)
			copy(indexDef.TableNames, defImpl.SIdx.TableNames)
			copy(indexDef.TableExists, defImpl.SIdx.TableExists)
			for i := range indexDef.Fields {
				copy(indexDef.Fields[i].Parts, defImpl.SIdx.Fields[i].Parts)
				copy(indexDef.Fields[i].Cols, defImpl.SIdx.Fields[i].Cols)
			}
			newTable.Defs[idx] = &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_SIdx{
					SIdx: indexDef,
				},
			}
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
		case *TableDef_DefType_Partition:
			partitionDef := &plan.PartitionInfo{
				Type:                defImpl.Partition.GetType(),
				Expr:                DeepCopyExpr(defImpl.Partition.Expr),
				PartitionExpression: defImpl.Partition.GetPartitionExpression(),
				Columns:             make([]*plan.Expr, len(defImpl.Partition.Columns)),
				PartitionColumns:    make([]string, len(defImpl.Partition.PartitionColumns)),
				PartitionNum:        defImpl.Partition.GetPartitionNum(),
				Partitions:          make([]*plan.PartitionItem, len(defImpl.Partition.Partitions)),
				Algorithm:           defImpl.Partition.GetAlgorithm(),
				IsSubPartition:      defImpl.Partition.GetIsSubPartition(),
				PartitionMsg:        defImpl.Partition.GetPartitionMsg(),
			}
			for i, e := range defImpl.Partition.Columns {
				partitionDef.Columns[i] = DeepCopyExpr(e)
			}
			copy(partitionDef.PartitionColumns, defImpl.Partition.PartitionColumns)
			for i, e := range defImpl.Partition.Partitions {
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
			newTable.Defs[idx] = &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_Partition{
					Partition: partitionDef,
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

func DeepCopyInsertValues(insert *plan.InsertValues) *plan.InsertValues {
	newInsert := &plan.InsertValues{
		DbName:        insert.DbName,
		TblName:       insert.TblName,
		ExplicitCols:  make([]*plan.ColDef, len(insert.ExplicitCols)),
		OtherCols:     make([]*plan.ColDef, len(insert.OtherCols)),
		Columns:       make([]*plan.Column, len(insert.Columns)),
		OrderAttrs:    make([]string, len(insert.OrderAttrs)),
		CompositePkey: DeepCopyColDef(insert.CompositePkey),
		ClusterTable:  DeepCopyClusterTable(insert.GetClusterTable()),
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
				Database:  df.DropIndex.Database,
				Table:     df.DropIndex.Table,
				IndexName: df.DropIndex.IndexName,
			},
		}

	case *plan.DataDefinition_TruncateTable:
		newDf.Definition = &plan.DataDefinition_TruncateTable{
			TruncateTable: &plan.TruncateTable{
				Table:        df.TruncateTable.Table,
				ClusterTable: DeepCopyClusterTable(df.TruncateTable.GetClusterTable()),
			},
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
				Func: &plan.ObjectRef{
					Server:     item.F.Func.GetServer(),
					Db:         item.F.Func.GetDb(),
					Schema:     item.F.Func.GetSchema(),
					Obj:        item.F.Func.GetObj(),
					ServerName: item.F.Func.GetServerName(),
					DbName:     item.F.Func.GetDbName(),
					SchemaName: item.F.Func.GetSchemaName(),
					ObjName:    item.F.Func.GetObjName(),
				},
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
