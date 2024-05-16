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

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"golang.org/x/exp/constraints"
)

func DeepCopyExprList(list []*Expr) []*Expr {
	if list == nil {
		return nil
	}
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
		PubInfo:    ref.PubInfo,
	}
}

func DeepCopyOnDupliateKeyCtx(ctx *plan.OnDuplicateKeyCtx) *plan.OnDuplicateKeyCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &plan.OnDuplicateKeyCtx{
		OnDuplicateIdx: make([]int32, len(ctx.OnDuplicateIdx)),
	}

	copy(newCtx.OnDuplicateIdx, ctx.OnDuplicateIdx)

	if ctx.OnDuplicateExpr != nil {
		newCtx.OnDuplicateExpr = make(map[string]*Expr)
		for k, v := range ctx.OnDuplicateExpr {
			newCtx.OnDuplicateExpr[k] = DeepCopyExpr(v)
		}
	}

	return newCtx
}

func DeepCopyInsertCtx(ctx *plan.InsertCtx) *plan.InsertCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &plan.InsertCtx{
		Ref:                 DeepCopyObjectRef(ctx.Ref),
		AddAffectedRows:     ctx.AddAffectedRows,
		IsClusterTable:      ctx.IsClusterTable,
		TableDef:            DeepCopyTableDef(ctx.TableDef, true),
		PartitionTableIds:   make([]uint64, len(ctx.PartitionTableIds)),
		PartitionTableNames: make([]string, len(ctx.PartitionTableNames)),
		PartitionIdx:        ctx.PartitionIdx,
	}
	copy(newCtx.PartitionTableIds, ctx.PartitionTableIds)
	copy(newCtx.PartitionTableNames, ctx.PartitionTableNames)
	return newCtx
}

func DeepCopyDeleteCtx(ctx *plan.DeleteCtx) *plan.DeleteCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &plan.DeleteCtx{
		CanTruncate:         ctx.CanTruncate,
		AddAffectedRows:     ctx.AddAffectedRows,
		RowIdIdx:            ctx.RowIdIdx,
		Ref:                 DeepCopyObjectRef(ctx.Ref),
		IsClusterTable:      ctx.IsClusterTable,
		TableDef:            DeepCopyTableDef(ctx.TableDef, true),
		PartitionTableIds:   make([]uint64, len(ctx.PartitionTableIds)),
		PartitionTableNames: make([]string, len(ctx.PartitionTableNames)),
		PartitionIdx:        ctx.PartitionIdx,
		PrimaryKeyIdx:       ctx.PrimaryKeyIdx,
	}
	copy(newCtx.PartitionTableIds, ctx.PartitionTableIds)
	copy(newCtx.PartitionTableNames, ctx.PartitionTableNames)
	return newCtx
}

func DeepCopyPreInsertCtx(ctx *plan.PreInsertCtx) *plan.PreInsertCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &plan.PreInsertCtx{
		Ref:        DeepCopyObjectRef(ctx.Ref),
		TableDef:   DeepCopyTableDef(ctx.TableDef, true),
		HasAutoCol: ctx.HasAutoCol,
		IsUpdate:   ctx.IsUpdate,
	}

	return newCtx
}

func DeepCopyPreInsertUkCtx(ctx *plan.PreInsertUkCtx) *plan.PreInsertUkCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &plan.PreInsertUkCtx{
		Columns:  make([]int32, len(ctx.Columns)),
		PkColumn: ctx.PkColumn,
		PkType:   ctx.PkType,
		UkType:   ctx.UkType,
	}
	copy(newCtx.Columns, ctx.Columns)

	return newCtx
}

func DeepCopyPreDeleteCtx(ctx *plan.PreDeleteCtx) *plan.PreDeleteCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &plan.PreDeleteCtx{
		Idx: make([]int32, len(ctx.Idx)),
	}
	copy(newCtx.Idx, ctx.Idx)

	return newCtx
}

func DeepCopyLockTarget(target *plan.LockTarget) *plan.LockTarget {
	if target == nil {
		return nil
	}
	return &plan.LockTarget{
		TableId:            target.TableId,
		PrimaryColIdxInBat: target.PrimaryColIdxInBat,
		PrimaryColTyp:      target.PrimaryColTyp,
		RefreshTsIdxInBat:  target.RefreshTsIdxInBat,
		FilterColIdxInBat:  target.FilterColIdxInBat,
		LockTable:          target.LockTable,
		Block:              target.Block,
	}
}

func DeepCopyNode(node *plan.Node) *plan.Node {
	newNode := &Node{
		NodeType:        node.NodeType,
		NodeId:          node.NodeId,
		ExtraOptions:    node.ExtraOptions,
		Children:        make([]int32, len(node.Children)),
		JoinType:        node.JoinType,
		BuildOnLeft:     node.BuildOnLeft,
		BindingTags:     make([]int32, len(node.BindingTags)),
		Limit:           DeepCopyExpr(node.Limit),
		Offset:          DeepCopyExpr(node.Offset),
		ProjectList:     make([]*plan.Expr, len(node.ProjectList)),
		OnList:          make([]*plan.Expr, len(node.OnList)),
		FilterList:      make([]*plan.Expr, len(node.FilterList)),
		BlockFilterList: make([]*plan.Expr, len(node.BlockFilterList)),
		GroupBy:         make([]*plan.Expr, len(node.GroupBy)),
		GroupingSet:     make([]*plan.Expr, len(node.GroupingSet)),
		AggList:         make([]*plan.Expr, len(node.AggList)),
		OrderBy:         make([]*plan.OrderBySpec, len(node.OrderBy)),
		DeleteCtx:       DeepCopyDeleteCtx(node.DeleteCtx),
		TblFuncExprList: make([]*plan.Expr, len(node.TblFuncExprList)),
		ClusterTable:    DeepCopyClusterTable(node.GetClusterTable()),
		InsertCtx:       DeepCopyInsertCtx(node.InsertCtx),
		NotCacheable:    node.NotCacheable,
		SourceStep:      node.SourceStep,
		PreInsertCtx:    DeepCopyPreInsertCtx(node.PreInsertCtx),
		PreInsertUkCtx:  DeepCopyPreInsertUkCtx(node.PreInsertUkCtx),
		PreDeleteCtx:    DeepCopyPreDeleteCtx(node.PreDeleteCtx),
		OnDuplicateKey:  DeepCopyOnDupliateKeyCtx(node.OnDuplicateKey),
		LockTargets:     make([]*plan.LockTarget, len(node.LockTargets)),
		AnalyzeInfo:     DeepCopyAnalyzeInfo(node.AnalyzeInfo),
		IsEnd:           node.IsEnd,
		ExternScan:      node.ExternScan,
		PartitionPrune:  DeepCopyPartitionPrune(node.PartitionPrune),
		SampleFunc:      DeepCopySampleFuncSpec(node.SampleFunc),
		OnUpdateExprs:   make([]*plan.Expr, len(node.OnUpdateExprs)),
	}
	newNode.Uuid = append(newNode.Uuid, node.Uuid...)

	copy(newNode.Children, node.Children)
	copy(newNode.BindingTags, node.BindingTags)

	for idx, target := range node.LockTargets {
		newNode.LockTargets[idx] = DeepCopyLockTarget(target)
	}

	for idx, expr := range node.ProjectList {
		newNode.ProjectList[idx] = DeepCopyExpr(expr)
	}

	for idx, expr := range node.OnList {
		newNode.OnList[idx] = DeepCopyExpr(expr)
	}

	for idx, expr := range node.FilterList {
		newNode.FilterList[idx] = DeepCopyExpr(expr)
	}

	for idx, expr := range node.BlockFilterList {
		newNode.BlockFilterList[idx] = DeepCopyExpr(expr)
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

	for idx, expr := range node.OnUpdateExprs {
		newNode.OnUpdateExprs[idx] = DeepCopyExpr(expr)
	}

	newNode.Stats = DeepCopyStats(node.Stats)

	newNode.ObjRef = DeepCopyObjectRef(node.ObjRef)
	newNode.ParentObjRef = DeepCopyObjectRef(node.ParentObjRef)

	if node.WinSpecList != nil {
		newNode.WinSpecList = make([]*Expr, len(node.WinSpecList))
		for i, w := range node.WinSpecList {
			newNode.WinSpecList[i] = DeepCopyExpr(w)
		}
	}

	if node.TableDef != nil {
		newNode.TableDef = DeepCopyTableDef(node.TableDef, true)
	}

	if node.RowsetData != nil {
		newNode.RowsetData = &plan.RowsetData{
			Cols:     make([]*plan.ColData, len(node.RowsetData.Cols)),
			RowCount: node.RowsetData.RowCount,
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

func DeepCopyType(typ *plan.Type) *plan.Type {
	if typ == nil {
		return nil
	}
	return &plan.Type{
		Id:          typ.Id,
		NotNullable: typ.NotNullable,
		Width:       typ.Width,
		Scale:       typ.Scale,
		AutoIncr:    typ.AutoIncr,
		Enumvalues:  typ.Enumvalues,
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
		Typ:       col.Typ,
		Default:   DeepCopyDefault(col.Default),
		Primary:   col.Primary,
		Pkidx:     col.Pkidx,
		Comment:   col.Comment,
		OnUpdate:  DeepCopyOnUpdate(col.OnUpdate),
		ClusterBy: col.ClusterBy,
		Hidden:    col.Hidden,
		Seqnum:    col.Seqnum,
		TblName:   col.TblName,
		DbName:    col.DbName,
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
	// Check whether the composite primary key column is included
	if pkeyDef.CompPkeyCol != nil {
		def.CompPkeyCol = DeepCopyColDef(pkeyDef.CompPkeyCol)
	}
	return def
}

func DeepCopyIndexDef(indexDef *plan.IndexDef) *plan.IndexDef {
	if indexDef == nil {
		return nil
	}
	newindexDef := &plan.IndexDef{
		IdxId:              indexDef.IdxId,
		IndexName:          indexDef.IndexName,
		Unique:             indexDef.Unique,
		TableExist:         indexDef.TableExist,
		IndexTableName:     indexDef.IndexTableName,
		Comment:            indexDef.Comment,
		Visible:            indexDef.Visible,
		IndexAlgo:          indexDef.IndexAlgo,
		IndexAlgoTableType: indexDef.IndexAlgoTableType,
		IndexAlgoParams:    indexDef.IndexAlgoParams,
	}
	newindexDef.Option = DeepCopyIndexOption(indexDef.Option)

	newParts := make([]string, len(indexDef.Parts))
	copy(newParts, indexDef.Parts)
	newindexDef.Parts = newParts
	return newindexDef
}

func DeepCopyIndexOption(indexOption *plan.IndexOption) *plan.IndexOption {
	if indexOption == nil {
		return nil
	}
	newIndexOption := &plan.IndexOption{
		CreateExtraTable: indexOption.CreateExtraTable,
	}

	return newIndexOption
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

func DeepCopyTableDefList(src []*plan.TableDef) []*plan.TableDef {
	if src == nil {
		return nil
	}
	ret := make([]*plan.TableDef, len(src))
	for i, def := range src {
		ret[i] = DeepCopyTableDef(def, true)
	}
	return ret
}

func DeepCopyPartitionPrune(partitionPrune *plan.PartitionPrune) *plan.PartitionPrune {
	if partitionPrune == nil {
		return nil
	}
	newPartitionPrune := &plan.PartitionPrune{
		IsPruned:           partitionPrune.IsPruned,
		SelectedPartitions: make([]*plan.PartitionItem, len(partitionPrune.SelectedPartitions)),
	}
	for i, e := range partitionPrune.SelectedPartitions {
		newPartitionPrune.SelectedPartitions[i] = &plan.PartitionItem{
			PartitionName:      e.PartitionName,
			OrdinalPosition:    e.OrdinalPosition,
			Description:        e.Description,
			Comment:            e.Comment,
			LessThan:           DeepCopyExprList(e.LessThan),
			InValues:           DeepCopyExprList(e.InValues),
			PartitionTableName: e.PartitionTableName,
		}
	}
	return newPartitionPrune
}

func DeepCopySampleFuncSpec(source *plan.SampleFuncSpec) *plan.SampleFuncSpec {
	if source == nil {
		return nil
	}
	return &plan.SampleFuncSpec{
		Rows:    source.Rows,
		Percent: source.Percent,
	}
}

func DeepCopyTableDef(table *plan.TableDef, withCols bool) *plan.TableDef {
	if table == nil {
		return nil
	}
	newTable := &plan.TableDef{
		TblId:          table.TblId,
		Name:           table.Name,
		Hidden:         table.Hidden,
		TableType:      table.TableType,
		Createsql:      table.Createsql,
		Version:        table.Version,
		Pkey:           DeepCopyPrimaryKeyDef(table.Pkey),
		Indexes:        make([]*IndexDef, len(table.Indexes)),
		Fkeys:          make([]*plan.ForeignKeyDef, len(table.Fkeys)),
		RefChildTbls:   make([]uint64, len(table.RefChildTbls)),
		Checks:         make([]*plan.CheckDef, len(table.Checks)),
		Props:          make([]*plan.PropertyDef, len(table.Props)),
		Defs:           make([]*plan.TableDef_DefType, len(table.Defs)),
		Name2ColIndex:  table.Name2ColIndex,
		IsLocked:       table.IsLocked,
		TableLockType:  table.TableLockType,
		IsTemporary:    table.IsTemporary,
		AutoIncrOffset: table.AutoIncrOffset,
		DbName:         table.DbName,
	}

	copy(newTable.RefChildTbls, table.RefChildTbls)

	if withCols {
		newTable.Cols = make([]*plan.ColDef, len(table.Cols))
		for idx, col := range table.Cols {
			newTable.Cols[idx] = DeepCopyColDef(col)
		}
	}

	for idx, fkey := range table.Fkeys {
		newTable.Fkeys[idx] = DeepCopyFkey(fkey)
	}

	for idx, col := range table.Checks {
		newTable.Checks[idx] = &plan.CheckDef{
			Name:  col.Name,
			Check: DeepCopyExpr(col.Check),
		}
	}

	for idx, prop := range table.Props {
		newTable.Props[idx] = &plan.PropertyDef{
			Key:   prop.Key,
			Value: prop.Value,
		}
	}

	if table.TblFunc != nil {
		newTable.TblFunc = &plan.TableFunction{
			Name:  table.TblFunc.Name,
			Param: make([]byte, len(table.TblFunc.Param)),
		}
		copy(newTable.TblFunc.Param, table.TblFunc.Param)
	}

	if table.ClusterBy != nil {
		newTable.ClusterBy = &plan.ClusterByDef{
			//Parts: make([]*plan.Expr, len(table.ClusterBy.Parts)),
			Name:         table.ClusterBy.Name,
			CompCbkeyCol: DeepCopyColDef(table.ClusterBy.CompCbkeyCol),
		}
		//for i, part := range table.ClusterBy.Parts {
		//	newTable.ClusterBy.Parts[i] = DeepCopyExpr(part)
		//}
	}

	if table.ViewSql != nil {
		newTable.ViewSql = &plan.ViewDef{
			View: table.ViewSql.View,
		}
	}

	if table.Partition != nil {
		newTable.Partition = DeepCopyPartitionByDef(table.Partition)
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
		Data: make([]*plan.RowsetExpr, len(col.Data)),
	}
	for i, e := range col.Data {
		newCol.Data[i] = &plan.RowsetExpr{
			Pos:    e.Pos,
			RowPos: e.RowPos,
			Expr:   DeepCopyExpr(e.Expr),
		}
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
	switch p := pl.Plan.(type) {
	case *Plan_Query:
		return &Plan{
			Plan: &plan.Plan_Query{
				Query: DeepCopyQuery(p.Query),
			},
			IsPrepare:   pl.IsPrepare,
			TryRunTimes: pl.TryRunTimes,
		}

	case *plan.Plan_Ddl:
		return &Plan{
			Plan: &plan.Plan_Ddl{
				Ddl: DeepCopyDataDefinition(p.Ddl),
			},
			IsPrepare:   pl.IsPrepare,
			TryRunTimes: pl.TryRunTimes,
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
				IfExists:   df.DropDatabase.IfExists,
				Database:   df.DropDatabase.Database,
				DatabaseId: df.DropDatabase.DatabaseId,
			},
		}

	case *plan.DataDefinition_CreateTable:
		CreateTable := &plan.CreateTable{
			Replace:         df.CreateTable.Replace,
			IfNotExists:     df.CreateTable.IfNotExists,
			Temporary:       df.CreateTable.Temporary,
			Database:        df.CreateTable.Database,
			TableDef:        DeepCopyTableDef(df.CreateTable.TableDef, true),
			IndexTables:     DeepCopyTableDefList(df.CreateTable.GetIndexTables()),
			FkDbs:           make([]string, len(df.CreateTable.FkDbs)),
			FkTables:        make([]string, len(df.CreateTable.FkTables)),
			FkCols:          make([]*plan.FkColName, len(df.CreateTable.FkCols)),
			PartitionTables: DeepCopyTableDefList(df.CreateTable.GetPartitionTables()),
		}
		copy(CreateTable.FkDbs, df.CreateTable.FkDbs)
		copy(CreateTable.FkTables, df.CreateTable.FkTables)
		for i, val := range df.CreateTable.FkCols {
			cols := &plan.FkColName{Cols: make([]string, len(val.Cols))}
			copy(cols.Cols, val.Cols)
			CreateTable.FkCols[i] = cols
		}
		newDf.Definition = &plan.DataDefinition_CreateTable{
			CreateTable: CreateTable,
		}

	case *plan.DataDefinition_AlterTable:
		AlterTable := &plan.AlterTable{
			Database:       df.AlterTable.Database,
			TableDef:       DeepCopyTableDef(df.AlterTable.TableDef, true),
			CopyTableDef:   DeepCopyTableDef(df.AlterTable.CopyTableDef, true),
			IsClusterTable: df.AlterTable.IsClusterTable,
			AlgorithmType:  df.AlterTable.AlgorithmType,
			CreateTableSql: df.AlterTable.CreateTableSql,
			InsertDataSql:  df.AlterTable.InsertDataSql,
			Actions:        make([]*plan.AlterTable_Action, len(df.AlterTable.Actions)),
		}
		for i, action := range df.AlterTable.Actions {
			switch act := action.Action.(type) {
			case *plan.AlterTable_Action_Drop:
				AlterTable.Actions[i] = &plan.AlterTable_Action{
					Action: &plan.AlterTable_Action_Drop{
						Drop: &plan.AlterTableDrop{
							Typ:  act.Drop.Typ,
							Name: act.Drop.Name,
						},
					},
				}
			case *plan.AlterTable_Action_AddFk:
				AddFk := &plan.AlterTable_Action_AddFk{
					AddFk: &plan.AlterTableAddFk{
						DbName:    act.AddFk.DbName,
						TableName: act.AddFk.TableName,
						Cols:      make([]string, len(act.AddFk.Cols)),
						Fkey:      DeepCopyFkey(act.AddFk.Fkey),
					},
				}
				copy(AddFk.AddFk.Cols, act.AddFk.Cols)
				AlterTable.Actions[i] = &plan.AlterTable_Action{
					Action: AddFk,
				}
			}
		}

		newDf.Definition = &plan.DataDefinition_AlterTable{
			AlterTable: AlterTable,
		}

	case *plan.DataDefinition_DropTable:
		newDf.Definition = &plan.DataDefinition_DropTable{
			DropTable: &plan.DropTable{
				IfExists:            df.DropTable.IfExists,
				Database:            df.DropTable.Database,
				Table:               df.DropTable.Table,
				IndexTableNames:     DeepCopyStringList(df.DropTable.GetIndexTableNames()),
				ClusterTable:        DeepCopyClusterTable(df.DropTable.GetClusterTable()),
				TableId:             df.DropTable.GetTableId(),
				ForeignTbl:          DeepCopyNumberList(df.DropTable.GetForeignTbl()),
				PartitionTableNames: DeepCopyStringList(df.DropTable.GetPartitionTableNames()),
				IsView:              df.DropTable.IsView,
				TableDef:            DeepCopyTableDef(df.DropTable.GetTableDef(), true),
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
					TableDef:    DeepCopyTableDef(df.CreateIndex.Index.TableDef, true),
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

	case *plan.DataDefinition_AlterSequence:
		newDf.Definition = &plan.DataDefinition_AlterSequence{
			AlterSequence: &plan.AlterSequence{
				IfExists: df.AlterSequence.IfExists,
				Database: df.AlterSequence.Database,
				TableDef: df.AlterSequence.TableDef,
			},
		}

	}

	return newDf
}

func DeepCopyFkey(fkey *ForeignKeyDef) *ForeignKeyDef {
	def := &ForeignKeyDef{
		Name:        fkey.Name,
		Cols:        make([]uint64, len(fkey.Cols)),
		ForeignTbl:  fkey.ForeignTbl,
		ForeignCols: make([]uint64, len(fkey.ForeignCols)),
		OnDelete:    fkey.OnDelete,
		OnUpdate:    fkey.OnUpdate,
	}
	copy(def.Cols, fkey.Cols)
	copy(def.ForeignCols, fkey.ForeignCols)
	return def
}

func DeepCopyExpr(expr *Expr) *Expr {
	if expr == nil {
		return nil
	}
	newExpr := &Expr{
		Typ:         expr.Typ,
		Ndv:         expr.Ndv,
		Selectivity: expr.Selectivity,
	}

	switch item := expr.Expr.(type) {
	case *plan.Expr_Lit:
		pc := &plan.Literal{
			Isnull: item.Lit.GetIsnull(),
			Src:    item.Lit.Src,
		}

		switch c := item.Lit.Value.(type) {
		case *plan.Literal_I8Val:
			pc.Value = &plan.Literal_I8Val{I8Val: c.I8Val}
		case *plan.Literal_I16Val:
			pc.Value = &plan.Literal_I16Val{I16Val: c.I16Val}
		case *plan.Literal_I32Val:
			pc.Value = &plan.Literal_I32Val{I32Val: c.I32Val}
		case *plan.Literal_I64Val:
			pc.Value = &plan.Literal_I64Val{I64Val: c.I64Val}
		case *plan.Literal_Dval:
			pc.Value = &plan.Literal_Dval{Dval: c.Dval}
		case *plan.Literal_Sval:
			pc.Value = &plan.Literal_Sval{Sval: c.Sval}
		case *plan.Literal_Bval:
			pc.Value = &plan.Literal_Bval{Bval: c.Bval}
		case *plan.Literal_U8Val:
			pc.Value = &plan.Literal_U8Val{U8Val: c.U8Val}
		case *plan.Literal_U16Val:
			pc.Value = &plan.Literal_U16Val{U16Val: c.U16Val}
		case *plan.Literal_U32Val:
			pc.Value = &plan.Literal_U32Val{U32Val: c.U32Val}
		case *plan.Literal_U64Val:
			pc.Value = &plan.Literal_U64Val{U64Val: c.U64Val}
		case *plan.Literal_Fval:
			pc.Value = &plan.Literal_Fval{Fval: c.Fval}
		case *plan.Literal_Dateval:
			pc.Value = &plan.Literal_Dateval{Dateval: c.Dateval}
		case *plan.Literal_Timeval:
			pc.Value = &plan.Literal_Timeval{Timeval: c.Timeval}
		case *plan.Literal_Datetimeval:
			pc.Value = &plan.Literal_Datetimeval{Datetimeval: c.Datetimeval}
		case *plan.Literal_Decimal64Val:
			pc.Value = &plan.Literal_Decimal64Val{Decimal64Val: &plan.Decimal64{A: c.Decimal64Val.A}}
		case *plan.Literal_Decimal128Val:
			pc.Value = &plan.Literal_Decimal128Val{Decimal128Val: &plan.Decimal128{A: c.Decimal128Val.A, B: c.Decimal128Val.B}}
		case *plan.Literal_Timestampval:
			pc.Value = &plan.Literal_Timestampval{Timestampval: c.Timestampval}
		case *plan.Literal_Jsonval:
			pc.Value = &plan.Literal_Jsonval{Jsonval: c.Jsonval}
		case *plan.Literal_Defaultval:
			pc.Value = &plan.Literal_Defaultval{Defaultval: c.Defaultval}
		case *plan.Literal_UpdateVal:
			pc.Value = &plan.Literal_UpdateVal{UpdateVal: c.UpdateVal}
		case *plan.Literal_EnumVal:
			pc.Value = &plan.Literal_EnumVal{EnumVal: c.EnumVal}
		}

		newExpr.Expr = &plan.Expr_Lit{
			Lit: pc,
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
				Name:   item.V.GetName(),
				Global: item.V.GetGlobal(),
				System: item.V.GetSystem(),
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

	case *plan.Expr_W:
		ps := make([]*Expr, len(item.W.PartitionBy))
		for i, p := range item.W.PartitionBy {
			ps[i] = DeepCopyExpr(p)
		}
		os := make([]*OrderBySpec, len(item.W.OrderBy))
		for i, o := range item.W.OrderBy {
			os[i] = DeepCopyOrderBy(o)
		}
		f := item.W.Frame
		newExpr.Expr = &plan.Expr_W{
			W: &plan.WindowSpec{
				WindowFunc:  DeepCopyExpr(item.W.WindowFunc),
				PartitionBy: ps,
				OrderBy:     os,
				Name:        item.W.Name,
				Frame: &plan.FrameClause{
					Type: f.Type,
					Start: &plan.FrameBound{
						Type:      f.Start.Type,
						UnBounded: f.Start.UnBounded,
						Val:       DeepCopyExpr(f.Start.Val),
					},
					End: &plan.FrameBound{
						Type:      f.End.Type,
						UnBounded: f.End.UnBounded,
						Val:       DeepCopyExpr(f.End.Val),
					},
				},
			},
		}

	case *plan.Expr_Sub:
		newExpr.Expr = &plan.Expr_Sub{
			Sub: &plan.SubqueryRef{
				NodeId:  item.Sub.GetNodeId(),
				Typ:     item.Sub.Typ,
				Op:      item.Sub.Op,
				RowSize: item.Sub.RowSize,
				Child:   DeepCopyExpr(item.Sub.Child),
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
			T: &plan.TargetType{},
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

	case *plan.Expr_Vec:
		newExpr.Expr = &plan.Expr_Vec{
			Vec: &plan.LiteralVec{
				Len:  item.Vec.Len,
				Data: bytes.Clone(item.Vec.Data),
			},
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

func DeepCopySliceInt64(s []int64) []int64 {
	if s == nil {
		return nil
	}
	result := make([]int64, 0, len(s))
	result = append(result, s...)
	return result
}

func DeepCopyAnalyzeInfo(analyzeinfo *plan.AnalyzeInfo) *plan.AnalyzeInfo {
	if analyzeinfo == nil {
		return nil
	}

	return &plan.AnalyzeInfo{
		InputRows:              analyzeinfo.GetInputRows(),
		OutputRows:             analyzeinfo.GetOutputRows(),
		InputSize:              analyzeinfo.GetInputSize(),
		OutputSize:             analyzeinfo.GetOutputSize(),
		TimeConsumed:           analyzeinfo.GetTimeConsumed(),
		TimeConsumedArrayMajor: DeepCopySliceInt64(analyzeinfo.GetTimeConsumedArrayMajor()),
		TimeConsumedArrayMinor: DeepCopySliceInt64(analyzeinfo.GetTimeConsumedArrayMinor()),
		MemorySize:             analyzeinfo.GetMemorySize(),
		WaitTimeConsumed:       analyzeinfo.GetWaitTimeConsumed(),
		DiskIO:                 analyzeinfo.GetDiskIO(),
		S3IOByte:               analyzeinfo.GetS3IOByte(),
		S3IOInputCount:         analyzeinfo.GetS3IOInputCount(),
		S3IOOutputCount:        analyzeinfo.GetS3IOOutputCount(),
		NetworkIO:              analyzeinfo.GetNetworkIO(),
		ScanTime:               analyzeinfo.GetScanTime(),
		InsertTime:             analyzeinfo.GetInsertTime(),
	}
}

func DeepCopyStringList(src []string) []string {
	if src == nil {
		return nil
	}
	ret := make([]string, len(src))
	copy(ret, src)
	return ret
}

func DeepCopyNumberList[T constraints.Integer](src []T) []T {
	if src == nil {
		return nil
	}
	ret := make([]T, len(src))
	copy(ret, src)
	return ret
}

func DeepCopyPartitionByDef(partiiondef *PartitionByDef) *PartitionByDef {
	partitionDef := &plan.PartitionByDef{
		Type:                partiiondef.GetType(),
		PartitionExpression: DeepCopyExpr(partiiondef.GetPartitionExpression()),
		PartitionNum:        partiiondef.GetPartitionNum(),
		Partitions:          make([]*plan.PartitionItem, len(partiiondef.Partitions)),
		Algorithm:           partiiondef.GetAlgorithm(),
		IsSubPartition:      partiiondef.GetIsSubPartition(),
		PartitionMsg:        partiiondef.GetPartitionMsg(),
		PartitionTableNames: DeepCopyStringList(partiiondef.GetPartitionTableNames()),
	}
	if partiiondef.PartitionExpr != nil {
		partitionDef.PartitionExpr = &plan.PartitionExpr{
			Expr:    DeepCopyExpr(partiiondef.PartitionExpr.Expr),
			ExprStr: partiiondef.PartitionExpr.GetExprStr(),
		}
	}

	if partiiondef.PartitionColumns != nil {
		partitionDef.PartitionColumns = &plan.PartitionColumns{
			Columns:          DeepCopyExprList(partiiondef.PartitionColumns.Columns),
			PartitionColumns: DeepCopyStringList(partiiondef.PartitionColumns.PartitionColumns),
		}
	}

	for i, e := range partiiondef.Partitions {
		partitionDef.Partitions[i] = &plan.PartitionItem{
			PartitionName:      e.PartitionName,
			OrdinalPosition:    e.OrdinalPosition,
			Description:        e.Description,
			Comment:            e.Comment,
			LessThan:           DeepCopyExprList(e.LessThan),
			InValues:           DeepCopyExprList(e.InValues),
			PartitionTableName: e.PartitionTableName,
		}
	}
	return partitionDef
}
