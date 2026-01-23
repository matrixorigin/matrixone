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
	"slices"
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

func DeepCopyOrderBySpec(orderBy *OrderBySpec) *OrderBySpec {
	if orderBy == nil {
		return nil
	}
	return &OrderBySpec{
		Expr:      DeepCopyExpr(orderBy.Expr),
		Collation: orderBy.Collation,
		Flag:      orderBy.Flag,
	}
}

func DeepCopyOrderBySpecList(orderByList []*OrderBySpec) []*OrderBySpec {
	if orderByList == nil {
		return nil
	}
	newList := make([]*OrderBySpec, len(orderByList))
	for idx, orderBy := range orderByList {
		newList[idx] = DeepCopyOrderBySpec(orderBy)
	}
	return newList
}

func DeepCopyObjectRef(ref *ObjectRef) *ObjectRef {
	if ref == nil {
		return nil
	}
	return &ObjectRef{
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

func DeepCopyUpdateCtxList(updateCtxList []*UpdateCtx) []*UpdateCtx {
	result := make([]*UpdateCtx, len(updateCtxList))
	for i, ctx := range updateCtxList {
		result[i] = &UpdateCtx{
			ObjRef:        DeepCopyObjectRef(ctx.ObjRef),
			TableDef:      DeepCopyTableDef(ctx.TableDef, true),
			InsertCols:    slices.Clone(ctx.InsertCols),
			DeleteCols:    slices.Clone(ctx.DeleteCols),
			PartitionCols: slices.Clone(ctx.PartitionCols),
		}
	}

	return result
}

func DeepCopyOnDuplicateKeyCtx(ctx *OnDuplicateKeyCtx) *OnDuplicateKeyCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &OnDuplicateKeyCtx{
		OnDuplicateIdx: slices.Clone(ctx.OnDuplicateIdx),
	}

	if ctx.OnDuplicateExpr != nil {
		newCtx.OnDuplicateExpr = make(map[string]*Expr)
		for k, v := range ctx.OnDuplicateExpr {
			newCtx.OnDuplicateExpr[k] = DeepCopyExpr(v)
		}
	}

	return newCtx
}

func DeepCopyInsertCtx(ctx *InsertCtx) *InsertCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &InsertCtx{
		Ref:             DeepCopyObjectRef(ctx.Ref),
		AddAffectedRows: ctx.AddAffectedRows,
		IsClusterTable:  ctx.IsClusterTable,
		TableDef:        DeepCopyTableDef(ctx.TableDef, true),
	}

	return newCtx
}

func DeepCopyDeleteCtx(ctx *DeleteCtx) *DeleteCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &DeleteCtx{
		CanTruncate:     ctx.CanTruncate,
		AddAffectedRows: ctx.AddAffectedRows,
		RowIdIdx:        ctx.RowIdIdx,
		Ref:             DeepCopyObjectRef(ctx.Ref),
		IsClusterTable:  ctx.IsClusterTable,
		TableDef:        DeepCopyTableDef(ctx.TableDef, true),
		PrimaryKeyIdx:   ctx.PrimaryKeyIdx,
	}

	return newCtx
}

func DeepCopyPreInsertCtx(ctx *PreInsertCtx) *PreInsertCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &PreInsertCtx{
		Ref:           DeepCopyObjectRef(ctx.Ref),
		TableDef:      DeepCopyTableDef(ctx.TableDef, true),
		HasAutoCol:    ctx.HasAutoCol,
		ColOffset:     ctx.ColOffset,
		CompPkeyExpr:  DeepCopyExpr(ctx.CompPkeyExpr),
		ClusterByExpr: DeepCopyExpr(ctx.ClusterByExpr),
		IsOldUpdate:   ctx.IsOldUpdate,
		IsNewUpdate:   ctx.IsNewUpdate,
	}

	return newCtx
}

func DeepCopyPreInsertUkCtx(ctx *PreInsertUkCtx) *PreInsertUkCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &PreInsertUkCtx{
		Columns:  slices.Clone(ctx.Columns),
		PkColumn: ctx.PkColumn,
		PkType:   ctx.PkType,
		UkType:   ctx.UkType,
	}

	return newCtx
}

func DeepCopyLockTarget(target *LockTarget) *LockTarget {
	if target == nil {
		return nil
	}
	return &LockTarget{
		TableId:            target.TableId,
		ObjRef:             DeepCopyObjectRef(target.ObjRef),
		PrimaryColIdxInBat: target.PrimaryColIdxInBat,
		PrimaryColTyp:      target.PrimaryColTyp,
		RefreshTsIdxInBat:  target.RefreshTsIdxInBat,
		FilterColIdxInBat:  target.FilterColIdxInBat,
		LockTable:          target.LockTable,
		Block:              target.Block,
		LockRows:           DeepCopyExpr(target.LockRows),
		LockTableAtTheEnd:  target.LockTableAtTheEnd,
	}
}

func DeepCopyDedupJoinCtx(ctx *DedupJoinCtx) *DedupJoinCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &DedupJoinCtx{
		OldColList:        slices.Clone(ctx.OldColList),
		UpdateColIdxList:  slices.Clone(ctx.UpdateColIdxList),
		UpdateColExprList: DeepCopyExprList(ctx.UpdateColExprList),
	}

	return newCtx
}

func DeepCopyNode(node *Node) *Node {
	newNode := &Node{
		NodeType:         node.NodeType,
		NodeId:           node.NodeId,
		ExtraOptions:     node.ExtraOptions,
		Children:         slices.Clone(node.Children),
		JoinType:         node.JoinType,
		IsRightJoin:      node.IsRightJoin,
		BindingTags:      slices.Clone(node.BindingTags),
		Limit:            DeepCopyExpr(node.Limit),
		Offset:           DeepCopyExpr(node.Offset),
		ProjectList:      DeepCopyExprList(node.ProjectList),
		OnList:           DeepCopyExprList(node.OnList),
		FilterList:       DeepCopyExprList(node.FilterList),
		BlockFilterList:  DeepCopyExprList(node.BlockFilterList),
		GroupBy:          DeepCopyExprList(node.GroupBy),
		GroupingFlag:     slices.Clone(node.GroupingFlag),
		AggList:          DeepCopyExprList(node.AggList),
		OrderBy:          DeepCopyOrderBySpecList(node.OrderBy),
		DeleteCtx:        DeepCopyDeleteCtx(node.DeleteCtx),
		TblFuncExprList:  DeepCopyExprList(node.TblFuncExprList),
		ClusterTable:     DeepCopyClusterTable(node.GetClusterTable()),
		InsertCtx:        DeepCopyInsertCtx(node.InsertCtx),
		ReplaceCtx:       DeepCopyReplaceCtx(node.ReplaceCtx),
		NotCacheable:     node.NotCacheable,
		SourceStep:       node.SourceStep,
		PreInsertCtx:     DeepCopyPreInsertCtx(node.PreInsertCtx),
		PreInsertUkCtx:   DeepCopyPreInsertUkCtx(node.PreInsertUkCtx),
		OnDuplicateKey:   DeepCopyOnDuplicateKeyCtx(node.OnDuplicateKey),
		LockTargets:      make([]*LockTarget, len(node.LockTargets)),
		AnalyzeInfo:      DeepCopyAnalyzeInfo(node.AnalyzeInfo),
		IsEnd:            node.IsEnd,
		ExternScan:       node.ExternScan,
		SampleFunc:       DeepCopySampleFuncSpec(node.SampleFunc),
		OnUpdateExprs:    DeepCopyExprList(node.OnUpdateExprs),
		DedupColName:     node.DedupColName,
		DedupColTypes:    slices.Clone(node.DedupColTypes),
		UpdateCtxList:    DeepCopyUpdateCtxList(node.UpdateCtxList),
		DedupJoinCtx:     DeepCopyDedupJoinCtx(node.DedupJoinCtx),
		IndexReaderParam: DeepCopyIndexReaderParam(node.IndexReaderParam),
	}
	newNode.Uuid = append(newNode.Uuid, node.Uuid...)

	for idx, target := range node.LockTargets {
		newNode.LockTargets[idx] = DeepCopyLockTarget(target)
	}

	newNode.Stats = DeepCopyStats(node.Stats)

	newNode.ObjRef = DeepCopyObjectRef(node.ObjRef)
	newNode.ParentObjRef = DeepCopyObjectRef(node.ParentObjRef)

	newNode.IndexScanInfo = IndexScanInfo{
		IsIndexScan:    node.IndexScanInfo.IsIndexScan,
		IndexName:      node.IndexScanInfo.IndexName,
		BelongToTable:  node.IndexScanInfo.BelongToTable,
		Parts:          slices.Clone(node.IndexScanInfo.Parts),
		IsUnique:       node.IndexScanInfo.IsUnique,
		IndexTableName: node.IndexScanInfo.IndexTableName,
	}

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
		newNode.RowsetData = &RowsetData{
			Cols:     make([]*ColData, len(node.RowsetData.Cols)),
			RowCount: node.RowsetData.RowCount,
		}

		for idx, col := range node.RowsetData.Cols {
			newNode.RowsetData.Cols[idx] = DeepCopyColData(col)
		}
	}

	return newNode
}

func DeepCopyIndexReaderParam(oldParam *IndexReaderParam) *IndexReaderParam {
	if oldParam == nil {
		return nil
	}

	ret := &IndexReaderParam{
		OrderBy: DeepCopyOrderBySpecList(oldParam.OrderBy),
		Limit:   DeepCopyExpr(oldParam.Limit),
	}

	if oldParam.DistRange != nil {
		ret.DistRange = &DistRange{
			LowerBoundType: oldParam.DistRange.LowerBoundType,
			UpperBoundType: oldParam.DistRange.UpperBoundType,
			LowerBound:     DeepCopyExpr(oldParam.DistRange.LowerBound),
			UpperBound:     DeepCopyExpr(oldParam.DistRange.UpperBound),
		}
	}

	return ret
}

func DeepCopyReplaceCtx(oldCtx *ReplaceCtx) *ReplaceCtx {
	if oldCtx == nil {
		return nil
	}
	ctx := &ReplaceCtx{
		Ref:                       DeepCopyObjectRef(oldCtx.Ref),
		AddAffectedRows:           oldCtx.AddAffectedRows,
		IsClusterTable:            oldCtx.IsClusterTable,
		TableDef:                  DeepCopyTableDef(oldCtx.TableDef, true),
		DeleteCond:                oldCtx.DeleteCond,
		RewriteFromOnDuplicateKey: oldCtx.RewriteFromOnDuplicateKey,
	}
	return ctx
}

func DeepCopyDefault(def *Default) *Default {
	if def == nil {
		return nil
	}
	return &Default{
		NullAbility:  def.NullAbility,
		Expr:         DeepCopyExpr(def.Expr),
		OriginString: def.OriginString,
	}
}

func DeepCopyType(typ *Type) *Type {
	if typ == nil {
		return nil
	}
	return &Type{
		Id:          typ.Id,
		NotNullable: typ.NotNullable,
		Width:       typ.Width,
		Scale:       typ.Scale,
		AutoIncr:    typ.AutoIncr,
		Enumvalues:  typ.Enumvalues,
	}
}

func DeepCopyColDef(col *ColDef) *ColDef {
	if col == nil {
		return nil
	}
	return &ColDef{
		ColId:      col.ColId,
		Name:       col.Name,
		OriginName: col.OriginName,
		Alg:        col.Alg,
		Typ:        col.Typ,
		Default:    DeepCopyDefault(col.Default),
		Primary:    col.Primary,
		Pkidx:      col.Pkidx,
		Comment:    col.Comment,
		OnUpdate:   DeepCopyOnUpdate(col.OnUpdate),
		ClusterBy:  col.ClusterBy,
		Hidden:     col.Hidden,
		Seqnum:     col.Seqnum,
		TblName:    col.TblName,
		DbName:     col.DbName,
	}
}

func DeepCopyColDefList(colDefs []*ColDef) []*ColDef {
	if colDefs == nil {
		return nil
	}
	newColDefs := make([]*ColDef, len(colDefs))
	for i, col := range colDefs {
		newColDefs[i] = DeepCopyColDef(col)
	}
	return newColDefs
}

func DeepCopyPrimaryKeyDef(pkeyDef *PrimaryKeyDef) *PrimaryKeyDef {
	if pkeyDef == nil {
		return nil
	}
	def := &PrimaryKeyDef{
		PkeyColName: pkeyDef.PkeyColName,
		Names:       slices.Clone(pkeyDef.Names),
	}
	// Check whether the composite primary key column is included
	if pkeyDef.CompPkeyCol != nil {
		def.CompPkeyCol = DeepCopyColDef(pkeyDef.CompPkeyCol)
	}
	return def
}

func DeepCopyIndexDef(indexDef *IndexDef) *IndexDef {
	if indexDef == nil {
		return nil
	}
	newindexDef := &IndexDef{
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
		Parts:              slices.Clone(indexDef.Parts),
	}
	newindexDef.Option = DeepCopyIndexOption(indexDef.Option)
	return newindexDef
}

func DeepCopyIndexOption(indexOption *IndexOption) *IndexOption {
	if indexOption == nil {
		return nil
	}
	newIndexOption := &IndexOption{
		CreateExtraTable: indexOption.CreateExtraTable,
	}

	return newIndexOption
}

func DeepCopyOnUpdate(old *OnUpdate) *OnUpdate {
	if old == nil {
		return nil
	}
	return &OnUpdate{
		Expr:         DeepCopyExpr(old.Expr),
		OriginString: old.OriginString,
	}
}

func DeepCopyTableDefList(src []*TableDef) []*TableDef {
	if src == nil {
		return nil
	}
	ret := make([]*TableDef, len(src))
	for i, def := range src {
		ret[i] = DeepCopyTableDef(def, true)
	}
	return ret
}

func DeepCopySampleFuncSpec(source *SampleFuncSpec) *SampleFuncSpec {
	if source == nil {
		return nil
	}
	return &SampleFuncSpec{
		Rows:    source.Rows,
		Percent: source.Percent,
	}
}

func DeepCopyTableDef(table *TableDef, withCols bool) *TableDef {
	if table == nil {
		return nil
	}
	newTable := &TableDef{
		TblId:          table.TblId,
		Name:           table.Name,
		Hidden:         table.Hidden,
		TableType:      table.TableType,
		LogicalId:      table.LogicalId,
		Createsql:      table.Createsql,
		Version:        table.Version,
		Pkey:           DeepCopyPrimaryKeyDef(table.Pkey),
		Indexes:        make([]*IndexDef, len(table.Indexes)),
		Fkeys:          make([]*ForeignKeyDef, len(table.Fkeys)),
		RefChildTbls:   slices.Clone(table.RefChildTbls),
		Checks:         make([]*CheckDef, len(table.Checks)),
		Props:          make([]*PropertyDef, len(table.Props)),
		Defs:           make([]*TableDef_DefType, len(table.Defs)),
		Name2ColIndex:  table.Name2ColIndex,
		IsLocked:       table.IsLocked,
		TableLockType:  table.TableLockType,
		IsTemporary:    table.IsTemporary,
		AutoIncrOffset: table.AutoIncrOffset,
		DbName:         table.DbName,
		DbId:           table.DbId,
		FeatureFlag:    table.FeatureFlag,
	}

	if withCols {
		newTable.Cols = DeepCopyColDefList(table.Cols)
	}

	for idx, fkey := range table.Fkeys {
		newTable.Fkeys[idx] = DeepCopyFkey(fkey)
	}

	for idx, col := range table.Checks {
		newTable.Checks[idx] = &CheckDef{
			Name:  col.Name,
			Check: DeepCopyExpr(col.Check),
		}
	}

	for idx, prop := range table.Props {
		newTable.Props[idx] = &PropertyDef{
			Key:   prop.Key,
			Value: prop.Value,
		}
	}

	if table.TblFunc != nil {
		newTable.TblFunc = &TableFunction{
			Name:  table.TblFunc.Name,
			Param: slices.Clone(table.TblFunc.Param),
		}
	}

	if table.ClusterBy != nil {
		newTable.ClusterBy = &ClusterByDef{
			//Parts: make([]*Expr, len(table.ClusterBy.Parts)),
			Name:         table.ClusterBy.Name,
			CompCbkeyCol: DeepCopyColDef(table.ClusterBy.CompCbkeyCol),
		}
		//for i, part := range table.ClusterBy.Parts {
		//	newTable.ClusterBy.Parts[i] = DeepCopyExpr(part)
		//}
	}

	if table.ViewSql != nil {
		newTable.ViewSql = &ViewDef{
			View: table.ViewSql.View,
		}
	}

	if table.Indexes != nil {
		for i, indexdef := range table.Indexes {
			newTable.Indexes[i] = DeepCopyIndexDef(indexdef)
		}
	}

	if table.Partition != nil {
		newTable.Partition = &Partition{
			PartitionDefs: make([]*PartitionDef, len(table.Partition.PartitionDefs)),
		}
		for i, def := range table.Partition.PartitionDefs {
			newTable.Partition.PartitionDefs[i] = &PartitionDef{
				Def: DeepCopyExpr(def.Def),
			}
		}
	}

	for idx, def := range table.Defs {
		switch defImpl := def.Def.(type) {
		case *TableDef_DefType_Properties:
			propDef := &PropertiesDef{
				Properties: make([]*Property, len(defImpl.Properties.Properties)),
			}
			for i, p := range defImpl.Properties.Properties {
				propDef.Properties[i] = &Property{
					Key:   p.Key,
					Value: p.Value,
				}
			}
			newTable.Defs[idx] = &TableDef_DefType{
				Def: &TableDef_DefType_Properties{
					Properties: propDef,
				},
			}
		}
	}

	return newTable
}

func DeepCopyColData(col *ColData) *ColData {
	newCol := &ColData{
		Data: make([]*RowsetExpr, len(col.Data)),
	}
	for i, e := range col.Data {
		newCol.Data[i] = &RowsetExpr{
			Expr: DeepCopyExpr(e.Expr),
		}
	}

	return newCol
}

func DeepCopyQuery(qry *Query) *Query {
	newQry := &Query{
		StmtType: qry.StmtType,
		Steps:    qry.Steps,
		Nodes:    make([]*Node, len(qry.Nodes)),
		Params:   DeepCopyExprList(qry.Params),
		Headings: qry.Headings,
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
			Plan: &Plan_Query{
				Query: DeepCopyQuery(p.Query),
			},
			IsPrepare:   pl.IsPrepare,
			TryRunTimes: pl.TryRunTimes,
		}

	case *Plan_Ddl:
		return &Plan{
			Plan: &Plan_Ddl{
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

func DeepCopyDataDefinition(old *DataDefinition) *DataDefinition {
	newDf := &DataDefinition{
		DdlType: old.DdlType,
	}
	if old.Query != nil {
		newDf.Query = DeepCopyQuery(old.Query)
	}

	switch df := old.Definition.(type) {
	case *DataDefinition_CreateDatabase:
		newDf.Definition = &DataDefinition_CreateDatabase{
			CreateDatabase: &CreateDatabase{
				IfNotExists: df.CreateDatabase.IfNotExists,
				Database:    df.CreateDatabase.Database,
			},
		}

	case *DataDefinition_AlterDatabase:
		newDf.Definition = &DataDefinition_AlterDatabase{
			AlterDatabase: &AlterDatabase{
				IfExists: df.AlterDatabase.IfExists,
				Database: df.AlterDatabase.Database,
			},
		}

	case *DataDefinition_DropDatabase:
		newDf.Definition = &DataDefinition_DropDatabase{
			DropDatabase: &DropDatabase{
				IfExists:   df.DropDatabase.IfExists,
				Database:   df.DropDatabase.Database,
				DatabaseId: df.DropDatabase.DatabaseId,
			},
		}

	case *DataDefinition_CreateTable:
		CreateTable := &CreateTable{
			Replace:     df.CreateTable.Replace,
			IfNotExists: df.CreateTable.IfNotExists,
			Temporary:   df.CreateTable.Temporary,
			Database:    df.CreateTable.Database,
			TableDef:    DeepCopyTableDef(df.CreateTable.TableDef, true),
			IndexTables: DeepCopyTableDefList(df.CreateTable.GetIndexTables()),
			FkDbs:       slices.Clone(df.CreateTable.FkDbs),
			FkTables:    slices.Clone(df.CreateTable.FkTables),
			FkCols:      make([]*FkColName, len(df.CreateTable.FkCols)),
		}
		for i, val := range df.CreateTable.FkCols {
			CreateTable.FkCols[i] = &FkColName{Cols: slices.Clone(val.Cols)}
		}
		newDf.Definition = &DataDefinition_CreateTable{
			CreateTable: CreateTable,
		}

	case *DataDefinition_AlterTable:
		AlterTable := &AlterTable{
			Database:          df.AlterTable.Database,
			TableDef:          DeepCopyTableDef(df.AlterTable.TableDef, true),
			CopyTableDef:      DeepCopyTableDef(df.AlterTable.CopyTableDef, true),
			IsClusterTable:    df.AlterTable.IsClusterTable,
			AlgorithmType:     df.AlterTable.AlgorithmType,
			CreateTmpTableSql: df.AlterTable.CreateTmpTableSql,
			InsertTmpDataSql:  df.AlterTable.InsertTmpDataSql,
			Actions:           make([]*AlterTable_Action, len(df.AlterTable.Actions)),
		}
		for i, action := range df.AlterTable.Actions {
			switch act := action.Action.(type) {
			case *AlterTable_Action_Drop:
				AlterTable.Actions[i] = &AlterTable_Action{
					Action: &AlterTable_Action_Drop{
						Drop: &AlterTableDrop{
							Typ:  act.Drop.Typ,
							Name: act.Drop.Name,
						},
					},
				}
			case *AlterTable_Action_AddFk:
				AddFk := &AlterTable_Action_AddFk{
					AddFk: &AlterTableAddFk{
						DbName:    act.AddFk.DbName,
						TableName: act.AddFk.TableName,
						Cols:      slices.Clone(act.AddFk.Cols),
						Fkey:      DeepCopyFkey(act.AddFk.Fkey),
					},
				}
				AlterTable.Actions[i] = &AlterTable_Action{
					Action: AddFk,
				}
			}
		}

		newDf.Definition = &DataDefinition_AlterTable{
			AlterTable: AlterTable,
		}

	case *DataDefinition_DropTable:
		newDf.Definition = &DataDefinition_DropTable{
			DropTable: &DropTable{
				IfExists:        df.DropTable.IfExists,
				Database:        df.DropTable.Database,
				Table:           df.DropTable.Table,
				IndexTableNames: slices.Clone(df.DropTable.GetIndexTableNames()),
				ClusterTable:    DeepCopyClusterTable(df.DropTable.GetClusterTable()),
				TableId:         df.DropTable.GetTableId(),
				ForeignTbl:      slices.Clone(df.DropTable.GetForeignTbl()),
				IsView:          df.DropTable.IsView,
				TableDef:        DeepCopyTableDef(df.DropTable.GetTableDef(), true),
			},
		}

	case *DataDefinition_CreateIndex:
		newDf.Definition = &DataDefinition_CreateIndex{
			CreateIndex: &CreateIndex{
				Database: df.CreateIndex.Database,
				Table:    df.CreateIndex.Table,
				Index: &CreateTable{
					IfNotExists: df.CreateIndex.Index.IfNotExists,
					Temporary:   df.CreateIndex.Index.Temporary,
					Database:    df.CreateIndex.Index.Database,
					TableDef:    DeepCopyTableDef(df.CreateIndex.Index.TableDef, true),
				},
				OriginTablePrimaryKey: df.CreateIndex.OriginTablePrimaryKey,
			},
		}

	case *DataDefinition_AlterIndex:
		newDf.Definition = &DataDefinition_AlterIndex{
			AlterIndex: &AlterIndex{
				Index: df.AlterIndex.Index,
			},
		}

	case *DataDefinition_DropIndex:
		newDf.Definition = &DataDefinition_DropIndex{
			DropIndex: &DropIndex{
				Database:  df.DropIndex.Database,
				Table:     df.DropIndex.Table,
				IndexName: df.DropIndex.IndexName,
			},
		}

	case *DataDefinition_TruncateTable:
		truncateTable := &TruncateTable{
			Database:        df.TruncateTable.Database,
			Table:           df.TruncateTable.Table,
			ClusterTable:    DeepCopyClusterTable(df.TruncateTable.GetClusterTable()),
			IndexTableNames: slices.Clone(df.TruncateTable.IndexTableNames),
		}
		newDf.Definition = &DataDefinition_TruncateTable{
			TruncateTable: truncateTable,
		}

	case *DataDefinition_ShowVariables:
		showVariables := &ShowVariables{
			Global: df.ShowVariables.Global,
			Where:  DeepCopyExprList(df.ShowVariables.Where),
		}

		newDf.Definition = &DataDefinition_ShowVariables{
			ShowVariables: showVariables,
		}

	case *DataDefinition_LockTables:
		newDf.Definition = &DataDefinition_LockTables{
			LockTables: &LockTables{
				TableLocks: df.LockTables.TableLocks,
			},
		}

	case *DataDefinition_UnlockTables:
		newDf.Definition = &DataDefinition_UnlockTables{
			UnlockTables: &UnLockTables{},
		}

	case *DataDefinition_AlterSequence:
		newDf.Definition = &DataDefinition_AlterSequence{
			AlterSequence: &AlterSequence{
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
		Cols:        slices.Clone(fkey.Cols),
		ForeignTbl:  fkey.ForeignTbl,
		ForeignCols: slices.Clone(fkey.ForeignCols),
		OnDelete:    fkey.OnDelete,
		OnUpdate:    fkey.OnUpdate,
	}
	return def
}

func DeepCopyRuntimeFilterSpec(rf *RuntimeFilterSpec) *RuntimeFilterSpec {
	if rf == nil {
		return nil
	}
	return &RuntimeFilterSpec{
		Tag:         rf.Tag,
		MatchPrefix: rf.MatchPrefix,
		UpperLimit:  rf.UpperLimit,
		Expr:        DeepCopyExpr(rf.Expr),
		NotOnPk:     rf.NotOnPk,
	}
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
	case *Expr_Lit:
		pc := &Literal{
			Isnull: item.Lit.GetIsnull(),
			Src:    item.Lit.Src,
		}

		switch c := item.Lit.Value.(type) {
		case *Literal_I8Val:
			pc.Value = &Literal_I8Val{I8Val: c.I8Val}
		case *Literal_I16Val:
			pc.Value = &Literal_I16Val{I16Val: c.I16Val}
		case *Literal_I32Val:
			pc.Value = &Literal_I32Val{I32Val: c.I32Val}
		case *Literal_I64Val:
			pc.Value = &Literal_I64Val{I64Val: c.I64Val}
		case *Literal_Dval:
			pc.Value = &Literal_Dval{Dval: c.Dval}
		case *Literal_Sval:
			pc.Value = &Literal_Sval{Sval: c.Sval}
		case *Literal_Bval:
			pc.Value = &Literal_Bval{Bval: c.Bval}
		case *Literal_U8Val:
			pc.Value = &Literal_U8Val{U8Val: c.U8Val}
		case *Literal_U16Val:
			pc.Value = &Literal_U16Val{U16Val: c.U16Val}
		case *Literal_U32Val:
			pc.Value = &Literal_U32Val{U32Val: c.U32Val}
		case *Literal_U64Val:
			pc.Value = &Literal_U64Val{U64Val: c.U64Val}
		case *Literal_Fval:
			pc.Value = &Literal_Fval{Fval: c.Fval}
		case *Literal_Dateval:
			pc.Value = &Literal_Dateval{Dateval: c.Dateval}
		case *Literal_Timeval:
			pc.Value = &Literal_Timeval{Timeval: c.Timeval}
		case *Literal_Datetimeval:
			pc.Value = &Literal_Datetimeval{Datetimeval: c.Datetimeval}
		case *Literal_Decimal64Val:
			pc.Value = &Literal_Decimal64Val{Decimal64Val: &Decimal64{A: c.Decimal64Val.A}}
		case *Literal_Decimal128Val:
			pc.Value = &Literal_Decimal128Val{Decimal128Val: &Decimal128{A: c.Decimal128Val.A, B: c.Decimal128Val.B}}
		case *Literal_Timestampval:
			pc.Value = &Literal_Timestampval{Timestampval: c.Timestampval}
		case *Literal_Jsonval:
			pc.Value = &Literal_Jsonval{Jsonval: c.Jsonval}
		case *Literal_Defaultval:
			pc.Value = &Literal_Defaultval{Defaultval: c.Defaultval}
		case *Literal_UpdateVal:
			pc.Value = &Literal_UpdateVal{UpdateVal: c.UpdateVal}
		case *Literal_EnumVal:
			pc.Value = &Literal_EnumVal{EnumVal: c.EnumVal}
		case *Literal_VecVal:
			pc.Value = &Literal_VecVal{VecVal: c.VecVal}
		}

		newExpr.Expr = &Expr_Lit{
			Lit: pc,
		}

	case *Expr_P:
		newExpr.Expr = &Expr_P{
			P: &ParamRef{
				Pos: item.P.GetPos(),
			},
		}

	case *Expr_V:
		newExpr.Expr = &Expr_V{
			V: &VarRef{
				Name:   item.V.GetName(),
				Global: item.V.GetGlobal(),
				System: item.V.GetSystem(),
			},
		}

	case *Expr_Col:
		newExpr.Expr = &Expr_Col{
			Col: &ColRef{
				RelPos: item.Col.GetRelPos(),
				ColPos: item.Col.GetColPos(),
				Name:   item.Col.GetName(),
			},
		}

	case *Expr_F:
		newArgs := make([]*Expr, len(item.F.Args))
		for idx, arg := range item.F.Args {
			newArgs[idx] = DeepCopyExpr(arg)
		}
		newExpr.Expr = &Expr_F{
			F: &Function{
				Func: DeepCopyObjectRef(item.F.Func),
				Args: newArgs,
			},
		}

	case *Expr_W:
		f := item.W.Frame
		newExpr.Expr = &Expr_W{
			W: &WindowSpec{
				WindowFunc:  DeepCopyExpr(item.W.WindowFunc),
				PartitionBy: DeepCopyExprList(item.W.PartitionBy),
				OrderBy:     DeepCopyOrderBySpecList(item.W.OrderBy),
				Name:        item.W.Name,
				Frame: &FrameClause{
					Type: f.Type,
					Start: &FrameBound{
						Type:      f.Start.Type,
						UnBounded: f.Start.UnBounded,
						Val:       DeepCopyExpr(f.Start.Val),
					},
					End: &FrameBound{
						Type:      f.End.Type,
						UnBounded: f.End.UnBounded,
						Val:       DeepCopyExpr(f.End.Val),
					},
				},
			},
		}

	case *Expr_Sub:
		newExpr.Expr = &Expr_Sub{
			Sub: &SubqueryRef{
				NodeId:  item.Sub.GetNodeId(),
				Typ:     item.Sub.Typ,
				Op:      item.Sub.Op,
				RowSize: item.Sub.RowSize,
				Child:   DeepCopyExpr(item.Sub.Child),
			},
		}

	case *Expr_Corr:
		newExpr.Expr = &Expr_Corr{
			Corr: &CorrColRef{
				ColPos: item.Corr.GetColPos(),
				RelPos: item.Corr.GetRelPos(),
				Depth:  item.Corr.GetDepth(),
			},
		}

	case *Expr_T:
		newExpr.Expr = &Expr_T{
			T: &TargetType{},
		}

	case *Expr_Max:
		newExpr.Expr = &Expr_Max{
			Max: &MaxValue{
				Value: item.Max.GetValue(),
			},
		}

	case *Expr_List:
		newExpr.Expr = &Expr_List{
			List: &ExprList{
				List: DeepCopyExprList(item.List.List),
			},
		}

	case *Expr_Vec:
		newExpr.Expr = &Expr_Vec{
			Vec: &LiteralVec{
				Len:  item.Vec.Len,
				Data: bytes.Clone(item.Vec.Data),
			},
		}

	case *Expr_Fold:
		newExpr.Expr = &Expr_Fold{
			Fold: &FoldVal{
				Id:      item.Fold.Id,
				IsConst: item.Fold.IsConst,
				Data:    bytes.Clone(item.Fold.Data),
			},
		}
	}

	return newExpr
}

func DeepCopyClusterTable(cluster *ClusterTable) *ClusterTable {
	if cluster == nil {
		return nil
	}

	newClusterTable := &ClusterTable{
		IsClusterTable:         cluster.GetIsClusterTable(),
		AccountIDs:             slices.Clone(cluster.GetAccountIDs()),
		ColumnIndexOfAccountId: cluster.GetColumnIndexOfAccountId(),
	}
	return newClusterTable
}

func DeepCopyAnalyzeInfo(analyzeinfo *AnalyzeInfo) *AnalyzeInfo {
	if analyzeinfo == nil {
		return nil
	}

	var copyAnalyzeInfo = *analyzeinfo
	copyAnalyzeInfo.TimeConsumedArrayMajor = slices.Clone(analyzeinfo.GetTimeConsumedArrayMajor())
	copyAnalyzeInfo.TimeConsumedArrayMinor = slices.Clone(analyzeinfo.GetTimeConsumedArrayMinor())

	// clear the unrecognized fields, do not mess with the proto stuff.
	copyAnalyzeInfo.XXX_unrecognized = nil
	copyAnalyzeInfo.XXX_sizecache = 0

	return &copyAnalyzeInfo
}

func DeepCopyStats(stats *Stats) *Stats {
	if stats == nil {
		return nil
	}
	var hashmapStats *HashMapStats
	if stats.HashmapStats != nil {
		hashmapStats = &HashMapStats{
			HashmapSize:   stats.HashmapStats.HashmapSize,
			HashOnPK:      stats.HashmapStats.HashOnPK,
			Shuffle:       stats.HashmapStats.Shuffle,
			ShuffleColIdx: stats.HashmapStats.ShuffleColIdx,
			ShuffleType:   stats.HashmapStats.ShuffleType,
			ShuffleColMin: stats.HashmapStats.ShuffleColMin,
			ShuffleColMax: stats.HashmapStats.ShuffleColMax,
			ShuffleMethod: stats.HashmapStats.ShuffleMethod,
		}
	}
	return &Stats{
		BlockNum:     stats.BlockNum,
		Rowsize:      stats.Rowsize,
		Cost:         stats.Cost,
		Outcnt:       stats.Outcnt,
		TableCnt:     stats.TableCnt,
		Selectivity:  stats.Selectivity,
		HashmapStats: hashmapStats,
		ForceOneCN:   stats.ForceOneCN,
	}
}
