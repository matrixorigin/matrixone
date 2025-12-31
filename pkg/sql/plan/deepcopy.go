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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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

func DeepCopyOrderBySpec(orderBy *plan.OrderBySpec) *plan.OrderBySpec {
	if orderBy == nil {
		return nil
	}
	return &plan.OrderBySpec{
		Expr:      DeepCopyExpr(orderBy.Expr),
		Collation: orderBy.Collation,
		Flag:      orderBy.Flag,
	}
}

func DeepCopyOrderBySpecList(orderByList []*plan.OrderBySpec) []*plan.OrderBySpec {
	if orderByList == nil {
		return nil
	}
	newList := make([]*plan.OrderBySpec, len(orderByList))
	for idx, orderBy := range orderByList {
		newList[idx] = DeepCopyOrderBySpec(orderBy)
	}
	return newList
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

func DeepCopyUpdateCtxList(updateCtxList []*plan.UpdateCtx) []*plan.UpdateCtx {
	result := make([]*plan.UpdateCtx, len(updateCtxList))
	for i, ctx := range updateCtxList {
		result[i] = &plan.UpdateCtx{
			ObjRef:        DeepCopyObjectRef(ctx.ObjRef),
			TableDef:      DeepCopyTableDef(ctx.TableDef, true),
			InsertCols:    slices.Clone(ctx.InsertCols),
			DeleteCols:    slices.Clone(ctx.DeleteCols),
			PartitionCols: slices.Clone(ctx.PartitionCols),
		}
	}

	return result
}

func DeepCopyOnDuplicateKeyCtx(ctx *plan.OnDuplicateKeyCtx) *plan.OnDuplicateKeyCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &plan.OnDuplicateKeyCtx{
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

func DeepCopyInsertCtx(ctx *plan.InsertCtx) *plan.InsertCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &plan.InsertCtx{
		Ref:             DeepCopyObjectRef(ctx.Ref),
		AddAffectedRows: ctx.AddAffectedRows,
		IsClusterTable:  ctx.IsClusterTable,
		TableDef:        DeepCopyTableDef(ctx.TableDef, true),
	}

	return newCtx
}

func DeepCopyDeleteCtx(ctx *plan.DeleteCtx) *plan.DeleteCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &plan.DeleteCtx{
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

func DeepCopyPreInsertCtx(ctx *plan.PreInsertCtx) *plan.PreInsertCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &plan.PreInsertCtx{
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

func DeepCopyPreInsertUkCtx(ctx *plan.PreInsertUkCtx) *plan.PreInsertUkCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &plan.PreInsertUkCtx{
		Columns:  slices.Clone(ctx.Columns),
		PkColumn: ctx.PkColumn,
		PkType:   ctx.PkType,
		UkType:   ctx.UkType,
	}

	return newCtx
}

func DeepCopyLockTarget(target *plan.LockTarget) *plan.LockTarget {
	if target == nil {
		return nil
	}
	return &plan.LockTarget{
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

func DeepCopyDedupJoinCtx(ctx *plan.DedupJoinCtx) *plan.DedupJoinCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &plan.DedupJoinCtx{
		OldColList:        slices.Clone(ctx.OldColList),
		UpdateColIdxList:  slices.Clone(ctx.UpdateColIdxList),
		UpdateColExprList: DeepCopyExprList(ctx.UpdateColExprList),
	}

	return newCtx
}

func DeepCopyNode(node *plan.Node) *plan.Node {
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
		LockTargets:      make([]*plan.LockTarget, len(node.LockTargets)),
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

	newNode.IndexScanInfo = plan.IndexScanInfo{
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
		newNode.RowsetData = &plan.RowsetData{
			Cols:     make([]*plan.ColData, len(node.RowsetData.Cols)),
			RowCount: node.RowsetData.RowCount,
		}

		for idx, col := range node.RowsetData.Cols {
			newNode.RowsetData.Cols[idx] = DeepCopyColData(col)
		}
	}

	return newNode
}

func DeepCopyIndexReaderParam(oldParam *plan.IndexReaderParam) *plan.IndexReaderParam {
	if oldParam == nil {
		return nil
	}

	ret := &plan.IndexReaderParam{
		OrderBy: DeepCopyOrderBySpecList(oldParam.OrderBy),
		Limit:   DeepCopyExpr(oldParam.Limit),
	}

	if oldParam.DistRange != nil {
		ret.DistRange = &plan.DistRange{
			LowerBoundType: oldParam.DistRange.LowerBoundType,
			UpperBoundType: oldParam.DistRange.UpperBoundType,
			LowerBound:     DeepCopyExpr(oldParam.DistRange.LowerBound),
			UpperBound:     DeepCopyExpr(oldParam.DistRange.UpperBound),
		}
	}

	return ret
}

func DeepCopyReplaceCtx(oldCtx *plan.ReplaceCtx) *plan.ReplaceCtx {
	if oldCtx == nil {
		return nil
	}
	ctx := &plan.ReplaceCtx{
		Ref:                       DeepCopyObjectRef(oldCtx.Ref),
		AddAffectedRows:           oldCtx.AddAffectedRows,
		IsClusterTable:            oldCtx.IsClusterTable,
		TableDef:                  DeepCopyTableDef(oldCtx.TableDef, true),
		DeleteCond:                oldCtx.DeleteCond,
		RewriteFromOnDuplicateKey: oldCtx.RewriteFromOnDuplicateKey,
	}
	return ctx
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

func DeepCopyColDefList(colDefs []*plan.ColDef) []*plan.ColDef {
	if colDefs == nil {
		return nil
	}
	newColDefs := make([]*plan.ColDef, len(colDefs))
	for i, col := range colDefs {
		newColDefs[i] = DeepCopyColDef(col)
	}
	return newColDefs
}

func DeepCopyPrimaryKeyDef(pkeyDef *plan.PrimaryKeyDef) *plan.PrimaryKeyDef {
	if pkeyDef == nil {
		return nil
	}
	def := &plan.PrimaryKeyDef{
		PkeyColName: pkeyDef.PkeyColName,
		Names:       slices.Clone(pkeyDef.Names),
	}
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
		Parts:              slices.Clone(indexDef.Parts),
	}
	newindexDef.Option = DeepCopyIndexOption(indexDef.Option)
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
		LogicalId:      table.LogicalId,
		Createsql:      table.Createsql,
		Version:        table.Version,
		Pkey:           DeepCopyPrimaryKeyDef(table.Pkey),
		Indexes:        make([]*IndexDef, len(table.Indexes)),
		Fkeys:          make([]*plan.ForeignKeyDef, len(table.Fkeys)),
		RefChildTbls:   slices.Clone(table.RefChildTbls),
		Checks:         make([]*plan.CheckDef, len(table.Checks)),
		Props:          make([]*plan.PropertyDef, len(table.Props)),
		Defs:           make([]*plan.TableDef_DefType, len(table.Defs)),
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
			Param: slices.Clone(table.TblFunc.Param),
		}
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

	if table.Indexes != nil {
		for i, indexdef := range table.Indexes {
			newTable.Indexes[i] = DeepCopyIndexDef(indexdef)
		}
	}

	if table.Partition != nil {
		newTable.Partition = &plan.Partition{
			PartitionDefs: make([]*plan.PartitionDef, len(table.Partition.PartitionDefs)),
		}
		for i, def := range table.Partition.PartitionDefs {
			newTable.Partition.PartitionDefs[i] = &plan.PartitionDef{
				Def: DeepCopyExpr(def.Def),
			}
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
			Expr: DeepCopyExpr(e.Expr),
		}
	}

	return newCol
}

func DeepCopyQuery(qry *plan.Query) *plan.Query {
	newQry := &plan.Query{
		StmtType: qry.StmtType,
		Steps:    qry.Steps,
		Nodes:    make([]*plan.Node, len(qry.Nodes)),
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
			Replace:     df.CreateTable.Replace,
			IfNotExists: df.CreateTable.IfNotExists,
			Temporary:   df.CreateTable.Temporary,
			Database:    df.CreateTable.Database,
			TableDef:    DeepCopyTableDef(df.CreateTable.TableDef, true),
			IndexTables: DeepCopyTableDefList(df.CreateTable.GetIndexTables()),
			FkDbs:       slices.Clone(df.CreateTable.FkDbs),
			FkTables:    slices.Clone(df.CreateTable.FkTables),
			FkCols:      make([]*plan.FkColName, len(df.CreateTable.FkCols)),
		}
		for i, val := range df.CreateTable.FkCols {
			CreateTable.FkCols[i] = &plan.FkColName{Cols: slices.Clone(val.Cols)}
		}
		newDf.Definition = &plan.DataDefinition_CreateTable{
			CreateTable: CreateTable,
		}

	case *plan.DataDefinition_AlterTable:
		AlterTable := &plan.AlterTable{
			Database:          df.AlterTable.Database,
			TableDef:          DeepCopyTableDef(df.AlterTable.TableDef, true),
			CopyTableDef:      DeepCopyTableDef(df.AlterTable.CopyTableDef, true),
			IsClusterTable:    df.AlterTable.IsClusterTable,
			AlgorithmType:     df.AlterTable.AlgorithmType,
			CreateTmpTableSql: df.AlterTable.CreateTmpTableSql,
			InsertTmpDataSql:  df.AlterTable.InsertTmpDataSql,
			Actions:           make([]*plan.AlterTable_Action, len(df.AlterTable.Actions)),
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
						Cols:      slices.Clone(act.AddFk.Cols),
						Fkey:      DeepCopyFkey(act.AddFk.Fkey),
					},
				}
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
				Database:  df.DropIndex.Database,
				Table:     df.DropIndex.Table,
				IndexName: df.DropIndex.IndexName,
			},
		}

	case *plan.DataDefinition_TruncateTable:
		truncateTable := &plan.TruncateTable{
			Database:        df.TruncateTable.Database,
			Table:           df.TruncateTable.Table,
			ClusterTable:    DeepCopyClusterTable(df.TruncateTable.GetClusterTable()),
			IndexTableNames: slices.Clone(df.TruncateTable.IndexTableNames),
		}
		newDf.Definition = &plan.DataDefinition_TruncateTable{
			TruncateTable: truncateTable,
		}

	case *plan.DataDefinition_ShowVariables:
		showVariables := &plan.ShowVariables{
			Global: df.ShowVariables.Global,
			Where:  DeepCopyExprList(df.ShowVariables.Where),
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
		Cols:        slices.Clone(fkey.Cols),
		ForeignTbl:  fkey.ForeignTbl,
		ForeignCols: slices.Clone(fkey.ForeignCols),
		OnDelete:    fkey.OnDelete,
		OnUpdate:    fkey.OnUpdate,
	}
	return def
}

func DeepCopyRuntimeFilterSpec(rf *plan.RuntimeFilterSpec) *plan.RuntimeFilterSpec {
	if rf == nil {
		return nil
	}
	return &plan.RuntimeFilterSpec{
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
		case *plan.Literal_VecVal:
			pc.Value = &plan.Literal_VecVal{VecVal: c.VecVal}
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
		f := item.W.Frame
		newExpr.Expr = &plan.Expr_W{
			W: &plan.WindowSpec{
				WindowFunc:  DeepCopyExpr(item.W.WindowFunc),
				PartitionBy: DeepCopyExprList(item.W.PartitionBy),
				OrderBy:     DeepCopyOrderBySpecList(item.W.OrderBy),
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
		newExpr.Expr = &plan.Expr_List{
			List: &plan.ExprList{
				List: DeepCopyExprList(item.List.List),
			},
		}

	case *plan.Expr_Vec:
		newExpr.Expr = &plan.Expr_Vec{
			Vec: &plan.LiteralVec{
				Len:  item.Vec.Len,
				Data: bytes.Clone(item.Vec.Data),
			},
		}

	case *plan.Expr_Fold:
		newExpr.Expr = &plan.Expr_Fold{
			Fold: &plan.FoldVal{
				Id:      item.Fold.Id,
				IsConst: item.Fold.IsConst,
				Data:    bytes.Clone(item.Fold.Data),
			},
		}
	}

	return newExpr
}

func DeepCopyClusterTable(cluster *plan.ClusterTable) *plan.ClusterTable {
	if cluster == nil {
		return nil
	}

	newClusterTable := &plan.ClusterTable{
		IsClusterTable:         cluster.GetIsClusterTable(),
		AccountIDs:             slices.Clone(cluster.GetAccountIDs()),
		ColumnIndexOfAccountId: cluster.GetColumnIndexOfAccountId(),
	}
	return newClusterTable
}

func DeepCopyAnalyzeInfo(analyzeinfo *plan.AnalyzeInfo) *plan.AnalyzeInfo {
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
