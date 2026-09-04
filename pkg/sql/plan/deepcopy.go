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

	"github.com/gogo/protobuf/proto"
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
		Server:           ref.Server,
		Db:               ref.Db,
		Schema:           ref.Schema,
		Obj:              ref.Obj,
		ServerName:       ref.ServerName,
		DbName:           ref.DbName,
		SchemaName:       ref.SchemaName,
		ObjName:          ref.ObjName,
		SubscriptionName: ref.SubscriptionName,
		PubInfo:          ref.PubInfo,
		NotLockMeta:      ref.NotLockMeta,
		Snapshot:         DeepCopySnapshot(ref.Snapshot),
	}
}

func DeepCopySnapshot(snapshot *plan.Snapshot) *plan.Snapshot {
	if snapshot == nil {
		return nil
	}
	cloned := *snapshot
	if snapshot.TS != nil {
		ts := *snapshot.TS
		cloned.TS = &ts
	}
	if snapshot.Tenant != nil {
		tenant := *snapshot.Tenant
		cloned.Tenant = &tenant
	}
	if snapshot.ExtraInfo != nil {
		extraInfo := *snapshot.ExtraInfo
		cloned.ExtraInfo = &extraInfo
	}
	return &cloned
}

func DeepCopyUpdateCtxList(updateCtxList []*plan.UpdateCtx) []*plan.UpdateCtx {
	result := make([]*plan.UpdateCtx, len(updateCtxList))
	for i, ctx := range updateCtxList {
		result[i] = &plan.UpdateCtx{
			ObjRef:                DeepCopyObjectRef(ctx.ObjRef),
			TableDef:              DeepCopyTableDef(ctx.TableDef, true),
			InsertCols:            slices.Clone(ctx.InsertCols),
			DeleteCols:            slices.Clone(ctx.DeleteCols),
			PartitionCols:         slices.Clone(ctx.PartitionCols),
			SkipInsertOnNullPk:    ctx.SkipInsertOnNullPk,
			InsertPkColIdx:        ctx.InsertPkColIdx,
			CountDeleteAffectRows: ctx.CountDeleteAffectRows,
			IgnoreAffectedRows:    ctx.IgnoreAffectedRows,
			DedupByTargetRowId:    ctx.DedupByTargetRowId,
			TargetUpdateCtxIdx:    ctx.TargetUpdateCtxIdx,
			AffectedRowsCols:      slices.Clone(ctx.AffectedRowsCols),
		}
		if ctx.ChangedRowsCol != nil {
			changedRowsCol := *ctx.ChangedRowsCol
			result[i].ChangedRowsCol = &changedRowsCol
		}
	}

	return result
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
		Ref:                DeepCopyObjectRef(ctx.Ref),
		TableDef:           DeepCopyTableDef(ctx.TableDef, true),
		HasAutoCol:         ctx.HasAutoCol,
		ColOffset:          ctx.ColOffset,
		CompPkeyExpr:       DeepCopyExpr(ctx.CompPkeyExpr),
		ClusterByExpr:      DeepCopyExpr(ctx.ClusterByExpr),
		IsOldUpdate:        ctx.IsOldUpdate,
		IsNewUpdate:        ctx.IsNewUpdate,
		HasTargetSelector:  ctx.HasTargetSelector,
		TargetRowNumberCol: ctx.TargetRowNumberCol,
		TargetActiveCol:    ctx.TargetActiveCol,
		TargetRowIdCol:     ctx.TargetRowIdCol,
	}

	return newCtx
}

func DeepCopyPreInsertUkCtx(ctx *plan.PreInsertUkCtx) *plan.PreInsertUkCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &plan.PreInsertUkCtx{
		Columns:                slices.Clone(ctx.Columns),
		PkColumn:               ctx.PkColumn,
		PkType:                 ctx.PkType,
		UkType:                 ctx.UkType,
		InsertIgnoreMultiDedup: ctx.InsertIgnoreMultiDedup,
		KeyColumns:             slices.Clone(ctx.KeyColumns),
		ConflictColumns:        slices.Clone(ctx.ConflictColumns),
		OutputColumns:          ctx.OutputColumns,
	}

	return newCtx
}

func DeepCopyPostDmlCtx(ctx *plan.PostDmlCtx) *plan.PostDmlCtx {
	if ctx == nil {
		return nil
	}
	return proto.Clone(ctx).(*plan.PostDmlCtx)
}

func DeepCopyLockTarget(target *plan.LockTarget) *plan.LockTarget {
	if target == nil {
		return nil
	}
	return &plan.LockTarget{
		TableId:              target.TableId,
		ObjRef:               DeepCopyObjectRef(target.ObjRef),
		PrimaryColIdxInBat:   target.PrimaryColIdxInBat,
		PrimaryColTyp:        target.PrimaryColTyp,
		RefreshTsIdxInBat:    target.RefreshTsIdxInBat,
		FilterColIdxInBat:    target.FilterColIdxInBat,
		LockTable:            target.LockTable,
		Block:                target.Block,
		Mode:                 target.Mode,
		PrimaryColRelPos:     target.PrimaryColRelPos,
		FilterColRelPos:      target.FilterColRelPos,
		LockRows:             DeepCopyExpr(target.LockRows),
		LockTableAtTheEnd:    target.LockTableAtTheEnd,
		PartitionColIdxInBat: target.PartitionColIdxInBat,
		HasPartitionCol:      target.HasPartitionCol,
	}
}

func DeepCopyDedupJoinCtx(ctx *plan.DedupJoinCtx) *plan.DedupJoinCtx {
	if ctx == nil {
		return nil
	}
	newCtx := &plan.DedupJoinCtx{
		OldColList:         slices.Clone(ctx.OldColList),
		UpdateColIdxList:   slices.Clone(ctx.UpdateColIdxList),
		UpdateColExprList:  DeepCopyExprList(ctx.UpdateColExprList),
		OldColCaptureList:  slices.Clone(ctx.OldColCaptureList),
		DedupBuildKeepLast: ctx.DedupBuildKeepLast,
	}

	return newCtx
}

func DeepCopyRankOption(opt *plan.RankOption) *plan.RankOption {
	if opt == nil {
		return nil
	}
	return &plan.RankOption{
		Mode: opt.Mode,
	}
}

func DeepCopyNode(node *plan.Node) *plan.Node {
	newNode := &Node{
		NodeType:        node.NodeType,
		NodeId:          node.NodeId,
		ExtraOptions:    node.ExtraOptions,
		Children:        slices.Clone(node.Children),
		JoinType:        node.JoinType,
		IsRightJoin:     node.IsRightJoin,
		AsofRightCol:    node.AsofRightCol,
		BindingTags:     slices.Clone(node.BindingTags),
		Limit:           DeepCopyExpr(node.Limit),
		Offset:          DeepCopyExpr(node.Offset),
		ProjectList:     DeepCopyExprList(node.ProjectList),
		OnList:          DeepCopyExprList(node.OnList),
		FilterList:      DeepCopyExprList(node.FilterList),
		BlockFilterList: DeepCopyExprList(node.BlockFilterList),
		GroupBy:         DeepCopyExprList(node.GroupBy),
		GroupingFlag:    slices.Clone(node.GroupingFlag),
		GroupByHashKey:  slices.Clone(node.GroupByHashKey),
		PhysicalEqualityKeyList: DeepCopyExprList(
			node.PhysicalEqualityKeyList),
		AggList:      DeepCopyExprList(node.AggList),
		OrderBy:      DeepCopyOrderBySpecList(node.OrderBy),
		Interval:     DeepCopyExpr(node.Interval),
		Sliding:      DeepCopyExpr(node.Sliding),
		Timestamp:    DeepCopyExpr(node.Timestamp),
		WEnd:         DeepCopyExpr(node.WEnd),
		FillType:     node.FillType,
		FillVal:      DeepCopyExprList(node.FillVal),
		GapFillMode:  node.GapFillMode,
		GapFillStart: DeepCopyExpr(node.GapFillStart),
		GapFillEnd:   DeepCopyExpr(node.GapFillEnd),

		TimeWindowPartitionBy:     DeepCopyExprList(node.TimeWindowPartitionBy),
		TimeWindowPartitionColPos: slices.Clone(node.TimeWindowPartitionColPos),
		FuzzyBuildSide:            node.FuzzyBuildSide,

		DeleteCtx:              DeepCopyDeleteCtx(node.DeleteCtx),
		TblFuncExprList:        DeepCopyExprList(node.TblFuncExprList),
		ClusterTable:           DeepCopyClusterTable(node.GetClusterTable()),
		InsertCtx:              DeepCopyInsertCtx(node.InsertCtx),
		NotCacheable:           node.NotCacheable,
		SourceStep:             slices.Clone(node.SourceStep),
		PreInsertCtx:           DeepCopyPreInsertCtx(node.PreInsertCtx),
		PreInsertUkCtx:         DeepCopyPreInsertUkCtx(node.PreInsertUkCtx),
		PreInsertSkCtx:         DeepCopyPreInsertUkCtx(node.PreInsertSkCtx),
		LockTargets:            make([]*plan.LockTarget, len(node.LockTargets)),
		AnalyzeInfo:            DeepCopyAnalyzeInfo(node.AnalyzeInfo),
		IsEnd:                  node.IsEnd,
		RecursiveSink:          node.RecursiveSink,
		ExternScan:             deepCopyExternScan(node.ExternScan),
		SampleFunc:             DeepCopySampleFuncSpec(node.SampleFunc),
		OnUpdateExprs:          DeepCopyExprList(node.OnUpdateExprs),
		DedupColName:           node.DedupColName,
		DedupColTypes:          slices.Clone(node.DedupColTypes),
		UpdateCtxList:          DeepCopyUpdateCtxList(node.UpdateCtxList),
		DedupJoinCtx:           DeepCopyDedupJoinCtx(node.DedupJoinCtx),
		IndexReaderParam:       DeepCopyIndexReaderParam(node.IndexReaderParam),
		ScanSnapshot:           DeepCopySnapshot(node.ScanSnapshot),
		VectorIndexScan:        DeepCopyVectorIndexScan(node.VectorIndexScan),
		OriginViews:            slices.Clone(node.OriginViews),
		DirectView:             node.DirectView,
		RankOption:             DeepCopyRankOption(node.RankOption),
		WindowIdx:              node.WindowIdx,
		RecursiveCte:           node.RecursiveCte,
		ApplyType:              node.ApplyType,
		PostDmlCtx:             DeepCopyPostDmlCtx(node.PostDmlCtx),
		OnDuplicateAction:      node.OnDuplicateAction,
		RollupFilter:           node.RollupFilter,
		RecursiveUnionDistinct: node.RecursiveUnionDistinct,
		FilterIsBarrier:        node.FilterIsBarrier,
		PartitionByCount:       node.PartitionByCount,
		PartitionAlgorithm:     node.PartitionAlgorithm,
		DedupInputKeysUnique:   node.DedupInputKeysUnique,
		EmitCompressedRowCount: node.EmitCompressedRowCount,
		SpillMem:               node.SpillMem,
		RuntimeFilterProbeList: DeepCopyRuntimeFilterSpecList(
			node.RuntimeFilterProbeList),
		RuntimeFilterBuildList: DeepCopyRuntimeFilterSpecList(
			node.RuntimeFilterBuildList),
		IfInsertFromUnique: node.IfInsertFromUnique,
		// Runtime execution plans are deep-copied before prepared parameters
		// are specialized.  Join compilation relies on these message headers
		// to recover the JoinMap tag; dropping them makes the copied plan panic
		// with "wrong joinmap tag".
		SendMsgList: slices.Clone(node.SendMsgList),
		RecvMsgList: slices.Clone(node.RecvMsgList),
	}
	if node.Fuzzymessage != nil {
		newNode.Fuzzymessage = &plan.OriginTableMessageForFuzzy{
			ParentTableName: node.Fuzzymessage.ParentTableName,
			ParentUniqueCols: DeepCopyColDefList(
				node.Fuzzymessage.ParentUniqueCols),
		}
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

func deepCopyExternScan(scan *plan.ExternScan) *plan.ExternScan {
	if scan == nil {
		return nil
	}
	return proto.Clone(scan).(*plan.ExternScan)
}

func DeepCopyIndexReaderParam(oldParam *plan.IndexReaderParam) *plan.IndexReaderParam {
	if oldParam == nil {
		return nil
	}

	ret := &plan.IndexReaderParam{
		OrderBy:        DeepCopyOrderBySpecList(oldParam.OrderBy),
		Limit:          DeepCopyExpr(oldParam.Limit),
		OrigFuncName:   oldParam.OrigFuncName,
		PartitionCnCnt: oldParam.PartitionCnCnt,
		PartitionCnIdx: oldParam.PartitionCnIdx,
		OverFetchLimit: oldParam.OverFetchLimit,
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

func DeepCopyDistRange(old *plan.DistRange) *plan.DistRange {
	if old == nil {
		return nil
	}
	return &plan.DistRange{
		LowerBoundType: old.LowerBoundType,
		UpperBoundType: old.UpperBoundType,
		LowerBound:     DeepCopyExpr(old.LowerBound),
		UpperBound:     DeepCopyExpr(old.UpperBound),
	}
}

func DeepCopyVectorIndexScan(old *plan.VectorIndexScan) *plan.VectorIndexScan {
	if old == nil {
		return nil
	}
	hidden := make([]*plan.VectorIndexTableRef, len(old.HiddenTables))
	for i, table := range old.HiddenTables {
		if table == nil {
			continue
		}
		hidden[i] = &plan.VectorIndexTableRef{
			Role:   table.Role,
			Object: DeepCopyObjectRef(table.Object),
			Table:  DeepCopyTableDef(table.Table, true),
		}
	}
	return &plan.VectorIndexScan{
		SourceTable:         DeepCopyObjectRef(old.SourceTable),
		SourceTableDef:      DeepCopyTableDef(old.SourceTableDef, true),
		Index:               DeepCopyIndexDef(old.Index),
		HiddenTables:        hidden,
		QueryVector:         DeepCopyExpr(old.QueryVector),
		DistanceFunction:    old.DistanceFunction,
		Direction:           old.Direction,
		CandidateLimit:      DeepCopyExpr(old.CandidateLimit),
		DistanceRange:       DeepCopyDistRange(old.DistanceRange),
		PreFilters:          DeepCopyExprList(old.PreFilters),
		IncludedColumns:     slices.Clone(old.IncludedColumns),
		InitialProbeCount:   old.InitialProbeCount,
		FirstRoundLimit:     DeepCopyExpr(old.FirstRoundLimit),
		BucketExpandStep:    old.BucketExpandStep,
		ThreadsSearch:       old.ThreadsSearch,
		ScanSnapshot:        DeepCopySnapshot(old.ScanSnapshot),
		PostFilterOverFetch: old.PostFilterOverFetch,
	}
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
		Table:       typ.Table,
		Enumvalues:  typ.Enumvalues,
		Charset:     typ.Charset,
		PadSpace:    typ.PadSpace,
	}
}

func DeepCopyColDef(col *plan.ColDef) *plan.ColDef {
	if col == nil {
		return nil
	}
	return &plan.ColDef{
		ColId:         col.ColId,
		Name:          col.Name,
		OriginName:    col.OriginName,
		Alg:           col.Alg,
		Typ:           col.Typ,
		Default:       DeepCopyDefault(col.Default),
		Primary:       col.Primary,
		Unique:        col.Unique,
		Pkidx:         col.Pkidx,
		Comment:       col.Comment,
		OnUpdate:      DeepCopyOnUpdate(col.OnUpdate),
		GeneratedCol:  DeepCopyGeneratedCol(col.GeneratedCol),
		ClusterBy:     col.ClusterBy,
		Hidden:        col.Hidden,
		Seqnum:        col.Seqnum,
		TblName:       col.TblName,
		OriginTblName: col.OriginTblName,
		DbName:        col.DbName,
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
		IncludedColumns:    slices.Clone(indexDef.IncludedColumns),
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
		Visibility:       indexOption.Visibility,
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

func DeepCopyGeneratedCol(old *plan.GeneratedCol) *plan.GeneratedCol {
	if old == nil {
		return nil
	}
	return &plan.GeneratedCol{
		Expr:         DeepCopyExpr(old.Expr),
		OriginString: old.OriginString,
		IsStored:     old.IsStored,
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

// CloneTableDefForPlan returns a planner-owned TableDef shell. When withCols is
// true, the Cols slice can also be changed without modifying table. Column
// definitions and all other schema metadata are shared and must be treated as
// immutable; callers that change nested schema objects must use DeepCopyTableDef.
func CloneTableDefForPlan(table *plan.TableDef, withCols bool) *plan.TableDef {
	if table == nil {
		return nil
	}

	cloned := *table
	if withCols {
		cloned.Cols = slices.Clone(table.Cols)
	} else {
		cloned.Cols = nil
	}
	return &cloned
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
		AutoIncrEpoch:  table.AutoIncrEpoch,
		DefaultCharset: table.DefaultCharset,
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
			Name:      col.Name,
			Check:     DeepCopyExpr(col.Check),
			OriginSql: col.OriginSql,
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
			Name:              table.TblFunc.Name,
			Param:             slices.Clone(table.TblFunc.Param),
			FulltextSourceRef: DeepCopyObjectRef(table.TblFunc.FulltextSourceRef),
			FulltextIndexRef:  DeepCopyObjectRef(table.TblFunc.FulltextIndexRef),
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
	backgroundQueries := make([]*plan.Query, len(qry.BackgroundQueries))
	for idx, query := range qry.BackgroundQueries {
		if query != nil {
			backgroundQueries[idx] = DeepCopyQuery(query)
		}
	}
	newQry := &plan.Query{
		StmtType:            qry.StmtType,
		Steps:               slices.Clone(qry.Steps),
		Nodes:               make([]*plan.Node, len(qry.Nodes)),
		Params:              DeepCopyExprList(qry.Params),
		Headings:            slices.Clone(qry.Headings),
		LoadTag:             qry.LoadTag,
		LoadWriteS3:         qry.LoadWriteS3,
		BackgroundQueries:   backgroundQueries,
		MaxDop:              qry.MaxDop,
		HasForeignKeyAction: qry.HasForeignKeyAction,
		HasReturning:        qry.HasReturning,
		ReturningStep:       qry.ReturningStep,
		ApplySqlSelectLimit: qry.ApplySqlSelectLimit,
		DetectSqls:          slices.Clone(qry.DetectSqls),
		CatalogDependencies: make([]*plan.ObjectRef, len(qry.CatalogDependencies)),
	}
	for idx, node := range qry.Nodes {
		newQry.Nodes[idx] = DeepCopyNode(node)
	}
	for idx, dependency := range qry.CatalogDependencies {
		newQry.CatalogDependencies[idx] = DeepCopyObjectRef(dependency)
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

	case *plan.Plan_Dcl:
		return &Plan{
			Plan: &plan.Plan_Dcl{
				Dcl: proto.Clone(p.Dcl).(*plan.DataControl),
			},
			IsPrepare:   pl.IsPrepare,
			TryRunTimes: pl.TryRunTimes,
		}

	default:
		// Only executable query, DDL, and SET-variable plans are supported.
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
		newDf.Definition = &plan.DataDefinition_CreateTable{
			CreateTable: DeepCopyCreateTable(df.CreateTable),
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
			if action == nil {
				continue
			}
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
			case *plan.AlterTable_Action_AlterAutoIncrement:
				AlterTable.Actions[i] = &plan.AlterTable_Action{
					Action: &plan.AlterTable_Action_AlterAutoIncrement{
						AlterAutoIncrement: &plan.AlterTableAutoIncrement{
							NewOffset: act.AlterAutoIncrement.NewOffset,
						},
					},
				}
			}
		}

		newDf.Definition = &plan.DataDefinition_AlterTable{
			AlterTable: AlterTable,
		}

	case *plan.DataDefinition_DropTable:
		newDf.Definition = &plan.DataDefinition_DropTable{
			DropTable: DeepCopyDropTable(df.DropTable),
		}

	case *plan.DataDefinition_CreateIndex:
		newDf.Definition = &plan.DataDefinition_CreateIndex{
			CreateIndex: &plan.CreateIndex{
				Database:              df.CreateIndex.Database,
				Table:                 df.CreateIndex.Table,
				TableDef:              DeepCopyTableDef(df.CreateIndex.TableDef, true),
				Index:                 DeepCopyCreateTable(df.CreateIndex.Index),
				OriginTablePrimaryKey: df.CreateIndex.OriginTablePrimaryKey,
				TableExist:            df.CreateIndex.TableExist,
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

// DeepCopyCreateTable clones the complete CreateTable message, including the
// execution-only fields used after the table metadata is created. In
// particular, CTAS execution depends on CreateAsSelectSql, while foreign-key
// bookkeeping and partition execution depend on the other fields that used to
// be omitted by the hand-written subset copy.
func DeepCopyCreateTable(src *plan.CreateTable) *plan.CreateTable {
	if src == nil {
		return nil
	}
	return proto.Clone(src).(*plan.CreateTable)
}

func DeepCopyFkey(fkey *ForeignKeyDef) *ForeignKeyDef {
	def := &ForeignKeyDef{
		Name:                fkey.Name,
		Cols:                slices.Clone(fkey.Cols),
		ForeignTbl:          fkey.ForeignTbl,
		ForeignCols:         slices.Clone(fkey.ForeignCols),
		OnDelete:            fkey.OnDelete,
		OnUpdate:            fkey.OnUpdate,
		ReferencedIndexName: fkey.ReferencedIndexName,
		OnDeleteOrigin:      fkey.OnDeleteOrigin,
		OnUpdateOrigin:      fkey.OnUpdateOrigin,
	}
	return def
}

func DeepCopyRuntimeFilterSpec(rf *plan.RuntimeFilterSpec) *plan.RuntimeFilterSpec {
	if rf == nil {
		return nil
	}
	return &plan.RuntimeFilterSpec{
		Tag:                 rf.Tag,
		MatchPrefix:         rf.MatchPrefix,
		UpperLimit:          rf.UpperLimit,
		Expr:                DeepCopyExpr(rf.Expr),
		BuildExpr:           DeepCopyExpr(rf.BuildExpr),
		NotOnPk:             rf.NotOnPk,
		UseMembershipFilter: rf.UseMembershipFilter,
		KeyEncoding:         rf.KeyEncoding,
		ProbeType:           DeepCopyType(rf.ProbeType),
		ScalarPredicate:     rf.ScalarPredicate,
		KeyComponentProbeTypes: slices.Clone(
			rf.KeyComponentProbeTypes,
		),
	}
}

func DeepCopyRuntimeFilterSpecList(
	specs []*plan.RuntimeFilterSpec,
) []*plan.RuntimeFilterSpec {
	if specs == nil {
		return nil
	}
	cloned := make([]*plan.RuntimeFilterSpec, len(specs))
	for i := range specs {
		cloned[i] = DeepCopyRuntimeFilterSpec(specs[i])
	}
	return cloned
}

func DeepCopyExpr(expr *Expr) *Expr {
	if expr == nil {
		return nil
	}
	newExpr := &Expr{
		Typ:             expr.Typ,
		Ndv:             expr.Ndv,
		Selectivity:     expr.Selectivity,
		PreparedNumeric: copyPreparedNumericMetadata(expr.PreparedNumeric),
	}
	// Positive AuxId values belong to later execution/zonemap numbering and
	// intentionally remain reset across a semantic deep copy.  Prepared numeric
	// fallback provenance is copied through its sparse plan metadata above; it
	// must not be encoded in AuxId because negative ids are executor memo keys.
	if expr.AuxId < 0 {
		newExpr.AuxId = expr.AuxId
	}

	switch item := expr.Expr.(type) {
	case *plan.Expr_Lit:
		pc := &plan.Literal{
			Isnull:       item.Lit.GetIsnull(),
			IsBin:        item.Lit.GetIsBin(),
			Src:          DeepCopyExpr(item.Lit.Src),
			IsSerialized: item.Lit.GetIsSerialized(),
			LiteralForm:  item.Lit.GetLiteralForm(),
			StringSource: item.Lit.GetStringSource(),
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
				Func:               DeepCopyObjectRef(item.F.Func),
				Args:               newArgs,
				AggConfig:          bytes.Clone(item.F.AggConfig),
				AggConfigType:      item.F.AggConfigType,
				SyntaxExplicitCast: item.F.SyntaxExplicitCast,
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
				Len:          item.Vec.Len,
				Data:         bytes.Clone(item.Vec.Data),
				IsSerialized: item.Vec.IsSerialized,
				StringSource: item.Vec.StringSource,
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

func DeepCopyDropTable(src *plan.DropTable) *plan.DropTable {
	if src == nil {
		return nil
	}
	dst := &plan.DropTable{
		IfExists:             src.IfExists,
		Database:             src.Database,
		Table:                src.Table,
		IndexTableNames:      slices.Clone(src.GetIndexTableNames()),
		ClusterTable:         DeepCopyClusterTable(src.GetClusterTable()),
		TableId:              src.GetTableId(),
		ForeignTbl:           slices.Clone(src.GetForeignTbl()),
		IsView:               src.IsView,
		TableDef:             DeepCopyTableDef(src.GetTableDef(), true),
		UpdateFkSqls:         slices.Clone(src.GetUpdateFkSqls()),
		FkChildTblsReferToMe: slices.Clone(src.GetFkChildTblsReferToMe()),
	}
	if len(src.GetTables()) > 0 {
		dst.Tables = make([]*plan.DropTable, len(src.GetTables()))
		for i, t := range src.GetTables() {
			dst.Tables[i] = DeepCopyDropTable(t)
		}
	}
	return dst
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
