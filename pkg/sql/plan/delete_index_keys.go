// Copyright 2026 Matrix Origin
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
	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type deleteIndexMaintenance struct {
	sourceStep int32
	tableDef   *plan.TableDef
	objRef     *plan.ObjectRef
	indexes    []*plan.IndexDef
	positions  map[string]int
}

// shareDeleteIndexSource places the common old-row image after the base and
// unique-index locks. Each ordinary index can then delete all its own physical
// addresses without multiplying the base-table mutation.
func (builder *QueryBuilder) shareDeleteIndexSource(
	bindCtx *BindContext, inputID, sourceTag int32, source *plan.Node, tableDef *plan.TableDef,
	colPos map[string]int32, indexes []*plan.IndexDef, mutation *plan.Node,
) (int32, int32, map[string]int32, error) {
	needed := map[string]bool{tableDef.Pkey.PkeyColName: true, catalog.Row_ID: true}
	if tableDef.Partition != nil {
		needed[getPartitionColName(tableDef.Partition.PartitionDefs[0].Def)] = true
	}
	for _, index := range indexes {
		if !isSpatialIndexDef(index) {
			for _, part := range index.Parts {
				needed[catalog.ResolveAlias(part)] = true
			}
		}
	}
	tag := builder.genNewBindTag()
	positions := make(map[string]int32, len(needed))
	refs := make(map[[2]int32]int32)
	projection := make([]*plan.Expr, 0, len(needed)+2*len(mutation.UpdateCtxList))
	for _, col := range tableDef.Cols {
		if !needed[col.Name] {
			continue
		}
		pos, ok := colPos[col.Name]
		if !ok || pos < 0 || int(pos) >= len(source.ProjectList) {
			return 0, 0, nil, moerr.NewInternalErrorf(builder.GetContext(), "DELETE cannot locate source column %s", col.Name)
		}
		newPos := int32(len(projection))
		positions[col.Name] = newPos
		refs[[2]int32{sourceTag, pos}] = newPos
		projection = append(projection, GetColExpr(source.ProjectList[pos].Typ, sourceTag, pos))
	}
	for name := range needed {
		if _, ok := positions[name]; !ok {
			return 0, 0, nil, moerr.NewInternalErrorf(builder.GetContext(), "DELETE cannot project source column %s", name)
		}
	}
	for _, update := range mutation.UpdateCtxList {
		for i := range update.DeleteCols {
			ref := &update.DeleteCols[i]
			old := [2]int32{ref.RelPos, ref.ColPos}
			pos, ok := refs[old]
			if !ok {
				if ref.ColPos < 0 || int(ref.ColPos) >= len(update.TableDef.Cols) {
					return 0, 0, nil, moerr.NewInternalError(builder.GetContext(), "DELETE cannot project index address")
				}
				pos = int32(len(projection))
				refs[old] = pos
				projection = append(projection, GetColExpr(update.TableDef.Cols[ref.ColPos].Typ, ref.RelPos, ref.ColPos))
			}
			ref.RelPos, ref.ColPos = tag, pos
		}
		for i := range update.PartitionCols {
			ref := &update.PartitionCols[i]
			pos, ok := refs[[2]int32{ref.RelPos, ref.ColPos}]
			if !ok {
				return 0, 0, nil, moerr.NewInternalError(builder.GetContext(), "DELETE cannot project partition column")
			}
			ref.RelPos, ref.ColPos = tag, pos
		}
	}
	projectID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT, Children: []int32{inputID}, BindingTags: []int32{tag}, ProjectList: projection,
	}, bindCtx)
	sinkID := appendSinkNodeWithTag(builder, bindCtx, projectID, tag)
	builder.keepSinkProjection(sinkID)
	step := builder.appendStep(sinkID)
	return builder.appendTaggedSinkScan(bindCtx, step, tag), step, positions, nil
}

func (builder *QueryBuilder) finishDeleteIndexMaintenance(query *plan.Query, bindCtx *BindContext, maint *deleteIndexMaintenance) error {
	if len(maint.indexes) == 0 {
		return builder.finishIrregularIndexMaintenance(query, bindCtx)
	}
	firstLateStep := len(query.Steps)
	delCtx := &dmlPlanCtx{tableDef: maint.tableDef, sourceStep: maint.sourceStep}
	for _, index := range maint.indexes {
		objRef, tableDef, err := builder.compCtx.ResolveIndexTableByRef(maint.objRef, index.IndexTableName, bindCtx.snapshot)
		if err != nil {
			return err
		}
		if tableDef == nil {
			return moerr.NewNoSuchTable(builder.GetContext(), maint.objRef.SchemaName, index.IndexTableName)
		}
		if err = builder.buildDeleteIndexStep(bindCtx, delCtx, index, objRef, tableDef, maint.positions, false, true); err != nil {
			return err
		}
	}
	if len(builder.irregularMaintIndexes) != 0 {
		return builder.finishIrregularIndexMaintenance(query, bindCtx)
	}
	// These steps only read existing sources. Preserve the already optimized
	// plan and its unique-key proofs while finalizing the new branches.
	lateSteps := query.Steps[firstLateStep:]
	for _, rootID := range lateSteps {
		ReCalcNodeStats(rootID, builder, true, false, true)
		builder.handleHashMapMessages(rootID)
	}
	builder.determineShuffleForDMLSteps(lateSteps)
	reCheckifNeedLockWholeTable(builder)
	return nil
}

// Older releases allowed ENUM primary keys. Their display values can be
// non-injective, so historical backfill owner bytes cannot always identify a
// unique base row. Keep the existing DELETE path for those catalog schemas.
func canBindStoredDeleteRows(tables []*plan.TableDef) bool {
	for _, table := range tables {
		if table == nil || table.Pkey == nil {
			return false
		}
		for _, col := range table.Cols {
			if !isEnumOrSetPlanType(&col.Typ) {
				continue
			}
			if col.Name == table.Pkey.PkeyColName {
				return false
			}
			for _, name := range table.Pkey.Names {
				if col.Name == name {
					return false
				}
			}
		}
	}
	return true
}

// needsDeleteIndexKeyVariants reports whether ADD INDEX could have written a
// display value where ordinary INSERT writes the stored ENUM or SET value.
func needsDeleteIndexKeyVariants(tableDef *plan.TableDef, indexDef *plan.IndexDef) bool {
	if !indexTableStoresSerializedKey(indexDef) || !catalog.IsRegularIndexAlgo(indexDef.IndexAlgo) {
		return false
	}
	for _, part := range indexDef.Parts {
		name := catalog.ResolveAlias(part)
		for _, col := range tableDef.Cols {
			if col.Name == name && isEnumOrSetPlanType(&col.Typ) {
				return true
			}
		}
	}
	return false
}

// deleteIndexKeys builds the physical key written by DML and, where needed,
// the display-value key written by ADD INDEX backfill.
func (builder *QueryBuilder) deleteIndexKeys(
	tableDef *plan.TableDef, indexDef *plan.IndexDef, colPos map[string]int32, sourceTag int32, historical bool,
) (*plan.Expr, *plan.Expr, error) {
	if isSpatialIndexDef(indexDef) {
		name := tableDef.Pkey.PkeyColName
		pos, ok := colPos[name]
		if !ok {
			return nil, nil, moerr.NewInternalErrorf(builder.GetContext(), "DELETE index cannot locate primary key %s", name)
		}
		for _, col := range tableDef.Cols {
			if col.Name == name {
				return GetColExpr(col.Typ, sourceTag, pos), nil, nil
			}
		}
		return nil, nil, moerr.NewInternalErrorf(builder.GetContext(), "DELETE index has no primary key definition for %s", name)
	}
	prefixes, err := catalog.IndexPrefixLengthsFromParamsWithError(indexDef.IndexAlgoParams)
	if err != nil {
		return nil, nil, err
	}
	storedArgs := make([]*plan.Expr, len(indexDef.Parts))
	var displayArgs []*plan.Expr
	var allSpecialNull *plan.Expr
	specialPrefix := false
	if historical {
		displayArgs = make([]*plan.Expr, len(indexDef.Parts))
	}
	for i, part := range indexDef.Parts {
		name := catalog.ResolveAlias(part)
		pos, ok := colPos[name]
		if !ok {
			return nil, nil, moerr.NewInternalErrorf(builder.GetContext(), "DELETE index cannot locate column %s", name)
		}
		var col *plan.ColDef
		for _, candidate := range tableDef.Cols {
			if candidate.Name == name {
				col = candidate
				break
			}
		}
		if col == nil {
			return nil, nil, moerr.NewInternalErrorf(builder.GetContext(), "DELETE index has no definition for column %s", name)
		}
		stored := GetColExpr(col.Typ, sourceTag, pos)
		storedArgs[i], err = builder.makeIndexPartExprFromInputExpr(stored, name, prefixes)
		if err != nil {
			return nil, nil, err
		}
		if historical {
			display := DeepCopyExpr(stored)
			if isEnumOrSetPlanType(&col.Typ) {
				if prefixes[name] > 0 {
					specialPrefix = true
				}
				if !indexDef.Unique {
					isNull, bindErr := BindFuncExprImplByPlanExpr(builder.GetContext(), "is_null", []*plan.Expr{DeepCopyExpr(stored)})
					if bindErr != nil {
						return nil, nil, bindErr
					}
					if allSpecialNull == nil {
						allSpecialNull = isNull
					} else {
						allSpecialNull, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "and", []*plan.Expr{allSpecialNull, isNull})
						if err != nil {
							return nil, nil, err
						}
					}
				}
				display, err = makeEnumOrSetDisplayValue(builder.GetContext(), display)
				if err != nil {
					return nil, nil, err
				}
			}
			displayArgs[i], err = builder.makeIndexPartExprFromInputExpr(display, name, prefixes)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	if !indexTableStoresSerializedKey(indexDef) {
		return storedArgs[0], nil, nil
	}
	function := "serial_full"
	if indexDef.Unique {
		function = "serial"
	}
	stored, err := BindFuncExprImplByPlanExpr(builder.GetContext(), function, storedArgs)
	if err != nil {
		return nil, nil, err
	}
	if !historical {
		return stored, nil, nil
	}
	display, err := BindFuncExprImplByPlanExpr(builder.GetContext(), function, displayArgs)
	if err != nil {
		return nil, nil, err
	}
	if specialPrefix {
		display, err = builder.distinctHistoricalDeleteKey(stored, display)
	} else if !indexDef.Unique {
		// Non-NULL ENUM/SET components and their display strings carry
		// different tuple type tags. Only all-NULL special components can
		// produce the same serial_full key.
		display, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "case", []*plan.Expr{
			allSpecialNull, makePlan2NullConstExprWithType(), display,
		})
	}
	return stored, display, err
}

// deleteIndexOwner identifies the base primary key stored in the hidden table.
// Schemas with ENUM/SET primary-key parts stay on the existing DELETE path.
func (builder *QueryBuilder) deleteIndexOwner(
	tableDef *plan.TableDef, colPos map[string]int32, sourceTag int32,
) (*plan.Expr, error) {
	name := tableDef.Pkey.PkeyColName
	pos, ok := colPos[name]
	if !ok {
		return nil, moerr.NewInternalErrorf(builder.GetContext(), "DELETE index cannot locate primary key %s", name)
	}
	for _, col := range tableDef.Cols {
		if col.Name == name {
			return bindPrimaryKeyIdentityExpr(builder, GetColExpr(col.Typ, sourceTag, pos), col.Typ)
		}
	}
	return nil, moerr.NewInternalErrorf(builder.GetContext(), "DELETE index has no primary key definition for %s", name)
}

// distinctHistoricalDeleteKey prevents a duplicate lookup when both writers
// produced the same physical key for the same owner (notably NULL parts).
func (builder *QueryBuilder) distinctHistoricalDeleteKey(storedKey, historicalKey *plan.Expr) (*plan.Expr, error) {
	same, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "<=>", []*plan.Expr{DeepCopyExpr(storedKey), DeepCopyExpr(historicalKey)})
	if err != nil {
		return nil, err
	}
	return BindFuncExprImplByPlanExpr(builder.GetContext(), "case", []*plan.Expr{same, makePlan2NullConstExprWithType(), historicalKey})
}

// appendDeleteIndexVariantJoins resolves both physical keys of a unique index.
// Each key is unique, so neither lookup multiplies the target rows.
func (builder *QueryBuilder) appendDeleteIndexVariantJoins(
	bindCtx *BindContext, sourceID int32, sourceTag int32, tableDef *plan.TableDef,
	indexDef *plan.IndexDef, indexObjRef *plan.ObjectRef, indexTableDef *plan.TableDef,
	colPos map[string]int32,
) (int32, []*plan.Node, error) {
	storedKey, historicalKey, err := builder.deleteIndexKeys(tableDef, indexDef, colPos, sourceTag, true)
	if err != nil {
		return 0, nil, err
	}
	owner, err := builder.deleteIndexOwner(tableDef, colPos, sourceTag)
	if err != nil {
		return 0, nil, err
	}
	keyPos, keyOK := indexTableDef.Name2ColIndex[catalog.IndexTableIndexColName]
	ownerPos, ownerOK := indexTableDef.Name2ColIndex[catalog.IndexTablePrimaryColName]
	_, rowOK := indexTableDef.Name2ColIndex[catalog.Row_ID]
	if !keyOK || !ownerOK || !rowOK {
		return 0, nil, moerr.NewInternalError(builder.GetContext(), "DELETE index table lacks key, owner, or rowid")
	}
	keys := []*plan.Expr{storedKey, historicalKey}
	owners := []*plan.Expr{owner, DeepCopyExpr(owner)}
	scans := make([]*plan.Node, 0, len(keys))
	for i, key := range keys {
		tag := builder.genNewBindTag()
		builder.addNameByColRef(tag, indexTableDef)
		scan := &plan.Node{
			NodeType: plan.Node_TABLE_SCAN, TableDef: indexTableDef, ObjRef: indexObjRef,
			BindingTags: []int32{tag}, ScanSnapshot: bindCtx.snapshot,
		}
		scanID := builder.appendNode(scan, bindCtx)
		matchKey, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
			key, GetColExpr(indexTableDef.Cols[keyPos].Typ, tag, keyPos),
		})
		if err != nil {
			return 0, nil, err
		}
		hiddenOwner := GetColExpr(indexTableDef.Cols[ownerPos].Typ, tag, ownerPos)
		hiddenOwner, err = bindPrimaryKeyIdentityExpr(builder, hiddenOwner, hiddenOwner.Typ)
		if err != nil {
			return 0, nil, err
		}
		matchOwner, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "<=>", []*plan.Expr{owners[i], hiddenOwner})
		if err != nil {
			return 0, nil, err
		}
		sourceID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN, Children: []int32{sourceID, scanID}, JoinType: plan.Node_LEFT,
			OnList: []*plan.Expr{matchKey, matchOwner},
		}, bindCtx)
		scans = append(scans, scan)
	}
	return sourceID, scans, nil
}

// buildDeleteIndexStep deletes every owned physical index row through an
// independent stream. Historical and stored keys share one hidden scan.
func (builder *QueryBuilder) buildDeleteIndexStep(
	bindCtx *BindContext, delCtx *dmlPlanCtx, indexDef *plan.IndexDef,
	indexObjRef *plan.ObjectRef, indexTableDef *plan.TableDef, positions map[string]int,
	isUnique, isSecondary bool,
) error {
	if len(indexTableDef.Name2ColIndex) == 0 {
		indexTableDef.Name2ColIndex = make(map[string]int32, len(indexTableDef.Cols))
		for i, col := range indexTableDef.Cols {
			indexTableDef.Name2ColIndex[col.Name] = int32(i)
		}
	}
	colPos := make(map[string]int32, len(positions))
	for name, pos := range positions {
		colPos[name] = int32(pos)
	}
	sourceTag := builder.genNewBindTag()
	sourceID := builder.appendTaggedSinkScan(bindCtx, delCtx.sourceStep, sourceTag)
	hasHistorical := needsDeleteIndexKeyVariants(delCtx.tableDef, indexDef)
	storedKey, historicalKey, err := builder.deleteIndexKeys(delCtx.tableDef, indexDef, colPos, sourceTag, hasHistorical)
	if err != nil {
		return err
	}
	var owner *plan.Expr
	if !isSpatialIndexDef(indexDef) {
		owner, err = builder.deleteIndexOwner(delCtx.tableDef, colPos, sourceTag)
		if err != nil {
			return err
		}
	}
	key := storedKey
	candidateInput := sourceID
	if hasHistorical {
		modeTag := builder.genNewBindTag()
		zero, one := makePlan2Int32ConstExprWithType(0), makePlan2Int32ConstExprWithType(1)
		modeID, _ := uuid.NewV7()
		modeScan := builder.appendNode(&plan.Node{
			NodeType: plan.Node_VALUE_SCAN,
			TableDef: &plan.TableDef{Cols: []*plan.ColDef{{Name: "mode", Typ: one.Typ}}},
			RowsetData: &plan.RowsetData{RowCount: 2, Cols: []*plan.ColData{{Data: []*plan.RowsetExpr{
				{Expr: zero}, {Expr: one},
			}}}},
			BindingTags: []int32{modeTag}, Uuid: modeID[:],
		}, bindCtx)
		candidateInput = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN, JoinType: plan.Node_INNER, Children: []int32{sourceID, modeScan},
		}, bindCtx)
		isStored, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{GetColExpr(one.Typ, modeTag, 0), one})
		if err != nil {
			return err
		}
		key, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "case", []*plan.Expr{isStored, storedKey, historicalKey})
		if err != nil {
			return err
		}
	} else if !indexTableStoresSerializedKey(indexDef) {
		key, err = bindPrimaryKeyIdentityExpr(builder, key, key.Typ)
		if err != nil {
			return err
		}
	}
	candidateTag := builder.genNewBindTag()
	candidateProjection := []*plan.Expr{key}
	if owner != nil {
		candidateProjection = append(candidateProjection, owner)
	}
	candidates := builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT, Children: []int32{candidateInput}, BindingTags: []int32{candidateTag},
		ProjectList: candidateProjection,
	}, bindCtx)
	keyPos, keyOK := indexTableDef.Name2ColIndex[indexLookupColumnName(indexDef)]
	pkPos, pkOK := indexTableDef.Name2ColIndex[indexTableDef.Pkey.PkeyColName]
	ownerPos, ownerOK := indexTableDef.Name2ColIndex[catalog.IndexTablePrimaryColName]
	rowPos, rowOK := indexTableDef.Name2ColIndex[catalog.Row_ID]
	if !keyOK || !pkOK || !ownerOK || !rowOK {
		return moerr.NewInternalError(builder.GetContext(), "DELETE index table lacks key, owner, or rowid")
	}
	hiddenTag := builder.genNewBindTag()
	scan := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN, TableDef: indexTableDef, ObjRef: indexObjRef,
		BindingTags: []int32{hiddenTag}, ScanSnapshot: bindCtx.snapshot, Stats: &plan.Stats{},
	}
	scanID := builder.appendNode(scan, bindCtx)
	hiddenKey := GetColExpr(indexTableDef.Cols[keyPos].Typ, hiddenTag, keyPos)
	if !indexTableStoresSerializedKey(indexDef) {
		hiddenKey, err = bindPrimaryKeyIdentityExpr(builder, hiddenKey, hiddenKey.Typ)
		if err != nil {
			return err
		}
	}
	keyMatch, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{hiddenKey, GetColExpr(key.Typ, candidateTag, 0)})
	if err != nil {
		return err
	}
	conditions := []*plan.Expr{keyMatch}
	if owner != nil {
		hiddenOwner := GetColExpr(indexTableDef.Cols[ownerPos].Typ, hiddenTag, ownerPos)
		hiddenOwner, err = bindPrimaryKeyIdentityExpr(builder, hiddenOwner, hiddenOwner.Typ)
		if err != nil {
			return err
		}
		ownerMatch, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "<=>", []*plan.Expr{
			hiddenOwner, GetColExpr(owner.Typ, candidateTag, 1),
		})
		if err != nil {
			return err
		}
		conditions = append(conditions, ownerMatch)
	}
	join := &plan.Node{
		NodeType: plan.Node_JOIN, JoinType: plan.Node_INNER, Children: []int32{scanID, candidates},
		OnList: conditions,
	}
	joinID := builder.appendNode(join, bindCtx)
	outputTag := builder.genNewBindTag()
	outputID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT, Children: []int32{joinID}, BindingTags: []int32{outputTag},
		ProjectList: []*plan.Expr{GetColExpr(indexTableDef.Cols[rowPos].Typ, hiddenTag, rowPos), GetColExpr(indexTableDef.Cols[pkPos].Typ, hiddenTag, pkPos)},
	}, bindCtx)
	counts := map[[2]int32]int{{outputTag, 0}: 1, {outputTag, 1}: 1}
	remap, err := builder.remapAllColRefs(outputID, int32(len(builder.qry.Steps)), counts, make(map[[2]int32]bool), make(map[[2]int32]int))
	if err != nil {
		return err
	}
	// Legacy index-maintenance plans are appended after runtime-filter generation.
	if hiddenKey.GetCol() != nil {
		probe := &plan.Expr{Typ: indexTableDef.Cols[keyPos].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: indexLookupColumnName(indexDef)}}}
		build := GetColExpr(indexTableDef.Cols[keyPos].Typ, 0, 0)
		rfTag := builder.genNewMsgTag()
		probeSpec, buildSpec, hasFilter := builder.makeExactRuntimeFilterPair(
			rfTag, false, GetInFilterCardLimitOnPK(builder.compCtx.GetProcess().GetService(), scan.Stats.TableCnt), probe, build, false,
		)
		if hasFilter {
			scan.RuntimeFilterProbeList = []*plan.RuntimeFilterSpec{probeSpec}
			scan.Stats.ForceOneCN = true
			join.RuntimeFilterBuildList = []*plan.RuntimeFilterSpec{buildSpec}
		}
	}
	rowRef, keyRef := remap.globalToLocal[[2]int32{outputTag, 0}], remap.globalToLocal[[2]int32{outputTag, 1}]
	projectID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT, Children: []int32{outputID},
		ProjectList: []*plan.Expr{
			GetColExpr(indexTableDef.Cols[rowPos].Typ, rowRef[0], rowRef[1]),
			GetColExpr(indexTableDef.Cols[pkPos].Typ, keyRef[0], keyRef[1]),
		},
	}, bindCtx)
	delInfo := makeDeleteNodeInfo(builder.compCtx, indexObjRef, indexTableDef, 0, false, 1, indexTableDef.Cols[pkPos].Typ, delCtx.lockTable)
	deleteID, err := makeOneDeletePlan(builder, bindCtx, projectID, delInfo, isUnique, isSecondary, false)
	putDeleteNodeInfo(delInfo)
	if err != nil {
		return err
	}
	builder.appendStep(deleteID)
	return nil
}
