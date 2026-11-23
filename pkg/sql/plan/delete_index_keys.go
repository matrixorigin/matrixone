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

// deleteIndexKeyVariants builds the exact key forms written by regular DML
// and by ADD INDEX's INSERT ... SELECT backfill. These may coexist in one
// released index; both must be matched against the actual index row.
func (builder *QueryBuilder) deleteIndexKeyVariants(
	tableDef *plan.TableDef, indexDef *plan.IndexDef, colPos map[string]int32, sourceTag int32,
) (*plan.Expr, *plan.Expr, error) {
	prefixes, err := catalog.IndexPrefixLengthsFromParamsWithError(indexDef.IndexAlgoParams)
	if err != nil {
		return nil, nil, err
	}
	storedArgs := make([]*plan.Expr, len(indexDef.Parts))
	displayArgs := make([]*plan.Expr, len(indexDef.Parts))
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
		display := DeepCopyExpr(stored)
		if isEnumOrSetPlanType(&col.Typ) {
			display, err = makeEnumOrSetDisplayValue(builder.GetContext(), display)
			if err != nil {
				return nil, nil, err
			}
		}
		storedArgs[i], err = builder.makeIndexPartExprFromInputExpr(stored, name, prefixes)
		if err != nil {
			return nil, nil, err
		}
		displayArgs[i], err = builder.makeIndexPartExprFromInputExpr(display, name, prefixes)
		if err != nil {
			return nil, nil, err
		}
	}
	function := "serial_full"
	if indexDef.Unique {
		function = "serial"
	}
	stored, err := BindFuncExprImplByPlanExpr(builder.GetContext(), function, storedArgs)
	if err != nil {
		return nil, nil, err
	}
	display, err := BindFuncExprImplByPlanExpr(builder.GetContext(), function, displayArgs)
	return stored, display, err
}

// deleteIndexOwner is the typed primary key stored in the hidden table.
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
			return GetColExpr(col.Typ, sourceTag, pos), nil
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

// appendDeleteIndexVariantJoins resolves both physical keys on the same source
// stream. The PK of the hidden table is unique, so neither lookup multiplies
// the target rows; both actual index rowids remain available to the mutation.
func (builder *QueryBuilder) appendDeleteIndexVariantJoins(
	bindCtx *BindContext, sourceID int32, sourceTag int32, tableDef *plan.TableDef,
	indexDef *plan.IndexDef, indexObjRef *plan.ObjectRef, indexTableDef *plan.TableDef,
	colPos map[string]int32,
) (int32, []*plan.Node, error) {
	storedKey, historicalKey, err := builder.deleteIndexKeyVariants(tableDef, indexDef, colPos, sourceTag)
	if err != nil {
		return 0, nil, err
	}
	owner, err := builder.deleteIndexOwner(tableDef, colPos, sourceTag)
	if err != nil {
		return 0, nil, err
	}
	historicalKey, err = builder.distinctHistoricalDeleteKey(storedKey, historicalKey)
	if err != nil {
		return 0, nil, err
	}
	keyPos, keyOK := indexTableDef.Name2ColIndex[catalog.IndexTableIndexColName]
	ownerPos, ownerOK := indexTableDef.Name2ColIndex[catalog.IndexTablePrimaryColName]
	rowPos, rowOK := indexTableDef.Name2ColIndex[catalog.Row_ID]
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
		matchOwner, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "<=>", []*plan.Expr{
			owners[i], GetColExpr(indexTableDef.Cols[ownerPos].Typ, tag, ownerPos),
		})
		if err != nil {
			return 0, nil, err
		}
		sourceID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN, Children: []int32{sourceID, scanID}, JoinType: plan.Node_LEFT,
			OnList: []*plan.Expr{matchKey, matchOwner},
		}, bindCtx)
		scans = append(scans, scan)
	}
	if !indexDef.Unique {
		first, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "is_not_null", []*plan.Expr{
			GetColExpr(indexTableDef.Cols[rowPos].Typ, scans[0].BindingTags[0], rowPos),
		})
		if err != nil {
			return 0, nil, err
		}
		second, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "is_not_null", []*plan.Expr{
			GetColExpr(indexTableDef.Cols[rowPos].Typ, scans[1].BindingTags[0], rowPos),
		})
		if err != nil {
			return 0, nil, err
		}
		matched, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "or", []*plan.Expr{first, second})
		if err != nil {
			return 0, nil, err
		}
		sourceID = builder.appendNode(&plan.Node{NodeType: plan.Node_FILTER, Children: []int32{sourceID}, FilterList: []*plan.Expr{matched}}, bindCtx)
	}
	return sourceID, scans, nil
}

// buildDeleteIndexVariantsStep is the legacy DELETE's independent index
// maintenance step. Both candidate keys share one hidden-index scan and every
// matched index row is deleted, including two keys for the same base row.
func (builder *QueryBuilder) buildDeleteIndexVariantsStep(
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
	storedKey, historicalKey, err := builder.deleteIndexKeyVariants(delCtx.tableDef, indexDef, colPos, sourceTag)
	if err != nil {
		return err
	}
	owner, err := builder.deleteIndexOwner(delCtx.tableDef, colPos, sourceTag)
	if err != nil {
		return err
	}
	historicalKey, err = builder.distinctHistoricalDeleteKey(storedKey, historicalKey)
	if err != nil {
		return err
	}
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
	crossID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN, JoinType: plan.Node_INNER, Children: []int32{sourceID, modeScan},
	}, bindCtx)
	isStored, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{GetColExpr(one.Typ, modeTag, 0), one})
	if err != nil {
		return err
	}
	key, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "case", []*plan.Expr{isStored, storedKey, historicalKey})
	if err != nil {
		return err
	}
	candidateTag := builder.genNewBindTag()
	candidates := builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT, Children: []int32{crossID}, BindingTags: []int32{candidateTag},
		ProjectList: []*plan.Expr{key, owner},
	}, bindCtx)
	keyPos, keyOK := indexTableDef.Name2ColIndex[catalog.IndexTableIndexColName]
	ownerPos, ownerOK := indexTableDef.Name2ColIndex[catalog.IndexTablePrimaryColName]
	rowPos, rowOK := indexTableDef.Name2ColIndex[catalog.Row_ID]
	if !keyOK || !ownerOK || !rowOK {
		return moerr.NewInternalError(builder.GetContext(), "DELETE index table lacks key, owner, or rowid")
	}
	hiddenTag := builder.genNewBindTag()
	scan := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN, TableDef: indexTableDef, ObjRef: indexObjRef,
		BindingTags: []int32{hiddenTag}, ScanSnapshot: bindCtx.snapshot, Stats: &plan.Stats{},
	}
	scanID := builder.appendNode(scan, bindCtx)
	hiddenKey := GetColExpr(indexTableDef.Cols[keyPos].Typ, hiddenTag, keyPos)
	keyMatch, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{hiddenKey, GetColExpr(key.Typ, candidateTag, 0)})
	if err != nil {
		return err
	}
	ownerMatch, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "<=>", []*plan.Expr{
		GetColExpr(indexTableDef.Cols[ownerPos].Typ, hiddenTag, ownerPos), GetColExpr(owner.Typ, candidateTag, 1),
	})
	if err != nil {
		return err
	}
	join := &plan.Node{
		NodeType: plan.Node_JOIN, JoinType: plan.Node_INNER, Children: []int32{scanID, candidates},
		OnList: []*plan.Expr{keyMatch, ownerMatch},
	}
	joinID := builder.appendNode(join, bindCtx)
	outputTag := builder.genNewBindTag()
	outputID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT, Children: []int32{joinID}, BindingTags: []int32{outputTag},
		ProjectList: []*plan.Expr{GetColExpr(indexTableDef.Cols[rowPos].Typ, hiddenTag, rowPos), DeepCopyExpr(hiddenKey)},
	}, bindCtx)
	counts := map[[2]int32]int{{outputTag, 0}: 1, {outputTag, 1}: 1}
	remap, err := builder.remapAllColRefs(outputID, int32(len(builder.qry.Steps)), counts, make(map[[2]int32]bool), make(map[[2]int32]int))
	if err != nil {
		return err
	}
	// Legacy index-maintenance plans are appended after runtime-filter generation.
	probe := &plan.Expr{Typ: indexTableDef.Cols[keyPos].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: indexTableDef.Pkey.PkeyColName}}}
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
	rowRef, keyRef := remap.globalToLocal[[2]int32{outputTag, 0}], remap.globalToLocal[[2]int32{outputTag, 1}]
	projectID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT, Children: []int32{outputID},
		ProjectList: []*plan.Expr{
			GetColExpr(indexTableDef.Cols[rowPos].Typ, rowRef[0], rowRef[1]),
			GetColExpr(indexTableDef.Cols[keyPos].Typ, keyRef[0], keyRef[1]),
		},
	}, bindCtx)
	delInfo := makeDeleteNodeInfo(builder.compCtx, indexObjRef, indexTableDef, 0, false, 1, indexTableDef.Cols[keyPos].Typ, delCtx.lockTable)
	deleteID, err := makeOneDeletePlan(builder, bindCtx, projectID, delInfo, isUnique, isSecondary, false)
	putDeleteNodeInfo(delInfo)
	if err != nil {
		return err
	}
	builder.appendStep(deleteID)
	return nil
}
