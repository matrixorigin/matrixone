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
	"context"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// applyTemporalIndexOnlyScan constructs an optional covering path while keeping
// every original predicate as a row-level residual. The caller is responsible
// for comparing its cost with the safe path, preserving the original query root
// for admission, and for per-execution placement/permission. Until those owners
// are connected this helper must not be used by index selection.
func (builder *QueryBuilder) applyTemporalIndexOnlyScan(scanID int32, index *plan.IndexDef, paramFree bool,
	prefixPositions []int, filterPos int, colMap map[[2]int32]*plan.Expr, cost *encodedRegularIndexCostContext,
) (int32, bool, error) {
	return builder.applyTemporalIndexOnlyScanResolved(scanID, index, paramFree, prefixPositions, filterPos, colMap, cost, nil)
}

type temporalResolvedIndex struct {
	ref    *plan.ObjectRef
	table  *plan.TableDef
	verify func() (bool, error)
}

func (builder *QueryBuilder) applyTemporalIndexOnlyScanResolved(scanID int32, index *plan.IndexDef, paramFree bool,
	prefixPositions []int, filterPos int, colMap map[[2]int32]*plan.Expr, cost *encodedRegularIndexCostContext, resolved *temporalResolvedIndex,
) (int32, bool, error) {
	if builder == nil || builder.qry == nil || scanID < 0 || int(scanID) >= len(builder.qry.Nodes) || colMap == nil || cost == nil {
		return scanID, false, nil
	}
	scan := builder.qry.Nodes[scanID]
	if scan == nil || scan.TableDef == nil || scan.ObjRef == nil || scan.Stats == nil || scan.TableDef.Pkey == nil || len(scan.BindingTags) == 0 ||
		cost.node != scan || builder.compCtx == nil || len(scan.ProjectList) != 0 || scan.NodeId != scanID || int(scan.NodeId) >= len(builder.ctxByNode) ||
		index == nil || !cost.indexCoversRequiredColumns(index) {
		return scanID, false, nil
	}
	if _, _, admitted, err := temporalIndexOperands(builder.GetContext(), builder.qry, scanID, paramFree, index, prefixPositions, filterPos); err != nil || !admitted {
		return scanID, false, err
	}
	tag := builder.genNewBindTag()
	snapshot := scan.ScanSnapshot
	if snapshot == nil {
		snapshot = &Snapshot{}
	}
	var objRef *plan.ObjectRef
	var table *plan.TableDef
	var err error
	if resolved != nil {
		objRef, table = resolved.ref, resolved.table
	} else {
		objRef, table, err = builder.compCtx.ResolveIndexTableByRef(scan.ObjRef, index.IndexTableName, snapshot)
		if err != nil {
			return scanID, false, err
		}
	}
	hint, admitted, err := temporalIndexHintWithPrefix(builder.GetContext(), builder.qry, scanID, paramFree,
		index, table, tag, prefixPositions, filterPos)
	if err != nil || !admitted {
		return scanID, false, err
	}
	for _, col := range table.Cols {
		if col == nil {
			return scanID, false, nil
		}
	}
	leading := GetColExpr(table.Cols[0].Typ, tag, 0)
	leading.GetCol().Name = table.Cols[0].Name
	localMap := make(map[[2]int32]*plan.Expr, len(index.Parts))
	for i, part := range index.Parts {
		name := catalog.ResolveAlias(part)
		pos := scan.TableDef.Name2ColIndex[name] // verified by temporalIndexHintWithPrefix
		key := [2]int32{scan.BindingTags[0], pos}
		switch {
		case len(index.Parts) == 1:
			localMap[key] = leading
		case name == scan.TableDef.Pkey.PkeyColName:
			if len(table.Cols) < 2 || table.Cols[1] == nil {
				return scanID, false, nil
			}
			localMap[key] = GetColExpr(table.Cols[1].Typ, tag, 1)
		default:
			localMap[key], err = MakeSerialExtractExpr(builder.GetContext(), DeepCopyExpr(leading), scan.TableDef.Cols[pos].Typ, int64(i))
			if err != nil {
				return scanID, false, err
			}
		}
	}
	residuals := make([]*plan.Expr, 0, len(scan.FilterList))
	for _, filter := range scan.FilterList {
		if !temporalIndexResidualColumnsMapped(filter, localMap) {
			return scanID, false, nil
		}
		residuals = append(residuals, replaceColumnsForExpr(DeepCopyExpr(filter), localMap))
	}
	verified := false
	if resolved != nil && resolved.verify != nil {
		ok, cause := resolved.verify()
		if cause != nil || !ok {
			return scanID, false, cause
		}
		verified = true
	}
	if cause := context.Cause(builder.GetContext()); cause != nil {
		return scanID, false, cause
	}
	for key, value := range localMap {
		colMap[key] = value
	}
	builder.addNameByColRef(tag, table)
	// A verified native pair needs the necessary candidate only for storage
	// pruning. Every returned row still evaluates the original SQL residual.
	// Legacy and unverified helpers retain the row candidate.
	filters := residuals
	if !verified {
		filters = append([]*plan.Expr{DeepCopyExpr(hint)}, residuals...)
	}
	id := builder.appendNode(&plan.Node{
		NodeType: plan.Node_TABLE_SCAN, TableDef: table, ObjRef: objRef, ParentObjRef: scan.ObjRef,
		IndexScanInfo: plan.IndexScanInfo{IsIndexScan: true, IndexName: index.IndexName, BelongToTable: scan.ObjRef.ObjName,
			Parts: slices.Clone(index.Parts), IsUnique: index.Unique, IndexTableName: index.IndexTableName},
		FilterList: filters, BlockFilterList: []*plan.Expr{hint}, BindingTags: []int32{tag}, ScanSnapshot: scan.ScanSnapshot,
	}, builder.ctxByNode[scan.NodeId])
	builder.inheritIndexHints(id, scan.NodeId)
	forceScanNodeStatsTP(id, builder)
	return id, true, nil
}

func temporalIndexResidualColumnsMapped(expr *plan.Expr, mapping map[[2]int32]*plan.Expr) bool {
	if expr == nil {
		return false
	}
	if col := expr.GetCol(); col != nil {
		return mapping[[2]int32{col.RelPos, col.ColPos}] != nil
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if !temporalIndexResidualColumnsMapped(arg, mapping) {
				return false
			}
		}
		return true
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if !temporalIndexResidualColumnsMapped(item, mapping) {
				return false
			}
		}
		return true
	}
	return expr.GetLit() != nil || expr.GetT() != nil
}

// applyTemporalIndexBackfill keeps the original scan and its complete residuals
// as the base side of an internal PK join. Its caller must separately compare
// the candidate's full cost and establish execution placement before enabling
// narrowing. It is not yet connected to access-path selection.
func (builder *QueryBuilder) applyTemporalIndexBackfill(scanID int32, index *plan.IndexDef, paramFree bool,
	prefixPositions []int, filterPos int,
) (int32, bool, error) {
	if builder == nil || builder.qry == nil || builder.compCtx == nil || scanID < 0 || int(scanID) >= len(builder.qry.Nodes) || index == nil {
		return scanID, false, nil
	}
	scan := builder.qry.Nodes[scanID]
	if scan == nil || scan.TableDef == nil || scan.TableDef.Pkey == nil || scan.ObjRef == nil || scan.Stats == nil || len(scan.BindingTags) == 0 ||
		scan.NodeId != scanID || int(scan.NodeId) >= len(builder.ctxByNode) {
		return scanID, false, nil
	}
	pkPos, ok := scan.TableDef.Name2ColIndex[scan.TableDef.Pkey.PkeyColName]
	if !ok || pkPos < 0 || int(pkPos) >= len(scan.TableDef.Cols) || scan.TableDef.Cols[pkPos] == nil {
		return scanID, false, nil
	}
	if _, _, admitted, err := temporalIndexOperands(builder.GetContext(), builder.qry, scanID, paramFree, index, prefixPositions, filterPos); err != nil || !admitted {
		return scanID, false, err
	}
	tag := builder.genNewBindTag()
	snapshot := scan.ScanSnapshot
	if snapshot == nil {
		snapshot = &Snapshot{}
	}
	objRef, table, err := builder.compCtx.ResolveIndexTableByRef(scan.ObjRef, index.IndexTableName, snapshot)
	if err != nil {
		return scanID, false, err
	}
	hint, admitted, err := temporalIndexHintWithPrefix(builder.GetContext(), builder.qry, scanID, paramFree,
		index, table, tag, prefixPositions, filterPos)
	if err != nil || !admitted {
		return scanID, false, err
	}
	if len(table.Cols) < 2 || table.Cols[1] == nil || !isSameColumnType(table.Cols[1].Typ, scan.TableDef.Cols[pkPos].Typ) ||
		!table.Cols[1].Typ.SameCollation(scan.TableDef.Cols[pkPos].Typ) {
		return scanID, false, nil
	}
	for _, col := range table.Cols {
		if col == nil {
			return scanID, false, nil
		}
	}
	pk := GetColExpr(scan.TableDef.Cols[pkPos].Typ, scan.BindingTags[0], pkPos)
	pk.GetCol().Name = scan.TableDef.Pkey.PkeyColName
	indexPK := GetColExpr(table.Cols[1].Typ, tag, 1)
	indexPK.GetCol().Name = table.Cols[1].Name
	joinCondition, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{pk, indexPK})
	if err != nil {
		return scanID, false, err
	}
	builder.addNameByColRef(tag, table)
	indexID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_TABLE_SCAN, TableDef: table, ObjRef: objRef, ParentObjRef: DeepCopyObjectRef(scan.ObjRef),
		IndexScanInfo: plan.IndexScanInfo{IsIndexScan: true, IndexName: index.IndexName, BelongToTable: scan.ObjRef.ObjName,
			Parts: slices.Clone(index.Parts), IsUnique: index.Unique, IndexTableName: index.IndexTableName},
		FilterList: []*plan.Expr{DeepCopyExpr(hint)}, BlockFilterList: []*plan.Expr{hint}, BindingTags: []int32{tag}, ScanSnapshot: scan.ScanSnapshot,
	}, builder.ctxByNode[scan.NodeId])
	builder.inheritIndexHints(indexID, scan.NodeId)
	forceScanNodeStatsTP(indexID, builder)
	joinID := builder.appendNode(&plan.Node{NodeType: plan.Node_JOIN, Children: []int32{scanID, indexID},
		JoinType: plan.Node_INDEX, OnList: []*plan.Expr{joinCondition}}, builder.ctxByNode[scan.NodeId])
	return joinID, true, nil
}

// TemporalPhysicalIndexScan identifies the marked covering scan emitted by
// the physical temporal scorer. It requires local placement for its cost
// model; the original residuals remain authoritative for SQL evaluation.
func TemporalPhysicalIndexScan(node *plan.Node) bool {
	if node == nil || node.NodeType != plan.Node_TABLE_SCAN || !node.IndexScanInfo.IsIndexScan {
		return false
	}
	for _, list := range [][]*plan.Expr{node.FilterList, node.BlockFilterList} {
		for _, e := range list {
			if e != nil && e.TemporalCandidateFilter {
				return true
			}
		}
	}
	return false
}

func HasTemporalPhysicalIndexScans(query *plan.Query) bool {
	if query == nil {
		return false
	}
	for _, node := range query.Nodes {
		if TemporalPhysicalIndexScan(node) {
			return true
		}
	}
	return false
}
