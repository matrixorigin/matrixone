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
	"math"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type temporalResolvedIndex struct {
	ref    *plan.ObjectRef
	table  *plan.TableDef
	verify func() (bool, error)
}

func (builder *QueryBuilder) applyTemporalIndexOnlyScanResolved(scanID int32, index *plan.IndexDef, colMap map[[2]int32]*plan.Expr, cost *encodedRegularIndexCostContext, resolved *temporalResolvedIndex,
) (int32, bool, error) {
	if builder == nil || builder.qry == nil || scanID < 0 || int(scanID) >= len(builder.qry.Nodes) || colMap == nil || cost == nil || resolved == nil || resolved.verify == nil || resolved.ref == nil || resolved.table == nil {
		return scanID, false, nil
	}
	scan := builder.qry.Nodes[scanID]
	if scan == nil || scan.TableDef == nil || scan.ObjRef == nil || scan.Stats == nil || scan.TableDef.Pkey == nil || len(scan.BindingTags) == 0 ||
		cost.node != scan || builder.compCtx == nil || len(scan.ProjectList) != 0 || scan.NodeId != scanID || int(scan.NodeId) >= len(builder.ctxByNode) ||
		index == nil || !cost.indexCoversRequiredColumns(index) {
		return scanID, false, nil
	}
	atom, admitted, err := temporalIndexOperands(builder.GetContext(), builder.qry, scanID, index, 0)
	if err != nil || !admitted {
		return scanID, false, err
	}
	if builder.nextBindTag == math.MaxInt32 {
		return scanID, false, nil
	}
	// Reserve locally; failed construction or verification publishes no tag.
	tag := builder.nextBindTag + 1
	objRef, table := resolved.ref, resolved.table
	hint, admitted, err := makeTemporalIndexHint(builder.GetContext(), atom, index, table, tag)
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
	localMap, ok, err := buildRegularIndexColumnMap(builder.GetContext(), scan, index, table, leading, tag)
	if err != nil || !ok {
		return scanID, false, err
	}
	residuals := make([]*plan.Expr, 0, len(scan.FilterList))
	for _, filter := range scan.FilterList {
		if !temporalIndexResidualColumnsMapped(filter, localMap) {
			return scanID, false, nil
		}
		residuals = append(residuals, replaceColumnsForExpr(DeepCopyExpr(filter), localMap))
	}
	if ok, cause := resolved.verify(); cause != nil || !ok {
		return scanID, false, cause
	}
	if builder.nextBindTag != tag-1 {
		return scanID, false, nil
	}
	if cause := context.Cause(builder.GetContext()); cause != nil {
		return scanID, false, cause
	}
	builder.genNewBindTag()
	for key, value := range localMap {
		colMap[key] = value
	}
	id := builder.appendRegularIndexOnlyScan(scan, makeRegularIndexOnlyScan(scan, index, &plan.Node{
		NodeType: plan.Node_TABLE_SCAN, TableDef: table, ObjRef: objRef,
		FilterList: residuals, BlockFilterList: []*plan.Expr{hint}, BindingTags: []int32{tag}, ScanSnapshot: scan.ScanSnapshot,
	}))
	builder.qry.Nodes[id].Stats.ForceOneCN = true
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
