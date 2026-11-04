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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// asyncDatalinkColumns must run before modern DML strips asynchronous indexes.
// Preserve names, not positions: the final stored row may have been projected.
func asyncDatalinkColumns(table *TableDef) ([]string, error) {
	if len(table.Indexes) == 0 {
		return nil, nil
	}
	hasDatalink := false
	for _, col := range table.Cols {
		if col.Typ.Id == int32(types.T_datalink) {
			hasDatalink = true
			break
		}
	}
	if !hasDatalink {
		return nil, nil
	}
	var columns []string
	seen := make(map[string]bool)
	for _, idx := range table.Indexes {
		plugin, ok := indexplugin.Get(idx.IndexAlgo)
		if !ok || !plugin.Catalog().SyncDescriptor().UsesCDC {
			continue
		}
		async, err := indexplugin.IsAsync(idx.IndexAlgo, idx.IndexAlgoParams)
		if err != nil {
			return nil, err
		}
		if !async {
			continue
		}
		for _, part := range idx.Parts {
			name := catalog.ResolveAlias(part)
			if seen[name] {
				continue
			}
			for _, col := range table.Cols {
				if col.Name == name && col.Typ.Id == int32(types.T_datalink) {
					columns = append(columns, name)
					seen[name] = true
					break
				}
			}
		}
	}
	return columns, nil
}

// asyncDatalinkUpdateColumns selects values produced by this UPDATE. The final
// projection also includes automatic and generated assignments. Repeated
// writable aliases may merge their values later, so retain full admission there.
func asyncDatalinkUpdateColumns(dmlCtx *DMLContext, target int, finalColumns map[string]int32) []string {
	columns := dmlCtx.asyncDatalinkColumns[target]
	if len(columns) == 0 {
		return nil
	}
	for i, table := range dmlCtx.tableDefs {
		if i != target && table.TblId == dmlCtx.tableDefs[target].TblId && len(dmlCtx.updateCol2Expr[i]) > 0 {
			return columns
		}
	}
	var assigned []string
	for _, name := range columns {
		if _, ok := finalColumns[dmlCtx.aliases[target]+"."+name]; ok {
			assigned = append(assigned, name)
		}
	}
	return assigned
}

// appendAsyncDatalinkAdmission checks already computed final values. The scalar
// owns eligibility masking before stage resolution; a boolean OR is insufficient.
func appendAsyncDatalinkAdmission(builder *QueryBuilder, bindCtx *BindContext, table *TableDef, columns []string, nodeID, inputTag int32, lookup func(string) (int32, bool), eligible *plan.Expr) (int32, error) {
	if len(columns) == 0 {
		return nodeID, nil
	}
	if eligible == nil {
		eligible = makePlan2BoolConstExprWithType(true)
	}
	filters := make([]*plan.Expr, 0, len(columns))
	for _, name := range columns {
		pos, ok := lookup(name)
		if !ok {
			return 0, moerr.NewInternalErrorf(builder.GetContext(), "missing final Datalink column %s.%s", table.Name, name)
		}
		value := &plan.Expr{Typ: plan.Type{Id: int32(types.T_datalink)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: inputTag, ColPos: pos, Name: name}}}
		check, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "_check_async_datalink_source", []*plan.Expr{value, DeepCopyExpr(eligible)})
		if err != nil {
			return 0, err
		}
		filters = append(filters, check)
	}
	return builder.appendNode(&plan.Node{NodeType: plan.Node_FILTER, Children: []int32{nodeID}, FilterList: filters, FilterIsBarrier: true, ProjectList: getProjectionByLastNodeIfAvailable(builder, nodeID)}, bindCtx), nil
}
