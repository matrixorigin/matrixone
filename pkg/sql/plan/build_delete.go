// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildDelete(stmt *tree.Delete, ctx CompilerContext) (*Plan, error) {
	aliasMap := make(map[string][2]string)
	for _, tbl := range stmt.TableRefs {
		getAliasToName(ctx, tbl, "", aliasMap)
	}
	tblInfo, err := getDmlTableInfo(ctx, stmt.Tables, stmt.With, aliasMap, "delete")
	if err != nil {
		return nil, err
	}
	builder := NewQueryBuilder(plan.Query_SELECT, ctx)

	queryBindCtx := NewBindContext(builder, nil)
	lastNodeId, err := deleteToSelect(builder, queryBindCtx, stmt, true, tblInfo)
	if err != nil {
		return nil, err
	}
	sourceStep := builder.appendStep(lastNodeId)
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}
	builder.qry.Steps = append(builder.qry.Steps[:sourceStep], builder.qry.Steps[sourceStep+1:]...)

	if tblInfo.isMulti {
		// append sink node
		lastNodeId = appendSinkNode(builder, queryBindCtx, lastNodeId)
		sourceStep = builder.appendStep(lastNodeId)
	} else {
		sourceStep = -1
	}

	allDelTableIDs := make(map[uint64]struct{})
	for _, tableDef := range tblInfo.tableDefs {
		allDelTableIDs[tableDef.TblId] = struct{}{}
	}

	// append delete plans
	beginIdx := 0
	// needLockTable := !tblInfo.isMulti && stmt.Where == nil && stmt.Limit == nil
	// todo will do not lock table now.
	needLockTable := false
	for i, tableDef := range tblInfo.tableDefs {
		deleteBindCtx := NewBindContext(builder, nil)
		delPlanCtx := getDmlPlanCtx()
		delPlanCtx.objRef = tblInfo.objRef[i]
		delPlanCtx.tableDef = tableDef
		delPlanCtx.beginIdx = beginIdx
		delPlanCtx.sourceStep = sourceStep
		delPlanCtx.isMulti = tblInfo.isMulti
		delPlanCtx.needAggFilter = tblInfo.needAggFilter
		delPlanCtx.updateColLength = 0
		delPlanCtx.rowIdPos = getRowIdPos(tableDef)
		delPlanCtx.allDelTableIDs = allDelTableIDs
		delPlanCtx.lockTable = needLockTable

		if tblInfo.isMulti {
			lastNodeId = appendSinkScanNode(builder, deleteBindCtx, sourceStep)
			lastNodeId, err = makePreUpdateDeletePlan(ctx, builder, deleteBindCtx, delPlanCtx, lastNodeId)
			if err != nil {
				return nil, err
			}
			if len(delPlanCtx.tableDef.RefChildTbls) > 0 || haveUniqueKey(delPlanCtx.tableDef) {
				lastNodeId = appendSinkNode(builder, deleteBindCtx, lastNodeId)
				nextSourceStep := builder.appendStep(lastNodeId)
				delPlanCtx.sourceStep = nextSourceStep
			} else {
				delPlanCtx.sourceStep = -1
			}
		} else {
			lastNodeId, err = makePreUpdateDeletePlan(ctx, builder, deleteBindCtx, delPlanCtx, lastNodeId)
			if err != nil {
				return nil, err
			}
			if len(delPlanCtx.tableDef.RefChildTbls) > 0 || haveUniqueKey(delPlanCtx.tableDef) {
				lastNodeId = appendSinkNode(builder, deleteBindCtx, lastNodeId)
				nextSourceStep := builder.appendStep(lastNodeId)
				delPlanCtx.sourceStep = nextSourceStep
			}
		}
		err = buildDeletePlans(ctx, builder, deleteBindCtx, delPlanCtx, lastNodeId)
		if err != nil {
			return nil, err
		}
		beginIdx = beginIdx + len(tableDef.Cols)
		putDmlPlanCtx(delPlanCtx)
	}

	query.StmtType = plan.Query_DELETE
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}
