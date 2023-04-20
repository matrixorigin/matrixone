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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildInsert(stmt *tree.Insert, ctx CompilerContext, isReplace bool) (p *Plan, err error) {
	if isReplace {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "Not support replace statement")
	}

	tblInfo, err := getDmlTableInfo(ctx, tree.TableExprs{stmt.Table}, nil, nil, "insert")
	if err != nil {
		return nil, err
	}
	rewriteInfo := &dmlSelectInfo{
		typ:     "insert",
		rootId:  -1,
		tblInfo: tblInfo,
	}
	tableDef := tblInfo.tableDefs[0]
	clusterTable, err := getAccountInfoOfClusterTable(ctx, stmt.Accounts, tableDef, tblInfo.isClusterTable[0])
	if err != nil {
		return nil, err
	}

	if len(stmt.OnDuplicateUpdate) > 0 && clusterTable.IsClusterTable {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "INSERT ... ON DUPLICATE KEY UPDATE ... for cluster table")
	}

	builder := NewQueryBuilder(plan.Query_SELECT, ctx)

	bindCtx := NewBindContext(builder, nil)
	err = initInsertStmt(builder, bindCtx, stmt, rewriteInfo)
	if err != nil {
		return nil, err
	}
	lastNodeId := rewriteInfo.rootId
	sourceStep := builder.appendStep(lastNodeId)
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}
	builder.qry.Steps = append(builder.qry.Steps[:sourceStep], builder.qry.Steps[sourceStep+1:]...)

	// new logic
	objRef := tblInfo.objRef[0]
	if len(rewriteInfo.onDuplicateIdx) > 0 {
		err = buildOnDuplicateKeyPlans(builder, bindCtx, rewriteInfo)
	} else {
		err = buildInsertPlans(ctx, builder, bindCtx, objRef, tableDef, rewriteInfo.rootId)
	}
	if err != nil {
		return nil, err
	}
	query.StmtType = plan.Query_INSERT
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}
