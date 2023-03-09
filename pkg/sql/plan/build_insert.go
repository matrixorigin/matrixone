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

	tblInfo, err := getDmlTableInfo(ctx, tree.TableExprs{stmt.Table}, nil, nil)
	if err != nil {
		return nil, err
	}
	rewriteInfo := &dmlSelectInfo{
		typ:     "insert",
		rootId:  -1,
		tblInfo: tblInfo,
	}
	tblDef := tblInfo.tableDefs[0]
	clusterTable, err := getAccountInfoOfClusterTable(ctx, stmt.Accounts, tblDef, tblInfo.isClusterTable[0])
	if err != nil {
		return nil, err
	}

	if len(stmt.OnDuplicateUpdate) > 0 && clusterTable.IsClusterTable {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "INSERT ... ON DUPLICATE KEY UPDATE ... for cluster table")
	}

	builder := NewQueryBuilder(plan.Query_SELECT, ctx)
	bindCtx := NewBindContext(builder, nil)
	bindCtx.groupTag = builder.genNewTag()
	bindCtx.aggregateTag = builder.genNewTag()
	bindCtx.projectTag = builder.genNewTag()

	err = initInsertStmt(builder, bindCtx, stmt, rewriteInfo)
	if err != nil {
		return nil, err
	}

	if tblInfo.haveConstraint {
		for i, tableDef := range tblInfo.tableDefs {
			err = rewriteDmlSelectInfo(builder, bindCtx, rewriteInfo, tableDef, rewriteInfo.derivedTableId, i)
			if err != nil {
				return nil, err
			}
		}
	}

	// append ProjectNode
	rewriteInfo.rootId = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: rewriteInfo.projectList,
		Children:    []int32{rewriteInfo.rootId},
		BindingTags: []int32{bindCtx.projectTag},
	}, bindCtx)

	bindCtx.results = rewriteInfo.projectList
	builder.qry.Steps = append(builder.qry.Steps, rewriteInfo.rootId)
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}

	// append insert node
	insertCtx := &plan.InsertCtx{
		Ref:      rewriteInfo.tblInfo.objRef[0],
		TableDef: rewriteInfo.tblInfo.tableDefs[0],

		IdxRef: rewriteInfo.onIdxTbl,
		IdxIdx: rewriteInfo.onIdx,

		OnDuplicateIdx:  rewriteInfo.onDuplicateIdx,
		OnDuplicateExpr: rewriteInfo.onDuplicateExpr,
	}
	if len(rewriteInfo.parentIdx) == 1 {
		insertCtx.ParentIdx = rewriteInfo.parentIdx[0]
	}

	node := &Node{
		NodeType:  plan.Node_INSERT,
		ObjRef:    insertCtx.Ref,
		TableDef:  insertCtx.TableDef,
		Children:  []int32{query.Steps[len(query.Steps)-1]},
		NodeId:    int32(len(query.Nodes)),
		InsertCtx: insertCtx,
	}
	query.Nodes = append(query.Nodes, node)
	query.Steps[len(query.Steps)-1] = node.NodeId
	query.StmtType = plan.Query_INSERT

	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}
