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
	bindCtx := NewBindContext(builder, nil)

	lastNodeId, err := deleteToSelect(builder, bindCtx, stmt, true, tblInfo)
	if err != nil {
		return nil, err
	}

	lastNode := builder.qry.Nodes[lastNodeId]
	lastTag := lastNode.BindingTags[0]
	// append sink node
	sinkTag := builder.genNewTag()
	sinkProjection := getProjectionByPreProjection(lastNode.ProjectList, lastTag)
	sinkNode := &Node{
		NodeType:    plan.Node_SINK,
		Children:    []int32{lastNodeId},
		BindingTags: []int32{sinkTag},
		ProjectList: sinkProjection,
	}
	lastNodeId = builder.appendNode(sinkNode, bindCtx)
	sourceStep := builder.appendStep(lastNodeId)
	currentStep := sourceStep + 1
	// append delete plans
	beginIdx := 0
	for i, tableDef := range tblInfo.tableDefs {
		currentStep, err = buildDeletePlans(ctx, builder, bindCtx, tblInfo.objRef[i], tableDef, beginIdx, sourceStep, currentStep)
		if err != nil {
			return nil, err
		}
		beginIdx = beginIdx + len(tableDef.Cols)
	}
	query, err := builder.createQuery()
	query.StmtType = plan.Query_DELETE
	if err != nil {
		return nil, err
	}

	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}
