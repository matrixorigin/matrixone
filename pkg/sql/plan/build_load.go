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
	"encoding/json"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

func buildLoad(stmt *tree.Load, ctx CompilerContext) (*Plan, error) {
	tblName := string(stmt.Table.ObjectName)
	dbName := string(stmt.Table.SchemaName)
	objRef, tableDef := ctx.Resolve(dbName, tblName)
	if tableDef == nil {
		return nil, moerr.NewInvalidInput("load table '%s' does not exists", tree.String(stmt.Table, dialect.MYSQL))
	}
	if tableDef.TableType == catalog.SystemExternalRel {
		return nil, moerr.NewInvalidInput("cannot load external table")
	}

	tableDef.Name2ColIndex = map[string]int32{}
	node1 := &plan.Node{}
	node1.NodeType = plan.Node_EXTERNAL_SCAN
	node1.Cost = &plan.Cost{}

	node2 := &plan.Node{}
	node2.NodeType = plan.Node_PROJECT
	node2.Cost = &plan.Cost{}
	node2.NodeId = 1
	node2.Children = []int32{0}

	node3 := &plan.Node{}
	node3.NodeType = plan.Node_INSERT
	node3.Cost = &plan.Cost{}
	node3.NodeId = 2
	node3.Children = []int32{1}

	for i := 0; i < len(tableDef.Cols); i++ {
		tableDef.Name2ColIndex[tableDef.Cols[i].Name] = int32(i)
		tmp := &plan.Expr{
			Typ: tableDef.Cols[i].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					ColPos: int32(i),
					Name:   tblName + "." + tableDef.Cols[i].Name,
				},
			},
		}
		node1.ProjectList = append(node1.ProjectList, tmp)
		node3.ProjectList = append(node3.ProjectList, tmp)
	}
	if err := GetProjectNode(stmt, ctx, node2, tableDef.Name2ColIndex); err != nil {
		return nil, err
	}

	node3.TableDef = tableDef
	node3.ObjRef = objRef

	stmt.Param.Tail.ColumnList = nil
	json_byte, err := json.Marshal(stmt.Param)
	if err != nil {
		return nil, err
	}

	tableDef.Createsql = string(json_byte)
	node1.TableDef = tableDef
	node1.ObjRef = objRef

	nodes := make([]*plan.Node, 3)
	nodes[0] = node1
	nodes[1] = node2
	nodes[2] = node3
	query := &plan.Query{
		StmtType: plan.Query_INSERT,
		Steps:    []int32{2},
		Nodes:    nodes,
	}
	pn := &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}

	return pn, nil
}

func GetProjectNode(stmt *tree.Load, ctx CompilerContext, node *plan.Node, Name2ColIndex map[string]int32) error {
	tblName := string(stmt.Table.ObjectName)
	dbName := string(stmt.Table.SchemaName)
	_, tableDef := ctx.Resolve(dbName, tblName)
	if tableDef == nil {
		return fmt.Errorf("invalid table name: %s", string(stmt.Table.ObjectName))
	}
	if len(stmt.Param.Tail.ColumnList) > len(tableDef.Cols) {
		return fmt.Errorf("the load data colnum list is larger than table colnum")
	}
	colToIndex := make(map[int32]string, 0)
	if len(stmt.Param.Tail.ColumnList) == 0 {
		for i := 0; i < len(tableDef.Cols); i++ {
			colToIndex[int32(i)] = tableDef.Cols[i].Name
		}
	} else {
		for i, col := range stmt.Param.Tail.ColumnList {
			switch realCol := col.(type) {
			case *tree.UnresolvedName:
				if _, ok := Name2ColIndex[realCol.Parts[0]]; !ok {
					return fmt.Errorf("column '%s' does not exist", realCol.Parts[0])
				}
				colToIndex[int32(i)] = realCol.Parts[0]
			case *tree.VarExpr:
				//NOTE:variable like '@abc' will be passed by.
			default:
				return fmt.Errorf("unsupported column type %v", realCol)
			}
		}
	}
	node.ProjectList = make([]*plan.Expr, len(tableDef.Cols))
	projectVec := make([]*plan.Expr, len(tableDef.Cols))
	for i := 0; i < len(tableDef.Cols); i++ {
		tmp := &plan.Expr{
			Typ: tableDef.Cols[i].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					ColPos: int32(i),
					Name:   tblName + "." + tableDef.Cols[i].Name,
				},
			},
		}
		projectVec[i] = tmp
	}
	for i := 0; i < len(tableDef.Cols); i++ {
		if v, ok := colToIndex[int32(i)]; ok {
			node.ProjectList[Name2ColIndex[v]] = projectVec[i]
		}
	}
	for i := 0; i < len(tableDef.Cols); i++ {
		if node.ProjectList[i] != nil {
			continue
		}
		var tmp *plan.Expr
		if tableDef.Cols[i].Default == nil || tableDef.Cols[i].Default.NullAbility {
			tmp = makePlan2NullConstExprWithType()
		} else {
			tmp = &plan.Expr{
				Typ:  tableDef.Cols[i].Default.Expr.Typ,
				Expr: tableDef.Cols[i].Default.Expr.Expr,
			}
		}
		node.ProjectList[i] = tmp
	}
	return nil
}
