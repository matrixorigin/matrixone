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

package plan2

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildFrom(stmt tree.TableExprs, ctx CompilerContext, query *Query, aliasCtx *AliasContext) error {
	for _, table := range stmt {
		err := buildTable(table, ctx, query, aliasCtx)
		if err != nil {
			return err
		}
	}
	return nil
}

func buildTable(stmt tree.TableExpr, ctx CompilerContext, query *Query, aliasCtx *AliasContext) error {
	switch tbl := stmt.(type) {
	case *tree.Select:
		buildSelect(tbl, ctx, query)
		return nil
	case *tree.TableName:
		name := string(tbl.ObjectName)
		if len(tbl.SchemaName) > 0 {
			name = strings.Join([]string{string(tbl.SchemaName), name}, ".")
		}
		obj, tableDef := ctx.Resolve(name)
		nodeLength := int32(len(query.Nodes))
		node := &plan.Node{
			NodeType: plan.Node_TABLE_SCAN, //todo confirm NodeType
			NodeId:   nodeLength,
			ObjRef:   obj,
			TableDef: tableDef,
		}
		query.Nodes = append(query.Nodes, node)
		return nil
	case *tree.JoinTableExpr:
		//todo confirm how to deal with alias
		return buildJoinTable(tbl, ctx, query, aliasCtx)
	case *tree.ParenTableExpr:
		return buildTable(tbl.Expr, ctx, query, aliasCtx)
	case *tree.AliasedTableExpr:
		err := buildTable(tbl.Expr, ctx, query, aliasCtx)
		if err != nil {
			return err
		}
		aliasCtx.tableAlias[string(tbl.As.Alias)] = query.Nodes[len(query.Nodes)-1].TableDef
		// fmt.Printf("[%v], [%v]", tbl.As.Alias, aliasCtx.tableAlias[string(tbl.As.Alias)])
		return nil
	case *tree.StatementSource:
		return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table expr: %T", stmt))
	}
	// Values table not support
	return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table expr: %T", stmt))
}

func buildJoinTable(tbl *tree.JoinTableExpr, ctx CompilerContext, query *Query, aliasCtx *AliasContext) error {
	err := buildTable(tbl.Right, ctx, query, aliasCtx)
	if err != nil {
		return err
	}
	rightNodeId := query.Nodes[len(query.Nodes)-1].NodeId
	rightTableDef := query.Nodes[len(query.Nodes)-1].TableDef

	err = buildTable(tbl.Left, ctx, query, aliasCtx)
	if err != nil {
		return err
	}
	leftNodeId := query.Nodes[len(query.Nodes)-1].NodeId
	leftTableDef := query.Nodes[len(query.Nodes)-1].TableDef

	var onList []*plan.Expr
	switch cond := tbl.Cond.(type) {
	case *tree.OnJoinCond:
		exprs, err := splitAndBuildExpr(cond.Expr, ctx, query, aliasCtx)
		if err != nil {
			return err
		}
		onList = exprs
	case *tree.UsingJoinCond:
		for _, identifier := range cond.Cols {
			name := string(identifier)
			leftColIndex := getColumnIndex(leftTableDef, name)
			if leftColIndex < 0 {
				return errors.New(errno.InvalidColumnReference, fmt.Sprintf("column '%v' is not exist", name))
			}
			rightColIndex := getColumnIndex(rightTableDef, name)
			if rightColIndex < 0 {
				return errors.New(errno.InvalidColumnReference, fmt.Sprintf("column '%v' is not exist", name))
			}

			col := &plan.Expr_Col{
				Col: &plan.ColRef{
					Name:   name,
					RelPos: rightColIndex,
					ColPos: leftColIndex,
				},
			}
			onList = append(onList, &plan.Expr{
				Expr: col,
			})
		}
	default:
		if tbl.JoinType == tree.JOIN_TYPE_NATURAL { //natural join.  the cond will be nil
			columns := getColumnsWithSameName(leftTableDef, rightTableDef)
			if len(columns) == 0 {
				return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("donot have same column name when using natural join"))
			}
			onList = append(onList, columns...)
		} else {
			return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport join condition '%v'", tree.String(tbl.Cond, dialect.MYSQL)))
		}

	}

	node := &plan.Node{
		NodeType: plan.Node_JOIN,
		NodeId:   int32(len(query.Nodes)),
		OnList:   onList,
		Children: []int32{rightNodeId, leftNodeId},
	}
	query.Nodes = append(query.Nodes, node)

	return nil
}
