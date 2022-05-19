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
	"go/constant"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

//splitExprToAND split a expression to a list of AND conditions.
func splitExprToAND(expr tree.Expr) []*tree.Expr {
	var exprs []*tree.Expr
	switch typ := expr.(type) {
	case nil:
	case *tree.AndExpr:
		exprs = append(exprs, splitExprToAND(typ.Left)...)
		exprs = append(exprs, splitExprToAND(typ.Right)...)
	case *tree.ParenExpr:
		exprs = append(exprs, splitExprToAND(typ.Expr)...)
	default:
		exprs = append(exprs, &expr)
	}
	return exprs
}

func getColumnIndex(projectList []*Expr, colName string) int32 {
	for idx, expr := range projectList {
		if expr.ColName == colName {
			return int32(idx)
		}
	}
	return -1
}

func getColumnsWithSameName(leftProjList []*Expr, rightProjList []*Expr) []*Expr {
	var commonList []*Expr

	leftMap := make(map[string]int)

	for idx, col := range leftProjList {
		leftMap[col.ColName] = idx
	}

	funName := getFunctionObjRef("=")
	for idx, col := range rightProjList {
		if leftIdx, ok := leftMap[col.ColName]; ok {
			commonList = append(commonList, &Expr{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: funName,
						Args: []*Expr{
							{
								TableName: leftProjList[leftIdx].TableName,
								ColName:   col.ColName,
								Expr: &plan.Expr_Col{
									Col: &ColRef{
										RelPos: 0,
										ColPos: int32(leftIdx),
									},
								},
							},
							{
								TableName: col.TableName,
								ColName:   col.ColName,
								Expr: &plan.Expr_Col{
									Col: &ColRef{
										RelPos: 1,
										ColPos: int32(idx),
									},
								},
							},
						},
					},
				},
			})
		}
	}

	return commonList
}

func appendQueryNode(query *Query, node *Node) int32 {
	nodeId := int32(len(query.Nodes))
	node.NodeId = nodeId
	query.Nodes = append(query.Nodes, node)

	return nodeId
}

func fillTableProjectList(query *Query, nodeId int32, alias tree.AliasClause) (int32, error) {
	node := query.Nodes[nodeId]
	if node.ProjectList == nil {
		if node.TableDef == nil {
			return nodeId, nil
		}

		if len(alias.Cols) > len(node.TableDef.Cols) {
			return 0, errors.New(errno.InvalidColumnReference, fmt.Sprintf("table %v has %v columns available but %v columns specified", alias.Alias, len(node.TableDef.Cols), len(alias.Cols)))
		}

		// Table scan
		if alias.Alias != "" {
			node.TableDef.Alias = string(alias.Alias)
		} else {
			node.TableDef.Alias = node.TableDef.Name
		}

		node.ProjectList = make([]*Expr, len(node.TableDef.Cols))
		for idx, col := range node.TableDef.Cols {
			if idx < len(alias.Cols) {
				col.Alias = string(alias.Cols[idx])
			} else {
				col.Alias = col.Name
			}

			node.ProjectList[idx] = &Expr{
				Typ:       col.Typ,
				TableName: node.TableDef.Alias,
				ColName:   col.Alias,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: 0,
						ColPos: int32(idx),
					},
				},
			}
		}
	} else {
		// Subquery
		if len(alias.Cols) > len(node.ProjectList) {
			return 0, errors.New(errno.InvalidColumnReference, fmt.Sprintf("table %v has %v columns available but %v columns specified", alias.Alias, len(node.ProjectList), len(alias.Cols)))
		}

		projNode := &Node{
			NodeType: plan.Node_PROJECT,
			Children: []int32{nodeId},
		}
		projNode.ProjectList = make([]*Expr, len(node.ProjectList))
		tableName := string(alias.Alias)
		var colName string
		for idx, col := range node.ProjectList {
			if idx < len(alias.Cols) {
				colName = string(alias.Cols[idx])
			} else {
				colName = col.ColName
			}

			col.TableName = tableName
			col.ColName = colName

			projNode.ProjectList[idx] = &Expr{
				Typ:       col.Typ,
				TableName: tableName,
				ColName:   colName,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: 0,
						ColPos: int32(idx),
					},
				},
			}
		}
		nodeId = appendQueryNode(query, projNode)
	}

	return nodeId, nil
}

func fillJoinProjectList(node *Node, leftChild *Node, rightChild *Node) {
	// TODO: NATURAL JOIN should not include duplicate columns

	resultLen := len(leftChild.ProjectList) + len(rightChild.ProjectList)
	if leftChild.JoinType&(plan.Node_SEMI|plan.Node_ANTI) != 0 {
		resultLen = len(leftChild.ProjectList)
	}

	node.ProjectList = make([]*Expr, resultLen)
	idx := 0

	for i, expr := range leftChild.ProjectList {
		if expr == nil {
			panic(i)
		}
		node.ProjectList[idx] = &Expr{
			Typ:       expr.Typ,
			TableName: expr.TableName,
			ColName:   expr.ColName,
			Expr: &plan.Expr_Col{
				Col: &ColRef{
					RelPos: 0,
					ColPos: int32(i),
				},
			},
		}
		idx++
	}

	if leftChild.JoinType&(plan.Node_SEMI|plan.Node_ANTI) != 0 {
		return
	}

	for i, expr := range rightChild.ProjectList {
		node.ProjectList[idx] = &Expr{
			Typ:       expr.Typ,
			TableName: expr.TableName,
			ColName:   expr.ColName,
			Expr: &plan.Expr_Col{
				Col: &ColRef{
					RelPos: 1,
					ColPos: int32(i),
				},
			},
		}
		idx++
	}
}

func buildUnresolvedName(query *Query, node *Node, colName string, tableName string, binderCtx *BinderContext) (*Expr, error) {
	var name string
	if tableName != "" {
		name = tableName + "." + colName
	} else {
		name = colName
	}

	colRef := &ColRef{
		RelPos: -1,
		ColPos: -1,
	}
	colExpr := &Expr{
		Expr: &plan.Expr_Col{
			Col: colRef,
		},
	}

	matchName := func(expr *Expr) bool {
		return colName == expr.ColName && (len(tableName) == 0 || tableName == expr.TableName)
	}

	if node.NodeType == plan.Node_TABLE_SCAN {
		// search name from TableDef
		if len(tableName) == 0 || tableName == node.TableDef.Alias {
			for j, col := range node.TableDef.Cols {
				if colName == col.Alias {
					if colRef.RelPos != -1 {
						return nil, errors.New(errno.AmbiguousColumn, fmt.Sprintf("column reference '%v' is ambiguous", name))
					}
					colRef.RelPos = 0
					colRef.ColPos = int32(j)

					colExpr.Typ = col.Typ
					colExpr.ColName = colName
					colExpr.TableName = tableName
				}
			}
		}
	} else {
		// Search name from children
		for i, child := range node.Children {
			for j, col := range query.Nodes[child].ProjectList {
				if matchName(col) {
					if colRef.RelPos != -1 {
						return nil, errors.New(errno.AmbiguousColumn, fmt.Sprintf("column reference '%v' is ambiguous", name))
					}
					colRef.RelPos = int32(i)
					colRef.ColPos = int32(j)

					colExpr.Typ = col.Typ
					colExpr.ColName = col.ColName
					colExpr.TableName = col.TableName
				}
			}
		}
	}

	if colRef.RelPos != -1 {
		return colExpr, nil
	}

	if len(binderCtx.subqueryParentIds) == 0 {
		return nil, errors.New(errno.InvalidColumnReference, fmt.Sprintf("column '%v' does not exist", name))
	}

	// Search from parent queries
	corrRef := &plan.CorrColRef{
		NodeId: -1,
		ColPos: -1,
	}
	corrExpr := &Expr{
		Expr: &plan.Expr_Corr{
			Corr: corrRef,
		},
	}

	for _, parentId := range binderCtx.subqueryParentIds {
		for i, col := range query.Nodes[parentId].ProjectList {
			if matchName(col) {
				if corrRef.NodeId != -1 {
					return nil, errors.New(errno.InvalidColumnReference, fmt.Sprintf("column '%v' in the field list is ambiguous", colName))
				}
				corrRef.NodeId = parentId
				corrRef.ColPos = int32(i)

				corrExpr.Typ = col.Typ
				corrExpr.ColName = col.ColName
				corrExpr.TableName = col.TableName
			}
		}
		if corrRef.ColPos != -1 {
			return corrExpr, nil
		}
	}

	return nil, errors.New(errno.InvalidColumnReference, fmt.Sprintf("column '%v' does not exist", name))
}

//if table == ""  => select * from tbl
//if table is not empty => select a.* from a,b on a.id = b.id
func unfoldStar(node *Node, list *plan.ExprList, table string) error {
	for _, col := range node.ProjectList {
		if len(table) == 0 || table == col.TableName {
			list.List = append(list.List, col)
		}
	}
	return nil
}

func getResolveTable(tableName string, ctx CompilerContext, binderCtx *BinderContext) (*ObjectRef, *TableDef, bool) {
	// get table from context
	objRef, tableDef := ctx.Resolve(tableName)
	if tableDef != nil {
		return objRef, tableDef, false
	}

	// get table from CTE
	tableDef, ok := binderCtx.cteTables[tableName]
	if ok {
		objRef = &ObjectRef{
			ObjName: tableName,
		}
		return objRef, tableDef, true
	}
	return nil, nil, false
}

//getLastTableDef get insert/update/delete tableDef
// FIXME
func getLastTableDef(query *Query) (*ObjectRef, *TableDef) {
	node := query.Nodes[query.Steps[len(query.Steps)-1]]
	for {
		if node.TableDef != nil {
			return node.ObjRef, node.TableDef
		}
		if len(node.Children) == 0 {
			break
		}
		node = query.Nodes[node.Children[0]]
	}
	return nil, nil
}

func newQueryAndSelectCtx(typ plan.Query_StatementType) (*Query, *BinderContext) {
	binderCtx := &BinderContext{
		columnAlias: make(map[string]*Expr),
		cteTables:   make(map[string]*TableDef),
	}
	query := &Query{
		StmtType: typ,
	}
	return query, binderCtx
}

func getTypeFromAst(typ tree.ResolvableTypeReference) (*plan.Type, error) {
	if n, ok := typ.(*tree.T); ok {
		switch uint8(n.InternalType.Oid) {
		case defines.MYSQL_TYPE_TINY:
			if n.InternalType.Unsigned {
				return &plan.Type{Id: plan.Type_UINT8, Width: n.InternalType.Width}, nil
			}
			return &plan.Type{Id: plan.Type_INT8, Width: n.InternalType.Width}, nil
		case defines.MYSQL_TYPE_SHORT:
			if n.InternalType.Unsigned {
				return &plan.Type{Id: plan.Type_UINT16, Width: n.InternalType.Width}, nil
			}
			return &plan.Type{Id: plan.Type_INT16, Width: n.InternalType.Width}, nil
		case defines.MYSQL_TYPE_LONG:
			if n.InternalType.Unsigned {
				return &plan.Type{Id: plan.Type_UINT32, Width: n.InternalType.Width}, nil
			}
			return &plan.Type{Id: plan.Type_INT32, Width: n.InternalType.Width}, nil
		case defines.MYSQL_TYPE_LONGLONG:
			if n.InternalType.Unsigned {
				return &plan.Type{Id: plan.Type_UINT64, Width: n.InternalType.Width}, nil
			}
			return &plan.Type{Id: plan.Type_INT64, Width: n.InternalType.Width}, nil
		case defines.MYSQL_TYPE_FLOAT:
			return &plan.Type{Id: plan.Type_FLOAT32, Width: n.InternalType.Width, Precision: n.InternalType.Precision}, nil
		case defines.MYSQL_TYPE_DOUBLE:
			return &plan.Type{Id: plan.Type_FLOAT64, Width: n.InternalType.Width, Precision: n.InternalType.Precision}, nil
		case defines.MYSQL_TYPE_STRING:
			if n.InternalType.DisplayWith == -1 { // type char
				return &plan.Type{Id: plan.Type_CHAR, Width: 1}, nil
			}
			return &plan.Type{Id: plan.Type_VARCHAR, Width: n.InternalType.DisplayWith}, nil
		case defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_VARCHAR:
			if n.InternalType.DisplayWith == -1 { // type char
				return &plan.Type{Id: plan.Type_CHAR, Width: 1}, nil
			}
			return &plan.Type{Id: plan.Type_VARCHAR, Width: n.InternalType.DisplayWith}, nil
		case defines.MYSQL_TYPE_DATE:
			return &plan.Type{Id: plan.Type_DATE}, nil
		case defines.MYSQL_TYPE_DATETIME:
			return &plan.Type{Id: plan.Type_DATETIME}, nil
		case defines.MYSQL_TYPE_TIMESTAMP:
			return &plan.Type{Id: plan.Type_TIMESTAMP, Precision: n.InternalType.Precision}, nil
		case defines.MYSQL_TYPE_DECIMAL:
			if n.InternalType.DisplayWith > 18 {
				return &plan.Type{Id: plan.Type_DECIMAL128, Width: n.InternalType.DisplayWith, Precision: n.InternalType.Precision}, nil
			}
			return &plan.Type{Id: plan.Type_DECIMAL64, Width: n.InternalType.DisplayWith, Precision: n.InternalType.Precision}, nil
		case defines.MYSQL_TYPE_BOOL:
			return &plan.Type{Id: plan.Type_BOOL}, nil
		}
	}
	return nil, errors.New(errno.IndeterminateDatatype, fmt.Sprintf("unsupport type: '%v'", typ))
}

func getDefaultExprFromColumn(column *tree.ColumnTableDef, typ *plan.Type) (*plan.DefaultExpr, error) {
	allowNull := true // be false when column has not null constraint
	isNullExpr := func(expr tree.Expr) bool {
		v, ok := expr.(*tree.NumVal)
		return ok && v.Value.Kind() == constant.Unknown
	}

	// get isAllowNull setting
	{
		for _, attr := range column.Attributes {
			if nullAttr, ok := attr.(*tree.AttributeNull); ok && nullAttr.Is == false {
				allowNull = false
				break
			}
		}
	}

	for _, attr := range column.Attributes {
		if d, ok := attr.(*tree.AttributeDefault); ok {
			defaultExpr := d.Expr
			// check allowNull
			if isNullExpr(defaultExpr) {
				if !allowNull {
					return nil, errors.New(errno.InvalidColumnDefinition, fmt.Sprintf("Invalid default value for '%s'", column.Name.Parts[0]))
				}
				return &plan.DefaultExpr{
					Exist:  true,
					IsNull: true,
				}, nil
			}

			value, err := buildExpr(d.Expr, nil, nil, nil, nil)
			if err != nil {
				return nil, err
			}
			// todo check value match type
			return &plan.DefaultExpr{
				Exist:  true,
				Value:  value,
				IsNull: false,
			}, nil
		}
	}

	return &plan.DefaultExpr{
		Exist: false,
	}, nil
}
