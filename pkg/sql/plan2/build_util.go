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
	"strings"

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

func getColumnIndex(tableDef *plan.TableDef, name string) int32 {
	for idx, col := range tableDef.Cols {
		if strings.EqualFold(col.Name, name) {
			return int32(idx)
		}
	}
	return -1
}

func getColumnsWithSameName(left *plan.TableDef, right *plan.TableDef) []*plan.Expr {
	var exprs []*plan.Expr

	funName := getFunctionObjRef("=")
	for leftIdx, leftCol := range left.Cols {
		for rightIdx, rightCol := range right.Cols {
			if leftCol.Name == rightCol.Name {
				exprs = append(exprs, &plan.Expr{
					Expr: &plan.Expr_F{
						F: &plan.Function{
							Func: funName,
							Args: []*plan.Expr{
								{
									Expr: &plan.Expr_Col{
										Col: &plan.ColRef{
											Name:   leftCol.Name,
											RelPos: 0,
											ColPos: int32(leftIdx),
										},
									},
								},
								{
									Expr: &plan.Expr_Col{
										Col: &plan.ColRef{
											Name:   rightCol.Name,
											RelPos: 1,
											ColPos: int32(rightIdx),
										},
									},
								},
							},
						},
					},
				})
			}
		}
	}
	return exprs
}

func appendQueryNode(query *Query, node *plan.Node, isFrom bool) {
	nodeLength := len(query.Nodes)
	node.NodeId = int32(nodeLength)
	query.Nodes = append(query.Nodes, node)

	if !isFrom && node.Children == nil && nodeLength > 0 {
		node.Children = []int32{int32(nodeLength) - 1}
	}
}

func fillTableScanProjectList(query *Query, alias string) {
	// log.Printf("fillTableScanProjectList")
	node := query.Nodes[len(query.Nodes)-1]

	//special sql like: select abs(-1)
	if node.TableDef == nil {
		return
	}

	if alias == "" {
		alias = node.TableDef.Name
	}
	exprs := make([]*plan.Expr, 0, len(node.TableDef.Cols))
	for idx, col := range node.TableDef.Cols {
		exprs = append(exprs,
			&plan.Expr{
				Alias: alias + "." + col.Name,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						Name:   col.Name,
						ColPos: int32(idx),
					},
				},
				Typ: col.Typ,
			})
	}
	node.ProjectList = exprs
}

func fillJoinProjectList(node *plan.Node, leftNode *plan.Node, rightNode *plan.Node) {
	exprs := make([]*plan.Expr, 0, len(leftNode.ProjectList)+len(rightNode.ProjectList))
	for idx, expr := range leftNode.ProjectList {
		exprs = append(exprs, &plan.Expr{
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					// Name:   expr.Expr.(*plan.Expr_Col).Col.Name,
					Name:   expr.Alias,
					RelPos: 0,
					ColPos: int32(idx),
				},
			},
			Alias: expr.Alias,
			Typ:   expr.Typ,
		})
	}
	for idx, expr := range rightNode.ProjectList {
		exprs = append(exprs, &plan.Expr{
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					// Name:   expr.Expr.(*plan.Expr_Col).Col.Name,
					Name:   expr.Alias,
					RelPos: 1,
					ColPos: int32(idx),
				},
			},
			Alias: expr.Alias,
			Typ:   expr.Typ,
		})
	}

	node.ProjectList = exprs
}

func getExprFromUnresolvedName(query *Query, name string, table string, selectCtx *SelectContext) (*plan.Expr, error) {
	aliasName := table + "." + name
	colRef := &plan.ColRef{
		Name: "",
	}
	colExpr := &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: colRef,
		},
	}

	matchName := func(alias string) bool {
		if table == "" {
			arr := strings.SplitN(alias, ".", 2)
			if len(arr) > 1 {
				return strings.EqualFold(arr[1], name)
			} else {
				return strings.EqualFold(arr[0], name)
			}
		}
		return strings.EqualFold(alias, aliasName)
	}

	//get name from select
	preNode := query.Nodes[len(query.Nodes)-1]
	for {
		if preNode.ProjectList != nil {
			for idx, col := range preNode.ProjectList {
				// log.Printf("col=%v, search=%v", col.Alias, aliasName)
				if matchName(col.Alias) {
					if colRef.Name != "" {
						return nil, errors.New(errno.InvalidColumnReference, fmt.Sprintf("Column '%v' in the field list is ambiguous", name))
					}
					colRef.Name = col.Alias
					colRef.RelPos = 0
					colRef.ColPos = int32(idx)

					colExpr.Alias = col.Alias
					colExpr.Typ = col.Typ
				}
			}
			break
		}
		if preNode.Children == nil {
			break
		}
		preNode = query.Nodes[preNode.Children[0]]
	}

	//if get from select ok, then return
	//see tpch-11
	if colRef.Name != "" {
		return colExpr, nil
	}

	//get from parent query
	corrRef := &plan.CorrColRef{
		Name: "",
	}
	corrExpr := &plan.Expr{
		Expr: &plan.Expr_Corr{
			Corr: corrRef,
		},
	}
	getNode := func(id int32) *plan.Node {
		for _, node := range query.Nodes {
			if node.NodeId == id {
				return node
			}
		}
		return nil
	}

	if selectCtx.subQueryParentId != nil {
		// log.Printf("parentId=%+v", selectCtx.subQueryParentId)
		for _, parentId := range selectCtx.subQueryParentId {
			preNode = getNode(parentId)
			if preNode == nil {
				return nil, errors.New(errno.InvalidColumnReference, "parent node id not found in subquery")
			}
			for {
				if preNode.ProjectList != nil {
					for idx, col := range preNode.ProjectList {
						if matchName(col.Alias) {
							if corrRef.Name != "" {
								return nil, errors.New(errno.InvalidColumnReference, fmt.Sprintf("Column '%v' in the field list is ambiguous", name))
							}
							selectCtx.subQueryIsCorrelated = true
							corrRef.Name = col.Alias
							corrRef.RelPos = 0
							corrRef.ColPos = int32(idx)
							corrRef.NodeId = parentId

							corrExpr.Alias = col.Alias
							corrExpr.Typ = col.Typ
						}
					}
					break
				}
				if preNode.Children == nil {
					break
				}
				preNode = query.Nodes[preNode.Children[0]]
			}
		}
	}

	if corrRef.Name != "" {
		return corrExpr, nil
	}
	return nil, errors.New(errno.InvalidColumnReference, fmt.Sprintf("Column '%v' not found", name))
}

//if table == ""  => select * from tbl
//if table is not empty => select a.* from a,b on a.id = b.id
func unfoldStar(query *Query, list *plan.ExprList, table string) error {
	preNode := query.Nodes[len(query.Nodes)-1]

	matchName := func(alias string) bool {
		if table != "" {
			arr := strings.SplitN(alias, ".", 2)
			return arr[0] == table
		}
		return true
	}

	for _, expr := range preNode.ProjectList {
		if matchName(expr.Alias) {
			list.List = append(list.List, expr)
		}
	}
	return nil
}

func setDerivedTableAlias(query *Query, ctx CompilerContext, selectCtx *SelectContext, alias string, cols tree.IdentifierList) error {
	//create a project node for reset projection list
	node := &plan.Node{
		NodeType: plan.Node_PROJECT,
	}
	preNode := query.Nodes[len(query.Nodes)-1]
	exprs := make([]*plan.Expr, 0, len(preNode.ProjectList))
	prefix := alias + "."
	if cols != nil {
		if len(preNode.ProjectList) != len(cols) {
			return errors.New(errno.InvalidColumnReference, "Derived table column length not match")
		}
		for idx, col := range cols {
			exprs = append(exprs, &plan.Expr{
				Expr:  nil,
				Alias: prefix + string(col),
				Typ:   preNode.ProjectList[idx].Typ,
			})
		}
		node.ProjectList = exprs
	} else {
		for _, col := range preNode.ProjectList {
			alias := col.Alias
			if !strings.HasPrefix(col.Alias, prefix) {
				alias = prefix + alias
			}
			exprs = append(exprs, &plan.Expr{
				Expr:  nil,
				Alias: alias,
				Typ:   col.Typ,
			})
		}
		node.ProjectList = exprs
	}
	appendQueryNode(query, node, false)

	//create new project node by default。
	//if next node is join, you need remove this node
	tmpNode := &plan.Node{
		NodeType: plan.Node_PROJECT,
	}
	appendQueryNode(query, tmpNode, false)
	return nil
}

func getResolveTable(tblName string, ctx CompilerContext, selectCtx *SelectContext) (*plan.ObjectRef, *plan.TableDef, bool) {
	//get table from context
	objRef, tableDef := ctx.Resolve(tblName)
	if tableDef != nil {
		return objRef, tableDef, false
	}

	//get table from CTE
	tableDef, ok := selectCtx.cteTables[strings.ToLower(tblName)]
	if ok {
		objRef = &plan.ObjectRef{
			ObjName: tblName,
		}
		return objRef, tableDef, true
	}
	return nil, nil, false
}

//getLastTableDef get insert/update/delete tableDef
func getLastTableDef(query *Query) (*plan.ObjectRef, *plan.TableDef) {
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

func newQueryAndSelectCtx(typ plan.Query_StatementType) (*Query, *SelectContext) {
	selectCtx := &SelectContext{
		columnAlias: make(map[string]*plan.Expr),
		cteTables:   make(map[string]*plan.TableDef),
	}
	query := &Query{
		StmtType: typ,
	}
	return query, selectCtx
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

	//get isAllowNull setting
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
			//check allowNull
			if isNullExpr(defaultExpr) {
				if !allowNull {
					return nil, errors.New(errno.InvalidColumnDefinition, fmt.Sprintf("Invalid default value for '%s'", column.Name.Parts[0]))
				}
				return &plan.DefaultExpr{
					Exist:  true,
					IsNull: true,
				}, nil
			}

			value, err := buildExpr(d.Expr, nil, nil, nil)
			if err != nil {
				return nil, err
			}
			//todo check value match type
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
