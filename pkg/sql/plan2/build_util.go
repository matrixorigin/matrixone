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
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
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

func getColumnIndexAndType(projectList []*Expr, colName string) (int32, *Type) {
	for idx, expr := range projectList {
		if expr.ColName == colName {
			return int32(idx), expr.Typ
		}
	}
	return -1, nil
}

func getColumnsWithSameName(leftProjList []*Expr, rightProjList []*Expr) ([]*Expr, map[string]int, error) {
	var commonList []*Expr
	usingCols := make(map[string]int)
	leftMap := make(map[string]int)

	for idx, col := range leftProjList {
		leftMap[col.ColName] = idx
	}
	for idx, col := range rightProjList {
		if leftIdx, ok := leftMap[col.ColName]; ok {
			leftColExpr := &plan.Expr{
				TableName: leftProjList[leftIdx].TableName,
				ColName:   col.ColName,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: 0,
						ColPos: int32(leftIdx),
					},
				},
				Typ: col.Typ,
			}
			rightColExpr := &plan.Expr{
				TableName: col.TableName,
				ColName:   col.ColName,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: 1,
						ColPos: int32(idx),
					},
				},
				Typ: leftProjList[leftIdx].Typ,
			}

			equalFunctionExpr, _, err := getFunctionExprByNameAndPlanExprs("=", false, []*Expr{leftColExpr, rightColExpr})
			if err != nil {
				return nil, nil, err
			}
			usingCols[col.ColName] = len(commonList)
			commonList = append(commonList, equalFunctionExpr)
		}
	}

	return commonList, usingCols, nil
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

func fillJoinProjectList(binderCtx *BinderContext, usingCols map[string]int, node *Node, leftChild *Node, rightChild *Node, projectNodeWhere []*Expr) *Node {
	joinResultLen := len(leftChild.ProjectList) + len(rightChild.ProjectList)
	usingColsLen := len(usingCols)
	projectResultLen := joinResultLen - usingColsLen

	isSemiOrAntiJoin := leftChild.JoinType&(plan.Node_SEMI|plan.Node_ANTI) != 0
	if isSemiOrAntiJoin {
		projectResultLen = len(leftChild.ProjectList)
	}

	// add new projectionNode after joinNode
	projectNode := &Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{node.NodeId},
		ProjectList: make([]*plan.Expr, projectResultLen),
		FilterList:  projectNodeWhere,
	}

	node.ProjectList = make([]*Expr, projectResultLen)
	joinNodeIdx := 0
	projNodeIdx := usingColsLen
	for i, expr := range leftChild.ProjectList {
		colExpr := &Expr{
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

		colIdx, ok := usingCols[expr.ColName]
		// we pop using columns to top.
		// R(r1, r2, b, r3, a),   S(s1, a, b, s2)
		// select * from R join S using(a, b)
		// we get projectList as :  [a, b, r1, r2, r3, s1, s2]
		if ok {
			node.ProjectList[colIdx] = DeepCopyExpr(colExpr)
			projectNode.ProjectList[colIdx] = DeepCopyExpr(colExpr)
		} else {
			node.ProjectList[projNodeIdx] = DeepCopyExpr(colExpr)
			projectNode.ProjectList[projNodeIdx] = DeepCopyExpr(colExpr)
			projNodeIdx++
		}
		joinNodeIdx++
	}

	for i, expr := range rightChild.ProjectList {
		// semi join or anti join, rightChild.ProjectList will abandon in the ProjectNode(after JoinNode)
		if !isSemiOrAntiJoin {
			// other join, we coalesced the using cols in the ProjectNode(after JoinNode)
			_, ok := usingCols[expr.ColName]
			if ok {
				binderCtx.usingCols[expr.ColName] = expr.TableName
			} else {
				node.ProjectList[projNodeIdx] = &Expr{
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

				projectNode.ProjectList[projNodeIdx] = &Expr{
					Typ:       expr.Typ,
					TableName: expr.TableName,
					ColName:   expr.ColName,
					Expr: &plan.Expr_Col{
						Col: &ColRef{
							RelPos: 0,
							ColPos: int32(projNodeIdx),
						},
					},
				}
				projNodeIdx++
			}
		}

		joinNodeIdx++
	}
	return projectNode
}

func buildUnresolvedName(query *Query, node *Node, colName string, tableName string, binderCtx *BinderContext) (*Expr, error) {
	colRef := &ColRef{
		RelPos: -1,
		ColPos: -1,
	}
	colExpr := &Expr{
		Expr: &plan.Expr_Col{
			Col: colRef,
		},
	}

	if colTblName, ok := binderCtx.usingCols[colName]; ok {
		if colTblName == tableName {
			tableName = ""
		}
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
						return nil, errors.New(errno.AmbiguousColumn, fmt.Sprintf("column reference '%v' is ambiguous", colName))
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
						return nil, errors.New(errno.AmbiguousColumn, fmt.Sprintf("column reference '%v' is ambiguous", colName))
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
		return nil, errors.New(errno.InvalidColumnReference, fmt.Sprintf("column '%v' does not exist", colName))
	}

	// Search from parent queries
	corrRef := &plan.CorrColRef{
		RelPos: -1,
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
				if corrRef.RelPos != -1 {
					return nil, errors.New(errno.InvalidColumnReference, fmt.Sprintf("column '%v' in the field list is ambiguous", colName))
				}
				corrRef.RelPos = parentId
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

	return nil, errors.New(errno.InvalidColumnReference, fmt.Sprintf("column '%v' does not exist", colName))
}

//if table == ""  => select * from tbl
//if table is not empty => select a.* from a,b on a.id = b.id
func unfoldStar(query *Query, node *Node, list *plan.ExprList, table string) error {
	// if node is TABLE_SCAN, we unfold star from tableDef (because this node have no children)
	if node.NodeType == plan.Node_TABLE_SCAN {
		if len(table) == 0 || table == node.TableDef.Name {
			for i, col := range node.TableDef.Cols {
				list.List = append(list.List, &Expr{
					Typ:       col.Typ,
					ColName:   col.Name,
					TableName: node.TableDef.Name,
					Expr: &plan.Expr_Col{
						Col: &ColRef{
							RelPos: 0,
							ColPos: int32(i),
						},
					},
				})
			}
		}
	} else {
		// other case, we unfold star from Children's projectionList. like unresolvename
		for idx, child := range node.Children {
			for _, col := range query.Nodes[child].ProjectList {
				if len(table) == 0 || table == col.TableName {
					if idx > 0 {
						// like `select * from a join b`.  we unfoldStar in joinNode,
						// we will reset col.ColPos to child's index
						resetColPos(col, int32(idx))
					}
					list.List = append(list.List, col)
				}
			}
		}
	}

	return nil
}

func resetColPos(col *plan.Expr, newPos int32) {
	switch expr := col.Expr.(type) {
	case *plan.Expr_Col:
		expr.Col.ColPos = newPos
	case *plan.Expr_F:
		for _, e := range expr.F.Args {
			resetColPos(e, newPos)
		}
	}
}

func getResolveTable(dbName string, tableName string, ctx CompilerContext, binderCtx *BinderContext) (*ObjectRef, *TableDef, bool) {
	// get table from context
	objRef, tableDef := ctx.Resolve(dbName, tableName)
	if tableDef != nil {
		return objRef, tableDef, false
	}

	// get table from CTE
	tableDef, ok := binderCtx.cteTables[tableName]
	if ok {
		objRef = &ObjectRef{
			SchemaName: dbName,
			ObjName:    tableName,
		}
		return objRef, tableDef, true
	}
	return nil, nil, false
}

func getTypeFromAst(typ tree.ResolvableTypeReference) (*plan.Type, error) {
	if n, ok := typ.(*tree.T); ok {
		switch uint8(n.InternalType.Oid) {
		case defines.MYSQL_TYPE_TINY:
			if n.InternalType.Unsigned {
				return &plan.Type{Id: plan.Type_UINT8, Width: n.InternalType.Width, Size: 1}, nil
			}
			return &plan.Type{Id: plan.Type_INT8, Width: n.InternalType.Width, Size: 1}, nil
		case defines.MYSQL_TYPE_SHORT:
			if n.InternalType.Unsigned {
				return &plan.Type{Id: plan.Type_UINT16, Width: n.InternalType.Width, Size: 2}, nil
			}
			return &plan.Type{Id: plan.Type_INT16, Width: n.InternalType.Width, Size: 2}, nil
		case defines.MYSQL_TYPE_LONG:
			if n.InternalType.Unsigned {
				return &plan.Type{Id: plan.Type_UINT32, Width: n.InternalType.Width, Size: 4}, nil
			}
			return &plan.Type{Id: plan.Type_INT32, Width: n.InternalType.Width, Size: 4}, nil
		case defines.MYSQL_TYPE_LONGLONG:
			if n.InternalType.Unsigned {
				return &plan.Type{Id: plan.Type_UINT64, Width: n.InternalType.Width, Size: 8}, nil
			}
			return &plan.Type{Id: plan.Type_INT64, Width: n.InternalType.Width, Size: 8}, nil
		case defines.MYSQL_TYPE_FLOAT:
			return &plan.Type{Id: plan.Type_FLOAT32, Width: n.InternalType.Width, Size: 4, Precision: n.InternalType.Precision}, nil
		case defines.MYSQL_TYPE_DOUBLE:
			return &plan.Type{Id: plan.Type_FLOAT64, Width: n.InternalType.Width, Size: 8, Precision: n.InternalType.Precision}, nil
		case defines.MYSQL_TYPE_STRING:
			width := n.InternalType.DisplayWith
			if width == -1 {
				// create table t1(a char) -> DisplayWith = -1；but get width=1 in MySQL and PgSQL
				width = 1
			}
			if n.InternalType.FamilyString == "char" { // type char
				return &plan.Type{Id: plan.Type_CHAR, Size: 24, Width: width}, nil
			}
			return &plan.Type{Id: plan.Type_VARCHAR, Size: 24, Width: width}, nil
		case defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_VARCHAR:
			width := n.InternalType.DisplayWith
			if width == -1 {
				// create table t1(a char) -> DisplayWith = -1；but get width=1 in MySQL and PgSQL
				width = 1
			}
			if n.InternalType.FamilyString == "char" { // type char
				return &plan.Type{Id: plan.Type_CHAR, Size: 24, Width: width}, nil
			}
			return &plan.Type{Id: plan.Type_VARCHAR, Size: 24, Width: width}, nil
		case defines.MYSQL_TYPE_DATE:
			return &plan.Type{Id: plan.Type_DATE, Size: 4}, nil
		case defines.MYSQL_TYPE_DATETIME:
			// currently the ast's width for datetime's is 26, this is not accurate and may need revise, not important though, as we don't need it anywhere else except to differentiate empty vector.Typ.
			return &plan.Type{Id: plan.Type_DATETIME, Size: 8, Width: n.InternalType.Width, Precision: n.InternalType.Precision}, nil
		case defines.MYSQL_TYPE_TIMESTAMP:
			return &plan.Type{Id: plan.Type_TIMESTAMP, Size: 8, Width: n.InternalType.Width, Precision: n.InternalType.Precision}, nil
		case defines.MYSQL_TYPE_DECIMAL:
			if n.InternalType.DisplayWith > 18 {
				return &plan.Type{Id: plan.Type_DECIMAL128, Size: 16, Width: n.InternalType.DisplayWith, Scale: n.InternalType.Precision}, nil
			}
			return &plan.Type{Id: plan.Type_DECIMAL64, Size: 8, Width: n.InternalType.DisplayWith, Scale: n.InternalType.Precision}, nil
		case defines.MYSQL_TYPE_BOOL:
			return &plan.Type{Id: plan.Type_BOOL, Size: 1}, nil
		default:
			return nil, errors.New("", fmt.Sprintf("Data type: '%s', will be supported in future version.", tree.String(&n.InternalType, dialect.MYSQL)))
		}
	}
	return nil, errors.New(errno.IndeterminateDatatype, "Unknown data type.")
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
			if nullAttr, ok := attr.(*tree.AttributeNull); ok && !nullAttr.Is {
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
					Value:  nil,
					IsNull: true,
				}, nil
			}

			value, err := buildConstant(typ, d.Expr)
			if err != nil {
				return nil, errors.New(errno.InvalidColumnDefinition, fmt.Sprintf("Invalid default value for '%s'", column.Name.Parts[0]))
			}
			_, err = rangeCheck(value, typ, "", 0)
			if err != nil {
				return nil, errors.New(errno.InvalidColumnDefinition, fmt.Sprintf("Invalid default value for '%s'", column.Name.Parts[0]))
			}
			constantValue := convertToPlanValue(value)
			return &plan.DefaultExpr{
				Exist:  true,
				Value:  constantValue,
				IsNull: false,
			}, nil
		}
	}
	if allowNull {
		return &plan.DefaultExpr{
			Exist:  true,
			Value:  nil,
			IsNull: true,
		}, nil
	}
	return &plan.DefaultExpr{
		Exist: false,
	}, nil
}

func convertToPlanValue(value interface{}) *plan.ConstantValue {
	switch v := value.(type) {
	case bool:
		return &plan.ConstantValue{
			ConstantValue: &plan.ConstantValue_BoolV{BoolV: v},
		}
	case int64:
		return &plan.ConstantValue{
			ConstantValue: &plan.ConstantValue_Int64V{Int64V: v},
		}
	case uint64:
		return &plan.ConstantValue{
			ConstantValue: &plan.ConstantValue_Uint64V{Uint64V: v},
		}
	case float32:
		return &plan.ConstantValue{
			ConstantValue: &plan.ConstantValue_Float32V{Float32V: v},
		}
	case float64:
		return &plan.ConstantValue{
			ConstantValue: &plan.ConstantValue_Float64V{Float64V: v},
		}
	case string:
		return &plan.ConstantValue{
			ConstantValue: &plan.ConstantValue_StringV{StringV: v},
		}
	case types.Date:
		return &plan.ConstantValue{
			ConstantValue: &plan.ConstantValue_DateV{DateV: int32(v)},
		}
	case types.Datetime:
		return &plan.ConstantValue{
			ConstantValue: &plan.ConstantValue_DateTimeV{DateTimeV: int64(v)},
		}
	case types.Timestamp:
		return &plan.ConstantValue{
			ConstantValue: &plan.ConstantValue_TimeStampV{TimeStampV: int64(v)},
		}
	case types.Decimal64:
		return &plan.ConstantValue{
			ConstantValue: &plan.ConstantValue_Decimal64V{Decimal64V: int64(v)},
		}
	case types.Decimal128:
		return &plan.ConstantValue{
			ConstantValue: &plan.ConstantValue_Decimal128V{Decimal128V: &plan.Decimal128{
				Lo: v.Lo,
				Hi: v.Hi,
			}},
		}
	}
	return &plan.ConstantValue{
		ConstantValue: &plan.ConstantValue_UnknownV{UnknownV: 1},
	}
}

// rangeCheck do range check for value, and do type conversion.
func rangeCheck(value interface{}, typ *plan.Type, columnName string, rowNumber int) (interface{}, error) {
	errString := "Out of range value for column '%s' at row %d"

	switch v := value.(type) {
	case int64:
		switch typ.GetId() {
		case plan.Type_INT8:
			if v <= math.MaxInt8 && v >= math.MinInt8 {
				return v, nil
			}
		case plan.Type_INT16:
			if v <= math.MaxInt16 && v >= math.MinInt16 {
				return v, nil
			}
		case plan.Type_INT32:
			if v <= math.MaxInt32 && v >= math.MinInt32 {
				return v, nil
			}
		case plan.Type_INT64:
			return v, nil
		default:
			return nil, errors.New(errno.DatatypeMismatch, "unexpected type and value")
		}
		return nil, errors.New(errno.DataException, fmt.Sprintf(errString, columnName, rowNumber))
	case uint64:
		switch typ.GetId() {
		case plan.Type_UINT8:
			if v <= math.MaxUint8 {
				return v, nil
			}
		case plan.Type_UINT16:
			if v <= math.MaxUint16 {
				return v, nil
			}
		case plan.Type_UINT32:
			if v <= math.MaxUint32 {
				return v, nil
			}
		case plan.Type_UINT64:
			return v, nil
		default:
			return nil, errors.New(errno.DatatypeMismatch, "unexpected type and value")
		}
		return nil, errors.New(errno.DataException, fmt.Sprintf(errString, columnName, rowNumber))
	case float32:
		if typ.GetId() == plan.Type_FLOAT32 {
			return v, nil
		}
		return nil, errors.New(errno.DatatypeMismatch, "unexpected type and value")
	case float64:
		switch typ.GetId() {
		case plan.Type_FLOAT32:
			if v <= math.MaxFloat32 && v >= -math.MaxFloat32 {
				return v, nil
			}
		case plan.Type_FLOAT64:
			return v, nil
		default:
			return nil, errors.New(errno.DatatypeMismatch, "unexpected type and value")
		}
		return nil, errors.New(errno.DataException, fmt.Sprintf(errString, columnName, rowNumber))
	case string:
		switch typ.GetId() {
		case plan.Type_CHAR, plan.Type_VARCHAR: // string family should compare the length but not value
			if len(v) > math.MaxUint16 {
				return nil, errors.New(errno.DataException, "length out of uint16 is unexpected for char / varchar value")
			}
			if len(v) <= int(typ.Width) {
				return v, nil
			}
		default:
			return nil, errors.New(errno.DatatypeMismatch, "unexpected type and value")
		}
		return nil, errors.New(errno.DataException, fmt.Sprintf("Data too long for column '%s' at row %d", columnName, rowNumber))
	case bool, types.Date, types.Datetime, types.Timestamp, types.Decimal64, types.Decimal128:
		return v, nil
	default:
		return nil, errors.New(errno.DatatypeMismatch, "unexpected type and value")
	}
}

var (
	// errors may happen while building constant
	ErrDivByZero        = errors.New(errno.SyntaxErrororAccessRuleViolation, "division by zero")
	ErrZeroModulus      = errors.New(errno.SyntaxErrororAccessRuleViolation, "zero modulus")
	errConstantOutRange = errors.New(errno.DataException, "constant value out of range")
	errBinaryOutRange   = errors.New(errno.DataException, "binary result out of range")
	errUnaryOutRange    = errors.New(errno.DataException, "unary result out of range")
)

func buildConstant(typ *plan.Type, n tree.Expr) (interface{}, error) {
	switch e := n.(type) {
	case *tree.ParenExpr:
		return buildConstant(typ, e.Expr)
	case *tree.NumVal:
		return buildConstantValue(typ, e)
	case *tree.UnaryExpr:
		if e.Op == tree.UNARY_PLUS {
			return buildConstant(typ, e.Expr)
		}
		if e.Op == tree.UNARY_MINUS {
			switch n := e.Expr.(type) {
			case *tree.NumVal:
				return buildConstantValue(typ, tree.NewNumVal(n.Value, "-"+n.String(), true))
			}

			v, err := buildConstant(typ, e.Expr)
			if err != nil {
				return nil, err
			}
			switch val := v.(type) {
			case int64:
				return val * -1, nil
			case uint64:
				if val != 0 {
					return nil, errUnaryOutRange
				}
			case float32:
				return val * -1, nil
			case float64:
				return val * -1, nil
			}
			return v, nil
		}
	case *tree.BinaryExpr:
		var floatResult float64
		var argTyp = &plan.Type{Id: plan.Type_FLOAT64, Size: 8}
		// build values of Part left and Part right.
		left, err := buildConstant(argTyp, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := buildConstant(argTyp, e.Right)
		if err != nil {
			return nil, err
		}
		// evaluate the result and make sure binary result is within range of float64.
		lf, rf := left.(float64), right.(float64)
		switch e.Op {
		case tree.PLUS:
			floatResult = lf + rf
			if lf > 0 && rf > 0 && floatResult <= 0 {
				return nil, errBinaryOutRange
			}
			if lf < 0 && rf < 0 && floatResult >= 0 {
				return nil, errBinaryOutRange
			}
		case tree.MINUS:
			floatResult = lf - rf
			if lf < 0 && rf > 0 && floatResult >= 0 {
				return nil, errBinaryOutRange
			}
			if lf > 0 && rf < 0 && floatResult <= 0 {
				return nil, errBinaryOutRange
			}
		case tree.MULTI:
			floatResult = lf * rf
			if floatResult < 0 {
				if (lf > 0 && rf > 0) || (lf < 0 && rf < 0) {
					return nil, errBinaryOutRange
				}
			} else if floatResult > 0 {
				if (lf > 0 && rf < 0) || (lf < 0 && rf > 0) {
					return nil, errBinaryOutRange
				}
			}
		case tree.DIV:
			if rf == 0 {
				return nil, ErrDivByZero
			}
			floatResult = lf / rf
			if floatResult < 0 {
				if (lf > 0 && rf > 0) || (lf < 0 && rf < 0) {
					return nil, errBinaryOutRange
				}
			} else if floatResult > 0 {
				if (lf > 0 && rf < 0) || (lf < 0 && rf > 0) {
					return nil, errBinaryOutRange
				}
			}
		case tree.INTEGER_DIV:
			if rf == 0 {
				return nil, ErrDivByZero
			}
			tempResult := lf / rf
			if tempResult > math.MaxInt64 || tempResult < math.MinInt64 {
				return nil, errBinaryOutRange
			}
			floatResult = float64(int64(tempResult))
		case tree.MOD:
			if rf == 0 {
				return nil, ErrZeroModulus
			}
			tempResult := int(lf / rf)
			floatResult = lf - float64(tempResult)*rf
		default:
			return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e.Op))
		}
		// buildConstant should make sure result is within int64 or uint64 or float32 or float64
		switch typ.GetId() {
		case plan.Type_INT8, plan.Type_INT16, plan.Type_INT32, plan.Type_INT64:
			if floatResult > 0 {
				if floatResult+0.5 > math.MaxInt64 {
					return nil, errBinaryOutRange
				}
				return int64(floatResult + 0.5), nil
			} else if floatResult < 0 {
				if floatResult-0.5 < math.MinInt64 {
					return nil, errBinaryOutRange
				}
				return int64(floatResult - 0.5), nil
			}
			return int64(floatResult), nil
		case plan.Type_UINT8, plan.Type_UINT16, plan.Type_UINT32, plan.Type_UINT64:
			if floatResult < 0 || floatResult+0.5 > math.MaxInt64 {
				return nil, errBinaryOutRange
			}
			return uint64(floatResult + 0.5), nil
		case plan.Type_FLOAT32:
			if floatResult == 0 {
				return float32(0), nil
			}
			if floatResult > math.MaxFloat32 || floatResult < -math.MaxFloat32 {
				return nil, errBinaryOutRange
			}
			return float32(floatResult), nil
		case plan.Type_FLOAT64:
			return floatResult, nil
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("unexpected return type '%v' for binary expression '%v'", typ, e.Op))
		}
	case *tree.UnresolvedName:
		floatResult, err := strconv.ParseFloat(e.Parts[0], 64)
		if err != nil {
			return nil, err
		}
		switch typ.GetId() {
		case plan.Type_INT8, plan.Type_INT16, plan.Type_INT32, plan.Type_INT64:
			if floatResult > 0 {
				if floatResult+0.5 > math.MaxInt64 {
					return nil, errBinaryOutRange
				}
				return int64(floatResult + 0.5), nil
			} else if floatResult < 0 {
				if floatResult-0.5 < math.MinInt64 {
					return nil, errBinaryOutRange
				}
				return int64(floatResult - 0.5), nil
			}
			return int64(floatResult), nil
		case plan.Type_UINT8, plan.Type_UINT16, plan.Type_UINT32, plan.Type_UINT64:
			if floatResult < 0 || floatResult+0.5 > math.MaxInt64 {
				return nil, errBinaryOutRange
			}
			return uint64(floatResult + 0.5), nil
		case plan.Type_FLOAT32:
			if floatResult == 0 {
				return float32(0), nil
			}
			if floatResult > math.MaxFloat32 || floatResult < -math.MaxFloat32 {
				return nil, errBinaryOutRange
			}
			return float32(floatResult), nil
		case plan.Type_FLOAT64:
			return floatResult, nil
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("unexpected return type '%v' for binary expression '%v'", typ, floatResult))
		}
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
}

func buildConstantValue(typ *plan.Type, num *tree.NumVal) (interface{}, error) {
	val := num.Value
	str := num.String()

	switch val.Kind() {
	case constant.Unknown:
		return nil, nil
	case constant.Bool:
		return constant.BoolVal(val), nil
	case constant.Int:
		switch typ.GetId() {
		case plan.Type_INT8, plan.Type_INT16, plan.Type_INT32, plan.Type_INT64:
			if num.Negative() {
				v, _ := constant.Uint64Val(val)
				if v > -math.MinInt64 {
					return nil, errConstantOutRange
				}
				return int64(-v), nil
			} else {
				v, _ := constant.Int64Val(val)
				if v < 0 {
					return nil, errConstantOutRange
				}
				return int64(v), nil
			}
		case plan.Type_DECIMAL64:
			return types.ParseStringToDecimal64(str, typ.Width, typ.Scale)
		case plan.Type_DECIMAL128:
			return types.ParseStringToDecimal128(str, typ.Width, typ.Scale)
		case plan.Type_UINT8, plan.Type_UINT16, plan.Type_UINT32, plan.Type_UINT64:
			v, _ := constant.Uint64Val(val)
			if num.Negative() {
				if v != 0 {
					return nil, errConstantOutRange
				}
			}
			return uint64(v), nil
		case plan.Type_FLOAT32:
			v, _ := constant.Float32Val(val)
			if num.Negative() {
				return float32(-v), nil
			}
			return float32(v), nil
		case plan.Type_FLOAT64:
			v, _ := constant.Float64Val(val)
			if num.Negative() {
				return float64(-v), nil
			}
			return float64(v), nil
		case plan.Type_TIMESTAMP:
			return types.ParseTimestamp(str, typ.Precision)
		}
	case constant.Float:
		switch typ.GetId() {
		case plan.Type_INT64, plan.Type_INT32, plan.Type_INT16, plan.Type_INT8:
			parts := strings.Split(str, ".")
			if len(parts) <= 1 { // integer constant within int64 range will be constant.Int but not constant.Float.
				return nil, errConstantOutRange
			}
			v, err := strconv.ParseInt(parts[0], 10, 64)
			if err != nil {
				return nil, errConstantOutRange
			}
			if len(parts[1]) > 0 && parts[1][0] >= '5' {
				if num.Negative() {
					if v-1 > v {
						return nil, errConstantOutRange
					}
					v--
				} else {
					if v+1 < v {
						return nil, errConstantOutRange
					}
					v++
				}
			}
			return v, nil
		case plan.Type_UINT64, plan.Type_UINT32, plan.Type_UINT16, plan.Type_UINT8:
			parts := strings.Split(str, ".")
			v, err := strconv.ParseUint(parts[0], 10, 64)
			if err != nil || len(parts) == 1 {
				return v, errConstantOutRange
			}
			if len(parts[1]) > 0 && parts[1][0] >= '5' {
				if v+1 < v {
					return nil, errConstantOutRange
				}
				v++
			}
			return v, nil
		case plan.Type_FLOAT32:
			v, _ := constant.Float32Val(val)
			if num.Negative() {
				return float32(-v), nil
			}
			return float32(v), nil
		case plan.Type_FLOAT64:
			v, _ := constant.Float64Val(val)
			if num.Negative() {
				return float64(-v), nil
			}
			return float64(v), nil
		case plan.Type_DECIMAL64:
			return types.ParseStringToDecimal64(str, typ.Width, typ.Scale)
		case plan.Type_DECIMAL128:
			return types.ParseStringToDecimal128(str, typ.Width, typ.Scale)
		}
	case constant.String:
		switch typ.GetId() {
		case plan.Type_BOOL:
			switch strings.ToLower(str) {
			case "false":
				return false, nil
			case "true":
				return true, nil
			}
		case plan.Type_INT8:
			return strconv.ParseInt(str, 10, 8)
		case plan.Type_INT16:
			return strconv.ParseInt(str, 10, 16)
		case plan.Type_INT32:
			return strconv.ParseInt(str, 10, 32)
		case plan.Type_INT64:
			return strconv.ParseInt(str, 10, 64)
		case plan.Type_UINT8:
			return strconv.ParseUint(str, 10, 8)
		case plan.Type_UINT16:
			return strconv.ParseUint(str, 10, 16)
		case plan.Type_UINT32:
			return strconv.ParseUint(str, 10, 32)
		case plan.Type_UINT64:
			return strconv.ParseUint(str, 10, 64)
		case plan.Type_FLOAT32:
			val, err := strconv.ParseFloat(str, 32)
			if err != nil {
				return nil, err
			}
			return float32(val), nil
		case plan.Type_FLOAT64:
			val, err := strconv.ParseFloat(str, 64)
			if err != nil {
				return nil, err
			}
			return val, nil
		case plan.Type_DECIMAL64:
			return types.ParseStringToDecimal64(str, typ.Width, typ.Scale)
		case plan.Type_DECIMAL128:
			return types.ParseStringToDecimal128(str, typ.Width, typ.Scale)
		}
		if !num.Negative() {
			switch typ.GetId() {
			case plan.Type_CHAR, plan.Type_VARCHAR:
				return constant.StringVal(val), nil
			case plan.Type_DATE:
				return types.ParseDate(constant.StringVal(val))
			case plan.Type_DATETIME:
				return types.ParseDatetime(constant.StringVal(val), typ.Precision)
			case plan.Type_TIMESTAMP:
				return types.ParseTimestamp(constant.StringVal(val), typ.Precision)
			}
		}
	}
	return nil, errors.New(errno.IndeterminateDatatype, fmt.Sprintf("unsupport value: %v", val))
}
