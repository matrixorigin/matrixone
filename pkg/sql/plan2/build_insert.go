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

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildInsert(stmt *tree.Insert, ctx CompilerContext) (p *Plan, err error) {
	query, _ := newQueryAndSelectCtx(plan.Query_INSERT)

	// get table
	objRef, tableDef, err := getInsertTable(stmt.Table, ctx, query)
	if err != nil {
		return nil, err
	}

	// get columns
	getColDef := func(name string) (*ColDef, error) {
		for _, col := range tableDef.Cols {
			if col.Name == name {
				return col, nil
			}
		}
		return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("column %T not exist", name))
	}
	var columns []*ColDef
	if stmt.Columns == nil {
		columns = tableDef.Cols
	} else {
		for _, identifier := range stmt.Columns {
			columnName := string(identifier)
			col, err := getColDef(columnName)
			if err != nil {
				return nil, err
			}
			columns = append(columns, col)
		}
	}
	columnCount := len(columns)
	rowset := &RowsetData{
		Schema: &TableDef{
			Name: tableDef.Name,
			Cols: columns,
		},
		Cols: make([]*plan.ColData, columnCount),
	}

	var nodeId int32
	// get rows
	switch rows := stmt.Rows.Select.(type) {
	case *tree.Select:
		binderCtx := &BinderContext{
			columnAlias: make(map[string]*Expr),
		}
		nodeId, err = buildSelect(rows, ctx, query, binderCtx)
		if err != nil {
			return
		}
		// check selectNode's projectionList match rowset.Schema
		selectNode := query.Nodes[nodeId]
		if len(selectNode.ProjectList) != columnCount {
			return nil, errors.New(errno.InvalidColumnDefinition, "insert column length does not match value length")
		}
		// TODO: Now MO don't check projectionList type match rowset type just like MySQL。
	case *tree.ValuesClause:
		rowCount := len(rows.Rows)
		for idx := range rowset.Cols {
			rowset.Cols[idx] = &plan.ColData{
				RowCount: int32(rowCount),
			}
		}
		err = getValues(rowset, rows, columnCount)
		if err != nil {
			return
		}
		node := &Node{
			NodeType:   plan.Node_VALUE_SCAN,
			RowsetData: rowset,
		}
		nodeId = appendQueryNode(query, node)
	default:
		return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport rows expr: %T", stmt))
	}

	node := &Node{
		NodeType: plan.Node_INSERT,
		ObjRef:   objRef,
		TableDef: tableDef,
		Children: []int32{nodeId},
	}
	appendQueryNode(query, node)

	preNode := query.Nodes[len(query.Nodes)-1]
	if len(query.Steps) > 0 {
		query.Steps[len(query.Steps)-1] = preNode.NodeId
	} else {
		query.Steps = append(query.Steps, preNode.NodeId)
	}

	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, nil
}

func getValues(rowset *RowsetData, rows *tree.ValuesClause, columnCount int) error {
	setColData := func(col *plan.ColData, typ *plan.Type, val constant.Value) error {
		switch val.Kind() {
		case constant.Int:
			switch typ.Id {
			case plan.Type_INT8, plan.Type_INT16, plan.Type_INT32, plan.Type_UINT8, plan.Type_UINT16:
				colVal, ok := constant.Int64Val(val)
				if !ok {
					return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("can not cast [%+v] as i64", val))
				}
				col.I32 = append(col.I32, int32(colVal))
				return nil
			case plan.Type_INT64, plan.Type_INT128, plan.Type_UINT32, plan.Type_UINT64, plan.Type_UINT128:
				colVal, ok := constant.Int64Val(val)
				if !ok {
					return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("can not cast [%+v] as  i64", val))
				}
				col.I64 = append(col.I64, colVal)
				return nil
			default:
				return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("[%v] type must be int", val))
			}
		case constant.Float:
			switch typ.Id {
			case plan.Type_FLOAT32:
				colVal, ok := constant.Float32Val(val)
				if !ok {
					return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("can not cast [%+v] as  f32", val))
				}
				col.F32 = append(col.F32, colVal)
				return nil
			case plan.Type_FLOAT64, plan.Type_DECIMAL, plan.Type_DECIMAL64, plan.Type_DECIMAL128:
				colVal, ok := constant.Float64Val(val)
				if !ok {
					return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("can not cast [%+v] as  f64", val))
				}
				col.F64 = append(col.F64, colVal)
				return nil
			default:
				return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("[%v] type must be float", val))
			}
		case constant.String:
			colVal := constant.StringVal(val)
			col.S = append(col.S, colVal)
			return nil
		default:
			return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupport value: %v", val))
		}
	}

	for rowIdx, row := range rows.Rows {
		if columnCount != len(row) {
			return errors.New(errno.InvalidColumnReference, fmt.Sprintf("Column count doesn't match value count at row '%v'", rowIdx))
		}

		for idx, colExpr := range row {
			switch astExpr := colExpr.(type) {
			case *tree.NumVal:
				err := setColData(rowset.Cols[idx], rowset.Schema.Cols[idx].Typ, astExpr.Value)
				if err != nil {
					return err
				}
			default:
				return errors.New(errno.InvalidSchemaName, "insert value must be constant")
			}
		}
	}

	return nil
}

func getInsertTable(stmt tree.TableExpr, ctx CompilerContext, query *Query) (*ObjectRef, *TableDef, error) {
	switch tbl := stmt.(type) {
	case *tree.TableName:
		tblName := string(tbl.ObjectName)
		dbName := string(tbl.SchemaName)
		objRef, tableDef := ctx.Resolve(dbName, tblName)
		if tableDef == nil {
			return nil, nil, errors.New(errno.InvalidSchemaName, fmt.Sprintf("table '%v' is not exist", tblName))
		}
		return objRef, tableDef, nil
	case *tree.ParenTableExpr:
		return getInsertTable(tbl.Expr, ctx, query)
	case *tree.AliasedTableExpr:
		return getInsertTable(tbl.Expr, ctx, query)
	case *tree.Select:
		return nil, nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table expr: %T", stmt))
	case *tree.StatementSource:
		return nil, nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table expr: %T", stmt))
	default:
		return nil, nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table expr: %T", stmt))
	}
}
