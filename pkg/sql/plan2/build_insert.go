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

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildInsert(stmt *tree.Insert, ctx CompilerContext, query *Query) error {
	//get table
	objRef, tableDef, err := getInsertTable(stmt.Table, ctx, query)
	if err != nil {
		return err
	}

	//get columns
	getColDef := func(name string) (*plan.ColDef, error) {
		for _, col := range tableDef.Cols {
			if col.Name == name {
				return col, nil
			}
		}
		return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("column %T not exist", name))
	}
	var columns []*plan.ColDef
	if stmt.Columns == nil {
		columns = tableDef.Cols
	} else {
		for _, identifier := range stmt.Columns {
			columnName := string(identifier)
			col, err := getColDef(columnName)
			if err != nil {
				return err
			}
			columns = append(columns, col)
		}
	}
	rowset := &plan.RowsetData{
		Schema: &plan.TableDef{
			Name: tableDef.Name,
			Cols: columns,
		},
		Cols: make([]*plan.ColData, len(columns)),
	}

	//get rows
	columnCount := len(columns)
	switch rows := stmt.Rows.Select.(type) {
	case *tree.ValuesClause:
		rowCount := len(rows.Rows)
		for idx := range rowset.Cols {
			rowset.Cols[idx] = &plan.ColData{
				RowCount: int32(rowCount),
			}
		}
		err := getValues(rowset, rows, columnCount)
		if err != nil {
			return err
		}
	default:
		return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport rows expr: %T", stmt))
	}

	node := &plan.Node{
		NodeType:   plan.Node_INSERT,
		ObjRef:     objRef,
		TableDef:   tableDef,
		RowsetData: rowset,
	}
	appendQueryNode(query, node, true)

	return nil
}

func getValues(rowset *plan.RowsetData, rows *tree.ValuesClause, columnCount int) error {
	setColData := func(col *plan.ColData, typ *plan.Type, val constant.Value) error {
		switch val.Kind() {
		case constant.Int:
			switch typ.Id {
			case plan.Type_INT8:
				fallthrough
			case plan.Type_INT16:
				fallthrough
			case plan.Type_INT32:
				fallthrough
			case plan.Type_UINT8:
				fallthrough
			case plan.Type_UINT16:
				colVal, ok := constant.Int64Val(val)
				if !ok {
					return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("can not cast [%+v] as i64", val))
				}
				col.I32 = append(col.I32, int32(colVal))
				return nil
			case plan.Type_INT64:
				fallthrough
			case plan.Type_INT128:
				fallthrough
			case plan.Type_UINT32:
				fallthrough
			case plan.Type_UINT64:
				fallthrough
			case plan.Type_UINT128:
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
				col.F32 = append(col.F32, float32(colVal))
				return nil
			case plan.Type_FLOAT64:
				fallthrough
			case plan.Type_DECIMAL:
				fallthrough
			case plan.Type_DECIMAL64:
				fallthrough
			case plan.Type_DECIMAL128:
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
				return errors.New(errno.InvalidSchemaName, fmt.Sprintf("insert value must be constant"))
			}
		}
	}

	return nil
}

func getInsertTable(stmt tree.TableExpr, ctx CompilerContext, query *Query) (*plan.ObjectRef, *plan.TableDef, error) {
	switch tbl := stmt.(type) {
	case *tree.TableName:
		name := string(tbl.ObjectName)
		if len(tbl.SchemaName) > 0 {
			name = strings.Join([]string{string(tbl.SchemaName), name}, ".")
		}
		objRef, tableDef := ctx.Resolve(name)
		if tableDef == nil {
			return nil, nil, errors.New(errno.InvalidSchemaName, fmt.Sprintf("table '%v' is not exist", name))
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
