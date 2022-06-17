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
	pn, err := runBuildSelectByBinder(plan.Query_SELECT, ctx, stmt.Rows)
	if err != nil {
		return nil, err
	}
	cols := GetResultColumnsFromPlan(pn)
	pn.Plan.(*plan.Plan_Query).Query.StmtType = plan.Query_INSERT
	if len(stmt.Columns) != 0 && len(stmt.Columns) < len(cols) {
		return nil, errors.New(errno.InvalidColumnReference, "INSERT has more expressions than target columns")
	}
	objRef, tableDef, err := getInsertTable(stmt.Table, ctx)
	if err != nil {
		return nil, err
	}

	// generate values expr
	exprs, err := getInsertExprs(stmt, cols, tableDef)
	if err != nil {
		return nil, err
	}

	// do type cast if needed
	for i := range tableDef.Cols {
		exprs[i], err = makePlan2CastExpr(exprs[i], tableDef.Cols[i].Typ)
		if err != nil {
			return nil, err
		}
	}
	qry := pn.Plan.(*plan.Plan_Query).Query
	n := &Node{
		ObjRef:      objRef,
		TableDef:    tableDef,
		NodeType:    plan.Node_INSERT,
		NodeId:      int32(len(qry.Nodes)),
		Children:    []int32{qry.Steps[len(qry.Steps)-1]},
		ProjectList: exprs,
	}
	qry.Nodes = append(qry.Nodes, n)
	qry.Steps[len(qry.Steps)-1] = n.NodeId
	return pn, nil
}

func getInsertExprs(stmt *tree.Insert, cols []*ColDef, tableDef *TableDef) ([]*Expr, error) {
	var exprs []*Expr
	var err error

	if len(stmt.Columns) == 0 {
		exprs = make([]*Expr, len(cols))
		for i := range exprs {
			exprs[i] = &plan.Expr{
				Typ: cols[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						ColPos: int32(i),
					},
				},
			}
		}
	} else {
		exprs = make([]*Expr, len(tableDef.Cols))
		tableColMap := make(map[string]int)
		targetMap := make(map[string]int)
		for i, col := range stmt.Columns {
			targetMap[string(col)] = i
		}
		for i, col := range tableDef.Cols {
			tableColMap[col.GetName()] = i
		}
		for i := range exprs {
			if ref, ok := targetMap[tableDef.Cols[i].GetName()]; ok {
				exprs[i] = &plan.Expr{
					Typ: cols[ref].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							ColPos: int32(ref),
						},
					},
				}
			} else {
				// get default value, and init constant expr.
				exprs[i], err = getDefaultExpr(tableDef.Cols[i].Default, tableDef.Cols[i].Typ)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return exprs, nil
}

func getDefaultExpr(e *plan.DefaultExpr, t *plan.Type) (*Expr, error) {
	if e == nil || !e.Exist || e.IsNull {
		return &plan.Expr{
			Typ: t,
			Expr: &plan.Expr_C{
				C: &Const{Isnull: true},
			},
		}, nil
	}
	var ret plan.Expr
	switch d := e.Value.ConstantValue.(type) {
	case *plan.ConstantValue_BoolV:
		ret.Expr = makePlan2BoolConstExpr(d.BoolV)
	case *plan.ConstantValue_Int64V:
		ret.Expr = makePlan2Int64ConstExpr(d.Int64V)
	case *plan.ConstantValue_Uint64V:
	case *plan.ConstantValue_Float32V:
		ret.Expr = makePlan2Float64ConstExpr(float64(d.Float32V))
	case *plan.ConstantValue_Float64V:
		ret.Expr = makePlan2Float64ConstExpr(d.Float64V)
	case *plan.ConstantValue_StringV:
		ret.Expr = makePlan2StringConstExpr(d.StringV)
	case *plan.ConstantValue_DateV:
	case *plan.ConstantValue_DateTimeV:
	case *plan.ConstantValue_Decimal64V:
	case *plan.ConstantValue_Decimal128V:
	case *plan.ConstantValue_TimeStampV:
	default:
		return nil, errors.New(errno.FeatureNotSupported, fmt.Sprintf("default type '%s' is not support now", d))
	}
	return &ret, nil
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

func getInsertTable(stmt tree.TableExpr, ctx CompilerContext) (*ObjectRef, *TableDef, error) {
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
		return getInsertTable(tbl.Expr, ctx)
	case *tree.AliasedTableExpr:
		return getInsertTable(tbl.Expr, ctx)
	case *tree.Select:
		return nil, nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table expr: %T", stmt))
	case *tree.StatementSource:
		return nil, nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table expr: %T", stmt))
	default:
		return nil, nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table expr: %T", stmt))
	}
}
