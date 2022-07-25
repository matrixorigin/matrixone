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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildInsert(stmt *tree.Insert, ctx CompilerContext) (p *Plan, err error) {
	rows := stmt.Rows
	switch t := rows.Select.(type) {
	case *tree.ValuesClause:
		return buildInsertValues(stmt, ctx)
	case *tree.SelectClause, *tree.ParenSelect:
		return buildInsertSelect(stmt, ctx)
	default:
		return nil, errors.New(errno.FeatureNotSupported, fmt.Sprintf("unknown select statement: %v", t))
	}
}

func buildInsertValues(_ *tree.Insert, _ CompilerContext) (p *Plan, err error) {
	return nil, errors.New(errno.FeatureNotSupported, "building plan for insert values is not supported now")
}

func buildInsertSelect(stmt *tree.Insert, ctx CompilerContext) (p *Plan, err error) {
	pn, err := runBuildSelectByBinder(plan.Query_SELECT, ctx, stmt.Rows)
	if err != nil {
		return nil, err
	}
	cols := GetResultColumnsFromPlan(pn)
	pn.Plan.(*plan.Plan_Query).Query.StmtType = plan.Query_INSERT
	if len(stmt.Columns) != 0 && len(stmt.Columns) != len(cols) {
		return nil, errors.New(errno.InvalidColumnReference, "Column count doesn't match value count")
	}

	objRef, tableDef, err := getInsertTable(stmt.Table, ctx)
	if err != nil {
		return nil, err
	}
	valueCount := len(stmt.Columns)
	if len(stmt.Columns) == 0 {
		valueCount = len(tableDef.Cols)
	}
	if valueCount != len(cols) {
		return nil, errors.New(errno.InvalidColumnReference, "Column count doesn't match value count")
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
	appendQueryNode(qry, n)
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
		// check if the column name is legal
		for k := range targetMap {
			if _, ok := tableColMap[k]; !ok {
				return nil, errors.New(errno.InvalidColumnReference, fmt.Sprintf("column '%s' not exist", k))
			}
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
				exprs[i], err = generateDefaultExpr(tableDef.Cols[i].Default, tableDef.Cols[i].Typ)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return exprs, nil
}

func generateDefaultExpr(e *plan.DefaultExpr, t *plan.Type) (*Expr, error) {
	if e == nil || !e.Exist || e.IsNull {
		return &plan.Expr{
			Typ: t,
			Expr: &plan.Expr_C{
				C: &Const{Isnull: true},
			},
		}, nil
	}
	var ret = plan.Expr{Typ: &plan.Type{
		Id:        t.Id,
		Nullable:  t.Nullable,
		Width:     t.Width,
		Precision: t.Precision,
		Size:      t.Size,
		Scale:     t.Scale,
	}}
	switch d := e.Value.ConstantValue.(type) {
	case *plan.ConstantValue_JsonV:
		ret.Expr = makePlan2JsonConstExpr(d.JsonV)
	case *plan.ConstantValue_BoolV:
		ret.Expr = makePlan2BoolConstExpr(d.BoolV)
	case *plan.ConstantValue_Int64V:
		ret.Expr = makePlan2Int64ConstExpr(d.Int64V)
		ret.Typ.Id = plan.Type_INT64
		ret.Typ.Size = 8
		ret.Typ.Width = 64
	case *plan.ConstantValue_Uint64V:
		ret.Expr = makePlan2Uint64ConstExpr(d.Uint64V)
		ret.Typ.Id = plan.Type_UINT64
		ret.Typ.Size = 8
		ret.Typ.Width = 64
	case *plan.ConstantValue_Float32V:
		ret.Expr = makePlan2Float32ConstExpr(d.Float32V)
	case *plan.ConstantValue_Float64V:
		ret.Expr = makePlan2Float64ConstExpr(d.Float64V)
	case *plan.ConstantValue_StringV:
		ret.Expr = makePlan2StringConstExpr(d.StringV)
		ret.Typ.Id = plan.Type_VARCHAR
	case *plan.ConstantValue_DateV:
		ret.Expr = makePlan2DateConstExpr(types.Date(d.DateV))
	case *plan.ConstantValue_DateTimeV:
		ret.Expr = makePlan2DatetimeConstExpr(types.Datetime(d.DateTimeV))
	case *plan.ConstantValue_Decimal64V:
		d64 := types.Decimal64FromInt64Raw(d.Decimal64V.A)
		ret.Expr = makePlan2Decimal64ConstExpr(d64)
	case *plan.ConstantValue_Decimal128V:
		d128 := types.Decimal128FromInt64Raw(d.Decimal128V.A, d.Decimal128V.B)
		ret.Expr = makePlan2Decimal128ConstExpr(d128)
	case *plan.ConstantValue_TimeStampV:
		ret.Expr = makePlan2TimestampConstExpr(types.Timestamp(d.TimeStampV))
	default:
		return nil, errors.New(errno.FeatureNotSupported, fmt.Sprintf("default type '%s' is not support now", t))
	}
	return &ret, nil
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
