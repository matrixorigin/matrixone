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

package frontend

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go/constant"
	"math"
	"strconv"
	"strings"
)

type InsertValues struct {
	tblName   string
	dbName    string
	currentDb string
	dataBatch *batch.Batch
	relation  engine.Relation
}

func (mce *MysqlCmdExecutor) handleInsertValues(stmt *tree.Insert, ts uint64) error {
	snapshot := mce.GetSession().GetTxnHandler().GetTxn().GetCtx()

	plan := &InsertValues{currentDb: mce.GetSession().GetDatabaseName()}

	if err := buildInsertValues(stmt, plan, mce.GetSession().GetStorage(), snapshot); err != nil {
		return err
	}

	defer plan.relation.Close(snapshot)
	if err := plan.relation.Write(ts, plan.dataBatch, snapshot); err != nil {
		return err
	}

	resp := NewOkResponse(uint64(vector.Length(plan.dataBatch.Vecs[0])), 0, 0, 0, int(COM_QUERY), "")
	if err := mce.GetSession().protocol.SendResponse(resp); err != nil {
		return fmt.Errorf("routine send response failed. error:%v ", err)
	}
	return nil
}

func getTableRef(tbl *tree.TableName, currentDB string, eg engine.Engine, snapshot engine.Snapshot) (string, string, engine.Relation, error) {
	if len(tbl.SchemaName) == 0 {
		tbl.SchemaName = tree.Identifier(currentDB)
	}

	db, err := eg.Database(string(tbl.SchemaName), snapshot)
	if err != nil {
		return "", "", nil, errors.New(errno.InvalidSchemaName, err.Error())
	}
	r, err := db.Relation(string(tbl.ObjectName), snapshot)
	if err != nil {
		return "", "", nil, errors.New(errno.UndefinedTable, err.Error())
	}
	return string(tbl.SchemaName), string(tbl.ObjectName), r, nil
}

func buildInsertValues(stmt *tree.Insert, plan *InsertValues, eg engine.Engine, snapshot engine.Snapshot) error {
	var attrs []string
	var bat *batch.Batch
	var rows *tree.ValuesClause

	// Unsupported Case
	if _, ok := stmt.Table.(*tree.TableName); !ok {
		return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table: '%v'", stmt.Table))
	}

	rows = stmt.Rows.Select.(*tree.ValuesClause)

	db, id, relation, err := getTableRef(stmt.Table.(*tree.TableName), plan.currentDb, eg, snapshot)
	if err != nil {
		return err
	}
	plan.tblName = id
	plan.relation = relation
	plan.dbName = db

	attrType := make(map[string]types.Type)   // Map from relation's attribute name to its type
	attrDefault := make(map[string]tree.Expr) // Map from relation's attribute name to its default value
	orderAttr := make([]string, 0, 32)        // order relation's attribute names
	{
		count := 0
		for _, def := range relation.TableDefs(snapshot) {
			if v, ok := def.(*engine.AttributeDef); ok {
				attrType[v.Attr.Name] = v.Attr.Type
				orderAttr = append(orderAttr, v.Attr.Name)
				if v.Attr.HasDefaultExpr() {
					value, null := v.Attr.GetDefaultExpr()
					attrDefault[v.Attr.Name] = makeExprFromVal(v.Attr.Type, value, null)
				}
				count++
			}
		}
		orderAttr = orderAttr[:count]
	}

	if len(stmt.Columns) > 0 {
		attrs = make([]string, len(stmt.Columns))
		for i, col := range stmt.Columns {
			attrs[i] = string(col)
		}
	} else {
		attrs = orderAttr // todo: need to use copy ?
	}
	// deal with default Expr
	rows.Rows, attrs, err = rewriteInsertRows(stmt.Columns == nil, attrs, orderAttr, rows.Rows, attrDefault)
	if err != nil {
		return err
	}

	for i, rows := range rows.Rows {
		if len(attrs) != len(rows) {
			return errors.New(errno.InvalidColumnReference, fmt.Sprintf("Column count doesn't match value count at row '%v'", i))
		}
	}

	bat = batch.New(true, attrs)
	for i, attr := range attrs {
		typ, ok := attrType[attr]
		if !ok {
			return errors.New(errno.UndefinedColumn, fmt.Sprintf("unknown column '%s' in 'filed list'", attrs[i]))
		}
		bat.Vecs[i] = vector.New(typ)
		delete(attrType, attr)
	}

	if len(rows.Rows) == 0 || len(rows.Rows[0]) == 0 {
		plan.dataBatch = bat
		return nil
	}

	// insert values for columns
	for i, vec := range bat.Vecs {
		switch vec.Typ.Oid {
		case types.T_bool:
			vs := make([]bool, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return err
					}
					if v == nil {
						nulls.Add(vec.Nsp, uint64(j))
					} else {
						if vv, err := rangeCheck(v.(bool), vec.Typ, bat.Attrs[i], j+1); err != nil {
							return err
						} else {
							vs[j] = vv.(bool)
						}
					}
				}
			}
			if err := vector.Append(vec, vs); err != nil {
				return err
			}
		case types.T_int8:
			vs := make([]int8, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return err
					}
					if v == nil {
						nulls.Add(vec.Nsp, uint64(j))
					} else {
						if vv, err := rangeCheck(v.(int64), vec.Typ, bat.Attrs[i], j+1); err != nil {
							return err
						} else {
							vs[j] = vv.(int8)
						}
					}
				}
			}
			if err := vector.Append(vec, vs); err != nil {
				return err
			}
		case types.T_int16:
			vs := make([]int16, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return err
					}
					if v == nil {
						nulls.Add(vec.Nsp, uint64(j))
					} else {
						if vv, err := rangeCheck(v.(int64), vec.Typ, bat.Attrs[i], j+1); err != nil {
							return err
						} else {
							vs[j] = vv.(int16)
						}
					}
				}
			}
			if err := vector.Append(vec, vs); err != nil {
				return err
			}
		case types.T_int32:
			vs := make([]int32, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return err
					}
					if v == nil {
						nulls.Add(vec.Nsp, uint64(j))
					} else {
						if vv, err := rangeCheck(v.(int64), vec.Typ, bat.Attrs[i], j+1); err != nil {
							return err
						} else {
							vs[j] = vv.(int32)
						}
					}
				}
			}
			if err := vector.Append(vec, vs); err != nil {
				return err
			}
		case types.T_int64:
			vs := make([]int64, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return err
					}
					if v == nil {
						nulls.Add(vec.Nsp, uint64(j))
					} else {
						if vv, err := rangeCheck(v.(int64), vec.Typ, bat.Attrs[i], j+1); err != nil {
							return err
						} else {
							vs[j] = vv.(int64)
						}
					}
				}
			}
			if err := vector.Append(vec, vs); err != nil {
				return err
			}
		case types.T_uint8:
			vs := make([]uint8, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return err
					}
					if v == nil {
						nulls.Add(vec.Nsp, uint64(j))
					} else {
						if vv, err := rangeCheck(v.(uint64), vec.Typ, bat.Attrs[i], j+1); err != nil {
							return err
						} else {
							vs[j] = vv.(uint8)
						}
					}
				}
			}
			if err := vector.Append(vec, vs); err != nil {
				return err
			}
		case types.T_uint16:
			vs := make([]uint16, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return err
					}
					if v == nil {
						nulls.Add(vec.Nsp, uint64(j))
					} else {
						if vv, err := rangeCheck(v.(uint64), vec.Typ, bat.Attrs[i], j+1); err != nil {
							return err
						} else {
							vs[j] = vv.(uint16)
						}
					}
				}
			}
			if err := vector.Append(vec, vs); err != nil {
				return err
			}
		case types.T_uint32:
			vs := make([]uint32, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return err
					}
					if v == nil {
						nulls.Add(vec.Nsp, uint64(j))
					} else {
						if vv, err := rangeCheck(v.(uint64), vec.Typ, bat.Attrs[i], j+1); err != nil {
							return err
						} else {
							vs[j] = vv.(uint32)
						}
					}
				}
			}
			if err := vector.Append(vec, vs); err != nil {
				return err
			}
		case types.T_uint64:
			vs := make([]uint64, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return err
					}
					if v == nil {
						nulls.Add(vec.Nsp, uint64(j))
					} else {
						if vv, err := rangeCheck(v.(uint64), vec.Typ, bat.Attrs[i], j+1); err != nil {
							return err
						} else {
							vs[j] = vv.(uint64)
						}
					}
				}
			}
			if err := vector.Append(vec, vs); err != nil {
				return err
			}
		case types.T_float32:
			vs := make([]float32, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return err
					}
					if v == nil {
						nulls.Add(vec.Nsp, uint64(j))
					} else {
						if vv, err := rangeCheck(v.(float32), vec.Typ, bat.Attrs[i], j+1); err != nil {
							return err
						} else {
							vs[j] = vv.(float32)
						}
					}
				}
			}
			if err := vector.Append(vec, vs); err != nil {
				return err
			}
		case types.T_float64:
			vs := make([]float64, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return err
					}
					if v == nil {
						nulls.Add(vec.Nsp, uint64(j))
					} else {
						if vv, err := rangeCheck(v.(float64), vec.Typ, bat.Attrs[i], j+1); err != nil {
							return err
						} else {
							vs[j] = vv.(float64)
						}
					}
				}
			}
			if err := vector.Append(vec, vs); err != nil {
				return err
			}
		case types.T_char, types.T_varchar:
			vs := make([][]byte, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return err
					}
					if v == nil {
						nulls.Add(vec.Nsp, uint64(j))
					} else {
						if vv, err := rangeCheck(v.(string), vec.Typ, bat.Attrs[i], j+1); err != nil {
							return err
						} else {
							vs[j] = []byte(vv.(string))
						}
					}
				}
			}
			if err := vector.Append(vec, vs); err != nil {
				return err
			}
		case types.T_date:
			vs := make([]types.Date, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return err
					}
					if v == nil {
						nulls.Add(vec.Nsp, uint64(j))
					} else {
						if vv, err := rangeCheck(v.(types.Date), vec.Typ, bat.Attrs[i], j+1); err != nil {
							return err
						} else {
							vs[j] = vv.(types.Date)
						}
					}
				}
			}
			if err := vector.Append(vec, vs); err != nil {
				return err
			}
		case types.T_datetime:
			vs := make([]types.Datetime, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return err
					}
					if v == nil {
						nulls.Add(vec.Nsp, uint64(j))
					} else {
						if vv, err := rangeCheck(v.(types.Datetime), vec.Typ, bat.Attrs[i], j+1); err != nil {
							return err
						} else {
							vs[j] = vv.(types.Datetime)
						}
					}
				}
			}
			if err := vector.Append(vec, vs); err != nil {
				return err
			}
		case types.T_timestamp:
			vs := make([]types.Timestamp, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return err
					}
					if v == nil {
						nulls.Add(vec.Nsp, uint64(j))
					} else {
						if vv, err := rangeCheck(v.(types.Timestamp), vec.Typ, bat.Attrs[i], j+1); err != nil {
							return err
						} else {
							vs[j] = vv.(types.Timestamp)
						}
					}
				}
			}
			if err := vector.Append(vec, vs); err != nil {
				return err
			}
		case types.T_decimal64:
			vs := make([]types.Decimal64, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return err
					}
					if v == nil {
						nulls.Add(vec.Nsp, uint64(j))
					} else {
						if vv, err := rangeCheck(v.(types.Decimal64), vec.Typ, bat.Attrs[i], j+1); err != nil {
							return err
						} else {
							vs[j] = vv.(types.Decimal64)
						}
					}
				}
			}
			if err := vector.Append(vec, vs); err != nil {
				return err
			}
		case types.T_decimal128:
			vs := make([]types.Decimal128, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return err
					}
					if v == nil {
						nulls.Add(vec.Nsp, uint64(j))
					} else {
						if vv, err := rangeCheck(v.(types.Decimal128), vec.Typ, bat.Attrs[i], j+1); err != nil {
							return err
						} else {
							vs[j] = vv.(types.Decimal128)
						}
					}
				}
			}
			if err := vector.Append(vec, vs); err != nil {
				return err
			}
		default:
			return errors.New(errno.DatatypeMismatch, fmt.Sprintf("insert for type '%v' not implement now", vec.Typ))
		}
	}
	// insert Null for other columns
	for k, v := range attrType {
		bat.Attrs = append(bat.Attrs, k)
		vec := vector.New(v)
		for i, j := 0, len(rows.Rows); i < j; i++ {
			nulls.Add(vec.Nsp, uint64(i))
		}
		switch vec.Typ.Oid {
		case types.T_int8:
			vec.Col = make([]int8, len(rows.Rows))
		case types.T_int16:
			vec.Col = make([]int16, len(rows.Rows))
		case types.T_int32:
			vec.Col = make([]int32, len(rows.Rows))
		case types.T_int64:
			vec.Col = make([]int64, len(rows.Rows))
		case types.T_uint8:
			vec.Col = make([]uint8, len(rows.Rows))
		case types.T_uint16:
			vec.Col = make([]uint16, len(rows.Rows))
		case types.T_uint32:
			vec.Col = make([]uint32, len(rows.Rows))
		case types.T_uint64:
			vec.Col = make([]uint64, len(rows.Rows))
		case types.T_float32:
			vec.Col = make([]float32, len(rows.Rows))
		case types.T_float64:
			vec.Col = make([]float64, len(rows.Rows))
		case types.T_char, types.T_varchar:
			col := &types.Bytes{}
			if err = col.Append(make([][]byte, len(rows.Rows))); err != nil {
				return err
			}
			vec.Col = col
		case types.T_date:
			vec.Col = make([]types.Date, len(rows.Rows))
		case types.T_datetime:
			vec.Col = make([]types.Datetime, len(rows.Rows))
		case types.T_decimal64:
			vec.Col = make([]types.Decimal64, len(rows.Rows))
		case types.T_decimal128:
			vec.Col = make([]types.Decimal128, len(rows.Rows))
		default:
			return errors.New(errno.DatatypeMismatch, fmt.Sprintf("insert for type '%v' not implement now", vec.Typ))
		}
		bat.Vecs = append(bat.Vecs, vec)
	}
	plan.dataBatch = bat
	return nil
}

// makeExprFromVal make an expr from value
func makeExprFromVal(typ types.Type, value interface{}, isNull bool) tree.Expr {
	if isNull {
		return tree.NewNumVal(constant.MakeUnknown(), "NULL", false)
	}
	switch typ.Oid {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
		res := value.(int64)
		str := strconv.FormatInt(res, 10)
		if res < 0 {
			return tree.NewNumVal(constant.MakeUint64(uint64(-res)), str, true)
		}
		return tree.NewNumVal(constant.MakeInt64(res), str, false)
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		res := value.(uint64)
		str := strconv.FormatUint(res, 10)
		return tree.NewNumVal(constant.MakeUint64(res), str, false)
	case types.T_float32, types.T_float64:
		res := value.(float64)
		str := strconv.FormatFloat(res, 'f', 10, 64)
		return tree.NewNumVal(constant.MakeFloat64(res), str, res < 0)
	case types.T_char, types.T_varchar:
		res := value.(string)
		return tree.NewNumVal(constant.MakeString(res), res, false)
	case types.T_date:
		res := value.(types.Date).String()
		return tree.NewNumVal(constant.MakeString(res), res, false)
	case types.T_datetime:
		res := value.(types.Datetime).String()
		return tree.NewNumVal(constant.MakeString(res), res, false)
	}
	return tree.NewNumVal(constant.MakeUnknown(), "NULL", false)
}

// rewriteInsertRows rewrite default expressions in valueClause's Rows
// and convert them to be column-default-expression.
func rewriteInsertRows(noInsertTarget bool, finalInsertTargets []string, relationAttrs []string, rows []tree.Exprs, defaultExprs map[string]tree.Expr) ([]tree.Exprs, []string, error) {
	var ok bool
	var targetLen int
	var orderDefault []*tree.Expr
	useOrder := false // true means use orderDefault to find default value
	allRowsNil := true

	// if length of finalInsertTargets less than relation columns
	// there should rewrite finalInsertTargets.
	if len(finalInsertTargets) < len(relationAttrs) {
		sourceLen := len(finalInsertTargets)
		for _, column := range relationAttrs {
			found := false
			for i := 0; i < sourceLen; i++ {
				if finalInsertTargets[i] == column {
					found = true
					break
				}
			}
			if !found {
				finalInsertTargets = append(finalInsertTargets, column)
			}
		}
	}
	targetLen = len(finalInsertTargets)

	// if insert to many values,  do not use map to found default value but an order slice
	if len(rows) > 10 {
		orderDefault = make([]*tree.Expr, targetLen)
		for i, attr := range finalInsertTargets {
			v, ok := defaultExprs[attr]
			if ok {
				orderDefault[i] = &v
			} else {
				orderDefault[i] = nil
			}
		}
		useOrder = true
	}

	for i := range rows {
		if rows[i] != nil {
			allRowsNil = false
			break
		}
	}

	for i := range rows {
		if rows[i] == nil {
			// nil expr will convert to defaultExpr when insertTargets and rows are both nil.
			if noInsertTarget && allRowsNil {
				rows[i] = make(tree.Exprs, targetLen)
				for j := 0; j < targetLen; j++ {
					rows[i][j] = tree.NewDefaultVal(nil)
				}
			}
		} else {
			// some cases need to fill the missing columns with default values
			for len(rows[i]) < targetLen {
				rows[i] = append(rows[i], tree.NewDefaultVal(nil))
			}
		}

		if useOrder { // number of insert values > 10
			for j := range rows[i] {
				if !isDefaultExpr(rows[i][j]) {
					continue
				}
				if useOrder {
					if orderDefault[j] == nil {
						return nil, nil, errors.New(errno.InvalidColumnDefinition, fmt.Sprintf("Field '%s' doesn't have a default value", finalInsertTargets[j]))
					}
					rows[i][j] = *orderDefault[j]
				}
			}
		} else {
			for j := range rows[i] {
				if !isDefaultExpr(rows[i][j]) {
					continue
				}
				rows[i][j], ok = defaultExprs[finalInsertTargets[j]]
				if !ok {
					return nil, nil, errors.New(errno.InvalidColumnDefinition, fmt.Sprintf("Field '%s' doesn't have a default value", finalInsertTargets[j]))
				}
			}
		}
	}
	return rows, finalInsertTargets, nil
}

// isDefaultExpr returns true when input expression means default expr
func isDefaultExpr(expr tree.Expr) bool {
	_, ok := expr.(*tree.DefaultVal)
	return ok
}

var (
	// errors may happen while building constant
	ErrDivByZero        = errors.New(errno.SyntaxErrororAccessRuleViolation, "division by zero")
	ErrZeroModulus      = errors.New(errno.SyntaxErrororAccessRuleViolation, "zero modulus")
	errConstantOutRange = errors.New(errno.DataException, "constant value out of range")
	errBinaryOutRange   = errors.New(errno.DataException, "binary result out of range")
	errUnaryOutRange    = errors.New(errno.DataException, "unary result out of range")
)

func buildConstant(typ types.Type, n tree.Expr) (interface{}, error) {
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
		var argTyp = types.Type{Oid: types.T_float64, Size: 8}
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
		switch typ.Oid {
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
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
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			if floatResult < 0 || floatResult+0.5 > math.MaxInt64 {
				return nil, errBinaryOutRange
			}
			return uint64(floatResult + 0.5), nil
		case types.T_float32:
			if floatResult == 0 {
				return float32(0), nil
			}
			if floatResult > math.MaxFloat32 || floatResult < -math.MaxFloat32 {
				return nil, errBinaryOutRange
			}
			return float32(floatResult), nil
		case types.T_float64:
			return floatResult, nil
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("unexpected return type '%v' for binary expression '%v'", typ, e.Op))
		}
	case *tree.UnresolvedName:
		floatResult, err := strconv.ParseFloat(e.Parts[0], 64)
		if err != nil {
			return nil, err
		}
		switch typ.Oid {
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
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
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			if floatResult < 0 || floatResult+0.5 > math.MaxInt64 {
				return nil, errBinaryOutRange
			}
			return uint64(floatResult + 0.5), nil
		case types.T_float32:
			if floatResult == 0 {
				return float32(0), nil
			}
			if floatResult > math.MaxFloat32 || floatResult < -math.MaxFloat32 {
				return nil, errBinaryOutRange
			}
			return float32(floatResult), nil
		case types.T_float64:
			return floatResult, nil
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("unexpected return type '%v' for binary expression '%v'", typ, floatResult))
		}
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
}

func buildConstantValue(typ types.Type, num *tree.NumVal) (interface{}, error) {
	val := num.Value
	str := num.String()

	switch val.Kind() {
	case constant.Unknown:
		return nil, nil
	case constant.Bool:
		return constant.BoolVal(val), nil
	case constant.Int:
		switch typ.Oid {
		case types.T_bool:
			return types.ParseValueToBool(num)
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
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
		case types.T_decimal64:
			return types.ParseStringToDecimal64(str, typ.Width, typ.Scale)
		case types.T_decimal128:
			return types.ParseStringToDecimal128(str, typ.Width, typ.Scale)
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			v, _ := constant.Uint64Val(val)
			if num.Negative() {
				if v != 0 {
					return nil, errConstantOutRange
				}
			}
			return uint64(v), nil
		case types.T_float32:
			v, _ := constant.Float32Val(val)
			if num.Negative() {
				return float32(-v), nil
			}
			return float32(v), nil
		case types.T_float64:
			v, _ := constant.Float64Val(val)
			if num.Negative() {
				return float64(-v), nil
			}
			return float64(v), nil
		case types.T_date:
			if !num.Negative() {
				return types.ParseDate(str)
			}
		case types.T_datetime:
			if !num.Negative() {
				return types.ParseDatetime(str)
			}
		}
	case constant.Float:
		switch typ.Oid {
		case types.T_int64, types.T_int32, types.T_int16, types.T_int8:
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
		case types.T_uint64, types.T_uint32, types.T_uint16, types.T_uint8:
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
		case types.T_float32:
			v, _ := constant.Float32Val(val)
			if num.Negative() {
				return float32(-v), nil
			}
			return float32(v), nil
		case types.T_float64:
			v, _ := constant.Float64Val(val)
			if num.Negative() {
				return float64(-v), nil
			}
			return float64(v), nil
		case types.T_datetime:
			return types.ParseDatetime(str)
		case types.T_decimal64:
			return types.ParseStringToDecimal64(str, typ.Width, typ.Scale)
		case types.T_decimal128:
			return types.ParseStringToDecimal128(str, typ.Width, typ.Scale)
		}
	case constant.String:
		switch typ.Oid {
		case types.T_decimal64:
			return types.ParseStringToDecimal64(str, typ.Width, typ.Scale)
		case types.T_decimal128:
			return types.ParseStringToDecimal128(str, typ.Width, typ.Scale)
		}
		if !num.Negative() {
			switch typ.Oid {
			case types.T_char, types.T_varchar:
				return constant.StringVal(val), nil
			case types.T_date:
				return types.ParseDate(constant.StringVal(val))
			case types.T_datetime:
				return types.ParseDatetime(constant.StringVal(val))
			case types.T_timestamp:
				return types.ParseTimestamp(constant.StringVal(val), typ.Precision)
			}
		}
	}
	return nil, errors.New(errno.IndeterminateDatatype, fmt.Sprintf("unsupport value: %v", val))
}

// rangeCheck do range check for value, and do type conversion.
func rangeCheck(value interface{}, typ types.Type, columnName string, rowNumber int) (interface{}, error) {
	errString := "Out of range value for column '%s' at row %d"

	switch v := value.(type) {
	case int64:
		switch typ.Oid {
		case types.T_int8:
			if v <= math.MaxInt8 && v >= math.MinInt8 {
				return int8(v), nil
			}
		case types.T_int16:
			if v <= math.MaxInt16 && v >= math.MinInt16 {
				return int16(v), nil
			}
		case types.T_int32:
			if v <= math.MaxInt32 && v >= math.MinInt32 {
				return int32(v), nil
			}
		case types.T_int64:
			return v, nil
		default:
			return nil, errors.New(errno.DatatypeMismatch, "unexpected type and value")
		}
		return nil, errors.New(errno.DataException, fmt.Sprintf(errString, columnName, rowNumber))
	case uint64:
		switch typ.Oid {
		case types.T_uint8:
			if v <= math.MaxUint8 {
				return uint8(v), nil
			}
		case types.T_uint16:
			if v <= math.MaxUint16 {
				return uint16(v), nil
			}
		case types.T_uint32:
			if v <= math.MaxUint32 {
				return uint32(v), nil
			}
		case types.T_uint64:
			return v, nil
		default:
			return nil, errors.New(errno.DatatypeMismatch, "unexpected type and value")
		}
		return nil, errors.New(errno.DataException, fmt.Sprintf(errString, columnName, rowNumber))
	case float32:
		if typ.Oid == types.T_float32 {
			return v, nil
		}
		return nil, errors.New(errno.DatatypeMismatch, "unexpected type and value")
	case float64:
		switch typ.Oid {
		case types.T_float32:
			if v <= math.MaxFloat32 && v >= -math.MaxFloat32 {
				return float32(v), nil
			}
		case types.T_float64:
			return v, nil
		default:
			return nil, errors.New(errno.DatatypeMismatch, "unexpected type and value")
		}
		return nil, errors.New(errno.DataException, fmt.Sprintf(errString, columnName, rowNumber))
	case string:
		switch typ.Oid {
		case types.T_char, types.T_varchar: // string family should compare the length but not value
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
	case types.Date, types.Datetime, types.Timestamp, types.Decimal64, types.Decimal128, bool:
		return v, nil
	default:
		return nil, errors.New(errno.DatatypeMismatch, "unexpected type and value")
	}
}
