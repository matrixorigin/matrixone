// Copyright 2021 Matrix Origin
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

package build

import (
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/op"
	"github.com/matrixorigin/matrixone/pkg/sql/op/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sqlerror"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go/constant"
	"math"
	"strconv"
)

func (b *build) buildInsert(stmt *tree.Insert) (op.OP, error) {
	var attrs []string
	var bat *batch.Batch

	if _, ok := stmt.Table.(*tree.TableName); !ok {
		return nil, sqlerror.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table: '%v'", stmt.Table))
	}
	db, id, r, err := b.tableName(stmt.Table.(*tree.TableName))
	if err != nil {
		return nil, err
	}
	mp := make(map[string]types.Type)
	{
		attrs := r.Attribute()
		for _, attr := range attrs {
			mp[attr.Name] = attr.Type
		}
	}
	rows, ok := stmt.Rows.Select.(*tree.ValuesClause)
	if !ok {
		return nil, sqlerror.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport clause: '%v'", stmt))
	}
	if len(stmt.Columns) > 0 {
		attrs = make([]string, len(stmt.Columns))
		for i, col := range stmt.Columns {
			attrs[i] = string(col)
		}
	} else {
		cols := r.Attribute()
		attrs = make([]string, len(r.Attribute()))
		for i, col := range cols {
			attrs[i] = col.Name
		}
	}
	rows.Rows, attrs, err = rewriteInsertRows(r, stmt.Columns, attrs, rows.Rows)
	if err != nil {
		return nil, err
	}
	for i, rows := range rows.Rows {
		if len(attrs) != len(rows) {
			return nil, sqlerror.New(errno.InvalidColumnReference, fmt.Sprintf("Column count doesn't match value count at row %v", i))
		}
	}
	bat = batch.New(true, attrs)
	for i, attr := range attrs {
		typ, ok := mp[attr]
		if !ok {
			return nil, sqlerror.New(errno.UndefinedColumn, fmt.Sprintf("unknown column '%s' in 'filed list'", attrs[i]))
		}
		bat.Vecs[i] = vector.New(typ)
		delete(mp, attr)
	}
	if len(rows.Rows) == 0 || len(rows.Rows[0]) == 0 {
		return insert.New(id, db, bat, r), nil
	}
	for i, vec := range bat.Vecs {
		switch vec.Typ.Oid {
		case types.T_int8:
			vs := make([]int8, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return nil, err
					}
					if v == nil {
						vec.Nsp.Add(uint64(j))
					} else {
						if vv, err := rangeCheck(v.(int64), vec.Typ, bat.Attrs[i], j + 1); err != nil {
							return nil, err
						} else {
							vs[j] = vv.(int8)
						}
					}
				}
			}
			if err := vec.Append(vs); err != nil {
				return nil, err
			}
		case types.T_int16:
			vs := make([]int16, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return nil, err
					}
					if v == nil {
						vec.Nsp.Add(uint64(j))
					} else {
						if vv, err := rangeCheck(v.(int64), vec.Typ, bat.Attrs[i], j + 1); err != nil {
							return nil, err
						} else {
							vs[j] = vv.(int16)
						}
					}
				}
			}
			if err := vec.Append(vs); err != nil {
				return nil, err
			}
		case types.T_int32:
			vs := make([]int32, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return nil, err
					}
					if v == nil {
						vec.Nsp.Add(uint64(j))
					} else {
						if vv, err := rangeCheck(v.(int64), vec.Typ, bat.Attrs[i], j + 1); err != nil {
							return nil, err
						} else {
							vs[j] = vv.(int32)
						}
					}
				}
			}
			if err := vec.Append(vs); err != nil {
				return nil, err
			}
		case types.T_int64:
			vs := make([]int64, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return nil, err
					}
					if v == nil {
						vec.Nsp.Add(uint64(j))
					} else {
						if vv, err := rangeCheck(v.(int64), vec.Typ, bat.Attrs[i], j + 1); err != nil {
							return nil, err
						} else {
							vs[j] = vv.(int64)
						}
					}
				}
			}
			if err := vec.Append(vs); err != nil {
				return nil, err
			}
		case types.T_uint8:
			vs := make([]uint8, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return nil, err
					}
					if v == nil {
						vec.Nsp.Add(uint64(j))
					} else {
						if vv, err := rangeCheck(v.(uint64), vec.Typ, bat.Attrs[i], j + 1); err != nil {
							return nil, err
						} else {
							vs[j] = vv.(uint8)
						}
					}
				}
			}
			if err := vec.Append(vs); err != nil {
				return nil, err
			}
		case types.T_uint16:
			vs := make([]uint16, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return nil, err
					}
					if v == nil {
						vec.Nsp.Add(uint64(j))
					} else {
						if vv, err := rangeCheck(v.(uint64), vec.Typ, bat.Attrs[i], j + 1); err != nil {
							return nil, err
						} else {
							vs[j] = vv.(uint16)
						}
					}
				}
			}
			if err := vec.Append(vs); err != nil {
				return nil, err
			}
		case types.T_uint32:
			vs := make([]uint32, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return nil, err
					}
					if v == nil {
						vec.Nsp.Add(uint64(j))
					} else {
						if vv, err := rangeCheck(v.(uint64), vec.Typ, bat.Attrs[i], j + 1); err != nil {
							return nil, err
						} else {
							vs[j] = vv.(uint32)
						}
					}
				}
			}
			if err := vec.Append(vs); err != nil {
				return nil, err
			}
		case types.T_uint64:
			vs := make([]uint64, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return nil, err
					}
					if v == nil {
						vec.Nsp.Add(uint64(j))
					} else {
						if vv, err := rangeCheck(v.(uint64), vec.Typ, bat.Attrs[i], j + 1); err != nil {
							return nil, err
						} else {
							vs[j] = vv.(uint64)
						}
					}
				}
			}
			if err := vec.Append(vs); err != nil {
				return nil, err
			}
		case types.T_float32:
			vs := make([]float32, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return nil, err
					}
					if v == nil {
						vec.Nsp.Add(uint64(j))
					} else {
						if vv, err := rangeCheck(v.(float32), vec.Typ, bat.Attrs[i], j + 1); err != nil {
							return nil, err
						} else {
							vs[j] = vv.(float32)
						}
					}
				}
			}
			if err := vec.Append(vs); err != nil {
				return nil, err
			}
		case types.T_float64:
			vs := make([]float64, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return nil, err
					}
					if v == nil {
						vec.Nsp.Add(uint64(j))
					} else {
						if vv, err := rangeCheck(v.(float64), vec.Typ, bat.Attrs[i], j + 1); err != nil {
							return nil, err
						} else {
							vs[j] = vv.(float64)
						}
					}
				}
			}
			if err := vec.Append(vs); err != nil {
				return nil, err
			}
		case types.T_char, types.T_varchar:
			vs := make([][]byte, len(rows.Rows))
			{
				for j, row := range rows.Rows {
					v, err := buildConstant(vec.Typ, row[i])
					if err != nil {
						return nil, err
					}
					if v == nil {
						vec.Nsp.Add(uint64(j))
					} else {
						if vv, err := rangeCheck(v.(string), vec.Typ, bat.Attrs[i], j + 1); err != nil {
							return nil, err
						} else {
							vs[j] = []byte(vv.(string))
						}
					}
				}
			}
			if err := vec.Append(vs); err != nil {
				return nil, err
			}
		}
	}
	for k, v := range mp {
		bat.Attrs = append(bat.Attrs, k)
		vec := vector.New(v)
		for i, j := uint64(0), uint64(len(rows.Rows)); i < j; i++ {
			vec.Nsp.Add(i)
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
				return nil, err
			}
			vec.Col = col
		}
		bat.Vecs = append(bat.Vecs, vec)
	}
	return insert.New(id, db, bat, r), nil
}

func (b *build) tableName(tbl *tree.TableName) (string, string, engine.Relation, error) {
	if len(tbl.SchemaName) == 0 {
		tbl.SchemaName = tree.Identifier(b.db)
	}
	db, err := b.e.Database(string(tbl.SchemaName))
	if err != nil {
		return "", "", nil, sqlerror.New(errno.InvalidSchemaName, err.Error())
	}
	r, err := db.Relation(string(tbl.ObjectName))
	if err != nil {
		return "", "", nil, sqlerror.New(errno.UndefinedTable, err.Error())
	}
	return string(tbl.SchemaName), string(tbl.ObjectName), r, nil
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
			return nil, errors.New("unexpected type and value")
		}
		return nil, sqlerror.New(errno.DataException , fmt.Sprintf(errString, columnName, rowNumber))
	case uint64:
		switch typ.Oid {
		case types.T_uint8:
			if v <= math.MaxUint8 && v >= 0 {
				return uint8(v), nil
			}
		case types.T_uint16:
			if v <= math.MaxUint16 && v >= 0 {
				return uint16(v), nil
			}
		case types.T_uint32:
			if v <= math.MaxUint32 && v >= 0 {
				return uint32(v), nil
			}
		case types.T_uint64:
			return v, nil
		default:
			return nil, errors.New("unexpected type and value")
		}
		return nil, sqlerror.New(errno.DataException, fmt.Sprintf(errString, columnName, rowNumber))
	case float32:
		if typ.Oid == types.T_float32 {
			return v, nil
		}
		return nil, errors.New("unexpected type and value")
	case float64:
		switch typ.Oid {
		case types.T_float32:
			if v <= math.MaxFloat32 && v >= math.SmallestNonzeroFloat32 {
				return float32(v), nil
			}
		case types.T_float64:
			return v, nil
		default:
			return nil, errors.New("unexpected type and value")
		}
		return nil, sqlerror.New(errno.DataException, fmt.Sprintf(errString, columnName, rowNumber))
	case string:
		switch typ.Oid {
		case types.T_char, types.T_varchar: // string family should compare the length but not value
			if len(v) <= int(typ.Width) {
				return v, nil
			}
		default:
			return nil, errors.New("unexpected type and value")
		}
		return nil, sqlerror.New(errno.DataException, fmt.Sprintf("Data too long for column '%s' at row %d", columnName, rowNumber))
	default:
		return nil, errors.New("unexpected type and value")
	}
}

// rewriteInsertRows rewrite default expressions in valueClause's Rows
// and convert them to be column-default-expression.
func rewriteInsertRows(rel engine.Relation, insertTargets tree.IdentifierList, finalInsertTargets []string, rows []tree.Exprs) ([]tree.Exprs, []string, error) {
	var ok bool
	var targetLen int
	targetsNil := insertTargets == nil
	allRowsNil := true

	defaultExprs := make(map[string]tree.Expr)
	for _, attr := range rel.Attribute() { // init a map from column name to its default expression
		if attr.HasDefaultExpr() {
			value, null := attr.GetDefaultExpr()
			defaultExprs[attr.Name] = makeExprFromVal(attr.Type, value, null)
		}
	}

	// if length of finalInsertTargets less than relation columns
	// there should rewrite finalInsertTargets.
	if len(finalInsertTargets) < len(rel.Attribute()) {
		sourceLen := len(finalInsertTargets)
		for _, attr := range rel.Attribute() {
			found := false
			column := attr.Name
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
	targetLen  = len(finalInsertTargets)

	for i := range rows {
		if rows[i] != nil {
			allRowsNil = false
			break
		}
	}

	for i := range rows {
		if rows[i] == nil {
			// nil expr will convert to defaultExpr when insertTargets and rows are both nil.
			if targetsNil && allRowsNil {
				rows[i] = make(tree.Exprs, targetLen)
				for j := 0; j < targetLen; j++{
					rows[i][j] = tree.NewDefaultVal(nil)
				}
			}
		} else {
			// some cases need to fill the missing columns with default values
			for len(rows[i]) < targetLen {
				rows[i] = append(rows[i], tree.NewDefaultVal(nil))
			}
		}

		for j := range rows[i] {
			if !isDefaultExpr(rows[i][j]) {
				continue
			}
			rows[i][j], ok = defaultExprs[finalInsertTargets[j]]
			if !ok {
				return nil, nil, sqlerror.New(errno.InvalidColumnDefinition, fmt.Sprintf("Field '%s' doesn't have a default value", finalInsertTargets[j]))
			}
		}
	}
	return rows, finalInsertTargets, nil
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
			return tree.NewNumVal(constant.MakeInt64(res), str, res < 0)
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
	}
	return tree.NewNumVal(constant.MakeUnknown(), "NULL", false)
}

// isDefaultExpr returns true when input expression means default expr
func isDefaultExpr(expr tree.Expr) bool {
	_, ok := expr.(*tree.DefaultVal)
	return ok
}

// isNullExpr returns true when input expression means null expr
func isNullExpr(expr tree.Expr) bool {
	v, ok := expr.(*tree.NumVal)
	return ok && v.Value.Kind() == constant.Unknown
}

func isParenExpr(expr tree.Expr) bool {
	_, ok := expr.(*tree.ParenExpr)
	return ok
}