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
	"fmt"
	"go/constant"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/insert"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/sqlerror"
	"matrixone/pkg/vm/engine"
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
			attrs[i] = string(col.Name)
		}
	}
	rows.Rows, err = rewriteInsertRows(r, stmt.Columns, attrs, rows.Rows)
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
						vs[j] = int8(v.(int64))
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
						vs[j] = int16(v.(int64))
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
						vs[j] = int32(v.(int64))
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
						vs[j] = v.(int64)
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
						vs[j] = uint8(v.(uint64))
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
						vs[j] = uint16(v.(uint64))
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
						vs[j] = uint32(v.(uint64))
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
						vs[j] = v.(uint64)
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
						vs[j] = v.(float32)
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
						vs[j] = v.(float64)
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
						vs[j] = []byte(v.(string))
					}
				}
			}
			if err := vec.Append(vs); err != nil {
				return nil, err
			}
		}
	}
	if err = insertValuesRangeCheck(bat.Vecs, bat.Attrs, rows.Rows); err != nil {
		return nil, err
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

// insertValuesRangeCheck returns error if final build result out of range.
func insertValuesRangeCheck(vecs []*vector.Vector, columnNames []string, sourceInput []tree.Exprs) error {
	var sourceValue, errString string

	for colIndex, vec := range vecs {
		for rowIndex := range sourceInput {
			// range check should ignore null value
			if isNullExpr(sourceInput[rowIndex][colIndex]) {
				continue
			}
			sourceValue = sourceInput[rowIndex][colIndex].String()
			errString = valueRangeCheck(sourceValue, vec.Typ)

			if len(errString) != 0 {
				return sqlerror.New(errno.DataException, fmt.Sprintf(errString, columnNames[colIndex], rowIndex + 1))
			}
		}
	}
	return nil
}

// valueRangeCheck returns error string if value is out of range of typ
func valueRangeCheck(value string, typ types.Type) string {
	switch typ.Oid {
	case types.T_int64, types.T_int32, types.T_int16, types.T_int8:
		if _, err := strconv.ParseInt(value, 10, int(typ.Width)); err != nil {
			return "Out of range value for column '%s' at row %d"
		}
	case types.T_uint64, types.T_uint32, types.T_uint16, types.T_uint8:
		if _, err := strconv.ParseUint(value, 10, int(typ.Width)); err != nil {
			return "Out of range value for column '%s' at row %d"
		}
	case types.T_float32, types.T_float64:
		if _, err := strconv.ParseFloat(value, int(typ.Width)); err != nil {
			return "Out of range value for column '%s' at row %d"
		}
	case types.T_char, types.T_varchar: // string family should compare the length but not value
		if len(value) > int(typ.Width) {
			return "Data too long for column '%s' at row %d"
		}
	}
	return ""
}

// rewriteInsertRows rewrite default expressions in valueClause's Rows
// and convert them to be column-default-expression.
func rewriteInsertRows(rel engine.Relation, insertTargets tree.IdentifierList, finalInsertTargets []string, rows []tree.Exprs) ([]tree.Exprs, error) {
	var ok bool
	targetsNil := insertTargets == nil
	allRowsNil := true
	targetLen  := len(finalInsertTargets)

	defaultExprs := make(map[string]tree.Expr)
	for _, attr := range rel.Attribute() { // init a map from column name to its default expression
		if attr.HasDefaultExpr() {
			str, null := attr.GetDefaultExpr()
			defaultExprs[attr.Name] = makeExprFromStr(attr.Type, str, null)
		}
	}

	for i := range rows {
		if rows[i] != nil {
			allRowsNil = false
			break
		}
	}

	for i := range rows {
		// nil expr will convert to defaultExpr when all insertTargets and rows are nil.
		if targetsNil && allRowsNil {
			rows[i] = make(tree.Exprs, targetLen)
			for j := 0; j < targetLen; j++{
				rows[i][j] = tree.NewDefaultVal()
			}
		}
		for j := range rows[i] {
			if !isDefaultExpr(rows[i][j]) {
				continue
			}
			rows[i][j], ok = defaultExprs[finalInsertTargets[j]]
			if !ok {
				return nil, sqlerror.New(errno.InvalidColumnDefinition, fmt.Sprintf("Field '%s' doesn't have a default value", finalInsertTargets[j]))
			}
		}
	}
	return rows, nil
}

// makeExprFromStr make an expr from expression string and its type
func makeExprFromStr(typ types.Type, str string, isNull bool) tree.Expr {
	if isNull {
		return tree.NewNumVal(constant.MakeUnknown(), "NULL", false)
	}
	switch typ.Oid {
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
			value, _ := strconv.ParseInt(str, 10, 64)
			return tree.NewNumVal(constant.MakeInt64(value), str, value < 0)
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			value, _ := strconv.ParseUint(str, 10, 64)
			return tree.NewNumVal(constant.MakeUint64(value), str, false)
		case types.T_float32, types.T_float64:
			value, _ := strconv.ParseFloat(str, 64)
			return tree.NewNumVal(constant.MakeFloat64(value), str, value < 0)
		case types.T_char, types.T_varchar:
			return tree.NewNumVal(constant.MakeString(str), str, false)
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