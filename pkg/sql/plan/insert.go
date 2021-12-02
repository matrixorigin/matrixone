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

package plan

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
	"strconv"
)

func (b *build) BuildInsert(stmt *tree.Insert, plan *Insert) error {
	var attrs []string
	var bat *batch.Batch
	var rows *tree.ValuesClause

	// Unsupported Case
	if _, ok := stmt.Table.(*tree.TableName); !ok {
		return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table: '%v'", stmt.Table))
	}
	switch typ := stmt.Rows.Select.(type) {
	case *tree.ValuesClause:
		rows = typ
	// case *tree.ParenSelect: // todo: not implement now.
	// case *tree.SelectClause:
	default:
		return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport clause: '%v'", typ))
	}

	db, id, r, err := b.tableName(stmt.Table.(*tree.TableName))
	if err != nil {
		return err
	}
	plan.Id = id
	plan.Relation = r
	plan.Db = db

	attrType := make(map[string]types.Type)   // Map from relation's attribute name to its type
	attrDefault := make(map[string]tree.Expr) // Map from relation's attribute name to its default value
	orderAttr := make([]string, 0, 32)        // order relation's attribute names
	{
		count := 0
		for _, def := range r.TableDefs() {
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
		plan.Bat = bat
		return nil
	}

	// insert values for columns
	for i, vec := range bat.Vecs {
		switch vec.Typ.Oid {
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
		default:
			return errors.New(errno.DatatypeMismatch, fmt.Sprintf("insert for type '%v' not implement now", vec.Typ))
		}
		bat.Vecs = append(bat.Vecs, vec)
	}
	plan.Bat = bat
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
