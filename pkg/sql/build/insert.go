package build

import (
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/insert"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/sqlerror"
	"matrixone/pkg/vm/engine"
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
