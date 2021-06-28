package build

import (
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/insert"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/vm/engine"
)

func (b *build) buildInsert(stmt *tree.Insert) (op.OP, error) {
	var bat *batch.Batch

	db, id, r, err := b.tableName(stmt.Table)
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
		return nil, fmt.Errorf("unsupport clause: '%v'", stmt)
	}
	attrs := make([]string, len(stmt.Columns))
	for i, col := range stmt.Columns {
		attrs[i] = string(col)
	}
	bat = batch.New(true, attrs)
	for i, attr := range attrs {
		typ, ok := mp[attr]
		if !ok {
			return nil, fmt.Errorf("unknown column '%s' in 'filed list'", attrs[i])
		}
		bat.Vecs[i] = vector.New(typ)
		delete(mp, attr)
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
					vs[j] = int8(v.(int64))
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
					vs[j] = int16(v.(int64))
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
					vs[j] = int32(v.(int64))
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
					vs[j] = v.(int64)
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
					vs[j] = uint8(v.(uint64))
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
					vs[j] = uint16(v.(uint64))
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
					vs[j] = uint32(v.(uint64))
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
					vs[j] = v.(uint64)
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
					vs[j] = v.(float32)
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
					vs[j] = v.(float64)
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
					vs[j] = []byte(v.(string))
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
		bat.Vecs = append(bat.Vecs, vec)
	}
	return insert.New(id, db, bat, r), nil
}

func (b *build) tableName(stmt tree.TableExpr) (string, string, engine.Relation, error) {
	jtbl, ok := stmt.(*tree.JoinTableExpr)
	if !ok {
		return "", "", nil, fmt.Errorf("unsupport table: '%v'", stmt)
	}
	atbl, ok := jtbl.Left.(*tree.AliasedTableExpr)
	if !ok {
		return "", "", nil, fmt.Errorf("unsupport table: '%v'", stmt)
	}
	tbl, ok := atbl.Expr.(*tree.TableName)
	if !ok {
		return "", "", nil, fmt.Errorf("unsupport table: '%v'", stmt)
	}
	if len(tbl.SchemaName) == 0 {
		tbl.SchemaName = tree.Identifier(b.db)
	}
	db, err := b.e.Database(string(tbl.SchemaName))
	if err != nil {
		return "", "", nil, err
	}
	r, err := db.Relation(string(tbl.ObjectName))
	if err != nil {
		return "", "", nil, err
	}
	return string(tbl.SchemaName), string(tbl.ObjectName), r, nil
}
