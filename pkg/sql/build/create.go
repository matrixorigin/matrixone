package build

import (
	"fmt"
	"matrixone/pkg/client"
	"matrixone/pkg/compress"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/createDatabase"
	"matrixone/pkg/sql/op/createTable"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/metadata"
)

func (b *build) buildCreateTable(stmt *tree.CreateTable) (op.OP, error) {
	var defs []engine.TableDef

	dbName, tblName, err := b.tableInfo(stmt.Table)
	if err != nil {
		return nil, err
	}
	db, err := b.e.Database(dbName)
	if err != nil {
		return nil, err
	}
	{
		for i := range stmt.Defs {
			def, err := b.getTableDef(stmt.Defs[i])
			if err != nil {
				return nil, err
			}
			defs = append(defs, def)
		}
	}
	return createTable.New(stmt.IfNotExists, tblName, defs, nil, db), nil
}

func (b *build) buildCreateDatabase(stmt *tree.CreateDatabase) (op.OP, error) {
	return createDatabase.New(stmt.IfNotExists, string(stmt.Name), b.e), nil
}

func (b *build) getTableDef(def tree.TableDef) (engine.TableDef, error) {
	switch n := def.(type) {
	case *tree.ColumnTableDef:
		typ, err := b.getTableDefType(n.Type)
		if err != nil {
			return nil, err
		}
		return &engine.AttributeDef{
			Attr: metadata.Attribute{
				Type: *typ,
				Alg:  compress.Lz4,
				Name: n.Name.Parts[0],
			},
		}, nil
	default:
		return nil, fmt.Errorf("unsupport table def: '%v'", def)
	}
}

func (b *build) getTableDefType(typ tree.ResolvableTypeReference) (*types.Type, error) {
	if n, ok := typ.(*tree.T); ok {
		switch uint8(n.InternalType.Oid) {
		case client.MYSQL_TYPE_TINY:
			if n.InternalType.Unsigned {
				return &types.Type{Oid: types.T_uint8, Size: 1, Width: n.InternalType.Width}, nil
			}
			return &types.Type{Oid: types.T_int8, Size: 1, Width: n.InternalType.Width}, nil
		case client.MYSQL_TYPE_SHORT:
			if n.InternalType.Unsigned {
				return &types.Type{Oid: types.T_uint16, Size: 2, Width: n.InternalType.Width}, nil
			}
			return &types.Type{Oid: types.T_int16, Size: 2, Width: n.InternalType.Width}, nil
		case client.MYSQL_TYPE_LONG:
			if n.InternalType.Unsigned {
				return &types.Type{Oid: types.T_uint32, Size: 4, Width: n.InternalType.Width}, nil
			}
			return &types.Type{Oid: types.T_int32, Size: 4, Width: n.InternalType.Width}, nil
		case client.MYSQL_TYPE_LONGLONG:
			if n.InternalType.Unsigned {
				return &types.Type{Oid: types.T_uint64, Size: 8, Width: n.InternalType.Width}, nil
			}
			return &types.Type{Oid: types.T_int64, Size: 8, Width: n.InternalType.Width}, nil
		case client.MYSQL_TYPE_FLOAT:
			return &types.Type{Oid: types.T_float32, Size: 4, Width: n.InternalType.Width}, nil
		case client.MYSQL_TYPE_DOUBLE:
			return &types.Type{Oid: types.T_float64, Size: 8, Width: n.InternalType.Width}, nil
		case client.MYSQL_TYPE_STRING:
			return &types.Type{Oid: types.T_char, Size: 24, Width: n.InternalType.Width}, nil
		case client.MYSQL_TYPE_VAR_STRING, client.MYSQL_TYPE_VARCHAR:
			return &types.Type{Oid: types.T_varchar, Size: 24, Width: n.InternalType.Width}, nil
		}
	}
	return nil, fmt.Errorf("unsupport type: '%v'", typ)
}

func (b *build) tableInfo(stmt tree.TableExpr) (string, string, error) {
	tbl, ok := stmt.(tree.TableName)
	if !ok {
		return "", "", fmt.Errorf("unsupport table: '%v'", stmt)
	}
	if len(tbl.SchemaName) == 0 {
		tbl.SchemaName = tree.Identifier(b.db)
	}
	return string(tbl.SchemaName), string(tbl.ObjectName), nil
}
