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

	"github.com/matrixorigin/matrixone/pkg/defines"

	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/op"
	"github.com/matrixorigin/matrixone/pkg/sql/op/createDatabase"
	"github.com/matrixorigin/matrixone/pkg/sql/op/createTable"
	"github.com/matrixorigin/matrixone/pkg/sql/tree"
	"github.com/matrixorigin/matrixone/pkg/sqlerror"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/metadata"
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
	if stmt.PartitionOption != nil {
		return nil, sqlerror.New(errno.SQLStatementNotYetComplete, "partitionBy not yet complete")
	}
	return createTable.New(stmt.IfNotExists, tblName, defs, nil, db), nil
}

func (b *build) buildCreateDatabase(stmt *tree.CreateDatabase) (op.OP, error) {
	return createDatabase.New(stmt.IfNotExists, string(stmt.Name), b.e), nil
}

func (b *build) getTableDef(def tree.TableDef) (engine.TableDef, error) {
	switch n := def.(type) {
	case *tree.ColumnTableDef:
		var defaultExpr metadata.DefaultExpr
		typ, err := b.getTableDefType(n.Type)
		if err != nil {
			return nil, err
		}
		defaultExpr, err = getDefaultExprFromColumnDef(n, typ)
		if err != nil {
			return nil, err
		}
		primaryKey := false
		for _, attr := range n.Attributes {
			if _, ok := attr.(*tree.AttributePrimaryKey); ok {
				primaryKey = true
			}
		}
		return &engine.AttributeDef{
			Attr: metadata.Attribute{
				Type:       *typ,
				Alg:        compress.Lz4,
				Name:       n.Name.Parts[0],
				Default:    defaultExpr,
				PrimaryKey: primaryKey,
			},
		}, nil
	default:
		return nil, sqlerror.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table def: '%v'", def))
	}
}

func (b *build) getTableDefType(typ tree.ResolvableTypeReference) (*types.Type, error) {
	if n, ok := typ.(*tree.T); ok {
		switch uint8(n.InternalType.Oid) {
		case defines.MYSQL_TYPE_TINY:
			if n.InternalType.Unsigned {
				return &types.Type{Oid: types.T_uint8, Size: 1, Width: n.InternalType.Width}, nil
			}
			return &types.Type{Oid: types.T_int8, Size: 1, Width: n.InternalType.Width}, nil
		case defines.MYSQL_TYPE_SHORT:
			if n.InternalType.Unsigned {
				return &types.Type{Oid: types.T_uint16, Size: 2, Width: n.InternalType.Width}, nil
			}
			return &types.Type{Oid: types.T_int16, Size: 2, Width: n.InternalType.Width}, nil
		case defines.MYSQL_TYPE_LONG:
			if n.InternalType.Unsigned {
				return &types.Type{Oid: types.T_uint32, Size: 4, Width: n.InternalType.Width}, nil
			}
			return &types.Type{Oid: types.T_int32, Size: 4, Width: n.InternalType.Width}, nil
		case defines.MYSQL_TYPE_LONGLONG:
			if n.InternalType.Unsigned {
				return &types.Type{Oid: types.T_uint64, Size: 8, Width: n.InternalType.Width}, nil
			}
			return &types.Type{Oid: types.T_int64, Size: 8, Width: n.InternalType.Width}, nil
		case defines.MYSQL_TYPE_FLOAT:
			return &types.Type{Oid: types.T_float32, Size: 4, Width: n.InternalType.Width}, nil
		case defines.MYSQL_TYPE_DOUBLE:
			return &types.Type{Oid: types.T_float64, Size: 8, Width: n.InternalType.Width}, nil
		case defines.MYSQL_TYPE_STRING:
			if n.InternalType.DisplayWith == -1 { // type char
				return &types.Type{Oid: types.T_char, Size: 24, Width: 1}, nil
			}
			return &types.Type{Oid: types.T_char, Size: 24, Width: n.InternalType.DisplayWith}, nil
		case defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_VARCHAR:
			return &types.Type{Oid: types.T_varchar, Size: 24, Width: n.InternalType.DisplayWith}, nil
		}
	}
	return nil, sqlerror.New(errno.IndeterminateDatatype, fmt.Sprintf("unsupport type: '%v'", typ))
}

func (b *build) tableInfo(stmt tree.TableExpr) (string, string, error) {
	tbl, ok := stmt.(tree.TableName)
	if !ok {
		return "", "", sqlerror.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table: '%v'", stmt))
	}
	if len(tbl.SchemaName) == 0 {
		tbl.SchemaName = tree.Identifier(b.db)
	}
	return string(tbl.SchemaName), string(tbl.ObjectName), nil
}

// getDefaultExprFromColumnDef returns
// has default expr or not / column default expr string / is null expression / error msg
// from column definition when create table
// it will check default expression's type and value, if default values does not adapt to column type
// there will make a simple type conversion for values TODO: not implement
// likes:
// 		create table testTb1 (first int default 15.6) ==> create table testTb1 (first int default 16)
//		create table testTb2 (first int default 'abc') ==> error(Invalid default value for 'first')
func getDefaultExprFromColumnDef(column *tree.ColumnTableDef, typ *types.Type) (metadata.DefaultExpr, error) {
	var ret string    // default expression string
	allowNull := true // be false when column has not null constraint

	{
		for _, attr := range column.Attributes {
			if nullAttr, ok := attr.(*tree.AttributeNull); ok && nullAttr.Is == false {
				allowNull = false
				break
			}
		}
	}

	for _, attr := range column.Attributes {
		if defaultExpr, ok := attr.(*tree.AttributeDefault); ok {
			if isNullExpr(defaultExpr.Expr) {
				if !allowNull {
					return metadata.EmptyDefaultExpr, sqlerror.New(errno.InvalidColumnDefinition, fmt.Sprintf("Invalid default value for '%s'", column.Name.Parts[0]))
				}
				return metadata.MakeDefaultExpr(true, "", true), nil
			}

			// check value and its type, only support constant value for default expression now.
			if _, err := buildConstant(*typ, defaultExpr.Expr); err != nil { // build constant failed
				return metadata.EmptyDefaultExpr, sqlerror.New(errno.InvalidColumnDefinition, fmt.Sprintf("Invalid default value for '%s'", column.Name.Parts[0]))
			} else {
				ret = defaultExpr.Expr.String()
				if errStr := valueRangeCheck(ret, *typ); len(errStr) != 0 { // value out of range
					return metadata.EmptyDefaultExpr, sqlerror.New(errno.InvalidColumnDefinition, fmt.Sprintf("Invalid default value for '%s'", column.Name.Parts[0]))
				}
			}
			return metadata.MakeDefaultExpr(true, ret, false), nil
		}
	}

	// if no definition and allow null value for this column, default will be null
	if allowNull {
		return metadata.MakeDefaultExpr(true, "", true), nil
	}
	return metadata.EmptyDefaultExpr, nil
}
