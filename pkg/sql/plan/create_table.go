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
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go/constant"
	"math"
)

func (b *build) BuildCreateTable(stmt *tree.CreateTable, plan *CreateTable) error {
	var primaryKeys []string // stores primary key column's names.
	defs := make([]engine.TableDef, 0, len(stmt.Defs))

	// semantic analysis
	dbName, tblName, err := b.tableInfo(stmt.Table)
	if err != nil {
		return err
	}
	db, err := b.e.Database(dbName)
	if err != nil {
		return err
	}

	addPrimaryIndex := true
	for i := range stmt.Defs {
		def, pkeys, err := b.getTableDef(stmt.Defs[i])
		if err != nil {
			return err
		}
		if primaryKeys != nil && pkeys != nil {
			return errors.New(errno.SyntaxErrororAccessRuleViolation, "Multiple primary key defined")
		}
		if _, ok := def.(*engine.PrimaryIndexDef); ok {
			addPrimaryIndex = false
		}
		primaryKeys = pkeys
		defs = append(defs, def)
	}
	if addPrimaryIndex && primaryKeys != nil {
		defs = append(defs, &engine.PrimaryIndexDef{Names: primaryKeys})
	}

	if stmt.PartitionOption != nil {
		return errors.New(errno.SQLStatementNotYetComplete, "partitionBy not yet complete")
	}
	// returns
	plan.IfNotExistFlag = stmt.IfNotExists
	plan.Defs = defs
	plan.Db = db
	plan.Id = tblName
	return nil
}

func (b *build) tableInfo(stmt tree.TableExpr) (string, string, error) {
	tbl, ok := stmt.(tree.TableName)
	if !ok {
		return "", "", errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table: '%v'", stmt))
	}
	if len(tbl.SchemaName) == 0 {
		tbl.SchemaName = tree.Identifier(b.db)
	}
	return string(tbl.SchemaName), string(tbl.ObjectName), nil
}

func (b *build) getTableDef(def tree.TableDef) (engine.TableDef, []string, error) {
	var primaryKeys []string = nil

	switch n := def.(type) {
	case *tree.ColumnTableDef:
		typ, err := b.getTableDefType(n.Type)
		if err != nil {
			return nil, nil, err
		}

		defaultExpr, err := getDefaultExprFromColumnDef(n, typ)
		if err != nil {
			return nil, nil, err
		}

		for _, attr := range n.Attributes {
			if _, ok := attr.(*tree.AttributePrimaryKey); ok {
				primaryKeys = append(primaryKeys, n.Name.Parts[0])
			}
		}

		return &engine.AttributeDef{
			Attr: engine.Attribute{
				Name:    n.Name.Parts[0],
				Alg:     compress.Lz4,
				Type:    *typ,
				Default: defaultExpr,
			},
		}, primaryKeys, nil
	case *tree.PrimaryKeyIndex: // todo: need to change if parser will use another AST
		mapPrimaryKeyNames := map[string]struct{}{}
		pkNames := make([]string, len(n.KeyParts))
		for i, key := range n.KeyParts {
			if _, ok := mapPrimaryKeyNames[key.ColName.Parts[0]]; ok {
				return nil, nil, errors.New(errno.InvalidTableDefinition, fmt.Sprintf("Duplicate column name '%s'", key.ColName.Parts[0]))
			}
			pkNames[i] = key.ColName.Parts[0]
			mapPrimaryKeyNames[key.ColName.Parts[0]] = struct{}{}
		}
		return &engine.PrimaryIndexDef{
			Names: pkNames,
		}, pkNames, nil
	default:
		return nil, nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table def: '%v'", def))
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
		case defines.MYSQL_TYPE_DATE:
			return &types.Type{Oid: types.T_date, Size: 4}, nil
		}
	}
	return nil, errors.New(errno.IndeterminateDatatype, fmt.Sprintf("unsupport type: '%v'", typ))
}

// getDefaultExprFromColumnDef returns
// has default expr or not / column default expr string / is null expression / error msg
// from column definition when create table
// it will check default expression's type and value, if default values does not adapt to column type
// there will make a simple type conversion for values
// likes:
// 		create table testTb1 (first int default 15.6) ==> create table testTb1 (first int default 16)
//		create table testTb2 (first int default 'abc') ==> error(Invalid default value for 'first')
func getDefaultExprFromColumnDef(column *tree.ColumnTableDef, typ *types.Type) (engine.DefaultExpr, error) {
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
		if d, ok := attr.(*tree.AttributeDefault); ok {
			defaultExpr := d.Expr
			if isNullExpr(defaultExpr) {
				if !allowNull {
					return engine.EmptyDefaultExpr, errors.New(errno.InvalidColumnDefinition, fmt.Sprintf("Invalid default value for '%s'", column.Name.Parts[0]))
				}
				return engine.MakeDefaultExpr(true, nil, true), nil
			}

			// check value and its type, only support constant value for default expression now.
			var value interface{}
			var err error
			if value, err = buildConstant(*typ, defaultExpr); err != nil { // build constant failed
				return engine.EmptyDefaultExpr, errors.New(errno.InvalidColumnDefinition, fmt.Sprintf("Invalid default value for '%s'", column.Name.Parts[0]))
			}
			if _, err = rangeCheck(value, *typ, "", 0); err != nil { // value out of range
				return engine.EmptyDefaultExpr, errors.New(errno.InvalidColumnDefinition, fmt.Sprintf("Invalid default value for '%s'", column.Name.Parts[0]))
			}
			return engine.MakeDefaultExpr(true, value, false), nil
		}
	}

	// if no definition and allow null value for this column, default will be null
	if allowNull {
		return engine.MakeDefaultExpr(true, "", true), nil
	}
	return engine.EmptyDefaultExpr, nil
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
	case types.Date:
		return v, nil
	default:
		return nil, errors.New(errno.DatatypeMismatch, "unexpected type and value")
	}
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
