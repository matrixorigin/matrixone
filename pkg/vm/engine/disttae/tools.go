// Copyright 2022 Matrix Origin
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

package disttae

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plantool "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func genCreateDatabaseTuple(sql string, accountId, userId, roleId uint32,
	name string, databaseId uint64, m *mpool.MPool) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(catalog.MoDatabaseSchema))
	bat.Attrs = append(bat.Attrs, catalog.MoDatabaseSchema...)
	bat.SetZs(1, m)
	{
		idx := catalog.MO_DATABASE_DAT_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoDatabaseTypes[idx]) // dat_id
		if err := bat.Vecs[idx].Append(uint64(databaseId), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_DATABASE_DAT_NAME_IDX
		bat.Vecs[idx] = vector.New(catalog.MoDatabaseTypes[idx]) // datname
		if err := bat.Vecs[idx].Append([]byte(name), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_DATABASE_DAT_CATALOG_NAME_IDX
		bat.Vecs[idx] = vector.New(catalog.MoDatabaseTypes[idx]) // dat_catalog_name
		if err := bat.Vecs[idx].Append([]byte(catalog.MO_CATALOG), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_DATABASE_CREATESQL_IDX
		bat.Vecs[idx] = vector.New(catalog.MoDatabaseTypes[idx])            // dat_createsql
		if err := bat.Vecs[idx].Append([]byte(sql), false, m); err != nil { // TODO
			return nil, err
		}
		idx = catalog.MO_DATABASE_OWNER_IDX
		bat.Vecs[idx] = vector.New(catalog.MoDatabaseTypes[idx]) // owner
		if err := bat.Vecs[idx].Append(roleId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_DATABASE_CREATOR_IDX
		bat.Vecs[idx] = vector.New(catalog.MoDatabaseTypes[idx]) // creator
		if err := bat.Vecs[idx].Append(userId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_DATABASE_CREATED_TIME_IDX
		bat.Vecs[idx] = vector.New(catalog.MoDatabaseTypes[idx]) // created_time
		if err := bat.Vecs[idx].Append(types.Timestamp(time.Now().Unix()), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_DATABASE_ACCOUNT_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoDatabaseTypes[idx]) // account_id
		if err := bat.Vecs[idx].Append(accountId, false, m); err != nil {
			return nil, err
		}
	}
	return bat, nil
}

func genDropDatabaseTuple(id uint64, name string, m *mpool.MPool) (*batch.Batch, error) {
	bat := batch.NewWithSize(2)
	bat.Attrs = append(bat.Attrs, catalog.MoDatabaseSchema[:2]...)
	bat.SetZs(1, m)
	{
		idx := catalog.MO_DATABASE_DAT_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoDatabaseTypes[idx]) // dat_id
		if err := bat.Vecs[idx].Append(id, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_DATABASE_DAT_NAME_IDX
		bat.Vecs[idx] = vector.New(catalog.MoDatabaseTypes[idx]) // datname
		if err := bat.Vecs[idx].Append([]byte(name), false, m); err != nil {
			return nil, err
		}
	}
	return bat, nil
}

func genCreateTableTuple(tbl *table, sql string, accountId, userId, roleId uint32, name string,
	tableId uint64, databaseId uint64, databaseName string,
	comment string, m *mpool.MPool) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(catalog.MoTablesSchema))
	bat.Attrs = append(bat.Attrs, catalog.MoTablesSchema...)
	bat.SetZs(1, m)
	{
		idx := catalog.MO_TABLES_REL_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // rel_id
		if err := bat.Vecs[idx].Append(tableId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_REL_NAME_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // relname
		if err := bat.Vecs[idx].Append([]byte(name), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_RELDATABASE_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // reldatabase
		if err := bat.Vecs[idx].Append([]byte(databaseName), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_RELDATABASE_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // reldatabase_id
		if err := bat.Vecs[idx].Append(databaseId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_RELPERSISTENCE_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // relpersistence
		if err := bat.Vecs[idx].Append([]byte(""), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_RELKIND_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // relkind
		if err := bat.Vecs[idx].Append([]byte(tbl.relKind), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_REL_COMMENT_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // rel_comment
		if err := bat.Vecs[idx].Append([]byte(tbl.comment), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_REL_CREATESQL_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // rel_createsql
		if err := bat.Vecs[idx].Append([]byte(tbl.createSql), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_CREATED_TIME_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // created_time
		if err := bat.Vecs[idx].Append(types.Timestamp(time.Now().Unix()), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_CREATOR_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // creator
		if err := bat.Vecs[idx].Append(userId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_OWNER_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // owner
		if err := bat.Vecs[idx].Append(roleId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_ACCOUNT_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // account_id
		if err := bat.Vecs[idx].Append(accountId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_PARTITIONED_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // partition
		if err := bat.Vecs[idx].Append([]byte(tbl.partition), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_VIEWDEF_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // viewdef
		if err := bat.Vecs[idx].Append([]byte(tbl.viewdef), false, m); err != nil {
			return nil, err
		}

	}
	return bat, nil
}

func genCreateColumnTuple(col column, m *mpool.MPool) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(catalog.MoColumnsSchema))
	bat.Attrs = append(bat.Attrs, catalog.MoColumnsSchema...)
	bat.SetZs(1, m)
	{
		idx := catalog.MO_COLUMNS_ATT_UNIQ_NAME_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_uniq_name
		if err := bat.Vecs[idx].Append([]byte(genColumnPrimaryKey(col.tableId, col.name)),
			false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ACCOUNT_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // account_id
		if err := bat.Vecs[idx].Append(col.accountId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_DATABASE_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_database_id
		if err := bat.Vecs[idx].Append(col.databaseId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_DATABASE_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_database
		if err := bat.Vecs[idx].Append([]byte(col.databaseName), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_RELNAME_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_relname_id
		if err := bat.Vecs[idx].Append(col.tableId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_RELNAME_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_relname
		if err := bat.Vecs[idx].Append([]byte(col.tableName), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATTNAME_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // attname
		if err := bat.Vecs[idx].Append([]byte(col.name), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATTTYP_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // atttyp
		if err := bat.Vecs[idx].Append(col.typ, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATTNUM_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // attnum
		if err := bat.Vecs[idx].Append(col.num, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_LENGTH_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_length
		if err := bat.Vecs[idx].Append(col.typLen, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATTNOTNULL_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // attnotnul
		if err := bat.Vecs[idx].Append(col.notNull, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATTHASDEF_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // atthasdef
		if err := bat.Vecs[idx].Append(col.hasDef, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_DEFAULT_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_default
		if err := bat.Vecs[idx].Append(col.defaultExpr, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATTISDROPPED_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // attisdropped
		if err := bat.Vecs[idx].Append(int8(0), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_CONSTRAINT_TYPE_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_constraint_type
		if err := bat.Vecs[idx].Append([]byte(col.constraintType), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_IS_UNSIGNED_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_is_unsigned
		if err := bat.Vecs[idx].Append(int8(0), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_IS_AUTO_INCREMENT_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_is_auto_increment
		if err := bat.Vecs[idx].Append(col.isAutoIncrement, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_COMMENT_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_comment
		if err := bat.Vecs[idx].Append([]byte(col.comment), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_IS_HIDDEN_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_is_hidden
		if err := bat.Vecs[idx].Append(col.isHidden, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_HAS_UPDATE_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_has_update
		if err := bat.Vecs[idx].Append(col.hasUpdate, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_UPDATE_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_update
		if err := bat.Vecs[idx].Append(col.updateExpr, false, m); err != nil {
			return nil, err
		}

	}
	return bat, nil
}

func genDropTableTuple(id, databaseId uint64, name, databaseName string,
	m *mpool.MPool) (*batch.Batch, error) {
	bat := batch.NewWithSize(4)
	bat.Attrs = append(bat.Attrs, catalog.MoTablesSchema[:4]...)
	bat.SetZs(1, m)
	{
		idx := catalog.MO_TABLES_REL_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // rel_id
		if err := bat.Vecs[idx].Append(id, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_REL_NAME_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // relname
		if err := bat.Vecs[idx].Append([]byte(name), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_RELDATABASE_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // reldatabase
		if err := bat.Vecs[idx].Append([]byte(databaseName), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_RELDATABASE_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // reldatabase_id
		if err := bat.Vecs[idx].Append(databaseId, false, m); err != nil {
			return nil, err
		}
	}
	return bat, nil
}

func genTruncateTableTuple(id, databaseId uint64, name, databaseName string,
	m *mpool.MPool) (*batch.Batch, error) {
	bat := batch.NewWithSize(4)
	bat.Attrs = append(bat.Attrs, catalog.MoTablesSchema[:4]...)
	bat.SetZs(1, m)
	{
		idx := catalog.MO_TABLES_REL_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // rel_id
		if err := bat.Vecs[idx].Append(id, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_REL_NAME_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // relname
		if err := bat.Vecs[idx].Append([]byte(name), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_RELDATABASE_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // reldatabase
		if err := bat.Vecs[idx].Append([]byte(databaseName), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_RELDATABASE_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // reldatabase_id
		if err := bat.Vecs[idx].Append(databaseId, false, m); err != nil {
			return nil, err
		}
	}
	return bat, nil
}

/*
func genDropColumnsTuple(name string) *batch.Batch {
	return &batch.Batch{}
}
*/

// genDatabaseIdExpr generate an expression to find database info
// by database name and accountId
func genDatabaseIdExpr(accountId uint32, name string) *plan.Expr {
	var left, right *plan.Expr

	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(MO_DATABASE_ID_NAME_IDX, types.T_varchar,
			catalog.MoDatabaseSchema[catalog.MO_DATABASE_DAT_NAME_IDX]))
		args = append(args, newStringConstVal(name))
		left = plantool.MakeExpr("=", args)
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(MO_DATABASE_ID_ACCOUNT_IDX, types.T_uint32,
			catalog.MoDatabaseSchema[catalog.MO_DATABASE_ACCOUNT_ID_IDX]))
		args = append(args, newIntConstVal(accountId))
		right = plantool.MakeExpr("=", args)
	}
	return plantool.MakeExpr("and", []*plan.Expr{left, right})
}

// genDatabaseIdExpr generate an expression to find database list
// by accountId
func genDatabaseListExpr(accountId uint32) *plan.Expr {
	var args []*plan.Expr

	args = append(args, newColumnExpr(MO_DATABASE_LIST_ACCOUNT_IDX, types.T_uint32,
		catalog.MoDatabaseSchema[catalog.MO_DATABASE_ACCOUNT_ID_IDX]))
	args = append(args, newIntConstVal(accountId))
	return plantool.MakeExpr("=", args)
}

// genTableInfoExpr generate an expression to find table info
// by database id and table name and accountId
func genTableInfoExpr(accountId uint32, databaseId uint64, name string) *plan.Expr {
	var left, right *plan.Expr

	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(catalog.MO_TABLES_REL_NAME_IDX, types.T_varchar,
			catalog.MoTablesSchema[catalog.MO_TABLES_REL_NAME_IDX]))
		args = append(args, newStringConstVal(name))
		left = plantool.MakeExpr("=", args)
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(catalog.MO_TABLES_RELDATABASE_ID_IDX, types.T_uint64,
			catalog.MoTablesSchema[catalog.MO_TABLES_RELDATABASE_ID_IDX]))
		args = append(args, newIntConstVal(databaseId))
		right = plantool.MakeExpr("=", args)
		left = plantool.MakeExpr("and", []*plan.Expr{left, right})
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(catalog.MO_TABLES_ACCOUNT_ID_IDX, types.T_uint32,
			catalog.MoTablesSchema[catalog.MO_TABLES_ACCOUNT_ID_IDX]))
		args = append(args, newIntConstVal(accountId))
		right = plantool.MakeExpr("=", args)
	}
	return plantool.MakeExpr("and", []*plan.Expr{left, right})
}

// genTableIdExpr generate an expression to find table info
// by database id and table name and accountId
func genTableIdExpr(accountId uint32, databaseId uint64, name string) *plan.Expr {
	var left, right *plan.Expr

	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(MO_TABLE_ID_NAME_IDX, types.T_varchar,
			catalog.MoTablesSchema[catalog.MO_TABLES_REL_NAME_IDX]))
		args = append(args, newStringConstVal(name))
		left = plantool.MakeExpr("=", args)
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(MO_TABLE_ID_DATABASE_ID_IDX, types.T_uint64,
			catalog.MoTablesSchema[catalog.MO_TABLES_RELDATABASE_ID_IDX]))
		args = append(args, newIntConstVal(databaseId))
		right = plantool.MakeExpr("=", args)
		left = plantool.MakeExpr("and", []*plan.Expr{left, right})
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(MO_TABLE_ID_ACCOUNT_IDX, types.T_uint32,
			catalog.MoTablesSchema[catalog.MO_TABLES_ACCOUNT_ID_IDX]))
		args = append(args, newIntConstVal(accountId))
		right = plantool.MakeExpr("=", args)
	}
	return plantool.MakeExpr("and", []*plan.Expr{left, right})
}

// genTableListExpr generate an expression to find table list
// by database id and accountId
func genTableListExpr(accountId uint32, databaseId uint64) *plan.Expr {
	var left, right *plan.Expr

	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(MO_TABLE_LIST_DATABASE_ID_IDX, types.T_uint64,
			catalog.MoTablesSchema[catalog.MO_TABLES_RELDATABASE_ID_IDX]))
		args = append(args, newIntConstVal(databaseId))
		left = plantool.MakeExpr("=", args)
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(MO_TABLE_LIST_ACCOUNT_IDX, types.T_uint32,
			catalog.MoTablesSchema[catalog.MO_TABLES_ACCOUNT_ID_IDX]))
		args = append(args, newIntConstVal(accountId))
		right = plantool.MakeExpr("=", args)
	}
	return plantool.MakeExpr("and", []*plan.Expr{left, right})
}

// genColumnInfoExpr generate an expression to find column info list
// by database id and table id and accountId
func genColumnInfoExpr(accountId uint32, databaseId, tableId uint64) *plan.Expr {
	var left, right *plan.Expr

	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(catalog.MO_COLUMNS_ATT_DATABASE_ID_IDX, types.T_varchar,
			catalog.MoColumnsSchema[catalog.MO_COLUMNS_ATT_DATABASE_ID_IDX]))
		args = append(args, newIntConstVal(databaseId))
		left = plantool.MakeExpr("=", args)
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(catalog.MO_COLUMNS_ATT_RELNAME_ID_IDX, types.T_uint64,
			catalog.MoTablesSchema[catalog.MO_COLUMNS_ATT_RELNAME_ID_IDX]))
		args = append(args, newIntConstVal(tableId))
		right = plantool.MakeExpr("=", args)
		left = plantool.MakeExpr("and", []*plan.Expr{left, right})
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(catalog.MO_COLUMNS_ACCOUNT_ID_IDX, types.T_uint32,
			catalog.MoTablesSchema[catalog.MO_COLUMNS_ACCOUNT_ID_IDX]))
		args = append(args, newIntConstVal(accountId))
		right = plantool.MakeExpr("=", args)
	}
	return plantool.MakeExpr("and", []*plan.Expr{left, right})
}

// genInsertExpr used to generate an expression to partition table data
func genInsertExpr(defs []engine.TableDef, dnNum int) *plan.Expr {
	var args []*plan.Expr

	i := 0
	for _, def := range defs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			if attr.Attr.Primary {
				args = append(args, newColumnExpr(i, attr.Attr.Type.Oid, attr.Attr.Name))
			}
			i++
		}
	}
	if len(args) == 0 {
		return nil
	}
	return plantool.MakeExpr("hash_value", args)
}

func newIntConstVal(v any) *plan.Expr {
	var val int64

	switch x := v.(type) {
	case int32:
		val = int64(x)
	case int64:
		val = int64(x)
	case uint32:
		val = int64(x)
	case uint64:
		val = int64(x)
	}
	return &plan.Expr{
		Typ: types.NewProtoType(types.T_int64),
		Expr: &plan.Expr_C{
			C: &plan.Const{
				Value: &plan.Const_Ival{Ival: int64(val)},
			},
		},
	}
}

func newStringConstVal(v string) *plan.Expr {
	return &plan.Expr{
		Typ: types.NewProtoType(types.T_varchar),
		Expr: &plan.Expr_C{
			C: &plan.Const{
				Value: &plan.Const_Sval{Sval: v},
			},
		},
	}
}

func newColumnExpr(pos int, oid types.T, name string) *plan.Expr {
	return &plan.Expr{
		Typ: types.NewProtoType(oid),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				Name:   name,
				ColPos: int32(pos),
			},
		},
	}
}

func genWriteReqs(writes [][]Entry) ([]txn.TxnRequest, error) {
	mq := make(map[string]DNStore)
	mp := make(map[string][]*api.Entry)
	for i := range writes {
		for _, e := range writes[i] {
			if e.bat.Length() == 0 {
				continue
			}
			pe, err := toPBEntry(e)
			if err != nil {
				return nil, err
			}
			mp[e.dnStore.UUID] = append(mp[e.dnStore.UUID], pe)
			if _, ok := mq[e.dnStore.UUID]; !ok {
				mq[e.dnStore.UUID] = e.dnStore
			}
		}
	}
	reqs := make([]txn.TxnRequest, 0, len(mp))
	for k := range mp {
		payload, err := types.Encode(api.PrecommitWriteCmd{EntryList: mp[k]})
		if err != nil {
			return nil, err
		}
		dn := mq[k]
		for _, info := range dn.Shards {
			reqs = append(reqs, txn.TxnRequest{
				CNRequest: &txn.CNOpRequest{
					OpCode:  uint32(api.OpCode_OpPreCommit),
					Payload: payload,
					Target: metadata.DNShard{
						DNShardRecord: metadata.DNShardRecord{
							ShardID: info.ShardID,
						},
						ReplicaID: info.ReplicaID,
						Address:   dn.ServiceAddress,
					},
				},
				Options: &txn.TxnRequestOptions{
					RetryCodes: []int32{
						// dn shard not found
						int32(moerr.ErrDNShardNotFound),
					},
					RetryInterval: int64(time.Second),
				},
			})
		}
	}
	return reqs, nil
}

func toPBEntry(e Entry) (*api.Entry, error) {
	var ebat *batch.Batch

	if e.typ == INSERT {
		ebat = batch.NewWithSize(0)
		ebat.Vecs = e.bat.Vecs[1:]
		ebat.Attrs = e.bat.Attrs[1:]
	} else {
		ebat = e.bat
	}
	typ := api.Entry_Insert
	if e.typ == DELETE {
		typ = api.Entry_Delete
	}
	bat, err := toPBBatch(ebat)
	if err != nil {
		return nil, err
	}
	return &api.Entry{
		Bat:          bat,
		EntryType:    typ,
		TableId:      e.tableId,
		DatabaseId:   e.databaseId,
		TableName:    e.tableName,
		DatabaseName: e.databaseName,
		FileName:     e.fileName,
		BlockId:      e.blockId,
	}, nil
}

func toPBBatch(bat *batch.Batch) (*api.Batch, error) {
	rbat := new(api.Batch)
	rbat.Attrs = bat.Attrs
	for _, vec := range bat.Vecs {
		pbVector, err := vector.VectorToProtoVector(vec)
		if err != nil {
			return nil, err
		}
		rbat.Vecs = append(rbat.Vecs, pbVector)
	}
	return rbat, nil
}

func getTableComment(defs []engine.TableDef) string {
	for _, def := range defs {
		if cdef, ok := def.(*engine.CommentDef); ok {
			return cdef.Comment
		}
	}
	return ""
}

func genTableDefOfComment(comment string) engine.TableDef {
	return &engine.CommentDef{
		Comment: comment,
	}
}

func getColumnsFromRows(rows [][]any) []column {
	cols := make([]column, len(rows))
	for i, row := range rows {
		cols[i].name = string(row[catalog.MO_COLUMNS_ATTNAME_IDX].([]byte))
		cols[i].comment = string(row[catalog.MO_COLUMNS_ATT_COMMENT_IDX].([]byte))
		cols[i].isHidden = row[catalog.MO_COLUMNS_ATT_IS_HIDDEN_IDX].(int8)
		cols[i].isAutoIncrement = row[catalog.MO_COLUMNS_ATT_IS_AUTO_INCREMENT_IDX].(int8)
		cols[i].constraintType = string(row[catalog.MO_COLUMNS_ATT_CONSTRAINT_TYPE_IDX].([]byte))
		cols[i].typ = row[catalog.MO_COLUMNS_ATTTYP_IDX].([]byte)
		cols[i].hasDef = row[catalog.MO_COLUMNS_ATTHASDEF_IDX].(int8)
		cols[i].defaultExpr = row[catalog.MO_COLUMNS_ATT_DEFAULT_IDX].([]byte)
		cols[i].hasUpdate = row[catalog.MO_COLUMNS_ATT_HAS_UPDATE_IDX].(int8)
		cols[i].updateExpr = row[catalog.MO_COLUMNS_ATT_UPDATE_IDX].([]byte)
		cols[i].num = row[catalog.MO_COLUMNS_ATTNUM_IDX].(int32)
	}
	sort.Sort(Columns(cols))
	return cols
}

func genTableDefOfColumn(col column) engine.TableDef {
	var attr engine.Attribute

	attr.Name = col.name
	attr.Alg = compress.Lz4
	attr.Comment = col.comment
	attr.IsHidden = col.isHidden == 1
	attr.AutoIncrement = col.isAutoIncrement == 1
	if err := types.Decode(col.typ, &attr.Type); err != nil {
		panic(err)
	}
	if col.hasDef == 1 {
		attr.Default = new(plan.Default)
		if err := types.Decode(col.defaultExpr, attr.Default); err != nil {
			panic(err)
		}
	}
	if col.hasUpdate == 1 {
		attr.OnUpdate = new(plan.Expr)
		if err := types.Decode(col.updateExpr, attr.OnUpdate); err != nil {
			panic(err)
		}
	}
	if col.constraintType == catalog.SystemColPKConstraint {
		attr.Primary = true
	}
	return &engine.AttributeDef{Attr: attr}
}

func genColumns(accountId uint32, tableName, databaseName string,
	tableId, databaseId uint64, defs []engine.TableDef) ([]column, error) {
	{ // XXX Why not store PrimaryIndexDef and
		// then use PrimaryIndexDef for all primary key constraints.
		mp := make(map[string]int)
		for i, def := range defs {
			if attr, ok := def.(*engine.AttributeDef); ok {
				mp[attr.Attr.Name] = i
			}
		}
		for _, def := range defs {
			if indexDef, ok := def.(*engine.PrimaryIndexDef); ok {
				for _, name := range indexDef.Names {
					attr, _ := defs[mp[name]].(*engine.AttributeDef)
					attr.Attr.Primary = true
				}
			}
		}
	}
	num := 0
	cols := make([]column, 0, len(defs))
	for _, def := range defs {
		attrDef, ok := def.(*engine.AttributeDef)
		if !ok {
			continue
		}
		typ, err := types.Encode(&attrDef.Attr.Type)
		if err != nil {
			return nil, err
		}
		col := column{
			typ:          typ,
			typLen:       int32(len(typ)),
			accountId:    accountId,
			tableId:      tableId,
			databaseId:   databaseId,
			name:         attrDef.Attr.Name,
			tableName:    tableName,
			databaseName: databaseName,
			num:          int32(num),
			comment:      attrDef.Attr.Comment,
		}
		col.hasDef = 0
		if attrDef.Attr.Default != nil {
			defaultExpr, err := types.Encode(attrDef.Attr.Default)
			if err != nil {
				return nil, err
			}
			if len(defaultExpr) > 0 {
				col.hasDef = 1
				col.defaultExpr = defaultExpr
			}
		}
		if attrDef.Attr.OnUpdate != nil {
			expr, err := types.Encode(attrDef.Attr.OnUpdate)
			if err != nil {
				return nil, err
			}
			if len(expr) > 0 {
				col.hasUpdate = 1
				col.updateExpr = expr
			}
		}
		if attrDef.Attr.IsHidden {
			col.isHidden = 1
		}
		if attrDef.Attr.AutoIncrement {
			col.isAutoIncrement = 1
		}
		if attrDef.Attr.Primary {
			col.constraintType = catalog.SystemColPKConstraint
		} else {
			col.constraintType = catalog.SystemColNoConstraint
		}
		cols = append(cols, col)
		num++
	}
	return cols, nil
}

func getSql(ctx context.Context) string {
	if v := ctx.Value(defines.SqlKey{}); v != nil {
		return v.(string)
	}
	return ""
}

func getAccountId(ctx context.Context) uint32 {
	if v := ctx.Value(defines.TenantIDKey{}); v != nil {
		return v.(uint32)
	}
	return 0
}

func getAccessInfo(ctx context.Context) (uint32, uint32, uint32) {
	var accountId, userId, roleId uint32

	if v := ctx.Value(defines.TenantIDKey{}); v != nil {
		accountId = v.(uint32)
	}
	if v := ctx.Value(defines.UserIDKey{}); v != nil {
		userId = v.(uint32)
	}
	if v := ctx.Value(defines.RoleIDKey{}); v != nil {
		roleId = v.(uint32)
	}
	return accountId, userId, roleId
}

func partitionBatch(bat *batch.Batch, expr *plan.Expr, proc *process.Process, dnNum int) ([]*batch.Batch, error) {
	pvec, err := colexec.EvalExpr(bat, proc, expr)
	if err != nil {
		return nil, err
	}
	defer pvec.Free(proc.Mp())
	bats := make([]*batch.Batch, dnNum)
	for i := range bats {
		bats[i] = batch.New(true, bat.Attrs)
		for j := range bats[i].Vecs {
			bats[i].SetVector(int32(j), vector.New(bat.GetVector(int32(j)).GetType()))
		}
	}
	vs := vector.GetFixedVectorValues[int64](pvec)
	for i := range bat.Vecs {
		vec := bat.GetVector(int32(i))
		for j, v := range vs {
			idx := uint64(v) % uint64(dnNum)
			if err := vector.UnionOne(bats[idx].GetVector(int32(i)), vec, int64(j), proc.Mp()); err != nil {
				for _, bat := range bats {
					bat.Clean(proc.Mp())
				}
				return nil, err
			}
		}
	}
	for i := range bats {
		bats[i].SetZs(bats[i].GetVector(0).Length(), proc.Mp())
	}
	return bats, nil
}

func partitionDeleteBatch(tbl *table, bat *batch.Batch) ([]*batch.Batch, error) {
	txn := tbl.db.txn
	bats := make([]*batch.Batch, len(tbl.parts))
	for i := range bats {
		bats[i] = batch.New(true, bat.Attrs)
		for j := range bats[i].Vecs {
			bats[i].SetVector(int32(j), vector.New(bat.GetVector(int32(j)).GetType()))
		}
	}
	vec := bat.GetVector(0)
	vs := vector.MustTCols[types.Rowid](vec)
	for i, v := range vs {
		for j, part := range tbl.parts {
			if part.Get(v, txn.meta.SnapshotTS) {
				if err := vector.UnionOne(bats[j].GetVector(0), vec, int64(i), txn.proc.Mp()); err != nil {
					for _, bat := range bats {
						bat.Clean(txn.proc.Mp())
					}
					return nil, err
				}
				break
			}
		}
	}
	for i := range bats {
		bats[i].SetZs(bats[i].GetVector(0).Length(), txn.proc.Mp())
	}
	return bats, nil
}

func genDatabaseKey(ctx context.Context, name string) databaseKey {
	return databaseKey{
		name:      name,
		accountId: getAccountId(ctx),
	}
}

func genTableKey(ctx context.Context, name string, databaseId uint64) tableKey {
	return tableKey{
		name:       name,
		databaseId: databaseId,
		accountId:  getAccountId(ctx),
	}
}

func genMetaTableName(id uint64) string {
	return fmt.Sprintf("_%v_meta", id)
}

func isMetaTable(name string) bool {
	ok, _ := regexp.MatchString(`\_\d+\_meta`, name)
	return ok
}

func genBlockMetas(rows [][]any, columnLength int, fs fileservice.FileService, m *mpool.MPool) ([]BlockMeta, error) {
	blockInfos := catalog.GenBlockInfo(rows)
	{
		mp := make(map[uint64]catalog.BlockInfo) // block list
		for i := range blockInfos {
			if blk, ok := mp[blockInfos[i].BlockID]; ok &&
				blk.CommitTs.Less(blockInfos[i].CommitTs) {
				mp[blk.BlockID] = blockInfos[i]
			} else {
				mp[blk.BlockID] = blockInfos[i]
			}
		}
		blockInfos = blockInfos[:0]
		for _, blk := range mp {
			blockInfos = append(blockInfos, blk)
		}
	}
	metas := make([]BlockMeta, len(rows))

	idxs := make([]uint16, columnLength)
	for i := 0; i < columnLength; i++ {
		idxs[i] = uint16(i)
	}

	for i, blockInfo := range blockInfos {
		zm, err := fetchZonemapFromBlockInfo(idxs, blockInfo, fs, m)
		if err != nil {
			return nil, err
		}
		metas[i] = BlockMeta{
			Info:    blockInfo,
			Zonemap: zm,
		}
	}
	return metas, nil
}

func inBlockList(blk BlockMeta, blks []BlockMeta) bool {
	for i := range blks {
		if blk.Eq(blks[i]) {
			return true
		}
	}
	return false
}

func genModifedBlocks(deletes map[uint64][]int, orgs, modfs []BlockMeta,
	expr *plan.Expr, tableDef *plan.TableDef, proc *process.Process) []ModifyBlockMeta {
	blks := make([]ModifyBlockMeta, 0, len(orgs)-len(modfs))
	for i, blk := range orgs {
		if !inBlockList(blk, modfs) {
			if needRead(expr, blk, tableDef, proc) {
				blks = append(blks, ModifyBlockMeta{
					meta:    orgs[i],
					deletes: deletes[orgs[i].Info.BlockID],
				})
			}
		}
	}
	return blks
}

func genInsertBatch(bat *batch.Batch, m *mpool.MPool) (*api.Batch, error) {
	var attrs []string
	var vecs []*vector.Vector

	{
		vec := vector.New(types.New(types.T_Rowid, 0, 0, 0))
		for i := 0; i < bat.Length(); i++ {
			val := types.Rowid(uuid.New())
			if err := vec.Append(val, false, m); err != nil {
				return nil, err
			}
		}
		vecs = append(vecs, vec)
		attrs = append(attrs, "rowid")
	}
	{
		var val types.TS

		vec := vector.New(types.New(types.T_TS, 0, 0, 0))
		for i := 0; i < bat.Length(); i++ {
			if err := vec.Append(val, false, m); err != nil {
				return nil, err
			}
		}
		vecs = append(vecs, vec)
		attrs = append(attrs, "timestamp")
	}
	bat.Vecs = append(vecs, bat.Vecs...)
	bat.Attrs = append(attrs, bat.Attrs...)
	return batch.BatchToProtoBatch(bat)
}

func genColumnPrimaryKey(tableId uint64, name string) string {
	return fmt.Sprintf("%v-%v", tableId, name)
}
