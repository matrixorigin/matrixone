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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plantool "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func genCreateDatabaseTuple(sql string, accountId, userId, roleId uint32,
	name string, databaseId uint64, typ string, m *mpool.MPool) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(catalog.MoDatabaseSchema))
	bat.Attrs = append(bat.Attrs, catalog.MoDatabaseSchema...)
	bat.SetZs(1, m)
	{
		idx := catalog.MO_DATABASE_DAT_ID_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoDatabaseTypes[idx]) // dat_id
		if err := vector.AppendFixed(bat.Vecs[idx], uint64(databaseId), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_DATABASE_DAT_NAME_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoDatabaseTypes[idx]) // datname
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(name), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_DATABASE_DAT_CATALOG_NAME_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoDatabaseTypes[idx]) // dat_catalog_name
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(catalog.MO_CATALOG), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_DATABASE_CREATESQL_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoDatabaseTypes[idx])                      // dat_createsql
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(sql), false, m); err != nil { // TODO
			return nil, err
		}
		idx = catalog.MO_DATABASE_OWNER_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoDatabaseTypes[idx]) // owner
		if err := vector.AppendFixed(bat.Vecs[idx], roleId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_DATABASE_CREATOR_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoDatabaseTypes[idx]) // creator
		if err := vector.AppendFixed(bat.Vecs[idx], userId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_DATABASE_CREATED_TIME_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoDatabaseTypes[idx]) // created_time
		if err := vector.AppendFixed(bat.Vecs[idx], types.Timestamp(time.Now().UnixMicro()+types.GetUnixEpochSecs()), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_DATABASE_ACCOUNT_ID_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoDatabaseTypes[idx]) // account_id
		if err := vector.AppendFixed(bat.Vecs[idx], accountId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_DATABASE_DAT_TYPE_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoDatabaseTypes[idx])                      // dat_type
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(typ), false, m); err != nil { // TODO
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
		bat.Vecs[idx] = vector.NewVec(catalog.MoDatabaseTypes[idx]) // dat_id
		if err := vector.AppendFixed(bat.Vecs[idx], id, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_DATABASE_DAT_NAME_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoDatabaseTypes[idx]) // datname
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(name), false, m); err != nil {
			return nil, err
		}
	}
	return bat, nil
}

func genTableConstraintTuple(tblId, dbId uint64, tblName, dbName string, constraint []byte,
	m *mpool.MPool) (*batch.Batch, error) {
	bat := batch.NewWithSize(5)
	bat.Attrs = append(bat.Attrs, catalog.MoTablesSchema[:4]...)
	bat.Attrs = append(bat.Attrs, catalog.SystemRelAttr_Constraint)
	bat.SetZs(1, m)

	{
		idx := catalog.MO_TABLES_REL_ID_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // rel_id
		if err := vector.AppendFixed(bat.Vecs[idx], tblId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_REL_NAME_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // relname
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(tblName), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_RELDATABASE_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // reldatabase
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(dbName), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_RELDATABASE_ID_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // reldatabase_id
		if err := vector.AppendFixed(bat.Vecs[idx], dbId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_UPDATE_CONSTRAINT
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[catalog.MO_TABLES_CONSTRAINT_IDX]) // constraint
		if err := vector.AppendBytes(bat.Vecs[idx], constraint, false, m); err != nil {
			return nil, err
		}
	}

	return bat, nil
}

func genCreateTableTuple(tbl *txnTable, sql string, accountId, userId, roleId uint32, name string,
	tableId uint64, databaseId uint64, databaseName string, m *mpool.MPool) (*batch.Batch, error) {
	_ = sql //TODO delete this param if not required
	bat := batch.NewWithSize(len(catalog.MoTablesSchema))
	bat.Attrs = append(bat.Attrs, catalog.MoTablesSchema...)
	bat.SetZs(1, m)
	{
		idx := catalog.MO_TABLES_REL_ID_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // rel_id
		if err := vector.AppendFixed(bat.Vecs[idx], tableId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_REL_NAME_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // relname
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(name), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_RELDATABASE_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // reldatabase
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(databaseName), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_RELDATABASE_ID_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // reldatabase_id
		if err := vector.AppendFixed(bat.Vecs[idx], databaseId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_RELPERSISTENCE_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // relpersistence
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(""), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_RELKIND_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // relkind
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(tbl.relKind), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_REL_COMMENT_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // rel_comment
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(tbl.comment), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_REL_CREATESQL_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // rel_createsql
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(tbl.createSql), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_CREATED_TIME_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // created_time
		if err := vector.AppendFixed(bat.Vecs[idx], types.Timestamp(time.Now().Unix()), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_CREATOR_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // creator
		if err := vector.AppendFixed(bat.Vecs[idx], userId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_OWNER_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // owner
		if err := vector.AppendFixed(bat.Vecs[idx], roleId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_ACCOUNT_ID_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // account_id
		if err := vector.AppendFixed(bat.Vecs[idx], accountId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_PARTITIONED_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // partition
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(tbl.partition), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_VIEWDEF_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // viewdef
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(tbl.viewdef), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_CONSTRAINT_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // constraint
		if err := vector.AppendBytes(bat.Vecs[idx], tbl.constraint, false, m); err != nil {
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
		bat.Vecs[idx] = vector.NewVec(catalog.MoColumnsTypes[idx]) // att_uniq_name
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(genColumnPrimaryKey(col.tableId, col.name)),
			false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ACCOUNT_ID_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoColumnsTypes[idx]) // account_id
		if err := vector.AppendFixed(bat.Vecs[idx], col.accountId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_DATABASE_ID_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoColumnsTypes[idx]) // att_database_id
		if err := vector.AppendFixed(bat.Vecs[idx], col.databaseId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_DATABASE_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoColumnsTypes[idx]) // att_database
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(col.databaseName), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_RELNAME_ID_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoColumnsTypes[idx]) // att_relname_id
		if err := vector.AppendFixed(bat.Vecs[idx], col.tableId, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_RELNAME_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoColumnsTypes[idx]) // att_relname
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(col.tableName), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATTNAME_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoColumnsTypes[idx]) // attname
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(col.name), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATTTYP_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoColumnsTypes[idx]) // atttyp
		if err := vector.AppendBytes(bat.Vecs[idx], col.typ, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATTNUM_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoColumnsTypes[idx]) // attnum
		if err := vector.AppendFixed(bat.Vecs[idx], col.num, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_LENGTH_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoColumnsTypes[idx]) // att_length
		if err := vector.AppendFixed(bat.Vecs[idx], col.typLen, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATTNOTNULL_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoColumnsTypes[idx]) // attnotnul
		if err := vector.AppendFixed(bat.Vecs[idx], col.notNull, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATTHASDEF_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoColumnsTypes[idx]) // atthasdef
		if err := vector.AppendFixed(bat.Vecs[idx], col.hasDef, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_DEFAULT_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoColumnsTypes[idx]) // att_default
		if err := vector.AppendBytes(bat.Vecs[idx], col.defaultExpr, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATTISDROPPED_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoColumnsTypes[idx]) // attisdropped
		if err := vector.AppendFixed(bat.Vecs[idx], int8(0), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_CONSTRAINT_TYPE_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoColumnsTypes[idx]) // att_constraint_type
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(col.constraintType), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_IS_UNSIGNED_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoColumnsTypes[idx]) // att_is_unsigned
		if err := vector.AppendFixed(bat.Vecs[idx], int8(0), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_IS_AUTO_INCREMENT_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoColumnsTypes[idx]) // att_is_auto_increment
		if err := vector.AppendFixed(bat.Vecs[idx], col.isAutoIncrement, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_COMMENT_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoColumnsTypes[idx]) // att_comment
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(col.comment), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_IS_HIDDEN_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoColumnsTypes[idx]) // att_is_hidden
		if err := vector.AppendFixed(bat.Vecs[idx], col.isHidden, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_HAS_UPDATE_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoColumnsTypes[idx]) // att_has_update
		if err := vector.AppendFixed(bat.Vecs[idx], col.hasUpdate, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_UPDATE_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoColumnsTypes[idx]) // att_update
		if err := vector.AppendBytes(bat.Vecs[idx], col.updateExpr, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATT_IS_CLUSTERBY
		bat.Vecs[idx] = vector.NewVec(catalog.MoColumnsTypes[idx]) // att_constraint_type
		if err := vector.AppendFixed(bat.Vecs[idx], col.isClusterBy, false, m); err != nil {
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
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // rel_id
		if err := vector.AppendFixed(bat.Vecs[idx], id, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_REL_NAME_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // relname
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(name), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_RELDATABASE_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // reldatabase
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(databaseName), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_RELDATABASE_ID_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // reldatabase_id
		if err := vector.AppendFixed(bat.Vecs[idx], databaseId, false, m); err != nil {
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
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // rel_id
		if err := vector.AppendFixed(bat.Vecs[idx], id, false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_REL_NAME_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // relname
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(name), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_RELDATABASE_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // reldatabase
		if err := vector.AppendBytes(bat.Vecs[idx], []byte(databaseName), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_RELDATABASE_ID_IDX
		bat.Vecs[idx] = vector.NewVec(catalog.MoTablesTypes[idx]) // reldatabase_id
		if err := vector.AppendFixed(bat.Vecs[idx], databaseId, false, m); err != nil {
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
/*
func genDatabaseIdExpr(ctx context.Context, accountId uint32, name string) *plan.Expr {
	var left, right *plan.Expr

	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(MO_DATABASE_ID_NAME_IDX, types.T_varchar,
			catalog.MoDatabaseSchema[catalog.MO_DATABASE_DAT_NAME_IDX]))
		args = append(args, newStringConstVal(name))
		left = plantool.MakeExpr(ctx, "=", args)
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(MO_DATABASE_ID_ACCOUNT_IDX, types.T_uint32,
			catalog.MoDatabaseSchema[catalog.MO_DATABASE_ACCOUNT_ID_IDX]))
		args = append(args, newIntConstVal(accountId))
		right = plantool.MakeExpr(ctx, "=", args)
	}
	return plantool.MakeExpr(ctx, "and", []*plan.Expr{left, right})
}
*/

/*
// genDatabaseIdExpr generate an expression to find database list
// by accountId
func genDatabaseListExpr(ctx context.Context, accountId uint32) *plan.Expr {
	var args []*plan.Expr

	args = append(args, newColumnExpr(MO_DATABASE_LIST_ACCOUNT_IDX, types.T_uint32,
		catalog.MoDatabaseSchema[catalog.MO_DATABASE_ACCOUNT_ID_IDX]))
	args = append(args, newIntConstVal(accountId))
	return plantool.MakeExpr(ctx, "=", args)
}

// genTableInfoExpr generate an expression to find table info
// by database id and table name and accountId
func genTableInfoExpr(ctx context.Context, accountId uint32, databaseId uint64, name string) *plan.Expr {
	var left, right *plan.Expr

	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(catalog.MO_TABLES_REL_NAME_IDX, types.T_varchar,
			catalog.MoTablesSchema[catalog.MO_TABLES_REL_NAME_IDX]))
		args = append(args, newStringConstVal(name))
		left = plantool.MakeExpr(ctx, "=", args)
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(catalog.MO_TABLES_RELDATABASE_ID_IDX, types.T_uint64,
			catalog.MoTablesSchema[catalog.MO_TABLES_RELDATABASE_ID_IDX]))
		args = append(args, newIntConstVal(databaseId))
		right = plantool.MakeExpr(ctx, "=", args)
		left = plantool.MakeExpr(ctx, "and", []*plan.Expr{left, right})
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(catalog.MO_TABLES_ACCOUNT_ID_IDX, types.T_uint32,
			catalog.MoTablesSchema[catalog.MO_TABLES_ACCOUNT_ID_IDX]))
		args = append(args, newIntConstVal(accountId))
		right = plantool.MakeExpr(ctx, "=", args)
	}
	return plantool.MakeExpr(ctx, "and", []*plan.Expr{left, right})
}

// genTableIdExpr generate an expression to find table info
// by database id and table name and accountId
func genTableIdExpr(ctx context.Context, accountId uint32, databaseId uint64, name string) *plan.Expr {
	var left, right *plan.Expr

	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(MO_TABLE_ID_NAME_IDX, types.T_varchar,
			catalog.MoTablesSchema[catalog.MO_TABLES_REL_NAME_IDX]))
		args = append(args, newStringConstVal(name))
		left = plantool.MakeExpr(ctx, "=", args)
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(MO_TABLE_ID_DATABASE_ID_IDX, types.T_uint64,
			catalog.MoTablesSchema[catalog.MO_TABLES_RELDATABASE_ID_IDX]))
		args = append(args, newIntConstVal(databaseId))
		right = plantool.MakeExpr(ctx, "=", args)
		left = plantool.MakeExpr(ctx, "and", []*plan.Expr{left, right})
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(MO_TABLE_ID_ACCOUNT_IDX, types.T_uint32,
			catalog.MoTablesSchema[catalog.MO_TABLES_ACCOUNT_ID_IDX]))
		args = append(args, newIntConstVal(accountId))
		right = plantool.MakeExpr(ctx, "=", args)
	}
	return plantool.MakeExpr(ctx, "and", []*plan.Expr{left, right})
}

// genTableListExpr generate an expression to find table list
// by database id and accountId
func genTableListExpr(ctx context.Context, accountId uint32, databaseId uint64) *plan.Expr {
	var left, right *plan.Expr

	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(MO_TABLE_LIST_DATABASE_ID_IDX, types.T_uint64,
			catalog.MoTablesSchema[catalog.MO_TABLES_RELDATABASE_ID_IDX]))
		args = append(args, newIntConstVal(databaseId))
		left = plantool.MakeExpr(ctx, "=", args)
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(MO_TABLE_LIST_ACCOUNT_IDX, types.T_uint32,
			catalog.MoTablesSchema[catalog.MO_TABLES_ACCOUNT_ID_IDX]))
		args = append(args, newIntConstVal(accountId))
		right = plantool.MakeExpr(ctx, "=", args)
	}
	return plantool.MakeExpr(ctx, "and", []*plan.Expr{left, right})
}

// genColumnInfoExpr generate an expression to find column info list
// by database id and table id and accountId
func genColumnInfoExpr(ctx context.Context, accountId uint32, databaseId, tableId uint64) *plan.Expr {
	var left, right *plan.Expr

	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(catalog.MO_COLUMNS_ATT_DATABASE_ID_IDX, types.T_uint64,
			catalog.MoColumnsSchema[catalog.MO_COLUMNS_ATT_DATABASE_ID_IDX]))
		args = append(args, newIntConstVal(databaseId))
		left = plantool.MakeExpr(ctx, "=", args)
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(catalog.MO_COLUMNS_ATT_RELNAME_ID_IDX, types.T_uint64,
			catalog.MoTablesSchema[catalog.MO_COLUMNS_ATT_RELNAME_ID_IDX]))
		args = append(args, newIntConstVal(tableId))
		right = plantool.MakeExpr(ctx, "=", args)
		left = plantool.MakeExpr(ctx, "and", []*plan.Expr{left, right})
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(catalog.MO_COLUMNS_ACCOUNT_ID_IDX, types.T_uint32,
			catalog.MoTablesSchema[catalog.MO_COLUMNS_ACCOUNT_ID_IDX]))
		args = append(args, newIntConstVal(accountId))
		right = plantool.MakeExpr(ctx, "=", args)
	}
	return plantool.MakeExpr(ctx, "and", []*plan.Expr{left, right})
}

// genInsertExpr used to generate an expression to partition table data
func genInsertExpr(ctx context.Context, defs []engine.TableDef, dnNum int) *plan.Expr {
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
	return plantool.MakeExpr(ctx, "hash_value", args)
}
*/

/*
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
	return plantool.MakePlan2Int64ConstExprWithType(val)
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
*/

func genWriteReqs(ctx context.Context, writes [][]Entry) ([]txn.TxnRequest, error) {
	mq := make(map[string]DNStore)
	mp := make(map[string][]*api.Entry)
	v := ctx.Value(defines.PkCheckByDN{})
	for i := range writes {
		for _, e := range writes[i] {
			if e.bat.Length() == 0 {
				continue
			}
			if v != nil {
				e.pkChkByDN = v.(int8)
			}
			pe, err := toPBEntry(e)
			if err != nil {
				return nil, err
			}
			mp[e.dnStore.ServiceID] = append(mp[e.dnStore.ServiceID], pe)
			if _, ok := mq[e.dnStore.ServiceID]; !ok {
				mq[e.dnStore.ServiceID] = e.dnStore
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
						Address:   dn.TxnServiceAddress,
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
		if e.bat.Attrs[0] == catalog.BlockMeta_MetaLoc {
			ebat.Vecs = e.bat.Vecs
			ebat.Attrs = e.bat.Attrs
		} else {
			ebat.Vecs = e.bat.Vecs[1:]
			ebat.Attrs = e.bat.Attrs[1:]
		}
	} else {
		ebat = e.bat
	}
	typ := api.Entry_Insert
	if e.typ == DELETE {
		typ = api.Entry_Delete
	} else if e.typ == UPDATE {
		typ = api.Entry_Update
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
		PkCheckByDn:  int32(e.pkChkByDN),
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

/*
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
		cols[i].isClusterBy = row[catalog.MO_COLUMNS_ATT_IS_CLUSTERBY].(int8)
	}
	sort.Sort(Columns(cols))
	return cols
}

func genTableDefOfColumn(col column) engine.TableDef {
	var attr engine.Attribute

	attr.ID = uint64(col.num)
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
		attr.OnUpdate = new(plan.OnUpdate)
		if err := types.Decode(col.updateExpr, attr.OnUpdate); err != nil {
			panic(err)
		}
	}
	if col.constraintType == catalog.SystemColPKConstraint {
		attr.Primary = true
	}
	if col.isClusterBy == 1 {
		attr.ClusterBy = true
	}
	return &engine.AttributeDef{Attr: attr}
}
*/

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
			if constraintDef, ok := def.(*engine.ConstraintDef); ok {
				for _, ct := range constraintDef.Cts {
					if pkdef, ok2 := ct.(*engine.PrimaryKeyDef); ok2 {
						pos := mp[pkdef.Pkey.PkeyColName]
						attr, _ := defs[pos].(*engine.AttributeDef)
						attr.Attr.Primary = true
					}
				}
			}

			if clusterByDef, ok := def.(*engine.ClusterByDef); ok {
				attr, _ := defs[mp[clusterByDef.Name]].(*engine.AttributeDef)
				attr.Attr.ClusterBy = true
			}
		}
	}
	var num int32 = 1
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
			num:          num,
			comment:      attrDef.Attr.Comment,
		}
		attrDef.Attr.ID = uint64(num)
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
		if attrDef.Attr.ClusterBy {
			col.isClusterBy = 1
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
func getTyp(ctx context.Context) string {
	if v := ctx.Value(defines.DatTypKey{}); v != nil {
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
			bats[i].SetVector(int32(j), vector.NewVec(*bat.GetVector(int32(j)).GetType()))
		}
	}
	vs := vector.MustFixedCol[int64](pvec)
	for i := range bat.Vecs {
		vec := bat.GetVector(int32(i))
		for j, v := range vs {
			idx := uint64(v) % uint64(dnNum)
			if err := bats[idx].GetVector(int32(i)).UnionOne(vec, int64(j), proc.Mp()); err != nil {
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

func partitionDeleteBatch(tbl *txnTable, bat *batch.Batch) ([]*batch.Batch, error) {
	txn := tbl.db.txn
	bats := make([]*batch.Batch, len(tbl.parts))
	for i := range bats {
		bats[i] = batch.New(true, bat.Attrs)
		for j := range bats[i].Vecs {
			bats[i].SetVector(int32(j), vector.NewVec(*bat.GetVector(int32(j)).GetType()))
		}
	}
	vec := bat.GetVector(0)
	vs := vector.MustFixedCol[types.Rowid](vec)
	for i, v := range vs {
		for j, part := range tbl.parts {
			var blks []BlockMeta

			if tbl.meta != nil {
				blks = tbl.meta.blocks[j]
			}
			if inPartition(v, part, txn.meta.SnapshotTS, blks) {
				if err := bats[j].GetVector(0).UnionOne(vec, int64(i), txn.proc.Mp()); err != nil {
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

var metaTableMatchRegexp *regexp.Regexp

func init() {
	metaTableMatchRegexp, _ = regexp.Compile(`\_\d+\_meta`)
}

func isMetaTable(name string) bool {
	return metaTableMatchRegexp.MatchString(name)
}

func genBlockMetas(
	ctx context.Context,
	blockInfos []catalog.BlockInfo,
	columnLength int,
	fs fileservice.FileService,
	m *mpool.MPool, prefetch bool) ([]BlockMeta, error) {
	{
		mp := make(map[types.Blockid]catalog.BlockInfo) // block list
		for i := range blockInfos {
			if blk, ok := mp[blockInfos[i].BlockID]; ok {
				if blk.CommitTs.Less(blockInfos[i].CommitTs) {
					mp[blk.BlockID] = blockInfos[i]
				}
			} else {
				mp[blockInfos[i].BlockID] = blockInfos[i]
			}
		}
		blockInfos = blockInfos[:0]
		for _, blk := range mp {
			blockInfos = append(blockInfos, blk)
		}
	}

	metas := make([]BlockMeta, len(blockInfos))

	idxs := make([]uint16, columnLength)
	for i := 0; i < columnLength; i++ {
		idxs[i] = uint16(i)
	}

	for i, blockInfo := range blockInfos {
		zm, rows, err := fetchZonemapAndRowsFromBlockInfo(ctx, idxs, blockInfo, fs, m)
		if err != nil {
			if prefetch {
				continue
			}
			return nil, err
		}
		metas[i] = BlockMeta{
			Rows:    int64(rows),
			Info:    blockInfos[i],
			Zonemap: zm,
		}
	}
	return metas, nil
}

func inBlockMap(blk BlockMeta, blockMap map[types.Blockid]bool) bool {
	_, ok := blockMap[blk.Info.BlockID]
	return ok
}

func genModifedBlocks(ctx context.Context, deletes map[types.Blockid][]int, orgs, modfs []BlockMeta,
	expr *plan.Expr, tableDef *plan.TableDef, proc *process.Process) []ModifyBlockMeta {
	blks := make([]ModifyBlockMeta, 0, len(orgs)-len(modfs))

	lenblks := len(modfs)
	blockMap := make(map[types.Blockid]bool, lenblks)
	for i := 0; i < lenblks; i++ {
		blockMap[modfs[i].Info.BlockID] = true
	}

	exprMono := plantool.CheckExprIsMonotonic(ctx, expr)
	columnMap, columns, maxCol := plantool.GetColumnsByExpr(expr, tableDef)
	for i, blk := range orgs {
		if !inBlockMap(blk, blockMap) {
			if !exprMono || needRead(ctx, expr, blk, tableDef, columnMap, columns, maxCol, proc) {
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
		vec := vector.NewVec(types.T_Rowid.ToType())
		for i := 0; i < bat.Length(); i++ {
			val := types.RandomRowid()
			if err := vector.AppendFixed(vec, val, false, m); err != nil {
				return nil, err
			}
		}
		vecs = append(vecs, vec)
		attrs = append(attrs, "rowid")
	}
	{
		var val types.TS

		vec := vector.NewVec(types.T_TS.ToType())
		for i := 0; i < bat.Length(); i++ {
			if err := vector.AppendFixed(vec, val, false, m); err != nil {
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

func inPartition(v types.Rowid, part *PartitionState,
	ts timestamp.Timestamp, blocks []BlockMeta) bool {
	if part.RowExists(v, types.TimestampToTS(ts)) {
		return true
	}
	if len(blocks) == 0 {
		return false
	}
	blkId := v.GetBlockid()
	for _, blk := range blocks {
		if blk.Info.BlockID == blkId {
			return true
		}
	}
	return false
}

func transferIval[T int32 | int64](v T, oid types.T) (bool, any) {
	switch oid {
	case types.T_int8:
		return true, int8(v)
	case types.T_int16:
		return true, int16(v)
	case types.T_int32:
		return true, int32(v)
	case types.T_int64:
		return true, int64(v)
	case types.T_uint8:
		return true, uint8(v)
	case types.T_uint16:
		return true, uint16(v)
	case types.T_uint32:
		return true, uint32(v)
	case types.T_uint64:
		return true, uint64(v)
	case types.T_float32:
		return true, float32(v)
	case types.T_float64:
		return true, float64(v)
	default:
		return false, nil
	}
}

func transferUval[T uint32 | uint64](v T, oid types.T) (bool, any) {
	switch oid {
	case types.T_int8:
		return true, int8(v)
	case types.T_int16:
		return true, int16(v)
	case types.T_int32:
		return true, int32(v)
	case types.T_int64:
		return true, int64(v)
	case types.T_uint8:
		return true, uint8(v)
	case types.T_uint16:
		return true, uint16(v)
	case types.T_uint32:
		return true, uint32(v)
	case types.T_uint64:
		return true, uint64(v)
	case types.T_float32:
		return true, float32(v)
	case types.T_float64:
		return true, float64(v)
	default:
		return false, nil
	}
}

func transferFval(v float32, oid types.T) (bool, any) {
	switch oid {
	case types.T_float32:
		return true, float32(v)
	case types.T_float64:
		return true, float64(v)
	default:
		return false, nil
	}
}

func transferDval(v float64, oid types.T) (bool, any) {
	switch oid {
	case types.T_float32:
		return true, float32(v)
	case types.T_float64:
		return true, float64(v)
	default:
		return false, nil
	}
}

func transferSval(v string, oid types.T) (bool, any) {
	switch oid {
	case types.T_json:
		return true, []byte(v)
	case types.T_char, types.T_varchar:
		return true, []byte(v)
	case types.T_text, types.T_blob:
		return true, []byte(v)
	case types.T_binary, types.T_varbinary:
		return true, []byte(v)
	case types.T_uuid:
		var uv types.Uuid
		copy(uv[:], []byte(v)[:])
		return true, uv
	default:
		return false, nil
	}
}

func transferBval(v bool, oid types.T) (bool, any) {
	switch oid {
	case types.T_bool:
		return true, v
	default:
		return false, nil
	}
}

func transferDateval(v int32, oid types.T) (bool, any) {
	switch oid {
	case types.T_date:
		return true, types.Date(v)
	default:
		return false, nil
	}
}

func transferTimeval(v int64, oid types.T) (bool, any) {
	switch oid {
	case types.T_time:
		return true, types.Time(v)
	default:
		return false, nil
	}
}

func transferDatetimeval(v int64, oid types.T) (bool, any) {
	switch oid {
	case types.T_datetime:
		return true, types.Datetime(v)
	default:
		return false, nil
	}
}

func transferTimestampval(v int64, oid types.T) (bool, any) {
	switch oid {
	case types.T_timestamp:
		return true, types.Timestamp(v)
	default:
		return false, nil
	}
}

func transferDecimal64val(v int64, oid types.T) (bool, any) {
	switch oid {
	case types.T_decimal64:
		return true, types.Decimal64(v)
	default:
		return false, nil
	}
}

func transferDecimal128val(a, b int64, oid types.T) (bool, any) {
	switch oid {
	case types.T_decimal128:
		return true, types.Decimal128{B0_63: uint64(a), B64_127: uint64(b)}
	default:
		return false, nil
	}
}
