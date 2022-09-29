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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plantool "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func genCreateDatabaseTuple(sql string, accountId, userId, roleId uint32, name string,
	m *mheap.Mheap) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(catalog.MoDatabaseSchema))
	bat.Attrs = append(bat.Attrs, catalog.MoDatabaseSchema...)
	{
		idx := catalog.MO_DATABASE_DAT_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoDatabaseTypes[idx]) // dat_id
		if err := bat.Vecs[idx].Append(uint64(0), false, m); err != nil {
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

func genDropDatabaseTuple(id uint64, name string, m *mheap.Mheap) (*batch.Batch, error) {
	bat := batch.NewWithSize(2)
	bat.Attrs = append(bat.Attrs, catalog.MoDatabaseSchema[:2]...)
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

func genCreateTableTuple(sql string, accountId, userId, roleId uint32, name string,
	databaseId uint64, databaseName string, comment string, m *mheap.Mheap) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(catalog.MoTablesSchema))
	bat.Attrs = append(bat.Attrs, catalog.MoTablesSchema...)
	{
		idx := catalog.MO_TABLES_REL_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // rel_id
		if err := bat.Vecs[idx].Append(uint64(0), false, m); err != nil {
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
		if err := bat.Vecs[idx].Append([]byte(""), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_REL_COMMENT_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // rel_comment
		if err := bat.Vecs[idx].Append([]byte(comment), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_TABLES_REL_CREATESQL_IDX
		bat.Vecs[idx] = vector.New(catalog.MoTablesTypes[idx]) // rel_createsql
		if err := bat.Vecs[idx].Append([]byte(sql), false, m); err != nil {
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
		if err := bat.Vecs[idx].Append([]byte(""), false, m); err != nil {
			return nil, err
		}
	}
	return bat, nil
}

func genCreateColumnTuple(col column, m *mheap.Mheap) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(catalog.MoColumnsSchema))
	bat.Attrs = append(bat.Attrs, catalog.MoColumnsSchema...)
	{
		idx := catalog.MO_COLUMNS_ATT_UNIQ_NAME_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_uniq_name
		if err := bat.Vecs[idx].Append([]byte(""), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ACCOUNT_ID_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // account_id
		if err := bat.Vecs[idx].Append(uint32(0), false, m); err != nil {
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
		if err := bat.Vecs[idx].Append(uint64(0), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATTTYP_IDX
		bat.Vecs[idx] = vector.New(catalog.MoColumnsTypes[idx]) // att_relname
		if err := bat.Vecs[idx].Append([]byte(col.tableName), false, m); err != nil {
			return nil, err
		}
		idx = catalog.MO_COLUMNS_ATTNUM_IDX
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
	}
	return bat, nil
}

func genDropTableTuple(id, databaseId uint64, name, databaseName string,
	m *mheap.Mheap) (*batch.Batch, error) {
	bat := batch.NewWithSize(4)
	bat.Attrs = append(bat.Attrs, catalog.MoTablesSchema[:4]...)
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
		left = plantool.MakeExpr("eq", args)
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(MO_DATABASE_ID_ACCOUNT_IDX, types.T_uint32,
			catalog.MoDatabaseSchema[catalog.MO_DATABASE_ACCOUNT_ID_IDX]))
		args = append(args, newIntConstVal(types.T_uint32, accountId))
		right = plantool.MakeExpr("eq", args)
	}
	return plantool.MakeExpr("and", []*plan.Expr{left, right})
}

// genDatabaseIdExpr generate an expression to find database list
// by accountId
func genDatabaseListExpr(accountId uint32) *plan.Expr {
	var args []*plan.Expr

	args = append(args, newColumnExpr(MO_DATABASE_LIST_ACCOUNT_IDX, types.T_uint32,
		catalog.MoDatabaseSchema[catalog.MO_DATABASE_ACCOUNT_ID_IDX]))
	args = append(args, newIntConstVal(types.T_uint32, accountId))
	return plantool.MakeExpr("eq", args)
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
		left = plantool.MakeExpr("eq", args)
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(catalog.MO_TABLES_RELDATABASE_ID_IDX, types.T_uint64,
			catalog.MoTablesSchema[catalog.MO_TABLES_RELDATABASE_ID_IDX]))
		args = append(args, newIntConstVal(types.T_uint64, databaseId))
		right = plantool.MakeExpr("eq", args)
		left = plantool.MakeExpr("and", []*plan.Expr{left, right})
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(catalog.MO_TABLES_ACCOUNT_ID_IDX, types.T_uint32,
			catalog.MoTablesSchema[catalog.MO_TABLES_ACCOUNT_ID_IDX]))
		args = append(args, newIntConstVal(types.T_uint32, accountId))
		right = plantool.MakeExpr("eq", args)
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
		left = plantool.MakeExpr("eq", args)
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(MO_TABLE_ID_DATABASE_ID_IDX, types.T_uint64,
			catalog.MoTablesSchema[catalog.MO_TABLES_RELDATABASE_ID_IDX]))
		args = append(args, newIntConstVal(types.T_uint64, databaseId))
		right = plantool.MakeExpr("eq", args)
		left = plantool.MakeExpr("and", []*plan.Expr{left, right})
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(MO_TABLE_ID_ACCOUNT_IDX, types.T_uint32,
			catalog.MoTablesSchema[catalog.MO_TABLES_ACCOUNT_ID_IDX]))
		args = append(args, newIntConstVal(types.T_uint32, accountId))
		right = plantool.MakeExpr("eq", args)
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
		args = append(args, newIntConstVal(types.T_uint64, databaseId))
		left = plantool.MakeExpr("eq", args)
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(MO_TABLE_LIST_ACCOUNT_IDX, types.T_uint32,
			catalog.MoTablesSchema[catalog.MO_TABLES_ACCOUNT_ID_IDX]))
		args = append(args, newIntConstVal(types.T_uint32, accountId))
		right = plantool.MakeExpr("eq", args)
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
		args = append(args, newIntConstVal(types.T_uint64, databaseId))
		left = plantool.MakeExpr("eq", args)
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(catalog.MO_COLUMNS_ATT_RELNAME_ID_IDX, types.T_uint64,
			catalog.MoTablesSchema[catalog.MO_COLUMNS_ATT_RELNAME_ID_IDX]))
		args = append(args, newIntConstVal(types.T_uint64, tableId))
		right = plantool.MakeExpr("eq", args)
		left = plantool.MakeExpr("and", []*plan.Expr{left, right})
	}
	{
		var args []*plan.Expr

		args = append(args, newColumnExpr(catalog.MO_COLUMNS_ACCOUNT_ID_IDX, types.T_uint32,
			catalog.MoTablesSchema[catalog.MO_COLUMNS_ACCOUNT_ID_IDX]))
		args = append(args, newIntConstVal(types.T_uint32, accountId))
		right = plantool.MakeExpr("eq", args)
	}
	return plantool.MakeExpr("and", []*plan.Expr{left, right})
}

// genInsertExpr used to generate an expression to partition table data
func genInsertExpr(defs []engine.TableDef, dnNum int) *plan.Expr {
	var args []*plan.Expr

	for i, def := range defs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			if attr.Attr.Primary {
				args = append(args, newColumnExpr(i, attr.Attr.Type.Oid, attr.Attr.Name))
			}
		}
	}
	return plantool.MakeExpr("%", []*plan.Expr{
		plantool.MakeExpr("hash_value", args),
		newIntConstVal(types.T_int64, int64(dnNum)),
	})
}

// genDeleteExpr used to generate an expression to partition table data
func genDeleteExpr(defs []engine.TableDef, dnNum int) *plan.Expr {
	var args []*plan.Expr

	cnt := 1
	for _, def := range defs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			if attr.Attr.Primary {
				args = append(args, newColumnExpr(cnt, attr.Attr.Type.Oid, attr.Attr.Name))
				cnt++
			}
		}
	}
	return plantool.MakeExpr("%", []*plan.Expr{
		plantool.MakeExpr("hash_value", args),
		newIntConstVal(types.T_int64, int64(dnNum)),
	})
}

func newIntConstVal(oid types.T, v any) *plan.Expr {
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
		Typ: types.NewProtoType(types.T_uint32),
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
		Typ: types.NewProtoType(types.T_uint32),
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
	bat, err := toPBBatch(e.bat)
	if err != nil {
		return nil, err
	}
	typ := api.Entry_Insert
	if e.typ == DELETE {
		typ = api.Entry_Delete
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
	}
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
	if col.constraintType == catalog.SystemColPKConstraint {
		attr.Primary = true
	}
	return &engine.AttributeDef{Attr: attr}
}

func genColumns(tableName, databaseName string, databaseId uint64,
	defs []engine.TableDef) ([]column, error) {
	num := 0
	cols := make([]column, 0, len(defs))
	for _, def := range defs {
		attrDef, ok := def.(*engine.AttributeDef)
		if !ok {
			continue
		}
		typ, err := types.Encode(attrDef.Attr.Type)
		if err != nil {
			return nil, err
		}
		col := column{
			typ:          typ,
			typLen:       int32(len(typ)),
			databaseId:   databaseId,
			name:         attrDef.Attr.Name,
			tableName:    tableName,
			databaseName: databaseName,
			num:          int32(num),
			comment:      attrDef.Attr.Comment,
		}
		if attrDef.Attr.Default != nil {
			col.hasDef = 1
			defaultExpr, err := attrDef.Attr.Default.Marshal()
			if err != nil {
				return nil, err
			}
			col.defaultExpr = defaultExpr
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

func partitionBatch(bat *batch.Batch, expr *plan.Expr, m *mheap.Mheap, dnNum int) ([]*batch.Batch, error) {
	proc := process.New(context.TODO(), m, nil, nil, nil)
	pvec, err := colexec.EvalExpr(bat, proc, expr)
	if err != nil {
		return nil, err
	}
	defer pvec.Free(m)
	bats := make([]*batch.Batch, dnNum)
	for i := range bats {
		bats[i] = batch.New(true, bat.Attrs)
		for j := range bats[i].Vecs {
			bat.SetVector(int32(j), vector.New(bat.Vecs[j].Typ))
		}
	}
	vs := vector.GetFixedVectorValues[int64](pvec)
	for i := range bat.Vecs {
		vec := bat.GetVector(int32(i))
		for j, v := range vs {
			if err := vector.UnionOne(bats[v].GetVector(int32(i)), vec, int64(j), m); err != nil {
				for _, bat := range bats {
					bat.Clean(m)
				}
				return nil, err
			}
		}
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

func genBlockMetas(rows [][]any) []BlockMeta {
	return []BlockMeta{}
}

func inBlockList(blk BlockMeta, blks []BlockMeta) bool {
	/* TODO
	for i := range blks {
		if blk.Eq(blks[i]) {
			return true
		}
	}
	*/
	return false
}

func genModifedBlocks(orgs, modfs []BlockMeta, expr *plan.Expr) []BlockMeta {
	blks := make([]BlockMeta, 0, len(orgs)-len(modfs))
	for i, blk := range orgs {
		if !inBlockList(blk, modfs) {
			if needRead(expr, blk) {
				blks = append(blks, orgs[i])
			}
		}
	}
	return blks
}
