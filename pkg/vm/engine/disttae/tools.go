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
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func genCreateDatabaseTuple(accountId, userId, roleId uint32, name string,
	m *mheap.Mheap) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(catalog.MoDatabaseSchema))
	bat.Attrs = append(bat.Attrs, catalog.MoDatabaseSchema...)
	{
		bat.Vecs[0] = vector.New(catalog.MoDatabaseTypes[0]) // dat_id
		if err := bat.Vecs[0].Append(uint64(0), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[1] = vector.New(catalog.MoDatabaseTypes[1]) // datname
		if err := bat.Vecs[1].Append([]byte(name), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[2] = vector.New(catalog.MoDatabaseTypes[2]) // dat_catalog_name
		if err := bat.Vecs[2].Append([]byte(catalog.MO_CATALOG), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[3] = vector.New(catalog.MoDatabaseTypes[3])             // dat_createsql
		if err := bat.Vecs[3].Append([]byte(""), false, m); err != nil { // TODO
			return nil, err
		}
		bat.Vecs[4] = vector.New(catalog.MoDatabaseTypes[4]) // owner
		if err := bat.Vecs[4].Append(roleId, false, m); err != nil {
			return nil, err
		}
		bat.Vecs[5] = vector.New(catalog.MoDatabaseTypes[5]) // creator
		if err := bat.Vecs[5].Append(userId, false, m); err != nil {
			return nil, err
		}
		bat.Vecs[6] = vector.New(catalog.MoDatabaseTypes[6]) // created_time
		if err := bat.Vecs[6].Append(types.Timestamp(time.Now().Unix()), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[7] = vector.New(catalog.MoDatabaseTypes[7]) // account_id
		if err := bat.Vecs[7].Append(accountId, false, m); err != nil {
			return nil, err
		}
	}
	return bat, nil
}

func genDropDatabaseTuple(id uint64, m *mheap.Mheap) (*batch.Batch, error) {
	bat := batch.NewWithSize(1)
	bat.Attrs = append(bat.Attrs, catalog.MoDatabaseSchema[:1]...)
	{
		bat.Vecs[0] = vector.New(catalog.MoDatabaseTypes[0]) // dat_id
		if err := bat.Vecs[0].Append(id, false, m); err != nil {
			return nil, err
		}
	}
	return bat, nil
}

func genCreateTableTuple(accountId, userId, roleId uint32, name string, databaseId uint64,
	databaseName string, comment string, m *mheap.Mheap) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(catalog.MoTablesSchema))
	bat.Attrs = append(bat.Attrs, catalog.MoTablesSchema...)
	{
		bat.Vecs[0] = vector.New(catalog.MoTablesTypes[0]) // rel_id
		if err := bat.Vecs[0].Append(uint64(0), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[1] = vector.New(catalog.MoTablesTypes[1]) // relname
		if err := bat.Vecs[1].Append([]byte(name), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[2] = vector.New(catalog.MoTablesTypes[2]) // reldatabase
		if err := bat.Vecs[2].Append([]byte(databaseName), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[3] = vector.New(catalog.MoTablesTypes[3]) // reldatabase_id
		if err := bat.Vecs[3].Append(databaseId, false, m); err != nil {
			return nil, err
		}
		bat.Vecs[4] = vector.New(catalog.MoTablesTypes[4]) // relpersistence
		if err := bat.Vecs[4].Append([]byte(""), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[5] = vector.New(catalog.MoTablesTypes[5]) // relkind
		if err := bat.Vecs[5].Append([]byte(""), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[6] = vector.New(catalog.MoTablesTypes[6]) // rel_comment
		if err := bat.Vecs[6].Append([]byte(comment), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[7] = vector.New(catalog.MoTablesTypes[7]) // rel_createsql
		if err := bat.Vecs[4].Append([]byte(""), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[8] = vector.New(catalog.MoTablesTypes[8]) // created_time
		if err := bat.Vecs[8].Append(types.Timestamp(time.Now().Unix()), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[9] = vector.New(catalog.MoDatabaseTypes[9]) // creator
		if err := bat.Vecs[9].Append(userId, false, m); err != nil {
			return nil, err
		}
		bat.Vecs[10] = vector.New(catalog.MoDatabaseTypes[10]) // owner
		if err := bat.Vecs[10].Append(roleId, false, m); err != nil {
			return nil, err
		}
		bat.Vecs[11] = vector.New(catalog.MoDatabaseTypes[11]) // account_id
		if err := bat.Vecs[11].Append(accountId, false, m); err != nil {
			return nil, err
		}
	}
	return bat, nil
}

func genCreateColumnTuple(col column, m *mheap.Mheap) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(catalog.MoColumnsSchema))
	bat.Attrs = append(bat.Attrs, catalog.MoColumnsSchema...)
	{
		bat.Vecs[0] = vector.New(catalog.MoColumnsTypes[0]) // att_uniq_name
		if err := bat.Vecs[0].Append([]byte(""), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[1] = vector.New(catalog.MoColumnsTypes[1]) // account_id
		if err := bat.Vecs[1].Append(uint32(0), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[2] = vector.New(catalog.MoColumnsTypes[2]) // att_database_id
		if err := bat.Vecs[2].Append(col.databaseId, false, m); err != nil {
			return nil, err
		}
		bat.Vecs[3] = vector.New(catalog.MoColumnsTypes[3]) // att_database
		if err := bat.Vecs[3].Append([]byte(col.databaseName), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[4] = vector.New(catalog.MoColumnsTypes[4]) // att_relname_id
		if err := bat.Vecs[4].Append(uint64(0), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[5] = vector.New(catalog.MoColumnsTypes[5]) // att_relname
		if err := bat.Vecs[5].Append([]byte(col.tableName), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[6] = vector.New(catalog.MoColumnsTypes[6]) // attname
		if err := bat.Vecs[6].Append([]byte(col.name), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[7] = vector.New(catalog.MoColumnsTypes[7]) // atttyp
		if err := bat.Vecs[7].Append(col.typ, false, m); err != nil {
			return nil, err
		}
		bat.Vecs[8] = vector.New(catalog.MoColumnsTypes[8]) // attnum
		if err := bat.Vecs[8].Append(col.num, false, m); err != nil {
			return nil, err
		}
		bat.Vecs[9] = vector.New(catalog.MoColumnsTypes[9]) // att_length
		if err := bat.Vecs[9].Append(col.typLen, false, m); err != nil {
			return nil, err
		}
		bat.Vecs[10] = vector.New(catalog.MoColumnsTypes[10]) // attnotnul
		if err := bat.Vecs[10].Append(col.notNull, false, m); err != nil {
			return nil, err
		}
		bat.Vecs[11] = vector.New(catalog.MoColumnsTypes[11]) // atthasdef
		if err := bat.Vecs[11].Append(col.hasDef, false, m); err != nil {
			return nil, err
		}
		bat.Vecs[12] = vector.New(catalog.MoColumnsTypes[12]) // att_default
		if err := bat.Vecs[12].Append(col.defaultExpr, false, m); err != nil {
			return nil, err
		}
		bat.Vecs[13] = vector.New(catalog.MoColumnsTypes[13]) // attisdropped
		if err := bat.Vecs[13].Append(int8(0), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[14] = vector.New(catalog.MoColumnsTypes[14]) // att_constraint_type
		if err := bat.Vecs[14].Append([]byte(col.constraintType), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[15] = vector.New(catalog.MoColumnsTypes[15]) // att_is_unsigned
		if err := bat.Vecs[15].Append(int8(0), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[16] = vector.New(catalog.MoColumnsTypes[16]) // att_is_auto_increment
		if err := bat.Vecs[16].Append(col.isAutoIncrement, false, m); err != nil {
			return nil, err
		}
		bat.Vecs[17] = vector.New(catalog.MoColumnsTypes[17]) // att_comment
		if err := bat.Vecs[17].Append([]byte(col.comment), false, m); err != nil {
			return nil, err
		}
		bat.Vecs[18] = vector.New(catalog.MoColumnsTypes[18]) // att_is_hidden
		if err := bat.Vecs[18].Append(col.isHidden, false, m); err != nil {
			return nil, err
		}
	}
	return bat, nil
}

func genDropTableTuple(id uint64, m *mheap.Mheap) (*batch.Batch, error) {
	bat := batch.NewWithSize(1)
	bat.Attrs = append(bat.Attrs, catalog.MoTablesSchema[:1]...)
	{
		bat.Vecs[0] = vector.New(catalog.MoTablesTypes[0]) // rel_id
		if err := bat.Vecs[0].Append(id, false, m); err != nil {
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
	return nil
}

// genDatabaseIdExpr generate an expression to find database list
// by accountId
func genDatabaseListExpr(accountId uint32) *plan.Expr {
	return nil
}

// genTableIdExpr generate an expression to find table info
// by database id and table name and accountId
func genTableIdExpr(accountId uint32, databaseId uint64, name string) *plan.Expr {
	return nil
}

// genTableListExpr generate an expression to find table list
// by database id and accountId
func genTableListExpr(accountId uint32, databaseId uint64) *plan.Expr {
	return nil
}

// genColumnInfoExpr generate an expression to find column info list
// by database id and table id and accountId
func genColumnInfoExpr(accountId uint32, databaseId, tableId uint64) *plan.Expr {
	return nil
}

func genRows(bat *batch.Batch) [][]any {
	rows := make([][]any, bat.VectorCount())
	for i := 0; i < bat.VectorCount(); i++ {
		vec := bat.GetVector(int32(i))
		rows[i] = make([]any, vec.Length())
		switch vec.GetType().Oid {
		case types.T_bool:
			col := vector.GetFixedVectorValues[bool](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[i][j] = col[j]
			}
		case types.T_int8:
			col := vector.GetFixedVectorValues[int8](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[i][j] = col[j]
			}
		case types.T_int16:
			col := vector.GetFixedVectorValues[int16](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[i][j] = col[j]
			}
		case types.T_int32:
			col := vector.GetFixedVectorValues[int32](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[i][j] = col[j]
			}
		case types.T_int64:
			col := vector.GetFixedVectorValues[int64](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[i][j] = col[j]
			}
		case types.T_uint8:
			col := vector.GetFixedVectorValues[uint8](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[i][j] = col[j]
			}
		case types.T_uint16:
			col := vector.GetFixedVectorValues[uint16](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[i][j] = col[j]
			}
		case types.T_uint32:
			col := vector.GetFixedVectorValues[uint32](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[i][j] = col[j]
			}
		case types.T_uint64:
			col := vector.GetFixedVectorValues[uint64](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[i][j] = col[j]
			}
		case types.T_float32:
			col := vector.GetFixedVectorValues[float32](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[i][j] = col[j]
			}
		case types.T_float64:
			col := vector.GetFixedVectorValues[float64](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[i][j] = col[j]
			}
		case types.T_date:
			col := vector.GetFixedVectorValues[types.Date](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[i][j] = col[j]
			}
		case types.T_datetime:
			col := vector.GetFixedVectorValues[types.Datetime](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[i][j] = col[j]
			}
		case types.T_timestamp:
			col := vector.GetFixedVectorValues[types.Timestamp](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[i][j] = col[j]
			}
		case types.T_decimal64:
			col := vector.GetFixedVectorValues[types.Decimal64](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[i][j] = col[j]
			}
		case types.T_decimal128:
			col := vector.GetFixedVectorValues[types.Decimal128](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[i][j] = col[j]
			}
		case types.T_uuid:
			col := vector.GetFixedVectorValues[types.Uuid](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[i][j] = col[j]
			}
		case types.T_TS:
			col := vector.GetFixedVectorValues[types.TS](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[i][j] = col[j]
			}
		case types.T_Rowid:
			col := vector.GetFixedVectorValues[types.Rowid](vec)
			for j := 0; j < vec.Length(); j++ {
				rows[i][j] = col[j]
			}
		case types.T_char, types.T_varchar, types.T_blob, types.T_json:
			col := vector.GetBytesVectorValues(vec)
			for j := 0; j < vec.Length(); j++ {
				rows[i][j] = col[j]
			}
		}
	}
	return rows
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
					OpCode:  uint32(api.OpCode_OpGetLogTail),
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
		cols[i].name = string(row[6].([]byte))
		cols[i].comment = string(row[17].([]byte))
		cols[i].isHidden = row[18].(int8)
		cols[i].isAutoIncrement = row[16].(int8)
		cols[i].constraintType = string(row[14].([]byte))
		cols[i].typ = row[7].([]byte)
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
	if col.constraintType == "p" {
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
			col.constraintType = "p"
		} else {
			col.constraintType = "n"
		}
		cols = append(cols, col)
		num++
	}
	return cols, nil
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
