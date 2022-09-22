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

package txnimpl

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type txnImpl struct {
	*txnbase.Txn
	catalog *catalog.Catalog
}

var TxnFactory = func(catalog *catalog.Catalog) txnbase.TxnFactory {
	return func(mgr *txnbase.TxnManager, store txnif.TxnStore, txnId uint64,
		start types.TS, info []byte) txnif.AsyncTxn {
		return newTxnImpl(catalog, mgr, store, txnId, start, info)
	}
}

func newTxnImpl(catalog *catalog.Catalog, mgr *txnbase.TxnManager, store txnif.TxnStore,
	txnId uint64, start types.TS, info []byte) *txnImpl {
	impl := &txnImpl{
		Txn:     txnbase.NewTxn(mgr, store, txnId, start, info),
		catalog: catalog,
	}
	return impl
}

func (txn *txnImpl) CreateDatabase(name string) (db handle.Database, err error) {
	return txn.Store.CreateDatabase(name)
}

func (txn *txnImpl) CreateDatabaseByDef(def any) (db handle.Database, err error) {
	//return txn.Store.CreateDatabase(name)
	panic(moerr.NewNYI("CreateDatabaseByDef is not implemented "))
}

func (txn *txnImpl) DropDatabase(name string) (db handle.Database, err error) {
	return txn.Store.DropDatabase(name)
}

func (txn *txnImpl) DropDatabaseByID(id uint64) (db handle.Database, err error) {
	panic(moerr.NewNYI("DropDatabaseByID is not implemented yet"))
}

func (txn *txnImpl) GetDatabase(name string) (db handle.Database, err error) {
	return txn.Store.GetDatabase(name)
}

func (txn *txnImpl) DatabaseNames() (names []string) {
	return txn.Store.DatabaseNames()
}

func (txn *txnImpl) LogTxnEntry(dbId, tableId uint64, entry txnif.TxnEntry, readed []*common.ID) (err error) {
	return txn.Store.LogTxnEntry(dbId, tableId, entry, readed)
}

// DBTuple tuple for MO system table of databases
type DBTuple struct {
	DbId          uint64
	DbName        string
	DbCatalogName string
	CreateSql     string
	RoleId        uint32
	UserId        uint32
	CreateTime    types.Timestamp
	AccountId     uint32
}

// TBTuple tuple for MO system table of tables
type TBTuple struct {
	RelId          uint64
	RelName        string
	DbName         string
	DbId           uint64
	RelPersistence string
	RelKind        string
	Comment        string
	CreateSql      string
	CreateTime     types.Timestamp
	UserId         uint32
	RoleId         uint32
	AccountId      uint32
}

// ColumnTuple tuple for MO system table of columns
type ColumnTuple struct {
	AttUniqName        string
	AccountId          uint32
	AttDbId            uint64
	AttDbName          string
	AttRelId           uint64
	AttRelName         string
	AttName            string
	AttType            []byte
	AttNum             int32
	AttLen             int32
	AttNotNul          int8
	AttHashDef         int8
	AttDefault         []byte
	AttIsDropped       int8
	AttConstraintType  string
	AttIsUnsigned      int8
	AttIsAutoIncrement int8
	AttComment         string
	AttIsHidden        int8
}

func genRowFrom(bat *batch.Batch) []any {
	var row []any
	for i := 0; i < bat.VectorCount(); i++ {
		vec := bat.GetVector(int32(i))
		switch vec.GetType().Oid {
		case types.T_bool:
			col := vector.GetFixedVectorValues[bool](vec)
			row = append(row, col[0])
		case types.T_int8:
			col := vector.GetFixedVectorValues[int8](vec)
			row = append(row, col[0])
		case types.T_int16:
			col := vector.GetFixedVectorValues[int16](vec)
			row = append(row, col[0])
		case types.T_int32:
			col := vector.GetFixedVectorValues[int32](vec)
			row = append(row, col[0])
		case types.T_int64:
			col := vector.GetFixedVectorValues[int64](vec)
			row = append(row, col[0])
		case types.T_uint8:
			col := vector.GetFixedVectorValues[uint8](vec)
			row = append(row, col[0])
		case types.T_uint16:
			col := vector.GetFixedVectorValues[uint16](vec)
			row = append(row, col[0])
		case types.T_uint32:
			col := vector.GetFixedVectorValues[uint32](vec)
			row = append(row, col[0])
		case types.T_uint64:
			col := vector.GetFixedVectorValues[uint64](vec)
			row = append(row, col[0])
		case types.T_float32:
			col := vector.GetFixedVectorValues[float32](vec)
			row = append(row, col[0])
		case types.T_float64:
			col := vector.GetFixedVectorValues[float64](vec)
			row = append(row, col[0])
		case types.T_date:
			col := vector.GetFixedVectorValues[types.Date](vec)
			row = append(row, col[0])
		case types.T_datetime:
			col := vector.GetFixedVectorValues[types.Datetime](vec)
			row = append(row, col[0])
		case types.T_timestamp:
			col := vector.GetFixedVectorValues[types.Timestamp](vec)
			row = append(row, col[0])
		case types.T_decimal64:
			col := vector.GetFixedVectorValues[types.Decimal64](vec)
			row = append(row, col[0])
		case types.T_decimal128:
			col := vector.GetFixedVectorValues[types.Decimal128](vec)
			row = append(row, col[0])
		case types.T_uuid:
			col := vector.GetFixedVectorValues[types.Uuid](vec)
			row = append(row, col[0])
		case types.T_TS:
			col := vector.GetFixedVectorValues[types.TS](vec)
			row = append(row, col[0])
		case types.T_Rowid:
			col := vector.GetFixedVectorValues[types.Rowid](vec)
			row = append(row, col[0])
		case types.T_char, types.T_varchar, types.T_blob, types.T_json:
			col := vector.GetBytesVectorValues(vec)
			row = append(row, col[0])
		}
	}
	return row
}

func DBTupleToSchema(db *DBTuple) *catalog.Schema {
	panic(moerr.NewNYI("DBTupleToSchema is not implemented yet"))

}

func BuildDBTupleFrom(bat *batch.Batch) *DBTuple {
	row := genRowFrom(bat)
	dbTuple := &DBTuple{}
	dbTuple.DbId = row[0].(uint64)
	dbTuple.DbName = string(row[1].([]byte))
	dbTuple.DbCatalogName = string(row[2].([]byte))
	dbTuple.CreateSql = string(row[3].([]byte))
	dbTuple.RoleId = row[4].(uint32)
	dbTuple.UserId = row[5].(uint32)
	dbTuple.CreateTime = row[6].(types.Timestamp)
	dbTuple.AccountId = row[7].(uint32)
	return dbTuple
}

func TBTupleToSchema(t *TBTuple) *catalog.Schema {
	panic(moerr.NewNYI("TBTupleToSchema is not implemented yet"))

}

func BuildTBTupleFrom(bat *batch.Batch) *TBTuple {
	row := genRowFrom(bat)
	tbTuple := &TBTuple{}
	tbTuple.RelId = row[0].(uint64)
	tbTuple.RelName = string(row[1].([]byte))
	tbTuple.DbName = string(row[2].([]byte))
	tbTuple.DbId = row[3].(uint64)
	tbTuple.RelPersistence = string(row[4].([]byte))
	tbTuple.RelKind = string(row[5].([]byte))
	tbTuple.Comment = string(row[6].([]byte))
	tbTuple.CreateSql = string(row[7].([]byte))
	tbTuple.CreateTime = row[8].(types.Timestamp)
	tbTuple.UserId = row[9].(uint32)
	tbTuple.RoleId = row[10].(uint32)
	tbTuple.AccountId = row[11].(uint32)
	return tbTuple
}

func BuildColumnTupleFrom(bat *batch.Batch) *ColumnTuple {
	row := genRowFrom(bat)
	colTuple := &ColumnTuple{}
	colTuple.AttUniqName = string(row[0].([]byte))
	colTuple.AccountId = row[1].(uint32)
	colTuple.AttDbId = row[2].(uint64)
	colTuple.AttDbName = string(row[3].([]byte))
	colTuple.AttRelId = row[4].(uint64)
	colTuple.AttRelName = string(row[5].([]byte))
	colTuple.AttName = string(row[6].([]byte))
	colTuple.AttType = row[7].([]byte)
	colTuple.AttNum = row[8].(int32)
	colTuple.AttLen = row[9].(int32)
	colTuple.AttNotNul = row[10].(int8)
	colTuple.AttHashDef = row[11].(int8)
	colTuple.AttDefault = row[12].([]byte)
	colTuple.AttIsDropped = row[13].(int8)
	colTuple.AttConstraintType = string(row[14].([]byte))
	colTuple.AttIsUnsigned = row[15].(int8)
	colTuple.AttIsAutoIncrement = row[16].(int8)
	colTuple.AttComment = string(row[17].([]byte))
	colTuple.AttIsHidden = row[18].(int8)
	return colTuple
}

func (txn *txnImpl) HandleCmd(cmd *api.Entry) (err error) {
	db, err := txn.GetDatabase(cmd.DatabaseName)
	if err != nil {
		return err
	}
	tb, err := db.GetRelationByName(cmd.TableName)
	if err != nil {
		return err
	}
	//Handle DDL
	if cmd.DatabaseId == catalog.SystemDBID {
		switch cmd.TableId {
		case catalog.SystemTable_DB_ID:
			if cmd.EntryType == api.Entry_Insert {
				dbTuple := BuildDBTupleFrom(batch.ProtoToMOBatch(cmd.Bat))
				//FIXME::need to transfer dbTuple to catalog.Schema?
				_, err = txn.CreateDatabaseByDef(DBTupleToSchema(dbTuple))
			} else {
				vec := batch.ProtoToMOBatch(cmd.Bat).GetVector(0)
				dbId := vector.GetFixedVectorValues[uint64](vec)[0]
				_, err = txn.DropDatabaseByID(dbId)
			}
		case catalog.SystemTable_Table_ID:
			if cmd.EntryType == api.Entry_Insert {
				tbTuple := BuildTBTupleFrom(batch.ProtoToMOBatch(cmd.Bat))
				//FIXME::transfer tbTuple to catalog.Schema?
				_, err = db.CreateRelation(TBTupleToSchema(tbTuple))
			} else {
				vec := batch.ProtoToMOBatch(cmd.Bat).GetVector(0)
				tbId := vector.GetFixedVectorValues[uint64](vec)[0]
				_, err = db.DropRelationByID(tbId)
			}
		case catalog.SystemBlock_Columns_ID:
			panic(moerr.NewNYI("Create/Delete a column is not implemented yet"))
		}
		return err
	}
	//Handle DML
	if cmd.EntryType == api.Entry_Insert {
		//Append a block had been bulk-loaded into S3
		if cmd.FileName != "" {
			//TODO::Precommit a block from S3
			//tb.PrecommitAppendBlock()
			panic(moerr.NewNYI("Precommit a block is not implemented yet"))
		}
		//Add a batch into table
		taeBat := containers.MOToTAEBatch(batch.ProtoToMOBatch(cmd.Bat),
			tb.GetMeta().(*catalog.TableEntry).GetSchema().AllNullables())
		//TODO::use PrecommitAppend, instead of Append.
		err = tb.Append(taeBat)
		return
	}
	//TODO:: handle delete rows of block had been bulk-loaded into S3.

	//Delete a batch
	moV, err := vector.ProtoVectorToVector(cmd.Bat.Vecs[0])
	if err != nil {
		panic(err)
	}
	taeV := containers.MOToTAEVector(moV, false)
	err = tb.DeleteByPhyAddrKeys(taeV)
	return
}
