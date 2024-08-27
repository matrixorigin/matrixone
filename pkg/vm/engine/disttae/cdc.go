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
	"bytes"
	"fmt"
	"os"

	"github.com/tidwall/btree"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
)

var _ DDLListener = new(DDLAnalyzer)

type DDLTableItem struct {
	CPKey      []byte
	AccountId  uint64
	DB         string
	Table      string
	DBId       uint64
	TableId    uint64
	OldTableId uint64
	Deleted    bool
	Ts         timestamp.Timestamp
}

func DDLTableItemLess(a, b *DDLTableItem) bool {
	return bytes.Compare(a.CPKey[:], b.CPKey[:]) < 0
}

type DDLDBItem struct {
	CPKey     []byte
	AccountId uint64
	DB        string
	DBId      uint64
	Deleted   bool
	Ts        timestamp.Timestamp
}

func DDLDBItemLess(a, b *DDLDBItem) bool {
	return bytes.Compare(a.CPKey[:], b.CPKey[:]) < 0
}

type DDLAnalyzer struct {
	ts     timestamp.Timestamp
	tables *btree.BTreeG[*DDLTableItem]
	dbs    *btree.BTreeG[*DDLDBItem]
	cache  *cache.CatalogCache
}

func NewDDLAnalyzer(
	catCache *cache.CatalogCache,
) *DDLAnalyzer {
	return &DDLAnalyzer{
		tables: btree.NewBTreeG(DDLTableItemLess),
		dbs:    btree.NewBTreeG(DDLDBItemLess),
		cache:  catCache,
	}
}

func analyzeTablesBatch(bat *batch.Batch, curTs timestamp.Timestamp, f func(*DDLTableItem)) {
	//rowids := vector.MustFixedCol[types.Rowid](bat.GetVector(cache.MO_ROWID_IDX))
	timestamps := vector.MustFixedCol[types.TS](bat.GetVector(cache.MO_TIMESTAMP_IDX))
	accounts := vector.MustFixedCol[uint32](bat.GetVector(catalog.MO_TABLES_ACCOUNT_ID_IDX + cache.MO_OFF))
	names := bat.GetVector(catalog.MO_TABLES_REL_NAME_IDX + cache.MO_OFF)
	ids := vector.MustFixedCol[uint64](bat.GetVector(catalog.MO_TABLES_REL_ID_IDX + cache.MO_OFF))
	databaseIds := vector.MustFixedCol[uint64](bat.GetVector(catalog.MO_TABLES_RELDATABASE_ID_IDX + cache.MO_OFF))
	databaseNames := bat.GetVector(catalog.MO_TABLES_RELDATABASE_IDX + cache.MO_OFF)
	kinds := bat.GetVector(catalog.MO_TABLES_RELKIND_IDX + cache.MO_OFF)
	//comments := bat.GetVector(catalog.MO_TABLES_REL_COMMENT_IDX + cache.MO_OFF)
	//createSqls := bat.GetVector(catalog.MO_TABLES_REL_CREATESQL_IDX + cache.MO_OFF)
	//viewDefs := bat.GetVector(catalog.MO_TABLES_VIEWDEF_IDX + cache.MO_OFF)
	//partitioneds := vector.MustFixedCol[int8](bat.GetVector(catalog.MO_TABLES_PARTITIONED_IDX + cache.MO_OFF))
	//paritions := bat.GetVector(catalog.MO_TABLES_PARTITION_INFO_IDX + cache.MO_OFF)
	//constraints := bat.GetVector(catalog.MO_TABLES_CONSTRAINT_IDX + cache.MO_OFF)
	//versions := vector.MustFixedCol[uint32](bat.GetVector(catalog.MO_TABLES_VERSION_IDX + cache.MO_OFF))
	//catalogVersions := vector.MustFixedCol[uint32](bat.GetVector(catalog.MO_TABLES_CATALOG_VERSION_IDX + cache.MO_OFF))
	pks := bat.GetVector(catalog.MO_TABLES_CPKEY_IDX + cache.MO_OFF)
	for i, account := range accounts {
		//skip item that does not happen at the same time
		if !curTs.Equal(timestamps[i].ToTimestamp()) {
			continue
		}
		item := new(DDLTableItem)
		item.TableId = ids[i]
		item.Table = names.GetStringAt(i)
		item.AccountId = uint64(account)
		item.DBId = databaseIds[i]
		item.DB = databaseNames.GetStringAt(i)
		item.Ts = timestamps[i].ToTimestamp()
		//item.Kind = kinds.GetStringAt(i)
		kind := kinds.GetStringAt(i)
		//skip non general table
		if needSkipThisTable(kind, item.DB, item.Table) {
			continue
		}
		//item.ViewDef = viewDefs.GetStringAt(i)
		//item.Constraint = append(item.Constraint, constraints.GetBytesAt(i)...)
		//item.Comment = comments.GetStringAt(i)
		//item.Partitioned = partitioneds[i]
		//item.Partition = paritions.GetStringAt(i)
		//item.CreateSql = createSqls.GetStringAt(i)
		//item.Version = versions[i]
		//item.CatalogVersion = catalogVersions[i]
		//item.PrimaryIdx = -1
		//item.PrimarySeqnum = -1
		//item.ClusterByIdx = -1
		//copy(item.Rowid[:], rowids[i][:])
		item.CPKey = append(item.CPKey, pks.GetBytesAt(i)...)

		f(item)
	}
}

func (analyzer *DDLAnalyzer) InsertTable(bat *batch.Batch) {
	analyzeTablesBatch(bat, analyzer.ts, func(item *DDLTableItem) {
		if prevItem, ok := analyzer.tables.Get(item); ok {
			//prev item must be labeled deleted
			if !prevItem.Deleted {
				fmt.Fprintln(os.Stderr, "analyzer [insert] ddl table item twice")
				return
			}
			prevItem.OldTableId = prevItem.TableId
			prevItem.TableId = item.TableId
		} else {
			analyzer.tables.Set(item)
		}
	})
}

func (analyzer *DDLAnalyzer) DeleteTable(bat *batch.Batch) {
	//cpks := bat.GetVector(cache.MO_OFF + 0)
	//timestamps := vector.MustFixedCol[types.TS](bat.GetVector(cache.MO_TIMESTAMP_IDX))
	//for i, ts := range timestamps {
	//	if !ts.ToTimestamp().Equal(analyzer.ts) {
	//		continue
	//	}
	//	pk := cpks.GetBytesAt(i)
	//	//the table item saved in catalogCache
	//	if item, ok := analyzer.cache.GetTableByCPKey(pk); ok {
	//		// Note: the newItem.Id is the latest id under the name of the table,
	//		// not the id that should be seen at the moment ts.
	//		// Lucy thing is that the wrong tableid hold by this delete item will never be used.
	//		newItem := &DDLTableItem{
	//			Deleted:    true,
	//			TableId:    item.Id,
	//			OldTableId: 0,
	//			Table:      item.Name,
	//			CPKey:      append([]byte{}, item.CPKey...),
	//			AccountId:  uint64(item.AccountId),
	//			DBId:       item.DatabaseId,
	//			DB:         item.DatabaseName,
	//			Ts:         ts.ToTimestamp(),
	//		}
	//		analyzer.tables.Set(newItem)
	//	}
	//}
}

func analyzeDatabaseBatch(bat *batch.Batch, curTs timestamp.Timestamp, f func(*DDLDBItem)) {
	//rowids := vector.MustFixedCol[types.Rowid](bat.GetVector(cache.MO_ROWID_IDX))
	timestamps := vector.MustFixedCol[types.TS](bat.GetVector(cache.MO_TIMESTAMP_IDX))
	accounts := vector.MustFixedCol[uint32](bat.GetVector(catalog.MO_DATABASE_ACCOUNT_ID_IDX + cache.MO_OFF))
	names := bat.GetVector(catalog.MO_DATABASE_DAT_NAME_IDX + cache.MO_OFF)
	ids := vector.MustFixedCol[uint64](bat.GetVector(catalog.MO_DATABASE_DAT_ID_IDX + cache.MO_OFF))
	typs := bat.GetVector(catalog.MO_DATABASE_DAT_TYPE_IDX + cache.MO_OFF)
	//createSqls := bat.GetVector(catalog.MO_DATABASE_CREATESQL_IDX + cache.MO_OFF)
	pks := bat.GetVector(catalog.MO_DATABASE_CPKEY_IDX + cache.MO_OFF)
	for i, account := range accounts {
		if !timestamps[i].ToTimestamp().Equal(curTs) {
			continue
		}
		item := new(DDLDBItem)
		item.DBId = ids[i]
		item.DB = names.GetStringAt(i)
		typ := typs.GetStringAt(i)
		if needSkipThisDatabase(typ, item.DB) {
			continue
		}
		item.AccountId = uint64(account)
		item.Ts = timestamps[i].ToTimestamp()

		//item.CreateSql = createSqls.GetStringAt(i)
		//copy(item.Rowid[:], rowids[i][:])
		item.CPKey = append(item.CPKey, pks.GetBytesAt(i)...)
		f(item)
	}
}

func (analyzer *DDLAnalyzer) InsertDatabase(bat *batch.Batch) {
	analyzeDatabaseBatch(bat, analyzer.ts, func(item *DDLDBItem) {
		analyzer.dbs.Set(item)
	})
}

func (analyzer *DDLAnalyzer) DeleteDatabase(bat *batch.Batch) {
	//cpks := bat.GetVector(cache.MO_OFF + 0)
	//timestamps := vector.MustFixedCol[types.TS](bat.GetVector(cache.MO_TIMESTAMP_IDX))
	//for i, ts := range timestamps {
	//	pk := cpks.GetBytesAt(i)
	//	if item, ok := analyzer.cache.GetDatabaseByCPKey(pk); ok {
	//		newItem := &DDLDBItem{
	//			Deleted:   true,
	//			DBId:      item.Id,
	//			DB:        item.Name,
	//			CPKey:     append([]byte{}, item.CPKey...),
	//			AccountId: uint64(item.AccountId),
	//			Ts:        ts.ToTimestamp(),
	//		}
	//		analyzer.dbs.Set(newItem)
	//		//fmt.Fprintln(os.Stderr, "[", cc.cdcId, "]", "catalog cache delete database",
	//		//	item.AccountId, item.Name, item.Id, item.Ts)
	//	}
	//}
}

func (analyzer *DDLAnalyzer) OnAction(typ ActionType, bat *batch.Batch) {
	switch typ {
	case ActionInsertTable:
		analyzer.InsertTable(bat)
	case ActionDeleteTable:
		analyzer.DeleteTable(bat)
	case ActionInsertDatabase:
		analyzer.InsertDatabase(bat)
	case ActionDeleteDatabase:
		analyzer.DeleteDatabase(bat)
	default:
	}
}

func (analyzer *DDLAnalyzer) GetDDLs() (res []*DDL) {
	analyzer.tables.Scan(func(item *DDLTableItem) bool {
		ddl := new(DDL)
		ddl.AccountId = item.AccountId
		ddl.DB = item.DB
		ddl.Table = item.Table
		ddl.DBId = item.DBId
		ddl.TableId = item.TableId
		ddl.OldTableId = item.OldTableId
		if item.Deleted {
			if item.OldTableId == 0 { //drop table
				ddl.Typ = DDLTypeDropTable
			} else {
				if item.OldTableId != item.TableId {
					//alter table copy or
					//truncate table
					ddl.Typ = DDLTypeAlterTableCopy
					//TODO: check schema change
				} else {
					//alter table inplace
					ddl.Typ = DDLTypeAlterTableInplace
				}
			}

		} else {
			//create table
			ddl.Typ = DDLTypeCreateTable
		}
		res = append(res, ddl)
		return true
	})

	analyzer.dbs.Scan(func(item *DDLDBItem) bool {
		ddl := new(DDL)
		ddl.AccountId = item.AccountId
		ddl.DB = item.DB
		ddl.DBId = item.DBId
		if item.Deleted {
			ddl.Typ = DDLTypeDropDB
		} else {
			ddl.Typ = DDLTypeCreateDB
		}
		res = append(res, ddl)
		return true
	})
	return
}

func (analyzer *DDLAnalyzer) Clear() {
	analyzer.ts = timestamp.Timestamp{}
	analyzer.tables.Clear()
	analyzer.dbs.Clear()
}

func (analyzer *DDLAnalyzer) Init(ts timestamp.Timestamp) {
	analyzer.ts = ts
}
