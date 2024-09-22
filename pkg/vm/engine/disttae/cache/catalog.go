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

package cache

import (
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"

	"github.com/tidwall/btree"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func NewCatalog() *CatalogCache {
	return &CatalogCache{
		tables: &tableCache{
			data:       btree.NewBTreeG(tableItemLess),
			cpkeyIndex: btree.NewBTreeG(tableItemCPKeyLess),
		},
		databases: &databaseCache{
			data:       btree.NewBTreeG(databaseItemLess),
			cpkeyIndex: btree.NewBTreeG(databaseItemCPKeyLess),
		},
		mu: struct {
			sync.Mutex
			start types.TS
			end   types.TS
		}{start: types.MaxTs()},
	}
}

func (cc *CatalogCache) UpdateDuration(start types.TS, end types.TS) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.mu.start = start
	cc.mu.end = end
	logutil.Info("FIND_TABLE CACHE update serve range",
		zap.String("start", cc.mu.start.ToString()),
		zap.String("end", cc.mu.end.ToString()))
}

func (cc *CatalogCache) UpdateStart(ts types.TS) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.mu.start != types.MaxTs() && ts.GT(&cc.mu.start) {
		cc.mu.start = ts
		logutil.Info("FIND_TABLE CACHE update serve range (by start)",
			zap.String("start", cc.mu.start.ToString()),
			zap.String("end", cc.mu.end.ToString()))
	}
}

func (cc *CatalogCache) CanServe(ts types.TS) bool {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return ts.GreaterEq(&cc.mu.start) && ts.LE(&cc.mu.end)
}

type GCReport struct {
	TScanItem  int
	TStaleItem int
	TStaleCpk  int
	TDelCpk    int
	DScanItem  int
	DStaleItem int
	DStaleCpk  int
	DDelCpk    int
}

func (cc *CatalogCache) GC(ts timestamp.Timestamp) GCReport {
	/*
							GC
		-----------------+--------> ts
		n1: I D     I,D                 all
		n2: I D   I                     first 2
		n3:                I D   I      no
		n4:  I D I          D           first 2
		n5:     I D            I        first 2

		To be GCed in table entries:
		delete all entries with ts < GCts, with one expection:
		if the the largest ts entry among them is a Insert Entry, keep that one.

		To be GCed in cpkIndex:
		delete n1 & n4.
		for every name, if the latest ts entry is a delete entry, delete it.

	*/

	inst := time.Now()
	r := GCReport{}
	{ // table cache gc
		var prevName string
		var prevDbId uint64
		deletedCpkey := make([]*TableItem, 0, 16)
		deletedItems := make([]*TableItem, 0, 16)
		seenLargest := false
		// TODO(aptend)gc stale deleted items
		cc.tables.data.Scan(func(item *TableItem) bool {
			if item.DatabaseId != prevDbId {
				prevDbId = item.DatabaseId
				prevName = ""
			}

			if item.Name != prevName {
				prevName = item.Name
				seenLargest = false
				if item.deleted {
					deletedCpkey = append(deletedCpkey, item)
				}
			}

			if item.Ts.Less(ts) {
				if !seenLargest {
					seenLargest = true
					if item.deleted {
						deletedItems = append(deletedItems, item)
					}
				} else {
					deletedItems = append(deletedItems, item)
				}
			}
			r.TScanItem++
			return true
		})
		r.TStaleItem = len(deletedItems)
		r.TStaleCpk = len(deletedCpkey)
		for _, item := range deletedItems {
			cc.tables.data.Delete(item)
		}
		for _, item := range deletedCpkey {
			lastest, exist := cc.tables.cpkeyIndex.Get(item)
			if !exist || lastest.Ts.Greater(item.Ts) {
				continue
			}
			r.TDelCpk += 1
			cc.tables.cpkeyIndex.Delete(item)
		}

		// TODO(aptend): Add Metric
	}
	{ // database cache gc
		var prevName string
		deletedCpkey := make([]*DatabaseItem, 0, 16)
		deletedItems := make([]*DatabaseItem, 0, 16)
		seenLargest := false
		cc.databases.data.Scan(func(item *DatabaseItem) bool {
			if item.Name != prevName {
				prevName = item.Name
				seenLargest = false
				if item.deleted {
					deletedCpkey = append(deletedCpkey, item)
				}
			}

			if item.Ts.Less(ts) {
				if !seenLargest {
					seenLargest = true
					if item.deleted {
						deletedItems = append(deletedItems, item)
					}
				} else {
					deletedItems = append(deletedItems, item)
				}
			}
			r.DScanItem++
			return true
		})

		r.DStaleItem = len(deletedItems)
		r.DStaleCpk = len(deletedCpkey)
		for _, item := range deletedItems {
			cc.databases.data.Delete(item)
		}
		for _, item := range deletedCpkey {
			lastest, exist := cc.databases.cpkeyIndex.Get(item)
			if !exist || lastest.Ts.Greater(item.Ts) {
				continue
			}
			r.DDelCpk += 1
			cc.databases.cpkeyIndex.Delete(item)
		}
	}
	cc.UpdateStart(types.TimestampToTS(ts))
	duration := time.Since(inst)
	logutil.Info("FIND_TABLE CACHE gc", zap.Any("report", r), zap.Duration("cost", duration))
	return r
}

func (cc *CatalogCache) Databases(accountId uint32, ts timestamp.Timestamp) []string {
	var rs []string

	key := &DatabaseItem{
		AccountId: accountId,
	}
	mp := make(map[string]struct{})
	cc.databases.data.Ascend(key, func(item *DatabaseItem) bool {
		if item.AccountId != accountId {
			return false
		}
		if item.Ts.Greater(ts) {
			return true
		}
		if _, ok := mp[item.Name]; !ok {
			mp[item.Name] = struct{}{}
			if !item.deleted {
				rs = append(rs, item.Name)
			}
		}
		return true
	})
	return rs
}

func (cc *CatalogCache) Tables(accountId uint32, databaseId uint64,
	ts timestamp.Timestamp) ([]string, []uint64) {
	var rs []string
	var rids []uint64

	key := &TableItem{
		AccountId:  accountId,
		DatabaseId: databaseId,
	}
	mp := make(map[string]struct{})
	cc.tables.data.Ascend(key, func(item *TableItem) bool {
		if item.AccountId != accountId || item.DatabaseId != databaseId {
			return false
		}

		if item.Ts.Greater(ts) {
			return true
		}
		if _, ok := mp[item.Name]; !ok {
			// How does this work?
			// 1. If there are two items in the same txn, non-deleted always comes first.
			// 2. if this item is deleted, the map will block the next item with the same name.
			mp[item.Name] = struct{}{}
			if !item.deleted {
				rs = append(rs, item.Name)
				rids = append(rids, item.Id)
			}
		}
		return true
	})
	return rs, rids
}

// GetTableByIdAndTime's complexicity is O(n), where n is the number of all tables in the database
// Note: if databaseId is 0, it means the database is not specified. will scan all tables under the account
func (cc *CatalogCache) GetTableByIdAndTime(accountID uint32, databaseId, tblId uint64, ts timestamp.Timestamp) *TableItem {
	// Snapshot of the table data:
	// acc dbid tblname ts
	//   0 42 A 5
	//   0 42 A 4
	//   0 42 A 3
	//   0 42 A 2
	//   0 42 A 1
	//   0 42 B 1
	//   0 42 C 2
	//   0 42 C 1
	// given the accountID = 0, databaseId = 42, tblId = 42421, ts = 3
	// we want to find out which table has tblid = 42421 from the three candidates:
	// a. 0 42 A 3
	// b. 0 42 B 1
	// c. 0 42 C 2

	var rel *TableItem

	key := &TableItem{
		AccountId:  accountID,
		DatabaseId: databaseId,
	}

	prevTableName := ""
	prevDbId := databaseId
	cc.tables.data.Ascend(key, func(item *TableItem) bool {
		if item.AccountId != accountID {
			return false
		}
		if item.DatabaseId != prevDbId {
			if databaseId > 0 {
				return false
			} else {
				// new database, reset table name check
				// consider tables in different databases that have the same name
				prevDbId = item.DatabaseId
				prevTableName = ""
			}
		}
		if item.Ts.Greater(ts) {
			return true // continue to find older version
		}

		// only check the first existing item visible to the timestamp
		if item.Name != prevTableName {
			// this is the first visible item with brand new name
			if !item.deleted && item.Id == tblId {
				rel = item
				return false
			}
			prevTableName = item.Name
		}

		// no need to check the items with the same name
		return true
	})
	return rel
}

func (cc *CatalogCache) scanThrough(aid uint32, did uint64, f func(*TableItem) bool) (ret *TableItem) {
	key := &TableItem{
		AccountId:  aid,
		DatabaseId: did,
	}
	cc.tables.data.Ascend(key, func(item *TableItem) bool {
		if item.AccountId != aid || item.DatabaseId != did {
			return false
		}
		// delete entry has incomplete information for tableitem
		if !item.deleted && f(item) {
			ret = item
			return false
		}
		return true
	})
	return
}

// GetTableById's complexicity is O(n), where n is the number of all items of the database.
func (cc *CatalogCache) GetTableById(aid uint32, databaseId, tblId uint64) *TableItem {
	return cc.scanThrough(aid, databaseId, func(item *TableItem) bool {
		return item.Id == tblId
	})
}

// GetTableByName's complexicity is O(n), where n is the number of all items of the database.
func (cc *CatalogCache) GetTableByName(aid uint32, databaseID uint64, tableName string) *TableItem {
	return cc.scanThrough(aid, databaseID, func(item *TableItem) bool {
		return item.Name == tableName
	})
}

func (cc *CatalogCache) GetTable(tbl *TableItem) bool {
	var find bool

	cc.tables.data.Ascend(tbl, func(item *TableItem) bool {
		if item.AccountId != tbl.AccountId ||
			item.DatabaseId != tbl.DatabaseId ||
			item.Name != tbl.Name {
			return false
		}

		// just find once
		if !item.deleted {
			find = true
			copyTableItem(tbl, item)
		}
		return false
	})

	return find
}

func (cc *CatalogCache) HasNewerVersion(qry *TableChangeQuery) bool {
	var find bool

	key := &TableItem{
		AccountId:  qry.AccountId,
		DatabaseId: qry.DatabaseId,
		Name:       qry.Name,
		Ts:         types.MaxTs().ToTimestamp(), // get the latest version
	}
	cc.tables.data.Ascend(key, func(item *TableItem) bool {
		if item.Name != qry.Name {
			return false
		}

		if item.Ts.Greater(qry.Ts) {
			if item.deleted || item.Id != qry.TableId || item.Version < qry.Version {
				find = true
			}
		}
		return false
	})
	return find
}

func (cc *CatalogCache) GetDatabase(db *DatabaseItem) bool {
	var find bool

	cc.databases.data.Ascend(db, func(item *DatabaseItem) bool {
		if item.AccountId != db.AccountId || item.Name != db.Name {
			return false
		}

		// just find once
		if !item.deleted {
			find = true
			copyDatabaseItem(db, item)
		}
		return false
	})

	return find
}

func (cc *CatalogCache) DeleteTable(bat *batch.Batch) {
	cpks := bat.GetVector(MO_OFF + 0)
	timestamps := vector.MustFixedColWithTypeCheck[types.TS](bat.GetVector(MO_TIMESTAMP_IDX))
	for i, ts := range timestamps {
		pk := cpks.GetBytesAt(i)
		if item, ok := cc.tables.cpkeyIndex.Get(&TableItem{CPKey: pk}); ok {
			// Note: the newItem.Id is the latest id under the name of the table,
			// not the id that should be seen at the moment ts.
			// Lucy thing is that the wrong tableid hold by this delete item will never be used.
			newItem := &TableItem{
				deleted:    true,
				Id:         item.Id,
				Name:       item.Name,
				CPKey:      append([]byte{}, item.CPKey...),
				AccountId:  item.AccountId,
				DatabaseId: item.DatabaseId,
				Ts:         ts.ToTimestamp(),
			}
			cc.tables.data.Set(newItem)
		}
	}
}

func (cc *CatalogCache) DeleteDatabase(bat *batch.Batch) {
	cpks := bat.GetVector(MO_OFF + 0)
	timestamps := vector.MustFixedColWithTypeCheck[types.TS](bat.GetVector(MO_TIMESTAMP_IDX))
	for i, ts := range timestamps {
		pk := cpks.GetBytesAt(i)
		if item, ok := cc.databases.cpkeyIndex.Get(&DatabaseItem{CPKey: pk}); ok {
			newItem := &DatabaseItem{
				deleted:   true,
				Id:        item.Id,
				Name:      item.Name,
				Rowid:     item.Rowid,
				CPKey:     append([]byte{}, item.CPKey...),
				AccountId: item.AccountId,
				Typ:       item.Typ,
				CreateSql: item.CreateSql,
				Ts:        ts.ToTimestamp(),
			}
			cc.databases.data.Set(newItem)
		}
	}
}

func ParseTablesBatchAnd(bat *batch.Batch, f func(*TableItem)) {
	timestamps := vector.MustFixedColWithTypeCheck[types.TS](bat.GetVector(MO_TIMESTAMP_IDX))
	accounts := vector.MustFixedColWithTypeCheck[uint32](bat.GetVector(catalog.MO_TABLES_ACCOUNT_ID_IDX + MO_OFF))
	names := bat.GetVector(catalog.MO_TABLES_REL_NAME_IDX + MO_OFF)
	ids := vector.MustFixedColWithTypeCheck[uint64](bat.GetVector(catalog.MO_TABLES_REL_ID_IDX + MO_OFF))
	databaseIds := vector.MustFixedColWithTypeCheck[uint64](bat.GetVector(catalog.MO_TABLES_RELDATABASE_ID_IDX + MO_OFF))
	databaseNames := bat.GetVector(catalog.MO_TABLES_RELDATABASE_IDX + MO_OFF)
	kinds := bat.GetVector(catalog.MO_TABLES_RELKIND_IDX + MO_OFF)
	comments := bat.GetVector(catalog.MO_TABLES_REL_COMMENT_IDX + MO_OFF)
	createSqls := bat.GetVector(catalog.MO_TABLES_REL_CREATESQL_IDX + MO_OFF)
	viewDefs := bat.GetVector(catalog.MO_TABLES_VIEWDEF_IDX + MO_OFF)
	partitioneds := vector.MustFixedColWithTypeCheck[int8](bat.GetVector(catalog.MO_TABLES_PARTITIONED_IDX + MO_OFF))
	paritions := bat.GetVector(catalog.MO_TABLES_PARTITION_INFO_IDX + MO_OFF)
	constraints := bat.GetVector(catalog.MO_TABLES_CONSTRAINT_IDX + MO_OFF)
	versions := vector.MustFixedColWithTypeCheck[uint32](bat.GetVector(catalog.MO_TABLES_VERSION_IDX + MO_OFF))
	catalogVersions := vector.MustFixedColWithTypeCheck[uint32](bat.GetVector(catalog.MO_TABLES_CATALOG_VERSION_IDX + MO_OFF))
	extraInfos := bat.GetVector(catalog.MO_TABLES_EXTRA_INFO_IDX + MO_OFF)
	pks := bat.GetVector(catalog.MO_TABLES_CPKEY_IDX + MO_OFF)
	for i, account := range accounts {
		item := new(TableItem)
		item.Id = ids[i]
		item.Name = names.GetStringAt(i)
		item.AccountId = account
		item.DatabaseId = databaseIds[i]
		item.DatabaseName = databaseNames.GetStringAt(i)
		item.Ts = timestamps[i].ToTimestamp()
		item.Kind = kinds.GetStringAt(i)
		item.ViewDef = viewDefs.GetStringAt(i)
		item.Constraint = append(item.Constraint, constraints.GetBytesAt(i)...)
		item.Comment = comments.GetStringAt(i)
		item.Partitioned = partitioneds[i]
		item.Partition = paritions.GetStringAt(i)
		item.CreateSql = createSqls.GetStringAt(i)
		item.Version = versions[i]
		item.CatalogVersion = catalogVersions[i]
		item.PrimaryIdx = -1
		item.PrimarySeqnum = -1
		item.ClusterByIdx = -1
		item.CPKey = append(item.CPKey, pks.GetBytesAt(i)...)
		item.ExtraInfo = api.MustUnmarshalTblExtra(extraInfos.GetBytesAt(i))
		f(item)
	}
}

func (cc *CatalogCache) InsertTable(bat *batch.Batch) {
	ParseTablesBatchAnd(bat, func(item *TableItem) {
		cc.tables.data.Set(item)
		cc.tables.cpkeyIndex.Set(item)
	})
}

func ParseColumnsBatchAnd(bat *batch.Batch, f func(map[TableItemKey]Columns)) {
	var tblKey TableItemKey

	mp := make(map[TableItemKey]Columns) // TableItem -> columns
	// get table key info
	timestamps := vector.MustFixedColWithTypeCheck[types.TS](bat.GetVector(MO_TIMESTAMP_IDX))
	accounts := vector.MustFixedColWithTypeCheck[uint32](bat.GetVector(catalog.MO_COLUMNS_ACCOUNT_ID_IDX + MO_OFF))
	databaseIds := vector.MustFixedColWithTypeCheck[uint64](bat.GetVector(catalog.MO_COLUMNS_ATT_DATABASE_ID_IDX + MO_OFF))
	tableNames := bat.GetVector(catalog.MO_COLUMNS_ATT_RELNAME_IDX + MO_OFF)
	tableIds := vector.MustFixedColWithTypeCheck[uint64](bat.GetVector(catalog.MO_COLUMNS_ATT_RELNAME_ID_IDX + MO_OFF))
	// get columns info
	names := bat.GetVector(catalog.MO_COLUMNS_ATTNAME_IDX + MO_OFF)
	comments := bat.GetVector(catalog.MO_COLUMNS_ATT_COMMENT_IDX + MO_OFF)
	isHiddens := vector.MustFixedColWithTypeCheck[int8](bat.GetVector(catalog.MO_COLUMNS_ATT_IS_HIDDEN_IDX + MO_OFF))
	isAutos := vector.MustFixedColWithTypeCheck[int8](bat.GetVector(catalog.MO_COLUMNS_ATT_IS_AUTO_INCREMENT_IDX + MO_OFF))
	constraintTypes := bat.GetVector(catalog.MO_COLUMNS_ATT_CONSTRAINT_TYPE_IDX + MO_OFF)
	typs := bat.GetVector(catalog.MO_COLUMNS_ATTTYP_IDX + MO_OFF)
	hasDefs := vector.MustFixedColWithTypeCheck[int8](bat.GetVector(catalog.MO_COLUMNS_ATTHASDEF_IDX + MO_OFF))
	defaultExprs := bat.GetVector(catalog.MO_COLUMNS_ATT_DEFAULT_IDX + MO_OFF)
	hasUpdates := vector.MustFixedColWithTypeCheck[int8](bat.GetVector(catalog.MO_COLUMNS_ATT_HAS_UPDATE_IDX + MO_OFF))
	updateExprs := bat.GetVector(catalog.MO_COLUMNS_ATT_UPDATE_IDX + MO_OFF)
	nums := vector.MustFixedColWithTypeCheck[int32](bat.GetVector(catalog.MO_COLUMNS_ATTNUM_IDX + MO_OFF))
	clusters := vector.MustFixedColWithTypeCheck[int8](bat.GetVector(catalog.MO_COLUMNS_ATT_IS_CLUSTERBY + MO_OFF))
	seqnums := vector.MustFixedColWithTypeCheck[uint16](bat.GetVector(catalog.MO_COLUMNS_ATT_SEQNUM_IDX + MO_OFF))
	enumValues := bat.GetVector(catalog.MO_COLUMNS_ATT_ENUM_IDX + MO_OFF)
	for i, account := range accounts {
		ts := timestamps[i].ToTimestamp()
		tblKey.Name = tableNames.GetStringAt(i)
		tblKey.AccountId = account
		tblKey.DatabaseId = databaseIds[i]
		tblKey.Id = tableIds[i]
		tblKey.Ts.fromTs(&ts)

		col := catalog.Column{
			Num:             nums[i],
			Name:            names.GetStringAt(i),
			Comment:         comments.GetStringAt(i),
			IsHidden:        isHiddens[i],
			IsAutoIncrement: isAutos[i],
			HasDef:          hasDefs[i],
			HasUpdate:       hasUpdates[i],
			ConstraintType:  constraintTypes.GetStringAt(i),
			IsClusterBy:     clusters[i],
			Seqnum:          seqnums[i],
			EnumValues:      enumValues.GetStringAt(i),
		}
		col.Typ = append(col.Typ, typs.GetBytesAt(i)...)
		col.UpdateExpr = append(col.UpdateExpr, updateExprs.GetBytesAt(i)...)
		col.DefaultExpr = append(col.DefaultExpr, defaultExprs.GetBytesAt(i)...)
		mp[tblKey] = append(mp[tblKey], col)
	}
	f(mp)
}

func InitTableItemWithColumns(item *TableItem, cols Columns) {
	sort.Sort(cols)
	coldefs := make([]engine.TableDef, 0, len(cols))
	for i, col := range cols {
		if col.ConstraintType == catalog.SystemColPKConstraint {
			item.PrimaryIdx = i
			item.PrimarySeqnum = int(col.Seqnum)
		}
		if col.IsClusterBy == 1 {
			item.ClusterByIdx = i
		}
		coldefs = append(coldefs, genTableDefOfColumn(col))
	}
	item.TableDef, item.Defs = getTableDef(item, coldefs)
}

func (cc *CatalogCache) InsertColumns(bat *batch.Batch) {
	ParseColumnsBatchAnd(bat, func(mp map[TableItemKey]Columns) {
		queryKey := new(TableItem)
		for k, cols := range mp {
			queryKey.Name = k.Name
			queryKey.AccountId = k.AccountId
			queryKey.DatabaseId = k.DatabaseId
			queryKey.Id = k.Id
			queryKey.Ts = k.Ts.toTs()
			item, exist := cc.tables.data.Get(queryKey)
			if !exist {
				logutil.Errorf(
					"catalog-cache table %v-%v-%v not found when inserting columns",
					k.AccountId, k.DatabaseId, k.Name,
				)
				continue
			}
			InitTableItemWithColumns(item, cols)
		}
	})
}

func (cc *CatalogCache) InsertDatabase(bat *batch.Batch) {
	ParseDatabaseBatchAnd(bat, func(item *DatabaseItem) {
		cc.databases.data.Set(item)
		cc.databases.cpkeyIndex.Set(item)
	})
}

func ParseDatabaseBatchAnd(bat *batch.Batch, f func(*DatabaseItem)) {
	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](bat.GetVector(MO_ROWID_IDX))
	timestamps := vector.MustFixedColWithTypeCheck[types.TS](bat.GetVector(MO_TIMESTAMP_IDX))
	accounts := vector.MustFixedColWithTypeCheck[uint32](bat.GetVector(catalog.MO_DATABASE_ACCOUNT_ID_IDX + MO_OFF))
	names := bat.GetVector(catalog.MO_DATABASE_DAT_NAME_IDX + MO_OFF)
	ids := vector.MustFixedColWithTypeCheck[uint64](bat.GetVector(catalog.MO_DATABASE_DAT_ID_IDX + MO_OFF))
	typs := bat.GetVector(catalog.MO_DATABASE_DAT_TYPE_IDX + MO_OFF)
	createSqls := bat.GetVector(catalog.MO_DATABASE_CREATESQL_IDX + MO_OFF)
	pks := bat.GetVector(catalog.MO_DATABASE_CPKEY_IDX + MO_OFF)
	for i, account := range accounts {
		item := new(DatabaseItem)
		item.Id = ids[i]
		item.Name = names.GetStringAt(i)
		item.AccountId = account
		item.Ts = timestamps[i].ToTimestamp()
		item.Typ = typs.GetStringAt(i)
		item.CreateSql = createSqls.GetStringAt(i)
		copy(item.Rowid[:], rowids[i][:])
		item.CPKey = append(item.CPKey, pks.GetBytesAt(i)...)
		f(item)
	}
}

func genTableDefOfColumn(col catalog.Column) engine.TableDef {
	var attr engine.Attribute

	attr.Name = col.Name
	attr.ID = uint64(col.Num)
	attr.Alg = compress.Lz4
	attr.Comment = col.Comment
	attr.IsHidden = col.IsHidden == 1
	attr.ClusterBy = col.IsClusterBy == 1
	attr.AutoIncrement = col.IsAutoIncrement == 1
	attr.Seqnum = col.Seqnum
	attr.EnumVlaues = col.EnumValues
	if err := types.Decode(col.Typ, &attr.Type); err != nil {
		panic(err)
	}
	attr.Default = new(plan.Default)
	if col.HasDef == 1 {
		if err := types.Decode(col.DefaultExpr, attr.Default); err != nil {
			panic(err)
		}
	}
	if col.HasUpdate == 1 {
		attr.OnUpdate = new(plan.OnUpdate)
		if err := types.Decode(col.UpdateExpr, attr.OnUpdate); err != nil {
			panic(err)
		}
	}
	if col.ConstraintType == catalog.SystemColPKConstraint {
		attr.Primary = true
	}
	return &engine.AttributeDef{Attr: attr}
}

func getTableDef(tblItem *TableItem, coldefs []engine.TableDef) (*plan.TableDef, []engine.TableDef) {
	var clusterByDef *plan.ClusterByDef
	var cols []*plan.ColDef
	var defs []*plan.TableDef_DefType
	var properties []*plan.Property
	var TableType string
	var Createsql string
	var partitionInfo *plan.PartitionByDef
	var viewSql *plan.ViewDef
	var foreignKeys []*plan.ForeignKeyDef
	var primarykey *plan.PrimaryKeyDef
	var indexes []*plan.IndexDef
	var refChildTbls []uint64

	tableDef := make([]engine.TableDef, 0)

	i := int32(0)
	name2index := make(map[string]int32)
	for _, def := range coldefs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			name := strings.ToLower(attr.Attr.Name)
			name2index[name] = i
			cols = append(cols, &plan.ColDef{
				ColId:      attr.Attr.ID,
				Name:       name,
				OriginName: attr.Attr.Name,
				Typ: plan.Type{
					Id:          int32(attr.Attr.Type.Oid),
					Width:       attr.Attr.Type.Width,
					Scale:       attr.Attr.Type.Scale,
					AutoIncr:    attr.Attr.AutoIncrement,
					Table:       tblItem.Name,
					NotNullable: attr.Attr.Default != nil && !attr.Attr.Default.NullAbility,
					Enumvalues:  attr.Attr.EnumVlaues,
				},
				Primary:   attr.Attr.Primary,
				Default:   attr.Attr.Default,
				OnUpdate:  attr.Attr.OnUpdate,
				Comment:   attr.Attr.Comment,
				ClusterBy: attr.Attr.ClusterBy,
				Hidden:    attr.Attr.IsHidden,
				Seqnum:    uint32(attr.Attr.Seqnum),
			})
			if attr.Attr.ClusterBy {
				clusterByDef = &plan.ClusterByDef{
					Name: name,
				}
			}
			i++
		}
	}

	if tblItem.Comment != "" {
		properties = append(properties, &plan.Property{
			Key:   catalog.SystemRelAttr_Comment,
			Value: tblItem.Comment,
		})

		tableDef = append(tableDef, &engine.CommentDef{Comment: tblItem.Comment})
	}

	if tblItem.Partitioned > 0 {
		p := &plan.PartitionByDef{}
		err := p.UnMarshalPartitionInfo(([]byte)(tblItem.Partition))
		if err != nil {
			logutil.Errorf(
				"catalog-cache error: unmarshal partition metadata information: %v-%v-%v, err: %v",
				tblItem.AccountId, tblItem.Id, tblItem.Name, err)
			return nil, nil
		}
		partitionInfo = p

		tableDef = append(tableDef, &engine.PartitionDef{
			Partitioned: tblItem.Partitioned,
			Partition:   tblItem.Partition,
		})
	}

	if tblItem.ViewDef != "" {
		viewSql = &plan.ViewDef{
			View: tblItem.ViewDef,
		}

		tableDef = append(tableDef, &engine.ViewDef{View: tblItem.ViewDef})
	}

	if len(tblItem.Constraint) > 0 {
		c := &engine.ConstraintDef{}
		err := c.UnmarshalBinary(tblItem.Constraint)
		if err != nil {
			logutil.Errorf("catalog-cache error: unmarshal table constraint information: %v-%v-%v, err: %v",
				tblItem.AccountId, tblItem.Id, tblItem.Name, err)
			return nil, nil
		}

		tableDef = append(tableDef, c)
		for _, ct := range c.Cts {
			switch k := ct.(type) {
			case *engine.IndexDef:
				indexes = k.Indexes
			case *engine.ForeignKeyDef:
				foreignKeys = k.Fkeys
			case *engine.RefChildTableDef:
				refChildTbls = k.Tables
			case *engine.PrimaryKeyDef:
				primarykey = k.Pkey
			case *engine.StreamConfigsDef:
				properties = append(properties, k.Configs...)
			}
		}
	}

	properties = append(properties, &plan.Property{
		Key:   catalog.SystemRelAttr_Kind,
		Value: tblItem.Kind,
	})
	TableType = tblItem.Kind

	props := &engine.PropertiesDef{}
	props.Properties = append(props.Properties, engine.Property{
		Key:   catalog.SystemRelAttr_Kind,
		Value: TableType,
	})
	props.Properties = append(props.Properties, engine.Property{
		Key:   catalog.PropSchemaExtra,
		Value: string(api.MustMarshalTblExtra(tblItem.ExtraInfo)),
	})

	if tblItem.CreateSql != "" {
		properties = append(properties, &plan.Property{
			Key:   catalog.SystemRelAttr_CreateSQL,
			Value: tblItem.CreateSql,
		})
		Createsql = tblItem.CreateSql

		props.Properties = append(props.Properties, engine.Property{
			Key:   catalog.SystemRelAttr_CreateSQL,
			Value: Createsql,
		})
	}

	tableDef = append(tableDef, props)
	tableDef = append(tableDef, coldefs...)
	if len(properties) > 0 {
		defs = append(defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{
					Properties: properties,
				},
			},
		})
	}

	if primarykey != nil && primarykey.PkeyColName == catalog.CPrimaryKeyColName {
		primarykey.CompPkeyCol = plan2.GetColDefFromTable(cols, catalog.CPrimaryKeyColName)
	}
	if clusterByDef != nil && util.JudgeIsCompositeClusterByColumn(clusterByDef.Name) {
		clusterByDef.CompCbkeyCol = plan2.GetColDefFromTable(cols, clusterByDef.Name)
	}

	return &plan.TableDef{
		TblId:         tblItem.Id,
		Name:          tblItem.Name,
		DbName:        tblItem.DatabaseName,
		Cols:          cols,
		Name2ColIndex: name2index,
		Defs:          defs,
		TableType:     TableType,
		Createsql:     Createsql,
		Pkey:          primarykey,
		ViewSql:       viewSql,
		Partition:     partitionInfo,
		Fkeys:         foreignKeys,
		RefChildTbls:  refChildTbls,
		ClusterBy:     clusterByDef,
		Indexes:       indexes,
		Version:       tblItem.Version,
	}, tableDef
}
