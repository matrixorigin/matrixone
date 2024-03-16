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
	"fmt"
	"math"
	"sort"

	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/tidwall/btree"
)

func NewCatalog() *CatalogCache {
	return &CatalogCache{
		tables: &tableCache{
			data:       btree.NewBTreeG(tableItemLess),
			rowidIndex: btree.NewBTreeG(tableItemRowidLess),
			tableGuard: newTableGuard(),
		},
		databases: &databaseCache{
			data:       btree.NewBTreeG(databaseItemLess),
			rowidIndex: btree.NewBTreeG(databaseItemRowidLess),
		},
	}
}

func (cc *CatalogCache) GC(ts timestamp.Timestamp) {
	{ // table cache gc
		var items []*TableItem

		cc.tables.data.Scan(func(item *TableItem) bool {
			if len(items) > GcBuffer {
				return false
			}
			if item.Ts.Less(ts) {
				items = append(items, item)
			}
			return true
		})
		for _, item := range items {
			cc.tables.data.Delete(item)
			if !item.deleted {
				cc.tables.rowidIndex.Delete(item)
			}
		}
	}
	{ // database cache gc
		var items []*DatabaseItem

		cc.databases.data.Scan(func(item *DatabaseItem) bool {
			if len(items) > GcBuffer {
				return false
			}
			if item.Ts.Less(ts) {
				items = append(items, item)
			}
			return true
		})
		for _, item := range items {
			cc.databases.data.Delete(item)
			if !item.deleted {
				cc.databases.rowidIndex.Delete(item)
			}
		}
	}
}

type tableIdNameKey struct {
	id   uint64
	name string
}

func (cc *CatalogCache) Tables(accountId uint32, databaseId uint64,
	ts timestamp.Timestamp) ([]string, []uint64) {
	var rs []string
	var rids []uint64

	key := &TableItem{
		AccountId:  accountId,
		DatabaseId: databaseId,
	}
	mp := make(map[tableIdNameKey]uint8)
	cc.tables.data.Ascend(key, func(item *TableItem) bool {
		if item.AccountId != accountId {
			return false
		}
		if item.DatabaseId != databaseId {
			return false
		}
		// In previous impl table id is used to deduplicate, but this a corner case: rename table t to newt, and rename newt back to t.
		// In this case newt is first found deleted and taking the place of active t's tableid.
		// What's more, if a table is truncated, a name can be occuppied by different ids. only use name to to dedup is also inadequate.
		if item.Ts.Greater(ts) {
			return true
		}
		key := tableIdNameKey{id: item.Id, name: item.Name}
		if _, ok := mp[key]; !ok {
			mp[key] = 0
			if !item.deleted {
				rs = append(rs, item.Name)
				rids = append(rids, item.Id)
			}
		}
		return true
	})
	return rs, rids
}

func (cc *CatalogCache) GetTableById(databaseId, tblId uint64) *TableItem {
	var rel *TableItem

	key := &TableItem{
		DatabaseId: databaseId,
	}
	// If account is much, the performance is very bad.
	cc.tables.data.Ascend(key, func(item *TableItem) bool {
		if item.Id == tblId {
			rel = item
			return false
		}
		return true
	})
	return rel
}

// GetTableByName returns the table item whose name is tableName in the database.
func (cc *CatalogCache) GetTableByName(databaseID uint64, tableName string) *TableItem {
	var rel *TableItem
	key := &TableItem{
		DatabaseId: databaseID,
	}
	cc.tables.data.Ascend(key, func(item *TableItem) bool {
		if item.Name == tableName {
			rel = item
			return false
		}
		return true
	})
	return rel
}

func (cc *CatalogCache) Databases(accountId uint32, ts timestamp.Timestamp) []string {
	var rs []string

	key := &DatabaseItem{
		AccountId: accountId,
	}
	mp := make(map[string]uint8)
	cc.databases.data.Ascend(key, func(item *DatabaseItem) bool {
		if item.AccountId != accountId {
			return false
		}
		if item.Ts.Greater(ts) {
			return true
		}
		if _, ok := mp[item.Name]; !ok {
			mp[item.Name] = 0
			if !item.deleted {
				rs = append(rs, item.Name)
			}
		}
		return true
	})
	return rs
}

func (cc *CatalogCache) GetTable(tbl *TableItem) bool {
	var find bool
	var ts timestamp.Timestamp
	/**
	In push mode.
	It is necessary to distinguish the case create table/drop table
	from truncate table.

	CORNER CASE 1:
	begin;
	create table t1(a int);//table id x. catalog.insertTable(table id x)
	insert into t1 values (1);
	drop table t1; //same table id x. catalog.deleteTable(table id x)
	commit;

	CORNER CASE 2:
	create table t1(a int); //table id x.
	begin;
	insert into t1 values (1);
	-- @session:id=1{
	truncate table t1;//insert table id y, then delete table id x. catalog.insertTable(table id y). catalog.deleteTable(table id x)
	-- @session}
	commit;

	CORNER CASE 3:
	create table t1(a int); //table id x.
	begin;
	truncate t1;//table id x changed to x1
	truncate t1;//table id x1 changed to x2
	truncate t1;//table id x2 changed to x3
	commit;//catalog.insertTable(table id x1,x2,x3). catalog.deleteTable(table id x,x1,x2)

	To be clear that the TableItem in catalogCache is sorted by the table id.
	*/
	var tableId uint64
	deleted := make(map[uint64]bool)
	inserted := make(map[uint64]*TableItem)
	tbl.Id = math.MaxUint64
	cc.tables.data.Ascend(tbl, func(item *TableItem) bool {
		if item.deleted && item.AccountId == tbl.AccountId &&
			item.DatabaseId == tbl.DatabaseId && item.Name == tbl.Name {
			if !ts.IsEmpty() {
				//if it is the truncate operation, we collect deleteTable together.
				if item.Ts.Equal(ts) {
					deleted[item.Id] = true
					return true
				} else {
					return false
				}
			}
			ts = item.Ts
			tableId = item.Id
			deleted[item.Id] = true
			return true
		}
		if !item.deleted && item.AccountId == tbl.AccountId &&
			item.DatabaseId == tbl.DatabaseId && item.Name == tbl.Name &&
			(ts.IsEmpty() || ts.Equal(item.Ts) && tableId != item.Id) {
			//if it is the truncate operation, we collect insertTable together first.
			if !ts.IsEmpty() && ts.Equal(item.Ts) && tableId != item.Id {
				inserted[item.Id] = item
				return true
			} else {
				find = true
				copyTableItem(tbl, item)
				return false
			}
		}
		if find {
			return false
		}
		return false
	})

	if find {
		return true
	}

	//handle truncate operation independently
	//remove deleted item from inserted item
	for rowid := range deleted {
		delete(inserted, rowid)
	}

	//if there is no inserted item, it means that the table is deleted.
	if len(inserted) == 0 {
		return false
	}

	//if there is more than one inserted item, it means that it is wrong
	if len(inserted) > 1 {
		panic(fmt.Sprintf("account %d database %d has multiple tables %s",
			tbl.AccountId, tbl.DatabaseId, tbl.Name))
	}

	//get item
	for _, item := range inserted {
		copyTableItem(tbl, item)
	}

	return true
}

func (cc *CatalogCache) GetDatabase(db *DatabaseItem) bool {
	var find bool

	cc.databases.data.Ascend(db, func(item *DatabaseItem) bool {
		if !item.deleted && item.AccountId == db.AccountId &&
			item.Name == db.Name {
			find = true
			db.Id = item.Id
			db.Rowid = item.Rowid
			db.CreateSql = item.CreateSql
			db.Typ = item.Typ
		}
		return false
	})
	return find
}

func (cc *CatalogCache) DeleteTable(bat *batch.Batch) {
	rowids := vector.MustFixedCol[types.Rowid](bat.GetVector(MO_ROWID_IDX))
	timestamps := vector.MustFixedCol[types.TS](bat.GetVector(MO_TIMESTAMP_IDX))
	for i, rowid := range rowids {
		if item, ok := cc.tables.rowidIndex.Get(&TableItem{Rowid: rowid}); ok {
			newItem := &TableItem{
				deleted:    true,
				Id:         item.Id,
				Name:       item.Name,
				Rowid:      item.Rowid,
				AccountId:  item.AccountId,
				DatabaseId: item.DatabaseId,
				Ts:         timestamps[i].ToTimestamp(),
			}
			cc.tables.addTableItem(newItem)

			key := TableKey{
				AccountId:  item.AccountId,
				DatabaseId: item.DatabaseId,
				Name:       item.Name,
			}

			oldVersion := cc.tables.tableGuard.getSchemaVersion(key)

			if oldVersion != nil && oldVersion.TableId != item.Id {
				// drop old table for alter table stmt
				oldVersion.Version = math.MaxUint32
				cc.tables.tableGuard.setSchemaVersion(key, oldVersion)
			} else {
				// normal drop table stmt
				cc.tables.tableGuard.setSchemaVersion(key, &TableVersion{
					Version: math.MaxUint32,
					Ts:      &item.Ts,
				})
			}
		}
	}
}

func (cc *CatalogCache) DeleteDatabase(bat *batch.Batch) {
	rowids := vector.MustFixedCol[types.Rowid](bat.GetVector(MO_ROWID_IDX))
	timestamps := vector.MustFixedCol[types.TS](bat.GetVector(MO_TIMESTAMP_IDX))
	for i, rowid := range rowids {
		if item, ok := cc.databases.rowidIndex.Get(&DatabaseItem{Rowid: rowid}); ok {
			newItem := &DatabaseItem{
				deleted:   true,
				Id:        item.Id,
				Name:      item.Name,
				Rowid:     item.Rowid,
				AccountId: item.AccountId,
				Typ:       item.Typ,
				CreateSql: item.CreateSql,
				Ts:        timestamps[i].ToTimestamp(),
			}
			cc.databases.data.Set(newItem)
		}
	}
}

func (cc *CatalogCache) InsertTable(bat *batch.Batch) {
	rowids := vector.MustFixedCol[types.Rowid](bat.GetVector(MO_ROWID_IDX))
	timestamps := vector.MustFixedCol[types.TS](bat.GetVector(MO_TIMESTAMP_IDX))
	accounts := vector.MustFixedCol[uint32](bat.GetVector(catalog.MO_TABLES_ACCOUNT_ID_IDX + MO_OFF))
	names := bat.GetVector(catalog.MO_TABLES_REL_NAME_IDX + MO_OFF)
	ids := vector.MustFixedCol[uint64](bat.GetVector(catalog.MO_TABLES_REL_ID_IDX + MO_OFF))
	databaseIds := vector.MustFixedCol[uint64](bat.GetVector(catalog.MO_TABLES_RELDATABASE_ID_IDX + MO_OFF))
	kinds := bat.GetVector(catalog.MO_TABLES_RELKIND_IDX + MO_OFF)
	comments := bat.GetVector(catalog.MO_TABLES_REL_COMMENT_IDX + MO_OFF)
	createSqls := bat.GetVector(catalog.MO_TABLES_REL_CREATESQL_IDX + MO_OFF)
	viewDefs := bat.GetVector(catalog.MO_TABLES_VIEWDEF_IDX + MO_OFF)
	partitioneds := vector.MustFixedCol[int8](bat.GetVector(catalog.MO_TABLES_PARTITIONED_IDX + MO_OFF))
	paritions := bat.GetVector(catalog.MO_TABLES_PARTITION_INFO_IDX + MO_OFF)
	constraints := bat.GetVector(catalog.MO_TABLES_CONSTRAINT_IDX + MO_OFF)
	versions := vector.MustFixedCol[uint32](bat.GetVector(catalog.MO_TABLES_VERSION_IDX + MO_OFF))
	catalogVersions := vector.MustFixedCol[uint32](bat.GetVector(catalog.MO_TABLES_CATALOG_VERSION_IDX + MO_OFF))
	for i, account := range accounts {
		item := new(TableItem)
		item.Id = ids[i]
		item.Name = names.GetStringAt(i)
		item.AccountId = account
		item.DatabaseId = databaseIds[i]
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
		copy(item.Rowid[:], rowids[i][:])
		// invalid old name table
		exist, ok := cc.tables.rowidIndex.Get(&TableItem{Rowid: rowids[i]})
		if ok && exist.Name != item.Name {
			logutil.Infof("rename invalidate %d-%s,v%d@%s", exist.Id, exist.Name, exist.Version, item.Ts.String())
			newItem := &TableItem{
				deleted:    true,
				Id:         exist.Id,
				Name:       exist.Name,
				Rowid:      exist.Rowid,
				AccountId:  exist.AccountId,
				DatabaseId: exist.DatabaseId,
				Version:    exist.Version,
				Ts:         item.Ts,
			}
			cc.tables.addTableItem(newItem)

			key := TableKey{
				AccountId:  account,
				DatabaseId: item.DatabaseId,
				Name:       exist.Name,
			}
			cc.tables.tableGuard.setSchemaVersion(key, &TableVersion{
				Version: math.MaxUint32,
				Ts:      &item.Ts,
				TableId: item.Id,
			})
		}

		key := TableKey{
			AccountId:  account,
			DatabaseId: item.DatabaseId,
			Name:       item.Name,
		}

		cc.tables.tableGuard.setSchemaVersion(key, &TableVersion{
			Version: item.Version,
			Ts:      &item.Ts,
			TableId: item.Id,
		})
		cc.tables.addTableItem(item)
		cc.tables.rowidIndex.Set(item)
	}
}

func (cc *CatalogCache) InsertColumns(bat *batch.Batch) {
	var tblKey tableItemKey

	mp := make(map[tableItemKey]columns) // TableItem -> columns
	key := new(TableItem)
	rowids := vector.MustFixedCol[types.Rowid](bat.GetVector(MO_ROWID_IDX))
	// get table key info
	timestamps := vector.MustFixedCol[types.TS](bat.GetVector(MO_TIMESTAMP_IDX))
	accounts := vector.MustFixedCol[uint32](bat.GetVector(catalog.MO_COLUMNS_ACCOUNT_ID_IDX + MO_OFF))
	databaseIds := vector.MustFixedCol[uint64](bat.GetVector(catalog.MO_COLUMNS_ATT_DATABASE_ID_IDX + MO_OFF))
	tableNames := bat.GetVector(catalog.MO_COLUMNS_ATT_RELNAME_IDX + MO_OFF)
	tableIds := vector.MustFixedCol[uint64](bat.GetVector(catalog.MO_COLUMNS_ATT_RELNAME_ID_IDX + MO_OFF))
	// get columns info
	names := bat.GetVector(catalog.MO_COLUMNS_ATTNAME_IDX + MO_OFF)
	comments := bat.GetVector(catalog.MO_COLUMNS_ATT_COMMENT_IDX + MO_OFF)
	isHiddens := vector.MustFixedCol[int8](bat.GetVector(catalog.MO_COLUMNS_ATT_IS_HIDDEN_IDX + MO_OFF))
	isAutos := vector.MustFixedCol[int8](bat.GetVector(catalog.MO_COLUMNS_ATT_IS_AUTO_INCREMENT_IDX + MO_OFF))
	constraintTypes := bat.GetVector(catalog.MO_COLUMNS_ATT_CONSTRAINT_TYPE_IDX + MO_OFF)
	typs := bat.GetVector(catalog.MO_COLUMNS_ATTTYP_IDX + MO_OFF)
	hasDefs := vector.MustFixedCol[int8](bat.GetVector(catalog.MO_COLUMNS_ATTHASDEF_IDX + MO_OFF))
	defaultExprs := bat.GetVector(catalog.MO_COLUMNS_ATT_DEFAULT_IDX + MO_OFF)
	hasUpdates := vector.MustFixedCol[int8](bat.GetVector(catalog.MO_COLUMNS_ATT_HAS_UPDATE_IDX + MO_OFF))
	updateExprs := bat.GetVector(catalog.MO_COLUMNS_ATT_UPDATE_IDX + MO_OFF)
	nums := vector.MustFixedCol[int32](bat.GetVector(catalog.MO_COLUMNS_ATTNUM_IDX + MO_OFF))
	clusters := vector.MustFixedCol[int8](bat.GetVector(catalog.MO_COLUMNS_ATT_IS_CLUSTERBY + MO_OFF))
	seqnums := vector.MustFixedCol[uint16](bat.GetVector(catalog.MO_COLUMNS_ATT_SEQNUM_IDX + MO_OFF))
	enumValues := bat.GetVector(catalog.MO_COLUMNS_ATT_ENUM_IDX + MO_OFF)
	for i, account := range accounts {
		key.AccountId = account
		key.Name = tableNames.GetStringAt(i)
		key.DatabaseId = databaseIds[i]
		key.Ts = timestamps[i].ToTimestamp()
		key.Id = tableIds[i]
		tblKey.Name = key.Name
		tblKey.AccountId = key.AccountId
		tblKey.DatabaseId = key.DatabaseId
		tblKey.NodeId = key.Ts.NodeID
		tblKey.LogicalTime = key.Ts.LogicalTime
		tblKey.PhysicalTime = uint64(key.Ts.PhysicalTime)
		tblKey.Id = tableIds[i]
		if _, ok := cc.tables.data.Get(key); ok {
			col := column{
				num:             nums[i],
				name:            names.GetStringAt(i),
				comment:         comments.GetStringAt(i),
				isHidden:        isHiddens[i],
				isAutoIncrement: isAutos[i],
				hasDef:          hasDefs[i],
				hasUpdate:       hasUpdates[i],
				constraintType:  constraintTypes.GetStringAt(i),
				isClusterBy:     clusters[i],
				seqnum:          seqnums[i],
				enumValues:      enumValues.GetStringAt(i),
			}
			copy(col.rowid[:], rowids[i][:])
			col.typ = append(col.typ, typs.GetBytesAt(i)...)
			col.updateExpr = append(col.updateExpr, updateExprs.GetBytesAt(i)...)
			col.defaultExpr = append(col.defaultExpr, defaultExprs.GetBytesAt(i)...)
			mp[tblKey] = append(mp[tblKey], col)
		}
	}
	for k, cols := range mp {
		sort.Sort(cols)
		key.Name = k.Name
		key.AccountId = k.AccountId
		key.DatabaseId = k.DatabaseId
		key.Ts = timestamp.Timestamp{
			NodeID:       k.NodeId,
			PhysicalTime: int64(k.PhysicalTime),
			LogicalTime:  k.LogicalTime,
		}
		key.Id = k.Id
		item, _ := cc.tables.data.Get(key)
		defs := make([]engine.TableDef, 0, len(cols))
		defs = append(defs, genTableDefOfComment(item.Comment))
		item.Rowids = make([]types.Rowid, len(cols))
		for i, col := range cols {
			if col.constraintType == catalog.SystemColPKConstraint {
				item.PrimaryIdx = i
				item.PrimarySeqnum = int(col.seqnum)
			}
			if col.isClusterBy == 1 {
				item.ClusterByIdx = i
			}
			defs = append(defs, genTableDefOfColumn(col))
			copy(item.Rowids[i][:], col.rowid[:])
		}
		item.Defs = defs
		item.TableDef = getTableDef(item, defs)
	}
}

func (cc *CatalogCache) InsertDatabase(bat *batch.Batch) {
	rowids := vector.MustFixedCol[types.Rowid](bat.GetVector(MO_ROWID_IDX))
	timestamps := vector.MustFixedCol[types.TS](bat.GetVector(MO_TIMESTAMP_IDX))
	accounts := vector.MustFixedCol[uint32](bat.GetVector(catalog.MO_DATABASE_ACCOUNT_ID_IDX + MO_OFF))
	names := bat.GetVector(catalog.MO_DATABASE_DAT_NAME_IDX + MO_OFF)
	ids := vector.MustFixedCol[uint64](bat.GetVector(catalog.MO_DATABASE_DAT_ID_IDX + MO_OFF))
	typs := bat.GetVector(catalog.MO_DATABASE_DAT_TYPE_IDX + MO_OFF)
	createSqls := bat.GetVector(catalog.MO_DATABASE_CREATESQL_IDX + MO_OFF)
	for i, account := range accounts {
		item := new(DatabaseItem)
		item.Id = ids[i]
		item.Name = names.GetStringAt(i)
		item.AccountId = account
		item.Ts = timestamps[i].ToTimestamp()
		item.Typ = typs.GetStringAt(i)
		item.CreateSql = createSqls.GetStringAt(i)
		copy(item.Rowid[:], rowids[i][:])
		cc.databases.data.Set(item)
		cc.databases.rowidIndex.Set(item)
	}
}

func genTableDefOfComment(comment string) engine.TableDef {
	return &engine.CommentDef{
		Comment: comment,
	}
}

func genTableDefOfColumn(col column) engine.TableDef {
	var attr engine.Attribute

	attr.Name = col.name
	attr.ID = uint64(col.num)
	attr.Alg = compress.Lz4
	attr.Comment = col.comment
	attr.IsHidden = col.isHidden == 1
	attr.ClusterBy = col.isClusterBy == 1
	attr.AutoIncrement = col.isAutoIncrement == 1
	attr.Seqnum = col.seqnum
	attr.EnumVlaues = col.enumValues
	if err := types.Decode(col.typ, &attr.Type); err != nil {
		panic(err)
	}
	attr.Default = new(plan.Default)
	if col.hasDef == 1 {
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
	return &engine.AttributeDef{Attr: attr}
}

/*
// getTableDef only return all cols and their index.
func getTableDef(name string, defs []engine.TableDef) *plan.TableDef {
	var cols []*plan.ColDef

	i := int32(0)
	name2index := make(map[string]int32)
	for _, def := range defs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			name2index[attr.Attr.Name] = i
			cols = append(cols, &plan.ColDef{
				ColId: attr.Attr.ID,
				Name:  attr.Attr.Name,
				Typ: &plan.Type{
					Id:         int32(attr.Attr.Type.Oid),
					Width:      attr.Attr.Type.Width,
					Scale:      attr.Attr.Type.Scale,
					AutoIncr:   attr.Attr.AutoIncrement,
					Enumvalues: attr.Attr.EnumVlaues,
				},
				Primary:  attr.Attr.Primary,
				Default:  attr.Attr.Default,
				OnUpdate: attr.Attr.OnUpdate,
				Comment:  attr.Attr.Comment,
				Hidden:   attr.Attr.IsHidden,
				Seqnum:   uint32(attr.Attr.Seqnum),
			})
			i++
		}
	}
	return &plan.TableDef{
		Name:          name,
		Cols:          cols,
		Name2ColIndex: name2index,
	}
}
*/

// GetSchemaVersion returns the version of table
func (cc *CatalogCache) GetSchemaVersion(name TableKey) *TableVersion {
	return cc.tables.tableGuard.getSchemaVersion(name)
}

// addTableItem inserts a new table item.
func (c *tableCache) addTableItem(item *TableItem) {
	c.data.Set(item)
}

func getTableDef(tblItem *TableItem, coldefs []engine.TableDef) *plan.TableDef {
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

	i := int32(0)
	name2index := make(map[string]int32)
	for _, def := range coldefs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			name2index[attr.Attr.Name] = i
			cols = append(cols, &plan.ColDef{
				ColId: attr.Attr.ID,
				Name:  attr.Attr.Name,
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
					Name: attr.Attr.Name,
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
	}

	if tblItem.Partitioned > 0 {
		p := &plan.PartitionByDef{}
		err := p.UnMarshalPartitionInfo(([]byte)(tblItem.Partition))
		if err != nil {
			//panic(fmt.Sprintf("cannot unmarshal partition metadata information: %s", err))
			return nil
		}
		partitionInfo = p
	}

	if tblItem.ViewDef != "" {
		viewSql = &plan.ViewDef{
			View: tblItem.ViewDef,
		}
	}

	if len(tblItem.Constraint) > 0 {
		c := &engine.ConstraintDef{}
		err := c.UnmarshalBinary(tblItem.Constraint)
		if err != nil {
			//panic(fmt.Sprintf("cannot unmarshal table constraint information: %s", err))
			return nil
		}
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

	if tblItem.CreateSql != "" {
		properties = append(properties, &plan.Property{
			Key:   catalog.SystemRelAttr_CreateSQL,
			Value: tblItem.CreateSql,
		})
		Createsql = tblItem.CreateSql
	}

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
	}
}
