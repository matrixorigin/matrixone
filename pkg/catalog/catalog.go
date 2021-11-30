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

package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixcube/server"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/pb"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/codec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/helper"
)

const (
	defaultCatalogId    = uint64(1)
	cPrefix             = "meta"
	cDBPrefix           = "DBINFO"
	cDBIDPrefix         = "DBID"
	cTablePrefix        = "Table"
	cTableIDPrefix      = "TID"
	cRoutePrefix        = "Route"
	cDeletedTablePrefix = "DeletedTableQueue"
	timeout             = 2000 * time.Millisecond
	idPoolSize          = 20
)

// Catalog is for handling meta information in a query.
type Catalog struct {
	Driver    driver.CubeDriver
	dbIdStart uint64
	dbIdEnd   uint64
	tidStart  uint64
	tidEnd    uint64
	pLock     int32
}

// NewCatalog creates a Catalog.
func NewCatalog(store driver.CubeDriver) *Catalog {
	catalog := Catalog{
		Driver: store,
	}
	tmpId, err := store.AllocID(codec.String2Bytes(cDBIDPrefix), idPoolSize)
	if err != nil {
		panic(fmt.Sprintf("init db id pool failed. %v", err))
	}
	catalog.dbIdEnd = tmpId
	catalog.dbIdStart = tmpId - idPoolSize + 1
	tmpId, err = store.AllocID(codec.String2Bytes(cTableIDPrefix), idPoolSize)
	if err != nil {
		panic(fmt.Sprintf("init table id pool failed. %v", err))
	}
	catalog.tidEnd = tmpId
	catalog.tidStart = tmpId - idPoolSize + 1
	logutil.Debugf("Catalog initialize finished, db id range is [%d, %d), table id range is [%d, %d)", catalog.dbIdStart, catalog.dbIdEnd, catalog.tidStart, catalog.tidEnd)

	return &catalog
}

// CreateDatabase creates a database with db info.
func (c *Catalog) CreateDatabase(epoch uint64, dbName string, typ int) (dbid uint64, err error) {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("CreateDatabase finished, dbname is %v, db id is %d, cost %d ms", dbName, dbid, time.Since(t0).Milliseconds())
	}()
	if _, err := c.checkDBNotExists(dbName); err != nil {
		return 0, err
	}
	dbid, err = c.allocId(cDBIDPrefix)
	if err != nil {
		return 0, err
	}
	if err = c.Driver.SetIfNotExist(c.dbIDKey(dbName), codec.Uint642Bytes(dbid)); err != nil {
		return 0, err
	}
	info := aoe.SchemaInfo{
		State:     aoe.StatePublic,
		Name:      dbName,
		Id:        dbid,
		CatalogId: 1,
		Type:      typ,
	}
	value, _ := json.Marshal(info)
	if err = c.Driver.Set(c.dbKey(dbid), value); err != nil {
		return 0, err
	}
	return dbid, nil
}

// DropDatabase drops whole database.
// it will set schema status in meta of this database and all tables in this database to "DeleteOnly"
func (c *Catalog) DropDatabase(epoch uint64, dbName string) (err error) {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("DropDatabase cost %d ms", time.Since(t0).Milliseconds())
	}()
	if db, _ := c.checkDBNotExists(dbName); db == nil {
		return ErrDBNotExists
	} else {
		if err = c.dropTables(epoch, db.Id); err != nil {
			return err
		}
		if err = c.Driver.Delete(c.dbKey(db.Id)); err != nil {
			return err
		}
		err = c.Driver.Delete(c.dbIDKey(dbName))
		return err
	}
}

// ListDatabases returns all databases.
func (c *Catalog) ListDatabases() ([]aoe.SchemaInfo, error) {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("ListDatabases cost %d ms", time.Since(t0).Milliseconds())
	}()
	values, err := c.Driver.PrefixScan(c.dbPrefix(), 0)
	if err != nil {
		return nil, err
	}
	var dbs []aoe.SchemaInfo

	for i := 1; i < len(values); i = i + 2 {
		db := aoe.SchemaInfo{}
		_ = json.Unmarshal(values[i], &db)
		if db.State != aoe.StatePublic {
			continue
		}
		dbs = append(dbs, db)
	}
	return dbs, nil
}

// GetDatabase gets the database meta with Name.
func (c *Catalog) GetDatabase(dbName string) (*aoe.SchemaInfo, error) {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("GetDatabase cost %d ms", time.Since(t0).Milliseconds())
	}()
	db, _ := c.checkDBNotExists(dbName)
	if db == nil {
		return nil, ErrDBNotExists
	}
	if db.State != aoe.StatePublic {
		return nil, ErrDBNotExists
	}
	return db, nil
}
func (c *Catalog) GetPrimaryKey(dbId uint64, tableName string) (pk *aoe.ColumnInfo, err error) {
	tbl, err := c.GetTable(dbId, tableName)
	if err != nil {
		return
	}

	for _, col := range tbl.Columns {
		if col.PrimaryKey {
			return &col, nil
		}
	}

	return nil, ErrPrimaryKeyNotExist
}
func (c *Catalog) DeletePrimaryKey(epoch, dbId uint64, tableName string) (err error) {
	tbl, err := c.GetTable(dbId, tableName)
	if err != nil {
		return
	}
	for _, col := range tbl.Columns {
		if col.PrimaryKey {
			col.PrimaryKey = false
			c.updateTableInfo(dbId, tbl)
			return
		}
	}
	return
}
func (c *Catalog) SetPrimaryKey(epoch, dbId uint64, tableName, columnName string) (err error) {
	tbl, err := c.GetTable(dbId, tableName)
	if err != nil {
		return
	}
	columnExist := false
	for _, col := range tbl.Columns {
		if col.PrimaryKey {
			if col.Name == columnName {
				return nil
			}
			col.PrimaryKey = false
		}
		if col.Name == columnName {
			col.PrimaryKey = true
		}
	}
	c.updateTableInfo(dbId, tbl)
	if columnExist {
		return nil
	}
	return ErrColumnNotExist
}
func (c *Catalog) updateTableInfo(dbId uint64, tbl *aoe.TableInfo) (err error) {
	meta, err := helper.EncodeTable(*tbl)
	if err != nil {
		return err
	}
	err = c.Driver.Set(c.tableKey(dbId, tbl.Id), meta)
	return err
}

// func (c *Catalog) UpdatePrimaryKey(epoch, dbId uint64, tableName, columnName string) (err error) {
// 	return
// }

// func (c *Catalog) IsPrimaryKey(epoch, dbId uint64, tableName, columnName string) (isPrimaryKey bool, err error) {
// 	return
// }

// CreateTable creates a table with tableInfo in database.
func (c *Catalog) CreateTable(epoch, dbId uint64, tbl aoe.TableInfo) (tid uint64, err error) {
	t0 := time.Now()
	defer func() {
		if err != nil && err != ErrTableCreateExists {
			if serr := c.Driver.Delete(c.tableIDKey(dbId, tbl.Name)); serr != nil {
				//TODO: need strategy handle this situation
				logutil.Errorf("delete meta for uncreated table, %v, %v, %v", dbId, tbl, serr)
			}
		}
		logutil.Debugf("CreateTable finished, table name is %v, table id is %d, cost %d ms", tbl.Name, tid, time.Since(t0).Milliseconds())
	}()
	_, err = c.checkDBExists(dbId)
	if err != nil {
		return tid, err
	}
	tid, err = c.allocId(cTableIDPrefix)
	if err != nil {
		return tid, err
	}
	tbl.Id = tid
	if err = c.Driver.SetIfNotExist(c.tableIDKey(dbId, tbl.Name), codec.Uint642Bytes(tbl.Id)); err != nil {
		return tid, ErrTableCreateExists
	}

	wg := sync.WaitGroup{}
	tbl.Epoch = epoch
	tbl.SchemaId = dbId
	if shardId, err := c.getAvailableShard(tbl.Id); err == nil {
		rkey := c.routeKey(dbId, tbl.Id, shardId)
		tableName := tbl.Name
		aoeTableName := c.encodeTabletName(shardId, tbl.Id)
		if err := c.Driver.CreateTablet(aoeTableName, shardId, &tbl); err != nil {
			logutil.Errorf("ErrTableCreateFailed, %v, %v, %v", shardId, tbl, err)
			return tid, ErrTabletCreateFailed
		}
		tbl.Name = tableName
		tbl.State = aoe.StatePublic
		meta, err := helper.EncodeTable(tbl)
		if err != nil {
			logutil.Errorf("ErrTableCreateFailed, %v", err)
			return tid, err
		}
		wg.Add(1)
		c.Driver.AsyncSet(rkey, []byte(tbl.Name), func(i server.CustomRequest, bytes []byte, rerr error) {
			defer wg.Done()
			if rerr != nil {
				err = rerr
				return
			}
		}, nil)
		wg.Add(1)
		c.Driver.AsyncSet(c.tableKey(dbId, tbl.Id), meta, func(i server.CustomRequest, bytes []byte, rerr error) {
			defer wg.Done()
			if rerr != nil {
				err = rerr
				return
			}
		}, nil)
		wg.Wait()

		return tbl.Id, err
	}
	return tid, ErrNoAvailableShard
}

// DropTable drops table in database.
// it will set schema status in meta of this table to "DeleteOnly"
func (c *Catalog) DropTable(epoch, dbId uint64, tableName string) (tid uint64, err error) {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("DropTable cost %d ms", time.Since(t0).Milliseconds())
	}()
	_, err = c.checkDBExists(dbId)
	if err != nil {
		return tid, err
	}
	tb, err := c.checkTableNotExists(dbId, tableName)
	if tb == nil {
		return tid, ErrTableNotExists
	}
	tid = tb.Id
	tb.State = aoe.StateDeleteOnly
	tb.Epoch = epoch
	value, _ := helper.EncodeTable(*tb)
	if err = c.Driver.Set(c.deletedTableKey(epoch, dbId, tb.Id), value); err != nil {
		return tid, err
	}
	if err = c.Driver.Delete(c.tableIDKey(dbId, tableName)); err != nil {
		return tid, err
	}
	if err = c.Driver.Delete(c.tableKey(dbId, tb.Id)); err != nil {
		return tid, err
	}
	return tb.Id, err
}

// ListTablesByName returns all tables meta in database.
func (c *Catalog) ListTablesByName(dbName string) ([]aoe.TableInfo, error) {
	if value, err := c.Driver.Get(c.dbIDKey(dbName)); err != nil || value == nil {
		return nil, ErrDBNotExists
	} else {
		id, _ := codec.Bytes2Uint64(value)
		return c.ListTables(id)
	}
}

//CreateIndex create an index
func (c *Catalog) CreateIndex(epoch uint64, idxInfo aoe.IndexInfo) error {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("CreateIndex cost %d ms", time.Since(t0).Milliseconds())
	}()
	_, err := c.checkDBExists(idxInfo.SchemaId)
	if err != nil {
		return err
	}
	tbl, err := c.checkTableExists(idxInfo.SchemaId, idxInfo.TableId)
	if err != nil {
		return err
	}
	//TODO
	for _, idx := range idxInfo.ColumnNames {
		for _, col := range tbl.Columns {
			if idx == col.Name {
				idxInfo.Columns = append(idxInfo.Columns, col.Id)
			}
		}
	}
	for _, indice := range tbl.Indices {
		if indice.Name == idxInfo.Name {
			return ErrIndexExist
		}
	}
	tbl.Epoch = epoch
	tbl.Indices = append(tbl.Indices, idxInfo)
	err = c.updateTableInfo(idxInfo.SchemaId, tbl)
	return err
}

//DropIndex drops an index
func (c *Catalog) DropIndex(epoch, tid, dbid uint64, idxName string) error {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("DropIndex cost %d ms", time.Since(t0).Milliseconds())
	}()
	_, err := c.checkDBExists(dbid)
	if err != nil {
		return err
	}
	tbl, err := c.checkTableExists(dbid, tid)
	if err != nil {
		return err
	}
	for i, indice := range tbl.Indices {
		if indice.Name == idxName {
			tbl.Epoch = epoch
			tbl.Indices = append(tbl.Indices[:i], tbl.Indices[i+1:]...)
			err = c.updateTableInfo(dbid, tbl)
			return err
		}
	}
	return ErrIndexNotExist
}

// ListTables returns all tables meta in database.
func (c *Catalog) ListTables(dbId uint64) ([]aoe.TableInfo, error) {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("ListTables cost %d ms", time.Since(t0).Milliseconds())
	}()
	if _, err := c.checkDBExists(dbId); err != nil {
		return nil, err
	} else {
		values, err := c.Driver.PrefixScan(c.tablePrefix(dbId), 0)
		if err != nil {
			logutil.Errorf("Call ListTables failed %v", err)
			return nil, err
		}
		var tables []aoe.TableInfo
		for i := 1; i < len(values); i = i + 2 {
			t, _ := helper.DecodeTable(values[i])
			if t.State != aoe.StatePublic {
				continue
			}
			tables = append(tables, t)
		}
		return tables, nil
	}
}

// GetTable gets the table meta in database with dbId and tableName.
func (c *Catalog) GetTable(dbId uint64, tableName string) (*aoe.TableInfo, error) {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("GetTable cost %d ms", time.Since(t0).Milliseconds())
	}()
	if _, err := c.checkDBExists(dbId); err != nil {
		return nil, err
	} else {
		tb, _ := c.checkTableNotExists(dbId, tableName)
		if tb == nil {
			logutil.Errorf("123")
			return nil, ErrTableNotExists
		}
		if tb.State != aoe.StatePublic {
			logutil.Errorf("456")
			return nil, ErrTableNotExists
		}
		return tb, nil
	}
}

// GetTablets gets all the tablets of the table in database with dbId and tableName.
func (c *Catalog) GetTablets(dbId uint64, tableName string) (tablets []aoe.TabletInfo, err error) {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("GetTablets return %d tablets, cost %d ms", len(tablets), time.Since(t0).Milliseconds())
	}()
	if _, err := c.checkDBExists(dbId); err != nil {
		return nil, err
	} else {
		tb, _ := c.checkTableNotExists(dbId, tableName)
		if tb == nil {
			return nil, ErrTableNotExists
		}
		if tb.State != aoe.StatePublic {
			return nil, ErrTableNotExists
		}
		shardIds, err := c.Driver.PrefixKeys(c.routePrefix(dbId, tb.Id), 0)
		if err != nil {
			return nil, err
		}
		for _, shardId := range shardIds {
			if sid, err := codec.Bytes2Uint64(shardId[len(c.routePrefix(dbId, tb.Id)):]); err != nil {
				logutil.Errorf("convert shardid failed, %v, shardid is %d, prefix length is %d", err, len(shardId), len(c.routePrefix(dbId, tb.Id)))
				continue
			} else {
				tablets = append(tablets, aoe.TabletInfo{
					Name:    c.encodeTabletName(sid, tb.Id),
					ShardId: sid,
					Table:   *tb,
				})
			}
		}
		return tablets, nil
	}
}

// RemoveDeletedTable trigger gc
// TODO: handle duplicated remove
func (c *Catalog) RemoveDeletedTable(epoch uint64) (cnt int, err error) {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("[RemoveDeletedTable] epoch is %d, removed %d tables, cost %d ms", epoch, cnt, time.Since(t0).Milliseconds())
	}()
	rsp, err := c.Driver.Scan(c.deletedPrefix(), c.deletedEpochPrefix(epoch+1), 0)
	if err != nil {
		logutil.Errorf("scan error, %v", err)
		return cnt, err
	}
	for i := 1; i < len(rsp); i += 2 {
		if tbl, err := helper.DecodeTable(rsp[i]); err != nil {
			logutil.Errorf("Decode err for table info, %v, %v", err, rsp[i])
			continue
		} else {
			shardIds, err := c.Driver.PrefixKeys(c.routePrefix(tbl.SchemaId, tbl.Id), 0)
			if err != nil {
				logutil.Errorf("Failed to get shards for table %v, %v", rsp[i], err)
				continue
			}
			success := true
			for _, shardId := range shardIds {
				if sid, err := codec.Bytes2Uint64(shardId[len(c.routePrefix(tbl.SchemaId, tbl.Id)):]); err != nil {
					logutil.Errorf("convert shardid failed, %v", err)
					success = false
					break
				} else {
					_, err = c.Driver.DropTablet(c.encodeTabletName(sid, tbl.Id), sid)
					if err != nil {
						logutil.Errorf("call local drop table failed %d, %d, %v", sid, tbl.Id, err)
						success = false
						break
					}
				}
			}
			if success {
				if c.Driver.Delete(c.deletedTableKey(tbl.Epoch, tbl.SchemaId, tbl.Id)) != nil {
					logutil.Errorf("remove marked deleted tableinfo failed, %v, %v", err, tbl)
				} else {
					cnt++
				}
			}
		}
	}
	return cnt, nil
}

//checkDBExists checks whether db exists.
//If the db exists and its state is not aoe.StateDeleteOnly, checkDBExists returns the db.
//If else, checkDBExists returns ErrDBNotExists.
func (c *Catalog) checkDBExists(id uint64) (*aoe.SchemaInfo, error) {
	db := aoe.SchemaInfo{}
	if v, err := c.Driver.Get(c.dbKey(id)); err != nil {
		return nil, ErrDBNotExists
	} else {
		if err = json.Unmarshal(v, &db); err != nil {
			return nil, ErrDBNotExists
		}
		if db.State == aoe.StateDeleteOnly {
			return nil, ErrDBNotExists
		}
	}
	return &db, nil
}

//dropTables drops all tables in the database whose id is dbId.
//The state of droped tables is set as aoe.StateDeleteOnly
func (c *Catalog) dropTables(epoch, dbId uint64) (err error) {
	_, err = c.checkDBExists(dbId)
	if err != nil {
		return err
	}
	tbs, err := c.ListTables(dbId)
	if err != nil {
		return err
	}
	if tbs == nil || len(tbs) == 0 {
		return nil
	}
	for _, tbl := range tbs {
		tbl.State = aoe.StateDeleteOnly
		tbl.Epoch = epoch
		value, _ := helper.EncodeTable(tbl)
		if err = c.Driver.Set(c.deletedTableKey(epoch, dbId, tbl.Id), value); err != nil {
			return err
		}
		if err = c.Driver.Delete(c.tableIDKey(dbId, tbl.Name)); err != nil {
			return err
		}
		if err = c.Driver.Delete(c.tableKey(dbId, tbl.Id)); err != nil {
			return err
		}
	}
	return err
}

//checkDBNotExists checks wherher the database exists by calling checkDBExists.
//If the database exists, it returns the database and ErrDBCreateExists.
//If not, it returns nil.
func (c *Catalog) checkDBNotExists(dbName string) (*aoe.SchemaInfo, error) {
	if value, err := c.Driver.Get(c.dbIDKey(dbName)); err != nil || value == nil {
		return nil, nil
	} else {
		id, _ := codec.Bytes2Uint64(value)
		db, err := c.checkDBExists(id)
		if err == ErrDBNotExists {
			return nil, nil
		}
		return db, ErrDBCreateExists
	}
}

//checkTableExists checks whether the table exists in the database.
//If the table exists and its state is not aoe.StateDeleteOnly, it returns the table.
//If else, it returns ErrTableNotExists.
func (c *Catalog) checkTableExists(dbId, id uint64) (*aoe.TableInfo, error) {
	if v, err := c.Driver.Get(c.tableKey(dbId, id)); err != nil {
		return nil, ErrTableNotExists
	} else {
		if table, err := helper.DecodeTable(v); err != nil {
			return nil, ErrTableNotExists
		} else {
			if table.State == aoe.StateDeleteOnly {
				return nil, ErrTableNotExists
			}
			return &table, nil
		}
	}
}

//checkTableNotExists checks whether the table exists in the database by calling checkTableExists.
//If the table exists, it returns the table and ErrTableCreateExists.
//If not, it returns nil.
func (c *Catalog) checkTableNotExists(dbId uint64, tableName string) (*aoe.TableInfo, error) {
	if value, err := c.Driver.Get(c.tableIDKey(dbId, tableName)); err != nil || value == nil {
		return nil, nil
	} else {
		id, _ := codec.Bytes2Uint64(value)
		tb, err := c.checkTableExists(dbId, id)
		if err == ErrTableNotExists {
			return nil, nil
		}
		return tb, ErrTableCreateExists
	}
}

//encodeTabletName encodes the groupId(the id of the shard) and tableId together to one string by calling codec.Bytes2String.
func (c *Catalog) encodeTabletName(groupId, tableId uint64) string {
	return strconv.Itoa(int(groupId)) + strconv.Itoa(int(tableId))
}

//genGlobalUniqIDs generates a global unique id by calling c.Driver.AllocID.
func (c *Catalog) genGlobalUniqIDs(idKey []byte) (uint64, error) {
	id, err := c.Driver.AllocID(idKey, 1)
	if err != nil {
		return 0, err
	}
	return id, nil
}

//dbIDKey returns encoded dbName with prefix "meta1DBID"
func (c *Catalog) dbIDKey(dbName string) []byte {
	return codec.EncodeKey(cPrefix, defaultCatalogId, cDBIDPrefix, dbName)
}

//dbKey returns encoded id with prefix "meta1DBINFO"
func (c *Catalog) dbKey(id uint64) []byte {
	return codec.EncodeKey(cPrefix, defaultCatalogId, cDBPrefix, id)
}

//dbPrefix returns the prefix "meta1DBINFO"
func (c *Catalog) dbPrefix() []byte {
	return codec.EncodeKey(cPrefix, defaultCatalogId, cDBPrefix)
}

//tableIDKey returns the encoded tableName with prefix "meta1TID$dbId$"
func (c *Catalog) tableIDKey(dbId uint64, tableName string) []byte {
	return codec.EncodeKey(cPrefix, defaultCatalogId, cTableIDPrefix, dbId, tableName)
}

//tableKey returns the encoded tID with prefix "meta1Table$dbId$"
func (c *Catalog) tableKey(dbId, tId uint64) []byte {
	return codec.EncodeKey(cPrefix, defaultCatalogId, cTablePrefix, dbId, tId)
}

//tablePrefix returns the prefix "meta1Table$dbId$"
func (c *Catalog) tablePrefix(dbId uint64) []byte {
	return codec.EncodeKey(cPrefix, defaultCatalogId, cTablePrefix, dbId)
}

//routeKey returns the encoded gId with prefix "meta1Route$dbId$$tId$"
func (c *Catalog) routeKey(dbId, tId, gId uint64) []byte {
	return codec.EncodeKey(cPrefix, defaultCatalogId, cRoutePrefix, dbId, tId, gId)
}

//routePrefix returns the prefix "meta1Route$dbId$$tId"
func (c *Catalog) routePrefix(dbId, tId uint64) []byte {
	return codec.EncodeKey(cPrefix, defaultCatalogId, cRoutePrefix, dbId, tId)
}

//deletedTableKey returns the encoded tId with the prefix "DeletedTableQueue$epoch$$dbId$"
func (c *Catalog) deletedTableKey(epoch, dbId, tId uint64) []byte {
	return codec.EncodeKey(cDeletedTablePrefix, epoch, dbId, tId)
}

//deletedEpochPrefix returns the prefix "DeletedTableQueue$epoch$"
func (c *Catalog) deletedEpochPrefix(epoch uint64) []byte {
	return codec.EncodeKey(cDeletedTablePrefix, epoch)
}

//deletedPrefix returns the prefix "DeletedTableQueue"
func (c *Catalog) deletedPrefix() []byte {
	return codec.EncodeKey(cDeletedTablePrefix)
}

//getAvailableShard get a shard from the shard pool and returns its id.
func (c *Catalog) getAvailableShard(tid uint64) (shardid uint64, err error) {
	t0 := time.Now()
	defer func() {
		logutil.Infof("[getAvailableShard] get shard for %d, returns %d, %v, cost %d ms", tid, shardid, err, time.Since(t0).Milliseconds())
	}()
	timeoutC := time.After(timeout)
	for {
		select {
		case <-timeoutC:
			logutil.Error("wait for available shard timeout")
			return shardid, ErrTableCreateTimeout
		default:
			shard, err := c.Driver.GetShardPool().Alloc(uint64(pb.AOEGroup), codec.Uint642Bytes(tid))
			if err == nil {
				return shard.ShardID, err
			}
			time.Sleep(time.Millisecond * 10)
		}
	}
}

//allocId alloc an id from id cache
func (c *Catalog) allocId(key string) (id uint64, err error) {
	defer func() {
		logutil.Debugf("allocId finished, idKey is %v, id is %d, err is %v", key, id, err)
	}()
	timeoutC := time.After(timeout)
	switch key {
	case cDBIDPrefix:
		func() {
			for {
				select {
				case <-timeoutC:
					logutil.Error("wait for available id timeout")
					err = ErrTableCreateTimeout
					return
				default:
					if atomic.LoadInt32(&c.pLock) == 0 {
						id = atomic.AddUint64(&c.dbIdStart, 1) - 1
						if id <= atomic.LoadUint64(&c.dbIdEnd) {
							logutil.Debugf("alloc db id finished, id is %d, endId is %d", id, c.dbIdEnd)
							return
						} else {
							c.refreshDBIDCache()
						}
					}
					time.Sleep(time.Millisecond * 10)
				}
			}
		}()
	case cTableIDPrefix:
		func() {
			for {
				select {
				case <-timeoutC:
					logutil.Errorf("wait for available tid timeout, current cache range is [%d, %d)", c.tidStart, c.tidEnd)
					err = ErrTableCreateTimeout
					return
				default:
					if atomic.LoadInt32(&c.pLock) == 0 {
						id = atomic.AddUint64(&c.tidStart, 1) - 1
						if id <= atomic.LoadUint64(&c.tidEnd) {
							logutil.Debugf("alloc table id finished, id is %d, endId is %d", id, c.tidEnd)
							return
						} else {
							c.refreshTableIDCache()
						}
					}
					time.Sleep(time.Millisecond * 10)
				}
			}
		}()
	default:
		return id, errors.New("unsupported id category")
	}
	return id, err
}

//refreshTableIDCache alloc table ids and refresh tidStart and tidEnd.
func (c *Catalog) refreshTableIDCache() {
	if !atomic.CompareAndSwapInt32(&c.pLock, 0, 1) {
		fmt.Println("failed to acquired pLock")
		return
	}
	t0 := time.Now()
	defer func() {
		atomic.StoreInt32(&c.pLock, 0)
		logutil.Debugf("refresh table id cache finished, cost %d, new range is [%d, %d)", time.Since(t0).Milliseconds(), c.tidStart, c.tidEnd)
	}()
	if c.tidStart <= c.tidEnd {
		logutil.Debugf("enter refreshTableIDCache, no need, return, [%d, %d)", c.tidStart, c.tidEnd)
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	c.Driver.AsyncAllocID(codec.String2Bytes(cTableIDPrefix), idPoolSize, func(i server.CustomRequest, data []byte, err error) {
		defer wg.Done()
		if err != nil {
			logutil.Errorf("refresh table id failed, checkpoint is %d, %d", c.tidStart, c.tidEnd)
			return
		}
		id, err := codec.Bytes2Uint64(data)
		if err != nil {
			logutil.Errorf("get result of AllocId failed, %v\n", err)
			return
		}

		atomic.SwapUint64(&c.tidEnd, id)
		atomic.SwapUint64(&c.tidStart, id-idPoolSize+1)
	}, nil)
	wg.Wait()
}

//refreshDBIDCache alloc database ids and refresh dbIdStart and dbIdEnd.
func (c *Catalog) refreshDBIDCache() {
	if !atomic.CompareAndSwapInt32(&c.pLock, 0, 1) {
		fmt.Println("failed to acquired pLock")
		return
	}
	t0 := time.Now()
	defer func() {
		atomic.StoreInt32(&c.pLock, 0)
		logutil.Debugf("refresh db id cache finished, cost %d, new range is [%d, %d)", time.Since(t0).Milliseconds(), c.dbIdStart, c.dbIdEnd)
	}()
	if c.dbIdStart <= c.dbIdEnd {
		logutil.Debugf("enter refreshDBIDCache, no need, return, [%d, %d)", c.dbIdStart, c.dbIdEnd)
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	c.Driver.AsyncAllocID(codec.String2Bytes(cDBIDPrefix), idPoolSize, func(i server.CustomRequest, data []byte, err error) {
		defer wg.Done()
		if err != nil {
			logutil.Errorf("refresh db id failed, checkpoint is %d, %d", c.dbIdStart, c.dbIdEnd)
			return
		}
		id, err := codec.Bytes2Uint64(data)
		if err != nil {
			logutil.Errorf("get result of AllocId failed, %v\n", err)
			return
		}

		atomic.SwapUint64(&c.dbIdEnd, id)
		atomic.SwapUint64(&c.dbIdStart, id-idPoolSize+1)
	}, nil)
	wg.Wait()
}
