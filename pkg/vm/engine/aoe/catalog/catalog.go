package catalog

import (
	"encoding/json"
	"fmt"
	"matrixone/pkg/logger"
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/engine/aoe/common/codec"
	"matrixone/pkg/vm/engine/aoe/common/helper"
	"matrixone/pkg/vm/engine/aoe/dist"
	"matrixone/pkg/vm/engine/aoe/dist/pb"
	"os"
	"sync"
	"time"
)

const (
	defaultCatalogId    = uint64(1)
	cPrefix             = "/meta"
	cDBPrefix           = "DB"
	cDBIDPrefix         = "DBID"
	cTablePrefix        = "Table"
	cTableIDPrefix      = "TID"
	cRoutePrefix        = "Route"
	cDeletedTablePrefix = "DeletedTableQueue"
	timeout             = 600 * time.Millisecond
)

var log = logger.New(os.Stderr, "catalog:")

type Catalog struct {
	Driver dist.CubeDriver
}

var gCatalog Catalog
var gInitOnce sync.Once

func DefaultCatalog(store dist.CubeDriver) Catalog {
	gInitOnce.Do(func() {
		gCatalog = Catalog{
			Driver: store,
		}
	})
	return gCatalog
}
func (c *Catalog) CreateDatabase(epoch uint64, dbName string, typ int) (uint64, error) {
	if _, err := c.checkDBNotExists(dbName); err != nil {
		return 0, err
	}
	id, err := c.genGlobalUniqIDs([]byte(cDBIDPrefix))
	if err != nil {
		return 0, err
	}
	if err = c.Driver.Set(c.dbIDKey(dbName), codec.Uint642Bytes(id)); err != nil {
		return 0, err
	}
	info := aoe.SchemaInfo{
		State:     aoe.StatePublic,
		Name:      dbName,
		Id:        id,
		CatalogId: 1,
		Type:      typ,
	}
	value, _ := json.Marshal(info)
	if err = c.Driver.Set(c.dbKey(id), value); err != nil {
		return 0, err
	}
	return id, nil
}
func (c *Catalog) DelDatabase(epoch uint64, dbName string) (err error) {
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
func (c *Catalog) GetDBs() ([]aoe.SchemaInfo, error) {
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
func (c *Catalog) GetDB(dbName string) (*aoe.SchemaInfo, error) {
	db, _ := c.checkDBNotExists(dbName)
	if db == nil {
		return nil, ErrDBNotExists
	}
	if db.State != aoe.StatePublic {
		return nil, ErrDBNotExists
	}
	return db, nil
}
func (c *Catalog) CreateTable(epoch, dbId uint64, tbl aoe.TableInfo) (uint64, error) {
	_, err := c.checkDBExists(dbId)
	if err != nil {
		return 0, err
	}
	_, err = c.checkTableNotExists(dbId, tbl.Name)
	if err != nil {
		return 0, err
	}
	tbl.Id, err = c.genGlobalUniqIDs([]byte(cTableIDPrefix))
	if err != nil {
		return 0, err
	}
	err = c.Driver.Set(c.tableIDKey(dbId, tbl.Name), codec.Uint642Bytes(tbl.Id))
	if err != nil {
		log.Errorf("ErrTableCreateFailed, %v", err)
		return 0, err
		//return 0, ErrTableCreateFailed
	}
	tbl.Epoch = epoch
	tbl.SchemaId = dbId
	if shardId, err := c.getAvailableShard(tbl.Id); err == nil {
		rkey := c.routeKey(dbId, tbl.Id, shardId)
		if err := c.Driver.CreateTablet(c.encodeTabletName(shardId, tbl.Id), shardId, &tbl); err != nil {
			log.Errorf("ErrTableCreateFailed, %v", err)
			return 0, err
			//return 0, ErrTableCreateFailed
		}
		if c.Driver.Set(rkey, []byte(tbl.Name)) != nil {
			log.Errorf("ErrTableCreateFailed, %v", err)
			return 0, err
			//return 0, ErrTableCreateFailed
		}
		tbl.State = aoe.StatePublic
		meta, err := helper.EncodeTable(tbl)
		if err != nil {
			log.Errorf("ErrTableCreateFailed, %v", err)
			return 0, err
			//return 0, ErrTableCreateFailed
		}
		//save metadata to kv
		err = c.Driver.Set(c.tableKey(dbId, tbl.Id), meta)
		if err != nil {
			log.Errorf("ErrTableCreateFailed, %v", err)
			return 0, err
		}
		log.Infof("Create Table finished, key is %v", c.tableKey(dbId, tbl.Id))
		return tbl.Id, nil
	}
	return 0, ErrNoAvailableShard
}
func (c *Catalog) DropTable(epoch, dbId uint64, tableName string) (tid uint64, err error) {
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
func (c *Catalog) dropTables(epoch, dbId uint64) (err error) {
	_, err = c.checkDBExists(dbId)
	if err != nil {
		return err
	}
	tbs, err := c.GetTables(dbId)
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
func (c *Catalog) GetTables(dbId uint64) ([]aoe.TableInfo, error) {
	if _, err := c.checkDBExists(dbId); err != nil {
		return nil, err
	} else {
		log.Errorf("Call GetTables, prefix is %v", c.tablePrefix(dbId))
		values, err := c.Driver.PrefixScan(c.tablePrefix(dbId), 0)
		if err != nil {
			log.Errorf("Call GetTables failed %v", err)
			return nil, err
		}
		log.Errorf("Call GetTables, prefix result size is %d", len(values))
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
func (c *Catalog) GetTable(dbId uint64, tableName string) (*aoe.TableInfo, error) {
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
		return tb, nil
	}
}
func (c *Catalog) GetTablets(dbId uint64, tableName string) ([]aoe.TabletInfo, error) {
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
		var tablets []aoe.TabletInfo
		for _, shardId := range shardIds {
			if sid, err := codec.Bytes2Uint64(shardId[len(c.routePrefix(dbId, tb.Id)):]); err != nil {
				log.Errorf("convert shardid failed, %v, shardid is %d, prefix length is %d", err, len(shardId), len(c.routePrefix(dbId, tb.Id)))
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
	rsp, err := c.Driver.Scan(c.deletedPrefix(), c.deletedEpochPrefix(epoch+1), 0)
	if err != nil {
		log.Errorf("scan error, %v", err)
		return cnt, err
	}
	for i := 1; i < len(rsp); i += 2 {
		if tbl, err := helper.DecodeTable(rsp[i]); err != nil {
			log.Errorf("Decode err for table info, %v, %v", err, rsp[i])
			continue
		} else {
			shardIds, err := c.Driver.PrefixKeys(c.routePrefix(tbl.SchemaId, tbl.Id), 0)
			if err != nil {
				log.Errorf("Failed to get shards for table %v, %v", rsp[i], err)
				continue
			}
			success := true
			for _, shardId := range shardIds {
				if sid, err := codec.Bytes2Uint64(shardId[len(c.routePrefix(tbl.SchemaId, tbl.Id)):]); err != nil {
					log.Errorf("convert shardid failed, %v", err)
					success = false
					break
				} else {
					_, err = c.Driver.DropTablet(c.encodeTabletName(sid, tbl.Id), sid)
					if err != nil {
						log.Errorf("call local drop table failed %d, %d, %v", sid, tbl.Id, err)
						success = false
						break
					}
				}
			}
			if success {
				if c.Driver.Delete(c.deletedTableKey(tbl.Epoch, tbl.SchemaId, tbl.Id)) != nil {
					log.Errorf("remove marked deleted tableinfo failed, %v, %v", err, tbl)
				} else {
					cnt++
				}
			}
		}
	}
	return cnt, nil
}
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
func (c *Catalog) encodeTabletName(groupId, tableId uint64) string {
	return fmt.Sprintf("%d#%d", groupId, tableId)
}
func (c *Catalog) genGlobalUniqIDs(idKey []byte) (uint64, error) {
	id, err := c.Driver.AllocID(idKey)
	if err != nil {
		return 0, err
	}
	return id, nil
}
func (c *Catalog) dbIDKey(dbName string) []byte {
	return codec.EncodeKey(cPrefix, defaultCatalogId, cDBIDPrefix, dbName)
}
func (c *Catalog) dbKey(id uint64) []byte {
	return codec.EncodeKey(cPrefix, defaultCatalogId, cDBPrefix, id)
}
func (c *Catalog) dbPrefix() []byte {
	return codec.EncodeKey(cPrefix, defaultCatalogId, cDBPrefix)
}
func (c *Catalog) tableIDKey(dbId uint64, tableName string) []byte {
	return codec.EncodeKey(cPrefix, defaultCatalogId, cTableIDPrefix, dbId, tableName)
}
func (c *Catalog) tableKey(dbId, tId uint64) []byte {
	return codec.EncodeKey(cPrefix, defaultCatalogId, cTablePrefix, dbId, tId)
}
func (c *Catalog) tablePrefix(dbId uint64) []byte {
	return codec.EncodeKey(cPrefix, defaultCatalogId, cTablePrefix, dbId)
}
func (c *Catalog) routeKey(dbId, tId, gId uint64) []byte {
	return codec.EncodeKey(cPrefix, defaultCatalogId, cRoutePrefix, dbId, tId, gId)
}
func (c *Catalog) routePrefix(dbId, tId uint64) []byte {
	return codec.EncodeKey(cPrefix, defaultCatalogId, cRoutePrefix, dbId, tId)
}
func (c *Catalog) deletedTableKey(epoch, dbId, tId uint64) []byte {
	return codec.EncodeKey(cDeletedTablePrefix, epoch, dbId, tId)
}
func (c *Catalog) deletedEpochPrefix(epoch uint64) []byte {
	return codec.EncodeKey(cDeletedTablePrefix, epoch)
}
func (c *Catalog) deletedPrefix() []byte {
	return codec.EncodeKey(cDeletedTablePrefix)
}
func (c *Catalog) getAvailableShard(tid uint64) (shardid uint64, err error) {
	timeoutC := time.After(timeout)
	for {
		select {
		case <-timeoutC:
			log.Errorn("wait for available shard timeout")
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
