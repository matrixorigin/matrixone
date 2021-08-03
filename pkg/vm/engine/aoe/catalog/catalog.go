package catalog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/cockroachdb/errors"
	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/raftstore"
	stdLog "log"
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/engine/aoe/common/helper"
	"matrixone/pkg/vm/engine/aoe/dist"
	"matrixone/pkg/vm/engine/aoe/dist/pb"
	"sync"
	"time"
)

const (
	defaultCatalogId = uint64(1)
	cPrefix          = "/meta"
	cDBPrefix        = "DB"
	cDBIDPrefix      = "DBID"
	cTablePrefix     = "Table"
	cTableIDPrefix   = "TID"
	cRoutePrefix     = "Route"
	cDeletedTablePrefix   = "DeletedTableQueue"
)

type Catalog struct {
	Store  dist.Storage
}

var gCatalog Catalog
var gInitOnce sync.Once

func DefaultCatalog(store dist.Storage) Catalog {
	gInitOnce.Do(func() {
		gCatalog = Catalog{
			Store:  store,
		}
	})
	return gCatalog
}
func (c *Catalog) CreateDatabase(dbName string, typ int) (uint64, error) {
	if _, err := c.checkDBNotExists(dbName); err != nil {
		return 0, err
	}
	id, err := c.genGlobalUniqIDs([]byte(cDBIDPrefix))
	if err != nil {
		return 0, err
	}
	if err = c.Store.Set(c.dbIDKey(dbName), format.Uint64ToBytes(id)); err != nil {
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
	if err = c.Store.Set(c.dbKey(id), value); err != nil {
		return 0, err
	}
	return id, nil
}
func (c *Catalog) DelDatabase(dbName string) (err error) {
	if db, _ := c.checkDBNotExists(dbName); db == nil {
		return ErrDBNotExists
	} else {
		if err = c.dropTables(db.Id); err != nil {
			return err
		}
		if err = c.Store.Delete(c.dbKey(db.Id)); err != nil {
			return err
		}
		err = c.Store.Delete(c.dbIDKey(dbName))
		// TODO: Data Cleanup Notify (Drop tables & related within deleted db)
		return err
	}
}
func (c *Catalog) GetDBs() ([]aoe.SchemaInfo, error) {
	values, err := c.Store.PrefixScan(c.dbPrefix(), 0)
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
func (c *Catalog) CreateTable(dbId uint64, tbl aoe.TableInfo) (uint64, error) {
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
	err = c.Store.Set(c.tableIDKey(dbId, tbl.Name), format.Uint64ToBytes(tbl.Id))
	if err != nil {
		return 0, ErrTableCreateFailed
	}
	if shardId, err := c.getAvailableShard(tbl.Id); err == nil {
		rkey := c.routeKey(dbId, tbl.Id, shardId)
		if err := c.Store.CreateTablet(fmt.Sprintf("%d#%d", tbl.Id, shardId), shardId, &tbl); err != nil {
			return 0, ErrTableCreateFailed
		}
		if c.Store.Set(rkey, []byte(tbl.Name)) != nil {
			//TODO: remove shard's lock
			return 0, ErrTableCreateFailed
		}
		tbl.State = aoe.StatePublic
		meta, err := helper.EncodeTable(tbl)
		if err != nil {
			return 0, ErrTableCreateFailed
		}
		//save metadata to kv
		err = c.Store.Set(c.tableKey(dbId, tbl.Id), meta)
		if err != nil {
			return 0, err
		}
		return tbl.Id, nil
	}
	//No available situation
	//TODO: wait cube support shard pool
	//tbl.State = aoe.StateNone
	//return c.createShardForTable(dbId, tbl)
	return 0, ErrTooMuchTableExists
}
func (c *Catalog) DropTable(dbId uint64, tableName string) (tid uint64, err error) {
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
	value, _ := json.Marshal(tb)
	if err = c.Store.Set(c.deletedTableKey(dbId, tb.Id), value); err != nil {
		return tid, err
	}
	if err = c.Store.Delete(c.tableIDKey(dbId, tableName)); err != nil {
		return tid, err
	}
	if err = c.Store.Delete(c.tableKey(dbId, tb.Id)); err != nil {
		return tid, err
	}
	return tb.Id, err
}
func (c *Catalog) dropTables(dbId uint64) (err error) {
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
	for _, tbl:= range tbs {
		tbl.State = aoe.StateDeleteOnly
		value, _ := helper.EncodeTable(tbl)
		if err = c.Store.Set(c.deletedTableKey(dbId, tbl.Id), value); err != nil {
			return err
		}
		if err = c.Store.Delete(c.tableIDKey(dbId, tbl.Name)); err != nil {
			return err
		}
		if err = c.Store.Delete(c.tableKey(dbId, tbl.Id)); err != nil {
			return err
		}
	}
	return err
}
func (c *Catalog) GetTables(dbId uint64) ([]aoe.TableInfo, error) {
	if _, err := c.checkDBExists(dbId); err != nil {
		return nil, err
	} else {
		values, err := c.Store.PrefixScan(c.tablePrefix(dbId), 0)
		if err != nil {
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
func (c *Catalog) GetTablets(dbId uint64, tableName string) ([]aoe.TabletInfo, error)  {
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
		shardIds, err := c.Store.PrefixKeys(c.routePrefix(dbId, tb.Id), 0)

		if err != nil{
			return nil, err
		}
		var tablets []aoe.TabletInfo
		for _, shardId := range shardIds {
			if sid, err := format.ParseStrUInt64(string(shardId[len(c.routePrefix(dbId, tb.Id)):])); err != nil {
				stdLog.Printf("convert shardid failed, %v", err)
				continue
			}else {
				tablets = append(tablets, aoe.TabletInfo{
					Name: c.EncodeTabletName(tb.Id, sid),
					ShardId: sid,
					Table: *tb,
				})
			}

		}
		return tablets, nil
	}

}
func (c *Catalog) RemoveDeletedTable(dbId, tid uint64) (err error){
	if err := c.Store.DeleteIfExist(c.tableKey(dbId, tid)); err != nil{
		return ErrTableNotExists
	}
	return nil
}
func (c *Catalog) GetMarkedDeletedTables(epoch uint64) (tbls []aoe.TableInfo, err error) {
	rsp, err := c.Store.PrefixScan(c.deletedTablePrefix(), 0)
	if err != nil {
		return tbls, err
	}
	for i:=1; i<len(rsp); i+=2 {
		if tbl, err := helper.DecodeTable(rsp[i]); err != nil {
			stdLog.Printf("Decode err for table info, %v, %v", err, rsp[i])
			continue
		}else {
			tbls = append(tbls, tbl)
		}
	}
	return tbls, nil
}
func (c *Catalog) checkDBExists(id uint64) (*aoe.SchemaInfo, error) {
	db := aoe.SchemaInfo{}
	if v, err := c.Store.Get(c.dbKey(id)); err != nil {
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
	if value, err := c.Store.Get(c.dbIDKey(dbName)); err != nil || value == nil {
		return nil, nil
	} else {
		id := format.MustBytesToUint64(value)
		db, err := c.checkDBExists(id)
		if err == ErrDBNotExists {
			return nil, nil
		}
		return db, ErrDBCreateExists
	}
}
func (c *Catalog) checkTableExists(dbId, id uint64) (*aoe.TableInfo, error) {
	if v, err := c.Store.Get(c.tableKey(dbId, id)); err != nil {
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
	if value, err := c.Store.Get(c.tableIDKey(dbId, tableName)); err != nil || value == nil {
		return nil, nil
	} else {
		id := format.MustBytesToUint64(value)
		tb, err := c.checkTableExists(dbId, id)
		if err == ErrTableNotExists {
			return nil, nil
		}
		return tb, ErrTableCreateExists
	}
}
func (c *Catalog) EncodeTabletName(tableId, groupId uint64) string {
	return fmt.Sprintf("%d#%d", tableId, groupId)
}
func (c *Catalog) genGlobalUniqIDs(idKey []byte) (uint64, error) {
	id, err := c.Store.AllocID(idKey)
	if err != nil {
		return 0, err
	}
	return id, nil
}
func (c *Catalog) dbIDKey(dbName string) []byte {
	return []byte(fmt.Sprintf("%s/%d/%s/%s", cPrefix, defaultCatalogId, cDBIDPrefix, dbName))
}
func (c *Catalog) dbKey(id uint64) []byte {
	return []byte(fmt.Sprintf("%s/%d/%s/%d", cPrefix, defaultCatalogId, cDBPrefix, id))
}
func (c *Catalog) dbPrefix() []byte {
	return []byte(fmt.Sprintf("%s/%d/%s", cPrefix, defaultCatalogId, cDBPrefix))
}
func (c *Catalog) tableIDKey(dbId uint64, tableName string) []byte {
	return []byte(fmt.Sprintf("%s/%d/%s/%d/%s", cPrefix, defaultCatalogId, cTableIDPrefix, dbId, tableName))
}
func (c *Catalog) tableKey(dbId, tId uint64) []byte {
	return []byte(fmt.Sprintf("%s/%d/%s/%d/%d", cPrefix, defaultCatalogId, cTablePrefix, dbId, tId))
}
func (c *Catalog) tablePrefix(dbId uint64) []byte {
	return []byte(fmt.Sprintf("%s/%d/%s/%d", cPrefix, defaultCatalogId, cTablePrefix, dbId))
}
func (c *Catalog) routeKey(dbId, tId, gId uint64) []byte {
	return []byte(fmt.Sprintf("%s/%d/%s/%d/%d/%d", cPrefix, defaultCatalogId, cRoutePrefix, dbId, tId, gId))
}
func (c *Catalog) routePrefix(dbId, tId uint64) []byte {
	return []byte(fmt.Sprintf("%s/%d/%s/%d/%d", cPrefix, defaultCatalogId, cRoutePrefix, dbId, tId))
}
func (c *Catalog) deletedTableKey(dbId, tId uint64) []byte {
	return []byte(fmt.Sprintf("%s/%d/%s/%d/%d", cPrefix, defaultCatalogId, cDeletedTablePrefix, dbId, tId))
}
func (c *Catalog) deletedTablePrefix() []byte {
	return []byte(fmt.Sprintf("%s/%d/%s", cPrefix, defaultCatalogId, cDeletedTablePrefix))
}
func (c *Catalog) getAvailableShard(tid uint64) (shardid uint64, err error) {
	var rsp []uint64
	c.Store.RaftStore().GetRouter().Every(uint64(pb.AOEGroup), true, func(shard *bhmetapb.Shard, store bhmetapb.Store) {
		if len(rsp) > 0 {
			return
		}
		err := c.Store.SetIfNotExist(format.UInt64ToString(shard.ID), format.UInt64ToString(tid))
		if err == nil {
			rsp = append(rsp, shard.ID)
		}
		return
	})
	if len(rsp) == 1 {
		return rsp[0], nil
	}
	return shardid, errors.New("No available shards")
}
func (c *Catalog) createShardForTable(dbId uint64, tbl aoe.TableInfo) (shardid uint64, err error) {
	meta, err := helper.EncodeTable(tbl)
	if err != nil {
		return 0, ErrTableCreateFailed
	}

	//save metadata to kv
	err = c.Store.Set(c.tableKey(dbId, tbl.Id), meta)
	if err != nil {
		return 0, err
	}

	// TODO: support shared tables
	// created shards
	client := c.Store.RaftStore().Prophet().GetClient()
	tKey := c.tableKey(dbId, tbl.Id)
	rKey := c.routePrefix(dbId, tbl.Id)
	header := format.Uint64ToBytes(uint64(len(tKey) + len(rKey) + len([]byte("#"))))
	buf := bytes.Buffer{}
	buf.Write(header)
	buf.Write(tKey)
	buf.Write([]byte("#"))
	buf.Write(rKey)
	buf.Write(meta)
	//find an available range for new shard
	start := uint64(0)
	c.Store.RaftStore().GetRouter().Every(uint64(pb.AOEGroup), true, func(shard *bhmetapb.Shard, store bhmetapb.Store) {
		end, _ := format.ParseStrUInt64(string(shard.End))
		if end > start {
			start = end
		}
	})
	err = client.AsyncAddResources(raftstore.NewResourceAdapterWithShard(
		bhmetapb.Shard{
			Start:  format.Uint64ToBytes(start),
			End:    format.Uint64ToBytes(start + 1),
			Unique: string(format.Uint64ToBytes(tbl.Id)),
			Group:  uint64(pb.AOEGroup),
			Data:   buf.Bytes(),
			DisableSplit: true,
		}))
	if err != nil {
		return 0, err
	}
	completedC := make(chan *aoe.TableInfo, 1)
	defer close(completedC)
	go func() {
		i := 0
		for {
			tb, _ := c.checkTableExists(dbId, tbl.Id)
			if tb != nil && tb.State == aoe.StatePublic {
				completedC <- tb
				break
			}
			i += 1
		}
	}()
	select {
	case <-completedC:
		return tbl.Id, nil
	case <-time.After(3 * time.Second):
		return tbl.Id, ErrTableCreateFailed
	}
}