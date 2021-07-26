package catalog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/cockroachdb/errors"
	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/raftstore"
	cmap "github.com/orcaman/concurrent-map"
	stdLog "log"
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/engine/aoe/common/helper"
	"matrixone/pkg/vm/engine/aoe/dist"
	"matrixone/pkg/vm/engine/aoe/dist/pb"
	"sync"
	"time"
)

const (
	DefaultCatalogId = uint64(1)
)

var (
	cPrefix        = "/meta"
	cDBPrefix      = "DB"
	cDBIDPrefix    = "DBID"
	cTablePrefix   = "Table"
	cTableIDPrefix = "TID"
	cStatePrefix   = "State"
	cRoutePrefix   = "Route"
	cSegmentPrefix = "Seg"
)

type Catalog struct {
	gMutex cmap.ConcurrentMap
	Store  dist.Storage
	//引用计数
}

var gCatalog Catalog
var gInitOnce sync.Once

func DefaultCatalog(store dist.Storage) Catalog {
	gInitOnce.Do(func() {
		gCatalog = Catalog{
			gMutex: cmap.New(),
			Store:  store,
		}
	})
	return gCatalog
}

func (c *Catalog) CreateDatabase(dbName string, typ int) (uint64, error) {
	if _, err := c.checkDBNotExists(dbName); err != nil {
		return 0, err
	}
	v, ok := c.gMutex.Get(string(c.dbIDKey(dbName)))
	if !ok {
		v = &sync.RWMutex{}
		c.gMutex.Set(string(c.dbIDKey(dbName)), v)
	}
	lock := v.(*sync.RWMutex)
	lock.Lock()
	defer lock.Unlock()
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

func (c *Catalog) DelDatabase(dbName string) (uint64, error) {
	if db, _ := c.checkDBNotExists(dbName); db == nil {
		return 0, ErrDBNotExists
	} else {
		v, ok := c.gMutex.Get(string(c.dbIDKey(dbName)))
		if !ok {
			return 0, nil
		}
		lock := v.(*sync.RWMutex)
		lock.Lock()
		defer func() {
			c.gMutex.Remove(string(c.dbIDKey(dbName)))
			lock.Unlock()
		}()
		value, err := c.Store.Get(c.dbKey(db.Id))
		if err != nil {
			return 0, err
		}
		db := aoe.SchemaInfo{}
		_ = json.Unmarshal(value, &db)
		db.State = aoe.StateDeleteOnly
		value, _ = json.Marshal(db)
		if err = c.Store.Set(c.dbKey(db.Id), value); err != nil {
			return 0, err
		}
		err = c.Store.Delete(c.dbIDKey(dbName))
		// TODO: Data Cleanup Notify (Drop tables & related within deleted db)
		return db.Id, err
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
	v, ok := c.gMutex.Get(string(c.tableIDKey(dbId, tbl.Name)))
	if !ok {
		v = &sync.RWMutex{}
		c.gMutex.Set(string(c.tableIDKey(dbId, tbl.Name)), v)
	}
	lock := v.(*sync.RWMutex)
	lock.RLock()
	defer lock.RUnlock()
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
			//TODO: remove shard's lock
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
	tbl.State = aoe.StateNone
	return c.CreateTable(dbId, tbl)
}

func (c *Catalog) DropTable(dbId uint64, tableName string) (uint64, error) {
	_, err := c.checkDBExists(dbId)
	if err != nil {
		return 0, err
	}
	tb, err := c.checkTableNotExists(dbId, tableName)
	if tb == nil {
		return 0, ErrTableNotExists
	}
	v, ok := c.gMutex.Get(string(c.tableIDKey(dbId, tableName)))
	if !ok {
		return 0, nil
	}
	lock := v.(*sync.RWMutex)
	lock.Lock()
	defer func() {
		c.gMutex.Remove(string(c.tableIDKey(dbId, tableName)))
		lock.Unlock()
	}()
	tb.State = aoe.StateDeleteOnly
	value, _ := json.Marshal(tb)
	if err = c.Store.Set(c.tableKey(dbId, tb.Id), value); err != nil {
		return 0, err
	}
	err = c.Store.Delete(c.tableIDKey(dbId, tableName))
	//TODO：Data Cleanup Notify
	return tb.Id, err

}
func (c *Catalog) GetTables(dbId uint64) ([]aoe.TableInfo, error) {
	if db, err := c.checkDBExists(dbId); err != nil {
		return nil, err
	} else {
		v, ok := c.gMutex.Get(string(c.dbIDKey(db.Name)))
		if !ok {
			return nil, ErrDBNotExists
		}
		lock := v.(*sync.RWMutex)
		lock.RLock()
		defer lock.RUnlock()
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
		v, ok := c.gMutex.Get(string(c.tableIDKey(dbId, tableName)))
		if !ok {
			return nil, nil
		}
		lock := v.(*sync.RWMutex)
		lock.RLock()
		defer lock.RUnlock()
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
func (c *Catalog) DispatchQueries(dbId, tid uint64) ([]aoe.RouteInfo, error) {
	items := make(map[uint64]map[uint64][]aoe.SegmentInfo)
	if _, err := c.checkDBExists(dbId); err != nil {
		return nil, err
	} else {
		tb, err := c.checkTableExists(dbId, tid)
		if err != nil {
			return nil, err
		}
		v, ok := c.gMutex.Get(string(c.tableIDKey(dbId, tb.Name)))
		if !ok {
			return nil, ErrTableNotExists
		}
		lock := v.(*sync.RWMutex)
		lock.RLock()
		defer lock.RUnlock()
		values, err := c.Store.PrefixScan(c.routePrefix(dbId, tid), 0)
		if err != nil {
			return nil, err
		}
		for i := 0; i < len(values); i = i + 2 {
			keys := bytes.Split(values[i], []byte("/"))
			pId := format.MustBytesToUint64(keys[len(keys)-1])
			gId := format.MustBytesToUint64(keys[len(keys)-2])
			value := values[i+1]
			seg := aoe.SegmentInfo{}
			_ = json.Unmarshal(value, &seg)
			items[gId][pId] = append(items[gId][pId], seg)
		}
	}
	var resp []aoe.RouteInfo
	for gId, p := range items {
		resp = append(resp, aoe.RouteInfo{
			Node:     []byte(c.Store.RaftStore().GetRouter().LeaderAddress(gId)),
			GroupId:  gId,
			Segments: p,
		})
	}
	return resp, nil
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
	v, ok := c.gMutex.Get(string(c.dbIDKey(dbName)))
	if !ok {
		return nil, nil
	}
	lock := v.(*sync.RWMutex)
	lock.RLock()
	defer lock.RUnlock()
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
	v, ok := c.gMutex.Get(string(c.tableIDKey(dbId, tableName)))
	if !ok {
		return nil, nil
	}
	lock := v.(*sync.RWMutex)
	lock.RLock()
	defer lock.RUnlock()
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
func (c *Catalog) EncodeTabletName(tableId uint64, groupId uint64) string {
	return fmt.Sprintf("%d#%d", tableId, groupId)
}
func (c *Catalog) genGlobalUniqIDs(idKey []byte) (uint64, error) {
	id, err := c.Store.AllocID(idKey)
	if err != nil {
		return 0, err
	}
	return id, nil
}
//where to generate id
func (c *Catalog) dbIDKey(dbName string) []byte {
	return []byte(fmt.Sprintf("%s/%d/%s/%s", cPrefix, DefaultCatalogId, cDBIDPrefix, dbName))
}
func (c *Catalog) dbKey(id uint64) []byte {
	return []byte(fmt.Sprintf("%s/%d/%s/%d", cPrefix, DefaultCatalogId, cDBPrefix, id))
}
func (c *Catalog) dbPrefix() []byte {
	return []byte(fmt.Sprintf("%s/%d/%s/", cPrefix, DefaultCatalogId, cDBPrefix))
}
func (c *Catalog) tableIDKey(dbId uint64, tableName string) []byte {
	return []byte(fmt.Sprintf("%s/%d/%s/%d/%s", cPrefix, DefaultCatalogId, cTableIDPrefix, dbId, tableName))
}
func (c *Catalog) tableKey(dbId uint64, tId uint64) []byte {
	return []byte(fmt.Sprintf("%s/%d/%s/%d/%d", cPrefix, DefaultCatalogId, cTablePrefix, dbId, tId))
}
func (c *Catalog) tablePrefix(dbId uint64) []byte {
	return []byte(fmt.Sprintf("%s/%d/%s/%d/", cPrefix, DefaultCatalogId, cTablePrefix, dbId))
}
func (c *Catalog) routeKey(dbId uint64, tId uint64, gId uint64) []byte {
	return []byte(fmt.Sprintf("%s/%d/%s/%d/%d/%d", cPrefix, DefaultCatalogId, cRoutePrefix, dbId, tId, gId))
}
func (c *Catalog) routePrefix(dbId uint64, tId uint64) []byte {
	return []byte(fmt.Sprintf("%s/%d/%s/%d/%d/", cPrefix, DefaultCatalogId, cRoutePrefix, dbId, tId))
}

func (c *Catalog) getAvailableShard(tid uint64) (shardid uint64, err error) {
	var rsp []uint64
	c.Store.RaftStore().GetRouter().Every(uint64(pb.AOEGroup), true, func(shard *bhmetapb.Shard, address string) {
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
	err = client.AsyncAddResources(raftstore.NewResourceAdapterWithShard(
		bhmetapb.Shard{
			Start:  format.Uint64ToBytes(tbl.Id),
			End:    format.Uint64ToBytes(tbl.Id + 1),
			Unique: string(format.Uint64ToBytes(tbl.Id)),
			Group:  uint64(pb.AOEGroup),
			Data:   buf.Bytes(),
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
