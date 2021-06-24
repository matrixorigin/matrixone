package catalog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/raftstore"
	cmap "github.com/orcaman/concurrent-map"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/engine/aoe/dist"
	"sync"
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
	store  dist.Storage
}

var gCatalog Catalog
var gInitOnce sync.Once

func DefaultCatalog(store dist.Storage) Catalog {
	gInitOnce.Do(func() {
		gCatalog = Catalog{
			gMutex: cmap.New(),
			store:  store,
		}
	})
	return gCatalog

}

func (c *Catalog) CreateDatabase(dbName string) (uint64, error) {
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
	if err = c.store.Set(c.dbIDKey(dbName), format.Uint64ToBytes(id)); err != nil {
		return 0, err
	}
	info := aoe.SchemaInfo{
		State:     aoe.StatePublic,
		Name:      dbName,
		Id:        id,
		CatalogId: 1,
	}
	value, _ := json.Marshal(info)
	if err = c.store.Set(c.dbKey(id), value); err != nil {
		return 0, err
	}
	return id, nil
}

func (c *Catalog) DelDatabase(dbName string) (uint64, error) {
	if id, err := c.checkDBExists(dbName); err != nil {
		return 0, err
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
		value, err := c.store.Get(c.dbKey(id))
		if err != nil {
			return 0, err
		}
		db := aoe.SchemaInfo{}
		_ = json.Unmarshal(value, &db)
		db.State = aoe.StateDeleteOnly
		value, _ = json.Marshal(db)
		if err = c.store.Set(c.dbKey(id), value); err != nil {
			return 0, err
		}
		err = c.store.Delete(c.dbIDKey(dbName))
		// TODO ADD Async Deletion Task Queue (Drop tables & related within deleted db)
		return id, err
	}
}

func (c *Catalog) GetDBs() ([]aoe.SchemaInfo, error) {
	values, err := c.store.PrefixScan(c.dbPrefix(), 0)
	if err != nil {
		return nil, err
	}
	var dbs []aoe.SchemaInfo

	for i := 1; i < len(values); i = i + 2 {
		db := aoe.SchemaInfo{}
		_ = json.Unmarshal(values[i], &db)
		dbs = append(dbs, db)
	}
	return dbs, nil
}

func (c *Catalog) GetDB(dbName string) (*aoe.SchemaInfo, error) {
	id, err := c.checkDBExists(dbName)
	if err != nil {
		return nil, err
	}
	v, ok := c.gMutex.Get(string(c.dbIDKey(dbName)))
	if !ok {
		return nil, ErrDBNotExists
	}
	lock := v.(*sync.RWMutex)
	lock.RLock()
	defer lock.RUnlock()
	resp, err := c.store.Get(c.dbKey(id))
	if err != nil || resp == nil {
		return nil, ErrDBNotExists
	}
	db := aoe.SchemaInfo{}
	_ = json.Unmarshal(resp, &db)
	if db.State != aoe.StatePublic {
		return nil, ErrDBNotExists
	}
	return &db, nil
}

func (c *Catalog) CreateTable(dbName, tableName, comment string, typ uint64, tableDefs []engine.TableDef, pdef *engine.PartitionBy) (uint64, error) {
	dbId, err := c.checkDBExists(dbName)
	if err != nil {
		return 0, err
	}
	err = c.checkTableNotExists(dbId, tableName)
	if err != nil {
		return 0, err
	}
	v, ok := c.gMutex.Get(string(c.tableIDKey(dbId, tableName)))
	if !ok {
		v = &sync.RWMutex{}
		c.gMutex.Set(string(c.tableIDKey(dbId, tableName)), v)
	}
	lock := v.(*sync.RWMutex)
	lock.RLock()
	defer lock.RUnlock()
	tid, err := c.genGlobalUniqIDs([]byte(cTableIDPrefix))
	if err != nil {
		return 0, err
	}
	err = c.store.Set(c.tableIDKey(dbId, tableName), format.Uint64ToBytes(tid))
	if err != nil {
		return 0, err
	}
	tInfo, err := aoe.Transfer(dbId, tid, typ, tableName, comment, tableDefs, pdef)
	if err != nil {
		return 0, ErrTableCreateFailed
	}
	tInfo.State = aoe.StateNone

	meta, err := aoe.EncodeTable(tInfo)
	if err != nil {
		return 0, ErrTableCreateFailed
	}

	//save metadata to kv
	err = c.store.Set(c.tableKey(dbId, tid), meta)
	if err != nil {
		return 0, err
	}

	// TODO: support shared tables
	// created shards
	client := c.store.RaftStore().Prophet().GetClient()
	tKey := c.tableKey(dbId, tid)
	rKey := c.routePrefix(dbId, tid)
	header := format.Uint64ToBytes(uint64(len(tKey) + len(rKey) + len([]byte("#"))))
	buf := bytes.Buffer{}
	buf.Write(header)
	buf.Write(tKey)
	buf.Write([]byte("#"))
	buf.Write(rKey)
	buf.Write(meta)
	err = client.AsyncAddResources(raftstore.NewResourceAdapterWithShard(
		bhmetapb.Shard{
			Start:  format.Uint64ToBytes(tid),
			End:    format.Uint64ToBytes(tid + 1),
			Unique: "gTable-" + string(format.UInt64ToString(tid)),
			Group:  uint64(aoe.AOEGroup),
			Data:   buf.Bytes(),
		}))
	// TODO: wait table meta state changed?
	if err != nil {
		return 0, err
	}
	return tid, nil
}

func (c *Catalog) GetTables(dbName string) ([]aoe.TableInfo, error) {
	if id, err := c.checkDBExists(dbName); err != nil {
		return nil, err
	} else {
		v, ok := c.gMutex.Get(string(c.dbIDKey(dbName)))
		if !ok {
			return nil, ErrDBNotExists
		}
		lock := v.(*sync.RWMutex)
		lock.RLock()
		defer lock.RUnlock()
		values, err := c.store.PrefixScan(c.tablePrefix(id), 0)
		if err != nil {
			return nil, err
		}
		var tables []aoe.TableInfo
		for i := 1; i < len(values); i = i + 2 {
			t, _ := aoe.DecodeTable(values[i])
			if t.State != aoe.StatePublic {
				continue
			}
			tables = append(tables, t)
		}
		return tables, nil
	}
}

func (c *Catalog) GetTable(dbName string, tableName string) (*aoe.TableInfo, error) {
	if dbId, err := c.checkDBExists(dbName); err != nil {
		return nil, err
	} else {
		tid, err := c.checkTableExists(dbId, tableName)
		if err != nil {
			return nil, err
		}
		v, ok := c.gMutex.Get(string(c.tableIDKey(dbId, tableName)))
		if !ok {
			return nil, ErrTableNotExists
		}
		lock := v.(*sync.RWMutex)
		lock.RLock()
		defer lock.RUnlock()
		value, err := c.store.Get(c.tableKey(dbId, tid))
		if err != nil || value == nil {
			return nil, ErrTableNotExists
		}
		t, _ := aoe.DecodeTable(value)
		if t.State != aoe.StatePublic {
			return nil, err
		}
		return &t, nil
	}
}

func (c *Catalog) DispatchQueries(dbName string, tableName string) ([]aoe.RouteInfo, error) {
	items := make(map[uint64]map[uint64][]aoe.SegmentInfo)
	if dbId, err := c.checkDBExists(dbName); err != nil {
		return nil, err
	} else {
		tid, err := c.checkTableExists(dbId, tableName)
		if err != nil {
			return nil, err
		}
		v, ok := c.gMutex.Get(string(c.tableIDKey(dbId, tableName)))
		if !ok {
			return nil, ErrTableNotExists
		}
		lock := v.(*sync.RWMutex)
		lock.RLock()
		defer lock.RUnlock()
		values, err := c.store.PrefixScan(c.routePrefix(dbId, tid), 0)
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
			Node:     []byte(c.store.RaftStore().GetRouter().LeaderAddress(gId)),
			GroupId:  gId,
			Segments: p,
		})
	}
	return resp, nil
}

func (c *Catalog) checkDBExists(dbName string) (uint64, error) {
	v, ok := c.gMutex.Get(string(c.dbIDKey(dbName)))
	if !ok {
		return 0, ErrDBNotExists
	}
	lock := v.(*sync.RWMutex)
	lock.RLock()
	defer lock.RUnlock()
	if value, err := c.store.Get(c.dbIDKey(dbName)); err != nil || value == nil {
		return 0, ErrDBNotExists
	} else {
		id := format.MustBytesToUint64(value)
		if v, err := c.store.Get(c.dbKey(id)); err != nil || value == nil {
			return 0, ErrDBNotExists
		} else {
			db := aoe.SchemaInfo{}
			if err = json.Unmarshal(v, &db); err != nil {
				return 0, ErrDBNotExists
			}
			if db.State == aoe.StateDeleteOnly {
				return 0, ErrDBNotExists
			}
		}
		return id, nil
	}
}

func (c *Catalog) checkDBNotExists(dbName string) (bool, error) {
	_, err := c.checkDBExists(dbName)
	if err == ErrDBNotExists {
		return true, nil
	}
	return false, ErrDBCreateExists
}

func (c *Catalog) checkTableExists(dbId uint64, tableName string) (uint64, error) {
	v, ok := c.gMutex.Get(string(c.tableIDKey(dbId, tableName)))
	if !ok {
		return 0, ErrTableNotExists
	}
	lock := v.(*sync.RWMutex)
	lock.RLock()
	defer lock.RUnlock()
	if value, err := c.store.Get(c.tableIDKey(dbId, tableName)); err != nil || value == nil {
		return 0, ErrTableNotExists
	} else {
		id := format.MustBytesToUint64(value)
		if v, err := c.store.Get(c.tableKey(dbId, id)); err != nil || value == nil {
			return 0, ErrTableNotExists
		} else {
			if table, err := aoe.DecodeTable(v); err != nil {
				return 0, ErrTableNotExists
			}else {
				if table.State == aoe.StateDeleteOnly {
					return 0, ErrTableNotExists
				}
			}
		}
		return id, nil
	}
}

func (c *Catalog) checkTableNotExists(dbId uint64, tableName string) error {
	_, err := c.checkTableExists(dbId, tableName)
	if err == ErrTableNotExists {
		return nil
	}
	return ErrTableCreateExists
}

func (c *Catalog) EncodeTabletName(tableId uint64, groupId uint64) string {
	return fmt.Sprintf("%d#%d", tableId, groupId)
}

func (c *Catalog) genGlobalUniqIDs(idKey []byte) (uint64, error) {
	id, err := c.store.AllocID(idKey)
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

func (c *Catalog) routeKey(dbId uint64, tId uint64, gId uint64, pId uint64) []byte {
	return []byte(fmt.Sprintf("%s/%d/%s/%d/%d/%d/%d", cPrefix, DefaultCatalogId, cRoutePrefix, dbId, tId, gId, pId))
}

func (c *Catalog) routePrefix(dbId uint64, tId uint64) []byte {
	return []byte(fmt.Sprintf("%s/%d/%s/%d/%d/", cPrefix, DefaultCatalogId, cRoutePrefix, dbId, tId))
}
