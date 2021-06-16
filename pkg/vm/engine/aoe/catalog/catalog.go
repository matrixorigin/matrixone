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
	for _, v := range values {
		db := aoe.SchemaInfo{}
		_ = json.Unmarshal(v, &db)
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
	if err != nil {
		return nil, err
	}
	db := aoe.SchemaInfo{}
	_ = json.Unmarshal(resp, &db)
	if db.State != aoe.StatePublic {
		return nil, ErrDBNotExists
	}
	return &db, nil
}

func (c *Catalog) CreateTable(dbName string, tableName string, tableDefs []engine.TableDef, pdef engine.PartitionBy) (uint64, error) {

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
	tInfo := aoe.TableInfo{}
	id := uint64(0)
	for _, def := range tableDefs {
		switch v := def.(type) {
		case *engine.AttributeDef:
			col := aoe.ColumnInfo{
				SchemaId: dbId,
				TableID:  tid,
				Id:       id,
				Name:     v.Attr.Name,
				Type:     v.Attr.Type,
				Alg:      v.Attr.Alg,
			}
			tInfo.Columns = append(tInfo.Columns, col)
		case *engine.IndexTableDef:
			// TODO: how to handle unsupported param
			continue
		}
	}
	tInfo.State = aoe.StateNone

	// TODO: call interface provided by mochen to replace json.Marshal
	tInfo.Partition, _ = json.Marshal(pdef)

	meta, _ := json.Marshal(tInfo)

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
		for _, v := range values {
			t := aoe.TableInfo{}
			_ = json.Unmarshal(v, &t)
			tables = append(tables, t)
		}
		return tables, nil
	}
}

func (c *Catalog) GetTable(dbName string, tableName string) (*aoe.TableInfo, error) {
	if dbId, err := c.checkDBExists(dbName); err != nil {
		return nil, err
	} else {
		var t aoe.TableInfo
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
		if err != nil {
			return nil, err
		}
		_ = json.Unmarshal(value, &t)
		if t.State != aoe.StatePublic {
			return nil, err
		}
		return &t, nil
	}
}

func (c *Catalog) checkDBExists(dbName string) (uint64, error) {
	v, ok := c.gMutex.Get(string(c.dbIDKey(dbName)))
	if !ok {
		return 0, ErrDBNotExists
	}
	lock := v.(*sync.RWMutex)
	lock.RLock()
	defer lock.RUnlock()
	if value, err := c.store.Get(c.dbIDKey(dbName)); err != nil {
		return 0, ErrDBNotExists
	} else {
		id := format.MustBytesToUint64(value)
		if v, err := c.store.Get(c.dbKey(id)); err != nil {
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
	if value, err := c.store.Get(c.tableIDKey(dbId, tableName)); err != nil {
		return 0, ErrTableNotExists
	} else {
		id := format.MustBytesToUint64(value)
		if v, err := c.store.Get(c.tableKey(dbId, id)); err != nil {
			return 0, ErrTableNotExists
		} else {
			table := aoe.TableInfo{}
			if err = json.Unmarshal(v, &table); err != nil {
				return 0, ErrTableNotExists
			}
			if table.State == aoe.StateDeleteOnly {
				return 0, ErrTableNotExists
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

func (c *Catalog) genGlobalUniqIDs(idKey []byte) (uint64, error) {
	return 0, nil
}

//where to generate id
func (c *Catalog) dbIDKey(dbName string) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s", cPrefix, cDBIDPrefix, dbName))
}

func (c *Catalog) dbKey(id uint64) []byte {
	return []byte(fmt.Sprintf("%s/%s/%d", cPrefix, cDBPrefix, id))
}

func (c *Catalog) dbPrefix() []byte {
	return []byte(fmt.Sprintf("%s/%s/", cPrefix, cDBPrefix))
}

func (c *Catalog) tableIDKey(dbId uint64, tableName string) []byte {
	return []byte(fmt.Sprintf("%s/%s/%d/%s", cPrefix, cTableIDPrefix, dbId, tableName))
}

func (c *Catalog) tableKey(dbId uint64, tId uint64) []byte {
	return []byte(fmt.Sprintf("%s/%s/%d/%d", cPrefix, cTablePrefix, dbId, tId))
}

func (c *Catalog) tablePrefix(dbId uint64) []byte {
	return []byte(fmt.Sprintf("%s/%s/%d/", cPrefix, cTablePrefix, dbId))
}

func (c *Catalog) routeKey(dbId uint64, tId uint64, gId uint64) []byte {
	return []byte(fmt.Sprintf("%s/%s/%d/%d/%d", cPrefix, cRoutePrefix, dbId, tId, gId))
}

func (c *Catalog) routePrefix(dbId uint64, tId uint64) []byte {
	return []byte(fmt.Sprintf("%s/%s/%d/%d/", cPrefix, cRoutePrefix, dbId, tId))
}
