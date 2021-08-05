package engine

import (
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/catalog"
	"matrixone/pkg/vm/engine/aoe/dist/pb"
	"strings"
)

func New() *aoeEngine {
	//1. Parse config
	//2. New Storage
	//3. New Catalog
	return &aoeEngine{}
}

func Mock(c *catalog.Catalog) *aoeEngine {
	//1. Parse config
	//2. New Storage
	//3. New Catalog
	return &aoeEngine{
		catalog: c,
	}
}

func (e *aoeEngine) Node(ip string) *engine.NodeInfo {
	var ni *engine.NodeInfo
	e.catalog.Store.RaftStore().GetRouter().Every(uint64(pb.AOEGroup), true, func(shard *bhmetapb.Shard, store bhmetapb.Store) {
		if ni != nil {
			return
		}
		if strings.HasPrefix(store.ClientAddr, ip) {
			stats := e.catalog.Store.RaftStore().GetRouter().GetStoreStats(store.ID)
			ni = &engine.NodeInfo{
				Mcpu: len(stats.GetCpuUsages()),
			}
		}
	})
	return ni
}

func (e *aoeEngine) Delete(epoch uint64, name string) error {
	err := e.catalog.DelDatabase(epoch, name)
	return err
}

func (e *aoeEngine) Create(epoch uint64, name string, typ int) error {
	_, err := e.catalog.CreateDatabase(epoch, name, typ)
	return err
}

func (e *aoeEngine) Databases() []string {
	var ds []string
	if dbs, err := e.catalog.GetDBs(); err == nil {
		for _, db := range dbs {
			ds = append(ds, db.Name)
		}
	}
	return ds
}

func (e *aoeEngine) Database(name string) (engine.Database, error) {
	db, err := e.catalog.GetDB(name)
	if err != nil {
		return nil, err
	}
	return &database{
		id:      db.Id,
		typ:     db.Type,
		catalog: e.catalog,
	}, nil
}
