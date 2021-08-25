package engine

import (
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/catalog"
	"matrixone/pkg/vm/engine/aoe/dist/pb"
	"strings"
	"time"
)

func New(c *catalog.Catalog) *aoeEngine {
	//1. Parse config
	//2. New Storage
	//3. New Catalog
	return &aoeEngine{
		catalog: c,
	}
}

func (e *aoeEngine) Node(ip string) *engine.NodeInfo {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0))
	}()
	var ni *engine.NodeInfo
	e.catalog.Driver.RaftStore().GetRouter().Every(uint64(pb.AOEGroup), true, func(shard *bhmetapb.Shard, store bhmetapb.Store) {
		if ni != nil {
			return
		}
		if strings.HasPrefix(store.ClientAddr, ip) {
			stats := e.catalog.Driver.RaftStore().GetRouter().GetStoreStats(store.ID)
			ni = &engine.NodeInfo{
				Mcpu: len(stats.GetCpuUsages()),
			}
		}
	})
	return ni
}

func (e *aoeEngine) Delete(epoch uint64, name string) error {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0))
	}()
	err := e.catalog.DropDatabase(epoch, name)
	return err
}

func (e *aoeEngine) Create(epoch uint64, name string, typ int) error {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0))
	}()
	_, err := e.catalog.CreateDatabase(epoch, name, typ)
	return err
}

func (e *aoeEngine) Databases() []string {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0))
	}()
	var ds []string
	if dbs, err := e.catalog.ListDatabases(); err == nil {
		for _, db := range dbs {
			ds = append(ds, db.Name)
		}
	}
	return ds
}

func (e *aoeEngine) Database(name string) (engine.Database, error) {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0))
	}()
	db, err := e.catalog.GetDatabase(name)
	if err != nil {
		return nil, err
	}
	return &database{
		id:      db.Id,
		typ:     db.Type,
		catalog: e.catalog,
	}, nil
}
