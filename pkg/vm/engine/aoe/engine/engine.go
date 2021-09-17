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

package engine

import (
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"matrixone/pkg/catalog"
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/driver/pb"
	"matrixone/pkg/vm/engine"
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

//
func (e *aoeEngine) Node(ip string) *engine.NodeInfo {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0).Milliseconds())
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

//Delete drops the database by calling e.catalog.DropDatabase(epoch uint64, dbName string)
func (e *aoeEngine) Delete(epoch uint64, name string) error {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0).Milliseconds())
	}()
	err := e.catalog.DropDatabase(epoch, name)
	return err
}

//Create creates database by calling e.catalog.CreateDatabase(epoch uint64, dbName string, typ int)
func (e *aoeEngine) Create(epoch uint64, name string, typ int) error {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0).Milliseconds())
	}()
	_, err := e.catalog.CreateDatabase(epoch, name, typ)
	return err
}

//Databases returns the names of the databases in the catalog
func (e *aoeEngine) Databases() []string {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0).Milliseconds())
	}()
	var ds []string
	if dbs, err := e.catalog.ListDatabases(); err == nil {
		for _, db := range dbs {
			ds = append(ds, db.Name)
		}
	}
	return ds
}

//Database returns the database by calling e.catalog.GetDatabase
//the field catalog of the returned database is the catalog of the aoeEngine
func (e *aoeEngine) Database(name string) (engine.Database, error) {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0).Milliseconds())
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
