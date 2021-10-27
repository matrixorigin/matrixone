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
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/pb"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
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

//Node returns the number of cores of the CPU
func (e *aoeEngine) Node(ip string) *engine.NodeInfo {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0).Milliseconds())
	}()
	var ni *engine.NodeInfo
	e.catalog.Driver.RaftStore().GetRouter().Every(uint64(pb.AOEGroup), true, func(shard meta.Shard, store meta.Store) bool {
		if ni != nil {
			return false
		}
		if strings.HasPrefix(store.ClientAddr, ip) {
			stats := e.catalog.Driver.RaftStore().GetRouter().GetStoreStats(store.ID)
			ni = &engine.NodeInfo{
				Mcpu: len(stats.GetCpuUsages()),
			}
		}
		return true
	})
	return ni
}

//Delete drops the database with the name.
func (e *aoeEngine) Delete(epoch uint64, name string) error {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0).Milliseconds())
	}()
	err := e.catalog.DropDatabase(epoch, name)
	return err
}

//Create creates a database with the name and the type.
func (e *aoeEngine) Create(epoch uint64, name string, typ int) error {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0).Milliseconds())
	}()
	_, err := e.catalog.CreateDatabase(epoch, name, typ)
	return err
}

//Databases returns all the databases in the catalog.
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

//Database returns the database.
//the field catalog of the returned database is the catalog of the aoeEngine.
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
