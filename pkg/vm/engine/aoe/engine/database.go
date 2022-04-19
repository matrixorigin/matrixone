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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/codec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/helper"
	adb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/aoedb/v1"
	aoedbName "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/aoedb/v1"
	log "github.com/sirupsen/logrus"

	//"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"time"
)

//Type return the type of the database
func (db *database) Type() int {
	return db.typ
}

//Delete deletes the table.
func (db *database) Delete(epoch uint64, name string) error {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0).Milliseconds())
	}()
	_, err := db.catalog.DropTable(epoch, db.id, name)
	return err
}

//Create creates the table
func (db *database) Create(epoch uint64, name string, defs []engine.TableDef) error {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0).Milliseconds())
	}()
	tbl, err := helper.Transfer(db.id, 0, 0, name, defs)
	if err != nil {
		return err
	}
	_, err = db.catalog.CreateTable(epoch, db.id, tbl)
	return err
}

//Relations returns names of all the tables in the database.
func (db *database) Relations() []string {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0).Milliseconds())
	}()
	rs := make([]string, 0)
	tbs, err := db.catalog.ListTables(db.id)
	if err != nil {
		return rs
	}
	for _, tb := range tbs {
		rs = append(rs, tb.Name)
	}
	return rs
}

//Relation returns an instance with the given name.
func (db *database) Relation(name string) (engine.Relation, error) {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0).Milliseconds())
	}()
	tablets, err := db.catalog.GetTablets(db.id, name)
	if err != nil {
		return nil, err
	}
	if tablets == nil || len(tablets) == 0 {
		return nil, catalog.ErrTableNotExists
	}

	r := &relation{
		mu:      sync.Mutex{},
		pid:     db.id,
		tbl:     &tablets[0].Table,
		catalog: db.catalog,
		mp:      make(map[string]*adb.Relation),
		cfg:     db.cfg,
	}
	r.tablets = tablets
	ldb := db.catalog.Driver.AOEStore()
	for _, tbl := range tablets {
		if ids, err := db.catalog.Driver.GetSegmentIds(tbl.Name, tbl.ShardId); err != nil {
			log.Errorf("get segmentInfos for tablet %s failed, %s", tbl.Name, err.Error())
			return nil, err
		} else {
			if len(ids.Ids) == 0 {
				continue
			}
			addr := db.catalog.Driver.RaftStore().GetRouter().LeaderReplicaStore(tbl.ShardId).ClientAddress
			storeId := db.catalog.Driver.RaftStore().GetRouter().LeaderReplicaStore(tbl.ShardId).ID
			if lRelation, err := ldb.Relation(aoedbName.IdToNameFactory.Encode(tbl.ShardId), tbl.Name); err == nil {
				r.mp[string(codec.Uint642Bytes(tbl.ShardId))] = lRelation
			}
			logutil.Debugf("ClientAddr: %v, shardId: %d, storeId: %d", addr, tbl.ShardId, storeId)
			if !Exist(r.nodes, addr) {
				r.nodes = append(r.nodes, engine.Node{
					Id:   string(codec.Uint642Bytes(storeId)),
					Addr: addr,
				})
			}
			for _, id := range ids.Ids {
				if storeId != db.catalog.Driver.RaftStore().Meta().ID {
					continue
				}
				logutil.Debugf("shardId: %d, segment: %d, Id: %d",
					tbl.ShardId, id, db.catalog.Driver.RaftStore().Meta().ID)
				r.segments = append(r.segments, SegmentInfo{
					Version:  ids.Version,
					Id:       string(codec.Uint642Bytes(id)),
					GroupId:  string(codec.Uint642Bytes(tbl.ShardId)),
					TabletId: string(codec.Uint642Bytes(tbl.ShardId)),
					Node: engine.Node{
						Id:   string(codec.Uint642Bytes(storeId)),
						Addr: addr,
					},
				})
			}

		}
	}
	logutil.Infof("nodes: %v, tablets len: %d", r.nodes, len(tablets))
	return r, nil
}

func Exist(nodes engine.Nodes, addr string) bool {
	exist := false
	for _, node := range nodes {
		if node.Addr == addr {
			exist = true
		}
	}
	return exist
}
