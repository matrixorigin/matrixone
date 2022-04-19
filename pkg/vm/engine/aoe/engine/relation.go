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
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"math/rand"

	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	log "github.com/sirupsen/logrus"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/codec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/helper"
	adb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/aoedb/v1"
	aoedbName "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/aoedb/v1"

	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/protocol"
)

const defaultRetryTimes = 5

//Close closes the relation. It closes all relations of the tablet in the aoe store.
func (r *relation) Close() {
	for _, v := range r.mp {
		v.Close()
	}
}

//ID returns the name of the table.
func (r *relation) ID() string {
	return r.tbl.Name
}

//Segment returns the segment according to the segmentInfo.
func (r *relation) Segment(si SegmentInfo) aoe.Segment {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0))
	}()
	return r.mp[si.TabletId].Segment(binary.BigEndian.Uint64([]byte(si.Id)))
}

//Segments returns all the SegmentIfo in the relation.
func (r *relation) Segments() []SegmentInfo {
	return r.segments
}

//Index returns all the indexes of the table.
func (r *relation) Index() []*engine.IndexTableDef {
	return helper.Index(*r.tbl)
}

//Attribute returns all the attributes of the table.
func (r *relation) Attribute() []engine.Attribute {
	return helper.Attribute(*r.tbl)
}

//Attribute writes the batch into the table.
func (r *relation) Write(_ uint64, bat *batch.Batch) error {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0).Milliseconds())
	}()
	if len(r.tablets) == 0 {
		return errors.New("no tablets exists")
	}
	var buf bytes.Buffer
	if err := protocol.EncodeBatch(bat, &buf); err != nil {
		return err
	}
	if buf.Len() == 0 {
		return errors.New("empty batch")
	}
	var err error
	for i := 0; i < defaultRetryTimes; i++ {
		r.mu.Lock()
		targetTbl := r.tablets[rand.Intn(len(r.tablets))]
		r.mu.Unlock()
		err = r.catalog.Driver.Append(targetTbl.Name, targetTbl.ShardId, buf.Bytes())
		if err == nil {
			break
		}
		if raftstore.IsShardUnavailableErr(err) {
			err = r.update()
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	return err
}

func (r *relation) update() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0).Milliseconds())
	}()
	dbid := r.tbl.SchemaId
	tblName := r.tbl.Name
	tablets, err := r.catalog.GetTablets(dbid, tblName)
	if err != nil {
		return err
	}
	if tablets == nil || len(tablets) == 0 {
		return catalog.ErrTableNotExists
	}

	r.tbl = &tablets[0].Table
	r.mp = make(map[string]*adb.Relation)
	r.tablets = tablets
	ldb := r.catalog.Driver.AOEStore()
	for _, tbl := range tablets {
		if ids, err := r.catalog.Driver.GetSegmentIds(
			tbl.Name, tbl.ShardId); err != nil {
			log.Errorf(
				"get segmentInfos for tablet %s failed, %s",
				tbl.Name, err.Error())
			return err
		} else {
			if len(ids.Ids) == 0 {
				continue
			}
			addr := r.catalog.Driver.RaftStore().GetRouter().
				LeaderReplicaStore(tbl.ShardId).ClientAddress
			storeId := r.catalog.Driver.RaftStore().
				GetRouter().LeaderReplicaStore(tbl.ShardId).ID
			if lRelation, err := ldb.Relation(
				aoedbName.IdToNameFactory.Encode(
					tbl.ShardId), tbl.Name); err == nil {
				r.mp[string(codec.Uint642Bytes(tbl.ShardId))] = lRelation
			}
			logutil.Debugf(
				"ClientAddr: %v, shardId: %d, storeId: %d",
				addr, tbl.ShardId, storeId)
			if !Exist(r.nodes, addr) {
				r.nodes = append(r.nodes, engine.Node{
					Id: string(
						codec.Uint642Bytes(storeId)),
					Addr: addr,
				})
			}
			for _, id := range ids.Ids {
				if storeId != r.
					catalog.Driver.RaftStore().Meta().ID {
					continue
				}
				logutil.Debugf(
					"shardId: %d, segment: %d, Id: %d",
					tbl.ShardId, id, r.catalog.Driver.RaftStore().Meta().ID)
				r.segments = append(r.segments, SegmentInfo{
					Version: ids.Version,
					Id:      string(codec.Uint642Bytes(id)),
					GroupId: string(
						codec.Uint642Bytes(tbl.ShardId)),
					TabletId: string(
						codec.Uint642Bytes(tbl.ShardId)),
					Node: engine.Node{
						Id: string(
							codec.Uint642Bytes(storeId)),
						Addr: addr,
					},
				})
			}

		}
	}
	logutil.Infof("nodes is %v", r.nodes)
	return nil
}

func (r *relation) CreateIndex(epoch uint64, defs []engine.TableDef) error {
	idxInfo := helper.IndexDefs(r.pid, r.tbl.Id, nil, defs)
	//TODO
	return r.catalog.CreateIndex(epoch, idxInfo[0])
}

func (r *relation) DropIndex(epoch uint64, name string) error {
	return r.catalog.DropIndex(epoch, r.tbl.Id, r.tbl.SchemaId, name)
}

func (r *relation) AddAttribute(_ uint64, _ engine.TableDef) error {
	return nil
}

func (r *relation) DelAttribute(_ uint64, _ engine.TableDef) error {
	return nil
}

func (r *relation) Rows() int64 {
	totalRows := int64(0)
	for _, aoeRelation := range r.mp {
		totalRows += aoeRelation.Rows()
	}
	return totalRows
}

func (r *relation) Size(attr string) int64 {
	totalSize := int64(0)
	for _, aoeRelation := range r.mp {
		totalSize += aoeRelation.Size(attr)
	}
	return totalSize
}

func (r *relation) Cardinality(_ string) int64 {
	return 0
}

func (r *relation) Nodes() engine.Nodes {
	return r.nodes
}

func (r *relation) GetPriKeyOrHideKey() ([]engine.Attribute, bool) {
	return nil, false
}

func (r *relation) TableDefs() []engine.TableDef {
	_, _, _, _, defs, _ := helper.UnTransfer(*r.tbl)
	return defs
}

func (r *relation) AddTableDef(u uint64, def engine.TableDef) error {
	return nil
}

func (r *relation) DelTableDef(u uint64, def engine.TableDef) error {
	return nil
}

func (r *relation) NewReader(num int, _ extend.Extend, _ []byte) []engine.Reader {
	iodepth := num / int(r.cfg.QueueMaxReaderCount)
	if num%int(r.cfg.QueueMaxReaderCount) > 0 {
		iodepth++
	}
	readStore := &store{
		iodepth: iodepth,
		start:   false,
		readers: make([]engine.Reader, num),
		rel:     r,
	}
	readStore.rhs = make([]chan *batData, readStore.iodepth)
	readStore.chs = make([]chan *batData, readStore.iodepth)
	var i int
	logutil.Infof("segments is %d", len(r.segments))
	if len(r.segments) == 0 {
		for i = 0; i < num; i++ {
			readStore.readers[i] = &aoeReader{reader: nil}
		}
		return readStore.readers
	}
	blocks := make([]aoe.Block, 0)
	for _, sid := range r.segments {
		segment := r.Segment(sid)
		ids := segment.Blocks()
		for _, id := range ids {
			blocks = append(blocks, segment.Block(id))
		}
	}
	readStore.SetBlocks(blocks)
	for i := 0; i < num; i++ {
		workerid := i / int(r.cfg.QueueMaxReaderCount)
		readStore.readers[i] = &aoeReader{reader: readStore, id: int32(i), workerid: int32(workerid), filter: make([]filterContext, 0)}
	}
	for i := 0; i < readStore.iodepth; i++ {
		readStore.rhs[i] = make(chan *batData,
			int(r.cfg.QueueMaxReaderCount)*int(r.cfg.ReaderBufferCount))
		readStore.chs[i] = make(chan *batData,
			int(r.cfg.QueueMaxReaderCount)*int(r.cfg.ReaderBufferCount))
	}
	return readStore.readers
}
