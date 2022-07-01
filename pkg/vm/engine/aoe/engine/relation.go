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
	"math/rand"
	"time"

	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/codec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/helper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/protocol"
	adb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/aoedb/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	log "github.com/sirupsen/logrus"
)

var FilterTypeMap = map[int]int32{
	overload.EQ: FileterEq,
	overload.NE: FileterNe,
	overload.LT: FileterLt,
	overload.LE: FileterLe,
	overload.GT: FileterGt,
	overload.GE: FileterGe,
}

const defaultRetryTimes = 5

//Close closes the relation. It closes all relations of the tablet in the aoe store.
func (r *relation) Close(_ engine.Snapshot) {
	for _, v := range r.mp {
		v.Close()
	}
}

//ID returns the name of the table.
func (r *relation) ID(_ engine.Snapshot) string {
	return r.tbl.Name
}

//Segment returns the segment according to the segmentInfo.
func (r *relation) Segment(si SegmentInfo) aoe.Segment {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("Segment time cost %d ms", time.Since(t0).Milliseconds())
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
func (r *relation) Write(_ uint64, bat *batch.Batch, _ engine.Snapshot) error {
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

func (r *relation) Delete(_ uint64, _ *vector.Vector, _ string, _ engine.Snapshot) error {
	return nil
}

func (r *relation) Update(_ uint64, bat *batch.Batch, _ engine.Snapshot) error {
	return errors.New("doesn't support now")
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
	if len(tablets) == 0 {
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
				adb.IDToNameFactory.Encode(
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

func (r *relation) AddAttribute(_ uint64, _ engine.TableDef, _ engine.Snapshot) error {
	return nil
}

func (r *relation) DelAttribute(_ uint64, _ engine.TableDef, _ engine.Snapshot) error {
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

func (r *relation) Nodes(_ engine.Snapshot) engine.Nodes {
	return r.nodes
}

func (r *relation) GetPrimaryKeys(_ engine.Snapshot) []*engine.Attribute {
	return nil
}

func (r *relation) Truncate(_ engine.Snapshot) (uint64, error) {
	panic(any("implement me"))
}

func (r *relation) GetHideKey(_ engine.Snapshot) *engine.Attribute {
	return nil
}

func (r *relation) GetPriKeyOrHideKey(_ engine.Snapshot) ([]engine.Attribute, bool) {
	return nil, false
}

func (r *relation) TableDefs(_ engine.Snapshot) []engine.TableDef {
	_, _, _, _, defs, _ := helper.UnTransfer(*r.tbl)
	return defs
}

func (r *relation) AddTableDef(u uint64, def engine.TableDef, _ engine.Snapshot) error {
	return nil
}

func (r *relation) DelTableDef(u uint64, def engine.TableDef, _ engine.Snapshot) error {
	return nil
}

func (r *relation) NewReader(num int, e extend.Extend, _ []byte, _ engine.Snapshot) []engine.Reader {
	fcs := getFilterContext(e)
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
		readStore.readers[i] = &aoeReader{reader: readStore, id: int32(i), workerid: int32(workerid), filter: fcs}
	}
	for i := 0; i < readStore.iodepth; i++ {
		readStore.rhs[i] = make(chan *batData,
			int(r.cfg.QueueMaxReaderCount)*int(r.cfg.ReaderBufferCount))
		readStore.chs[i] = make(chan *batData,
			int(r.cfg.QueueMaxReaderCount)*int(r.cfg.ReaderBufferCount))
	}
	return readStore.readers
}

// only filter conditions similar to the following are supported:
//  	. a > 1
// 		. a > 1 and b < 2
func getFilterContext(e extend.Extend) []filterContext {
	var fcs []filterContext

	es := extend.AndExtends(e, nil)
	if len(es) == 0 {
		return nil
	}
	for i := range es {
		fc := new(filterContext)
		fc.extent = make([]filterExtent, 0)
		fc = getFilterContextFromExtend(fc, es[i])
		if fc != nil && len(fc.extent) > 0 {
			fcs = append(fcs, *fc)
		}
	}
	return fcs
}

func getFilterContextFromExtend(f *filterContext, e extend.Extend) *filterContext {
	v, ok := e.(*extend.BinaryExtend)
	if !ok {
		return nil
	}
	if v.Op == overload.Or {
		getFilterContextFromExtend(f, v.Left)
		getFilterContextFromExtend(f, v.Right)
		return f
	}
	ft, ok := FilterTypeMap[v.Op]
	if ok {
		f = newFilterContext(ft, v, f)
	}
	return f
}

func newFilterContext(ft int32, e *extend.BinaryExtend, fcs *filterContext) *filterContext {
	if attr, ok := e.Left.(*extend.Attribute); ok {
		if val, ok := e.Right.(*extend.ValueExtend); ok {
			param := cast(val.V, attr.Type)
			if param == nil {
				return fcs
			}
			fcs.extent = append(fcs.extent, filterExtent{
				param1:     param,
				filterType: ft,
				attr:       attr.Name,
			})
		}
	}
	if attr, ok := e.Right.(*extend.Attribute); ok {
		if val, ok := e.Left.(*extend.ValueExtend); ok {
			param := cast(val.V, attr.Type)
			if param == nil {
				return fcs
			}
			fcs.extent = append(fcs.extent, filterExtent{
				param1:     param,
				filterType: ft,
				attr:       attr.Name,
			})
		}
	}
	return fcs
}

func cast(vec *vector.Vector, typ types.T) interface{} {
	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	proc := process.New(mheap.New(gm))
	ops := overload.BinOps[overload.Typecast]
	retVec := vec
	for _, op := range ops {
		if op.LeftType == vec.Typ.Oid && op.RightType == typ {
			retVec, _ = op.Fn(vec, vector.New(types.Type{Oid: typ}), proc, true, true)
			if retVec == nil {
				return nil
			}
		}
	}
	return getVectorValue(retVec)
}

func getVectorValue(vec *vector.Vector) interface{} {
	switch vec.Typ.Oid {
	case types.T_int8:
		return vec.Col.([]int8)[0]
	case types.T_int16:
		return vec.Col.([]int16)[0]
	case types.T_int32:
		return vec.Col.([]int32)[0]
	case types.T_int64:
		return vec.Col.([]int64)[0]
	case types.T_uint8:
		return vec.Col.([]uint8)[0]
	case types.T_uint16:
		return vec.Col.([]uint16)[0]
	case types.T_uint32:
		return vec.Col.([]uint32)[0]
	case types.T_uint64:
		return vec.Col.([]uint64)[0]
	case types.T_float32:
		return vec.Col.([]float32)[0]
	case types.T_float64:
		return vec.Col.([]float64)[0]
	case types.T_date:
		return vec.Col.([]types.Date)[0]
	case types.T_datetime:
		return vec.Col.([]types.Datetime)[0]
	case types.T_char, types.T_varchar:
		return vec.Col.(*types.Bytes).Data
	}
	return nil
}
