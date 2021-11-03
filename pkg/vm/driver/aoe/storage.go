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

package aoe

import (
	"bytes"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/protocol"
	errDriver "github.com/matrixorigin/matrixone/pkg/vm/driver/error"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/pb"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/codec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/helper"
	store "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	adb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/handle"

	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/stats"
)

const (
	sPrefix             = "MateTbl"
	sShardId           	= "ShardId"
	sLogIndex         	= "LogIndex"
	sMetadata        	= "Metadata"
)

// Storage memory storage
type Storage struct {
	DB    *adb.DB
	stats stats.Stats
}

func (s *Storage) Sync(ids []uint64) error {
	for _, shardId := range ids {
		s.DB.Flush(sPrefix + strconv.Itoa(int(shardId)))
	}
	//TODO: implement me
	return nil
}

// NewStorage returns pebble kv store on a default options
func NewStorage(dir string) (*Storage, error) {
	return NewStorageWithOptions(dir, &store.Options{})
}

// NewStorageWithOptions returns badger kv store
func NewStorageWithOptions(dir string, opts *store.Options) (*Storage, error) {
	db, err := adb.OpenWithWalBroker(dir, opts)
	if err != nil {
		return nil, err
	}
	return &Storage{
		DB: db,
	}, nil
}

//Stats returns the stats of the Storage
func (s *Storage) Stats() stats.Stats {
	return s.stats
}

//Append appends batch in the table
func (s *Storage) Append(index uint64, offset int, batchSize int, shardId uint64, cmd []byte, key []byte) (uint64, int64, []byte) {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("[logIndex:%d,%d]append handler cost %d ms", index, offset, time.Since(t0).Milliseconds())
	}()
	customReq := &pb.AppendRequest{}
	protoc.MustUnmarshal(customReq, cmd)
	bat, _, err := protocol.DecodeBatch(customReq.Data)
	if err != nil {
		resp := errDriver.ErrorResp(err)
		return 0, 0, resp
	}
	size := 0
	for _, vec := range bat.Vecs {
		size += len(vec.Data)
	}
	atomic.AddUint64(&s.stats.WrittenKeys, uint64(bat.Vecs[0].Length()))
	atomic.AddUint64(&s.stats.WrittenBytes, uint64(size))
	err = s.DB.Append(dbi.AppendCtx{
		ShardId:   shardId,
		OpIndex:   index,
		OpOffset:  offset,
		OpSize:    batchSize,
		TableName: customReq.TabletName,
		Data:      bat,
	})
	if err != nil {
		resp := errDriver.ErrorResp(err)
		return 0, 0, resp
	}
	writtenBytes := uint64(len(key) + len(customReq.Data))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, nil
}

//Relation  returns a relation of the db and the table
func (s *Storage) Relation(tabletName string) (*adb.Relation, error) {
	return s.DB.Relation(tabletName)
}

//GetSnapshot gets the snapshot from the table.
//If there's no segment, it returns an empty snapshot.
func (s *Storage) GetSnapshot(ctx *dbi.GetSnapshotCtx) (*handle.Snapshot, error) {
	return s.DB.GetSnapshot(ctx)
}

//GetSegmentIds returns the ids of segments of the table
func (s *Storage) getSegmentIds(cmd []byte) []byte {
	customReq := &pb.GetSegmentIdsRequest{}
	protoc.MustUnmarshal(customReq, cmd)
	rsp := s.DB.GetSegmentIds(dbi.GetSegmentsCtx{
		TableName: customReq.Name,
	})
	resp, _ := json.Marshal(rsp)
	return resp
}

//GetShardPesistedId returns the smallest segmente id among the tables starts with prefix
func (s *Storage) GetShardPesistedId(shardId uint64) uint64 {
	return s.DB.GetShardCheckpointId(shardId)
}

//GetSegmentedId returns the smallest segmente id among the tables starts with prefix
func (s *Storage) getSegmentedId(cmd []byte) []byte {
	customReq := &pb.GetSegmentedIdRequest{}
	protoc.MustUnmarshal(customReq, cmd)
	rsp := s.GetShardPesistedId(customReq.ShardId)
	resp := codec.Uint642Bytes(rsp)
	return resp
}

//CreateTable creates a table in the storage.
//It returns the id of the created table.
//If the storage is closed, it panics.
func (s *Storage) createTable(index uint64, shardId uint64, cmd []byte, key []byte) (uint64, int64, []byte) {
	customReq := &pb.CreateTabletRequest{}
	protoc.MustUnmarshal(customReq, cmd)
	t, err := helper.DecodeTable(customReq.TableInfo)
	if err != nil {
		buf := errDriver.ErrorResp(err)

		return 0, 0, buf
	}
	id, err := s.DB.CreateTable(&t, dbi.TableOpCtx{
		ShardId:   shardId,
		OpIndex:   index,
		TableName: customReq.Name,
	})
	if err != nil {
		buf := errDriver.ErrorResp(err, "Call CreateTable Failed")
		return 0, 0, buf
	}
	buf := codec.Uint642Bytes(id)
	writtenBytes := uint64(len(key) + len(customReq.TableInfo))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, buf
}

//DropTable drops the table in the storage.
//If the storage is closed, it panics.
func (s *Storage) dropTable(index uint64, shardId uint64, cmd []byte, key []byte) (uint64, int64, []byte) {
	customReq := &pb.CreateTabletRequest{}
	protoc.MustUnmarshal(customReq, cmd)
	id, err := s.DB.DropTable(dbi.DropTableCtx{
		ShardId:   shardId,
		OpIndex:   index,
		TableName: customReq.Name,
	})
	if err != nil {
		buf := errDriver.ErrorResp(err, "Call CreateTable Failed")
		return 0, 0, buf
	}
	buf := codec.Uint642Bytes(id)
	writtenBytes := uint64(len(key) + len(customReq.TableInfo))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, buf
}

//TableIDs returns the ids of all the tables in the storage.
func (s *Storage) TableIDs() (ids []uint64, err error) {
	return s.DB.TableIDs()
}

//TableIDs returns the names of all the tables in the storage.
func (s *Storage) tableNames() (ids []string) {
	return s.DB.TableNames()
}

//TODO
func (s *Storage) SplitCheck(start []byte, end []byte, size uint64) (currentSize uint64, currentKeys uint64, splitKeys [][]byte, err error) {
	return 0, 0, nil, err

}

//TODO
func (s *Storage) CreateSnapshot(shardID uint64, path string) (uint64, error) {
	if _, err := os.Stat(path); err != nil {
		os.MkdirAll(path, os.FileMode(0755))
	}
	return 0, nil
}

//TODO
func (s *Storage) ApplySnapshot(shardID uint64, path string) error {
	return nil
}

//Close closes the storage.
func (s *Storage) Close() error {
	return s.DB.Close()
}

func (s *Storage) NewWriteBatch() storage.Resetable {
	return nil
}

func (s *Storage) GetInitialStates() ([]storage.ShardMetadata, error) {
	tblNames := s.tableNames()
	var values []storage.ShardMetadata
	logutil.Infof("tblNames len  is %d\n", len(tblNames))
	for _, tblName := range tblNames {
		//TODO:Strictly judge whether it is a "shard metadata table"
		if !strings.Contains(tblName, sPrefix) {
			continue
		}
		rel, err := s.Relation(tblName)
		if err != nil {
			return nil, err
		}
		attrs := make([]string, 0)
		for _, ColDef := range rel.Meta.Schema.ColDefs {
			attrs = append(attrs, ColDef.Name)
		}
		rel.Data.GetBlockFactory()
		if len(rel.Meta.SegmentSet) < 1 {
			continue
		}
		segment := rel.Meta.SegmentSet[len(rel.Meta.SegmentSet)-1]
		seg := rel.Segment(segment.Id, nil)
		blks := seg.Blocks()
		blk := seg.Block(blks[len(blks)-1], nil)
		cds := make([]*bytes.Buffer, 3)
		dds := make([]*bytes.Buffer, 3)
		for i := range cds {
			cds[i] = bytes.NewBuffer(make([]byte, 0))
			dds[i] = bytes.NewBuffer(make([]byte, 0))
		}
		refs := make([]uint64, len(attrs))
		bat, _ := blk.Read(refs, attrs, cds, dds)
		shardId := bat.GetVector(attrs[0])
		logIndex := bat.GetVector(attrs[1])
		metadate := bat.GetVector(attrs[2])
		logutil.Infof("GetInitialStates Metadata is %v\n",
			metadate.Col.(*types.Bytes).Data[:metadate.Col.(*types.Bytes).Lengths[0]])
		values = append(values, storage.ShardMetadata{
			ShardID:  shardId.Col.([]uint64)[0],
			LogIndex: logIndex.Col.([]uint64)[0],
			Metadata: metadate.Col.(*types.Bytes).Data[:metadate.Col.(*types.Bytes).Lengths[0]],
		})
	}
	return values, nil
}

func (s *Storage) Write(ctx storage.WriteContext) error {
	batch := ctx.Batch()
	batchSize := len(batch.Requests)
	shard := ctx.Shard()
	for idx, r := range batch.Requests {
		cmd := r.Cmd
		CmdType := r.CmdType
		key := r.Key
		var rep []byte
		var writtenBytes uint64
		var changedBytes int64
		switch CmdType {
		case uint64(pb.CreateTablet):
			writtenBytes, changedBytes, rep = s.createTable(batch.Index, shard.ID, cmd, key)
		case uint64(pb.DropTablet):
			writtenBytes, changedBytes, rep = s.dropTable(batch.Index, shard.ID, cmd, key)
		case uint64(pb.Append):
			writtenBytes, changedBytes, rep = s.Append(batch.Index, idx, batchSize, shard.ID, cmd, key)
		}
		ctx.AppendResponse(rep)
		ctx.SetWrittenBytes(writtenBytes)
		ctx.SetDiffBytes(changedBytes)
	}
	return nil
}

func (s *Storage) Read(ctx storage.ReadContext) ([]byte, error) {
	Request := ctx.Request()
	cmd := Request.Cmd
	CmdType := Request.CmdType
	var rep []byte
	switch CmdType {
	case uint64(pb.TabletNames):
		rsp := s.tableNames()
		rep, _ = json.Marshal(rsp)
	case uint64(pb.GetSegmentIds):
		rep = s.getSegmentIds(cmd)
	case uint64(pb.GetSegmentedId):
		rep = s.getSegmentedId(cmd)
	}
	return rep, nil
}

func (s *Storage) GetPersistentLogIndex(shardID uint64) (uint64, error) {
	rsp, err := s.DB.GetSegmentedId(dbi.GetSegmentedIdCtx{
		Matchers: []*dbi.StringMatcher{
			{
				Type:    dbi.MTPrefix,
				Pattern: codec.Uint642String(shardID),
			},
		},
	})
	if err != nil {
		if err == adb.ErrNotFound {
			rsp = 0
		} else {
			panic(err)
		}
		return rsp, nil
	}
	return rsp, nil
}

func (s *Storage) SaveShardMetadata(metadatas []storage.ShardMetadata) error {
	for _, metadata := range metadatas {
		tableName := sPrefix + strconv.Itoa(int(metadata.ShardID))
		tbl := s.DB.Store.Catalog.SimpleGetTableByName(tableName)
		if tbl == nil {
			mateTblInfo := aoe.TableInfo{
				Name:    tableName,
				Indices: make([]aoe.IndexInfo, 0),
			}
			ShardId := aoe.ColumnInfo{
				Name: sShardId,
			}
			ShardId.Type = types.Type{Oid: types.T_uint64, Size: 8}
			mateTblInfo.Columns = append(mateTblInfo.Columns, ShardId)
			LogIndex := aoe.ColumnInfo{
				Name: sLogIndex,
			}
			LogIndex.Type = types.Type{Oid: types.T_uint64, Size: 8}
			mateTblInfo.Columns = append(mateTblInfo.Columns, LogIndex)
			colInfo := aoe.ColumnInfo{
				Name: sMetadata,
			}
			colInfo.Type = types.Type{Oid: types.T(types.T_varchar), Size: 128}
			mateTblInfo.Columns = append(mateTblInfo.Columns, colInfo)
			_, err := s.DB.CreateTable(&mateTblInfo, dbi.TableOpCtx{
				ShardId:   metadata.ShardID,
				OpIndex:   metadata.LogIndex,
				TableName: tableName,
			})
			if err != nil {
				logutil.Errorf("CreateTable is failed: %v\n", err.Error())
				return err
			}
			tbl = s.DB.Store.Catalog.SimpleGetTableByName(tableName)
		}
		var attrs []string
		for _, colDef := range tbl.Schema.ColDefs {
			attrs = append(attrs, colDef.Name)
		}
		bat := batch.New(true, attrs)
		vMetadata := vector.New(types.Type{Oid: types.T_varchar, Size: int32(len(metadata.Metadata))})
		vMetadata.Ref = 1
		logutil.Infof("SaveShardMetadata Metadata is %v\n", metadata.Metadata)
		vMetadata.Col = &types.Bytes{
			Data:    metadata.Metadata,
			Offsets: []uint32{0},
			Lengths: []uint32{uint32(len(metadata.Metadata))},
		}
		bat.Vecs[2] = vMetadata
		vShardID := vector.New(types.Type{Oid: types.T_uint64, Size: 8})
		vShardID.Ref = 1
		vShardID.Col = []uint64{metadata.ShardID}
		bat.Vecs[0] = vShardID
		vLogIndex := vector.New(types.Type{Oid: types.T_uint64, Size: 8})
		vLogIndex.Ref = 1
		vLogIndex.Col = []uint64{metadata.LogIndex}
		bat.Vecs[1] = vLogIndex
		err := s.DB.Append(dbi.AppendCtx{
			ShardId:   metadata.ShardID,
			OpIndex:   metadata.LogIndex,
			OpOffset:  0,
			OpSize:    1,
			TableName: sPrefix + strconv.Itoa(int(metadata.ShardID)),
			Data:      bat,
		})
		if err != nil {
			logutil.Errorf("SaveShardMetadata is failed: %v\n", err.Error())
			return err
		}
	}
	return nil
}

func (s *Storage) RemoveShardData(shard meta.Shard) error {
	return nil
}
