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
	"fmt"
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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/adaptor"
	aoedbName "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/aoedb/v1"
	aoedb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/aoedb/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/handle"
	aoeMeta "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"

	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/stats"
)

const (
	sPrefix   = "MetaTbl"
	sShardId  = "ShardId"
	sLogIndex = "LogIndex"
	sMetadata = "Metadata"
)

// Storage memory storage
type Storage struct {
	DB    *aoedb.DB
	stats stats.Stats
}

func (s *Storage) Sync(ids []uint64) error {
	for _, shardId := range ids {
		s.DB.FlushDatabase(aoedbName.ShardIdToName(shardId))
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
	db, err := aoedb.Open(dir, opts)
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
	ctx := aoedb.AppendCtx{
		TableMutationCtx: aoedb.TableMutationCtx{
			DBMutationCtx: aoedb.DBMutationCtx{
				Id:     index,
				Offset: offset,
				Size:   batchSize,
				DB:     aoedbName.ShardIdToName(shardId),
			},
			Table: customReq.TabletName,
		},
		Data: bat,
	}
	err = s.DB.Append(&ctx)
	if err != nil {
		resp := errDriver.ErrorResp(err)
		return 0, 0, resp
	}
	writtenBytes := uint64(len(key) + len(customReq.Data))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, nil
}

//Relation  returns a relation of the db and the table
func (s *Storage) Relation(dbname, tabletName string) (*aoedb.Relation, error) {
	return s.DB.Relation(dbname, tabletName)
}

//GetSnapshot gets the snapshot from the table.
//If there's no segment, it returns an empty snapshot.
func (s *Storage) GetSnapshot(ctx *dbi.GetSnapshotCtx) (*handle.Snapshot, error) {
	return s.DB.GetSnapshot(ctx)
}

//GetSegmentIds returns the ids of segments of the table
func (s *Storage) getSegmentIds(cmd []byte, shardId uint64) []byte {
	customReq := &pb.GetSegmentIdsRequest{}
	protoc.MustUnmarshal(customReq, cmd)
	rsp := s.DB.GetSegmentIds(aoedbName.ShardIdToName(shardId), customReq.Name)
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
func (s *Storage) createTable(index uint64, offset int, batchsize int, shardId uint64, cmd []byte, key []byte) (uint64, int64, []byte) {
	if err := s.DB.Closed.Load(); err != nil {
		panic(err)
	}
	if offset >= batchsize {
		panic(fmt.Sprintf("bad index %d: offset %d, size %d", index, offset, batchsize))
	}
	customReq := &pb.CreateTabletRequest{}
	protoc.MustUnmarshal(customReq, cmd)

	tblInfo, err := helper.DecodeTable(customReq.TableInfo)
	if err != nil {
		buf := errDriver.ErrorResp(err)
		return 0, 0, buf
	}
	schema := adaptor.TableInfoToSchema(s.DB.Store.Catalog, &tblInfo)
	ctx := aoedb.CreateTableCtx{
		DBMutationCtx: aoedb.DBMutationCtx{
			Id:     index,
			Offset: offset,
			Size:   batchsize,
			DB:     aoedbName.ShardIdToName(shardId),
		},
		Schema: schema,
	}
	tbl, err := s.DB.CreateTable(&ctx)
	if err != nil {
		buf := errDriver.ErrorResp(err)
		return 0, 0, buf
	}
	buf := codec.Uint642Bytes(tbl.Id)
	writtenBytes := uint64(len(key) + len(customReq.TableInfo))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, buf
}

//DropTable drops the table in the storage.
//If the storage is closed, it panics.
func (s *Storage) dropTable(index uint64, offset, batchsize int, shardId uint64, cmd []byte, key []byte) (uint64, int64, []byte) {
	customReq := &pb.CreateTabletRequest{}
	protoc.MustUnmarshal(customReq, cmd)

	ctx := aoedb.DropTableCtx{
		DBMutationCtx: aoedb.DBMutationCtx{
			Id:     index,
			Offset: offset,
			Size:   batchsize,
			DB:     aoedbName.ShardIdToName(shardId),
		},
		Table: customReq.Name,
	}

	tbl, err := s.DB.DropTable(&ctx)

	if err != nil {
		buf := errDriver.ErrorResp(err, "Call DropTable Failed")
		return 0, 0, buf
	}
	buf := codec.Uint642Bytes(tbl.Id)
	writtenBytes := uint64(len(key) + len(customReq.TableInfo))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, buf
}

//TableIDs returns the ids of all the tables in the storage.
func (s *Storage) tableIDs() []byte {
	var ids []uint64
	dbs := s.DB.DatabaseNames()
	for _, db := range dbs {
		tbNames, err := s.DB.TableIDs(db)
		if err != nil {
			return errDriver.ErrorResp(err)
		}
		ids = append(ids, tbNames...)
	}
	rep, _ := json.Marshal(ids)
	return rep
}

//tableNames returns the names of all the tables in the storage.
func (s *Storage) tableNames() (names []string) {
	dbs := s.DB.DatabaseNames()
	for _, db := range dbs {
		tbNames := s.DB.TableNames(db)
		names = append(names, tbNames...)
	}
	return
}

//TODO
func (s *Storage) SplitCheck(shard meta.Shard, size uint64) (currentApproximateSize uint64,
	currentApproximateKeys uint64, splitKeys [][]byte, ctx []byte, err error) {
	return 0, 0, nil, nil, err

}

//TODO
func (s *Storage) CreateSnapshot(shardID uint64, path string) (uint64, uint64, error) {
	if _, err := os.Stat(path); err != nil {
		os.MkdirAll(path, os.FileMode(0755))
	}
	return 0, 0, nil
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

func (s *Storage) GetInitialStates() ([]meta.ShardMetadata, error) {
	var values []meta.ShardMetadata
	dbs := s.DB.DatabaseNames()
	for _, db := range dbs {
		tblNames := s.DB.TableNames(db)
		for _, tblName := range tblNames {
			//TODO:Strictly judge whether it is a "shard metadata table"
			if !strings.Contains(tblName, sPrefix) {
				continue
			}
			rel, err := s.Relation(db, tblName)
			if err != nil {
				return nil, err
			}
			attrs := make([]string, 0)
			for _, ColDef := range rel.Meta.Schema.ColDefs {
				attrs = append(attrs, ColDef.Name)
			}
			rel.Data.GetBlockFactory()
			if len(rel.Meta.SegmentSet) < 1 {
				logutil.Infof("continue 1")
				continue
			}
			segment := rel.Meta.SegmentSet[len(rel.Meta.SegmentSet)-1]
			seg := rel.Segment(segment.Id, nil)
			blks := seg.Blocks()
			blk := seg.Block(blks[len(blks)-1], nil)
			cds := make([]*bytes.Buffer, len(attrs))
			dds := make([]*bytes.Buffer, len(attrs))
			for i := range cds {
				cds[i] = bytes.NewBuffer(make([]byte, 0))
				dds[i] = bytes.NewBuffer(make([]byte, 0))
			}
			refs := make([]uint64, len(attrs))
			bat, _ := blk.Read(refs, attrs, cds, dds)
			shardId := bat.GetVector(sShardId)
			logIndex := bat.GetVector(sLogIndex)
			metadate := bat.GetVector(sMetadata)
			logutil.Infof("GetInitialStates Metadata is %v\n",
				metadate.Col.(*types.Bytes).Data[:metadate.Col.(*types.Bytes).Lengths[0]])
			customReq := &meta.ShardLocalState{}
			protoc.MustUnmarshal(customReq, metadate.Col.(*types.Bytes).Data[:metadate.Col.(*types.Bytes).Lengths[0]])
			values = append(values, meta.ShardMetadata{
				ShardID:  shardId.Col.([]uint64)[0],
				LogIndex: logIndex.Col.([]uint64)[0],
				Metadata: *customReq,
			})
			logutil.Infof("GetInitialStates LogIndex is %d, ShardID is %d \n",
				logIndex.Col.([]uint64)[0], shardId.Col.([]uint64)[0])

		}
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
			writtenBytes, changedBytes, rep = s.createTable(batch.Index, idx, batchSize, shard.ID, cmd, key)
		case uint64(pb.DropTablet):
			writtenBytes, changedBytes, rep = s.dropTable(batch.Index, idx, batchSize, shard.ID, cmd, key)
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
		rep = s.getSegmentIds(cmd, ctx.Shard().ID)
	case uint64(pb.GetSegmentedId):
		rep = s.getSegmentedId(cmd)
	case uint64(pb.TabletIds):
		rep = s.tableIDs()
	}
	return rep, nil
}

func (s *Storage) GetPersistentLogIndex(shardID uint64) (uint64, error) {
	db, _ := s.DB.Store.Catalog.SimpleGetDatabaseByName(aoedbName.ShardIdToName(shardID))
	rsp := s.DB.GetShardCheckpointId(db.GetShardId())
	if rsp == 0 {
		rsp = 1
	}
	return rsp, nil
}

func (s *Storage) SaveShardMetadata(metadatas []meta.ShardMetadata) error {
	for _, metadata := range metadatas {
		tableName := sPrefix + strconv.Itoa(int(metadata.ShardID))
		db, err := s.DB.Store.Catalog.SimpleGetDatabaseByName(aoedbName.ShardIdToName(metadata.ShardID))
		if err != nil && err != aoeMeta.DatabaseNotFoundErr {
			return err
		}
		createDatabase := false
		if db == nil {
			ctx := aoedb.CreateDBCtx{
				Id:     metadata.LogIndex,
				Offset: 0,
				Size:   3,
				DB:     aoedbName.ShardIdToName(metadata.ShardID),
			}
			db, err = s.DB.CreateDatabase(&ctx)
			if err != nil {
				return err
			}
			createDatabase = true
		}
		tbl, err := s.DB.Store.Catalog.SimpleGetTableByName(aoedbName.ShardIdToName(metadata.ShardID), tableName)
		if err != nil && err != aoeMeta.TableNotFoundErr {
			return err
		}
		createTable := false
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
			colInfo.Type = types.Type{Oid: types.T(types.T_varchar)}
			mateTblInfo.Columns = append(mateTblInfo.Columns, colInfo)
			offset := 0
			size := 2
			if createDatabase {
				offset = 1
				size = 3
			}
			schema := adaptor.TableInfoToSchema(s.DB.Store.Catalog, &mateTblInfo)
			ctx := aoedb.CreateTableCtx{
				DBMutationCtx: aoedb.DBMutationCtx{
					Id:     metadata.LogIndex,
					Offset: offset,
					Size:   size,
					DB:     aoedbName.ShardIdToName(metadata.ShardID),
				},
				Schema: schema,
			}
			_, err = s.DB.CreateTable(&ctx)
			if err != nil {
				return err
			}
			createTable = true
		}

		attrs := []string{sShardId, sLogIndex, sMetadata}
		bat := batch.New(true, attrs)
		vShardID := vector.New(types.Type{Oid: types.T_uint64, Size: 8})
		vShardID.Ref = 1
		vShardID.Col = []uint64{metadata.ShardID}
		bat.Vecs[0] = vShardID
		vLogIndex := vector.New(types.Type{Oid: types.T_uint64, Size: 8})
		vLogIndex.Ref = 1
		vLogIndex.Col = []uint64{metadata.LogIndex}
		bat.Vecs[1] = vLogIndex
		vMetadata := vector.New(types.Type{Oid: types.T_varchar, Size: int32(len(protoc.MustMarshal(&metadata.Metadata)))})
		vMetadata.Ref = 1
		vMetadata.Col = &types.Bytes{
			Data:    protoc.MustMarshal(&metadata.Metadata),
			Offsets: []uint32{0},
			Lengths: []uint32{uint32(len(protoc.MustMarshal(&metadata.Metadata)))},
		}
		bat.Vecs[2] = vMetadata

		offset := 0
		size := 1
		if createTable {
			offset = 1
			size = 2
		}
		if createDatabase {
			offset = 2
			size = 3
		}
		ctx := aoedb.AppendCtx{
			TableMutationCtx: aoedb.TableMutationCtx{
				DBMutationCtx: aoedb.DBMutationCtx{
					Id:     metadata.LogIndex,
					Offset: offset,
					Size:   size,
					DB:     aoedbName.ShardIdToName(metadata.ShardID),
				},
				Table: tableName,
			},
			Data: bat,
		}
		err = s.DB.Append(&ctx)
		if err != nil {
			db, _ := s.DB.Store.Catalog.SimpleGetDatabaseByName(aoedbName.ShardIdToName(metadata.ShardID))
			index := ctx.ToLogIndex(db)
			tbl, _ := db.GetTableByNameAndLogIndex(tableName, index)
			tbls := s.DB.TableNames(aoedbName.ShardIdToName(metadata.ShardID))
			logutil.Infof("SaveShardMetadata, offset is %v, tbl is %v,index is %v, table name is %v", offset, tbl, index, tableName)
			logutil.Infof("SaveShardMetadata, shard id is %v, tbls are %v", metadata.ShardID, tbls)
			logutil.Errorf("SaveShardMetadata is failed: %v\n", err.Error())
			return err
		}
	}
	return nil
}

func (s *Storage) RemoveShard(shard meta.Shard, removeData bool) error {
	var err error
	if removeData {
		ctx := aoedb.DropDBCtx{
			Id:     shard.ID,
			Offset: 0,
			Size:   1,
			DB:     aoedbName.ShardIdToName(shard.ID),
		}
		_, err = s.DB.DropDatabase(&ctx)
	}
	return err
}

func (s *Storage) Split(old meta.ShardMetadata, news []meta.ShardMetadata, ctx []byte) error {
	return nil
}
