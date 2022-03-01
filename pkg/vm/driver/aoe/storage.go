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
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	errDriver "github.com/matrixorigin/matrixone/pkg/vm/driver/error"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/pb"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/codec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/helper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/protocol"
	store "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/adaptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/aoedb/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
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
		err := s.DB.FlushDatabase(aoedb.IdToNameFactory.Encode(shardId))
		logutil.Infof("flush shard %v", shardId)
		if err != nil {
			return err
		}
	}
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

func (s *Storage) createIndex(index uint64, offset int, batchSize int, shardId uint64, cmd []byte, key []byte) (uint64, int64, []byte) {
	if err := s.DB.Closed.Load(); err != nil {
		panic(err)
	}
	if offset >= batchSize {
		panic(fmt.Sprintf("bad index %d: offset %d, size %d", index, offset, batchSize))
	}
	t0 := time.Now()
	defer func() {
		logutil.Debugf("[S-%d|logIndex:%d,%d]createIndex handler cost %d ms", shardId, index, offset, time.Since(t0).Milliseconds())
	}()
	customReq := &pb.CreateIndexRequest{}
	protoc.MustUnmarshal(customReq, cmd)
	indiceInfo, err := helper.DecodeIndex(customReq.Indices)
	indice := adaptor.IndiceInfoToIndiceSchema(&indiceInfo)
	if err != nil {
		resp := errDriver.ErrorResp(err)
		return 0, 0, resp
	}
	ctx := aoedb.CreateIndexCtx{
		DBMutationCtx: aoedb.DBMutationCtx{
			Id:     index,
			Offset: offset,
			Size:   batchSize,
			DB:     aoedb.IdToNameFactory.Encode(shardId),
		},
		Table:   customReq.TableName,
		Indices: indice,
	}
	err = s.DB.CreateIndex(&ctx)
	if err != nil {
		resp := errDriver.ErrorResp(err)
		return 0, 0, resp
	}
	writtenBytes := uint64(len(key) + len(customReq.Indices))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, nil
}

func (s *Storage) dropIndex(index uint64, offset int, batchsize int, shardId uint64, cmd []byte, key []byte) (uint64, int64, []byte) {
	if err := s.DB.Closed.Load(); err != nil {
		panic(err)
	}
	if offset >= batchsize {
		panic(fmt.Sprintf("bad index %d: offset %d, size %d", index, offset, batchsize))
	}
	t0 := time.Now()
	defer func() {
		logutil.Debugf("[S-%d|logIndex:%d,%d]dropIndex handler cost %d ms", shardId, index, offset, time.Since(t0).Milliseconds())
	}()
	customReq := &pb.DropIndexRequest{}
	protoc.MustUnmarshal(customReq, cmd)
	idxNames := []string{customReq.IndexName}
	ctx := aoedb.DropIndexCtx{
		DBMutationCtx: aoedb.DBMutationCtx{
			Id:     index,
			Offset: offset,
			Size:   batchsize,
			DB:     aoedb.IdToNameFactory.Encode(shardId),
		},
		Table:      customReq.TableName,
		IndexNames: idxNames,
	}
	err := s.DB.DropIndex(&ctx)
	if err != nil {
		resp := errDriver.ErrorResp(err)
		return 0, 0, resp
	}
	writtenBytes := uint64(len(key) + len(customReq.TableName))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, nil
}

//Append appends batch in the table
func (s *Storage) Append(index uint64, offset int, batchSize int, shardId uint64, cmd []byte, key []byte) (uint64, int64, []byte) {
	if err := s.DB.Closed.Load(); err != nil {
		panic(err)
	}
	if offset >= batchSize {
		panic(fmt.Sprintf("bad index %d: offset %d, size %d", index, offset, batchSize))
	}
	t0 := time.Now()
	defer func() {
		logutil.Debugf("[S-%d|logIndex:%d,%d]append handler cost %d ms", shardId, index, offset, time.Since(t0).Milliseconds())
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
	atomic.AddUint64(&s.stats.WrittenKeys, uint64(vector.Length(bat.Vecs[0])))
	atomic.AddUint64(&s.stats.WrittenBytes, uint64(size))
	ctx := aoedb.AppendCtx{
		TableMutationCtx: aoedb.TableMutationCtx{
			DBMutationCtx: aoedb.DBMutationCtx{
				Id:     index,
				Offset: offset,
				Size:   batchSize,
				DB:     aoedb.IdToNameFactory.Encode(shardId),
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
	rsp := s.DB.GetSegmentIds(aoedb.IdToNameFactory.Encode(shardId), customReq.Name)
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
	t0 := time.Now()
	defer func() {
		logutil.Debugf("[S-%d|logIndex:%d,%d]createTable handler cost %d ms", shardId, index, offset, time.Since(t0).Milliseconds())
	}()
	customReq := &pb.CreateTabletRequest{}
	protoc.MustUnmarshal(customReq, cmd)

	tblInfo, err := helper.DecodeTable(customReq.TableInfo)
	if err != nil {
		buf := errDriver.ErrorResp(err)
		return 0, 0, buf
	}
	schema, indexSchema := adaptor.TableInfoToSchema(s.DB.Store.Catalog, &tblInfo)
	schema.Name = customReq.Name
	ctx := aoedb.CreateTableCtx{
		DBMutationCtx: aoedb.DBMutationCtx{
			Id:     index,
			Offset: offset,
			Size:   batchsize,
			DB:     aoedb.IdToNameFactory.Encode(shardId),
		},
		Schema: schema,
		Indice: indexSchema,
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
	if err := s.DB.Closed.Load(); err != nil {
		panic(err)
	}
	if offset >= batchsize {
		panic(fmt.Sprintf("bad index %d: offset %d, size %d", index, offset, batchsize))
	}
	t0 := time.Now()
	defer func() {
		logutil.Debugf("[S-%d|logIndex:%d,%d]dropTable handler cost %d ms", shardId, index, offset, time.Since(t0).Milliseconds())
	}()
	customReq := &pb.DropTabletRequest{}
	protoc.MustUnmarshal(customReq, cmd)

	ctx := aoedb.DropTableCtx{
		DBMutationCtx: aoedb.DBMutationCtx{
			Id:     index,
			Offset: offset,
			Size:   batchsize,
			DB:     aoedb.IdToNameFactory.Encode(shardId),
		},
		Table: customReq.Name,
	}

	tbl, err := s.DB.DropTable(&ctx)

	if err != nil {
		buf := errDriver.ErrorResp(err, "Call DropTable Failed")
		return 0, 0, buf
	}
	buf := codec.Uint642Bytes(tbl.Id)
	writtenBytes := uint64(len(key) + len(customReq.Name))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, buf
}

//TableIDs returns the ids of all the tables in the storage.
func (s *Storage) tableIDs() []byte {
	var ids []uint64
	dbs := s.DB.DatabaseNames()
	for _, db := range dbs {
		tableIDs, err := s.DB.TableIDs(db)
		if err != nil {
			return errDriver.ErrorResp(err)
		}
		ids = append(ids, tableIDs...)
	}
	rep, _ := json.Marshal(ids)
	return rep
}

//tableNames returns the names of all the tables in the storage.
func (s *Storage) tableNames() (names []string) {
	dbs := s.DB.DatabaseNames()
	for _, db := range dbs {
		tbNames := s.DB.TableNames(db)
		for _, tbName := range tbNames {
			if strings.Contains(tbName, sPrefix) {
				continue
			}
			names = append(names, tbName)
		}
	}
	return names
}

type splitCtx struct {
	Ctx         []byte
	SplitKeyCnt int
}

//SplitCheck checks before the split
func (s *Storage) SplitCheck(shard meta.Shard, size uint64) (currentApproximateSize uint64,
	currentApproximateKeys uint64, splitKeys [][]byte, ctx []byte, err error) {
	prepareSplitCtx := aoedb.PrepareSplitCtx{
		DB:   aoedb.IdToNameFactory.Encode(shard.ID),
		Size: size,
	}
	currentApproximateSize, currentApproximateKeys, splitKeys, ctx, err = s.DB.PrepareSplitDatabase(&prepareSplitCtx)
	logutil.Debugf("split check, len split key is %v", len(splitKeys))

	cubeSplitKeys := make([][]byte, 0)
	var start []byte
	if len(shard.Start) < len(shard.End) {
		start = make([]byte, len(shard.End))
		for i, b := range shard.Start {
			start[i] = b
		}
	} else {
		start = shard.Start
	}
	for i := 1; i < len(splitKeys); i++ {
		cubeSplitKeys = append(cubeSplitKeys, []byte(fmt.Sprintf("%v%c", string(start), i)))
	}

	splitctx := splitCtx{
		Ctx:         ctx,
		SplitKeyCnt: len(splitKeys),
	}
	ctx, _ = json.Marshal(splitctx)

	return currentApproximateSize, currentApproximateKeys, cubeSplitKeys, ctx, err

}

//CreateSnapshot create a snapshot
func (s *Storage) CreateSnapshot(shardID uint64, path string) error {
	s.DB.FlushDatabase(aoedb.IdToNameFactory.Encode(shardID))
	ctx := aoedb.CreateSnapshotCtx{
		DB:   aoedb.IdToNameFactory.Encode(shardID),
		Path: path,
		Sync: false,
	}
	idx, err := s.DB.CreateSnapshot(&ctx)
	logutil.Infof("createSnapshot, index is %v, storage is %v", idx, s)
	return err
}

//ApplySnapshot apply the snapshot in the storage
func (s *Storage) ApplySnapshot(shardID uint64, path string) error {
	ctx := aoedb.ApplySnapshotCtx{
		DB:   aoedb.IdToNameFactory.Encode(shardID),
		Path: path,
	}
	logutil.Infof("applysnapshot, storage is %v", s)
	err := s.DB.ApplySnapshot(&ctx)
	return err
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
			ids := rel.SegmentIds()
			if len(ids.Ids) < 1 {
				continue
			}
			seg := rel.Segment(ids.Ids[len(ids.Ids)-1])
			blks := seg.Blocks()
			blk := seg.Block(blks[len(blks)-1])
			cds := make([]*bytes.Buffer, len(attrs))
			dds := make([]*bytes.Buffer, len(attrs))
			for i := range cds {
				cds[i] = bytes.NewBuffer(make([]byte, 0))
				dds[i] = bytes.NewBuffer(make([]byte, 0))
			}
			refs := make([]uint64, len(attrs))
			bat, _ := blk.Read(refs, attrs, cds, dds)
			shardId := batch.GetVector(bat, sShardId)
			logIndex := batch.GetVector(bat, sLogIndex)
			metadate := batch.GetVector(bat, sMetadata)
			customReq := &meta.ShardLocalState{}
			offset := metadate.Col.(*types.Bytes).Offsets[vector.Length(metadate)-1]
			lengths := metadate.Col.(*types.Bytes).Lengths[vector.Length(metadate)-1]
			protoc.MustUnmarshal(customReq, metadate.Col.(*types.Bytes).Data[offset:offset+lengths])
			values = append(values, meta.ShardMetadata{
				ShardID:  shardId.Col.([]uint64)[len(shardId.Col.([]uint64))-1],
				LogIndex: logIndex.Col.([]uint64)[len(shardId.Col.([]uint64))-1],
				Metadata: *customReq,
			})
			logutil.Infof("GetInitialStates LogIndex is %d, ShardID is %d, Metadata is %v\n",
				logIndex.Col.([]uint64)[len(shardId.Col.([]uint64))-1], shardId.Col.([]uint64)[len(shardId.Col.([]uint64))-1], customReq.String())

		}
	}
	return values, nil
}

func (s *Storage) Write(ctx storage.WriteContext) error {
	batch := ctx.Batch()
	batchSize := len(batch.Requests)
	shard := ctx.Shard()
	totalWrittenBytes := uint64(0)
	totalDiffBytes := int64(0)
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
		case uint64(pb.CreateIndex):
			writtenBytes, changedBytes, rep = s.createIndex(batch.Index, idx, batchSize, shard.ID, cmd, key)
		case uint64(pb.DropIndex):
			writtenBytes, changedBytes, rep = s.dropIndex(batch.Index, idx, batchSize, shard.ID, cmd, key)
		}
		ctx.AppendResponse(rep)
		totalWrittenBytes += writtenBytes
		totalDiffBytes += changedBytes
	}
	ctx.SetWrittenBytes(totalWrittenBytes)
	ctx.SetDiffBytes(totalDiffBytes)
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
	db, _ := s.DB.Store.Catalog.SimpleGetDatabaseByName(aoedb.IdToNameFactory.Encode(shardID))
	if db == nil {
		logutil.Infof("GetPersistentLogIndex, shard id is %v, LogIndex is %v, storage is %v", shardID, 0, s)
		return 0, nil
	}
	rsp := s.DB.GetShardCheckpointId(db.GetShardId())
	if rsp == 0 {
		rsp = 1
	}
	logutil.Infof("GetPersistentLogIndex, shard id is %v, LogIndex is %v, storage is %v", shardID, rsp, s)
	return rsp, nil
}

func shardMetadataToBatch(metadata *meta.ShardMetadata) (*batch.Batch, error) {
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
	return bat, nil
}

func createMetadataTableInfo(shardId uint64) *aoe.TableInfo {
	tableName := sPrefix + strconv.Itoa(int(shardId))
	metaTblInfo := aoe.TableInfo{
		Name:    tableName,
		Indices: make([]aoe.IndexInfo, 0),
	}
	ShardId := aoe.ColumnInfo{
		Name: sShardId,
	}
	ShardId.Type = types.Type{Oid: types.T_uint64, Size: 8}
	metaTblInfo.Columns = append(metaTblInfo.Columns, ShardId)
	LogIndex := aoe.ColumnInfo{
		Name: sLogIndex,
	}
	LogIndex.Type = types.Type{Oid: types.T_uint64, Size: 8}
	metaTblInfo.Columns = append(metaTblInfo.Columns, LogIndex)
	colInfo := aoe.ColumnInfo{
		Name: sMetadata,
	}
	colInfo.Type = types.Type{Oid: types.T(types.T_varchar)}
	metaTblInfo.Columns = append(metaTblInfo.Columns, colInfo)
	return &metaTblInfo
}

func (s *Storage) SaveShardMetadata(metadatas []meta.ShardMetadata) error {
	for _, metadata := range metadatas {
		err := s.saveShardMetadata(&metadata, false)
		if err != nil {
			return err
		}
		err = s.DB.FlushDatabase(aoedb.IdToNameFactory.Encode(metadata.ShardID))
		if err != nil {
			logutil.Errorf("SaveShardMetadata is failed: %v", err.Error())
			return err
		}
	}
	return nil
}

func waitExpect(timeout int, expect func() bool) {
	end := time.Now().Add(time.Duration(timeout) * time.Millisecond)
	interval := time.Duration(timeout) * time.Millisecond / 400
	for time.Now().Before(end) && !expect() {
		time.Sleep(interval)
	}
}

func (s *Storage) RemoveShard(shard meta.Shard, removeData bool) error {
	if removeData {
		t0 := time.Now()
		dbName := aoedb.IdToNameFactory.Encode(shard.ID)
		db, err := s.DB.Store.Catalog.GetDatabaseByName(dbName)
		if err == metadata.DatabaseNotFoundErr {
			return nil
		}
		if err != nil {
			return err
		}
		s.DB.FlushDatabase(dbName)
		logIndex := db.GetCheckpointId()
		defer func() {
			logutil.Infof(
				"[S-%d|logIndex:%d,%d]RemoveShard handler cost %d ms",
				shard.ID, logIndex+1, 0, time.Since(t0).Milliseconds())
		}()

		ctx := aoedb.DropDBCtx{
			Id:     logIndex + 1,
			Offset: 0,
			Size:   1,
			DB:     dbName,
		}
		_, err = s.DB.DropDatabase(&ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) createAOEDatabaseIfNotExist(sid, logIndex uint64, offset, size int) (db *aoeMeta.Database, dbExisted bool, err error) {
	dbName := aoedb.IdToNameFactory.Encode(sid)
	db, err = s.DB.Store.Catalog.SimpleGetDatabaseByName(dbName)
	if db != nil {
		return db, true, err
	}
	if err != nil && err != aoeMeta.DatabaseNotFoundErr {
		return nil, false, err
	}
	if logIndex == math.MaxUint64 {
		return nil, false, storage.ErrShardNotFound
	}
	ctx := aoedb.CreateDBCtx{
		Id:     logIndex,
		Offset: offset,
		Size:   size,
		DB:     dbName,
	}
	logutil.Debugf("[%v]create database, S-%v: %v-%v/%v", s, sid, logIndex, offset, size)
	db, err = s.DB.CreateDatabase(&ctx)
	logutil.Infof("raft sid is %v, aoe sid is %v", sid, db.Id)
	return db, false, err
}

func (s *Storage) createAOEMetaTableIfNotExist(sid, logIndex uint64, offset, size int) (tbl *aoeMeta.Table, tblExisted bool, err error) {
	dbName := aoedb.IdToNameFactory.Encode(sid)
	tableName := encodeMetatableName(sid)
	tbl, err = s.DB.Store.Catalog.SimpleGetTableByName(dbName, tableName)
	if tbl != nil {
		return tbl, true, err
	}
	if err != nil && err != aoeMeta.TableNotFoundErr {
		return nil, false, err
	}
	metaTblInfo := createMetadataTableInfo(sid)
	schema, indexSchema := adaptor.TableInfoToSchema(s.DB.Store.Catalog, metaTblInfo)
	ctx := aoedb.CreateTableCtx{
		DBMutationCtx: aoedb.DBMutationCtx{
			Id:     logIndex,
			Offset: offset,
			Size:   size,
			DB:     dbName,
		},
		Schema: schema,
		Indice: indexSchema,
	}
	logutil.Debugf("[%v]create metatable, S-%v: %v-%v/%v", s, sid, logIndex, offset, size)
	_, err = s.DB.CreateTable(&ctx)
	return tbl, false, err
}

func (s *Storage) appendShardMetadata(sid, logIndex uint64, offset, size int, metadata *meta.ShardMetadata) (err error) {
	dbName := aoedb.IdToNameFactory.Encode(sid)
	tableName := encodeMetatableName(sid)
	bat, _ := shardMetadataToBatch(metadata)

	ctx := aoedb.AppendCtx{
		TableMutationCtx: aoedb.TableMutationCtx{
			DBMutationCtx: aoedb.DBMutationCtx{
				Id:     logIndex,
				Offset: offset,
				Size:   size,
				DB:     dbName,
			},
			Table: tableName,
		},
		Data: bat,
	}
	logutil.Debugf("[%v]appendShardMetadata, S-%v: %v-%v/%v", s, sid, logIndex, offset, size)
	err = s.DB.Append(&ctx)
	if err != nil {
		return err
	}
	return nil
}

func (s *Storage) saveShardMetadata(metadata *meta.ShardMetadata, dropTable bool) error {

	offset := 0
	size := 3
	if dropTable {
		size++
	}

	_, dbExisted, err := s.createAOEDatabaseIfNotExist(metadata.ShardID, metadata.LogIndex, offset, size)
	if err != nil {
		return err
	}

	offset++
	if dbExisted {
		offset--
		size--
	}
	_, tblExisted, err := s.createAOEMetaTableIfNotExist(metadata.ShardID, metadata.LogIndex, offset, size)
	if err != nil {
		logutil.Errorf("SaveShardMetadata is failed: %v", err.Error())
		return err
	}

	offset++
	if tblExisted {
		offset--
		size--
	}
	err = s.appendShardMetadata(metadata.ShardID, metadata.LogIndex, offset, size, metadata)
	if err != nil {
		logutil.Errorf("SaveShardMetadata is failed: %v", err.Error())
		return err
	}

	return nil
}

func (s *Storage) Split(old meta.ShardMetadata, news []meta.ShardMetadata, ctx []byte) error {
	oldtableName := sPrefix + strconv.Itoa(int(old.ShardID))
	newNames := make([]string, len(news))
	for i, shard := range news {
		name := aoedb.IdToNameFactory.Encode(shard.ShardID)
		newNames[i] = name
	}
	logutil.Infof("split, from %v to %v", old.ShardID, newNames)

	renameTable := func(oldName, dbName string) string {
		return oldName
	}

	splitctx := splitCtx{}
	err := json.Unmarshal(ctx, &splitctx)
	if err != nil {
		logutil.Errorf("Split:S-%d Marshal fail.", old.ShardID)
		return err
	}
	splitkey := make([][]byte, splitctx.SplitKeyCnt)
	for i := 0; i < splitctx.SplitKeyCnt; i++ {
		splitkey[i] = []byte("1")
	}

	db, _ := s.DB.Store.Catalog.GetDatabaseByName(aoedb.IdToNameFactory.Encode(old.ShardID))
	tbnames := s.tableNames()
	metaTbls := make([]*aoeMeta.Table, 0)
	for _, tb := range tbnames {
		metaTbl := db.SimpleGetTableByName(tb)
		metaTbls = append(metaTbls, metaTbl)
	}

	execSplitCtx := &aoedb.ExecSplitCtx{
		DBMutationCtx: aoedb.DBMutationCtx{
			Id:     old.LogIndex,
			Offset: 0,
			Size:   1,
			DB:     aoedb.IdToNameFactory.Encode(old.ShardID),
		},
		NewNames:    newNames,
		RenameTable: renameTable,
		SplitKeys:   splitkey,
		SplitCtx:    splitctx.Ctx,
	}
	err = s.DB.ExecSplitDatabase(execSplitCtx)
	if err != nil {
		logutil.Errorf("Split:S-%d ExecSplitDatabase fail.", old.ShardID)
		return err
	}

	waitExpect(2500, func() bool {
		for _, tb := range metaTbls {
			if tb ==nil {
				continue
			}
			// logutil.Infof("gc tbl:%v,%v", tb.Schema.Name, tb.IsHardDeleted())
			if !tb.IsHardDeleted() {
				return false
			}
		}
		return true
	})

	for _, shard := range news {
		s.saveShardMetadata(&shard, true)

		dropCtx := aoedb.DropTableCtx{
			DBMutationCtx: aoedb.DBMutationCtx{
				Id:     shard.LogIndex,
				Offset: 2,
				Size:   3,
				DB:     aoedb.IdToNameFactory.Encode(shard.ShardID),
			},
			Table: oldtableName,
		}
		s.DB.DropTable(&dropCtx)
	}
	return err
}

//for test
func (s *Storage) IsTablesSame(s2 *Storage, sid uint64) bool {
	tbls := s.DB.TableNames(aoedb.IdToNameFactory.Encode(sid))
	tbls2 := s.DB.TableNames(aoedb.IdToNameFactory.Encode(sid))
	if len(tbls) != len(tbls2) {
		return false
	}
	for _, tbl := range tbls {
		if strings.Contains(tbl, sPrefix) {
			continue
		}
		exist := false
		for _, tbl2 := range tbls2 {
			if tbl == tbl2 {
				exist = true
				break
			}
		}
		if !exist {
			return false
		}
	}
	return true
}

//for test
func (s *Storage) ReadAll(sid uint64, tbl string) ([]*batch.Batch, error) {
	var batchs []*batch.Batch
	relation, err := s.Relation(aoedb.IdToNameFactory.Encode(sid), tbl)
	logutil.Infof("Relation err is %v, db is %v", err, aoedb.IdToNameFactory.Encode(sid))
	attrs := make([]string, 0)
	if relation == nil {
		logutil.Infof("relation is nil")
		return nil, nil
	}
	if relation.Meta == nil {
		logutil.Infof("meta is nil")
		return nil, nil
	}
	if relation.Meta.Schema == nil {
		logutil.Infof("Schema is nil")
		return nil, nil
	}
	if relation.Meta.Schema.ColDefs == nil {
		logutil.Infof("ColDefs is nil")
		return nil, nil
	}
	for _, ColDef := range relation.Meta.Schema.ColDefs {
		attrs = append(attrs, ColDef.Name)
	}
	relation.Data.GetBlockFactory()
	for _, segment := range relation.Meta.SegmentSet {
		seg := relation.Segment(segment.Id)
		blks := seg.Blocks()
		for _, blk := range blks {
			block := seg.Block(blk)
			cds := make([]*bytes.Buffer, len(attrs))
			dds := make([]*bytes.Buffer, len(attrs))
			for i := range cds {
				cds[i] = bytes.NewBuffer(make([]byte, 0))
				dds[i] = bytes.NewBuffer(make([]byte, 0))
			}
			refs := make([]uint64, len(attrs))
			bat, _ := block.Read(refs, attrs, cds, dds)
			batchs = append(batchs, bat)
		}
	}
	return batchs, nil
}

//for test
func (s *Storage) TotalRows(sid uint64) (rows uint64, err error) {
	db, err := s.DB.Store.Catalog.GetDatabaseByName(aoedb.IdToNameFactory.Encode(sid))
	for _, tb := range db.TableSet {
		if strings.Contains(tb.Schema.Name, sPrefix) {
			continue
		}
		tb.RLock()
		defer tb.RUnlock()
		td, _ := s.DB.Store.DataTables.WeakRefTable(tb.Id)
		row := td.GetRowCount()
		logutil.Infof("tbl is %v, row is %v", tb.Schema.Name, row)
		rows += row
	}
	logutil.Infof("total rows is %v", rows)
	return
}
