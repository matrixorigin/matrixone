// Copyright 2021-2024 Matrix Origin
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

package disttae

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const StreamReaderLease = time.Minute * 2

type shardingRemoteReader struct {
	streamID  types.Uuid
	rd        engine.Reader
	colTypes  []types.Type
	deadline  time.Time
	release   func()
	mu        sync.Mutex
	closed    bool
	closeOnce sync.Once
}

func (sr *shardingRemoteReader) close() {
	sr.closeOnce.Do(func() {
		sr.mu.Lock()
		defer sr.mu.Unlock()
		sr.closed = true
		sr.rd.Close()
		if sr.release != nil {
			sr.release()
		}
	})
}

func (sr *shardingRemoteReader) updateCols(cols []string, tblDef *plan.TableDef) {
	if len(sr.colTypes) == 0 {
		sr.colTypes = make([]types.Type, len(cols))
		for i, column := range cols {
			column = strings.ToLower(column)
			if objectio.IsPhysicalAddr(column) {
				sr.colTypes[i] = objectio.RowidType
			} else if strings.EqualFold(column, objectio.DefaultCommitTS_Attr) {
				sr.colTypes[i] = types.T_TS.ToType()
			} else {
				colIdx := tblDef.Name2ColIndex[column]
				colDef := tblDef.Cols[colIdx]
				sr.colTypes[i] = types.T(colDef.Typ.Id).ToType()
				sr.colTypes[i].Scale = colDef.Typ.Scale
				sr.colTypes[i].Width = colDef.Typ.Width
			}
		}
	}
}

type streamHandle struct {
	sync.Mutex
	streamReaders map[types.Uuid]*shardingRemoteReader
	gcJob         *tasks.CancelableJob
}

var streamHandler streamHandle

func closeExpiredStreamReaders(now time.Time) {
	streamHandler.Lock()
	var expired []*shardingRemoteReader
	for id, sr := range streamHandler.streamReaders {
		if now.After(sr.deadline) {
			delete(streamHandler.streamReaders, id)
			expired = append(expired, sr)
		}
	}
	streamHandler.Unlock()
	for _, sr := range expired {
		sr.close()
	}
}

func init() {
	streamHandler.streamReaders = make(map[types.Uuid]*shardingRemoteReader)
	streamHandler.gcJob = tasks.NewCancelableCronJob(
		"streamReaderGC",
		StreamReaderLease,
		func(ctx context.Context) {
			closeExpiredStreamReaders(time.Now())
		},
		true,
		1,
	)
	streamHandler.gcJob.Start()
}

// HandleShardingReadRows handles sharding read rows
func HandleShardingReadRows(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, release, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}
	defer release()

	rows, err := tbl.Rows(ctx)
	if err != nil {
		return nil, err
	}
	return buffer.EncodeUint64(rows), nil
}

// HandleShardingReadSize handles sharding read size
func HandleShardingReadSize(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, release, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}
	defer release()

	rows, err := tbl.Size(
		ctx,
		param.SizeParam.ColumnName,
	)
	if err != nil {
		return nil, err
	}
	return buffer.EncodeUint64(rows), nil
}

// HandleShardingReadStatus handles sharding read status
func HandleShardingReadStatus(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, release, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}
	defer release()

	info, err := tbl.Stats(
		ctx,
		param.StatsParam.Sync,
	)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, nil
	}

	bys, err := info.Marshal()
	if err != nil {
		return nil, err
	}
	return buffer.EncodeBytes(bys), nil
}

// HandleShardingReadApproxObjectsNum handles sharding read ApproxObjectsNum
func HandleShardingReadApproxObjectsNum(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, release, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}
	defer release()

	num := tbl.ApproxObjectsNum(
		ctx,
	)
	return buffer.EncodeInt(num), nil
}

// HandleShardingReadRanges handles sharding read Ranges
func HandleShardingReadRanges(
	ctx context.Context,
	shard shard.TableShard,
	eng engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, release, err := getTxnTable(
		ctx,
		param,
		eng,
	)
	if err != nil {
		return nil, err
	}
	defer release()
	rangesParam := engine.RangesParam{
		BlockFilters:   param.RangesParam.Exprs,
		PreAllocBlocks: int(param.RangesParam.PreAllocSize),
		TxnOffset:      int(param.RangesParam.TxnOffset),
		Policy:         engine.DataCollectPolicy(param.RangesParam.DataCollectPolicy),
	}
	ranges, err := tbl.doRanges(ctx, rangesParam)
	if err != nil {
		return nil, err
	}

	bys, err := ranges.MarshalBinary()
	if err != nil {
		return nil, err
	}

	return buffer.EncodeBytes(bys), nil
}

// HandleShardingReadReader handles sharding read Reader
func HandleShardingReadBuildReader(
	ctx context.Context,
	shard shard.TableShard,
	e engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, release, err := getTxnTable(
		ctx,
		param,
		e,
	)
	if err != nil {
		return nil, err
	}
	transferred := false
	var rd engine.Reader
	defer func() {
		if !transferred {
			if rd != nil {
				rd.Close()
			}
			release()
		}
	}()

	relData, err := readutil.UnmarshalRelationData(param.ReaderBuildParam.RelData)
	if err != nil {
		return nil, err
	}

	ds, err := tbl.buildLocalDataSource(
		ctx,
		0,
		relData,
		engine.TombstoneApplyPolicy(param.ReaderBuildParam.TombstoneApplyPolicy),
		engine.ShardingRemoteDataSource,
	)
	if err != nil {
		return nil, err
	}

	rd, err = readutil.NewReader(
		ctx,
		tbl.proc.Load().Mp(),
		e.(*Engine).packerPool,
		e.(*Engine).fs,
		tbl.tableDef,
		tbl.db.op.SnapshotTS(),
		param.ReaderBuildParam.Expr,
		ds,
		readutil.GetThresholdForReader(1),
		engine.FilterHint{},
	)
	if err != nil {
		return nil, err
	}

	uuid, err := types.BuildUuid()
	if err != nil {
		return nil, err
	}
	streamHandler.Lock()
	defer streamHandler.Unlock()
	streamHandler.streamReaders[uuid] = &shardingRemoteReader{
		streamID: uuid,
		rd:       rd,
		deadline: time.Now().Add(StreamReaderLease),
		release:  release,
	}
	transferred = true

	return buffer.EncodeBytes(types.EncodeUuid(&uuid)), nil
}

func HandleShardingReadNext(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {

	tbl, release, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}
	defer release()
	mp := tbl.proc.Load().Mp()

	streamID := types.DecodeUuid(param.ReadNextParam.Uuid)
	cols := param.ReadNextParam.Columns
	//find reader by streamID
	streamHandler.Lock()
	sr, ok := streamHandler.streamReaders[streamID]
	if !ok {
		streamHandler.Unlock()
		return nil, moerr.NewInternalErrorNoCtx("stream reader not found, may be expired")
	}
	sr.deadline = time.Now().Add(StreamReaderLease)
	streamHandler.Unlock()

	sr.mu.Lock()
	defer sr.mu.Unlock()
	if sr.closed {
		return nil, moerr.NewInternalErrorNoCtx("stream reader is closed")
	}

	sr.updateCols(cols, tbl.tableDef)

	buildBatch := func() *batch.Batch {
		bat := batch.NewWithSize(len(sr.colTypes))
		bat.Attrs = append(bat.Attrs, cols...)

		for i := 0; i < len(sr.colTypes); i++ {
			bat.Vecs[i] = vector.NewVec(sr.colTypes[i])

		}
		return bat
	}
	bat := buildBatch()
	defer func() {
		bat.Clean(mp)
	}()

	isEnd, err := sr.rd.Read(
		ctx,
		cols,
		nil,
		mp,
		bat,
	)
	if err != nil {
		return nil, err
	}
	if isEnd {
		return buffer.EncodeBytes(types.EncodeBool(&isEnd)), nil
	}

	var w bytes.Buffer
	if _, err := w.Write(types.EncodeBool(&isEnd)); err != nil {
		return nil, err
	}
	encBat, err := bat.MarshalBinary()
	if err != nil {
		return nil, err
	}
	l := uint32(len(encBat))
	if _, err := w.Write(types.EncodeUint32(&l)); err != nil {
		return nil, err
	}
	if _, err := w.Write(encBat); err != nil {
		return nil, err
	}
	return buffer.EncodeBytes(w.Bytes()), nil
}

func HandleShardingReadClose(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	streamID := types.DecodeUuid(param.ReadCloseParam.Uuid)
	//find reader by streamID
	streamHandler.Lock()
	sr, ok := streamHandler.streamReaders[streamID]
	if !ok {
		streamHandler.Unlock()
		return nil, moerr.NewInternalErrorNoCtx("stream reader not found, may be expired")
	}
	delete(streamHandler.streamReaders, sr.streamID)
	streamHandler.Unlock()
	sr.close()
	return nil, nil
}

func HandleShardingReadCollectTombstones(
	ctx context.Context,
	shard shard.TableShard,
	eng engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, release, err := getTxnTable(
		ctx,
		param,
		eng,
	)
	if err != nil {
		return nil, err
	}
	defer release()

	tombstones, err := tbl.CollectTombstones(
		ctx,
		0,
		engine.TombstoneCollectPolicy(param.CollectTombstonesParam.CollectPolicy),
	)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	err = tombstones.MarshalBinaryWithBuffer(&buf)
	if err != nil {
		return nil, err
	}
	return buffer.EncodeBytes(buf.Bytes()), nil
}

// HandleShardingReadGetColumMetadataScanInfo handles sharding read GetColumMetadataScanInfo
func HandleShardingReadGetColumMetadataScanInfo(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, release, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}
	defer release()

	infos, err := tbl.GetColumMetadataScanInfo(
		ctx,
		param.GetColumMetadataScanInfoParam.ColumnName,
		false,
	)
	if err != nil {
		return nil, err
	}

	v := plan.MetadataScanInfos{
		Infos: infos,
	}
	bys, err := v.Marshal()
	if err != nil {
		panic(err)
	}
	return buffer.EncodeBytes(bys), nil
}

// HandleShardingReadPrimaryKeysMayBeModified handles sharding read PrimaryKeysMayBeModified
func HandleShardingReadPrimaryKeysMayBeModified(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, release, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}
	defer release()

	var from, to types.TS
	err = from.Unmarshal(param.PrimaryKeysMayBeModifiedParam.From)
	if err != nil {
		return nil, err
	}

	err = to.Unmarshal(param.PrimaryKeysMayBeModifiedParam.To)
	if err != nil {
		return nil, err
	}

	keyVector := vector.NewVecFromReuse()
	err = keyVector.UnmarshalBinary(param.PrimaryKeysMayBeModifiedParam.KeyVector)
	if err != nil {
		return nil, err
	}

	batch := batch.NewWithSize(1)
	batch.SetVector(0, keyVector)
	modify, err := tbl.PrimaryKeysMayBeModified(
		ctx,
		from,
		to,
		batch,
		0,
		-1,
	)
	if err != nil {
		return nil, err
	}
	var r uint16
	if modify {
		r = 1
	}
	return buffer.EncodeUint16(r), nil
}

func HandleShardingReadPrimaryKeysMayBeUpserted(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, release, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}
	defer release()

	var from, to types.TS
	err = from.Unmarshal(param.PrimaryKeysMayBeModifiedParam.From)
	if err != nil {
		return nil, err
	}

	err = to.Unmarshal(param.PrimaryKeysMayBeModifiedParam.To)
	if err != nil {
		return nil, err
	}

	keyVector := vector.NewVecFromReuse()
	err = keyVector.UnmarshalBinary(param.PrimaryKeysMayBeModifiedParam.KeyVector)
	if err != nil {
		return nil, err
	}

	batch := batch.NewWithSize(1)
	batch.SetVector(0, keyVector)
	modify, err := tbl.PrimaryKeysMayBeUpserted(
		ctx,
		from,
		to,
		batch,
		0,
	)
	if err != nil {
		return nil, err
	}
	var r uint16
	if modify {
		r = 1
	}
	return buffer.EncodeUint16(r), nil
}

// HandleShardingReadMergeObjects handles sharding read MergeObjects
func HandleShardingReadMergeObjects(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, release, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}
	defer release()

	objstats := make([]objectio.ObjectStats, len(param.MergeObjectsParam.Objstats))
	for i, o := range param.MergeObjectsParam.Objstats {
		objstats[i].UnMarshal(o)
	}

	entry, err := tbl.MergeObjects(
		ctx,
		objstats,
		param.MergeObjectsParam.TargetObjSize,
	)
	if err != nil {
		return nil, err
	}

	bys, err := entry.Marshal()
	if err != nil {
		return nil, err
	}
	return buffer.EncodeBytes(bys), nil
}

func HandleShardingReadVisibleObjectStats(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, release, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}
	defer release()

	stats, err := tbl.GetNonAppendableObjectStats(ctx)
	if err != nil {
		return nil, err
	}

	b := new(bytes.Buffer)
	size := len(stats)
	marshalSize := size * (objectio.ObjectStatsLen)
	b.Grow(marshalSize)
	for _, stat := range stats {
		b.Write(stat.Marshal())
	}
	return buffer.EncodeBytes(b.Bytes()), nil
}

func getTxnTable(
	ctx context.Context,
	param shard.ReadParam,
	engine engine.Engine,
) (*txnTable, func(), error) {
	// TODO: reduce mem allocate
	proc, err := process.GetCodecService(engine.GetService()).Decode(
		ctx,
		param.Process,
	)
	if err != nil {
		return nil, nil, err
	}
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(proc.Free)
	}
	ws := NewTxnWorkSpace(engine.(*Engine), proc)
	proc.GetTxnOperator().AddWorkspace(ws)
	ws.BindTxnOp(proc.GetTxnOperator())

	db := &txnDatabase{
		op:           proc.GetTxnOperator(),
		databaseName: param.TxnTable.DatabaseName,
		databaseId:   param.TxnTable.DatabaseID,
	}

	item, err := db.getTableItem(
		ctx,
		uint32(param.TxnTable.AccountID),
		param.TxnTable.TableName,
		engine.(*Engine),
	)
	if err != nil {
		release()
		return nil, nil, err
	}
	if item == nil {
		release()
		return nil, nil, moerr.NewParseErrorf(ctx, "table %q does not exist", param.TxnTable.TableName)
	}

	tbl := newTxnTableWithItem(
		db,
		*item,
		proc,
		engine.(*Engine),
	)
	tbl.remoteWorkspace = true
	tbl.createdInTxn = param.TxnTable.CreatedInTxn
	return tbl, release, nil
}
