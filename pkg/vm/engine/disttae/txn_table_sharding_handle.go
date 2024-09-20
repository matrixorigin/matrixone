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

	"github.com/matrixorigin/matrixone/pkg/catalog"
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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/gc"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const StreamReaderLease = time.Minute * 2

type shardingRemoteReader struct {
	streamID types.Uuid
	rd       engine.Reader
	colTypes []types.Type
	deadline time.Time
}

func (sr *shardingRemoteReader) updateCols(cols []string, tblDef *plan.TableDef) {
	if len(sr.colTypes) == 0 {
		sr.colTypes = make([]types.Type, len(cols))
		for i, column := range cols {
			column = strings.ToLower(column)
			if column == catalog.Row_ID {
				sr.colTypes[i] = objectio.RowidType
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
	streamReaders map[types.Uuid]shardingRemoteReader
	GCManager     *gc.Manager
}

var streamHandler streamHandle

func init() {
	streamHandler.streamReaders = make(map[types.Uuid]shardingRemoteReader)
	streamHandler.GCManager = gc.NewManager(
		gc.WithCronJob(
			"streamReaderGC",
			StreamReaderLease,
			func(ctx context.Context) error {
				streamHandler.Lock()
				defer streamHandler.Unlock()
				for id, sr := range streamHandler.streamReaders {
					if time.Now().After(sr.deadline) {
						delete(streamHandler.streamReaders, id)
					}
				}
				return nil
			},
		),
	)

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
	tbl, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}

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
	tbl, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}

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
	tbl, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}

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
	tbl, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}

	num := tbl.ApproxObjectsNum(
		ctx,
	)
	return buffer.EncodeInt(num), nil
}

// HandleShardingReadRanges handles sharding read Ranges
func HandleShardingReadRanges(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}

	var uncommittedRanges []objectio.ObjectStats
	n := len(param.RangesParam.UncommittedObjects) / objectio.ObjectStatsLen
	for i := 0; i < n; i++ {
		var stat objectio.ObjectStats
		stat.UnMarshal(param.RangesParam.UncommittedObjects[i*objectio.ObjectStatsLen : (i+1)*objectio.ObjectStatsLen])
		uncommittedRanges = append(uncommittedRanges, stat)
	}

	ranges, err := tbl.doRanges(
		ctx,
		param.RangesParam.Exprs,
		uncommittedRanges,
	)
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
	tbl, err := getTxnTable(
		ctx,
		param,
		e,
	)
	if err != nil {
		return nil, err
	}

	relData, err := UnmarshalRelationData(param.ReaderBuildParam.RelData)
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

	rd, err := NewReader(
		ctx,
		tbl.proc.Load(),
		e.(*Engine),
		tbl.tableDef,
		tbl.db.op.SnapshotTS(),
		param.ReaderBuildParam.Expr,
		ds,
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
	streamHandler.streamReaders[uuid] = shardingRemoteReader{
		streamID: uuid,
		rd:       rd,
		deadline: time.Now().Add(StreamReaderLease),
	}

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

	tbl, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}
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
	streamHandler.Unlock()
	sr.deadline = time.Now().Add(StreamReaderLease)

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
	defer streamHandler.Unlock()
	sr, ok := streamHandler.streamReaders[streamID]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("stream reader not found, may be expired")
	}
	sr.rd.Close()
	delete(streamHandler.streamReaders, sr.streamID)
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
	tbl, err := getTxnTable(
		ctx,
		param,
		eng,
	)
	if err != nil {
		return nil, err
	}

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
	tbl, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}

	infos, err := tbl.GetColumMetadataScanInfo(
		ctx,
		param.GetColumMetadataScanInfoParam.ColumnName,
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
	tbl, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}

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

	modify, err := tbl.PrimaryKeysMayBeModified(
		ctx,
		from,
		to,
		keyVector,
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
	tbl, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}

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
	tbl, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}

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
) (*txnTable, error) {
	// TODO: reduce mem allocate
	proc, err := process.GetCodecService(engine.GetService()).Decode(
		ctx,
		param.Process,
	)
	if err != nil {
		return nil, err
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
		return nil, err
	}

	tbl := newTxnTableWithItem(
		db,
		item,
		proc,
		engine.(*Engine),
	)
	tbl.remoteWorkspace = true
	tbl.createdInTxn = param.TxnTable.CreatedInTxn
	return tbl, nil
}
