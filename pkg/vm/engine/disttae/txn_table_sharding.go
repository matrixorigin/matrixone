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
	"context"
	"time"

	"github.com/fagongzi/goetty/v2/buf"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/shard"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type ReaderPhase uint8

const (
	InLocal ReaderPhase = iota
	InRemote
	InEnd
)

func newTxnTableWithItem(
	db *txnDatabase,
	item cache.TableItem,
	process *process.Process,
	eng engine.Engine,
) *txnTable {
	tbl := &txnTable{
		db:            db,
		accountId:     item.AccountId,
		tableId:       item.Id,
		version:       item.Version,
		tableName:     item.Name,
		defs:          item.Defs,
		tableDef:      item.TableDef,
		primaryIdx:    item.PrimaryIdx,
		primarySeqnum: item.PrimarySeqnum,
		clusterByIdx:  item.ClusterByIdx,
		relKind:       item.Kind,
		viewdef:       item.ViewDef,
		comment:       item.Comment,
		partitioned:   item.Partitioned,
		partition:     item.Partition,
		createSql:     item.CreateSql,
		constraint:    item.Constraint,
		extraInfo:     item.ExtraInfo,
		lastTS:        db.op.SnapshotTS(),
		eng:           eng,
	}
	tbl.proc.Store(process)
	return tbl
}

type txnTableDelegate struct {
	origin *txnTable

	// sharding info
	shard struct {
		service shardservice.ShardService
		policy  shard.Policy
		tableID uint64
		is      bool
	}
	isMock  bool
	isLocal func() (bool, error)
}

func MockTableDelegate(
	tableDelegate engine.Relation,
	service shardservice.ShardService,
) (engine.Relation, error) {
	delegate := tableDelegate.(*txnTableDelegate)
	tbl := &txnTableDelegate{
		origin: delegate.origin,
		isMock: true,
	}
	tbl.shard.service = service

	tbl.shard.service = service
	tbl.shard.is = false
	tbl.isLocal = tbl.isLocalFunc

	if service.Config().Enable &&
		tbl.origin.db.databaseId != catalog.MO_CATALOG_ID {
		tableID, policy, is, err := service.GetShardInfo(tbl.origin.tableId)
		if err != nil {
			return nil, err
		}

		tbl.shard.is = is
		tbl.shard.policy = policy
		tbl.shard.tableID = tableID
	}
	return tbl, nil
}

func newTxnTable(
	db *txnDatabase,
	item cache.TableItem,
	process *process.Process,
	service shardservice.ShardService,
	eng engine.Engine,
) (engine.Relation, error) {
	tbl := &txnTableDelegate{
		origin: newTxnTableWithItem(
			db,
			item,
			process,
			eng,
		),
	}

	tbl.shard.service = service
	tbl.shard.is = false
	tbl.isLocal = tbl.isLocalFunc

	if service.Config().Enable &&
		db.databaseId != catalog.MO_CATALOG_ID {
		tableID, policy, is, err := service.GetShardInfo(item.Id)
		if err != nil {
			return nil, err
		}

		tbl.shard.is = is
		tbl.shard.policy = policy
		tbl.shard.tableID = tableID
	}

	return tbl, nil
}

func (tbl *txnTableDelegate) CollectChanges(ctx context.Context, from, to types.TS, mp *mpool.MPool) (engine.ChangesHandle, error) {
	return tbl.origin.CollectChanges(ctx, from, to, mp)
}

func (tbl *txnTableDelegate) Stats(
	ctx context.Context,
	sync bool,
) (*pb.StatsInfo, error) {
	is, err := tbl.isLocal()
	if err != nil {
		return nil, err
	}

	if is {
		return tbl.origin.Stats(
			ctx,
			sync,
		)
	}

	var stats pb.StatsInfo
	has := false
	err = tbl.forwardRead(
		ctx,
		shardservice.ReadStats,
		func(param *shard.ReadParam) {
			param.StatsParam.Sync = sync
		},
		func(resp []byte) {
			if len(resp) > 0 {
				has = true
				err := stats.Unmarshal(resp)
				if err != nil {
					panic(err)
				}
			}

			// TODO: hash shard need to merge all shard in future
		},
	)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, nil
	}
	return &stats, nil
}

func (tbl *txnTableDelegate) Rows(
	ctx context.Context,
) (uint64, error) {
	is, err := tbl.isLocal()
	if err != nil {
		return 0, err
	}

	if is {
		return tbl.origin.Rows(
			ctx,
		)
	}

	rows := uint64(0)
	err = tbl.forwardRead(
		ctx,
		shardservice.ReadRows,
		func(param *shard.ReadParam) {},
		func(resp []byte) {
			rows += buf.Byte2Uint64(resp)
		},
	)
	if err != nil {
		return 0, err
	}
	return rows, nil
}

func (tbl *txnTableDelegate) Size(
	ctx context.Context,
	columnName string,
) (uint64, error) {
	is, err := tbl.isLocal()
	if err != nil {
		return 0, err
	}
	if is {
		return tbl.origin.Size(
			ctx,
			columnName,
		)
	}

	size := uint64(0)
	err = tbl.forwardRead(
		ctx,
		shardservice.ReadSize,
		func(param *shard.ReadParam) {
			param.SizeParam.ColumnName = columnName
		},
		func(resp []byte) {
			size += buf.Byte2Uint64(resp)
		},
	)
	if err != nil {
		return 0, err
	}
	return size, nil
}

func (tbl *txnTableDelegate) Ranges(
	ctx context.Context,
	exprs []*plan.Expr,
	txnOffset int,
) (engine.RelData, error) {
	is, err := tbl.isLocal()
	if err != nil {
		return nil, err
	}
	if is {
		return tbl.origin.Ranges(
			ctx,
			exprs,
			txnOffset,
		)
	}

	buf := morpc.NewBuffer()
	defer buf.Close()
	uncommitted, _ := tbl.origin.collectUnCommittedDataObjs(txnOffset)
	buf.Mark()
	for _, v := range uncommitted {
		buf.EncodeBytes(v[:])
	}

	var rs []engine.RelData
	err = tbl.forwardRead(
		ctx,
		shardservice.ReadRanges,
		func(param *shard.ReadParam) {
			param.RangesParam.Exprs = exprs
			param.RangesParam.UncommittedObjects = buf.GetMarkedData()
		},
		func(resp []byte) {
			data, err := UnmarshalRelationData(resp)
			if err != nil {
				panic(err)
			}
			rs = append(rs, data)
		},
	)
	if err != nil {
		return nil, err
	}

	ret := NewBlockListRelationData(0)
	for _, r := range rs {
		blks := r.GetBlockInfoSlice()
		ret.blklist.Append(blks)
	}
	return ret, nil
}

func (tbl *txnTableDelegate) CollectTombstones(
	ctx context.Context,
	txnOffset int,
	policy engine.TombstoneCollectPolicy) (engine.Tombstoner, error) {
	is, err := tbl.isLocal()
	if err != nil {
		return nil, err
	}
	if is {
		return tbl.origin.CollectTombstones(
			ctx,
			txnOffset,
			policy,
		)
	}

	localTombstones, err := tbl.origin.CollectTombstones(
		ctx,
		txnOffset,
		engine.Policy_CollectUncommittedTombstones,
	)
	if err != nil {
		return nil, err
	}
	var remoteTombstones engine.Tombstoner
	err = tbl.forwardRead(
		ctx,
		shardservice.ReadCollectTombstones,
		func(param *shard.ReadParam) {
			param.CollectTombstonesParam.CollectPolicy = engine.Policy_CollectCommittedTombstones
		},
		func(resp []byte) {
			tombstones, err := UnmarshalTombstoneData(resp)
			if err != nil {
				panic(err)
			}
			remoteTombstones = tombstones
		},
	)
	if err != nil {
		return nil, err
	}
	localTombstones.Merge(remoteTombstones)
	return localTombstones, nil
}

func (tbl *txnTableDelegate) GetColumMetadataScanInfo(
	ctx context.Context,
	name string,
) ([]*plan.MetadataScanInfo, error) {
	is, err := tbl.isLocal()
	if err != nil {
		return nil, err
	}
	if is {
		return tbl.origin.GetColumMetadataScanInfo(
			ctx,
			name,
		)
	}

	var m plan.MetadataScanInfos
	err = tbl.forwardRead(
		ctx,
		shardservice.ReadGetColumMetadataScanInfo,
		func(param *shard.ReadParam) {
			param.GetColumMetadataScanInfoParam.ColumnName = name
		},
		func(resp []byte) {
			err := m.Unmarshal(resp)
			if err != nil {
				panic(err)
			}
			// TODO: hash shard need to merge all shard in future
		},
	)
	if err != nil {
		return nil, err
	}
	return m.Infos, nil
}

func (tbl *txnTableDelegate) ApproxObjectsNum(
	ctx context.Context,
) int {
	is, err := tbl.isLocal()
	if err != nil {
		logutil.Infof("approx objects num err: %v", err)
		return 0
	}
	if is {
		return tbl.origin.ApproxObjectsNum(
			ctx,
		)
	}

	num := 0
	err = tbl.forwardRead(
		ctx,
		shardservice.ReadApproxObjectsNum,
		func(param *shard.ReadParam) {},
		func(resp []byte) {
			num += buf.Byte2Int(resp)
		},
	)
	if err != nil {
		//logutil.Info("approx objects num err",
		//	zap.Error(err))
		logutil.Infof("approx objects num err: %v", err)
		return 0
	}
	return num
}

func (tbl *txnTableDelegate) BuildReaders(
	ctx context.Context,
	proc any,
	expr *plan.Expr,
	relData engine.RelData,
	num int,
	txnOffset int,
	orderBy bool,
	policy engine.TombstoneApplyPolicy) ([]engine.Reader, error) {
	is, err := tbl.isLocal()
	if err != nil {
		return nil, err
	}
	if is {
		return tbl.origin.BuildReaders(
			ctx,
			proc,
			expr,
			relData,
			num,
			txnOffset,
			orderBy,
			engine.Policy_CheckAll,
		)
	}
	return tbl.BuildShardingReaders(
		ctx,
		proc,
		expr,
		relData,
		num,
		txnOffset,
		orderBy,
		engine.Policy_CheckAll,
	)
}

type shardingLocalReader struct {
	iteratePhase ReaderPhase
	closed       bool
	lrd          engine.Reader
	tblDelegate  *txnTableDelegate
	streamID     types.Uuid
	//relation data to distribute to remote CN which holds shard's partition state.
	remoteRelData         engine.RelData
	remoteTombApplyPolicy engine.TombstoneApplyPolicy
	remoteScanType        int
}

// TODO::
func MockShardingLocalReader() engine.Reader {
	return &shardingLocalReader{}
}

func (r *shardingLocalReader) Read(
	ctx context.Context,
	cols []string,
	expr *plan.Expr,
	mp *mpool.MPool,
	bat *batch.Batch,
) (isEnd bool, err error) {
	defer func() {
		if err != nil || isEnd {
			r.close()
		}
	}()

	for {

		switch r.iteratePhase {
		case InLocal:
			if r.lrd != nil {
				isEnd, err = r.lrd.Read(ctx, cols, expr, mp, bat)
				if err != nil {
					return
				}
				if !isEnd {
					return
				}
			}
			if r.remoteRelData == nil || r.remoteRelData.DataCnt() == 0 {
				r.iteratePhase = InEnd
				return
			}
			relData, err := r.remoteRelData.MarshalBinary()
			if err != nil {
				return false, err
			}
			err = r.tblDelegate.forwardRead(
				ctx,
				shardservice.ReadBuildReader,
				func(param *shard.ReadParam) {
					param.ReaderBuildParam.RelData = relData
					param.ReaderBuildParam.Expr = expr
					param.ReaderBuildParam.ScanType = int32(r.remoteScanType)
					param.ReaderBuildParam.TombstoneApplyPolicy = int32(r.remoteTombApplyPolicy)
				},
				func(resp []byte) {
					r.streamID = types.DecodeUuid(resp)
				},
			)
			if err != nil {
				return false, err
			}
			r.iteratePhase = InRemote
		case InRemote:
			err = r.tblDelegate.forwardRead(
				ctx,
				shardservice.ReadNext,
				func(param *shard.ReadParam) {
					param.ReadNextParam.Uuid = types.EncodeUuid(&r.streamID)
					param.ReadNextParam.Columns = cols
				},
				func(resp []byte) {
					isEnd = types.DecodeBool(resp)
					if isEnd {
						return
					}
					resp = resp[1:]
					l := types.DecodeUint32(resp)
					resp = resp[4:]
					if err := bat.UnmarshalBinary(resp[:l]); err != nil {
						panic(err)
					}
				},
			)
			if err != nil {
				return false, err
			}
			if isEnd {
				r.iteratePhase = InEnd
			}
			return
		case InEnd:
			return true, nil
		}

	}

}

func (r *shardingLocalReader) Close() error {
	return r.close()
}

func (r *shardingLocalReader) close() error {
	if !r.closed {
		if r.lrd != nil {
			r.lrd.Close()
		}
		if r.remoteRelData != nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			err := r.tblDelegate.forwardRead(
				ctx,
				shardservice.ReadClose,
				func(param *shard.ReadParam) {
					param.ReadCloseParam.Uuid = types.EncodeUuid(&r.streamID)
				},
				func(resp []byte) {
				},
			)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *shardingLocalReader) SetOrderBy(orderby []*plan.OrderBySpec) {
}

func (r *shardingLocalReader) GetOrderBy() []*plan.OrderBySpec {
	return nil
}

func (r *shardingLocalReader) SetFilterZM(zm objectio.ZoneMap) {
}

func (tbl *txnTableDelegate) BuildShardingReaders(
	ctx context.Context,
	p any,
	expr *plan.Expr,
	relData engine.RelData,
	num int,
	txnOffset int,
	orderBy bool,
	policy engine.TombstoneApplyPolicy,
) ([]engine.Reader, error) {
	var rds []engine.Reader
	proc := p.(*process.Process)

	if plan2.IsFalseExpr(expr) {
		return []engine.Reader{new(emptyReader)}, nil
	}

	if orderBy && num != 1 {
		return nil, moerr.NewInternalErrorNoCtx("orderBy only support one reader")
	}

	_, uncommittedObjNames := tbl.origin.collectUnCommittedDataObjs(txnOffset)
	uncommittedTombstones, err := tbl.origin.CollectTombstones(
		ctx,
		txnOffset,
		engine.Policy_CollectUncommittedTombstones)
	if err != nil {
		return nil, err
	}
	group := func(rd engine.RelData) (local engine.RelData, remote engine.RelData) {
		local = rd.BuildEmptyRelData()
		remote = rd.BuildEmptyRelData()
		engine.ForRangeBlockInfo(0, rd.DataCnt(), rd, func(bi objectio.BlockInfo) (bool, error) {
			if bi.IsMemBlk() {
				local.AppendBlockInfo(&bi)
				remote.AppendBlockInfo(&bi)
				return true, nil
			}
			if _, ok := uncommittedObjNames[*objectio.ShortName(&bi.BlockID)]; ok {
				local.AppendBlockInfo(&bi)
			} else {
				remote.AppendBlockInfo(&bi)
			}
			return true, nil
		})
		return
	}

	//relData maybe is nil, indicate that only read data from memory.
	if relData == nil || relData.DataCnt() == 0 {
		relData = NewBlockListRelationData(1)
	}

	blkCnt := relData.DataCnt()
	newNum := num
	if blkCnt < num {
		newNum = blkCnt
		for i := 0; i < num-blkCnt; i++ {
			rds = append(rds, new(emptyReader))
		}
	}

	scanType := determineScanType(relData, newNum)
	mod := blkCnt % newNum
	divide := blkCnt / newNum
	current := 0
	var shard engine.RelData
	for i := 0; i < newNum; i++ {
		if i < mod {
			shard = relData.DataSlice(current, current+divide+1)
			current = current + divide + 1
		} else {
			shard = relData.DataSlice(current, current+divide)
			current = current + divide
		}

		localRelData, remoteRelData := group(shard)

		srd := &shardingLocalReader{
			//lrd:                   lrd,
			tblDelegate: tbl,
			//remoteRelData:         remoteRelData,
			remoteTombApplyPolicy: engine.Policy_SkipUncommitedInMemory | engine.Policy_SkipUncommitedS3,
			remoteScanType:        scanType,
		}

		if localRelData.DataCnt() > 0 {
			ds, err := tbl.origin.buildLocalDataSource(
				ctx,
				txnOffset,
				localRelData,
				policy|engine.Policy_SkipCommittedInMemory|engine.Policy_SkipCommittedS3,
				engine.ShardingLocalDataSource)
			if err != nil {
				return nil, err
			}
			lrd, err := NewReader(
				ctx,
				proc,
				tbl.origin.getTxn().engine,
				tbl.origin.GetTableDef(ctx),
				tbl.origin.db.op.SnapshotTS(),
				expr,
				ds,
			)
			if err != nil {
				return nil, err
			}
			lrd.scanType = scanType
			srd.lrd = lrd
		}

		if remoteRelData.DataCnt() > 0 {
			remoteRelData.AttachTombstones(uncommittedTombstones)
			srd.remoteRelData = remoteRelData
		}
		rds = append(rds, srd)
	}

	return rds, nil
}

func (tbl *txnTableDelegate) PrimaryKeysMayBeModified(
	ctx context.Context,
	from types.TS,
	to types.TS,
	keyVector *vector.Vector,
) (bool, error) {
	is, err := tbl.isLocal()
	if err != nil {
		return false, err
	}
	if is {
		return tbl.origin.PrimaryKeysMayBeModified(
			ctx,
			from,
			to,
			keyVector,
		)
	}

	modify := false
	err = tbl.forwardRead(
		ctx,
		shardservice.ReadPrimaryKeysMayBeModified,
		func(param *shard.ReadParam) {
			f, err := from.Marshal()
			if err != nil {
				panic(err)
			}
			t, err := to.Marshal()
			if err != nil {
				panic(err)
			}
			v, err := keyVector.MarshalBinary()
			if err != nil {
				panic(err)
			}
			param.PrimaryKeysMayBeModifiedParam.From = f
			param.PrimaryKeysMayBeModifiedParam.To = t
			param.PrimaryKeysMayBeModifiedParam.KeyVector = v
		},
		func(resp []byte) {
			if buf.Byte2Uint16(resp) > 0 {
				modify = true
			}
		},
	)
	if err != nil {
		return false, err
	}
	return modify, nil
}

func (tbl *txnTableDelegate) MergeObjects(ctx context.Context, objstats []objectio.ObjectStats, targetObjSize uint32) (*api.MergeCommitEntry, error) {
	is, err := tbl.isLocal()
	if err != nil {
		return nil, err
	}
	if is {
		return tbl.origin.MergeObjects(ctx, objstats, targetObjSize)
	}

	var entry api.MergeCommitEntry
	err = tbl.forwardRead(
		ctx,
		shardservice.ReadMergeObjects,
		func(param *shard.ReadParam) {
			os := make([][]byte, len(objstats))
			for i, o := range objstats {
				os[i] = o.Marshal()
			}
			param.MergeObjectsParam.Objstats = os
			param.MergeObjectsParam.TargetObjSize = targetObjSize
		},
		func(resp []byte) {
			err := entry.Unmarshal(resp)
			if err != nil {
				panic(err)
			}
			// TODO: hash shard need to merge all shard in future
		},
	)
	if err != nil {
		return nil, err
	}
	return &entry, nil
}

func (tbl *txnTableDelegate) GetNonAppendableObjectStats(ctx context.Context) ([]objectio.ObjectStats, error) {
	is, err := tbl.isLocal()
	if err != nil {
		return nil, err
	}
	if is {
		return tbl.origin.GetNonAppendableObjectStats(
			ctx,
		)
	}

	var stats []objectio.ObjectStats
	err = tbl.forwardRead(
		ctx,
		shardservice.ReadVisibleObjectStats,
		func(param *shard.ReadParam) {},
		func(resp []byte) {
			if len(resp)%objectio.ObjectStatsLen != 0 {
				panic("invalid resp")
			}
			size := len(resp) / objectio.ObjectStatsLen
			stats = make([]objectio.ObjectStats, size)
			for i := range size {
				stats[i].UnMarshal(resp[i*objectio.ObjectStatsLen:])
			}
		},
	)
	if err != nil {
		return nil, err
	}
	return stats, nil
}

func (tbl *txnTableDelegate) TableDefs(
	ctx context.Context,
) ([]engine.TableDef, error) {
	return tbl.origin.TableDefs(ctx)
}

func (tbl *txnTableDelegate) GetTableDef(
	ctx context.Context,
) *plan.TableDef {
	return tbl.origin.GetTableDef(ctx)
}

func (tbl *txnTableDelegate) CopyTableDef(
	ctx context.Context,
) *plan.TableDef {
	return tbl.origin.CopyTableDef(ctx)
}

func (tbl *txnTableDelegate) GetPrimaryKeys(
	ctx context.Context,
) ([]*engine.Attribute, error) {
	return tbl.origin.GetPrimaryKeys(ctx)
}

func (tbl *txnTableDelegate) GetHideKeys(
	ctx context.Context,
) ([]*engine.Attribute, error) {
	return tbl.origin.GetHideKeys(ctx)
}

func (tbl *txnTableDelegate) Write(
	ctx context.Context,
	bat *batch.Batch,
) error {
	return tbl.origin.Write(ctx, bat)
}

func (tbl *txnTableDelegate) Update(
	ctx context.Context,
	bat *batch.Batch,
) error {
	return tbl.origin.Update(ctx, bat)
}

func (tbl *txnTableDelegate) Delete(
	ctx context.Context,
	bat *batch.Batch,
	name string,
) error {
	return tbl.origin.Delete(ctx, bat, name)
}

func (tbl *txnTableDelegate) AddTableDef(
	ctx context.Context,
	def engine.TableDef,
) error {
	return tbl.origin.AddTableDef(ctx, def)
}

func (tbl *txnTableDelegate) DelTableDef(
	ctx context.Context,
	def engine.TableDef,
) error {
	return tbl.origin.DelTableDef(ctx, def)
}

func (tbl *txnTableDelegate) AlterTable(
	ctx context.Context,
	c *engine.ConstraintDef,
	reqs []*api.AlterTableReq,
) error {
	return tbl.origin.AlterTable(ctx, c, reqs)
}

func (tbl *txnTableDelegate) UpdateConstraint(
	ctx context.Context,
	c *engine.ConstraintDef,
) error {
	return tbl.origin.UpdateConstraint(ctx, c)
}

func (tbl *txnTableDelegate) TableRenameInTxn(
	ctx context.Context,
	constraint [][]byte,
) error {
	return tbl.origin.TableRenameInTxn(ctx, constraint)
}

func (tbl *txnTableDelegate) GetTableID(
	ctx context.Context,
) uint64 {
	return tbl.origin.GetTableID(ctx)
}

func (tbl *txnTableDelegate) GetTableName() string {
	return tbl.origin.GetTableName()
}

func (tbl *txnTableDelegate) GetDBID(
	ctx context.Context,
) uint64 {
	return tbl.origin.GetDBID(ctx)
}

func (tbl *txnTableDelegate) TableColumns(
	ctx context.Context,
) ([]*engine.Attribute, error) {
	return tbl.origin.TableColumns(ctx)
}

func (tbl *txnTableDelegate) MaxAndMinValues(
	ctx context.Context,
) ([][2]any, []uint8, error) {
	return tbl.origin.MaxAndMinValues(ctx)
}

func (tbl *txnTableDelegate) GetEngineType() engine.EngineType {
	return tbl.origin.GetEngineType()
}

func (tbl *txnTableDelegate) GetProcess() any {
	return tbl.origin.GetProcess()
}

func (tbl *txnTableDelegate) isLocalFunc() (bool, error) {
	if !tbl.shard.service.Config().Enable || // sharding not enabled
		!tbl.shard.is || // sharding not enabled
		(tbl.shard.policy == shard.Policy_Partition && tbl.origin.tableId == tbl.shard.tableID) { // partition table self.
		return true, nil
	}

	return tbl.hasAllLocalReplicas() // all shard replicas on local
}

func (tbl *txnTableDelegate) hasAllLocalReplicas() (bool, error) {
	return tbl.shard.service.HasLocalReplica(
		tbl.shard.tableID,
		tbl.origin.tableId,
	)
}

func (tbl *txnTableDelegate) getReadRequest(
	method int,
	apply func([]byte),
) (shardservice.ReadRequest, error) {
	processInfo, err := tbl.origin.proc.Load().BuildProcessInfo(
		tbl.origin.createSql,
	)
	if err != nil {
		return shardservice.ReadRequest{}, err
	}

	return shardservice.ReadRequest{
		TableID: tbl.shard.tableID,
		Method:  method,
		Param: shard.ReadParam{
			Process: processInfo,
			TxnTable: shard.TxnTable{
				DatabaseID:   tbl.origin.db.databaseId,
				DatabaseName: tbl.origin.db.databaseName,
				AccountID:    uint64(tbl.origin.accountId),
				TableName:    tbl.origin.tableName,
				CreatedInTxn: tbl.origin.isCreatedInTxn(),
			},
		},
		Apply: apply,
	}, nil
}

// Just for UT.
func (tbl *txnTableDelegate) mockForwardRead(
	ctx context.Context,
	method int,
	request shardservice.ReadRequest,
) ([]byte, error) {

	handles := map[int]shardservice.ReadFunc{
		shardservice.ReadRows:                     HandleShardingReadRows,
		shardservice.ReadSize:                     HandleShardingReadSize,
		shardservice.ReadStats:                    HandleShardingReadStatus,
		shardservice.ReadApproxObjectsNum:         HandleShardingReadApproxObjectsNum,
		shardservice.ReadRanges:                   HandleShardingReadRanges,
		shardservice.ReadGetColumMetadataScanInfo: HandleShardingReadGetColumMetadataScanInfo,
		shardservice.ReadBuildReader:              HandleShardingReadBuildReader,
		shardservice.ReadPrimaryKeysMayBeModified: HandleShardingReadPrimaryKeysMayBeModified,
		shardservice.ReadMergeObjects:             HandleShardingReadMergeObjects,
		shardservice.ReadVisibleObjectStats:       HandleShardingReadVisibleObjectStats,
		shardservice.ReadClose:                    HandleShardingReadClose,
		shardservice.ReadNext:                     HandleShardingReadNext,
		shardservice.ReadCollectTombstones:        HandleShardingReadCollectTombstones,
	}
	buf := morpc.NewBuffer()
	resp, err := handles[method](
		ctx,
		shard.TableShard{},
		tbl.origin.getEngine(),
		request.Param,
		tbl.origin.db.op.SnapshotTS(),
		buf,
	)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (tbl *txnTableDelegate) forwardRead(
	ctx context.Context,
	method int,
	applyParam func(*shard.ReadParam),
	apply func([]byte),
) error {
	request, err := tbl.getReadRequest(
		method,
		apply,
	)
	if err != nil {
		return err
	}

	applyParam(&request.Param)

	if tbl.isMock {
		res, err := tbl.mockForwardRead(ctx, method, request)
		if err != nil {
			return err
		}
		apply(res)
		return nil
	}

	shardID := uint64(0)
	switch tbl.shard.policy {
	case shard.Policy_Partition:
		// Partition sharding only send to the current shard. The partition table id
		// is the shard id
		shardID = tbl.origin.tableId
	default:
		// otherwise, we need send to all shards
	}

	err = tbl.shard.service.Read(
		ctx,
		request,
		shardservice.ReadOptions{}.Shard(shardID),
	)
	if err != nil {
		return err
	}

	return nil
}
