// Copyright 2022 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/tools"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

func NewCdcEngine(
	ctx context.Context,
	service string,
	mp *mpool.MPool,
	fs fileservice.FileService,
	etlFs fileservice.FileService,
	cli client.TxnClient,
	cdcId string,
	inQueue Queue[tools.Pair[*TableCtx, *DecoderInput]],
	cnEng engine.Engine,
	cnTxnClient client.TxnClient,
) *CdcEngine {
	cdcEng := &CdcEngine{
		service: service,
		mp:      mp,
		fs:      fs,
		etlFs:   etlFs,
		packerPool: fileservice.NewPool(
			128,
			func() *types.Packer {
				return types.NewPacker()
			},
			func(packer *types.Packer) {
				packer.Reset()
			},
			func(packer *types.Packer) {
				packer.Close()
			},
		),
		cli:         cli,
		cdcId:       cdcId,
		inQueue:     inQueue,
		cnEng:       cnEng,
		cnTxnClient: cnTxnClient,
	}

	if err := cdcEng.init(ctx); err != nil {
		panic(err)
	}

	cdcEng.pClient.LogtailRPCClientFactory = DefaultNewRpcStreamToTnLogTailService
	cdcEng.pClient.cdcId = cdcId
	return cdcEng
}

func (cdcEng *CdcEngine) init(ctx context.Context) (err error) {
	cdcEng.Lock()
	defer cdcEng.Unlock()

	cdcEng.catalog = cache.NewCatalog()
	cdcEng.catalog.SetCdcId(cdcEng.cdcId)
	cdcEng.partitions = make(map[[2]uint64]*logtailreplay.Partition)

	_, _, _, _, err = initEngine(ctx, cdcEng.service, cdcEng.catalog, cdcEng.partitions, cdcEng.mp, cdcEng.packerPool)
	return err
}

func (cdcEng *CdcEngine) Enqueue(tail *logtail.TableLogtail) {}

func (cdcEng *CdcEngine) GetOrCreateLatestPart(databaseId uint64, tableId uint64) *logtailreplay.Partition {
	cdcEng.Lock()
	defer cdcEng.Unlock()
	partition, ok := cdcEng.partitions[[2]uint64{databaseId, tableId}]
	if !ok { // create a new table
		partition = logtailreplay.NewPartition(cdcEng.service, tableId)
		cdcEng.partitions[[2]uint64{databaseId, tableId}] = partition
	}
	return partition
}

func (cdcEng *CdcEngine) GetLatestCatalogCache() *cache.CatalogCache {
	return cdcEng.catalog
}

func (cdcEng *CdcEngine) Get(ptr **types.Packer) fileservice.PutBack[*types.Packer] {
	return cdcEng.packerPool.Get(ptr)
}

func (cdcEng *CdcEngine) GetMPool() *mpool.MPool {
	return cdcEng.mp
}

func (cdcEng *CdcEngine) GetFS() fileservice.FileService {
	return cdcEng.fs
}

func (cdcEng *CdcEngine) FS() fileservice.FileService {
	return cdcEng.fs
}

func (cdcEng *CdcEngine) GetService() string {
	return cdcEng.service
}

func (cdcEng *CdcEngine) getTNServices() []DNStore {
	return getTNServices(cdcEng.service)
}

func (cdcEng *CdcEngine) setPushClientStatus(ready bool) {
	cdcEng.Lock()
	defer cdcEng.Unlock()
	cdcEng.pClient.setStatusUnlock(ready)
}

func (cdcEng *CdcEngine) abortAllRunningTxn() {}

func (cdcEng *CdcEngine) CopyPartitions() map[[2]uint64]*logtailreplay.Partition {
	return copyPartitionsLock(&cdcEng.RWMutex, cdcEng.partitions)
}

func (cdcEng *CdcEngine) cleanMemoryTableWithTable(dbId, tblId uint64) {
	cleanMemoryTableWithTableLock(&cdcEng.RWMutex, cdcEng.partitions, nil, dbId, tblId)
}

func (cdcEng *CdcEngine) PushClient() *PushClient {
	return &cdcEng.pClient
}

func (cdcEng *CdcEngine) Cli() client.TxnClient {
	//return cdcEng.cli
	panic("usp")
}

func (cdcEng *CdcEngine) New(ctx context.Context, op client.TxnOperator) error {
	//panic("usp")
	return nil
}

func (cdcEng *CdcEngine) TryToSubscribeTable(ctx context.Context, dbID, tbID uint64) error {
	return cdcEng.PushClient().TryToSubscribeTable(ctx, dbID, tbID)
}

func (cdcEng *CdcEngine) UnsubscribeTable(ctx context.Context, u uint64, u2 uint64) error {
	//TODO implement me
	panic("implement me")
}

func (cdcEng *CdcEngine) Delete(ctx context.Context, databaseName string, op client.TxnOperator) error {
	//TODO implement me
	panic("implement me")
}

func (cdcEng *CdcEngine) Create(ctx context.Context, databaseName string, op client.TxnOperator) error {
	//TODO implement me
	panic("implement me")
}

func (cdcEng *CdcEngine) Databases(ctx context.Context, op client.TxnOperator) (databaseNames []string, err error) {
	//TODO implement me
	panic("implement me")
}

func (cdcEng *CdcEngine) Database(ctx context.Context, databaseName string, op client.TxnOperator) (engine.Database, error) {
	//TODO implement me
	panic("implement me")
}

func (cdcEng *CdcEngine) Nodes(isInternal bool, tenant string, username string, cnLabel map[string]string) (cnNodes engine.Nodes, err error) {
	//TODO implement me
	panic("implement me")
}

func (cdcEng *CdcEngine) Hints() engine.Hints {
	//TODO implement me
	panic("implement me")
}

func (cdcEng *CdcEngine) NewBlockReader(ctx context.Context, num int, ts timestamp.Timestamp, expr *plan.Expr, blockReadPKFilter any, ranges []byte, tblDef *plan.TableDef, proc any) ([]engine.Reader, error) {
	//TODO implement me
	panic("implement me")
}

func (cdcEng *CdcEngine) GetNameById(ctx context.Context, op client.TxnOperator, tableId uint64) (dbName string, tblName string, err error) {
	//TODO implement me
	panic("implement me")
}

func (cdcEng *CdcEngine) GetRelationById(ctx context.Context, op client.TxnOperator, tableId uint64) (dbName string, tblName string, rel engine.Relation, err error) {
	//TODO implement me
	panic("implement me")
}

func (cdcEng *CdcEngine) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (cdcEng *CdcEngine) Stats(ctx context.Context, key pb.StatsInfoKey, sync bool) *pb.StatsInfo {
	//TODO implement me
	panic("implement me")
}

func (cdcEng *CdcEngine) GetMessageCenter() any {
	//TODO implement me
	panic("implement me")
}

func (cdcEng *CdcEngine) IsCdcEngine() bool {
	return true
}

func (cdcEng *CdcEngine) ToCdc(cdcCtx *TableCtx, decInput *DecoderInput) {
	cdcEng.inQueue.Push(tools.NewPair[*TableCtx, *DecoderInput](cdcCtx, decInput))
}

func (cdcEng *CdcEngine) BuildBlockReaders(ctx context.Context, proc any, ts timestamp.Timestamp, expr *plan.Expr, def *plan.TableDef, relData engine.RelData, num int) ([]engine.Reader, error) {
	//TODO implement me
	panic("implement me")
}

func NewCdcRelation(
	db, table string,
	accountId, dbId, tableId uint64,
	cdcEng *CdcEngine,
) *CdcRelation {
	return &CdcRelation{
		db:        db,
		table:     table,
		accountId: accountId,
		dbId:      dbId,
		tableId:   tableId,
		cdcEng:    cdcEng,
	}
}

func (cdcTbl *CdcRelation) Stats(ctx context.Context, sync bool) (*pb.StatsInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) Rows(ctx context.Context) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) Size(ctx context.Context, columnName string) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) TableDefs(ctx context.Context) ([]engine.TableDef, error) {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) GetTableDef(ctx context.Context) *plan.TableDef {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) CopyTableDef(ctx context.Context) *plan.TableDef {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) GetPrimaryKeys(ctx context.Context) ([]*engine.Attribute, error) {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) GetHideKeys(ctx context.Context) ([]*engine.Attribute, error) {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) Write(ctx context.Context, b *batch.Batch) error {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) Update(ctx context.Context, b *batch.Batch) error {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) Delete(ctx context.Context, b *batch.Batch, s string) error {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) AddTableDef(ctx context.Context, def engine.TableDef) error {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) DelTableDef(ctx context.Context, def engine.TableDef) error {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) UpdateConstraint(ctx context.Context, def *engine.ConstraintDef) error {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) AlterTable(ctx context.Context, def *engine.ConstraintDef, reqs []*api.AlterTableReq) error {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) TableRenameInTxn(ctx context.Context, constraint [][]byte) error {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) GetTableID(ctx context.Context) uint64 {
	return cdcTbl.tableId
}

func (cdcTbl *CdcRelation) GetTableName() string {
	return cdcTbl.table
}

func (cdcTbl *CdcRelation) GetDBID(ctx context.Context) uint64 {
	return cdcTbl.dbId
}

func (cdcTbl *CdcRelation) NewReader(ctx context.Context, i int, expr *plan.Expr, i2 []byte, b bool, i3 int) ([]engine.Reader, error) {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) TableColumns(ctx context.Context) ([]*engine.Attribute, error) {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) MaxAndMinValues(ctx context.Context) ([][2]any, []uint8, error) {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) GetEngineType() engine.EngineType {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) GetColumMetadataScanInfo(ctx context.Context, name string) ([]*plan.MetadataScanInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) PrimaryKeysMayBeModified(ctx context.Context, from types.TS, to types.TS, keyVector *vector.Vector) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) ApproxObjectsNum(ctx context.Context) int {
	//TODO implement me
	panic("implement me")
}

func (cdcTbl *CdcRelation) GetOldTableID() uint64 {
	return 0
}

func (cdcTbl *CdcRelation) GetDBName() string {
	return cdcTbl.db
}

func (cdcTbl *CdcRelation) GetPrimarySeqNum() int {
	return 0
}

func (cdcTbl *CdcRelation) tryToSubscribe(ctx context.Context) (ps *logtailreplay.PartitionState, err error) {
	ps, err = cdcTbl.cdcEng.PushClient().toSubscribeTable(ctx, cdcTbl)
	if err != nil {
		return nil, err
	}
	part := cdcTbl.cdcEng.GetOrCreateLatestPart(cdcTbl.GetDBID(ctx), cdcTbl.GetTableID(ctx))
	part.TableInfo.AccountId = cdcTbl.accountId
	return
}

func (cdcTbl *CdcRelation) getPartitionState(
	ctx context.Context,
	dbId, tableId uint64,
) (*logtailreplay.PartitionState, error) {
	if cdcTbl._partState.Load() == nil {
		ps, err := cdcTbl.tryToSubscribe(ctx)
		if err != nil {
			return nil, err
		}
		if ps == nil {
			ps = cdcTbl.cdcEng.GetOrCreateLatestPart(dbId, tableId).Snapshot()
		}
		cdcTbl._partState.Store(ps)
	}
	return cdcTbl._partState.Load(), nil
}

func SubscribeCdcTable(ctx context.Context, cdcTbl *CdcRelation, dbId, tableId uint64) error {
	//to subscribe the logtail for the table
	_, err := cdcTbl.getPartitionState(ctx, dbId, tableId)
	if err != nil {
		return err
	}
	return nil
}

//func Transform(
//	ctx context.Context,
//	cnEngine engine.Engine,
//	cnTxnClient client.TxnClient,
//	ts timestamp.Timestamp,
//	accId uint64,
//	db, table string) (dbId uint64, tableId uint64, err error) {
//	ctxAccid, _ := defines.GetAccountId(ctx)
//	var op client.TxnOperator
//	op, err = cnTxnClient.New(ctx, timestamp.Timestamp{}, client.WithSkipPushClientReady(), client.WithSnapshotTS(ts))
//	if err != nil {
//		return
//	}
//	_ = cnEngine.New(ctx, op)
//	defer func() {
//		if err != nil {
//			op.Rollback(ctx)
//		} else {
//			op.Commit(ctx)
//		}
//	}()
//
//	sql := getSqlForDbIdAndTableId(accId, db, table)
//	//TODO: bind account id?
//	res, err := execReadSql(ctx, op, sql, false)
//	if err != nil {
//		return
//	}
//	defer res.Close()
//
//	found := false
//	for _, b := range res.Batches {
//		rowCnt := b.RowCount()
//		if rowCnt == 0 {
//			continue
//		}
//		for rowIndex := 0; rowIndex < rowCnt; rowIndex++ {
//			dbId = vector.GetFixedAt[uint64](b.Vecs[0], rowIndex)
//			tableId = vector.GetFixedAt[uint64](b.Vecs[1], rowIndex)
//			found = true
//			break
//		}
//		if found {
//			break
//		}
//	}
//	if !found {
//		return 0, 0, moerr.NewInternalError(ctx, "do not find accId:%d ctxAccid:%d %s.%s in mo_catalog.mo_tables",
//			accId,
//			ctxAccid,
//			db,
//			table)
//	}
//	return dbId, tableId, nil
//}
