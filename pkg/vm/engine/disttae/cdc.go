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
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/tidwall/btree"

	"github.com/matrixorigin/matrixone/pkg/catalog"
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

	cdcEng.ddlListener = NewDDLAnalyzer(cdcEng.catalog)

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

func (cdcEng *CdcEngine) InQueue() Queue[tools.Pair[*TableCtx, *DecoderInput]] {
	return cdcEng.inQueue
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
	return err
}

var _ DDLListener = new(DDLAnalyzer)

type DDLTableItem struct {
	CPKey      []byte
	AccountId  uint64
	DB         string
	Table      string
	DBId       uint64
	TableId    uint64
	OldTableId uint64
	Deleted    bool
	Ts         timestamp.Timestamp
}

func DDLTableItemLess(a, b *DDLTableItem) bool {
	return bytes.Compare(a.CPKey[:], b.CPKey[:]) < 0
}

type DDLDBItem struct {
	CPKey     []byte
	AccountId uint64
	DB        string
	DBId      uint64
	Deleted   bool
	Ts        timestamp.Timestamp
}

func DDLDBItemLess(a, b *DDLDBItem) bool {
	return bytes.Compare(a.CPKey[:], b.CPKey[:]) < 0
}

type DDLAnalyzer struct {
	ts     timestamp.Timestamp
	tables *btree.BTreeG[*DDLTableItem]
	dbs    *btree.BTreeG[*DDLDBItem]
	cache  *cache.CatalogCache
}

func NewDDLAnalyzer(
	catCache *cache.CatalogCache,
) *DDLAnalyzer {
	return &DDLAnalyzer{
		tables: btree.NewBTreeG(DDLTableItemLess),
		dbs:    btree.NewBTreeG(DDLDBItemLess),
		cache:  catCache,
	}
}

func analyzeTablesBatch(bat *batch.Batch, curTs timestamp.Timestamp, f func(*DDLTableItem)) {
	//rowids := vector.MustFixedCol[types.Rowid](bat.GetVector(cache.MO_ROWID_IDX))
	timestamps := vector.MustFixedCol[types.TS](bat.GetVector(cache.MO_TIMESTAMP_IDX))
	accounts := vector.MustFixedCol[uint32](bat.GetVector(catalog.MO_TABLES_ACCOUNT_ID_IDX + cache.MO_OFF))
	names := bat.GetVector(catalog.MO_TABLES_REL_NAME_IDX + cache.MO_OFF)
	ids := vector.MustFixedCol[uint64](bat.GetVector(catalog.MO_TABLES_REL_ID_IDX + cache.MO_OFF))
	databaseIds := vector.MustFixedCol[uint64](bat.GetVector(catalog.MO_TABLES_RELDATABASE_ID_IDX + cache.MO_OFF))
	databaseNames := bat.GetVector(catalog.MO_TABLES_RELDATABASE_IDX + cache.MO_OFF)
	kinds := bat.GetVector(catalog.MO_TABLES_RELKIND_IDX + cache.MO_OFF)
	//comments := bat.GetVector(catalog.MO_TABLES_REL_COMMENT_IDX + cache.MO_OFF)
	//createSqls := bat.GetVector(catalog.MO_TABLES_REL_CREATESQL_IDX + cache.MO_OFF)
	//viewDefs := bat.GetVector(catalog.MO_TABLES_VIEWDEF_IDX + cache.MO_OFF)
	//partitioneds := vector.MustFixedCol[int8](bat.GetVector(catalog.MO_TABLES_PARTITIONED_IDX + cache.MO_OFF))
	//paritions := bat.GetVector(catalog.MO_TABLES_PARTITION_INFO_IDX + cache.MO_OFF)
	//constraints := bat.GetVector(catalog.MO_TABLES_CONSTRAINT_IDX + cache.MO_OFF)
	//versions := vector.MustFixedCol[uint32](bat.GetVector(catalog.MO_TABLES_VERSION_IDX + cache.MO_OFF))
	//catalogVersions := vector.MustFixedCol[uint32](bat.GetVector(catalog.MO_TABLES_CATALOG_VERSION_IDX + cache.MO_OFF))
	pks := bat.GetVector(catalog.MO_TABLES_CPKEY_IDX + cache.MO_OFF)
	for i, account := range accounts {
		//skip item that does not happen at the same time
		if !curTs.Equal(timestamps[i].ToTimestamp()) {
			continue
		}
		item := new(DDLTableItem)
		item.TableId = ids[i]
		item.Table = names.GetStringAt(i)
		item.AccountId = uint64(account)
		item.DBId = databaseIds[i]
		item.DB = databaseNames.GetStringAt(i)
		item.Ts = timestamps[i].ToTimestamp()
		//item.Kind = kinds.GetStringAt(i)
		kind := kinds.GetStringAt(i)
		//skip non general table
		if needSkipThisTable(kind, item.DB, item.Table) {
			continue
		}
		//item.ViewDef = viewDefs.GetStringAt(i)
		//item.Constraint = append(item.Constraint, constraints.GetBytesAt(i)...)
		//item.Comment = comments.GetStringAt(i)
		//item.Partitioned = partitioneds[i]
		//item.Partition = paritions.GetStringAt(i)
		//item.CreateSql = createSqls.GetStringAt(i)
		//item.Version = versions[i]
		//item.CatalogVersion = catalogVersions[i]
		//item.PrimaryIdx = -1
		//item.PrimarySeqnum = -1
		//item.ClusterByIdx = -1
		//copy(item.Rowid[:], rowids[i][:])
		item.CPKey = append(item.CPKey, pks.GetBytesAt(i)...)

		f(item)
	}
}

func (analyzer *DDLAnalyzer) InsertTable(bat *batch.Batch) {
	analyzeTablesBatch(bat, analyzer.ts, func(item *DDLTableItem) {
		if prevItem, ok := analyzer.tables.Get(item); ok {
			//prev item must be labeled deleted
			if !prevItem.Deleted {
				fmt.Fprintln(os.Stderr, "analyzer [insert] ddl table item twice")
				return
			}
			prevItem.OldTableId = prevItem.TableId
			prevItem.TableId = item.TableId
		} else {
			analyzer.tables.Set(item)
		}
	})
}

func (analyzer *DDLAnalyzer) DeleteTable(bat *batch.Batch) {
	cpks := bat.GetVector(cache.MO_OFF + 0)
	timestamps := vector.MustFixedCol[types.TS](bat.GetVector(cache.MO_TIMESTAMP_IDX))
	for i, ts := range timestamps {
		if !ts.ToTimestamp().Equal(analyzer.ts) {
			continue
		}
		pk := cpks.GetBytesAt(i)
		//the table item saved in catalogCache
		if item, ok := analyzer.cache.GetTableByCPKey(pk); ok {
			// Note: the newItem.Id is the latest id under the name of the table,
			// not the id that should be seen at the moment ts.
			// Lucy thing is that the wrong tableid hold by this delete item will never be used.
			newItem := &DDLTableItem{
				Deleted:    true,
				TableId:    item.Id,
				OldTableId: 0,
				Table:      item.Name,
				CPKey:      append([]byte{}, item.CPKey...),
				AccountId:  uint64(item.AccountId),
				DBId:       item.DatabaseId,
				DB:         item.DatabaseName,
				Ts:         ts.ToTimestamp(),
			}
			analyzer.tables.Set(newItem)
		}
	}
}

func analyzeDatabaseBatch(bat *batch.Batch, curTs timestamp.Timestamp, f func(*DDLDBItem)) {
	//rowids := vector.MustFixedCol[types.Rowid](bat.GetVector(cache.MO_ROWID_IDX))
	timestamps := vector.MustFixedCol[types.TS](bat.GetVector(cache.MO_TIMESTAMP_IDX))
	accounts := vector.MustFixedCol[uint32](bat.GetVector(catalog.MO_DATABASE_ACCOUNT_ID_IDX + cache.MO_OFF))
	names := bat.GetVector(catalog.MO_DATABASE_DAT_NAME_IDX + cache.MO_OFF)
	ids := vector.MustFixedCol[uint64](bat.GetVector(catalog.MO_DATABASE_DAT_ID_IDX + cache.MO_OFF))
	typs := bat.GetVector(catalog.MO_DATABASE_DAT_TYPE_IDX + cache.MO_OFF)
	//createSqls := bat.GetVector(catalog.MO_DATABASE_CREATESQL_IDX + cache.MO_OFF)
	pks := bat.GetVector(catalog.MO_DATABASE_CPKEY_IDX + cache.MO_OFF)
	for i, account := range accounts {
		if !timestamps[i].ToTimestamp().Equal(curTs) {
			continue
		}
		item := new(DDLDBItem)
		item.DBId = ids[i]
		item.DB = names.GetStringAt(i)
		typ := typs.GetStringAt(i)
		if needSkipThisDatabase(typ, item.DB) {
			continue
		}
		item.AccountId = uint64(account)
		item.Ts = timestamps[i].ToTimestamp()

		//item.CreateSql = createSqls.GetStringAt(i)
		//copy(item.Rowid[:], rowids[i][:])
		item.CPKey = append(item.CPKey, pks.GetBytesAt(i)...)
		f(item)
	}
}

func (analyzer *DDLAnalyzer) InsertDatabase(bat *batch.Batch) {
	analyzeDatabaseBatch(bat, analyzer.ts, func(item *DDLDBItem) {
		analyzer.dbs.Set(item)
	})
}

func (analyzer *DDLAnalyzer) DeleteDatabase(bat *batch.Batch) {
	cpks := bat.GetVector(cache.MO_OFF + 0)
	timestamps := vector.MustFixedCol[types.TS](bat.GetVector(cache.MO_TIMESTAMP_IDX))
	for i, ts := range timestamps {
		pk := cpks.GetBytesAt(i)
		if item, ok := analyzer.cache.GetDatabaseByCPKey(pk); ok {
			newItem := &DDLDBItem{
				Deleted:   true,
				DBId:      item.Id,
				DB:        item.Name,
				CPKey:     append([]byte{}, item.CPKey...),
				AccountId: uint64(item.AccountId),
				Ts:        ts.ToTimestamp(),
			}
			analyzer.dbs.Set(newItem)
			//fmt.Fprintln(os.Stderr, "[", cc.cdcId, "]", "catalog cache delete database",
			//	item.AccountId, item.Name, item.Id, item.Ts)
		}
	}
}

func (analyzer *DDLAnalyzer) OnAction(typ ActionType, bat *batch.Batch) {
	switch typ {
	case ActionInsertTable:
		analyzer.InsertTable(bat)
	case ActionDeleteTable:
		analyzer.DeleteTable(bat)
	case ActionInsertDatabase:
		analyzer.InsertDatabase(bat)
	case ActionDeleteDatabase:
		analyzer.DeleteDatabase(bat)
	default:
	}
}

func (analyzer *DDLAnalyzer) GetDDLs() (res []*DDL) {
	analyzer.tables.Scan(func(item *DDLTableItem) bool {
		ddl := new(DDL)
		ddl.AccountId = item.AccountId
		ddl.DB = item.DB
		ddl.Table = item.Table
		ddl.DBId = item.DBId
		ddl.TableId = item.TableId
		ddl.OldTableId = item.OldTableId
		if item.Deleted {
			if item.OldTableId == 0 { //drop table
				ddl.Typ = DDLTypeDropTable
			} else {
				if item.OldTableId != item.TableId {
					//alter table copy or
					//truncate table
					ddl.Typ = DDLTypeAlterTableCopy
					//TODO: check schema change
				} else {
					//alter table inplace
					ddl.Typ = DDLTypeAlterTableInplace
				}
			}

		} else {
			//create table
			ddl.Typ = DDLTypeCreateTable
		}
		res = append(res, ddl)
		return true
	})

	analyzer.dbs.Scan(func(item *DDLDBItem) bool {
		ddl := new(DDL)
		ddl.AccountId = item.AccountId
		ddl.DB = item.DB
		ddl.DBId = item.DBId
		if item.Deleted {
			ddl.Typ = DDLTypeDropDB
		} else {
			ddl.Typ = DDLTypeCreateDB
		}
		res = append(res, ddl)
		return true
	})
	return
}

func (analyzer *DDLAnalyzer) Clear() {
	analyzer.ts = timestamp.Timestamp{}
	analyzer.tables.Clear()
	analyzer.dbs.Clear()
}

func (analyzer *DDLAnalyzer) Init(ts timestamp.Timestamp) {
	analyzer.ts = ts
}
