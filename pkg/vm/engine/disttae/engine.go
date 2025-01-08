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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	client2 "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/stack"
	"github.com/matrixorigin/matrixone/pkg/version"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/route"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ engine.Engine = new(Engine)

func New(
	ctx context.Context,
	service string,
	mp *mpool.MPool,
	fs fileservice.FileService,
	cli client.TxnClient,
	hakeeper logservice.CNHAKeeperClient,
	keyRouter client2.KeyRouter[pb.StatsInfoKey],
	updateWorkerFactor int,
	options ...EngineOptions,
) *Engine {
	cluster := clusterservice.GetMOCluster(service)
	services := cluster.GetAllTNServices()

	var tnID string
	if len(services) > 0 {
		tnID = services[0].ServiceID
	}

	ls, ok := moruntime.ServiceRuntime(service).GetGlobalVariables(moruntime.LockService)
	if !ok {
		logutil.Fatalf("missing lock service")
	}

	e := &Engine{
		service:  service,
		mp:       mp,
		fs:       fs,
		ls:       ls.(lockservice.LockService),
		hakeeper: hakeeper,
		cli:      cli,
		idGen:    hakeeper,
		tnID:     tnID,
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
	}
	e.mu.snapParts = make(map[[2]uint64]*struct {
		sync.Mutex
		snaps []*logtailreplay.Partition
	})

	pool, err := ants.NewPool(GCPoolSize)
	if err != nil {
		panic(err)
	}
	e.gcPool = pool

	e.globalStats = NewGlobalStats(ctx, e, keyRouter,
		WithUpdateWorkerFactor(updateWorkerFactor))

	e.messageCenter = &message.MessageCenter{
		StmtIDToBoard: make(map[uuid.UUID]*message.MessageBoard, 64),
		RwMutex:       &sync.Mutex{},
	}

	for _, opt := range options {
		opt(e)
	}
	e.fillDefaults()

	if err := e.init(ctx); err != nil {
		panic(err)
	}

	e.pClient.LogtailRPCClientFactory = DefaultNewRpcStreamToTnLogTailService
	e.pClient.ctx = ctx

	err = initMoTableStatsConfig(ctx, e)
	if err != nil {
		panic(err)
	}

	return e
}

func (e *Engine) Close() error {
	if e.gcPool != nil {
		_ = e.gcPool.ReleaseTimeout(time.Second * 3)
	}
	e.dynamicCtx.Close()
	return nil
}

func (e *Engine) fillDefaults() {
	if e.config.insertEntryMaxCount <= 0 {
		e.config.insertEntryMaxCount = InsertEntryThreshold
	}
	if e.config.commitWorkspaceThreshold <= 0 {
		e.config.commitWorkspaceThreshold = CommitWorkspaceThreshold
	}
	if e.config.writeWorkspaceThreshold <= 0 {
		e.config.writeWorkspaceThreshold = WriteWorkspaceThreshold
	}
	if e.config.extraWorkspaceThreshold <= 0 {
		e.config.extraWorkspaceThreshold = ExtraWorkspaceThreshold
	}
	if e.config.cnTransferTxnLifespanThreshold <= 0 {
		e.config.cnTransferTxnLifespanThreshold = CNTransferTxnLifespanThreshold
	}
	if e.config.quota.Load() <= 0 {
		mem := objectio.TotalMem() / 100 * 5
		e.config.quota.Store(mem)
		v2.TxnExtraWorkspaceQuotaGauge.Set(float64(mem))
	}

	logutil.Info(
		"INIT-ENGINE-CONFIG",
		zap.Int("InsertEntryMaxCount", e.config.insertEntryMaxCount),
		zap.Uint64("CommitWorkspaceThreshold", e.config.commitWorkspaceThreshold),
		zap.Uint64("WriteWorkspaceThreshold", e.config.writeWorkspaceThreshold),
		zap.Uint64("ExtraWorkspaceThresholdQuota", e.config.quota.Load()),
		zap.Duration("CNTransferTxnLifespanThreshold", e.config.cnTransferTxnLifespanThreshold),
	)
}

// SetWorkspaceThreshold updates the commit and write workspace thresholds (in MB).
// Non-zero values override the current thresholds, while zero keeps them unchanged.
// Returns the previous thresholds (in MB).
func (e *Engine) SetWorkspaceThreshold(commitThreshold, writeThreshold uint64) (commit, write uint64) {
	commit = e.config.commitWorkspaceThreshold / mpool.MB
	write = e.config.writeWorkspaceThreshold / mpool.MB
	if commitThreshold != 0 {
		e.config.commitWorkspaceThreshold = commitThreshold * mpool.MB
	}
	if writeThreshold != 0 {
		e.config.writeWorkspaceThreshold = writeThreshold * mpool.MB
	}
	return
}

func (e *Engine) AcquireQuota(v uint64) (uint64, bool) {
	for {
		oldRemaining := e.config.quota.Load()
		if oldRemaining < v {
			return 0, false
		}
		remaining := oldRemaining - v
		if e.config.quota.CompareAndSwap(oldRemaining, remaining) {
			v2.TxnExtraWorkspaceQuotaGauge.Set(float64(remaining))
			return remaining, true
		}
	}
}

func (e *Engine) ReleaseQuota(quota uint64) (remaining uint64) {
	e.config.quota.Add(quota)
	remaining = e.config.quota.Load()
	v2.TxnExtraWorkspaceQuotaGauge.Set(float64(remaining))
	return
}

func (e *Engine) GetService() string {
	return e.service
}

func (e *Engine) Create(ctx context.Context, name string, op client.TxnOperator) error {
	if op.IsSnapOp() {
		return moerr.NewInternalErrorNoCtx("create database in snapshot txn")
	}
	txn, err := txnIsValid(op)
	if err != nil {
		return err
	}
	typ := getTyp(ctx)
	sql := getSql(ctx)
	accountId, userId, roleId, err := getAccessInfo(ctx)
	if err != nil {
		return err
	}
	databaseId, err := txn.allocateID(ctx)
	if err != nil {
		return err
	}

	var packer *types.Packer
	put := e.packerPool.Get(&packer)
	defer put.Put()
	bat, err := catalog.GenCreateDatabaseTuple(sql, accountId, userId, roleId,
		name, databaseId, typ, txn.proc.Mp(), packer)
	if err != nil {
		return err
	}
	// non-io operations do not need to pass context
	note := noteForCreate(uint64(accountId), name)
	if _, err = txn.WriteBatch(INSERT, note, catalog.System_Account, catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID,
		catalog.MO_CATALOG, catalog.MO_DATABASE, bat, txn.tnStores[0]); err != nil {
		bat.Clean(txn.proc.Mp())
		return err
	}

	key := genDatabaseKey(accountId, name)
	txn.databaseOps.addCreateDatabase(key, txn.statementID, &txnDatabase{
		op:           op,
		databaseId:   databaseId,
		databaseName: name,
	})
	return nil
}

func (e *Engine) loadDatabaseFromStorage(
	ctx context.Context,
	accountID uint32,
	name string, op client.TxnOperator) (*cache.DatabaseItem, error) {
	sql := fmt.Sprintf(catalog.MoDatabaseAllQueryFormat, accountID, name)
	now := time.Now()
	defer func() {
		if time.Since(now) > time.Second {
			logutil.Info("FIND_TABLE slow loadDatabaseFromStorage",
				zap.String("sql", sql), zap.Duration("cost", time.Since(now)))
		}
	}()
	res, err := execReadSql(ctx, op, sql, true)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	logerror := func() {
		logutil.Error("FIND_TABLE bad loadDatabaseFromStorage", zap.String("batch", stringifySlice(res.Batches, func(a any) string {
			bat := a.(*batch.Batch)
			return common.MoBatchToString(bat, 10)
		})), zap.String("sql", sql))
	}

	if len(res.Batches) != 1 { // not found
		if len(res.Batches) > 1 {
			logerror()
		}
		return nil, nil
	}
	if row := res.Batches[0].RowCount(); row != 1 {
		logerror()
		panic("FIND_TABLE loadDatabaseFromStorage failed: table result row cnt != 1")
	}
	bat := res.Batches[0]

	ts := types.TimestampToTS(op.SnapshotTS())
	if err := fillTsVecForSysTableQueryBatch(bat, ts, res.Mp); err != nil {
		return nil, err
	}
	var ret *cache.DatabaseItem
	cache.ParseDatabaseBatchAnd(bat, func(di *cache.DatabaseItem) {
		ret = di
	})
	return ret, nil
}

func (e *Engine) Database(
	ctx context.Context,
	name string,
	op client.TxnOperator,
) (engine.Database, error) {
	common.DoIfDebugEnabled(func() {
		logutil.Debug(
			"Transaction.Database",
			zap.String("txn", op.Txn().DebugString()),
			zap.String("name", name),
		)
	})

	txn, err := txnIsValid(op)
	if err != nil {
		return nil, err
	}
	if name == catalog.MO_CATALOG {
		db := &txnDatabase{
			op:           op,
			databaseId:   catalog.MO_CATALOG_ID,
			databaseName: name,
		}
		return db, nil
	}
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}

	// check the database is deleted or not
	key := genDatabaseKey(accountId, name)
	if txn.databaseOps.existAndDeleted(key) {
		return nil, moerr.NewParseErrorf(ctx, "database %q does not exist", name)
	}

	if v := txn.databaseOps.existAndActive(key); v != nil {
		return v, nil
	}

	item := &cache.DatabaseItem{
		Name:      name,
		AccountId: accountId,
		Ts:        txn.op.SnapshotTS(),
	}

	catalog := e.GetLatestCatalogCache()

	if ok := catalog.GetDatabase(item); !ok {
		if !catalog.CanServe(types.TimestampToTS(op.SnapshotTS())) {
			logutil.Info("FIND_TABLE loadDatabaseFromStorage", zap.String("name", name), zap.String("cacheTs", catalog.GetStartTS().ToString()), zap.String("txn", op.Txn().DebugString()))
			// read batch from storage
			if item, err = e.loadDatabaseFromStorage(ctx, accountId, name, op); err != nil {
				return nil, err
			}
			if item == nil {
				return nil, moerr.GetOkExpectedEOB()
			}
		} else {
			return nil, moerr.GetOkExpectedEOB()
		}
	}

	return &txnDatabase{
		op:                op,
		databaseName:      name,
		databaseId:        item.Id,
		databaseType:      item.Typ,
		databaseCreateSql: item.CreateSql,
	}, nil
}

func (e *Engine) Databases(ctx context.Context, op client.TxnOperator) ([]string, error) {
	aid, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}
	sql := fmt.Sprintf(catalog.MoDatabasesInEngineQueryFormat, aid)

	res, err := execReadSql(ctx, op, sql, true)
	if err != nil {
		return nil, err
	}

	defer res.Close()

	var dbs []string
	for _, b := range res.Batches {
		for i, v := 0, b.Vecs[0]; i < v.Length(); i++ {
			dbs = append(dbs, v.GetStringAt(i))
		}
	}
	return dbs, nil
}

func (e *Engine) GetNameById(ctx context.Context, op client.TxnOperator, tableId uint64) (dbName string, tblName string, err error) {
	dbName, tblName, _, err = e.GetRelationById(ctx, op, tableId)
	return
}

func loadNameByIdFromStorage(ctx context.Context, op client.TxnOperator, accountId uint32, tableId uint64) (dbName string, tblName string, err error) {
	sql := fmt.Sprintf(catalog.MoTablesQueryNameById, accountId, tableId)
	tblanmes, dbnames := []string{}, []string{}
	result, err := execReadSql(ctx, op, sql, true)
	if err != nil {
		return "", "", err
	}
	for _, b := range result.Batches {
		for i := 0; i < b.RowCount(); i++ {
			tblanmes = append(tblanmes, b.Vecs[0].GetStringAt(i))
			dbnames = append(dbnames, b.Vecs[1].GetStringAt(i))
		}
	}
	if len(tblanmes) != 1 {
		logutil.Warn("FIND_TABLE GetRelationById sql failed",
			zap.Uint64("tableId", tableId), zap.Uint32("accountId", accountId),
			zap.Strings("tblanmes", tblanmes), zap.Strings("dbnames", dbnames), zap.String("txn", op.Txn().DebugString()))
	} else {
		tblName = tblanmes[0]
		dbName = dbnames[0]
	}
	return
}

func (e *Engine) GetRelationById(ctx context.Context, op client.TxnOperator, tableId uint64) (dbName, tableName string, rel engine.Relation, err error) {
	if catalog.IsSystemTable(tableId) {
		dbName = catalog.MO_CATALOG
		db := &txnDatabase{
			op:           op,
			databaseId:   catalog.MO_CATALOG_ID,
			databaseName: dbName,
		}
		switch tableId {
		case catalog.MO_DATABASE_ID:
			tableName = catalog.MO_DATABASE
		case catalog.MO_TABLES_ID:
			tableName = catalog.MO_TABLES
		case catalog.MO_COLUMNS_ID:
			tableName = catalog.MO_COLUMNS
		}
		rel, err = db.Relation(ctx, tableName, nil)
		return
	}

	accountId, _ := defines.GetAccountId(ctx)
	txn := op.GetWorkspace().(*Transaction)
	dbName, tableName, deleted := txn.tableOps.queryNameByTid(tableId)
	if tableName == "" && deleted {
		return "", "", nil, moerr.NewInternalErrorf(ctx, "can not find table by id %d: accountId: %v. Deleted in txn", tableId, accountId)
	}

	// not found in tableOps, try cache
	if tableName == "" {
		cache := e.GetLatestCatalogCache()
		cacheItem := cache.GetTableByIdAndTime(accountId, 0 /*db is not specified */, tableId, txn.op.SnapshotTS())
		if cacheItem != nil {
			tableName = cacheItem.Name
			dbName = cacheItem.DatabaseName
		} else if !cache.CanServe(types.TimestampToTS(op.SnapshotTS())) {
			// not found in cache, try storage
			logutil.Info("FIND_TABLE loadNameByIdFromStorage", zap.String("txn", op.Txn().DebugString()), zap.Uint64("tableId", tableId))
			dbName, tableName, err = loadNameByIdFromStorage(ctx, op, accountId, tableId)
			if err != nil {
				return "", "", nil, err
			}
		}
	}

	if tableName == "" {
		accountId, _ := defines.GetAccountId(ctx)
		logutil.Error("FIND_TABLE GetRelationById failed",
			zap.Uint64("tableId", tableId), zap.Uint32("accountId", accountId), zap.String("workspace", txn.PPString()))
		return "", "", nil, moerr.NewInternalErrorf(ctx, "can not find table by id %d: accountId: %v ", tableId, accountId)
	}

	txnDb, err := e.Database(ctx, dbName, op)
	if err != nil {
		return "", "", nil, err
	}

	txnTable, err := txnDb.Relation(ctx, tableName, nil)
	if err != nil {
		return "", "", nil, err
	}

	return dbName, tableName, txnTable, nil
}

func (e *Engine) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	return e.idGen.AllocateIDByKey(ctx, key)
}

func (e *Engine) Delete(ctx context.Context, name string, op client.TxnOperator) (err error) {
	defer func() {
		if err != nil {
			if strings.Contains(name, "sysbench_db") {
				logutil.Errorf("delete database %s failed: %v", name, err)
				logutil.Errorf("stack: %s", stack.Callers(3))
				logutil.Errorf("txnmeta %v", op.Txn().DebugString())
			}
		}
	}()
	if op.IsSnapOp() {
		return moerr.NewInternalErrorNoCtx("delete database in snapshot txn")
	}

	var txn *Transaction
	txn, err = txnIsValid(op)
	if err != nil {
		return err
	}

	// Get the database to be deleted
	toDelDB, err := e.Database(ctx, name, op)
	if err != nil {
		return err
	}

	// delete all tables of the database
	rels, err := toDelDB.Relations(ctx)
	if err != nil {
		return err
	}
	for _, relName := range rels {
		if err := toDelDB.Delete(ctx, relName); err != nil {
			return err
		}
	}

	// fetch (accountid, databaseid, rowid) to delete the database
	databaseId := toDelDB.(*txnDatabase).databaseId
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	res, err := execReadSql(ctx, op, fmt.Sprintf(catalog.MoDatabaseRowidQueryFormat, accountId, name), true)
	if err != nil {
		return err
	}
	if len(res.Batches) != 1 || res.Batches[0].Vecs[0].Length() != 1 {
		logutil.Error("FIND_TABLE deleteDatabaseError",
			zap.String("bat", stringifySlice(res.Batches, func(a any) string {
				bat := a.(*batch.Batch)
				return common.MoBatchToString(bat, 10)
			})),
			zap.Uint32("accountId", accountId),
			zap.String("name", name),
			zap.Uint64("did", databaseId),
			zap.String("workspace", op.GetWorkspace().PPString()))
		panic("delete table failed: query failed")
	}
	rowId := vector.GetFixedAtNoTypeCheck[types.Rowid](res.Batches[0].Vecs[0], 0)

	// write the batch to delete the database
	var packer *types.Packer
	put := e.packerPool.Get(&packer)
	defer put.Put()
	bat, err := catalog.GenDropDatabaseTuple(rowId, accountId, databaseId, name, txn.proc.Mp(), packer)
	if err != nil {
		return err
	}

	if bat = txn.deleteBatch(bat, catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID); bat.RowCount() > 0 {
		note := noteForDrop(uint64(accountId), name)
		if _, err := txn.WriteBatch(DELETE, note, catalog.System_Account, catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID,
			catalog.MO_CATALOG, catalog.MO_DATABASE, bat, txn.tnStores[0]); err != nil {
			bat.Clean(txn.proc.Mp())
			return err
		}
	}

	// adjust the state of txn cache
	key := genDatabaseKey(accountId, name)
	txn.databaseOps.addDeleteDatabase(key, txn.statementID, databaseId)
	return nil
}

func (e *Engine) New(ctx context.Context, op client.TxnOperator) error {
	common.DoIfDebugEnabled(func() {
		logutil.Debug(
			"Transaction.New",
			zap.String("txn", op.Txn().DebugString()),
		)
	})
	proc := process.NewTopProcess(
		ctx,
		e.mp,
		e.cli,
		op,
		e.fs,
		e.ls,
		e.qc,
		e.hakeeper,
		e.us,
		nil,
	)
	txn := NewTxnWorkSpace(e, proc)
	op.AddWorkspace(txn)
	txn.BindTxnOp(op)

	e.pClient.validLogTailMustApplied(txn.op.SnapshotTS())
	return nil
}

func (e *Engine) Nodes(
	isInternal bool, tenant string, username string, cnLabel map[string]string,
) (engine.Nodes, error) {
	var ncpu = system.GoMaxProcs()
	var nodes engine.Nodes

	start := time.Now()
	defer func() {
		v2.TxnStatementNodesHistogram.Observe(time.Since(start).Seconds())
	}()

	cluster := clusterservice.GetMOCluster(e.service)
	var selector clusterservice.Selector

	// If the requested labels are empty, return all CN servers.
	if len(cnLabel) == 0 {
		cluster.GetCNService(selector, func(c metadata.CNService) bool {
			if c.CommitID == version.CommitID {
				nodes = append(nodes, engine.Node{
					// should use c.CPUTotal to set Mcpu for the compile and pipeline.
					// ref: https://github.com/matrixorigin/matrixone/issues/17935
					Mcpu: ncpu,
					Id:   c.ServiceID,
					Addr: c.PipelineServiceAddress,
				})
			}
			return true
		})
		return nodes, nil
	}

	selector = clusterservice.NewSelector().SelectByLabel(cnLabel, clusterservice.EQ_Globbing)
	if isInternal || strings.ToLower(tenant) == "sys" {
		route.RouteForSuperTenant(
			e.service,
			selector,
			username,
			nil,
			func(s *metadata.CNService) {
				if s.CommitID == version.CommitID {
					nodes = append(nodes, engine.Node{
						Mcpu: ncpu,
						Id:   s.ServiceID,
						Addr: s.PipelineServiceAddress,
					})
				}
			},
		)
	} else {
		route.RouteForCommonTenant(
			e.service,
			selector,
			nil,
			func(s *metadata.CNService) {
				if s.CommitID == version.CommitID {
					nodes = append(nodes, engine.Node{
						Mcpu: ncpu,
						Id:   s.ServiceID,
						Addr: s.PipelineServiceAddress,
					})
				}
			},
		)
	}
	return nodes, nil
}

func (e *Engine) Hints() (h engine.Hints) {
	h.CommitOrRollbackTimeout = time.Minute * 5
	return
}

func (e *Engine) BuildBlockReaders(
	ctx context.Context,
	p any,
	ts timestamp.Timestamp,
	expr *plan.Expr,
	def *plan.TableDef,
	relData engine.RelData,
	num int) ([]engine.Reader, error) {
	var rds []engine.Reader
	proc := p.(*process.Process)
	blkCnt := relData.DataCnt()
	newNum := num
	if blkCnt < num {
		newNum = blkCnt
		for i := 0; i < num-blkCnt; i++ {
			rds = append(rds, new(readutil.EmptyReader))
		}
	}
	if blkCnt == 0 {
		return rds, nil
	}
	fs, err := fileservice.Get[fileservice.FileService](e.fs, defines.SharedFileServiceName)
	if err != nil {
		return nil, err
	}

	shards := relData.Split(newNum)
	for i := 0; i < newNum; i++ {
		ds := readutil.NewRemoteDataSource(ctx, fs, ts, shards[i])
		rd, err := readutil.NewReader(
			ctx,
			proc.Mp(),
			e.packerPool,
			e.fs,
			def,
			ts,
			expr,
			ds,
			readutil.GetThresholdForReader(newNum),
			engine.FilterHint{},
		)
		if err != nil {
			return nil, err
		}
		rds = append(rds, rd)
	}
	return rds, nil
}

func (e *Engine) GetTNServices() []DNStore {
	cluster := clusterservice.GetMOCluster(e.service)
	return cluster.GetAllTNServices()
}

func (e *Engine) setPushClientStatus(ready bool) {
	e.Lock()
	defer e.Unlock()

	if ready {
		e.cli.Resume()
	} else {
		e.cli.Pause()
	}

	e.pClient.receivedLogTailTime.ready.Store(ready)
	if e.pClient.subscriber != nil {
		if ready {
			e.pClient.subscriber.setReady()
		} else {
			e.pClient.subscriber.setNotReady()
		}
	}
}

func (e *Engine) cleanMemoryTableWithTable(dbId, tblId uint64) {
	e.Lock()
	defer e.Unlock()
	// XXX it's probably not a good way to do that.
	// after we set it to empty, actually this part of memory was not immediately released.
	// maybe a very old transaction still using that.
	delete(e.partitions, [2]uint64{dbId, tblId})

	//  When removing the PartitionState, you need to remove the tid in globalStats,
	// When re-subscribing, globalStats will wait for the PartitionState to be consumed before updating the object state.
	e.globalStats.RemoveTid(tblId)
	logutil.Debugf("clean memory table of tbl[dbId: %d, tblId: %d]", dbId, tblId)
}

func (e *Engine) safeToUnsubscribe(tid uint64) bool {
	return e.globalStats.safeToUnsubscribe(tid)
}

func (e *Engine) PushClient() *PushClient {
	return &e.pClient
}

// TryToSubscribeTable implements the LogtailEngine interface.
func (e *Engine) TryToSubscribeTable(ctx context.Context, dbID, tbID uint64) error {
	return e.PushClient().TryToSubscribeTable(ctx, dbID, tbID)
}

// UnsubscribeTable implements the LogtailEngine interface.
func (e *Engine) UnsubscribeTable(ctx context.Context, dbID, tbID uint64) error {
	return e.PushClient().UnsubscribeTable(ctx, dbID, tbID)
}

func (e *Engine) Stats(ctx context.Context, key pb.StatsInfoKey, sync bool) *pb.StatsInfo {
	return e.globalStats.Get(ctx, key, sync)
}

// return true if the prefetch is received
// return false if the prefetch is not rejected
func (e *Engine) PrefetchTableMeta(ctx context.Context, key pb.StatsInfoKey) bool {
	return e.globalStats.PrefetchTableMeta(ctx, key)
}

func (e *Engine) GetMessageCenter() any {
	return e.messageCenter
}

func (e *Engine) FS() fileservice.FileService {
	return e.fs
}

func (e *Engine) PackerPool() *fileservice.Pool[*types.Packer] {
	return e.packerPool
}

func (e *Engine) LatestLogtailAppliedTime() timestamp.Timestamp {
	return e.pClient.LatestLogtailAppliedTime()
}
