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
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	_ "go.uber.org/automaxprocs"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
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
	txn2 "github.com/matrixorigin/matrixone/pkg/pb/txn"
	client2 "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/route"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ engine.Engine = new(Engine)
var ncpu = runtime.GOMAXPROCS(0)

func New(
	ctx context.Context,
	mp *mpool.MPool,
	fs fileservice.FileService,
	cli client.TxnClient,
	hakeeper logservice.CNHAKeeperClient,
	keyRouter client2.KeyRouter[pb.StatsInfoKey],
	threshold int,
) *Engine {
	cluster := clusterservice.GetMOCluster()
	services := cluster.GetAllTNServices()

	var tnID string
	if len(services) > 0 {
		tnID = services[0].ServiceID
	}

	ls, ok := moruntime.ProcessLevelRuntime().GetGlobalVariables(moruntime.LockService)
	if !ok {
		logutil.Fatalf("missing lock service")
	}

	e := &Engine{
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
				return types.NewPacker(mp)
			},
			func(packer *types.Packer) {
				packer.Reset()
				packer.FreeMem()
			},
			func(packer *types.Packer) {
				packer.Reset()
				packer.FreeMem()
			},
		),
	}
	e.snapCatalog = &struct {
		sync.Mutex
		snaps []*cache.CatalogCache
	}{}
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
		WithLogtailUpdateStatsThreshold(threshold),
	)

	if err := e.init(ctx); err != nil {
		panic(err)
	}

	return e
}

func (e *Engine) Create(ctx context.Context, name string, op client.TxnOperator) error {
	if op.IsSnapOp() {
		return moerr.NewInternalErrorNoCtx("create database in snapshot txn")
	}
	txn := op.GetWorkspace().(*Transaction)
	if txn == nil {
		return moerr.NewTxnClosedNoCtx(op.Txn().ID)
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

	bat, err := genCreateDatabaseTuple(sql, accountId, userId, roleId,
		name, databaseId, typ, txn.proc.Mp())
	if err != nil {
		return err
	}
	vec := vector.NewVec(types.T_Rowid.ToType())
	rowId := txn.genRowId()
	if err := vector.AppendFixed(vec, rowId, false, txn.proc.Mp()); err != nil {
		vec.Free(txn.proc.Mp())
		return err
	}

	bat.Vecs = append([]*vector.Vector{vec}, bat.Vecs...)
	bat.Attrs = append([]string{catalog.Row_ID}, bat.Attrs...)
	// non-io operations do not need to pass context
	if err = txn.WriteBatch(INSERT, 0, catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID,
		catalog.MO_CATALOG, catalog.MO_DATABASE, bat, txn.tnStores[0], -1, true, false); err != nil {
		bat.Clean(txn.proc.Mp())
		return err
	}

	key := genDatabaseKey(accountId, name)
	txn.databaseMap.Store(key, &txnDatabase{
		op:           op,
		databaseId:   databaseId,
		databaseName: name,
		rowId:        rowId,
	})

	txn.deletedDatabaseMap.Delete(key)
	return nil
}

func (e *Engine) DatabaseByAccountID(
	accountID uint32,
	name string,
	op client.TxnOperator) (engine.Database, error) {
	logDebugf(op.Txn(), "Engine.DatabaseByAccountID %s", name)
	txn := op.GetWorkspace().(*Transaction)
	if txn == nil || txn.op.Status() == txn2.TxnStatus_Aborted {
		return nil, moerr.NewTxnClosedNoCtx(op.Txn().ID)
	}
	if v, ok := txn.databaseMap.Load(databaseKey{name: name, accountId: accountID}); ok {
		return v.(*txnDatabase), nil
	}
	if name == catalog.MO_CATALOG {
		db := &txnDatabase{
			op:           op,
			databaseId:   catalog.MO_CATALOG_ID,
			databaseName: name,
		}
		return db, nil
	}
	key := &cache.DatabaseItem{
		Name:      name,
		AccountId: accountID,
		Ts:        txn.op.SnapshotTS(),
	}
	var catalog *cache.CatalogCache
	var err error
	if !txn.op.IsSnapOp() {
		catalog = e.getLatestCatalogCache()
	} else {
		catalog, err = e.getOrCreateSnapCatalogCache(
			context.Background(),
			types.TimestampToTS(txn.op.SnapshotTS()))
		if err != nil {
			return nil, err
		}
	}
	if ok := catalog.GetDatabase(key); !ok {
		return nil, moerr.GetOkExpectedEOB()
	}
	return &txnDatabase{
		op:                op,
		databaseName:      name,
		databaseId:        key.Id,
		rowId:             key.Rowid,
		databaseType:      key.Typ,
		databaseCreateSql: key.CreateSql,
	}, nil
}

func (e *Engine) Database(ctx context.Context, name string,
	op client.TxnOperator) (engine.Database, error) {
	logDebugf(op.Txn(), "Engine.Database %s", name)
	txn := op.GetWorkspace().(*Transaction)
	if txn == nil || txn.op.Status() == txn2.TxnStatus_Aborted {
		return nil, moerr.NewTxnClosedNoCtx(op.Txn().ID)
	}
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}

	// check the database is deleted or not
	key := genDatabaseKey(accountId, name)
	if _, exist := txn.deletedDatabaseMap.Load(key); exist {
		return nil, moerr.NewParseError(ctx, "database %q does not exist", name)
	}

	if v, ok := txn.databaseMap.Load(key); ok {
		return v.(*txnDatabase), nil
	}

	if name == catalog.MO_CATALOG {
		db := &txnDatabase{
			op:           op,
			databaseId:   catalog.MO_CATALOG_ID,
			databaseName: name,
		}
		return db, nil
	}

	item := &cache.DatabaseItem{
		Name:      name,
		AccountId: accountId,
		Ts:        txn.op.SnapshotTS(),
	}

	var catalog *cache.CatalogCache
	if !txn.op.IsSnapOp() {
		catalog = e.getLatestCatalogCache()
	} else {
		catalog, err = e.getOrCreateSnapCatalogCache(
			ctx,
			types.TimestampToTS(txn.op.SnapshotTS()))
		if err != nil {
			return nil, err
		}
	}

	if ok := catalog.GetDatabase(item); !ok {
		return nil, moerr.GetOkExpectedEOB()
	}

	return &txnDatabase{
		op:                op,
		databaseName:      name,
		databaseId:        item.Id,
		rowId:             item.Rowid,
		databaseType:      item.Typ,
		databaseCreateSql: item.CreateSql,
	}, nil
}

func (e *Engine) Databases(ctx context.Context, op client.TxnOperator) ([]string, error) {
	var dbs []string

	txn := op.GetWorkspace().(*Transaction)
	if txn == nil {
		return nil, moerr.NewTxnClosed(ctx, op.Txn().ID)
	}
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}

	//first get all delete tables
	deleteDatabases := make(map[string]any)
	txn.deletedDatabaseMap.Range(func(k, _ any) bool {
		key := k.(databaseKey)
		if key.accountId == accountId {
			deleteDatabases[key.name] = nil
		}
		return true
	})

	txn.databaseMap.Range(func(k, _ any) bool {
		key := k.(databaseKey)
		if key.accountId == accountId {
			// if the database is deleted, do not save it.
			if _, exist := deleteDatabases[key.name]; !exist {
				dbs = append(dbs, key.name)
			}
		}
		return true
	})

	var catalog *cache.CatalogCache
	if !txn.op.IsSnapOp() {
		catalog = e.getLatestCatalogCache()
	} else {
		catalog, err = e.getOrCreateSnapCatalogCache(
			ctx,
			types.TimestampToTS(txn.op.SnapshotTS()))
		if err != nil {
			return nil, err
		}
	}
	dbsInCatalog := catalog.Databases(accountId, txn.op.SnapshotTS())
	dbsExceptDelete := removeIf[string](dbsInCatalog, func(t string) bool {
		return find[string](deleteDatabases, t)
	})
	dbs = append(dbs, dbsExceptDelete...)
	return dbs, nil
}

func (e *Engine) GetNameById(ctx context.Context, op client.TxnOperator, tableId uint64) (dbName string, tblName string, err error) {
	txn := op.GetWorkspace().(*Transaction)
	if txn == nil {
		return "", "", moerr.NewTxnClosed(ctx, op.Txn().ID)
	}
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return "", "", err
	}
	var db engine.Database
	noRepCtx := errutil.ContextWithNoReport(ctx, true)
	txn.databaseMap.Range(func(k, _ any) bool {
		key := k.(databaseKey)
		dbName = key.name
		if key.accountId == accountId {
			db, err = e.Database(noRepCtx, key.name, op)
			if err != nil {
				return false
			}
			distDb := db.(*txnDatabase)
			tblName, err = distDb.getTableNameById(ctx, key.id)
			if err != nil {
				return false
			}
			if tblName != "" {
				return false
			}
		}
		return true
	})
	var catalog *cache.CatalogCache
	if !op.IsSnapOp() {
		catalog = e.getLatestCatalogCache()
	} else {
		catalog, err = e.getOrCreateSnapCatalogCache(
			ctx,
			types.TimestampToTS(op.SnapshotTS()))
		if err != nil {
			return "", "", err
		}
	}
	if tblName == "" {
		dbNames := getDatabasesExceptDeleted(accountId, catalog, txn)
		for _, databaseName := range dbNames {
			db, err = e.Database(noRepCtx, databaseName, op)
			if err != nil {
				return "", "", err
			}
			distDb := db.(*txnDatabase)
			tableName, rel, err := distDb.getRelationById(noRepCtx, tableId)
			if err != nil {
				return "", "", err
			}
			if rel != nil {
				tblName = tableName
				dbName = databaseName
				break
			}
		}
	}

	if tblName == "" {
		return "", "", moerr.NewInternalError(ctx, "can not find table name by id %d", tableId)
	}

	return
}

func (e *Engine) GetRelationById(ctx context.Context, op client.TxnOperator, tableId uint64) (dbName, tableName string, rel engine.Relation, err error) {
	txn := op.GetWorkspace().(*Transaction)
	if txn == nil {
		return "", "", nil, moerr.NewTxnClosed(ctx, op.Txn().ID)
	}
	switch tableId {
	case catalog.MO_DATABASE_ID:
		db := &txnDatabase{
			op:           op,
			databaseId:   catalog.MO_CATALOG_ID,
			databaseName: catalog.MO_CATALOG,
		}
		defs := catalog.MoDatabaseTableDefs
		return catalog.MO_CATALOG, catalog.MO_DATABASE,
			db.openSysTable(nil, tableId, catalog.MO_DATABASE, defs), nil
	case catalog.MO_TABLES_ID:
		db := &txnDatabase{
			op:           op,
			databaseId:   catalog.MO_CATALOG_ID,
			databaseName: catalog.MO_CATALOG,
		}
		defs := catalog.MoTablesTableDefs
		return catalog.MO_CATALOG, catalog.MO_TABLES,
			db.openSysTable(nil, tableId, catalog.MO_TABLES, defs), nil
	case catalog.MO_COLUMNS_ID:
		db := &txnDatabase{
			op:           op,
			databaseId:   catalog.MO_CATALOG_ID,
			databaseName: catalog.MO_CATALOG,
		}
		defs := catalog.MoColumnsTableDefs
		return catalog.MO_CATALOG, catalog.MO_COLUMNS,
			db.openSysTable(nil, tableId, catalog.MO_COLUMNS, defs), nil
	}
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return "", "", nil, err
	}
	var db engine.Database
	noRepCtx := errutil.ContextWithNoReport(ctx, true)
	txn.databaseMap.Range(func(k, _ any) bool {
		key := k.(databaseKey)
		dbName = key.name
		// the mo_catalog now can be accessed by all accounts
		if dbName == catalog.MO_CATALOG || key.accountId == accountId {
			db, err = e.Database(noRepCtx, key.name, op)
			if err != nil {
				return false
			}
			distDb := db.(*txnDatabase)
			tableName, rel, err = distDb.getRelationById(noRepCtx, tableId)
			if err != nil {
				return false
			}
			if rel != nil {
				return false
			}
		}
		return true
	})
	var catache *cache.CatalogCache
	if !op.IsSnapOp() {
		catache = e.getLatestCatalogCache()
	} else {
		catache, err = e.getOrCreateSnapCatalogCache(
			ctx,
			types.TimestampToTS(op.SnapshotTS()))
		if err != nil {
			return "", "", nil, err
		}
	}
	if rel == nil {
		dbNames := getDatabasesExceptDeleted(accountId, catache, txn)
		fn := func(dbName string) error {
			db, err = e.Database(noRepCtx, dbName, op)
			if err != nil {
				return err
			}
			distDb := db.(*txnDatabase)
			tableName, rel, err = distDb.getRelationById(noRepCtx, tableId)
			if err != nil {
				return err
			}
			return nil
		}
		for _, dbName = range dbNames {
			if err := fn(dbName); err != nil {
				return "", "", nil, err
			}
			if rel != nil {
				break
			}
		}
		if rel == nil {
			if err := fn(catalog.MO_CATALOG); err != nil {
				return "", "", nil, err
			}
		}
	}

	if rel == nil {
		if tableId == 2 {
			logutil.Errorf("can not find table by id %d: accountId: %v", tableId, accountId)
			tbls, tblIds := catache.Tables(accountId, 1, op.SnapshotTS())
			logutil.Errorf("tables: %v, tableIds: %v", tbls, tblIds)
			util.CoreDump()
		}
		return "", "", nil, moerr.NewInternalError(ctx, "can not find table by id %d: accountId: %v ", tableId, accountId)
	}
	return
}

func (e *Engine) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	return e.idGen.AllocateIDByKey(ctx, key)
}

func (e *Engine) Delete(ctx context.Context, name string, op client.TxnOperator) error {
	var databaseId uint64
	var rowId types.Rowid
	//var db *txnDatabase
	if op.IsSnapOp() {
		return moerr.NewInternalErrorNoCtx("delete database in snapshot txn")
	}

	txn := op.GetWorkspace().(*Transaction)
	if txn == nil {
		return moerr.NewTxnClosedNoCtx(op.Txn().ID)
	}

	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	key := genDatabaseKey(accountId, name)
	if val, ok := txn.databaseMap.Load(key); ok {
		txn.databaseMap.Delete(key)
		database := val.(*txnDatabase)
		databaseId = database.databaseId
		rowId = database.rowId
		//return nil
	} else {
		item := &cache.DatabaseItem{
			Name:      name,
			AccountId: accountId,
			Ts:        txn.op.SnapshotTS(),
		}
		if ok = e.getLatestCatalogCache().GetDatabase(item); !ok {
			return moerr.GetOkExpectedEOB()
		}

		databaseId = item.Id
		rowId = item.Rowid
		//db = &txnDatabase{
		//	op:           op,
		//	databaseName: name,
		//	databaseId:   item.Id,
		//	rowId:        item.Rowid,
		//}
	}

	dbNew := &txnDatabase{
		op:           op,
		databaseName: name,
		databaseId:   databaseId,
		rowId:        rowId,
	}

	rels, err := dbNew.Relations(ctx)
	if err != nil {
		return err
	}
	for _, relName := range rels {
		if err := dbNew.Delete(ctx, relName); err != nil {
			return err
		}
	}
	bat, err := genDropDatabaseTuple(rowId, databaseId, name, txn.proc.Mp())
	if err != nil {
		return err
	}
	// non-io operations do not need to pass context
	if err := txn.WriteBatch(DELETE, 0, catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID,
		catalog.MO_CATALOG, catalog.MO_DATABASE, bat, txn.tnStores[0], -1, true, false); err != nil {
		bat.Clean(txn.proc.Mp())
		return err
	}

	dbNew.getTxn().deletedDatabaseMap.Store(key, databaseId)
	return nil
}

func (e *Engine) New(ctx context.Context, op client.TxnOperator) error {
	logDebugf(op.Txn(), "Engine.New")
	proc := process.New(
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

	id := objectio.NewSegmentid()
	bytes := types.EncodeUuid(id)
	txn := &Transaction{
		op:     op,
		proc:   proc,
		engine: e,
		//meta:     op.TxnRef(),
		idGen:    e.idGen,
		tnStores: e.getTNServices(),
		tableCache: struct {
			cachedIndex int
			tableMap    *sync.Map
		}{
			tableMap: new(sync.Map),
		},
		databaseMap:        new(sync.Map),
		deletedDatabaseMap: new(sync.Map),
		createMap:          new(sync.Map),
		deletedTableMap:    new(sync.Map),
		rowId: [6]uint32{
			types.DecodeUint32(bytes[0:4]),
			types.DecodeUint32(bytes[4:8]),
			types.DecodeUint32(bytes[8:12]),
			types.DecodeUint32(bytes[12:16]),
			0,
			0,
		},
		segId: *id,
		deletedBlocks: &deletedBlocks{
			offsets: map[types.Blockid][]int64{},
		},
		cnBlkId_Pos:          map[types.Blockid]Pos{},
		batchSelectList:      make(map[*batch.Batch][]int64),
		toFreeBatches:        make(map[tableKey][]*batch.Batch),
		syncCommittedTSCount: e.cli.GetSyncLatestCommitTSTimes(),
	}

	txn.blockId_tn_delete_metaLoc_batch = struct {
		sync.RWMutex
		data map[types.Blockid][]*batch.Batch
	}{data: make(map[types.Blockid][]*batch.Batch)}

	txn.readOnly.Store(true)
	// transaction's local segment for raw batch.
	colexec.Get().PutCnSegment(id, colexec.TxnWorkSpaceIdType)
	op.AddWorkspace(txn)

	e.pClient.validLogTailMustApplied(txn.op.SnapshotTS())
	return nil
}

func (e *Engine) Nodes(
	isInternal bool, tenant string, username string, cnLabel map[string]string,
) (engine.Nodes, error) {
	var nodes engine.Nodes

	start := time.Now()
	defer func() {
		v2.TxnStatementNodesHistogram.Observe(time.Since(start).Seconds())
	}()

	cluster := clusterservice.GetMOCluster()
	var selector clusterservice.Selector

	// If the requested labels are empty, return all CN servers.
	if len(cnLabel) == 0 {
		cluster.GetCNService(selector, func(c metadata.CNService) bool {
			nodes = append(nodes, engine.Node{
				Mcpu: ncpu,
				Id:   c.ServiceID,
				Addr: c.PipelineServiceAddress,
			})
			return true
		})
		return nodes, nil
	}

	selector = clusterservice.NewSelector().SelectByLabel(cnLabel, clusterservice.EQ_Globbing)
	if isInternal || strings.ToLower(tenant) == "sys" {
		route.RouteForSuperTenant(selector, username, nil, func(s *metadata.CNService) {
			nodes = append(nodes, engine.Node{
				Mcpu: ncpu,
				Id:   s.ServiceID,
				Addr: s.PipelineServiceAddress,
			})
		})
	} else {
		route.RouteForCommonTenant(selector, nil, func(s *metadata.CNService) {
			nodes = append(nodes, engine.Node{
				Mcpu: ncpu,
				Id:   s.ServiceID,
				Addr: s.PipelineServiceAddress,
			})
		})
	}
	return nodes, nil
}

func (e *Engine) Hints() (h engine.Hints) {
	h.CommitOrRollbackTimeout = time.Minute * 5
	return
}

func (e *Engine) NewBlockReader(ctx context.Context, num int, ts timestamp.Timestamp,
	expr *plan.Expr, ranges []byte, tblDef *plan.TableDef, proc any) ([]engine.Reader, error) {
	blkSlice := objectio.BlockInfoSlice(ranges)
	rds := make([]engine.Reader, num)
	blkInfos := make([]*objectio.BlockInfo, 0, blkSlice.Len())
	for i := 0; i < blkSlice.Len(); i++ {
		blkInfos = append(blkInfos, blkSlice.Get(i))
	}
	if len(blkInfos) < num || len(blkInfos) == 1 {
		for i, blk := range blkInfos {
			//FIXME::why set blk.EntryState = false ?
			blk.EntryState = false
			rds[i] = newBlockReader(
				ctx, tblDef, ts, []*objectio.BlockInfo{blk}, expr, e.fs, proc.(*process.Process),
			)
		}
		for j := len(blkInfos); j < num; j++ {
			rds[j] = &emptyReader{}
		}
		return rds, nil
	}

	infos, steps := groupBlocksToObjects(blkInfos, num)
	fs, err := fileservice.Get[fileservice.FileService](e.fs, defines.SharedFileServiceName)
	if err != nil {
		return nil, err
	}
	blockReaders := newBlockReaders(ctx, fs, tblDef, ts, num, expr, proc.(*process.Process))
	distributeBlocksToBlockReaders(blockReaders, num, len(blkInfos), infos, steps)
	for i := 0; i < num; i++ {
		rds[i] = blockReaders[i]
	}
	return rds, nil
}

func (e *Engine) getTNServices() []DNStore {
	cluster := clusterservice.GetMOCluster()
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

func (e *Engine) abortAllRunningTxn() {
	e.Lock()
	defer e.Unlock()
	e.cli.AbortAllRunningTxn()
}

func (e *Engine) cleanMemoryTableWithTable(dbId, tblId uint64) {
	e.Lock()
	defer e.Unlock()
	// XXX it's probably not a good way to do that.
	// after we set it to empty, actually this part of memory was not immediately released.
	// maybe a very old transaction still using that.
	delete(e.partitions, [2]uint64{dbId, tblId})
	logutil.Debugf("clean memory table of tbl[dbId: %d, tblId: %d]", dbId, tblId)
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
