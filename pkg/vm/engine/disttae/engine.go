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
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	txn2 "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/route"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ engine.Engine = new(Engine)

func New(
	ctx context.Context,
	mp *mpool.MPool,
	fs fileservice.FileService,
	cli client.TxnClient,
	hakeeper logservice.CNHAKeeperClient,
) *Engine {
	var services []metadata.TNService
	cluster := clusterservice.GetMOCluster()
	cluster.GetTNService(clusterservice.NewSelector(),
		func(d metadata.TNService) bool {
			services = append(services, d)
			return true
		})

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
			},
			func(packer *types.Packer) {
				packer.FreeMem()
			},
		),
	}

	if err := e.init(ctx); err != nil {
		panic(err)
	}

	// TODO(ghs)
	// is debug for #13151, will remove later
	e.enableDebug()

	return e
}

func (e *Engine) enableDebug() {
	function.DebugGetDatabaseExpectedEOB = func(caller string, proc *process.Process) {
		txnState := fmt.Sprintf("account-%s, accId-%d, user-%s, role-%s, timezone-%s, txn-%s",
			proc.SessionInfo.Account, proc.SessionInfo.AccountId,
			proc.SessionInfo.User, proc.SessionInfo.Role,
			proc.SessionInfo.TimeZone.String(),
			proc.TxnOperator.Txn().DebugString(),
		)
		e.DebugGetDatabaseExpectedEOB(caller, txnState)
	}
}

func (e *Engine) DebugGetDatabaseExpectedEOB(caller string, txnState string) {
	dbs, tbls := e.catalog.TraverseDbAndTbl()
	sort.Slice(dbs, func(i, j int) bool {
		if dbs[i].AccountId != dbs[j].AccountId {
			return dbs[i].AccountId < dbs[j].AccountId
		}
		return dbs[i].Id < dbs[j].Id
	})

	sort.Slice(tbls, func(i, j int) bool {
		if tbls[i].AccountId != tbls[j].AccountId {
			return tbls[i].AccountId < tbls[j].AccountId
		}

		if tbls[i].DatabaseId != tbls[j].DatabaseId {
			return tbls[i].DatabaseId < tbls[j].DatabaseId
		}

		return tbls[i].Id < tbls[j].Id
	})

	var out bytes.Buffer
	tIdx := 0
	for idx := range dbs {
		out.WriteString(fmt.Sprintf("%s\n", dbs[idx].String()))
		for ; tIdx < len(tbls); tIdx++ {
			if tbls[tIdx].AccountId != dbs[idx].AccountId ||
				tbls[tIdx].DatabaseId != dbs[idx].Id {
				break
			}

			out.WriteString(fmt.Sprintf("\t%s\n", tbls[tIdx].String()))
		}
	}

	logutil.Infof("%s.Database got ExpectEOB, current txn state: \n%s\n%s",
		caller, txnState, out.String())
}

func (e *Engine) Create(ctx context.Context, name string, op client.TxnOperator) error {
	txn := e.getTransaction(op)
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
	// non-io operations do not need to pass context
	if err = txn.WriteBatch(INSERT, catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID,
		catalog.MO_CATALOG, catalog.MO_DATABASE, bat, txn.tnStores[0], -1, false, false); err != nil {
		bat.Clean(txn.proc.Mp())
		return err
	}
	txn.databaseMap.Store(genDatabaseKey(accountId, name), &txnDatabase{
		txn:          txn,
		databaseId:   databaseId,
		databaseName: name,
	})
	return nil
}

func (e *Engine) Database(ctx context.Context, name string,
	op client.TxnOperator) (engine.Database, error) {
	logDebugf(op.Txn(), "Engine.Database %s", name)
	txn := e.getTransaction(op)
	if txn == nil || txn.op.Status() == txn2.TxnStatus_Aborted {
		return nil, moerr.NewTxnClosedNoCtx(op.Txn().ID)
	}
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}
	if v, ok := txn.databaseMap.Load(genDatabaseKey(accountId, name)); ok {
		return v.(*txnDatabase), nil
	}
	if name == catalog.MO_CATALOG {
		db := &txnDatabase{
			txn:          txn,
			databaseId:   catalog.MO_CATALOG_ID,
			databaseName: name,
		}
		return db, nil
	}
	key := &cache.DatabaseItem{
		Name:      name,
		AccountId: accountId,
		Ts:        txn.op.SnapshotTS(),
	}
	if ok := e.catalog.GetDatabase(key); !ok {
		return nil, moerr.GetOkExpectedEOB()
	}
	return &txnDatabase{
		txn:               txn,
		databaseName:      name,
		databaseId:        key.Id,
		databaseType:      key.Typ,
		databaseCreateSql: key.CreateSql,
	}, nil
}

func (e *Engine) Databases(ctx context.Context, op client.TxnOperator) ([]string, error) {
	var dbs []string

	txn := e.getTransaction(op)
	if txn == nil {
		return nil, moerr.NewTxnClosed(ctx, op.Txn().ID)
	}
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}
	txn.databaseMap.Range(func(k, _ any) bool {
		key := k.(databaseKey)
		if key.accountId == accountId {
			dbs = append(dbs, key.name)
		}
		return true
	})
	dbs = append(dbs, e.catalog.Databases(accountId, txn.op.SnapshotTS())...)
	return dbs, nil
}

func (e *Engine) GetNameById(ctx context.Context, op client.TxnOperator, tableId uint64) (dbName string, tblName string, err error) {
	txn := e.getTransaction(op)
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

	if tblName == "" {
		dbNames := e.catalog.Databases(accountId, txn.op.SnapshotTS())
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
	txn := e.getTransaction(op)
	if txn == nil {
		return "", "", nil, moerr.NewTxnClosed(ctx, op.Txn().ID)
	}
	switch tableId {
	case catalog.MO_DATABASE_ID:
		db := &txnDatabase{
			txn:          txn,
			databaseId:   catalog.MO_CATALOG_ID,
			databaseName: catalog.MO_CATALOG,
		}
		defs := catalog.MoDatabaseTableDefs
		return catalog.MO_CATALOG, catalog.MO_DATABASE,
			db.openSysTable(nil, tableKey{}, tableId, catalog.MO_DATABASE, defs), nil
	case catalog.MO_TABLES_ID:
		db := &txnDatabase{
			txn:          txn,
			databaseId:   catalog.MO_CATALOG_ID,
			databaseName: catalog.MO_CATALOG,
		}
		defs := catalog.MoTablesTableDefs
		return catalog.MO_CATALOG, catalog.MO_TABLES,
			db.openSysTable(nil, tableKey{}, tableId, catalog.MO_TABLES, defs), nil
	case catalog.MO_COLUMNS_ID:
		db := &txnDatabase{
			txn:          txn,
			databaseId:   catalog.MO_CATALOG_ID,
			databaseName: catalog.MO_CATALOG,
		}
		defs := catalog.MoColumnsTableDefs
		return catalog.MO_CATALOG, catalog.MO_COLUMNS,
			db.openSysTable(nil, tableKey{}, tableId, catalog.MO_COLUMNS, defs), nil
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
		if key.accountId == accountId {
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

	if rel == nil {
		dbNames := e.catalog.Databases(accountId, txn.op.SnapshotTS())
		for _, dbName = range dbNames {
			db, err = e.Database(noRepCtx, dbName, op)
			if err != nil {
				return "", "", nil, err
			}
			distDb := db.(*txnDatabase)
			tableName, rel, err = distDb.getRelationById(noRepCtx, tableId)
			if err != nil {
				return "", "", nil, err
			}
			if rel != nil {
				break
			}
		}
	}

	if rel == nil {
		if tableId == 2 {
			logutil.Errorf("can not find table by id %d: accountId: %v, txnId: %v",
				tableId, accountId, op.Txn().DebugString())
			tbls, tblIds := e.catalog.Tables(accountId, 1, op.SnapshotTS())
			logutil.Errorf("tables: %v, tableIds: %v", tbls, tblIds)
		}
		return "", "", nil, moerr.NewInternalError(ctx, "can not find table by id %d", tableId)
	}
	return
}

func (e *Engine) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	return e.idGen.AllocateIDByKey(ctx, key)
}

func (e *Engine) Delete(ctx context.Context, name string, op client.TxnOperator) error {
	var db *txnDatabase

	txn := e.getTransaction(op)
	if txn == nil {
		return moerr.NewTxnClosedNoCtx(op.Txn().ID)
	}

	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	key := genDatabaseKey(accountId, name)
	if _, ok := txn.databaseMap.Load(key); ok {
		txn.databaseMap.Delete(key)
		return nil
	} else {
		key := &cache.DatabaseItem{
			Name:      name,
			AccountId: accountId,
			Ts:        txn.op.SnapshotTS(),
		}
		if ok := e.catalog.GetDatabase(key); !ok {
			return moerr.GetOkExpectedEOB()
		}
		db = &txnDatabase{
			txn:          txn,
			databaseName: name,
			databaseId:   key.Id,
		}
	}
	rels, err := db.Relations(ctx)
	if err != nil {
		return err
	}
	for _, relName := range rels {
		if err := db.Delete(ctx, relName); err != nil {
			return err
		}
	}
	bat, err := genDropDatabaseTuple(db.databaseId, name, txn.proc.Mp())
	if err != nil {
		return err
	}
	// non-io operations do not need to pass context
	if err := txn.WriteBatch(DELETE, catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID,
		catalog.MO_CATALOG, catalog.MO_DATABASE, bat, txn.tnStores[0], -1, false, false); err != nil {
		bat.Clean(txn.proc.Mp())
		return err
	}
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
		e.qs,
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
		databaseMap:     new(sync.Map),
		createMap:       new(sync.Map),
		deletedTableMap: new(sync.Map),
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
		blockId_raw_batch:    make(map[types.Blockid]*batch.Batch),
		batchSelectList:      make(map[*batch.Batch][]int64),
		toFreeBatches:        make(map[[2]string][]*batch.Batch),
		syncCommittedTSCount: e.cli.GetSyncLatestCommitTSTimes(),
	}

	txn.blockId_tn_delete_metaLoc_batch = struct {
		sync.RWMutex
		data map[types.Blockid][]*batch.Batch
	}{data: make(map[types.Blockid][]*batch.Batch)}

	txn.readOnly.Store(true)
	// transaction's local segment for raw batch.
	colexec.Srv.PutCnSegment(id, colexec.TxnWorkSpaceIdType)
	e.newTransaction(op, txn)

	e.pClient.validLogTailMustApplied(txn.op.SnapshotTS())
	return nil
}

func (e *Engine) Nodes(
	isInternal bool, tenant string, username string, cnLabel map[string]string,
) (engine.Nodes, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementNodesHistogram.Observe(time.Since(start).Seconds())
	}()
	var nodes engine.Nodes
	cluster := clusterservice.GetMOCluster()
	var selector clusterservice.Selector

	// If the requested labels are empty, return all CN servers.
	if len(cnLabel) == 0 {
		cluster.GetCNService(selector, func(c metadata.CNService) bool {
			nodes = append(nodes, engine.Node{
				Mcpu: runtime.NumCPU(),
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
				Mcpu: runtime.NumCPU(),
				Id:   s.ServiceID,
				Addr: s.PipelineServiceAddress,
			})
		})
	} else {
		route.RouteForCommonTenant(selector, nil, func(s *metadata.CNService) {
			nodes = append(nodes, engine.Node{
				Mcpu: runtime.NumCPU(),
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
	expr *plan.Expr, ranges [][]byte, tblDef *plan.TableDef, proc any) ([]engine.Reader, error) {
	rds := make([]engine.Reader, num)
	blkInfos := make([]*catalog.BlockInfo, 0, len(ranges))
	for _, r := range ranges {
		blkInfos = append(blkInfos, catalog.DecodeBlockInfo(r))
	}
	if len(blkInfos) < num || len(blkInfos) == 1 {
		for i, blk := range blkInfos {
			//FIXME::why set blk.EntryState = false ?
			blk.EntryState = false
			rds[i] = newBlockReader(
				ctx, tblDef, ts, []*catalog.BlockInfo{blk}, expr, e.fs, proc.(*process.Process),
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
	blockReaders := newBlockReaders(ctx, fs, tblDef, -1, ts, num, expr, proc.(*process.Process))
	distributeBlocksToBlockReaders(blockReaders, num, infos, steps)
	for i := 0; i < num; i++ {
		rds[i] = blockReaders[i]
	}
	return rds, nil
}

func (e *Engine) newTransaction(op client.TxnOperator, txn *Transaction) {
	op.AddWorkspace(txn)
}

func (e *Engine) getTransaction(op client.TxnOperator) *Transaction {
	return op.GetWorkspace().(*Transaction)
}

func (e *Engine) getTNServices() []DNStore {
	var values []DNStore
	cluster := clusterservice.GetMOCluster()
	cluster.GetTNService(clusterservice.NewSelector(),
		func(d metadata.TNService) bool {
			values = append(values, d)
			return true
		})
	return values
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
