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

	txn2 "github.com/matrixorigin/matrixone/pkg/pb/txn"

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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ engine.Engine = new(Engine)

func New(
	ctx context.Context,
	mp *mpool.MPool,
	fs fileservice.FileService,
	cli client.TxnClient,
	idGen IDGenerator,
) *Engine {
	var services []metadata.DNService
	cluster := clusterservice.GetMOCluster()
	cluster.GetDNService(clusterservice.NewSelector(),
		func(d metadata.DNService) bool {
			services = append(services, d)
			return true
		})

	var dnID string
	if len(services) > 0 {
		dnID = services[0].ServiceID
	}

	ls, ok := moruntime.ProcessLevelRuntime().GetGlobalVariables(moruntime.LockService)
	if !ok {
		logutil.Fatalf("missing lock service")
	}

	e := &Engine{
		mp:         mp,
		fs:         fs,
		ls:         ls.(lockservice.LockService),
		cli:        cli,
		idGen:      idGen,
		catalog:    cache.NewCatalog(),
		dnID:       dnID,
		partitions: make(map[[2]uint64]*logtailreplay.Partition),
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

	if err := e.init(ctx, mp); err != nil {
		panic(err)
	}

	return e
}

func (e *Engine) Create(ctx context.Context, name string, op client.TxnOperator) error {
	txn := e.getTransaction(op)
	if txn == nil {
		return moerr.NewTxnClosedNoCtx(op.Txn().ID)
	}
	typ := getTyp(ctx)
	sql := getSql(ctx)
	accountId, userId, roleId := getAccessInfo(ctx)
	databaseId, err := txn.allocateID(ctx)
	if err != nil {
		return err
	}
	bat, err := genCreateDatabaseTuple(sql, accountId, userId, roleId,
		name, databaseId, typ, e.mp)
	if err != nil {
		return err
	}
	// non-io operations do not need to pass context
	if err := txn.WriteBatch(INSERT, catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID,
		catalog.MO_CATALOG, catalog.MO_DATABASE, bat, txn.dnStores[0], -1, false, false); err != nil {
		return err
	}
	txn.databaseMap.Store(genDatabaseKey(ctx, name), &txnDatabase{
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
	if txn == nil || txn.meta.GetStatus() == txn2.TxnStatus_Aborted {
		return nil, moerr.NewTxnClosedNoCtx(op.Txn().ID)
	}
	if v, ok := txn.databaseMap.Load(genDatabaseKey(ctx, name)); ok {
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
		AccountId: defines.GetAccountId(ctx),
		Ts:        txn.meta.SnapshotTS,
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
	accountId := defines.GetAccountId(ctx)
	txn.databaseMap.Range(func(k, _ any) bool {
		key := k.(databaseKey)
		if key.accountId == accountId {
			dbs = append(dbs, key.name)
		}
		return true
	})
	dbs = append(dbs, e.catalog.Databases(defines.GetAccountId(ctx), txn.meta.SnapshotTS)...)
	return dbs, nil
}

func (e *Engine) GetNameById(ctx context.Context, op client.TxnOperator, tableId uint64) (dbName string, tblName string, err error) {
	txn := e.getTransaction(op)
	if txn == nil {
		return "", "", moerr.NewTxnClosed(ctx, op.Txn().ID)
	}
	accountId := defines.GetAccountId(ctx)
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
			tblName = distDb.getTableNameById(ctx, key.id)
			if tblName != "" {
				return false
			}
		}
		return true
	})

	if tblName == "" {
		dbNames := e.catalog.Databases(accountId, txn.meta.SnapshotTS)
		for _, databaseName := range dbNames {
			db, err = e.Database(noRepCtx, databaseName, op)
			if err != nil {
				return "", "", err
			}
			distDb := db.(*txnDatabase)
			tableName, rel := distDb.getRelationById(noRepCtx, tableId)
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
	accountId := defines.GetAccountId(ctx)
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
			tableName, rel = distDb.getRelationById(noRepCtx, tableId)
			if rel != nil {
				return false
			}
		}
		return true
	})

	if rel == nil {
		dbNames := e.catalog.Databases(accountId, txn.meta.SnapshotTS)
		for _, dbName = range dbNames {
			db, err = e.Database(noRepCtx, dbName, op)
			if err != nil {
				return "", "", nil, err
			}
			distDb := db.(*txnDatabase)
			tableName, rel = distDb.getRelationById(noRepCtx, tableId)
			if rel != nil {
				break
			}
		}
	}

	if rel == nil {
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
	key := genDatabaseKey(ctx, name)
	if _, ok := txn.databaseMap.Load(key); ok {
		txn.databaseMap.Delete(key)
		return nil
	} else {
		key := &cache.DatabaseItem{
			Name:      name,
			AccountId: defines.GetAccountId(ctx),
			Ts:        txn.meta.SnapshotTS,
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
	bat, err := genDropDatabaseTuple(db.databaseId, name, e.mp)
	if err != nil {
		return err
	}
	// non-io operations do not need to pass context
	if err := txn.WriteBatch(DELETE, catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID,
		catalog.MO_CATALOG, catalog.MO_DATABASE, bat, txn.dnStores[0], -1, false, false); err != nil {
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
		nil,
	)

	id := objectio.NewSegmentid()
	bytes := types.EncodeUuid(id)
	txn := &Transaction{
		op:       op,
		proc:     proc,
		engine:   e,
		meta:     op.TxnRef(),
		idGen:    e.idGen,
		dnStores: e.getDNServices(),
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
		cnBlkId_Pos:                     map[types.Blockid]Pos{},
		blockId_raw_batch:               make(map[types.Blockid]*batch.Batch),
		blockId_dn_delete_metaLoc_batch: make(map[types.Blockid][]*batch.Batch),
		batchSelectList:                 make(map[*batch.Batch][]int64),
	}
	if txn.meta.IsRCIsolation() {
		txn.tableCache.cachedIndex = e.catalog.GetDeletedTableIndex()
	}
	txn.readOnly.Store(true)
	// transaction's local segment for raw batch.
	colexec.Srv.PutCnSegment(id, colexec.TxnWorkSpaceIdType)
	e.newTransaction(op, txn)

	e.pClient.validLogTailMustApplied(txn.meta.SnapshotTS)
	return nil
}

func (e *Engine) Nodes(
	isInternal bool, tenant string, username string, cnLabel map[string]string,
) (engine.Nodes, error) {
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

	selector = clusterservice.NewSelector().SelectByLabel(cnLabel, clusterservice.EQ)
	if isInternal || strings.ToLower(tenant) == "sys" {
		SelectForSuperTenant(selector, username, nil, func(s *metadata.CNService) {
			nodes = append(nodes, engine.Node{
				Mcpu: runtime.NumCPU(),
				Id:   s.ServiceID,
				Addr: s.PipelineServiceAddress,
			})
		})
	} else {
		SelectForCommonTenant(selector, nil, func(s *metadata.CNService) {
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

func (e *Engine) getDNServices() []DNStore {
	var values []DNStore
	cluster := clusterservice.GetMOCluster()
	cluster.GetDNService(clusterservice.NewSelector(),
		func(d metadata.DNService) bool {
			values = append(values, d)
			return true
		})
	return values
}

func (e *Engine) setPushClientStatus(ready bool) {
	e.Lock()
	defer e.Unlock()

	if tc, ok := e.cli.(client.TxnClientWithFeature); ok {
		if ready {
			tc.Resume()
		} else {
			tc.Pause()
		}
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
