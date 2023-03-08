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
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memorytable"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
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

	dnMap := make(map[string]int)
	for i := range services {
		dnMap[services[i].ServiceID] = i
	}

	e := &Engine{
		mp:         mp,
		fs:         fs,
		cli:        cli,
		idGen:      idGen,
		catalog:    cache.NewCatalog(),
		txns:       make(map[string]*Transaction),
		dnMap:      dnMap,
		partitions: make(map[[2]uint64]Partitions),
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
	sql := getSql(ctx)
	accountId, userId, roleId := getAccessInfo(ctx)
	databaseId, err := txn.allocateID(ctx)
	if err != nil {
		return err
	}
	bat, err := genCreateDatabaseTuple(sql, accountId, userId, roleId,
		name, databaseId, e.mp)
	if err != nil {
		return err
	}
	// non-io operations do not need to pass context
	if err := txn.WriteBatch(INSERT, catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID,
		catalog.MO_CATALOG, catalog.MO_DATABASE, bat, txn.dnStores[0], -1); err != nil {
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
	txn := e.getTransaction(op)
	if txn == nil {
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
		AccountId: getAccountId(ctx),
		Ts:        txn.meta.SnapshotTS,
	}
	if ok := e.catalog.GetDatabase(key); !ok {
		return nil, moerr.GetOkExpectedEOB()
	}
	return &txnDatabase{
		txn:          txn,
		databaseName: name,
		databaseId:   key.Id,
	}, nil
}

func (e *Engine) Databases(ctx context.Context, op client.TxnOperator) ([]string, error) {
	var dbs []string

	txn := e.getTransaction(op)
	if txn == nil {
		return nil, moerr.NewTxnClosed(ctx, op.Txn().ID)
	}
	accountId := getAccountId(ctx)
	txn.databaseMap.Range(func(k, _ any) bool {
		key := k.(databaseKey)
		if key.accountId == accountId {
			dbs = append(dbs, key.name)
		}
		return true
	})
	dbs = append(dbs, e.catalog.Databases(getAccountId(ctx), txn.meta.SnapshotTS)...)
	return dbs, nil
}

func (e *Engine) GetNameById(ctx context.Context, op client.TxnOperator, tableId uint64) (dbName string, tblName string, err error) {
	txn := e.getTransaction(op)
	if txn == nil {
		return "", "", moerr.NewTxnClosed(ctx, op.Txn().ID)
	}
	accountId := getAccountId(ctx)
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
		for _, dbName := range dbNames {
			db, err = e.Database(noRepCtx, dbName, op)
			if err != nil {
				return "", "", err
			}
			distDb := db.(*txnDatabase)
			tableName, rel, _ := distDb.getRelationById(noRepCtx, tableId)
			if rel != nil {
				tblName = tableName
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
	accountId := getAccountId(ctx)
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
			if rel != nil {
				return false
			}
		}
		return true
	})

	if rel == nil {
		dbNames := e.catalog.Databases(accountId, txn.meta.SnapshotTS)
		for _, dbName := range dbNames {
			db, err = e.Database(noRepCtx, dbName, op)
			if err != nil {
				return "", "", nil, err
			}
			distDb := db.(*txnDatabase)
			tableName, rel, err = distDb.getRelationById(noRepCtx, tableId)
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
			AccountId: getAccountId(ctx),
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
		catalog.MO_CATALOG, catalog.MO_DATABASE, bat, txn.dnStores[0], -1); err != nil {
		return err
	}
	return nil
}

// hasConflict used to detect if a transaction on a cn is in conflict,
// currently an empty implementation, assuming all transactions on a cn are conflict free
func (e *Engine) hasConflict(txn *Transaction) bool {
	return false
}

// hasDuplicate used to detect if a transaction on a cn has duplicate.
func (e *Engine) hasDuplicate(ctx context.Context, txn *Transaction) bool {
	for i := range txn.writes {
		for _, e := range txn.writes[i] {
			if e.typ == DELETE {
				continue
			}
			if e.bat.Length() == 0 {
				continue
			}
			key := genTableKey(ctx, e.tableName, e.databaseId)
			v, ok := txn.tableMap.Load(key)
			if !ok {
				continue
			}
			tbl := v.(*txnTable)
			if tbl.meta == nil {
				continue
			}
			if tbl.primaryIdx == -1 {
				continue
			}
		}
	}
	return false
}

func (e *Engine) New(ctx context.Context, op client.TxnOperator) error {
	proc := process.New(
		ctx,
		e.mp,
		e.cli,
		op,
		e.fs,
	)
	workspace := memorytable.NewTable[RowID, *workspaceRow, *workspaceRow]()
	workspace.DisableHistory()
	txn := &Transaction{
		op:          op,
		proc:        proc,
		engine:      e,
		readOnly:    true,
		meta:        op.Txn(),
		idGen:       e.idGen,
		rowId:       [2]uint64{math.MaxUint64, 0},
		workspace:   workspace,
		dnStores:    e.getDNServices(),
		fileMap:     make(map[string]uint64),
		tableMap:    new(sync.Map),
		databaseMap: new(sync.Map),
		createMap:   new(sync.Map),
	}
	txn.writes = append(txn.writes, make([]Entry, 0, 1))
	e.newTransaction(op, txn)

	if e.UsePushModelOrNot() {
		if err := e.receiveLogTailTime.blockUntilTxnTimeIsLegal(ctx, txn.meta.SnapshotTS); err != nil {
			e.delTransaction(txn)
			return err
		}
	} else {
		// update catalog's cache
		table := &txnTable{
			db: &txnDatabase{
				txn: &Transaction{
					engine: e,
				},
				databaseId: catalog.MO_CATALOG_ID,
			},
		}
		table.tableId = catalog.MO_DATABASE_ID
		table.tableName = catalog.MO_DATABASE
		if err := e.UpdateOfPull(ctx, txn.dnStores[:1], table, op, catalog.MO_TABLES_REL_ID_IDX,
			catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID, txn.meta.SnapshotTS); err != nil {
			e.delTransaction(txn)
			return err
		}

		table.tableId = catalog.MO_TABLES_ID
		table.tableName = catalog.MO_TABLES
		if err := e.UpdateOfPull(ctx, txn.dnStores[:1], table, op, catalog.MO_TABLES_REL_ID_IDX,
			catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID, txn.meta.SnapshotTS); err != nil {
			e.delTransaction(txn)
			return err
		}

		table.tableId = catalog.MO_COLUMNS_ID
		table.tableName = catalog.MO_COLUMNS
		if err := e.UpdateOfPull(ctx, txn.dnStores[:1], table, op, catalog.MO_TABLES_REL_ID_IDX,
			catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID, txn.meta.SnapshotTS); err != nil {
			e.delTransaction(txn)
			return err
		}
	}

	return nil
}

func (e *Engine) Commit(ctx context.Context, op client.TxnOperator) error {
	txn := e.getTransaction(op)
	if txn == nil {
		return moerr.NewTxnClosedNoCtx(op.Txn().ID)
	}
	defer e.delTransaction(txn)
	if txn.readOnly {
		return nil
	}
	if e.hasConflict(txn) {
		return moerr.NewTxnWriteConflictNoCtx("write conflict")
	}
	if e.hasDuplicate(ctx, txn) {
		return moerr.NewDuplicateNoCtx()
	}
	reqs, err := genWriteReqs(txn.writes)
	if err != nil {
		return err
	}
	_, err = op.Write(ctx, reqs)
	return err
}

func (e *Engine) Rollback(ctx context.Context, op client.TxnOperator) error {
	txn := e.getTransaction(op)
	if txn == nil {
		return nil // compatible with existing logic
		//	return moerr.NewTxnClosed()
	}
	defer e.delTransaction(txn)
	return nil
}

func (e *Engine) Nodes() (engine.Nodes, error) {
	var nodes engine.Nodes
	cluster := clusterservice.GetMOCluster()
	cluster.GetCNService(clusterservice.NewSelector(), func(c metadata.CNService) bool {
		nodes = append(nodes, engine.Node{
			Mcpu: runtime.NumCPU(),
			Id:   c.ServiceID,
			Addr: c.PipelineServiceAddress,
		})
		return true
	})
	return nodes, nil
}

func (e *Engine) Hints() (h engine.Hints) {
	h.CommitOrRollbackTimeout = time.Minute * 5
	return
}

func (e *Engine) NewBlockReader(ctx context.Context, num int, ts timestamp.Timestamp,
	expr *plan.Expr, ranges [][]byte, tblDef *plan.TableDef) ([]engine.Reader, error) {
	rds := make([]engine.Reader, num)
	blks := make([]BlockMeta, len(ranges))
	for i := range ranges {
		blks[i] = blockUnmarshal(ranges[i])
		blks[i].Info.EntryState = false
	}
	if len(ranges) < num {
		for i := range ranges {
			rds[i] = &blockReader{
				fs:         e.fs,
				tableDef:   tblDef,
				primaryIdx: -1,
				expr:       expr,
				ts:         ts,
				ctx:        ctx,
				blks:       []BlockMeta{blks[i]},
			}
		}
		for j := len(ranges); j < num; j++ {
			rds[j] = &emptyReader{}
		}
		return rds, nil
	}
	step := len(ranges) / num
	if step < 1 {
		step = 1
	}
	for i := 0; i < num; i++ {
		if i == num-1 {
			rds[i] = &blockReader{
				fs:         e.fs,
				tableDef:   tblDef,
				primaryIdx: -1,
				expr:       expr,
				ts:         ts,
				ctx:        ctx,
				blks:       blks[i*step:],
			}
		} else {
			rds[i] = &blockReader{
				fs:         e.fs,
				tableDef:   tblDef,
				primaryIdx: -1,
				expr:       expr,
				ts:         ts,
				ctx:        ctx,
				blks:       blks[i*step : (i+1)*step],
			}
		}
	}
	return rds, nil
}

func (e *Engine) newTransaction(op client.TxnOperator, txn *Transaction) {
	e.Lock()
	defer e.Unlock()
	e.txns[string(op.Txn().ID)] = txn
}

func (e *Engine) getTransaction(op client.TxnOperator) *Transaction {
	e.RLock()
	defer e.RUnlock()
	return e.txns[string(op.Txn().ID)]
}

func (e *Engine) delTransaction(txn *Transaction) {
	for i := range txn.writes {
		for j := range txn.writes[i] {
			txn.writes[i][j].bat.Clean(e.mp)
		}
	}
	txn.tableMap = nil
	txn.createMap = nil
	txn.databaseMap = nil
	e.Lock()
	defer e.Unlock()
	delete(e.txns, string(txn.meta.ID))
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
