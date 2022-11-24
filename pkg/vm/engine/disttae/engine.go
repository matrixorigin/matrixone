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
	"container/heap"
	"context"
	"math"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memtable"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func New(
	ctx context.Context,
	mp *mpool.MPool,
	fs fileservice.FileService,
	cli client.TxnClient,
	idGen IDGenerator,
	getClusterDetails engine.GetClusterDetailsFunc,
) *Engine {
	cluster, err := getClusterDetails()
	if err != nil {
		panic(err)
	}
	db := newDB(cluster.DNStores)
	if err := db.init(ctx, mp); err != nil {
		panic(err)
	}
	e := &Engine{
		db:                db,
		mp:                mp,
		fs:                fs,
		cli:               cli,
		idGen:             idGen,
		txnHeap:           &transactionHeap{},
		getClusterDetails: getClusterDetails,
		txns:              make(map[string]*Transaction),
	}
	go e.gc(ctx)
	return e
}

var _ engine.Engine = new(Engine)

func (e *Engine) Create(ctx context.Context, name string, op client.TxnOperator) error {
	txn := e.getTransaction(op)
	if txn == nil {
		return moerr.NewTxnClosed(op.Txn().ID)
	}
	sql := getSql(ctx)
	accountId, userId, roleId := getAccessInfo(ctx)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute) // TODO
	defer cancel()
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
	return nil
}

func (e *Engine) Database(ctx context.Context, name string,
	op client.TxnOperator) (engine.Database, error) {
	txn := e.getTransaction(op)
	if txn == nil {
		return nil, moerr.NewTxnClosed(op.Txn().ID)
	}
	key := genDatabaseKey(ctx, name)
	if db, ok := txn.databaseMap.Load(key); ok {
		return db.(*database), nil
	}
	if name == catalog.MO_CATALOG {
		db := &database{
			txn:          txn,
			db:           e.db,
			fs:           e.fs,
			databaseId:   catalog.MO_CATALOG_ID,
			databaseName: name,
		}
		txn.databaseMap.Store(key, db)
		return db, nil
	}
	id, err := txn.getDatabaseId(ctx, name)
	if err != nil {
		return nil, err
	}
	db := &database{
		txn:          txn,
		db:           e.db,
		fs:           e.fs,
		databaseId:   id,
		databaseName: name,
	}
	txn.databaseMap.Store(key, db)
	return db, nil
}

func (e *Engine) Databases(ctx context.Context, op client.TxnOperator) ([]string, error) {
	txn := e.getTransaction(op)
	if txn == nil {
		return nil, moerr.NewTxnClosed(op.Txn().ID)
	}
	return txn.getDatabaseList(ctx)
}

func (e *Engine) Delete(ctx context.Context, name string, op client.TxnOperator) error {
	var db *database

	txn := e.getTransaction(op)
	if txn == nil {
		return moerr.NewTxnClosed(op.Txn().ID)
	}
	key := genDatabaseKey(ctx, name)
	if v, ok := txn.databaseMap.Load(key); ok {
		db = v.(*database)
		txn.databaseMap.Delete(key)
	} else {
		id, err := txn.getDatabaseId(ctx, name)
		if err != nil {
			return err
		}
		db = &database{
			txn:          txn,
			db:           e.db,
			fs:           e.fs,
			databaseId:   id,
			databaseName: name,
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
			tbl := v.(*table)
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
	cluster, err := e.getClusterDetails()
	if err != nil {
		return err
	}
	proc := process.New(
		ctx,
		e.mp,
		e.cli,
		op,
		e.fs,
		e.getClusterDetails,
	)
	txn := &Transaction{
		op:             op,
		proc:           proc,
		db:             e.db,
		readOnly:       true,
		meta:           op.Txn(),
		idGen:          e.idGen,
		rowId:          [2]uint64{math.MaxUint64, 0},
		workspace:      memtable.NewTable[RowID, *workspaceRow, *workspaceRow](),
		dnStores:       cluster.DNStores,
		fileMap:        make(map[string]uint64),
		tableMap:       new(sync.Map),
		databaseMap:    new(sync.Map),
		createTableMap: make(map[uint64]uint8),
	}
	txn.writes = append(txn.writes, make([]Entry, 0, 1))
	e.newTransaction(op, txn)
	// update catalog's cache
	table := &table{
		db: &database{
			fs:         e.fs,
			databaseId: catalog.MO_CATALOG_ID,
		},
	}
	table.tableId = catalog.MO_DATABASE_ID
	table.tableName = catalog.MO_DATABASE
	if err := e.db.Update(ctx, txn.dnStores[:1], table, op, catalog.MO_TABLES_REL_ID_IDX,
		catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID, txn.meta.SnapshotTS); err != nil {
		e.delTransaction(txn)
		return err
	}
	table.tableId = catalog.MO_TABLES_ID
	table.tableName = catalog.MO_TABLES
	if err := e.db.Update(ctx, txn.dnStores[:1], table, op, catalog.MO_TABLES_REL_ID_IDX,
		catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID, txn.meta.SnapshotTS); err != nil {
		e.delTransaction(txn)
		return err
	}
	table.tableId = catalog.MO_COLUMNS_ID
	table.tableName = catalog.MO_COLUMNS
	if err := e.db.Update(ctx, txn.dnStores[:1], table, op, catalog.MO_TABLES_REL_ID_IDX,
		catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID, txn.meta.SnapshotTS); err != nil {
		e.delTransaction(txn)
		return err
	}
	return nil
}

func (e *Engine) Commit(ctx context.Context, op client.TxnOperator) error {
	txn := e.getTransaction(op)
	if txn == nil {
		return moerr.NewTxnClosed(op.Txn().ID)
	}
	defer e.delTransaction(txn)
	if txn.readOnly {
		return nil
	}
	if e.hasConflict(txn) {
		return moerr.NewTxnWriteConflict("write conflict")
	}
	if e.hasDuplicate(ctx, txn) {
		return moerr.NewDuplicate()
	}
	reqs, err := genWriteReqs(txn.writes)
	if err != nil {
		return err
	}
	_, err = op.Write(ctx, reqs)
	if err == nil {
		for _, name := range txn.deleteMetaTables {
			txn.db.delMetaTable(name)
		}
	}
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
	clusterDetails, err := e.getClusterDetails()
	if err != nil {
		return nil, err
	}

	var nodes engine.Nodes
	for _, store := range clusterDetails.CNStores {
		nodes = append(nodes, engine.Node{
			Mcpu: 10, // TODO
			Id:   store.UUID,
			Addr: store.ServiceAddress,
		})
	}
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
	heap.Push(e.txnHeap, txn)
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
	e.Lock()
	defer e.Unlock()
	for i, tmp := range *e.txnHeap {
		if bytes.Equal(txn.meta.ID, tmp.meta.ID) {
			heap.Remove(e.txnHeap, i)
			break
		}
	}
	delete(e.txns, string(txn.meta.ID))
}

func (e *Engine) gc(ctx context.Context) {
	var ps []Partitions
	var ts timestamp.Timestamp

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(GcCycle):
			e.RLock()
			if len(*e.txnHeap) == 0 {
				e.RUnlock()
				continue
			}
			ts = (*e.txnHeap)[0].meta.SnapshotTS
			e.RUnlock()
			e.Lock()
			for k := range e.db.tables {
				ps = append(ps, e.db.tables[k])
			}
			e.Unlock()
			for i := range ps {
				for j := range ps[i] {
					ps[i][j].Lock()
					ps[i][j].GC(ts)
					ps[i][j].Unlock()
				}
			}
		}
	}
}
