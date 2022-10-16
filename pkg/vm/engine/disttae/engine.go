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
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type GetClusterDetailsFunc = func() (logservice.ClusterDetails, error)

func New(
	ctx context.Context,
	mp *mpool.MPool,
	fs fileservice.FileService,
	cli client.TxnClient,
	idGen IDGenerator,
	getClusterDetails GetClusterDetailsFunc,
) *Engine {
	cluster, err := getClusterDetails()
	if err != nil {
		panic(err)
	}
	db := newDB(cli, cluster.DNStores)
	if err := db.init(ctx, mp); err != nil {
		panic(err)
	}
	return &Engine{
		db:                db,
		mp:                mp,
		fs:                fs,
		cli:               cli,
		idGen:             idGen,
		txnHeap:           &transactionHeap{},
		getClusterDetails: getClusterDetails,
		txns:              make(map[string]*Transaction),
	}
}

var _ engine.Engine = new(Engine)

func (e *Engine) Create(ctx context.Context, name string, op client.TxnOperator) error {
	txn := e.getTransaction(op)
	if txn == nil {
		return moerr.NewTxnClosed()
	}
	sql := getSql(ctx)
	accountId, userId, roleId := getAccessInfo(ctx)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute) // TODO
	defer cancel()
	databaseId, err := txn.idGen.AllocateID(ctx)
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
		catalog.MO_CATALOG, catalog.MO_DATABASE, bat, txn.dnStores[0]); err != nil {
		return err
	}
	return nil
}

func (e *Engine) Database(ctx context.Context, name string,
	op client.TxnOperator) (engine.Database, error) {
	txn := e.getTransaction(op)
	if txn == nil {
		return nil, moerr.NewTxnClosed()
	}
	key := genDatabaseKey(ctx, name)
	if db, ok := txn.databaseMap.Load(key); ok {
		return db.(*database), nil
	}
	if name == catalog.MO_CATALOG {
		db := &database{
			txn:          txn,
			db:           e.db,
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
		databaseId:   id,
		databaseName: name,
	}
	txn.databaseMap.Store(key, db)
	return db, nil
}

func (e *Engine) Databases(ctx context.Context, op client.TxnOperator) ([]string, error) {
	txn := e.getTransaction(op)
	if txn == nil {
		return nil, moerr.NewTxnClosed()
	}
	return txn.getDatabaseList(ctx)
}

func (e *Engine) Delete(ctx context.Context, name string, op client.TxnOperator) error {
	txn := e.getTransaction(op)
	if txn == nil {
		return moerr.NewTxnClosed()
	}
	key := genDatabaseKey(ctx, name)
	txn.databaseMap.Delete(key)
	id, err := txn.getDatabaseId(ctx, name)
	if err != nil {
		return err
	}
	bat, err := genDropDatabaseTuple(id, name, e.mp)
	if err != nil {
		return err
	}
	// non-io operations do not need to pass context
	if err := txn.WriteBatch(DELETE, catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID,
		catalog.MO_CATALOG, catalog.MO_DATABASE, bat, txn.dnStores[0]); err != nil {
		return err
	}
	return nil
}

// hasConflict used to detect if a transaction on a cn is in conflict,
// currently an empty implementation, assuming all transactions on a cn are conflict free
func (e *Engine) hasConflict(txn *Transaction) bool {
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
	)
	txn := &Transaction{
		proc:           proc,
		db:             e.db,
		readOnly:       true,
		meta:           op.Txn(),
		idGen:          e.idGen,
		rowId:          [2]uint64{math.MaxUint64, 0},
		dnStores:       cluster.DNStores,
		fileMap:        make(map[string]uint64),
		tableMap:       new(sync.Map),
		databaseMap:    new(sync.Map),
		createTableMap: make(map[uint64]uint8),
	}
	txn.writes = append(txn.writes, make([]Entry, 0, 1))
	e.newTransaction(op, txn)
	// update catalog's cache
	if err := e.db.Update(ctx, txn.dnStores[:1], catalog.MO_CATALOG_ID,
		catalog.MO_DATABASE_ID, txn.meta.SnapshotTS); err != nil {
		return err
	}
	if err := e.db.Update(ctx, txn.dnStores[:1], catalog.MO_CATALOG_ID,
		catalog.MO_TABLES_ID, txn.meta.SnapshotTS); err != nil {
		return err
	}
	if err := e.db.Update(ctx, txn.dnStores[:1], catalog.MO_CATALOG_ID,
		catalog.MO_COLUMNS_ID, txn.meta.SnapshotTS); err != nil {
		return err
	}
	return nil
}

func (e *Engine) Commit(ctx context.Context, op client.TxnOperator) error {
	txn := e.getTransaction(op)
	if txn == nil {
		return moerr.NewTxnClosed()
	}
	defer e.delTransaction(txn)
	if txn.readOnly {
		return nil
	}
	if e.hasConflict(txn) {
		return moerr.NewTxnWriteConflict("write conflict")
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
	return nil, nil
	/*
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
	*/
}

func (e *Engine) Hints() (h engine.Hints) {
	h.CommitOrRollbackTimeout = time.Minute * 5
	return
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

/*
func (e *Engine) minActiveTimestamp() timestamp.Timestamp {
	e.RLock()
	defer e.RUnlock()
	return (*e.txnHeap)[0].meta.SnapshotTS
}
*/
