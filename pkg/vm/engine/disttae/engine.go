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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

type GetClusterDetailsFunc = func() (logservice.ClusterDetails, error)

func New(
	m *mheap.Mheap,
	ctx context.Context,
	cli client.TxnClient,
	getClusterDetails GetClusterDetailsFunc,
) *Engine {
	cluster, err := getClusterDetails()
	if err != nil {
		return nil
	}
	return &Engine{
		m:                 m,
		cli:               cli,
		getClusterDetails: getClusterDetails,
		db:                newDB(cli, cluster.DNStores),
		txns:              make(map[string]*Transaction),
	}
}

var _ engine.Engine = new(Engine)

func (e *Engine) Create(ctx context.Context, name string, op client.TxnOperator) error {
	txn := e.getTransaction(op)
	if txn == nil {
		return moerr.NewTxnClosed()
	}
	accountId, userId, roleId := getAccessInfo(ctx)
	bat, err := genCreateDatabaseTuple(accountId, userId, roleId, name, e.m)
	if err != nil {
		return err
	}
	defer bat.Clean(e.m)
	// non-io operations do not need to pass context
	if err := txn.WriteBatch(INSERT, catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID,
		catalog.MO_CATALOG, catalog.MO_DATABASE, bat); err != nil {
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
	id, err := txn.getDatabaseId(ctx, name)
	if err != nil {
		return nil, err
	}
	return &database{
		m:            e.m,
		txn:          txn,
		db:           e.db,
		databaseId:   id,
		databaseName: name,
	}, nil
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
	id, err := txn.getDatabaseId(ctx, name)
	if err != nil {
		return err
	}
	bat, err := genDropDatabaseTuple(id, e.m)
	if err != nil {
		return err
	}
	defer bat.Clean(e.m)
	// non-io operations do not need to pass context
	if err := txn.WriteBatch(DELETE, catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID,
		catalog.MO_CATALOG, catalog.MO_DATABASE, bat); err != nil {
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
	txn := &Transaction{
		db:       e.db,
		readOnly: false,
		meta:     op.Txn(),
		dnStores: cluster.DNStores,
		fileMap:  make(map[string]uint64),
	}
	txn.writes = append(txn.writes, make([]Entry, 0, 1))
	e.newTransaction(op, txn)
	if len(txn.dnStores) > 0 {
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
	}
	return nil
}

func (e *Engine) Commit(ctx context.Context, op client.TxnOperator) error {
	txn := e.getTransaction(op)
	if txn == nil {
		return moerr.NewTxnClosed()
	}
	defer e.delTransaction(txn)
	if e.hasConflict(txn) {
		return moerr.NewTxnWriteConflict("")
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
		return moerr.NewTxnClosed()
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
			Mcpu: 1,
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
			txn.writes[i][j].bat.Clean(e.m)
		}
	}
	e.Lock()
	defer e.Unlock()
	delete(e.txns, string(txn.meta.ID))
}
