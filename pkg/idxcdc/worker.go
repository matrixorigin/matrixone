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

package idxcdc

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)

// func ExecuteSql(
// 	ctx context.Context,
// 	txn client.TxnOperator,
// 	cnServiceID string,
// 	sql string,
// 	) (executor.Result, error){
// 	v, ok := moruntime.ServiceRuntime(cnServiceID).GetGlobalVariables(moruntime.InternalSQLExecutor)
// 	if !ok {
// 		panic("missing lock service")
// 	}

// 	lower := c.getLower()

// 	exec := v.(executor.SQLExecutor)
// 	opts := executor.Options{}.
// 	// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
// 	// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
// 	WithDisableIncrStatement().
// 	WithTxn(txn).
// 	WithDatabase(c.db).
// 	WithTimeZone(c.proc.GetSessionInfo().TimeZone)
// 	// WithLowerCaseTableNames(&lower)

// 	return exec.Exec(ctx, sql, opts)
// }

type Worker interface {
	Submit(iteration *Iteration) error
	Stop()
}

type worker struct {
	queue sm.Queue
}

func NewWorker() Worker {
	worker := &worker{}
	worker.queue = sm.NewSafeQueue(10000, 100, worker.onItem)
	worker.queue.Start()
	return worker
}

func (w *worker) Submit(iteration *Iteration) error {
	_, err := w.queue.Enqueue(iteration)
	return err
}

func (w *worker) onItem(items ...any) {
	for _, item := range items {
		item.(*Iteration).Run()
	}
}

func (w *worker) Stop() {
	w.queue.Stop()
}

// type CNSinker struct {
// 	ctx         context.Context
// 	initTableFn func(context.Context, []engine.TableDef) error
// 	relFactory  relationFactory
// 	currentTxn  client.TxnOperator
// 	currentRel  engine.Relation
// 	def         []engine.TableDef

// 	mp *mpool.MPool
// }

// // TODO stop sinker, drop table
// func MockCNSinker(
// 	ctx context.Context,
// 	relFactory relationFactory,
// 	initTableFn func(context.Context, []engine.TableDef) error,
// 	def []engine.TableDef,
// 	mp *mpool.MPool,
// ) (cdc.Sinker, error) {
// 	return &CNSinker{
// 		relFactory:  relFactory,
// 		initTableFn: initTableFn,
// 		def:         def,
// 		ctx:         ctx,
// 		mp:          mp,
// 	}, nil
// }

// func (sinker *CNSinker) Run(ctx context.Context, ar *cdc.ActiveRoutine) {
// 	err := sinker.initTableFn(ctx, sinker.def)
// 	if err != nil {
// 		panic(err)
// 	}
// }
// func (sinker *CNSinker) Sink(ctx context.Context, data *cdc.DecoderOutput) {
// 	var initSnapshotSplitTxn bool
// 	var txn client.TxnOperator
// 	var rel engine.Relation
// 	var err error
// 	if sinker.currentRel == nil {
// 		initSnapshotSplitTxn = true
// 		rel, txn, err = sinker.relFactory(ctx)
// 		if err != nil {
// 			panic(err)
// 		}
// 	} else {
// 		txn = sinker.currentTxn
// 		rel = sinker.currentRel
// 	}
// 	insertBat := data.GetInsertAtmBatch()
// 	deleteBat := data.GetDeleteAtmBatch()
// 	if insertBat != nil {
// 		insertBat.Vecs[len(insertBat.Vecs)-1].Free(sinker.mp)
// 		insertBat.Vecs = insertBat.Vecs[:len(insertBat.Vecs)-1]
// 		err := rel.Write(ctx, insertBat)
// 		if err != nil {
// 			panic(err)
// 		}
// 	}
// 	if deleteBat != nil {
// 		deleteBat.Vecs[len(deleteBat.Vecs)-1].Free(sinker.mp)
// 		deleteBat.Vecs = deleteBat.Vecs[:len(deleteBat.Vecs)-1]
// 		err := rel.Delete(ctx, deleteBat, catalog.Row_ID)
// 		if err != nil {
// 			panic(err)
// 		}
// 	}
// 	if initSnapshotSplitTxn {
// 		txn.Commit(ctx)
// 	}
// }
// func (sinker *CNSinker) SendBegin() {
// 	var err error
// 	sinker.currentRel, sinker.currentTxn, err = sinker.relFactory(sinker.ctx)
// 	if err != nil {
// 		panic(err)
// 	}
// }
// func (sinker *CNSinker) SendCommit() {
// 	sinker.currentTxn.Commit(sinker.ctx)
// 	sinker.currentRel = nil
// 	sinker.currentTxn = nil
// }
// func (sinker *CNSinker) SendRollback() {
// 	sinker.currentTxn.Rollback(sinker.ctx)
// 	sinker.currentRel = nil
// 	sinker.currentTxn = nil
// }

// // SendDummy to guarantee the last sql is sent
// func (sinker *CNSinker) SendDummy() {}

// // Error must be called after Sink
// func (sinker *CNSinker) Error() error {
// 	return nil
// }
// func (sinker *CNSinker) ClearError() {}
// func (sinker *CNSinker) Reset()      {}
// func (sinker *CNSinker) Close()      {}
