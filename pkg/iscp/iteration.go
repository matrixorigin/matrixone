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
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"go.uber.org/zap"
)

// 1. init sinker
// 2. dirty sinkers
// 3. all sinkers
type Iteration struct {
	table   *TableEntry
	sinkers []*SinkerEntry
	from    types.TS
	to      types.TS
	err     []error
	startAt time.Time
	endAt   time.Time
}

func (iter *Iteration) Run() {
	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, iter.table.accountID)
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	iter.err = make([]error, len(iter.sinkers))
	iter.startAt = time.Now()
	var txn client.TxnOperator
	defer func() {
		iter.endAt = time.Now()
		iter.table.OnIterationFinished(iter)
		for i, sinkerErr := range iter.err {
			if sinkerErr != nil {
				logutil.Error(
					"Async-Index-CDC-Task iteration failed",
					zap.Uint32("tenantID", iter.table.accountID),
					zap.Uint64("tableID", iter.table.tableID),
					zap.String("indexName", iter.sinkers[i].indexName),
					zap.String("from", iter.from.ToString()),
					zap.String("to", iter.to.ToString()),
					zap.Error(sinkerErr),
				)
			}
		}
	}()
	txn, err := iter.table.exec.txnFactory()
	if txn != nil {
		defer txn.Commit(ctx)
	}
	if msg, injected := objectio.CDCExecutorInjected(); injected && msg == "iterationCreateTxn" {
		err = moerr.NewInternalErrorNoCtx(msg)
	}
	if err != nil {
		for i := range iter.sinkers {
			iter.err[i] = err
		}
		return
	}
	iter.startAt = time.Now()
	table, err := iter.table.exec.getRelation(
		ctx,
		txn,
		iter.table.dbName,
		iter.table.tableName,
	)
	if msg, injected := objectio.CDCExecutorInjected(); injected && msg == "iterationGetRelation" {
		err = moerr.NewInternalErrorNoCtx(msg)
	}
	if err != nil {
		for i := range iter.sinkers {
			iter.err[i] = err
		}
		return
	}
	if iter.from.IsEmpty() {
		iter.to = types.TimestampToTS(txn.SnapshotTS())
	}
	iter.err = CollectChangesForIteration(
		ctx,
		iter,
		table,
		iter.from,
		iter.to,
		iter.sinkers,
		false,
		iter.table.exec.packer,
		iter.table.exec.mp,
	)
}
