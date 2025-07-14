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
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"go.uber.org/zap"
)

// 1. init sinker
// 2. dirty sinkers
// 3. all sinkers
type Iteration struct {
	table   *TableInfo_2
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
	defer func() {
		for i, sinkerErr := range iter.err {
			if sinkerErr != nil {
				logutil.Error(
					"Async-Index-CDC-Task iteration failed",
					zap.Uint32("tenantID", iter.table.accountID),
					zap.Uint64("tableID", iter.table.tableID),
					zap.String("indexName", iter.sinkers[i].indexName),
					zap.Error(sinkerErr),
				)
			}
		}
	}()
	txn, err := iter.table.exec.txnFactory()
	if err != nil {
		for i := range iter.sinkers {
			iter.err[i] = err
		}
		return
	}
	defer txn.Commit(ctx)
	iter.startAt = time.Now()
	table, err := iter.table.exec.getRelation(
		ctx,
		txn,
		iter.table.dbName,
		iter.table.tableName,
	)
	if err != nil {
		for i := range iter.sinkers {
			iter.err[i] = err
		}
		return
	}
	if iter.from.IsEmpty() {
		iter.to = types.TimestampToTS(txn.SnapshotTS())
	}
	iter.err = CollectChanges_2(
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
	iter.endAt = time.Now()
	err = iter.insertAsyncIndexIterations(ctx, txn)
	if err != nil {
		indexNames := ""
		for _, sinker := range iter.sinkers {
			indexNames = fmt.Sprintf("%s%s, ", indexNames, sinker.indexName)
		}

		errorStr := ""
		for _, err := range iter.err {
			if err != nil {
				errorStr = fmt.Sprintf("%s%s, ", errorStr, err.Error())
			} else {
				errorStr = fmt.Sprintf("%s%s, ", errorStr, " ")
			}
		}

		logutil.Error(
			"Async-Index-CDC-Task sink iteration failed",
			zap.Uint32("tenantID", iter.table.accountID),
			zap.Uint64("tableID", iter.table.tableID),
			zap.String("indexName", indexNames),
			zap.String("error", errorStr),
			zap.String("from", iter.from.ToString()),
			zap.String("to", iter.to.ToString()),
			zap.String("startAt", iter.startAt.Format(time.RFC3339)),
			zap.String("endAt", iter.endAt.Format(time.RFC3339)),
		)
	}
	iter.table.OnIterationFinished(iter)
}

func (iter *Iteration) insertAsyncIndexIterations(ctx context.Context, txn client.TxnOperator) error {
	indexNames := ""
	for _, sinker := range iter.sinkers {
		indexNames = fmt.Sprintf("%s%s, ", indexNames, sinker.indexName)
	}

	errorStr := ""
	for _, err := range iter.err {
		if err != nil {
			errorStr = fmt.Sprintf("%s%s, ", errorStr, err.Error())
		} else {
			errorStr = fmt.Sprintf("%s%s, ", errorStr, " ")
		}
	}

	sql := cdc.CDCSQLBuilder.AsyncIndexIterationsInsertSQL(
		iter.table.accountID,
		iter.table.tableID,
		indexNames,
		iter.from,
		iter.to,
		errorStr,
		iter.startAt,
		iter.endAt,
	)
	_, err := ExecWithResult(ctx, sql, iter.table.exec.cnUUID, txn)
	if err != nil {
		return err
	}
	return err
}
