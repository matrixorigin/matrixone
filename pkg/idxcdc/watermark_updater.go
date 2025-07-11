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
	"encoding/json"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func ExecWithResult(
	ctx context.Context,
	sql string,
	cnUUID string,
	txn client.TxnOperator,
) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(cnUUID).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(txn)

	return exec.Exec(ctx, sql, opts)
}

// return true if create, return false if task already exists, return error when error
func RegisterJob(
	ctx context.Context,
	cnUUID string,
	txn client.TxnOperator,
	pitr_name string,
	sinkerinfo_json *ConsumerInfo,
) (ok bool, err error) {
	tenantId, err := defines.GetAccountId(ctx)
	consumerInfoJson, err := json.Marshal(sinkerinfo_json)
	if err != nil {
		return false, err
	}

	tableIDSql := cdc.CDCSQLBuilder.GetTableIDSQL(
		tenantId,
		sinkerinfo_json.DbName,
		sinkerinfo_json.TableName,
	)
	result, err := ExecWithResult(ctx, tableIDSql, cnUUID, txn)
	if err != nil {
		return false, err
	}
	defer result.Close()
	var tableID uint64
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows != 1 {
			panic(fmt.Sprintf("invalid rows %d", rows))
		}
		for i := 0; i < rows; i++ {
			tableID = vector.MustFixedColWithTypeCheck[uint64](cols[0])[i]
		}
		return true
	})

	sql := cdc.CDCSQLBuilder.AsyncIndexLogInsertSQL(
		tenantId,
		tableID,
		sinkerinfo_json.IndexName,
		"",
		string(consumerInfoJson),
	)
	_, err = ExecWithResult(ctx, sql, cnUUID, txn)
	if err != nil {
		// TODO: if duplicate, update ok
		return false, err
	}
	return true, nil
}

// return true if delete success, return false if no task found, return error when delete failed.
func UnregisterJob(
	ctx context.Context,
	cnUUID string,
	txn client.TxnOperator,
	consumerInfo *ConsumerInfo,
) (bool, error) {
	tenantId, err := defines.GetAccountId(ctx)
	if err != nil {
		return false, err
	}
	tableIDSql := cdc.CDCSQLBuilder.GetTableIDSQL(
		tenantId,
		consumerInfo.DbName,
		consumerInfo.TableName,
	)
	result, err := ExecWithResult(ctx, tableIDSql, cnUUID, txn)
	if err != nil {
		return false, err
	}
	defer result.Close()
	var tableID uint64
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows != 1 {
			panic(fmt.Sprintf("invalid rows %d", rows))
		}
		for i := 0; i < rows; i++ {
			tableID = vector.MustFixedColWithTypeCheck[uint64](cols[0])[i]
		}
		return true
	})
	sql := cdc.CDCSQLBuilder.AsyncIndexLogUpdateDropAtSQL(
		tenantId,
		tableID,
		consumerInfo.IndexName,
	)
	_, err = ExecWithResult(ctx, sql, cnUUID, txn)
	if err != nil {
		// TODO: if duplicate, update ok
		return false, err
	}
	return true, nil
}
