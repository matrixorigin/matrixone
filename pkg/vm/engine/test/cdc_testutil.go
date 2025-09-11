// Copyright 2024 Matrix Origin
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

package test

import (
	"context"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"

	"github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

type internalExecResult struct {
	affectedRows uint64
	batch        *batch.Batch
	err          error
}

func (res *internalExecResult) GetUint64(ctx context.Context, u uint64, u2 uint64) (uint64, error) {
	return 0, nil
}

func (res *internalExecResult) Error() error {
	return res.err
}

func (res *internalExecResult) ColumnCount() uint64 {
	return 1
}

func (res *internalExecResult) Column(ctx context.Context, i uint64) (name string, typ uint8, signed bool, err error) {
	return "test", 1, true, nil
}

func (res *internalExecResult) RowCount() uint64 {
	if res.batch == nil {
		return 0
	}
	return uint64(res.batch.RowCount())
}

func (res *internalExecResult) Row(ctx context.Context, i uint64) ([]interface{}, error) {
	return nil, nil
}

func (res *internalExecResult) Value(ctx context.Context, ridx uint64, cidx uint64) (interface{}, error) {
	return nil, nil
}

func (res *internalExecResult) GetFloat64(ctx context.Context, ridx uint64, cid uint64) (float64, error) {
	return 0.0, nil
}
func (res *internalExecResult) GetString(ctx context.Context, ridx uint64, cid uint64) (string, error) {
	return res.batch.Vecs[cid].GetStringAt(int(ridx)), nil
}

type mockCDCIE struct {
	de *testutil.TestDisttaeEngine
}

func (m *mockCDCIE) Exec(ctx context.Context, s string, options internalExecutor.SessionOverrideOptions) error {
	panic("implement me")
}

func (m *mockCDCIE) Query(ctx context.Context, s string, options internalExecutor.SessionOverrideOptions) internalExecutor.InternalExecResult {
	res, err := execSql(m.de, ctx, s)
	var bat *batch.Batch
	if len(res.Batches) > 0 {
		bat = res.Batches[0]
	}
	return &internalExecResult{
		batch: bat,
		err:   err,
	}
}
func (m *mockCDCIE) ApplySessionOverride(options internalExecutor.SessionOverrideOptions) {
	panic("implement me")
}

func execSql(
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
	sql string,
) (res executor.Result, err error) {

	v, ok := moruntime.ServiceRuntime("").GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	exec := v.(executor.SQLExecutor)
	res, err = exec.Exec(ctx, sql, executor.Options{})
	if err != nil {
		return
	}
	return
}
func mock_mo_indexes(
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
) (err error) {
	sql := "CREATE TABLE `mo_catalog`.`mo_indexes` ( " +
		"`id` bigint unsigned NOT NULL," +
		"`table_id` bigint unsigned NOT NULL," +
		"`database_id` bigint unsigned NOT NULL," +
		"`name` varchar(64) NOT NULL," +
		"`type` varchar(11) NOT NULL," +
		"`algo` varchar(11) DEFAULT NULL," +
		"`algo_table_type` varchar(11) DEFAULT NULL," +
		"`algo_params` varchar(2048) DEFAULT NULL," +
		"`is_visible` tinyint NOT NULL," +
		"`hidden` tinyint NOT NULL," +
		"`comment` varchar(2048) NOT NULL," +
		"`column_name` varchar(256) NOT NULL," +
		"`ordinal_position` int unsigned NOT NULL," +
		"`options` text DEFAULT NULL," +
		"`index_table_name` varchar(5000) DEFAULT NULL," +
		"PRIMARY KEY (`table_id`,`column_name`)" + // use table_id as primary key instead of id to avoid duplicate
		")"

	v, ok := moruntime.ServiceRuntime("").GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	exec := v.(executor.SQLExecutor)
	txn, err := de.NewTxnOperator(ctx, de.Now())
	if err != nil {
		return err
	}
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(txn)

	_, err = exec.Exec(ctx, sql, opts)
	if err != nil {
		return err
	}
	if err = txn.Commit(ctx); err != nil {
		return err
	}
	return err
}
