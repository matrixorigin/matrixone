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
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

type internalExecResult struct {
	batch     *batch.Batch
	err       error
	stringErr error
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
	if res.stringErr != nil {
		return "", res.stringErr
	}
	return res.batch.Vecs[cid].GetStringAt(int(ridx)), nil
}

type mockCDCIE struct {
	de        *testutil.TestDisttaeEngine
	err       error
	stringErr error
}

func (m *mockCDCIE) setError(err error) {
	m.err = err
}

func (m *mockCDCIE) setStringError(err error) {
	m.stringErr = err
}

func (m *mockCDCIE) Exec(ctx context.Context, s string, options internalExecutor.SessionOverrideOptions) error {
	panic("implement me")
}

func (m *mockCDCIE) Query(ctx context.Context, s string, options internalExecutor.SessionOverrideOptions) internalExecutor.InternalExecResult {
	if m.err != nil {
		return &internalExecResult{
			batch: nil,
			err:   m.err,
		}
	}
	res, err := execSql(m.de, ctx, s)
	var bat *batch.Batch
	if len(res.Batches) > 0 {
		bat = res.Batches[0]
	}
	if m.stringErr != nil {
		return &internalExecResult{
			batch:     bat,
			err:       err,
			stringErr: m.stringErr,
		}
	}
	return &internalExecResult{
		batch: bat,
		err:   err,
	}
}
func (m *mockCDCIE) ApplySessionOverride(options internalExecutor.SessionOverrideOptions) {
	panic("implement me")
}

func mock_mo_intra_system_change_propagation_log(
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
) (err error) {
	sql := frontend.MoCatalogMoISCPLogDDL

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

func exec_sql(
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
	sql string,
) (err error) {

	v, ok := moruntime.ServiceRuntime("").GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	exec := v.(executor.SQLExecutor)
	_, err = exec.Exec(ctx, sql, executor.Options{})
	if err != nil {
		return err
	}
	return nil
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
		"`included_columns` text DEFAULT NULL," +
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
	// CREATE DATABASE now persists defaults in this tenant catalog table.
	return exec_sql(de, ctx, catalog.MoDatabaseDefaultsDDL)
}
func mock_mo_foreign_keys(
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
) (err error) {
	sql := "CREATE TABLE `mo_catalog`.`mo_foreign_keys` (" +
		"`constraint_name` varchar(5000) NOT NULL," +
		"`constraint_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`db_name` varchar(5000) NOT NULL," +
		"`db_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`table_name` varchar(5000) NOT NULL," +
		"`table_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`column_name` varchar(256) NOT NULL," +
		"`column_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`refer_db_name` varchar(5000) NOT NULL," +
		"`refer_db_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`refer_table_name` varchar(5000) NOT NULL," +
		"`refer_table_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`refer_column_name` varchar(256) NOT NULL," +
		"`refer_column_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`on_delete` varchar(128) NOT NULL," +
		"`on_update` varchar(128) NOT NULL," +
		"`referenced_index_name` varchar(5000) NOT NULL DEFAULT ''," +
		"`on_delete_origin` varchar(64) NOT NULL DEFAULT 'ACTION_ORIGIN_LEGACY_AMBIGUOUS'," +
		"`on_update_origin` varchar(64) NOT NULL DEFAULT 'ACTION_ORIGIN_LEGACY_AMBIGUOUS'," +
		"PRIMARY KEY (`constraint_name`,`constraint_id`,`db_name`,`db_id`,`table_name`,`table_id`,`column_name`,`column_id`,`refer_db_name`,`refer_db_id`,`refer_table_name`,`refer_table_id`,`refer_column_name`,`refer_column_id`)" +
		")"

	result, err := execSql(de, ctx, sql)
	result.Close()
	return err
}

// prepareISCPConsumerTarget separates empty sink setup from the propagation
// budget. Call before registering the job, so its consumer cannot race DDL.
// Keep CREATE TABLE strict: an unexpectedly reused target is a fixture error.
func prepareISCPConsumerTarget(t *testing.T, ctx context.Context, sourceDB, sourceTable string, tableID uint64, jobName string) {
	t.Helper()
	quote := func(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }
	v, ok := moruntime.ServiceRuntime("").GetGlobalVariables(moruntime.InternalSQLExecutor)
	require.True(t, ok)
	exec := v.(executor.SQLExecutor)
	target := fmt.Sprintf("test_table_%d_%s", tableID, jobName)
	// Both statements belong to one fixture transaction, avoiding an extra
	// durable commit. ExecTxn rolls back partial setup on a statement error.
	err := exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		for _, sql := range []string{
			fmt.Sprintf("create database if not exists %s", quote(iscp.TargetDbName)),
			fmt.Sprintf("create table %s.%s like %s.%s", quote(iscp.TargetDbName), quote(target), quote(sourceDB), quote(sourceTable)),
		} {
			result, err := txn.Exec(sql, executor.StatementOption{})
			result.Close()
			if err != nil {
				return err
			}
		}
		return nil
	}, executor.Options{})
	require.NoError(t, err)
}

func checkISCPConsumerData(t *testing.T, ctx context.Context, sourceDB, sourceTable string, tableID uint64, jobName string) {
	t.Helper()
	v, ok := moruntime.ServiceRuntime("").GetGlobalVariables(moruntime.InternalSQLExecutor)
	require.True(t, ok)
	exec := v.(executor.SQLExecutor)
	source := fmt.Sprintf("%s.%s", sourceDB, sourceTable)
	target := fmt.Sprintf("%s.test_table_%d_%s", iscp.TargetDbName, tableID, jobName)
	for _, pair := range [][2]string{{source, target}, {target, source}} {
		sql := fmt.Sprintf("select * from %s except select * from %s", pair[0], pair[1])
		result, err := exec.Exec(ctx, sql, executor.Options{})
		rows := 0
		if err == nil {
			result.ReadRows(func(n int, _ []*vector.Vector) bool {
				rows += n
				return true
			})
		}
		result.Close()
		require.NoError(t, err)
		require.Zero(t, rows, sql)
	}
}
