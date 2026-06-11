// Copyright 2026 Matrix Origin
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

package dml

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

const dataBranchMergePickTxnError = "DATA BRANCH MERGE/PICK is not supported in transactions"

func TestDataBranchMerge(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
			defer cancel()

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			port := cn1.GetServiceConfig().CN.Frontend.Port
			dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port)
			sqlDB, err := sql.Open("mysql", dsn)
			require.NoError(t, err)
			defer sqlDB.Close()

			t.Log("merge simple LCA regression")
			runMergeSimpleLCARegression(t, ctx, sqlDB)

			t.Log("merge and pick reject explicit transactions")
			runDataBranchMergePickExplicitTxnError(t, ctx, sqlDB)

			t.Log("merge and pick reject autocommit off implicit transactions")
			runDataBranchMergePickAutocommitOff(t, ctx, sqlDB)
		},
	)
}

func runMergeSimpleLCARegression(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, "use mo_catalog")
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()

	execSQLDB(t, ctx, db, "create table t0 (a int, b int, primary key(a))")
	execSQLDB(t, ctx, db, "insert into t0 values (1,1),(2,2),(3,3)")

	execSQLDB(t, ctx, db, "data branch create table t1 from t0")
	execSQLDB(t, ctx, db, "insert into t1 values (4,4)")

	execSQLDB(t, ctx, db, "data branch create table t2 from t0")
	execSQLDB(t, ctx, db, "insert into t2 values (5,5)")

	execSQLDB(t, ctx, db, "data branch merge t2 into t1")

	require.Equal(
		t,
		[][]string{
			{"1", "1"},
			{"2", "2"},
			{"3", "3"},
			{"4", "4"},
			{"5", "5"},
		},
		queryStringRows(t, ctx, db, "select a, b from t1 order by a"),
	)

	require.Equal(
		t,
		[][]string{
			{"t1", "INSERT", "4", "4"},
		},
		queryStringRows(t, ctx, db, "data branch diff t2 against t1"),
	)
}

func runDataBranchMergePickExplicitTxnError(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, "use mo_catalog")
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()

	execSQLDB(t, ctx, db, "create table base (id int primary key, v int)")
	execSQLDB(t, ctx, db, "insert into base values (1,1),(2,2)")

	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "update src set v = 9 where id = 1")
	execSQLDB(t, ctx, db, "begin")
	errMsg := execExpectError(t, ctx, db, "data branch merge src into base when conflict accept")
	require.Contains(t, errMsg, dataBranchMergePickTxnError)
	execSQLDB(t, ctx, db, "rollback")

	execSQLDB(t, ctx, db, "data branch create table pick_src from base")
	execSQLDB(t, ctx, db, "insert into pick_src values (3,3)")
	execSQLDB(t, ctx, db, "begin")
	errMsg = execExpectError(t, ctx, db, "data branch pick pick_src into base keys(3)")
	require.Contains(t, errMsg, dataBranchMergePickTxnError)
	execSQLDB(t, ctx, db, "rollback")
}

func runDataBranchMergePickAutocommitOff(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, "set autocommit = 1")
		execSQLDB(t, ctx, db, "use mo_catalog")
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()

	execSQLDB(t, ctx, db, "create table base (id int primary key, v int)")
	execSQLDB(t, ctx, db, "insert into base values (1,1),(2,2)")

	execSQLDB(t, ctx, db, "data branch create table merge_src from base")
	execSQLDB(t, ctx, db, "update merge_src set v = 9 where id = 1")

	// Under autocommit=0 the first write opens an implicit transaction.
	// MERGE/PICK cannot follow that transaction, so they must be rejected
	// instead of silently committing in a separate background transaction.
	// The outer rollback then leaves base untouched.
	execSQLDB(t, ctx, db, "set autocommit = 0")
	execSQLDB(t, ctx, db, "insert into base values (10,10)")
	errMsg := execExpectError(t, ctx, db, "data branch merge merge_src into base when conflict accept")
	require.Contains(t, errMsg, dataBranchMergePickTxnError)
	execSQLDB(t, ctx, db, "rollback")
	execSQLDB(t, ctx, db, "set autocommit = 1")

	require.Equal(
		t,
		[][]string{
			{"1", "1"},
			{"2", "2"},
		},
		queryStringRows(t, ctx, db, "select id, v from base order by id"),
	)

	execSQLDB(t, ctx, db, "data branch create table pick_src from base")
	execSQLDB(t, ctx, db, "insert into pick_src values (3,3)")

	execSQLDB(t, ctx, db, "set autocommit = 0")
	execSQLDB(t, ctx, db, "insert into base values (20,20)")
	errMsg = execExpectError(t, ctx, db, "data branch pick pick_src into base keys(3)")
	require.Contains(t, errMsg, dataBranchMergePickTxnError)
	execSQLDB(t, ctx, db, "rollback")
	execSQLDB(t, ctx, db, "set autocommit = 1")

	require.Equal(
		t,
		[][]string{
			{"1", "1"},
			{"2", "2"},
		},
		queryStringRows(t, ctx, db, "select id, v from base order by id"),
	)
}
