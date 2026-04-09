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
