// Copyright 2021 - 2026 Matrix Origin
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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestIssue27734RestoreCommitFencesSecondCNBeforeRecovery(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		cnA, err := cluster.GetCNService(0)
		require.NoError(t, err)
		cnB, err := cluster.GetCNService(1)
		require.NoError(t, err)
		open := func(port int64) *sql.DB {
			db, openErr := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
			require.NoError(t, openErr)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			return db
		}
		dbA := open(cnA.GetServiceConfig().CN.Frontend.Port)
		dbB := open(cnB.GetServiceConfig().CN.Frontend.Port)
		databaseName := testutils.GetDatabaseName(t)
		mustExec27734(t, ctx, dbA, fmt.Sprintf("create database `%s`", databaseName))
		defer dbA.ExecContext(context.Background(), fmt.Sprintf("drop database if exists `%s`", databaseName))
		mustExec27734(t, ctx, dbA, fmt.Sprintf("use `%s`", databaseName))
		mustExec27734(t, ctx, dbB, fmt.Sprintf("use `%s`", databaseName))
		mustExec27734(t, ctx, dbA, "create table source_table(a int)")
		mustExec27734(t, ctx, dbA, "create view target_view as select a from source_table")
		waitQuery27734(t, ctx, dbB, "desc target_view", true)
		mustExec27734(t, ctx, dbA, "create snapshot issue27734_restore_snapshot for account")
		defer dbA.ExecContext(context.Background(), "drop snapshot if exists issue27734_restore_snapshot")
		mustExec27734(t, ctx, dbA, "alter table source_table modify a bigint")
		waitQuery27734(t, ctx, dbB, "desc target_view", true)

		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseRecovery := func() { releaseOnce.Do(func() { close(release) }) }
		reachedA := make(chan struct{}, 1)
		reachedB := make(chan struct{}, 1)
		cleanupA := compile.SetViewMetadataRecoveryBarrierForTest(
			cnA.GetServiceConfig().CN.UUID, reachedA, release)
		cleanupB := compile.SetViewMetadataRecoveryBarrierForTest(
			cnB.GetServiceConfig().CN.UUID, reachedB, release)
		defer cleanupA()
		defer cleanupB()
		// Registered after map cleanup so LIFO teardown releases blocked workers
		// before removing their barriers, including FailNow/timeout paths.
		defer releaseRecovery()
		select {
		case <-reachedA:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		select {
		case <-reachedB:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}

		mustExec27734(t, ctx, dbA, fmt.Sprintf(
			"restore table `%s`.source_table{snapshot='issue27734_restore_snapshot'}", databaseName))
		_, err = dbB.ExecContext(ctx, "desc target_view")
		require.ErrorContains(t, err, "invalid view")

		releaseRecovery()
		waitQuery27734(t, ctx, dbB, "desc target_view", true)
	})
}

func mustExec27734(t *testing.T, ctx context.Context, db *sql.DB, statement string) {
	t.Helper()
	_, err := db.ExecContext(ctx, statement)
	require.NoError(t, err, statement)
}

func waitQuery27734(t *testing.T, ctx context.Context, db *sql.DB, query string, wantSuccess bool) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		err := query27734(ctx, db, query)
		if (err == nil) == wantSuccess {
			return
		}
		if time.Now().After(deadline) {
			require.NoError(t, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func query27734(ctx context.Context, db *sql.DB, query string) error {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
	}
	return rows.Err()
}
