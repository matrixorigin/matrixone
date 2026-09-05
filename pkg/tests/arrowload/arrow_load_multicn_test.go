// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package arrowload

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// TestArrowLoadMultiCN covers two public-path cases that need more than one CN:
// distributed record-batch fan-out correctness and cancelling a LOAD with KILL
// QUERY. Both cancellation statements request distributed record-batch fanout,
// so their coordinator cancellation must propagate to worker scopes on the
// second CN. Worker shutdown runs last because it intentionally removes that CN
// from the fixture.
func TestArrowLoadMultiCN(t *testing.T) {
	c := startArrowLoadCluster(t, 2, true, false, true)
	db := openArrowLoadDB(t, c, 0)
	mustExec(t, db, "create database if not exists arrow_multicn")
	mustExec(t, db, "use arrow_multicn")
	path, ddl := fixtureLarge(t)

	t.Run("DistributedRecordBatchFanout", func(t *testing.T) { testArrowMultiCNFanout(t, db, path, ddl) })
	t.Run("CancelMidLoad", func(t *testing.T) { testArrowCancelMidLoad(t, c, path, ddl) })
	t.Run("ClientContextCancel", func(t *testing.T) { testArrowClientContextCancel(t, c, path, ddl) })
	t.Run("WorkerCNShutdown", func(t *testing.T) { testArrowWorkerCNShutdown(t, c, path, ddl) })
}

// testArrowMultiCNFanout loads the "large" multi-record-batch fixture with
// `PARALLEL 'true'` against the 2-CN cluster and checks full row-count and content
// correctness. Shard-routing internals are already unit-tested in
// pkg/sql/compile's arrow_scope_test.go; this test's job is proving the whole thing
// produces correct data on a real multi-CN cluster, not re-deriving that routing.
func testArrowMultiCNFanout(t *testing.T, db *sql.DB, path, ddl string) {
	mustExec(t, db, "drop table if exists large_fanout")
	mustExec(t, db, fmt.Sprintf("create table large_fanout(%s)", ddl))
	mustExec(t, db, fmt.Sprintf(
		"load data infile {'filepath'='%s','format'='arrow'} into table large_fanout parallel 'true'", path))

	require.Equal(t, int64(largeFixtureRows), queryCount(t, db, "select count(*) from large_fanout"))
	require.Equal(t, int64(largeFixtureRows), queryCount(t, db, "select count(distinct id) from large_fanout"))
	require.Equal(t, int64(0), queryCount(t, db,
		fmt.Sprintf("select count(*) from large_fanout where id < 0 or id >= %d", largeFixtureRows)))
}

// testArrowCancelMidLoad opens one dedicated connection for the LOAD and a second
// for the killer, polls processlist() (no sleep-based guessing) until the LOAD is
// observed actually running on the server, issues KILL QUERY, and asserts: the LOAD
// returns an error, the target table ends up with zero rows (the multi-file
// all-or-nothing invariant applies to a single cancelled statement too), and the
// killed connection itself remains usable afterward (KILL QUERY, not KILL
// CONNECTION), mirroring pkg/frontend/mysql_protocol_test.go's kill-query test.
func testArrowCancelMidLoad(t *testing.T, c embed.Cluster, path, ddl string) {
	loaderDB := openArrowLoadDB(t, c, 0)
	killerDB := openArrowLoadDB(t, c, 0)

	ctx := context.Background()
	conn, err := loaderDB.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "use arrow_multicn")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "drop table if exists cancel_mid_load")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, fmt.Sprintf("create table cancel_mid_load(%s)", ddl))
	require.NoError(t, err)

	var connID int64
	require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&connID))

	loadErrCh := make(chan error, 1)
	go func() {
		_, execErr := conn.ExecContext(ctx, fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow'} into table cancel_mid_load parallel 'true'", path))
		loadErrCh <- execErr
	}()

	waitUntilStatementRunning(t, killerDB, connID, "load data", 30*time.Second)
	mustExec(t, killerDB, fmt.Sprintf("kill query %d", connID))

	select {
	case err := <-loadErrCh:
		require.Error(t, err, "a killed LOAD must return an error, not succeed")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the killed LOAD statement to return")
	}

	verifyDB := openArrowLoadDB(t, c, 0)
	require.Equal(t, int64(0), queryCount(t, verifyDB, "select count(*) from arrow_multicn.cancel_mid_load"),
		"a canceled LOAD must not leave any partially committed rows")

	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, conn.PingContext(pingCtx), "the connection must remain usable after KILL QUERY (not KILL CONNECTION)")
}

// testArrowClientContextCancel covers the other public cancellation source:
// client-side context cancellation closes the in-flight request instead of
// issuing KILL QUERY from a second session. The server must still roll back the
// whole multi-batch LOAD and leave the cluster usable for verification.
func testArrowClientContextCancel(t *testing.T, c embed.Cluster, path, ddl string) {
	loaderDB := openArrowLoadDB(t, c, 0)
	observerDB := openArrowLoadDB(t, c, 0)
	ctx := context.Background()
	conn, err := loaderDB.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.ExecContext(ctx, "use arrow_multicn")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "drop table if exists client_cancel_load")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, fmt.Sprintf("create table client_cancel_load(%s)", ddl))
	require.NoError(t, err)

	var connID int64
	require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&connID))
	loadCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	loadErrCh := make(chan error, 1)
	go func() {
		_, execErr := conn.ExecContext(loadCtx, fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow'} into table client_cancel_load parallel 'true'", path))
		loadErrCh <- execErr
	}()

	waitUntilStatementRunning(t, observerDB, connID, "load data", 30*time.Second)
	cancel()
	select {
	case err := <-loadErrCh:
		require.Error(t, err, "a client-canceled LOAD must return an error")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the client-canceled LOAD statement to return")
	}

	verifyDB := openArrowLoadDB(t, c, 0)
	require.Equal(t, int64(0), queryCount(t, verifyDB, "select count(*) from arrow_multicn.client_cancel_load"),
		"client cancellation must not leave partially committed rows")
}

// testArrowWorkerCNShutdown closes the second CN only after a distributed LOAD
// is visible on the coordinator. CN shutdown may either drain its already
// admitted worker scope or cancel the statement, but the transaction boundary
// permits only the complete fixture or zero rows. A partial row count would mean
// service teardown published an incomplete LOAD.
func testArrowWorkerCNShutdown(t *testing.T, c embed.Cluster, path, ddl string) {
	loaderDB := openArrowLoadDB(t, c, 0)
	observerDB := openArrowLoadDB(t, c, 0)
	ctx := context.Background()
	conn, err := loaderDB.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.ExecContext(ctx, "use arrow_multicn")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "drop table if exists worker_shutdown_load")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, fmt.Sprintf("create table worker_shutdown_load(%s)", ddl))
	require.NoError(t, err)

	var connID int64
	require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&connID))
	loadErrCh := make(chan error, 1)
	go func() {
		_, execErr := conn.ExecContext(ctx, fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow'} into table worker_shutdown_load parallel 'true'", path))
		loadErrCh <- execErr
	}()
	waitUntilStatementRunning(t, observerDB, connID, "load data", 30*time.Second)

	worker, err := c.GetCNService(1)
	require.NoError(t, err)
	closeErrCh := make(chan error, 1)
	go func() { closeErrCh <- worker.Close() }()
	select {
	case err := <-closeErrCh:
		require.NoError(t, err)
	case <-time.After(60 * time.Second):
		t.Fatal("timed out waiting for the worker CN to drain and stop")
	}

	var loadErr error
	select {
	case loadErr = <-loadErrCh:
	// Worker loss is detected by the cluster's own bounded liveness checks,
	// which can take about 30 seconds. Keep this outer deadline comfortably
	// above that boundary, especially under -race; it guards against a true
	// non-terminating LOAD rather than racing the expected failure detector.
	case <-time.After(90 * time.Second):
		t.Fatal("timed out waiting for LOAD after worker CN shutdown")
	}
	verifyDB := openArrowLoadDB(t, c, 0)
	rows := queryCount(t, verifyDB, "select count(*) from arrow_multicn.worker_shutdown_load")
	if loadErr == nil {
		require.Equal(t, int64(largeFixtureRows), rows,
			"a drained LOAD must publish the complete fixture")
	} else {
		require.Zero(t, rows, "a canceled LOAD must publish no rows")
	}
}
