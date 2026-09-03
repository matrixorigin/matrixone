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
// QUERY. It uses its own dedicated 2-CN cluster, so nothing here shares state,
// config, or lifecycle with the 1-CN suite. CN shutdown, rolling binaries, and
// cross-node cancellation faults remain separate release tests.
func TestArrowLoadMultiCN(t *testing.T) {
	c := startArrowLoadCluster(t, 2, true /*enabled*/, true /*s3Enabled*/, true /*distributedEnabled*/)
	db := openArrowLoadDB(t, c, 0)
	mustExec(t, db, "create database if not exists arrow_multicn")
	mustExec(t, db, "use arrow_multicn")

	t.Run("DistributedRecordBatchFanout", func(t *testing.T) { testArrowMultiCNFanout(t, db) })
	t.Run("CancelMidLoad", func(t *testing.T) { testArrowCancelMidLoad(t, c) })
}

// testArrowMultiCNFanout loads the "large" multi-record-batch fixture with
// `PARALLEL 'true'` against the 2-CN cluster and checks full row-count and content
// correctness. Shard-routing internals are already unit-tested in
// pkg/sql/compile's arrow_scope_test.go; this test's job is proving the whole thing
// produces correct data on a real multi-CN cluster, not re-deriving that routing.
func testArrowMultiCNFanout(t *testing.T, db *sql.DB) {
	path, ddl := fixtureLarge(t)
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
func testArrowCancelMidLoad(t *testing.T, c embed.Cluster) {
	loaderDB := openArrowLoadDB(t, c, 0)
	killerDB := openArrowLoadDB(t, c, 0)

	ctx := context.Background()
	conn, err := loaderDB.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "use arrow_multicn")
	require.NoError(t, err)
	path, ddl := fixtureLarge(t)
	_, err = conn.ExecContext(ctx, "drop table if exists cancel_mid_load")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, fmt.Sprintf("create table cancel_mid_load(%s)", ddl))
	require.NoError(t, err)

	var connID int64
	require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&connID))

	loadErrCh := make(chan error, 1)
	go func() {
		_, execErr := conn.ExecContext(ctx, fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow'} into table cancel_mid_load", path))
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
