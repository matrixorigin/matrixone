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
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

// startArrowLoadCluster starts a dedicated, non-shared embedded cluster with the
// `cn.frontend.arrow-load` rollout gates set as requested, closing it when the test
// completes. Every call here gets its own cluster and its own process-local state
// (unlike pkg/embed's package-level basicClusterState/singleCNClusterState), so
// sequential calls across this package's tests never trip the "accidental second
// embedded test cluster" exclusivity guard, and the feature never leaks into any
// other package's shared cluster or the default etc/launch* BVT configuration.
func startArrowLoadCluster(t *testing.T, cnCount int, enabled, s3Enabled, distributedEnabled bool) embed.Cluster {
	t.Helper()
	c, err := embed.StartTestCluster(
		embed.WithCNCount(cnCount),
		embed.WithPreStart(func(svc embed.ServiceOperator) {
			if svc.ServiceType() != metadata.ServiceType_CN {
				return
			}
			svc.Adjust(func(cfg *embed.ServiceConfig) {
				cfg.CN.Frontend.ArrowLoad.Enabled = enabled
				cfg.CN.Frontend.ArrowLoad.S3Enabled = s3Enabled
				cfg.CN.Frontend.ArrowLoad.DistributedEnabled = distributedEnabled
			})
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, c.Close())
	})
	return c
}

// openArrowLoadDB opens a real MySQL-protocol connection (not the internal SQL
// executor) against the given CN, so statements run through the same frontend path
// a real client would use. This is required for KILL QUERY, multi-session isolation, and
// SHOW-PROCESSLIST-style observation to mean anything.
func openArrowLoadDB(t *testing.T, c embed.Cluster, cnIndex int) *sql.DB {
	t.Helper()
	cn, err := c.GetCNService(cnIndex)
	require.NoError(t, err)
	port := cn.GetServiceConfig().CN.Frontend.Port
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, db.PingContext(ctx))
	return db
}

func mustExec(t *testing.T, db *sql.DB, stmt string, args ...any) {
	t.Helper()
	_, err := db.Exec(stmt, args...)
	require.NoError(t, err, stmt)
}

func queryCount(t *testing.T, db *sql.DB, query string, args ...any) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.QueryRow(query, args...).Scan(&n), query)
	return n
}

func queryConnectionID(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	return queryCount(t, db, "select connection_id()")
}

// waitUntilStatementRunning polls information_schema.processlist from a second
// connection until the target connection's current statement text contains needle,
// proving the statement is actually mid-execution before the caller acts on it (e.g.
// issues KILL QUERY). This is a deterministic synchronization point, not a
// fixed-duration sleep: it returns as soon as the condition holds, and fails the
// test if it never does within deadline.
func waitUntilStatementRunning(t *testing.T, observer *sql.DB, connID int64, needle string, deadline time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var info sql.NullString
		err := observer.QueryRowContext(ctx,
			"select info from information_schema.processlist where conn_id = ?", connID,
		).Scan(&info)
		if err == nil && info.Valid && strings.Contains(strings.ToLower(info.String), strings.ToLower(needle)) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for connection %d to run a statement containing %q (last info=%q, err=%v)",
				connID, needle, info.String, err)
		case <-ticker.C:
		}
	}
}
