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
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

// Arrow LOAD tests use dedicated, non-shared embedded clusters and close them at
// cleanup. This keeps process-local metrics and lifecycle state out of pkg/embed's
// package-level shared clusters. Default-path tests leave Arrow configuration
// untouched; focused rollback tests explicitly override the kill switches.
type arrowLoadClusterOptions struct {
	cnCount            int
	enabled            bool
	s3Enabled          bool
	distributedEnabled bool
	forceMaterialize   bool
	useDefaults        bool
}

const (
	arrowLoadClusterReadyTimeout = 30 * time.Second
	arrowLoadClusterReadyPoll    = 100 * time.Millisecond
)

// waitArrowLoadClusterReady closes the gap between process startup and the
// lockservice membership contract used by Arrow LOAD. StartTestCluster returns
// after the services are started, while each service's cached CN inventory may
// still be empty or carry a CN without its lock endpoint. A distributed LOAD
// can reach that window before the lock-table cleaner and be rejected as an
// orphan transaction. Refresh every local CN view and require every expected
// owner to be present in the raw inventory before a test opens its frontend.
func waitArrowLoadClusterReady(t testing.TB, c embed.Cluster) {
	t.Helper()

	var expected []string
	c.ForeachServices(func(svc embed.ServiceOperator) bool {
		if svc.ServiceType() == metadata.ServiceType_CN {
			expected = append(expected, svc.ServiceID())
		}
		return true
	})
	require.NotEmpty(t, expected, "Arrow LOAD cluster must have at least one CN")

	ctx, cancel := context.WithTimeout(context.Background(), arrowLoadClusterReadyTimeout)
	defer cancel()
	poll := time.NewTicker(arrowLoadClusterReadyPoll)
	defer poll.Stop()
	started := time.Now()
	var lastStatus string
	for {
		ready, status := arrowLoadClusterReadyStatus(ctx, expected)
		if ready {
			t.Logf("MO_UT_SETUP fixture=arrowload phase=lockservice-ready duration=%s cn_count=%d status=ready",
				time.Since(started), len(expected))
			return
		}
		lastStatus = status

		select {
		case <-ctx.Done():
			require.Failf(t,
				"Arrow LOAD cluster did not become lockservice-ready",
				"elapsed=%s; expected CNs=%v; last status=%s",
				time.Since(started), expected, lastStatus)
			return
		case <-poll.C:
		}
	}
}

func arrowLoadClusterReadyStatus(ctx context.Context, expected []string) (bool, string) {
	for _, serviceID := range expected {
		cluster, err := clusterservice.GetMOClusterWithContext(ctx, serviceID)
		if err != nil {
			return false, fmt.Sprintf("CN %s cluster lookup failed: %v", serviceID, err)
		}
		refresher, ok := cluster.(clusterservice.AuthoritativeRefresher)
		if !ok {
			return false, fmt.Sprintf("CN %s cluster has no authoritative refresher", serviceID)
		}
		if err := refresher.Refresh(ctx); err != nil {
			return false, fmt.Sprintf("CN %s inventory refresh failed: %v", serviceID, err)
		}

		observed := make(map[string]string, len(expected))
		if err := clusterservice.GetCNServiceRawWithContext(
			ctx,
			cluster,
			clusterservice.NewSelectAll(),
			func(cn metadata.CNService) bool {
				observed[cn.ServiceID] = cn.LockServiceAddress
				return true
			},
		); err != nil {
			return false, fmt.Sprintf("CN %s raw inventory read failed: %v", serviceID, err)
		}

		for _, expectedID := range expected {
			address, found := observed[expectedID]
			if !found {
				return false, fmt.Sprintf(
					"CN %s view is missing owner %s (observed=%v)",
					serviceID, expectedID, observed)
			}
			if address == "" {
				return false, fmt.Sprintf(
					"CN %s view has empty lock endpoint for owner %s (observed=%v)",
					serviceID, expectedID, observed)
			}
		}
	}
	return true, "all expected CN lock endpoints are present in every local inventory"
}

func startArrowLoadCluster(t testing.TB, cnCount int, enabled, s3Enabled, distributedEnabled bool) embed.Cluster {
	t.Helper()
	return startArrowLoadClusterWithOptions(t, arrowLoadClusterOptions{
		cnCount: cnCount, enabled: enabled, s3Enabled: s3Enabled,
		distributedEnabled: distributedEnabled,
	})
}

// startArrowLoadClusterWithDefaults deliberately installs no Arrow-specific
// configuration. Tests using it prove the product default is available.
func startArrowLoadClusterWithDefaults(t testing.TB, cnCount int) embed.Cluster {
	t.Helper()
	return startArrowLoadClusterWithOptions(t, arrowLoadClusterOptions{
		cnCount: cnCount, useDefaults: true,
	})
}

func startArrowLoadClusterWithOptions(t testing.TB, options arrowLoadClusterOptions) embed.Cluster {
	t.Helper()
	clusterOptions := []embed.Option{embed.WithCNCount(options.cnCount)}
	if !options.useDefaults {
		clusterOptions = append(clusterOptions, embed.WithPreStart(func(svc embed.ServiceOperator) {
			if svc.ServiceType() != metadata.ServiceType_CN {
				return
			}
			svc.Adjust(func(cfg *embed.ServiceConfig) {
				cfg.CN.Frontend.ArrowLoad.Enabled = options.enabled
				cfg.CN.Frontend.ArrowLoad.S3Enabled = options.s3Enabled
				cfg.CN.Frontend.ArrowLoad.DistributedEnabled = options.distributedEnabled
				cfg.CN.Frontend.ArrowLoad.ForceMaterialize = options.forceMaterialize
			})
		}))
	}
	c, err := embed.StartTestCluster(clusterOptions...)
	if c != nil {
		t.Cleanup(func() {
			require.NoError(t, c.Close())
		})
	}
	require.NoError(t, err)
	waitArrowLoadClusterReady(t, c)
	return c
}

// startArrowLoadClusterWithForceModes provisions one CN for each ownership
// policy in the fallback test. Both CNs use the same storage and fixture, so
// the policy comparison pays for one cluster lifecycle while still compiling
// and executing each mode through an independent public frontend.
func startArrowLoadClusterWithForceModes(t testing.TB) embed.Cluster {
	t.Helper()
	nextCN := 0
	c, err := embed.StartTestCluster(
		embed.WithCNCount(2),
		embed.WithPreStart(func(svc embed.ServiceOperator) {
			if svc.ServiceType() != metadata.ServiceType_CN {
				return
			}
			forceMaterialize := nextCN == 1
			nextCN++
			svc.Adjust(func(cfg *embed.ServiceConfig) {
				cfg.CN.Frontend.ArrowLoad.Enabled = true
				cfg.CN.Frontend.ArrowLoad.S3Enabled = true
				cfg.CN.Frontend.ArrowLoad.DistributedEnabled = true
				cfg.CN.Frontend.ArrowLoad.ForceMaterialize = forceMaterialize
			})
		}))
	if c != nil {
		t.Cleanup(func() { require.NoError(t, c.Close()) })
	}
	require.NoError(t, err)
	waitArrowLoadClusterReady(t, c)
	return c
}

// adjustArrowLoadCluster changes only the next CN generation's rollout
// settings. Callers close the current generation before adjustment and restart
// afterward, so an admitted statement always keeps the policy snapshot carried
// in its compiled external-scan payload.
func adjustArrowLoadCluster(c embed.Cluster, options arrowLoadClusterOptions) {
	c.ForeachServices(func(svc embed.ServiceOperator) bool {
		if svc.ServiceType() == metadata.ServiceType_CN {
			svc.Adjust(func(cfg *embed.ServiceConfig) {
				cfg.CN.Frontend.ArrowLoad.Enabled = options.enabled
				cfg.CN.Frontend.ArrowLoad.S3Enabled = options.s3Enabled
				cfg.CN.Frontend.ArrowLoad.DistributedEnabled = options.distributedEnabled
				cfg.CN.Frontend.ArrowLoad.ForceMaterialize = options.forceMaterialize
			})
		}
		return true
	})
}

// openArrowLoadDB opens a real MySQL-protocol connection (not the internal SQL
// executor) against the given CN, so statements run through the same frontend path
// a real client would use. This is required for KILL QUERY, multi-session isolation, and
// SHOW-PROCESSLIST-style observation to mean anything.
func openArrowLoadDB(t testing.TB, c embed.Cluster, cnIndex int) *sql.DB {
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

func mustExec(t testing.TB, db *sql.DB, stmt string, args ...any) {
	t.Helper()
	_, err := db.Exec(stmt, args...)
	require.NoError(t, err, stmt)
}

func queryCount(t testing.TB, db *sql.DB, query string, args ...any) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.QueryRow(query, args...).Scan(&n), query)
	return n
}

// waitUntilStatementRunning polls information_schema.processlist from a second
// connection until the target connection's current statement text contains
// needle. Callers use it only when the test fixture keeps the statement blocked
// at a deterministic lifecycle boundary before taking its next action.
func waitUntilStatementRunning(t testing.TB, observer *sql.DB, connID int64, needle string, deadline time.Duration) {
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
