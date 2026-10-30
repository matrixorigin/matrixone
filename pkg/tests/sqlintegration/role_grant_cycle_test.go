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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// Two disjoint GRANTs must validate against the same committed role graph.
// The lock hook releases both statements at their first graph lock so the
// old read-before-lock path deterministically validates the stale graph.
func TestConcurrentRoleGrantCannotCommitCycle(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		runConcurrentRoleGrantCycle(t, cn, cn)
	})
}

func TestConcurrentRoleGrantCannotCommitCycleTwoCN(t *testing.T) {
	require.NoError(t, embed.CloseSingleCNBaseClusterTests())
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2))
	if cluster != nil {
		defer func() { require.NoError(t, cluster.Close()) }()
	}
	require.NoError(t, err)
	firstCN, err := cluster.GetCNService(0)
	require.NoError(t, err)
	secondCN, err := cluster.GetCNService(1)
	require.NoError(t, err)
	runConcurrentRoleGrantCycle(t, firstCN, secondCN)
}

func runConcurrentRoleGrantCycle(t *testing.T, firstCN, secondCN embed.ServiceOperator) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	firstPort := firstCN.GetServiceConfig().CN.Frontend.Port
	secondPort := secondCN.GetServiceConfig().CN.Frontend.Port
	root, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", firstPort))
	require.NoError(t, err)
	defer root.Close()
	require.NoError(t, testutils.WaitSystemBootstrap(ctx, root))
	const account = "role_grant_cycle_31677"
	defer cleanupSQLIntegration(t, firstCN, "drop account if exists "+account)
	_, err = root.ExecContext(ctx, "create account "+account+" admin_name 'root' identified by '111'")
	require.NoError(t, err)
	tenant0, err := sql.Open("mysql", fmt.Sprintf(account+"#root#accountadmin:111@tcp(127.0.0.1:%d)/", firstPort))
	require.NoError(t, err)
	defer tenant0.Close()
	tenant1, err := sql.Open("mysql", fmt.Sprintf(account+"#root#accountadmin:111@tcp(127.0.0.1:%d)/", secondPort))
	require.NoError(t, err)
	defer tenant1.Close()
	if firstCN.ServiceID() != secondCN.ServiceID() {
		require.Eventually(t, func() bool {
			pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			return tenant1.PingContext(pingCtx) == nil
		}, 30*time.Second, 100*time.Millisecond)
	}
	exec := func(q string) {
		t.Helper()
		_, err := tenant0.ExecContext(ctx, q)
		require.NoError(t, err, q)
	}
	exec("create role ra, rb, rc, rd")
	exec("grant rb to ra")
	exec("grant rd to rc")
	var graphTableID uint64
	require.NoError(t, tenant0.QueryRowContext(ctx,
		"select rel_id from mo_catalog.mo_tables where relname='mo_role_grant'").Scan(&graphTableID))

	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	var mu sync.Mutex
	seen := make(map[string]bool)
	var restoreHooks []func()
	installHook := func(cn embed.ServiceOperator) {
		if !moruntime.InTesting(cn.ServiceID()) {
			moruntime.SetupServiceRuntimeTestingContext(cn.ServiceID())
		}
		tc := moruntime.MustGetTestingContext(cn.ServiceID())
		before := tc.GetBeforeLockFunc()
		restoreHooks = append(restoreHooks, func() { tc.SetBeforeLockFunc(before) })
		tc.SetBeforeLockFunc(func(txnID []byte, tableID uint64) {
			before(txnID, tableID)
			if tableID != graphTableID {
				return
			}
			key := string(txnID)
			mu.Lock()
			first := !seen[key]
			seen[key] = true
			mu.Unlock()
			if first {
				arrived <- struct{}{}
				<-release
			}
		})
	}
	installHook(firstCN)
	if firstCN.ServiceID() != secondCN.ServiceID() {
		installHook(secondCN)
	}
	restored := false
	restore := func() {
		if !restored {
			for _, restoreHook := range restoreHooks {
				restoreHook()
			}
			restored = true
		}
	}
	defer restore()

	statements := [2]string{"grant rc to rb", "grant ra to rd"}
	tenantDBs := [2]*sql.DB{tenant0, tenant1}
	var results [2]error
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range statements {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, err := tenantDBs[i].Conn(ctx)
			if err != nil {
				results[i] = err
				return
			}
			defer conn.Close()
			<-start
			_, results[i] = conn.ExecContext(ctx, statements[i])
		}(i)
	}
	close(start)
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	for i := 0; i < 2; i++ {
		select {
		case <-arrived:
		case <-timer.C:
			close(release)
			wg.Wait()
			t.Fatal("both GRANT transactions did not reach the graph lock")
		case <-ctx.Done():
			close(release)
			wg.Wait()
			t.Fatal(ctx.Err())
		}
	}
	close(release)
	wg.Wait()
	restore()

	require.NotEqual(t, results[0] == nil, results[1] == nil,
		"exactly one cyclic GRANT must be rejected: %v", results)
	for _, result := range results {
		if result == nil {
			continue
		}
		var mysqlErr *mysql.MySQLError
		require.ErrorAs(t, result, &mysqlErr)
		require.Equal(t, moerr.ER_ROLE_GRANTED_TO_ITSELF, mysqlErr.Number)
	}
	rows, err := tenant0.QueryContext(ctx,
		"select child.role_name,parent.role_name from mo_catalog.mo_role_grant g "+
			"join mo_catalog.mo_role parent on g.granted_id=parent.role_id "+
			"join mo_catalog.mo_role child on g.grantee_id=child.role_id "+
			"where child.role_name in ('ra','rb','rc','rd') "+
			"and parent.role_name in ('ra','rb','rc','rd')")
	require.NoError(t, err)
	defer rows.Close()
	edges := map[string]bool{}
	for rows.Next() {
		var child, parent string
		require.NoError(t, rows.Scan(&child, &parent))
		edges[child+"->"+parent] = true
	}
	require.NoError(t, rows.Err())
	require.True(t, edges["ra->rb"])
	require.True(t, edges["rc->rd"])
	require.Len(t, edges, 3, "committed role graph: %v", edges)
	require.False(t, edges["ra->rb"] && edges["rb->rc"] &&
		edges["rc->rd"] && edges["rd->ra"], "committed role graph contains a cycle: %v", edges)
}
