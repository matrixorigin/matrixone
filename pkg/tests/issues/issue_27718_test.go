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

package issues

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue27718ConcurrentSnapshotQuota(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 360*time.Second)
		defer cancel()

		cn0, err := c.GetCNService(0)
		require.NoError(t, err)
		cn1, err := c.GetCNService(1)
		require.NoError(t, err)
		cns := []embed.ServiceOperator{cn0, cn1}
		commitClients := make([]issue27718CommitClient, 0, len(cns))
		for _, cn := range cns {
			commitClients = append(commitClients, cn.RawService().(cnservice.Service).GetTxnClient())
		}
		ports := []int64{
			cn0.GetServiceConfig().CN.Frontend.Port,
			cn1.GetServiceConfig().CN.Frontend.Port,
		}

		sysDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", ports[0]))
		require.NoError(t, err)
		defer sysDB.Close()
		execSQLRequire(t, ctx, sysDB, "set role moadmin")

		const accountName = "issue_27718"
		defer func() {
			checkIssue27718RegistryRows(t, sysDB, "before account cleanup")
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, sysDB, "drop account if exists "+accountName)
			checkIssue27718RegistryRows(t, sysDB, "after account cleanup attempt")
		}()
		execSQLRequire(t, ctx, sysDB, "drop account if exists "+accountName)
		accountID := testutils.CreateAccount(t, c, accountName, "111")
		execSQLRequire(t, ctx, sysDB,
			"select mo_feature_registry_upsert('snapshot', 'Snapshot feature', '{\"allowed_scope\":[\"account\",\"database\",\"table\"]}', true)")

		tenantDB, err := sql.Open("mysql", fmt.Sprintf(
			"%s#root#accountadmin:111@tcp(127.0.0.1:%d)/", accountName, ports[0]))
		require.NoError(t, err)
		defer tenantDB.Close()
		execSQLRequire(t, ctx, tenantDB, "create database issue_27718_db")
		execSQLRequire(t, ctx, tenantDB, "create table issue_27718_db.t (id int primary key)")
		execSQLRequire(t, ctx, tenantDB, "insert into issue_27718_db.t values (1)")
		testutils.WaitDatabaseCreatedWithAccount(t, accountID, "issue_27718_db", cn1)
		testutils.WaitTableCreatedWithAccount(t, accountID, "issue_27718_db", "t", cn1)

		modes := []struct {
			name      string
			mode      pbtxn.TxnMode
			isolation pbtxn.TxnIsolation
		}{
			{name: "pessimistic_rc", mode: pbtxn.TxnMode_Pessimistic, isolation: pbtxn.TxnIsolation_RC},
			{name: "optimistic_si", mode: pbtxn.TxnMode_Optimistic, isolation: pbtxn.TxnIsolation_SI},
		}
		for _, mode := range modes {
			t.Run(mode.name, func(t *testing.T) {
				restore := setIssue27718TxnConfig(cns, mode.mode, mode.isolation)
				defer restore()
				if mode.mode == pbtxn.TxnMode_Optimistic {
					defer func() {
						// The shared clients were constructed with pessimistic
						// freshness settings. Changing the runtime mode does not
						// enable CN-based consistency. Drain completed optimistic
						// commits before parent cleanup opens a new RC transaction
						// in the independent sys session. Restore still runs if
						// this bounded barrier fails.
						fenceCtx, fenceCancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer fenceCancel()
						require.NoError(t, waitIssue27718Commits(fenceCtx, commitClients))
					}()
				}
				runIssue27718SnapshotQuotaMode(
					t, ctx, sysDB, tenantDB, accountName, accountID, ports, mode.name)
			})
		}
	})
}

type issue27718CommitClient interface {
	GetLatestCommitTS() timestamp.Timestamp
	WaitLogTailAppliedAt(context.Context, timestamp.Timestamp) (timestamp.Timestamp, error)
}

type issue27718CommitProbe struct {
	latest func() timestamp.Timestamp
	wait   func(context.Context, timestamp.Timestamp) (timestamp.Timestamp, error)
}

func (p issue27718CommitProbe) GetLatestCommitTS() timestamp.Timestamp { return p.latest() }
func (p issue27718CommitProbe) WaitLogTailAppliedAt(ctx context.Context, ts timestamp.Timestamp) (timestamp.Timestamp, error) {
	return p.wait(ctx, ts)
}

func TestIssue27718CommitFence(t *testing.T) {
	for _, failFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("first_wait_fails=%v", failFirst), func(t *testing.T) {
			var events []string
			frontier := timestamp.Timestamp{PhysicalTime: 9}
			waitErr := errors.New("apply failed")
			clients := make([]issue27718CommitClient, 0, 2)
			for i, committed := range []int64{9, 3} {
				clients = append(clients, issue27718CommitProbe{
					latest: func() timestamp.Timestamp {
						events = append(events, fmt.Sprintf("read%d", i))
						return timestamp.Timestamp{PhysicalTime: committed}
					},
					wait: func(_ context.Context, ts timestamp.Timestamp) (timestamp.Timestamp, error) {
						events = append(events, fmt.Sprintf("wait%d", i))
						require.Equal(t, frontier, ts)
						if failFirst && i == 0 {
							return timestamp.Timestamp{}, waitErr
						}
						return ts.Next(), nil
					},
				})
			}
			err := waitIssue27718Commits(t.Context(), clients)
			if failFirst {
				require.ErrorIs(t, err, waitErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, []string{"read0", "read1", "wait0", "wait1"}, events)
		})
	}
	t.Run("no commits", func(t *testing.T) {
		require.NoError(t, waitIssue27718Commits(t.Context(), []issue27718CommitClient{
			issue27718CommitProbe{latest: func() timestamp.Timestamp { return timestamp.Timestamp{} }},
		}))
	})
	t.Run("cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := waitIssue27718Commits(ctx, []issue27718CommitClient{issue27718CommitProbe{
			latest: func() timestamp.Timestamp { return timestamp.Timestamp{PhysicalTime: 1} },
			wait: func(ctx context.Context, _ timestamp.Timestamp) (timestamp.Timestamp, error) {
				return timestamp.Timestamp{}, ctx.Err()
			},
		}})
		require.ErrorIs(t, err, context.Canceled)
	})
}

func waitIssue27718Commits(ctx context.Context, clients []issue27718CommitClient) error {
	var frontier timestamp.Timestamp
	for _, client := range clients {
		if committed := client.GetLatestCommitTS(); committed.Greater(frontier) {
			frontier = committed
		}
	}
	if frontier.IsEmpty() {
		return nil
	}
	var err error
	for i, client := range clients {
		if _, waitErr := client.WaitLogTailAppliedAt(ctx, frontier); waitErr != nil {
			err = errors.Join(err, fmt.Errorf("CN %d apply completed optimistic commits: %w", i, waitErr))
		}
	}
	return err
}

// Observe the registry from separate statements without inserting a statement
// into DROP ACCOUNT's background transaction. In particular, do not flush or
// advance that transaction's workspace while diagnosing a visibility failure.
func checkIssue27718RegistryRows(t *testing.T, db *sql.DB, phase string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx,
		"select __mo_rowid from mo_catalog.mo_feature_registry where feature_code = 'SNAPSHOT' limit 3")
	if err != nil {
		t.Errorf("%s: registry probe: %v", phase, err)
		return
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Errorf("%s: scan registry row: %v", phase, err)
			return
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Errorf("%s: read registry rows: %v", phase, err)
		return
	}
	t.Logf("%s: SNAPSHOT registry row IDs: %v", phase, ids)
	if len(ids) != 1 {
		t.Errorf("%s: expected one SNAPSHOT registry row, got %d (probe limited to 3)", phase, len(ids))
	}
}

func setIssue27718TxnConfig(
	cns []embed.ServiceOperator,
	mode pbtxn.TxnMode,
	isolation pbtxn.TxnIsolation,
) func() {
	type previousTxnConfig struct {
		runtime      moruntime.Runtime
		mode         any
		hadMode      bool
		isolation    any
		hadIsolation bool
	}
	previous := make([]previousTxnConfig, 0, len(cns))
	for _, cn := range cns {
		rt := moruntime.ServiceRuntime(cn.ServiceID())
		oldMode, hadMode := rt.GetGlobalVariables(moruntime.TxnMode)
		oldIsolation, hadIsolation := rt.GetGlobalVariables(moruntime.TxnIsolation)
		previous = append(previous, previousTxnConfig{
			runtime: rt, mode: oldMode, hadMode: hadMode,
			isolation: oldIsolation, hadIsolation: hadIsolation,
		})
		rt.SetGlobalVariables(moruntime.TxnMode, mode)
		rt.SetGlobalVariables(moruntime.TxnIsolation, isolation)
	}
	return func() {
		for _, config := range previous {
			if config.hadMode {
				config.runtime.SetGlobalVariables(moruntime.TxnMode, config.mode)
			} else {
				config.runtime.SetGlobalVariables(moruntime.TxnMode, pbtxn.TxnMode_Pessimistic)
			}
			if config.hadIsolation {
				config.runtime.SetGlobalVariables(moruntime.TxnIsolation, config.isolation)
			} else {
				config.runtime.SetGlobalVariables(moruntime.TxnIsolation, pbtxn.TxnIsolation_RC)
			}
		}
	}
}

func runIssue27718SnapshotQuotaMode(
	t *testing.T,
	ctx context.Context,
	sysDB *sql.DB,
	tenantDB *sql.DB,
	accountName string,
	accountID int32,
	ports []int64,
	modeName string,
) {
	t.Helper()
	const creators = 4
	tenantDBs := make([]*sql.DB, len(ports))
	for i, port := range ports {
		db, err := sql.Open("mysql", fmt.Sprintf(
			"%s#root#accountadmin:111@tcp(127.0.0.1:%d)/", accountName, port))
		require.NoError(t, err)
		db.SetMaxOpenConns(creators/len(ports) + 1)
		tenantDBs[i] = db
		defer db.Close()
	}

	connections := make([]*sql.Conn, creators)
	for i := range connections {
		conn, err := tenantDBs[i%len(tenantDBs)].Conn(ctx)
		require.NoError(t, err)
		connections[i] = conn
		defer conn.Close()
		var probe int
		require.NoError(t, conn.QueryRowContext(ctx, "select 1").Scan(&probe))
		require.Equal(t, 1, probe)
	}

	runConcurrentCreates := func(prefix string, quota, expectedSuccesses int) []string {
		t.Helper()
		execSQLRequire(t, ctx, sysDB, fmt.Sprintf(
			"select mo_feature_limit_upsert(%d, 'snapshot', 'table', %d)", accountID, quota))

		type createResult struct {
			name string
			err  error
		}
		start := make(chan struct{})
		results := make(chan createResult, creators)
		createCtx, createCancel := context.WithTimeout(ctx, 30*time.Second)
		defer createCancel()
		for i := 1; i <= creators; i++ {
			name := fmt.Sprintf("%s_%d", prefix, i)
			conn := connections[i-1]
			go func() {
				<-start
				_, createErr := conn.ExecContext(createCtx, fmt.Sprintf(
					"create snapshot %s for table issue_27718_db t", name))
				results <- createResult{name: name, err: createErr}
			}()
		}
		close(start)

		// Collect every creator before assertions can unwind the mode and
		// capture its commit frontier. A timeout cancels the actual SQL too.
		completed := make([]createResult, 0, creators)
		for i := 0; i < creators; i++ {
			select {
			case result := <-results:
				completed = append(completed, result)
			case <-createCtx.Done():
				t.Fatal("concurrent snapshot creator did not finish")
			}
		}
		successes := make([]string, 0, creators)
		for _, result := range completed {
			if quota < 0 {
				require.NoError(t, result.err,
					"unlimited quota must admit snapshot %s", result.name)
				successes = append(successes, result.name)
				continue
			}
			if result.err == nil {
				successes = append(successes, result.name)
				continue
			}
			if quota == 0 {
				require.Contains(t, result.err.Error(),
					"feature SNAPSHOT with scope table has disabled for account "+accountName)
			} else {
				require.Contains(t, result.err.Error(), fmt.Sprintf(
					"feature SNAPSHOT with scope table has reached the limit of %d", quota))
			}
			require.NotContains(t, strings.ToLower(result.err.Error()), "txn need retry")
		}
		require.Len(t, successes, expectedSuccesses)

		var persisted int
		require.NoError(t, tenantDB.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_snapshots where account_name = ? and sname in (?, ?, ?, ?)",
			accountName, prefix+"_1", prefix+"_2", prefix+"_3", prefix+"_4").Scan(&persisted))
		require.Equal(t, expectedSuccesses, persisted)
		return successes
	}

	prefix := "issue27718_" + strings.ReplaceAll(modeName, "_", "")
	quotaOneSnapshots := runConcurrentCreates(prefix+"_q1", 1, 1)
	execSQLRequire(t, ctx, tenantDB, "drop snapshot "+quotaOneSnapshots[0])
	replacement := prefix + "_replacement"
	overLimit := prefix + "_over_limit"
	execSQLRequire(t, ctx, tenantDB, fmt.Sprintf(
		"create snapshot %s for table issue_27718_db t", replacement))
	_, err := tenantDB.ExecContext(ctx, fmt.Sprintf(
		"create snapshot %s for table issue_27718_db t", overLimit))
	require.Error(t, err)
	require.Contains(t, err.Error(), "feature SNAPSHOT with scope table has reached the limit of 1")
	var overLimitRows int
	require.NoError(t, tenantDB.QueryRowContext(ctx,
		"select count(*) from mo_catalog.mo_snapshots where account_name = ? and sname = ?",
		accountName, overLimit).Scan(&overLimitRows))
	require.Zero(t, overLimitRows)
	execSQLRequire(t, ctx, tenantDB, "drop snapshot "+replacement)

	quotaTwoSnapshots := runConcurrentCreates(prefix+"_q2", 2, 2)
	for _, name := range quotaTwoSnapshots {
		execSQLRequire(t, ctx, tenantDB, "drop snapshot "+name)
	}
	runConcurrentCreates(prefix+"_disabled", 0, 0)

	unlimitedSnapshots := runConcurrentCreates(prefix+"_unlimited", -1, creators)
	for _, name := range unlimitedSnapshots {
		execSQLRequire(t, ctx, tenantDB, "drop snapshot "+name)
	}
}
