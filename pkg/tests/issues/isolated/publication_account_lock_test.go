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

package isolated

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

func startAccountLockCluster(t *testing.T, cnCount int, budget, maxSlice toml.ByteSize) embed.Cluster {
	t.Helper()
	cluster, err := embed.StartTestCluster(embed.WithCNCount(cnCount), embed.WithPreStart(func(s embed.ServiceOperator) {
		if s.ServiceType() == metadata.ServiceType_CN {
			s.Adjust(func(cfg *embed.ServiceConfig) {
				cfg.CN.LockService.MaxLockRowCount = budget
				cfg.CN.LockService.MaxFixedSliceSize = maxSlice
				cfg.CN.SQLAddress = fmt.Sprintf("127.0.0.1:%d", cfg.CN.Frontend.Port)
				cfg.CN.Frontend.SkipCheckUser = false
			})
		}
	}))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	return cluster
}

func openAccountLockSQL(t *testing.T, cluster embed.Cluster, index int) (*sql.DB, string) {
	t.Helper()
	cn, err := cluster.GetCNService(index)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db, cn.ServiceID()
}

func TestDropAccountWithMinimumRowLockCount(t *testing.T) {
	cluster := startAccountLockCluster(t, 1, 1, 0)
	db, _ := openAccountLockSQL(t, cluster, 0)
	ctx := t.Context()
	exec := func(stmt string) {
		t.Helper()
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	count := func(query string) int {
		t.Helper()
		var n int
		require.NoError(t, db.QueryRowContext(ctx, query).Scan(&n))
		return n
	}

	exec("create account direct_drop admin_name 'admin' identified by '111'")
	exec("drop account direct_drop")
	require.Zero(t, count("select count(*) from mo_catalog.mo_account where account_name='direct_drop'"))

	// Cluster restore drops accounts created after the snapshot inside its own transaction.
	exec("create database restore_scope")
	exec("create table restore_scope.t (id int primary key)")
	exec("insert into restore_scope.t values (1)")
	exec("create snapshot before_account for cluster")
	exec("create account restore_drop admin_name 'admin' identified by '111'")
	exec("update restore_scope.t set id=2 where id=1")
	exec("restore cluster {snapshot='before_account'}")
	require.Zero(t, count("select count(*) from mo_catalog.mo_account where account_name='restore_drop'"))
	require.Equal(t, 1, count("select id from restore_scope.t"))
}

func TestAlterPublicationExplicitTargetLockScope(t *testing.T) {
	const budget = 16
	cluster := startAccountLockCluster(t, 2, budget, 0)
	left, leftID := openAccountLockSQL(t, cluster, 0)
	right, rightID := openAccountLockSQL(t, cluster, 1)
	ctx := t.Context()
	exec := func(t *testing.T, stmt string) {
		t.Helper()
		_, err := left.ExecContext(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	number := func(t *testing.T, query string, args ...any) int {
		t.Helper()
		var n int
		require.NoError(t, left.QueryRowContext(ctx, query, args...).Scan(&n))
		return n
	}
	account := func(name string) {
		exec(t, "create account "+name+" admin_name 'admin' identified by '111'")
	}
	exec(t, "create database scope_db")
	targets := []string{"scope_first"}
	account(targets[0])
	account("scope_gap")
	for i := range budget {
		name := fmt.Sprintf("scope_target_%d", i)
		account(name)
		targets = append(targets, name)
	}
	account("scope_other")
	const accountID = "select account_id from mo_catalog.mo_account where account_name=?"
	gapID := number(t, accountID, "scope_gap")
	require.Less(t, number(t, accountID, targets[0]), gapID)
	require.Less(t, gapID, number(t, accountID, targets[budget]))

	for _, tc := range []struct {
		name      string
		targets   []string
		unrelated string
	}{
		{"wide", targets, "scope_gap"},
		{"singleton", targets[:1], "scope_other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pub := "scope_" + tc.name
			target := tc.targets[len(tc.targets)-1]
			deletedID := number(t, accountID, target)
			exec(t, "create publication "+pub+" database scope_db account scope_first")
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			var wg sync.WaitGroup
			defer wg.Wait()
			defer cancel()
			start := func(db *sql.DB, stmt string) <-chan error {
				result := make(chan error, 1)
				wg.Go(func() {
					_, err := db.ExecContext(ctx, stmt)
					result <- err
				})
				return result
			}
			// Hold ALTER at its publication write, after it validated the recipients.
			gate, err := left.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer gate.Rollback()
			var locked string
			require.NoError(t, gate.QueryRowContext(ctx, "select pub_name from mo_catalog.mo_pubs where account_id=0 and pub_name=? for update", pub).Scan(&locked))
			require.Equal(t, pub, locked)
			waiting := func(op <-chan error, cnID, table string) {
				t.Helper()
				waitCtx, stop := context.WithTimeout(ctx, 10*time.Second)
				defer stop()
				tick := time.NewTicker(30 * time.Millisecond)
				defer tick.Stop()
				for {
					var n int
					err := left.QueryRowContext(waitCtx, `select count(*) from mo_transactions() t where cn_id=? and table_id in
						(select cast(rel_id as char) from mo_catalog.mo_tables where account_id=0 and reldatabase='mo_catalog' and relname=?)`, cnID, table).Scan(&n)
					require.NoError(t, err)
					if n > 0 {
						return
					}
					select {
					case err := <-op:
						t.Fatalf("DDL returned %v before waiting on %s", err, table)
					case <-waitCtx.Done():
						t.Fatalf("%s did not wait on %s: %v", cnID, table, waitCtx.Err())
					case <-tick.C:
					}
				}
			}
			// An ordinary shared range that predates ALTER must disappear with its
			// holder. Otherwise ALTER can inherit it and block the gap account.
			var covering *sql.Tx
			if tc.name == "wide" {
				covering, err = left.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer covering.Rollback()
				rows, err := covering.QueryContext(ctx, "select account_id from mo_catalog.mo_account for share")
				require.NoError(t, err)
				defer rows.Close()
				var count int
				for rows.Next() {
					var id int
					require.NoError(t, rows.Scan(&id))
					count++
				}
				require.NoError(t, rows.Err())
				require.NoError(t, rows.Close())
				require.Greater(t, count, budget)
				require.Positive(t, number(t, `select count(*) from mo_locks() l where l.lock_key='range' and lower(l.lock_mode)='shared' and l.table_id in
					(select cast(rel_id as char) from mo_catalog.mo_tables where account_id=0 and reldatabase='mo_catalog' and relname='mo_account')`))
			}
			alter := start(right, "alter publication "+pub+" account "+strings.Join(tc.targets, ","))
			if covering != nil {
				waiting(alter, rightID, "mo_account")
				require.NoError(t, covering.Rollback())
			}
			waiting(alter, rightID, "mo_pubs")
			_, err = left.ExecContext(ctx, "drop account "+tc.unrelated)
			require.NoError(t, err, "unrelated DROP must commit before releasing ALTER")

			drop := start(left, "drop account "+target)
			waiting(drop, leftID, "mo_account")
			require.NoError(t, gate.Rollback())
			require.NoError(t, <-alter)
			require.NoError(t, <-drop)
			const grants = "select count(*) from mo_catalog.mo_subs where pub_account_id=0 and pub_name=?"
			require.Zero(t, number(t, grants+" and sub_account_id=?", pub, deletedID))
			require.Equal(t, len(tc.targets)-1, number(t, grants+" and status=0", pub))
		})
	}
}

func TestCreatePublicationBeyondLockSliceCapacity(t *testing.T) {
	const maxSlice = 128
	cluster := startAccountLockCluster(t, 2, 16, maxSlice)
	owner, _ := openAccountLockSQL(t, cluster, 0)
	publisher, _ := openAccountLockSQL(t, cluster, 1)
	ctx := t.Context()
	_, err := owner.ExecContext(ctx, "create database wide_db")
	require.NoError(t, err)
	names := make([]string, maxSlice+1)
	for i := range names {
		names[i] = fmt.Sprintf("recipient_%03d", i)
		_, err := owner.ExecContext(ctx, "create account "+names[i]+" admin_name 'admin' identified by '111'")
		require.NoError(t, err)
	}
	_, err = publisher.ExecContext(ctx, "create publication wide_pub database wide_db account "+strings.Join(names, ","))
	require.NoError(t, err)
	rows, err := owner.QueryContext(ctx, `select sub_account_name from mo_catalog.mo_subs where pub_account_id=0 and pub_name='wide_pub' and status=0 order by sub_account_name`)
	require.NoError(t, err)
	defer rows.Close()
	var granted []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		granted = append(granted, name)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, names, granted)
	// The other CN must be able to delete keys from both held-key segments.
	dropCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for _, name := range []string{names[0], names[maxSlice]} {
		_, err = owner.ExecContext(dropCtx, "drop account "+name)
		require.NoError(t, err)
	}
	var remaining int
	require.NoError(t, owner.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_subs where pub_account_id=0 and pub_name='wide_pub' and status=0").Scan(&remaining))
	require.Equal(t, len(names)-2, remaining)
}
