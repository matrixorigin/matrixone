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
	"github.com/stretchr/testify/require"
)

func TestAlterPublicationExplicitTargetLockScope(t *testing.T) {
	const budget = 16
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2), embed.WithPreStart(func(s embed.ServiceOperator) {
		if s.ServiceType() == metadata.ServiceType_CN {
			s.Adjust(func(cfg *embed.ServiceConfig) {
				cfg.CN.LockService.MaxLockRowCount = budget
				cfg.CN.SQLAddress = fmt.Sprintf("127.0.0.1:%d", cfg.CN.Frontend.Port)
				cfg.CN.Frontend.SkipCheckUser = false
			})
		}
	}))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	open := func(index int) (*sql.DB, string) {
		t.Helper()
		cn, err := cluster.GetCNService(index)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		return db, cn.ServiceID()
	}
	left, leftID := open(0)
	right, rightID := open(1)
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
			alter := start(right, "alter publication "+pub+" account "+strings.Join(tc.targets, ","))
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
