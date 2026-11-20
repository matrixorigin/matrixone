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

package embed

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/require"
)

// TestRenameUserIdentity exercises public SQL against the task-owned cluster.
// Engine race/multi-CN/restart validation is separate from this single-CN BVT.
func TestRenameUserIdentity(t *testing.T) {
	defer func() { require.NoError(t, CloseBaseClusterTests()) }()
	RunBaseClusterTests(t, func(cluster Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
		defer cancel()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer func() {
			cleanup, c := context.WithTimeout(context.Background(), time.Minute)
			defer c()
			_, _ = conn.ExecContext(cleanup, "drop user if exists rename_src, rename_dst, rename_end, rename_taken")
			_, _ = conn.ExecContext(cleanup, "drop role if exists rename_role")
		}()
		exec := func(query string) { t.Helper(); _, err := conn.ExecContext(ctx, query); require.NoError(t, err, query) }
		count := func(query string, want int) {
			t.Helper()
			var n int
			require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&n))
			require.Equal(t, want, n, query)
		}
		exec("create role rename_role")
		exec("create user rename_src identified by 'RenameTestPassword1'")
		exec("create user rename_taken identified by 'RenameTestPassword1'")
		exec("grant rename_role to rename_src")
		var id int64
		var password string
		require.NoError(t, conn.QueryRowContext(ctx, "select user_id, authentication_string from mo_catalog.mo_user where user_name='rename_src'").Scan(&id, &password))
		_, err = conn.ExecContext(ctx, "rename user rename_src to rename_dst, rename_taken to rename_dst")
		require.Error(t, err)
		count("select count(*) from mo_catalog.mo_user where user_name='rename_src'", 1)
		count("select count(*) from mo_catalog.mo_user where user_name='rename_dst'", 0)
		const roleCollision = "rename user rename_src to rename_dst, rename_taken to rename_role"
		_, err = conn.ExecContext(ctx, roleCollision)
		require.ErrorContains(t, err, "RENAME USER destination is already a role")
		count(fmt.Sprintf("select count(*) from mo_catalog.mo_user where user_name='rename_src' and user_id=%d", id), 1)
		count("select count(*) from mo_catalog.mo_user where user_name='rename_taken'", 1)
		count("select count(*) from mo_catalog.mo_user where user_name in ('rename_dst','rename_role')", 0)
		exec("rename user rename_src to rename_dst, rename_dst to rename_end")
		var newID int64
		var newPassword string
		require.NoError(t, conn.QueryRowContext(ctx, "select user_id, authentication_string from mo_catalog.mo_user where user_name='rename_end'").Scan(&newID, &newPassword))
		require.Equal(t, id, newID)
		require.Equal(t, password, newPassword)
		count(fmt.Sprintf("select count(*) from mo_catalog.mo_user_grant g join mo_catalog.mo_role r on g.role_id=r.role_id where g.user_id=%d and r.role_name='rename_role'", id), 1)
		for _, query := range []string{"rename user rename_end to root", "rename user root to rename_src", "rename user rename_end@'remote' to rename_src", "rename user missing_user to rename_src", "rename user rename_end to rename_taken"} {
			_, err = conn.ExecContext(ctx, query)
			require.Error(t, err, query)
		}
		userDB, err := sql.Open("mysql", fmt.Sprintf("rename_end:RenameTestPassword1@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer userDB.Close()
		require.NoError(t, userDB.PingContext(ctx))
		_, err = userDB.ExecContext(ctx, "rename user rename_taken to rename_src")
		require.Error(t, err)
	})
}

func TestRenameUserSerializesRoleNameClaim(t *testing.T) {
	defer func() { require.NoError(t, CloseSingleCNBaseClusterTests()) }()
	RunSingleCNBaseClusterTests(t, func(cluster Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()

		_, err = db.ExecContext(ctx, "create user ns_claim_source identified by 'RenameTestPassword1'")
		require.NoError(t, err)
		var userID int32
		require.NoError(t, db.QueryRowContext(ctx, "select user_id from mo_catalog.mo_user where user_name='ns_claim_source'").Scan(&userID))

		lockService := lockservice.GetLockServiceByServiceID(cn.ServiceID())
		require.NotNil(t, lockService)
		waitForRowWaiter := func(tableName string, rowID int32) {
			t.Helper()
			var tableID uint64
			require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where account_id=0 and reldatabase='mo_catalog' and relname=?", tableName).Scan(&tableID))
			packer := types.NewPacker()
			packer.EncodeInt32(rowID)
			rowKey := bytes.Clone(packer.Bytes())
			packer.Close()
			require.Eventually(t, func() bool {
				found := false
				lockService.IterLocks(func(table uint64, keys [][]byte, held lockservice.Lock) bool {
					if table == tableID {
						for _, key := range keys {
							if bytes.Equal(key, rowKey) {
								held.IterWaiters(func(lock.WaitTxn) bool { found = true; return false })
								break
							}
						}
					}
					return !found
				})
				return found
			}, 10*time.Second, 20*time.Millisecond, "expected waiter on %s", tableName)
		}

		holder, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		var workers sync.WaitGroup
		defer func() {
			cancel()
			_ = holder.Rollback()
			workers.Wait()
		}()
		require.NoError(t, holder.QueryRowContext(ctx, "select user_id from mo_catalog.mo_user where user_id=? for update", userID).Scan(&userID))

		runDDL := func(stmt string) <-chan error {
			done := make(chan error, 1)
			workers.Go(func() {
				_, err := db.ExecContext(ctx, stmt)
				done <- err
			})
			return done
		}
		// Hold the rename after namespace admission and before it can update
		// the source row. A role creator must wait on the account row.
		rename := runDDL("rename user ns_claim_source to ns_claim_destination")
		waitForRowWaiter("mo_user", userID)
		createRole := runDDL("create role ns_claim_destination")
		waitForRowWaiter("mo_account", 0)
		require.NoError(t, holder.Rollback())
		require.NoError(t, <-rename)
		require.ErrorContains(t, <-createRole, "there is a user with the same name as the role")

		var actualID int32
		require.NoError(t, db.QueryRowContext(ctx, "select user_id from mo_catalog.mo_user where user_name='ns_claim_destination'").Scan(&actualID))
		require.Equal(t, userID, actualID)
		var roles int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_role where role_name='ns_claim_destination'").Scan(&roles))
		require.Zero(t, roles)
	})
}
