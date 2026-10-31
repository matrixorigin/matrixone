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
	"context"
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
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
