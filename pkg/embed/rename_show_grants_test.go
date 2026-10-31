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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// TestRenameUserShowGrants uses the frontend SQL path and retains the original
// authenticated connection across a rename and reuse of its former name.
func TestRenameUserShowGrants(t *testing.T) {
	defer func() { require.NoError(t, CloseSingleCNBaseClusterTests()) }()
	RunSingleCNBaseClusterTests(t, func(cluster Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		adminDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer adminDB.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
		defer cancel()
		admin, err := adminDB.Conn(ctx)
		require.NoError(t, err)
		defer admin.Close()
		defer func() {
			cleanup, done := context.WithTimeout(context.Background(), time.Minute)
			defer done()
			_, _ = admin.ExecContext(cleanup, "drop user if exists sg30467_a, sg30467_b, sg30467_moved")
			_, _ = admin.ExecContext(cleanup, "drop role if exists sg30467_read, sg30467_write")
			_, _ = admin.ExecContext(cleanup, "drop database if exists sg30467_db")
		}()
		exec := func(query string) {
			t.Helper()
			_, err := admin.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		grants := func(conn *sql.Conn, query string) []string {
			t.Helper()
			rows, err := conn.QueryContext(ctx, query)
			require.NoError(t, err, query)
			defer rows.Close()
			var result []string
			for rows.Next() {
				var value string
				require.NoError(t, rows.Scan(&value))
				result = append(result, value)
			}
			require.NoError(t, rows.Err())
			sort.Strings(result)
			return result
		}
		exec("create database sg30467_db")
		exec("create table sg30467_db.t (v int)")
		exec("create role sg30467_read, sg30467_write")
		exec("grant connect on account * to sg30467_read, sg30467_write")
		exec("grant select on table sg30467_db.t to sg30467_read")
		exec("grant insert on table sg30467_db.t to sg30467_write")
		exec("create user sg30467_a identified by 'sg30467_password', sg30467_b identified by 'sg30467_password'")
		exec("grant sg30467_read to sg30467_a")
		exec("grant sg30467_write to sg30467_b")
		exec("set default role sg30467_read to sg30467_a")
		exec("set default role sg30467_write to sg30467_b")
		oldDB, err := sql.Open("mysql", fmt.Sprintf("sg30467_a:sg30467_password@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer oldDB.Close()
		old, err := oldDB.Conn(ctx)
		require.NoError(t, err)
		defer old.Close()
		var id uint32
		require.NoError(t, admin.QueryRowContext(ctx, "select user_id from mo_catalog.mo_user where user_name='sg30467_a'").Scan(&id))
		before := grants(admin, "show grants for 'sg30467_a'")
		require.NotEmpty(t, before)
		require.Equal(t, before, grants(old, "show grants"))
		var beforeID uint32
		require.NoError(t, old.QueryRowContext(ctx, "select current_user_id()").Scan(&beforeID))
		require.Equal(t, id, beforeID)
		exec("set session transaction_read_only = 1")
		_, writeErr := admin.ExecContext(ctx, "insert into sg30467_db.t values (1)")
		var readOnlyErr *mysql.MySQLError
		require.ErrorAs(t, writeErr, &readOnlyErr)
		require.Equal(t, uint16(1792), readOnlyErr.Number)
		exec("rename user sg30467_a to sg30467_moved")
		exec("set session transaction_read_only = 0")
		movedBeforeReuse := grants(admin, "show grants for 'sg30467_moved'")
		require.NotEmpty(t, movedBeforeReuse)
		for _, query := range []string{"show grants", "show grants for current_user", "show grants for current_user()"} {
			require.Equal(t, movedBeforeReuse, grants(old, query), query)
		}
		staleDB, err := sql.Open("mysql", fmt.Sprintf("sg30467_a:sg30467_password@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer staleDB.Close()
		var loginErr *mysql.MySQLError
		require.ErrorAs(t, staleDB.PingContext(ctx), &loginErr)
		require.Equal(t, uint16(1045), loginErr.Number)
		require.Equal(t, [5]byte{'2', '8', '0', '0', '0'}, loginErr.SQLState)
		require.NoError(t, staleDB.Close())
		exec("rename user sg30467_b to sg30467_a")
		var afterID uint32
		require.NoError(t, old.QueryRowContext(ctx, "select current_user_id()").Scan(&afterID))
		require.Equal(t, id, afterID)
		wantA := grants(admin, "show grants for 'sg30467_moved'")
		wantB := grants(admin, "show grants for 'sg30467_a'")
		require.NotEmpty(t, wantA)
		require.NotEmpty(t, wantB)
		require.Contains(t, strings.ToUpper(strings.Join(wantA, "\n")), "SELECT")
		require.Contains(t, strings.ToUpper(strings.Join(wantB, "\n")), "INSERT")
		require.NotEqual(t, wantA, wantB)
		for _, query := range []string{"show grants", "show grants for current_user", "show grants for current_user()"} {
			require.Equal(t, wantA, grants(old, query), query)
		}
		require.Equal(t, wantB, grants(old, "show grants for 'sg30467_a'"))
		require.Equal(t, wantA, grants(old, "show grants for 'sg30467_moved'"))
		require.Equal(t, wantA, grants(old, "show grants for 'sg30467_moved'@'%'"))
		require.Equal(t, grants(admin, "show grants for role sg30467_read"), grants(old, "show grants for role sg30467_read"))
	})
}
