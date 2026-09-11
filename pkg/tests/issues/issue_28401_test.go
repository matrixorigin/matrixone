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
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestIssue28401SubstringIndexDecimalCount(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?timeout=5s&readTimeout=15s&writeTimeout=15s", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		require.NoError(t, db.PingContext(ctx))
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		for _, tc := range []struct {
			expression string
			want       string
		}{
			{"cast(0.499999999999999999 as decimal(18,18))", ""},
			{"cast(0.500000000000000000 as decimal(18,18))", "a"},
			{"-cast(0.500000000000000001 as decimal(18,18))", "d"},
			{"-cast(0.4999999999999999999999999999999999999 as decimal(38,37))", ""},
			{"cast(0.5000000000000000000000000000000000000 as decimal(38,37))", "a"},
			{"cast(1.5 as decimal(65,1))", "a,b"},
			{"cast(-1.5 as decimal(65,1))", "c,d"},
			{"cast(2.9 as double)", "a,b"},
		} {
			var got string
			require.NoError(t, conn.QueryRowContext(ctx,
				"select substring_index('a,b,c,d', ',', "+tc.expression+")").Scan(&got))
			require.Equal(t, tc.want, got, tc.expression)
		}
		database := testutils.GetDatabaseName(t)
		_, err = conn.ExecContext(ctx, "create database `"+database+"`")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, execIssue28401(
				context.Background(), conn, "drop database if exists `"+database+"`"))
		}()
		_, err = conn.ExecContext(ctx, "use `"+database+"`")
		require.NoError(t, err)

		func() {
			rows, err := conn.QueryContext(ctx, `select substring_index('a,b,c,d', ',', cast(1.4 as decimal(4,1))), substring_index('a,b,c,d', ',', cast(1.5 as decimal(4,1))), substring_index('a,b,c,d', ',', cast(-1.5 as decimal(4,1)))`)
			require.NoError(t, err)
			defer rows.Close()
			require.True(t, rows.Next())
			var a, b, result string
			require.NoError(t, rows.Scan(&a, &b, &result))
			require.Equal(t, "a", a)
			require.Equal(t, "a,b", b)
			require.Equal(t, "c,d", result)
			require.NoError(t, rows.Err())
		}()

		_, err = conn.ExecContext(ctx, "create table counts (n decimal(4,1), wide decimal(65,1))")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "insert into counts values (1.5, 1.5), (-1.5, -1.5)")
		require.NoError(t, err)
		func() {
			rows, err := conn.QueryContext(ctx,
				"select substring_index('a,b,c,d', ',', n), substring_index('a,b,c,d', ',', wide) "+
					"from counts order by n desc")
			require.NoError(t, err)
			defer rows.Close()
			var got []string
			for rows.Next() {
				var narrow, wide string
				require.NoError(t, rows.Scan(&narrow, &wide))
				got = append(got, narrow, wide)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, []string{"a,b", "a,b", "c,d", "c,d"}, got)
		}()

		require.NoError(t, execIssue28401(ctx, conn,
			`prepare stmt from "select substring_index('a,b,c,d', ',', ?)"`))
		defer func() {
			require.NoError(t, execIssue28401(context.Background(), conn, "deallocate prepare stmt"))
		}()
		for _, tc := range []struct {
			expression string
			want       string
		}{
			{"cast(1.5 as decimal(4,1))", "a,b"},
			{"cast(1.5 as decimal(65,1))", "a,b"},
			{"cast(-1.5 as decimal(65,1))", "c,d"},
		} {
			require.NoError(t, execIssue28401(ctx, conn, "set @v = "+tc.expression))
			var prepared string
			require.NoError(t, conn.QueryRowContext(ctx, "execute stmt using @v").Scan(&prepared))
			require.Equalf(t, tc.want, prepared, "prepared count expression: %s", tc.expression)
		}
	})
}

type issue28401Execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func execIssue28401(ctx context.Context, db issue28401Execer, query string) error {
	_, err := db.ExecContext(ctx, query)
	return err
}
