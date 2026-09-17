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

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue28469IntegerAssignment(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		mustExec(t, ctx, conn, "create database issue_28469_assignment")
		defer func() { _, _ = conn.ExecContext(context.Background(), "drop database issue_28469_assignment") }()
		mustExec(t, ctx, conn, "use issue_28469_assignment")
		mustExec(t, ctx, conn, "create table dst(id int primary key,v bigint)")
		for _, tc := range []struct {
			name, value string
			want        int64
		}{
			{"exact_positive", "2.5", 3},
			{"exact_negative", "-2.5", -3},
			{"approximate_positive_even", "2.5E0", 2},
			{"approximate_negative_even", "-2.5E0", -2},
			{"approximate_positive_odd", "3.5E0", 4},
			{"approximate_negative_odd", "-3.5E0", -4},
		} {
			t.Run(tc.name, func(t *testing.T) {
				mustExec(t, ctx, conn, "delete from dst where id=1")
				mustExec(t, ctx, conn, "insert into dst values (1,"+tc.value+")")
				var got int64
				require.NoError(t, conn.QueryRowContext(ctx, "select v from dst").Scan(&got))
				require.Equal(t, tc.want, got)
				mustExec(t, ctx, conn, "insert into dst values (1,"+tc.value+") on duplicate key update v=values(v)")
				require.NoError(t, conn.QueryRowContext(ctx, "select v from dst").Scan(&got))
				require.Equal(t, tc.want, got)
			})
		}
		t.Run("sql_prepare", func(t *testing.T) {
			mustExec(t, ctx, conn, "prepare p from 'insert into dst values (1,?)'")
			defer func() { _, _ = conn.ExecContext(context.Background(), "deallocate prepare p") }()
			for _, tc := range []struct {
				value string
				want  int64
			}{
				{"2.5", 3}, {"cast(2.5 as double)", 2}, {"cast(-2.5 as decimal(5,1))", -3}, {"7", 7},
				{"cast(3.5 as double)", 4}, {"2.5", 3},
			} {
				t.Run(tc.value, func(t *testing.T) {
					mustExec(t, ctx, conn, "delete from dst where id=1")
					mustExec(t, ctx, conn, "set @v="+tc.value)
					mustExec(t, ctx, conn, "execute p using @v")
					var got int64
					require.NoError(t, conn.QueryRowContext(ctx, "select v from dst").Scan(&got))
					require.Equal(t, tc.want, got)
				})
			}
		})
		t.Run("binary_prepare", func(t *testing.T) {
			stmt, err := conn.PrepareContext(ctx, "insert into dst values (1,?)")
			require.NoError(t, err)
			defer stmt.Close()
			for _, tc := range []struct {
				value any
				want  int64
			}{
				{float64(2.5), 2}, {float64(-2.5), -2}, {float64(3.5), 4}, {int64(7), 7},
				{float64(2.5), 2}, {"7", 7},
			} {
				t.Run(fmt.Sprint(tc.value), func(t *testing.T) {
					mustExec(t, ctx, conn, "delete from dst where id=1")
					_, err := stmt.ExecContext(ctx, tc.value)
					require.NoError(t, err)
					var got int64
					require.NoError(t, conn.QueryRowContext(ctx, "select v from dst").Scan(&got))
					require.Equal(t, tc.want, got)
				})
			}
			mustExec(t, ctx, conn, "delete from dst where id=1")
			_, err = stmt.ExecContext(ctx, "2.5")
			require.Error(t, err)
			var count int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from dst").Scan(&count))
			require.Zero(t, count)
			_, err = stmt.ExecContext(ctx, float64(-2.5))
			require.NoError(t, err)
			var got int64
			require.NoError(t, conn.QueryRowContext(ctx, "select v from dst").Scan(&got))
			require.Equal(t, int64(-2), got)
		})
		t.Run("typed_assignment_controls", func(t *testing.T) {
			mustExec(t, ctx, conn, "delete from dst where id=1")
			mustExec(t, ctx, conn, "insert into dst select 1,cast(2.5 as decimal(5,1))")
			var got int64
			require.NoError(t, conn.QueryRowContext(ctx, "select v from dst").Scan(&got))
			require.Equal(t, int64(3), got)
			mustExec(t, ctx, conn, "update dst set v=cast(-2.5 as double)")
			require.NoError(t, conn.QueryRowContext(ctx, "select v from dst").Scan(&got))
			require.Equal(t, int64(-2), got)
			_, err := conn.ExecContext(ctx, "insert into dst values (2,2.5),(3,1E100)")
			require.Error(t, err)
			var count int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from dst").Scan(&count))
			require.Equal(t, 1, count)
		})
	})
}
