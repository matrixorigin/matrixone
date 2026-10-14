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
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestSetOperationSignedZero(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop database if exists set_signed_zero")
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		for _, q := range []string{
			"create database set_signed_zero", "use set_signed_zero",
			"create table zeros(id int primary key, f float, d double, n decimal(4,2))",
			"insert into zeros values (1,-0e0,-0e0,-0.00),(2,0e0,0e0,0.00),(3,-0e0,-0e0,-0.00),(4,0e0,0e0,0.00),(5,NULL,NULL,NULL),(6,1.25,1.25,1.25),(7,1.25,1.25,1.25)",
		} {
			_, err := conn.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		for _, col := range []string{"f", "d"} {
			for _, tc := range []struct {
				name, set string
				want      int
			}{
				{"union", "select %[1]s from zeros where id=1 union select %[1]s from zeros where id=2", 2},
				{"intersect", "select %[1]s from zeros where id=1 intersect select %[1]s from zeros where id=2", 0},
				{"except", "select %[1]s from zeros where id=1 except select %[1]s from zeros where id=2", 1},
				{"same-sign", "select %[1]s from zeros where id=1 union select %[1]s from zeros where id=3", 1},
				{"union-all", "select %[1]s from zeros where id=1 union all select %[1]s from zeros where id=2", 2},
				{"intersect-all-opposite", "select %[1]s from zeros where id in(1,3) intersect all select %[1]s from zeros where id in(2,4)", 0},
				{"except-all-opposite", "select %[1]s from zeros where id in(1,3) except all select %[1]s from zeros where id=2", 2},
				{"intersect-all-same", "select %[1]s from zeros where id in(1,3) intersect all select %[1]s from zeros where id=1", 1},
				{"except-all-same", "select %[1]s from zeros where id in(1,3) except all select %[1]s from zeros where id=1", 1},
				{"null-and-finite", "select %[1]s from zeros union select %[1]s from zeros", 4},
				{"parenthesized-self-union", "(select %[1]s from zeros) union (select %[1]s from zeros)", 4},
				{"empty", "select %[1]s from zeros where id<0 union select %[1]s from zeros where id<0", 0},
				{"reverse-except", "select %[1]s from zeros where id=2 except select %[1]s from zeros where id=1", 1},
				{"nested-chain", "select %[1]s from zeros where id=1 union select %[1]s from zeros where id=2 union select %[1]s from zeros where id=3", 2},
			} {
				t.Run(col+"/"+tc.name, func(t *testing.T) {
					q := "select count(*) from (" + fmt.Sprintf(tc.set, col) + ") s"
					if tc.name == "null-and-finite" {
						rows, err := conn.QueryContext(ctx, fmt.Sprintf("select %[1]s is null,cast(%[1]s as varchar) from (%[2]s) s order by %[1]s is null desc,serial(%[1]s)", col, fmt.Sprintf(tc.set, col)))
						require.NoError(t, err)
						require.Equal(t, []string{"1:NULL", "0:-0", "0:0", "0:1.25"}, readWindowNullRows(t, rows))
					}
					var count int
					require.NoError(t, conn.QueryRowContext(ctx, q).Scan(&count), q)
					require.Equal(t, tc.want, count, q)
				})
			}
			t.Run(col+"/representatives", func(t *testing.T) {
				q := fmt.Sprintf("select cast(x as varchar) from (select %[1]s x from zeros where id=1 union select %[1]s from zeros where id=2) s order by serial(x)", col)
				rows, err := conn.QueryContext(ctx, q)
				require.NoError(t, err)
				require.Equal(t, []string{"-0", "0"}, readWindowNullRows(t, rows))
			})
		}
		for _, tc := range []struct {
			name, q string
			want    int
		}{
			{"decimal", "select count(*) from (select n from zeros where id=1 union select n from zeros where id=2) s", 1},
			{"scalar", "select -0e0=0e0", 1},
			{"distinct", "select count(distinct d) from zeros where id<=4", 1},
			{"select-distinct", "select count(*) from (select distinct d from zeros where id<=4) s", 1},
			{"group", "select count(*) from (select d from zeros where id<=4 group by d) s", 1},
			{"literal-union", "select count(*) from (select -0e0 union select 0e0) s", 2},
			{"literal-intersect", "select count(*) from (select -0e0 intersect select 0e0) s", 0},
			{"literal-except", "select count(*) from (select -0e0 except select 0e0) s", 1},
			{"pruned-payload", "select count(*) from (select 1 k,d from zeros where id=1 union select 1,d from zeros where id=2) s", 2},
			{"pad-space-float", "select count(*) from (select cast('a' as char(4)) s,d from zeros where id=1 union select cast('a ' as varchar(4)),d from zeros where id=2) s", 2},
			{"pad-space-same-sign", "select count(*) from (select cast('a' as char(4)) s,d from zeros where id=1 union select cast('a ' as varchar(4)),d from zeros where id=3) s", 1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var count int
				require.NoError(t, conn.QueryRowContext(ctx, tc.q).Scan(&count), tc.q)
				require.Equal(t, tc.want, count, tc.q)
			})
		}
		t.Run("stored-signs", func(t *testing.T) {
			rows, err := conn.QueryContext(ctx, "select hex(serial(d)) from zeros where id<=2 order by id")
			require.NoError(t, err)
			require.Equal(t, []string{"217FFFFFFFFFFFFFFF", "218000000000000000"}, readWindowNullRows(t, rows))
			rows, err = conn.QueryContext(ctx, "select cast(x as varchar) from (select d x from zeros where id=1 except select d from zeros where id=2) s")
			require.NoError(t, err)
			require.Equal(t, []string{"-0"}, readWindowNullRows(t, rows))
		})
		t.Run("aggregate-controls", func(t *testing.T) {
			rows, err := conn.QueryContext(ctx, "select count(*),count(distinct d),approx_count_distinct(d),hll_cardinality(hll_add_agg(d)) from zeros where id<=4")
			require.NoError(t, err)
			require.Equal(t, []string{"4:1:1:1"}, readWindowNullRows(t, rows))
		})
		t.Run("prepared-reuse", func(t *testing.T) {
			stmt, err := conn.PrepareContext(ctx, "select count(*) from (select d from zeros where id=? union select d from zeros where id=?) s")
			require.NoError(t, err)
			defer stmt.Close()
			for _, tc := range []struct{ left, right, count int }{{1, 2, 2}, {1, 3, 1}, {5, 5, 1}, {1, 2, 2}} {
				var count int
				require.NoError(t, stmt.QueryRowContext(ctx, tc.left, tc.right).Scan(&count))
				require.Equal(t, tc.count, count)
			}
		})
		t.Run("prepared-domains", func(t *testing.T) {
			stmt, err := conn.PrepareContext(ctx, "select count(*) from (select ? x union select ?) s")
			require.NoError(t, err)
			defer stmt.Close()
			for _, tc := range []struct {
				left, right any
				count       int
			}{{math.Copysign(0, -1), float64(0), 2}, {int64(0), int64(0), 1}, {"0", "0", 1}, {nil, nil, 1}, {math.Copysign(0, -1), float64(0), 2}} {
				var count int
				require.NoError(t, stmt.QueryRowContext(ctx, tc.left, tc.right).Scan(&count))
				require.Equal(t, tc.count, count)
			}
		})
	})
}
