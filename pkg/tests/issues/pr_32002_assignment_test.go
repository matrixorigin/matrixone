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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestPR32002ReviewBoundaries(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer func() { require.NoError(t, db.Close()) }()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer func() { require.NoError(t, conn.Close()) }()
		name := testutils.GetDatabaseName(t)
		_, err = conn.ExecContext(ctx, "create database `"+name+"`")
		require.NoError(t, err)
		defer func() {
			cleanup, done := context.WithTimeout(context.Background(), time.Minute)
			defer done()
			_, dropErr := conn.ExecContext(cleanup, "drop database if exists `"+name+"`")
			require.NoError(t, dropErr)
		}()
		_, err = conn.ExecContext(ctx, "use `"+name+"`")
		require.NoError(t, err)

		exec := func(query string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		warningCodes := func() []int {
			t.Helper()
			rows, err := conn.QueryContext(ctx, "show warnings")
			require.NoError(t, err)
			defer rows.Close()
			var codes []int
			for rows.Next() {
				var level, message string
				var code int
				require.NoError(t, rows.Scan(&level, &code, &message))
				codes = append(codes, code)
			}
			require.NoError(t, rows.Err())
			return codes
		}

		exec("set session sql_mode=''")
		exec("create table ints (s tinyint, u tinyint unsigned)")
		exec("insert into ints values ('-18446744073709551616','-18446744073709551616')")
		var signed, unsigned int
		require.NoError(t, conn.QueryRowContext(ctx, "select s,u from ints").Scan(&signed, &unsigned))
		require.Equal(t, -128, signed)
		require.Equal(t, 0, unsigned)
		exec("insert ignore into ints values ('3e2','3e2')")
		require.NoError(t, conn.QueryRowContext(ctx, "select s,u from ints order by s desc limit 1").Scan(&signed, &unsigned))
		require.Equal(t, 127, signed)
		require.Equal(t, 255, unsigned)
		exec("delete from ints")
		exec("insert into ints values ('300tail','-1')")
		require.Equal(t, []int{int(moerr.ER_WARN_DATA_OUT_OF_RANGE), int(moerr.ER_WARN_DATA_OUT_OF_RANGE)}, warningCodes())
		require.NoError(t, conn.QueryRowContext(ctx, "select s,u from ints").Scan(&signed, &unsigned))
		require.Equal(t, 127, signed)
		require.Equal(t, 0, unsigned)
		exec("delete from ints")
		exec("insert into ints values ('12tail','12tail')")
		require.Equal(t, []int{int(moerr.WARN_DATA_TRUNCATED), int(moerr.WARN_DATA_TRUNCATED)}, warningCodes())
		require.NoError(t, conn.QueryRowContext(ctx, "select s,u from ints").Scan(&signed, &unsigned))
		require.Equal(t, 12, signed)
		require.Equal(t, 12, unsigned)
		exec("delete from ints")

		exec("set session sql_mode='STRICT_TRANS_TABLES'")
		_, err = conn.ExecContext(ctx, "insert into ints values ('300tail',0)")
		require.Error(t, err)
		_, err = conn.ExecContext(ctx, "insert into ints values (0,'-1')")
		require.Error(t, err)
		var intCount int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from ints").Scan(&intCount))
		require.Zero(t, intCount)
		exec("insert ignore into ints values ('300tail','-1')")
		require.Equal(t, []int{int(moerr.ER_WARN_DATA_OUT_OF_RANGE), int(moerr.ER_WARN_DATA_OUT_OF_RANGE)}, warningCodes())
		require.NoError(t, conn.QueryRowContext(ctx, "select s,u from ints").Scan(&signed, &unsigned))
		require.Equal(t, 127, signed)
		require.Equal(t, 0, unsigned)
		exec("delete from ints")
		exec("insert ignore into ints values ('12tail','12tail')")
		require.Equal(t, []int{int(moerr.WARN_DATA_TRUNCATED), int(moerr.WARN_DATA_TRUNCATED)}, warningCodes())
		require.NoError(t, conn.QueryRowContext(ctx, "select s,u from ints").Scan(&signed, &unsigned))
		require.Equal(t, 12, signed)
		require.Equal(t, 12, unsigned)

		exec("create table dates (d date)")
		for _, value := range []string{"2026-10-21 10-20-30", "2026-10-21 23:59:59.9999999"} {
			_, err = conn.ExecContext(ctx, "insert into dates values ('"+value+"')")
			require.Error(t, err, value)
		}
		exec("insert into dates values ('2026-10-21 00:00:00.0000000')")
		var date string
		require.NoError(t, conn.QueryRowContext(ctx, "select d from dates").Scan(&date))
		require.Equal(t, "2026-10-21", date)

		exec("set session sql_mode=''")
		exec("create table decimals (d decimal(4,1))")
		exec("insert into decimals values (123.456)")
		require.Equal(t, []int{int(moerr.WARN_DATA_TRUNCATED)}, warningCodes())
		exec("insert into decimals values ('12.34567')")
		require.Equal(t, []int{int(moerr.WARN_DATA_TRUNCATED)}, warningCodes())
		exec("insert into decimals values ('12.30000')")
		require.Empty(t, warningCodes())
		rows, err := conn.QueryContext(ctx, "select d from decimals order by d")
		require.NoError(t, err)
		defer rows.Close()
		var values []string
		for rows.Next() {
			var value string
			require.NoError(t, rows.Scan(&value))
			values = append(values, value)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []string{"12.3", "12.3", "123.5"}, values)

		exec("set session sql_mode='STRICT_TRANS_TABLES'")
		exec("create table decimal_source (d decimal(12,2))")
		exec("create table decimal_wider (d decimal(14,2))")
		exec("insert into decimal_source values (-9999999999.99), (0.01), (null)")
		exec("insert into decimal_wider select d from decimal_source")
		require.Empty(t, warningCodes())
		var negative, positive, missing int
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(case when d = -9999999999.99 then 1 end), count(case when d = 0.01 then 1 end), count(case when d is null then 1 end) from decimal_wider").
			Scan(&negative, &positive, &missing))
		require.Equal(t, [3]int{1, 1, 1}, [3]int{negative, positive, missing})

		exec("create table decimal_less_integer_room (d decimal(14,6))")
		_, err = conn.ExecContext(ctx, "insert into decimal_less_integer_room select d from decimal_source")
		require.Error(t, err)
		exec("insert ignore into decimal_less_integer_room select d from decimal_source")
		require.Equal(t, []int{int(moerr.ER_WARN_DATA_OUT_OF_RANGE)}, warningCodes())
		var clamped int
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from decimal_less_integer_room where d = -99999999.999999").Scan(&clamped))
		require.Equal(t, 1, clamped)

		exec("create table median_input (g int, v decimal(38,0))")
		exec("insert into median_input values (1,1),(1,2),(2,3),(2,4)")
		exec("create table median_output (v decimal(40,2))")
		exec("insert into median_output select median(m) from (select median(v) as m from median_input group by g) grouped")
		var count int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from median_output where v = 2.50").Scan(&count))
		require.Equal(t, 1, count)
	})
}
