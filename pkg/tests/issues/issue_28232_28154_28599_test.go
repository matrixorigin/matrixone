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
)

// TestIssueTemporalCompatibility exercises three temporal fixes through the
// public MySQL protocol with deterministic value and metadata assertions.
func TestIssueTemporalCompatibility(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(query string) {
			t.Helper()
			_, e := conn.ExecContext(ctx, query)
			require.NoError(t, e, query)
		}
		var text string
		var count int
		exec("drop database if exists qa_temporal_transfer")
		exec("create database qa_temporal_transfer")
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, "drop database if exists qa_temporal_transfer")
		}()
		exec("use qa_temporal_transfer")

		t.Run("28232_conditional_fractional_precision", func(t *testing.T) {
			exec("create table fsp(id int primary key, d date, dt datetime(6), tm time(6))")
			exec("insert into fsp values(1,'2024-01-01','2024-01-01 01:02:03.123456','12:34:56.123456'),(2,'2024-01-02','2024-01-02 02:03:04.654321','23:59:59.999999')")
			for _, q := range []string{
				"select microsecond(if(id=1,d,dt)) from fsp where id=2",
				"select microsecond(case when id=1 then d else dt end) from fsp where id=2",
				"select microsecond(ifnull(case when id=1 then d end,dt)) from fsp where id=2",
			} {
				require.NoError(t, conn.QueryRowContext(ctx, q).Scan(&count), q)
				require.Equal(t, 654321, count, q)
			}
			require.NoError(t, conn.QueryRowContext(ctx,
				"select microsecond(case when id=1 then dt else null end) from fsp where id=1").Scan(&count))
			require.Equal(t, 123456, count)
			stmt, e := conn.PrepareContext(ctx, "select microsecond(if(id=1,d,dt)) from fsp where id=?")
			require.NoError(t, e)
			defer stmt.Close()
			require.NoError(t, stmt.QueryRowContext(ctx, 2).Scan(&count))
			require.Equal(t, 654321, count)
			require.NoError(t, stmt.QueryRowContext(ctx, 1).Scan(&count))
			require.Zero(t, count)
			exec("create table fsp_copy as select if(id=1,d,dt) as if_v, case when id=1 then tm else dt end as case_v from fsp")
			rows, e := conn.QueryContext(ctx, "select datetime_precision from information_schema.columns where table_schema='qa_temporal_transfer' and table_name='fsp_copy' order by ordinal_position")
			require.NoError(t, e)
			defer rows.Close()
			for i := 0; i < 2; i++ {
				require.True(t, rows.Next())
				require.NoError(t, rows.Scan(&count))
				require.Equal(t, 6, count)
			}
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		})

		t.Run("28154_noop_update_timestamp", func(t *testing.T) {
			exec("create table noop(id int primary key, payload int not null, ts timestamp(6) default current_timestamp(6) on update current_timestamp(6))")
			exec("insert into noop(id,payload) values(1,10)")
			require.NoError(t, conn.QueryRowContext(ctx, "select cast(ts as char) from noop where id=1").Scan(&text))
			before := text
			time.Sleep(1100 * time.Millisecond)
			exec("update noop set payload=payload where id=1")
			require.NoError(t, conn.QueryRowContext(ctx, "select cast(ts as char) from noop where id=1").Scan(&text))
			require.Equal(t, before, text, "no-op UPDATE must not refresh auto timestamp")
			time.Sleep(1100 * time.Millisecond)
			exec("update noop set payload=11 where id=1")
			require.NoError(t, conn.QueryRowContext(ctx, "select cast(ts as char) from noop where id=1").Scan(&text))
			require.NotEqual(t, before, text, "real UPDATE must refresh auto timestamp")
			exec("begin")
			exec("update noop set payload=12 where id=1")
			exec("rollback")
			require.NoError(t, conn.QueryRowContext(ctx, "select payload from noop where id=1").Scan(&count))
			require.Equal(t, 11, count)
		})

		t.Run("28599_session_timestamp_override", func(t *testing.T) {
			exec("set session time_zone='+00:00'")
			exec("set session timestamp=1704067200.123456")
			var configured float64
			require.NoError(t, conn.QueryRowContext(ctx, "select @@session.timestamp").Scan(&configured))
			require.InDelta(t, 1704067200.123456, configured, 0.000001)
			for _, q := range []string{
				"select cast(now(6) as char)",
				"select cast(current_timestamp(6) as char)",
				"select cast(localtime(6) as char)",
				"select cast(localtimestamp(6) as char)",
				"select cast(utc_timestamp(6) as char)",
			} {
				require.NoError(t, conn.QueryRowContext(ctx, q).Scan(&text), q)
				require.Equal(t, "2024-01-01 00:00:00.123456", text, q)
			}
			require.NoError(t, conn.QueryRowContext(ctx, "select unix_timestamp()").Scan(&count))
			require.Equal(t, 1704067200, count)
			require.NoError(t, conn.QueryRowContext(ctx, "select year(sysdate())").Scan(&count))
			require.NotEqual(t, 2024, count, "SYSDATE must remain tied to the real clock")
			stmt, e := conn.PrepareContext(ctx, "select cast(now(6) as char)")
			require.NoError(t, e)
			defer stmt.Close()
			for i := 0; i < 2; i++ {
				require.NoError(t, stmt.QueryRowContext(ctx).Scan(&text))
				require.Equal(t, "2024-01-01 00:00:00.123456", text)
			}
			exec("create table override_t(id int primary key, created_at timestamp(6) default current_timestamp(6), updated_at timestamp(6) default current_timestamp(6) on update current_timestamp(6))")
			exec("insert into override_t(id) values(1)")
			require.NoError(t, conn.QueryRowContext(ctx, "select cast(created_at as char) from override_t where id=1").Scan(&text))
			require.Equal(t, "2024-01-01 00:00:00.123456", text)
			exec("set session timestamp=1704067260.5")
			exec("update override_t set id=2 where id=1")
			require.NoError(t, conn.QueryRowContext(ctx, "select cast(updated_at as char) from override_t where id=2").Scan(&text))
			require.Equal(t, "2024-01-01 00:01:00.500000", text)
			exec("set session timestamp=0")
			require.NoError(t, conn.QueryRowContext(ctx, "select year(now())").Scan(&count))
			require.NotEqual(t, 2024, count)
		})

		exec("drop database qa_temporal_transfer")
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_database where account_id=0 and datname='qa_temporal_transfer'").Scan(&count))
		require.Zero(t, count)
	})
}
