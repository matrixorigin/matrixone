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
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// One authenticated shared fixture covers persisted semantics and live reads.
// Values and wire column scales are independent oracles, not plan self-checks.
func TestMaterializedViewCapturedSemantics(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const schema = "mv_captured_semantics"
		views := []string{"precision_0", "precision_4", "precision_10", "precision_30", "mode_mv", "named_mv", "fixed_mv", "failure_mv"}
		cleanup := make([]string, 0, len(views)+3)
		cleanup = append(cleanup, "drop user if exists mv_semantics_reader", "drop role if exists mv_semantics_role")
		for _, v := range views {
			cleanup = append(cleanup, "drop materialized view if exists "+schema+"."+v)
		}
		cleanup = append(cleanup, "drop database if exists "+schema)
		defer func() { cleanupSQLIntegration(t, cn, cleanup...) }()
		exec := func(q string, args ...any) {
			t.Helper()
			_, err := conn.ExecContext(ctx, q, args...)
			require.NoError(t, err, q)
		}
		scalar := func(q string) string {
			t.Helper()
			var s string
			require.NoError(t, conn.QueryRowContext(ctx, q).Scan(&s), q)
			return s
		}
		reject := func(q, reason string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, q)
			require.ErrorContains(t, err, reason, q)
		}
		empty := func(q sqlQuerier) {
			t.Helper()
			rows, err := q.QueryContext(ctx, "select * from precision_10 limit 0")
			require.NoError(t, err)
			defer rows.Close()
			cols, err := rows.ColumnTypes()
			require.NoError(t, err)
			require.Len(t, cols, 1)
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		}
		exec("create database " + schema)
		exec("use " + schema)
		exec("set experimental_materialized_view=1")
		exec("set time_zone='+00:00'")
		exec("create table facts(a decimal(10,0))")
		exec("insert into facts values(1)")
		for _, precision := range []int{0, 4, 10, 30} {
			name := fmt.Sprintf("precision_%d", precision)
			exec(fmt.Sprintf("set div_precision_increment=%d", precision))
			exec("create materialized view " + name + " refresh manual as select a/3 as q from facts")
			reject("select * from "+name+" limit 0", "MV_NOT_POPULATED")
			reject("select * from "+name+" where false", "MV_NOT_POPULATED")
			exec("set div_precision_increment=4")
			exec("refresh materialized view " + name)
			rows, err := conn.QueryContext(ctx, "select q from "+name)
			require.NoError(t, err)
			func() {
				defer rows.Close()
				cols, err := rows.ColumnTypes()
				require.NoError(t, err)
				require.Len(t, cols, 1)
				_, scale, ok := cols[0].DecimalSize()
				require.True(t, ok)
				require.Equal(t, int64(precision), scale)
				require.True(t, rows.Next())
				var got string
				require.NoError(t, rows.Scan(&got))
				want := "0"
				if precision > 0 {
					want = "0." + strings.Repeat("3", precision)
				}
				require.Equal(t, want, got)
				require.False(t, rows.Next())
				require.NoError(t, rows.Err())
			}()
		}
		empty(conn)
		func() {
			rows, err := conn.QueryContext(ctx, "select * from facts limit 0")
			require.NoError(t, err)
			defer rows.Close()
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		}()
		exec("set sql_mode='NO_UNSIGNED_SUBTRACTION'")
		exec("create materialized view mode_mv refresh manual as select cast(a as unsigned)-2 as q from facts")
		exec("set sql_mode=''")
		exec("refresh materialized view mode_mv")
		require.Equal(t, "-1", scalar("select q from mode_mv"))
		exec("create table dates(id int,ts timestamp)")
		exec("insert into dates values(1,'2024-01-15 12:00:00'),(2,'2024-07-15 12:00:00')")
		exec("set time_zone='America/New_York'")
		exec("create materialized view named_mv refresh manual as select id,cast(ts as datetime) as local_dt from dates")
		exec("set time_zone='+08:00'")
		exec("create materialized view fixed_mv refresh manual as select id,cast(ts as datetime) as local_dt from dates")
		exec("set time_zone='+00:00'")
		exec("refresh materialized view named_mv")
		exec("refresh materialized view fixed_mv")
		require.Equal(t, "2024-01-15 07:00:00", scalar("select local_dt from named_mv where id=1"))
		require.Equal(t, "2024-07-15 08:00:00", scalar("select local_dt from named_mv where id=2"))
		require.Equal(t, "2024-01-15 20:00:00", scalar("select local_dt from fixed_mv where id=1"))
		require.Equal(t, "2024-07-15 20:00:00", scalar("select local_dt from fixed_mv where id=2"))
		exec("create role mv_semantics_role")
		exec("create user mv_semantics_reader identified by 'mv-fixture-only'")
		exec("grant select on table " + schema + ".precision_10 to mv_semantics_role")
		exec("grant mv_semantics_role to mv_semantics_reader")
		readerConfig := mysql.NewConfig()
		readerConfig.User, readerConfig.Passwd = "sys:mv_semantics_reader:mv_semantics_role", "mv-fixture-only"
		readerConfig.Net, readerConfig.Addr, readerConfig.DBName = "tcp", fmt.Sprintf("127.0.0.1:%d", port), schema
		readerConnector, err := mysql.NewConnector(readerConfig)
		require.NoError(t, err)
		reader := sql.OpenDB(readerConnector)
		defer reader.Close()
		reader.SetMaxOpenConns(1)
		func() {
			rows, err := reader.QueryContext(ctx, "select * from precision_10 limit 0")
			if rows != nil {
				defer rows.Close()
				require.False(t, rows.Next())
				require.NoError(t, rows.Err())
			}
			require.Error(t, err, "MV grant alone does not grant source SELECT")
		}()
		exec("grant select on table " + schema + ".facts to mv_semantics_role")
		empty(reader)
		p, err := reader.PrepareContext(ctx, "select * from precision_10 limit 0")
		require.NoError(t, err)
		defer p.Close()
		preparedEmpty := func() {
			t.Helper()
			rows, err := p.QueryContext(ctx)
			require.NoError(t, err)
			defer rows.Close()
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		}
		preparedEmpty()
		exec("revoke select on table " + schema + ".facts from mv_semantics_role")
		func() {
			rows, err := p.QueryContext(ctx)
			if rows != nil {
				defer rows.Close()
				require.False(t, rows.Next())
				require.NoError(t, rows.Err())
			}
			require.Error(t, err, "prepared LIMIT 0 must recheck live source grants")
		}()
		exec("grant select on table " + schema + ".facts to mv_semantics_role")
		preparedEmpty()
		exec("create table failure_source(id int,v bigint)")
		exec("insert into failure_source values(1,0)")
		exec("create materialized view failure_mv refresh manual as select id,v+9223372036854775807 as v from failure_source")
		exec("refresh materialized view failure_mv")
		before := scalar("select concat(last_run,':',successful_sequence,':',source_physical,':',source_logical) from mo_catalog.mo_materialized_views where database_name='" + schema + "' and name='failure_mv'")
		exec("update failure_source set v=1")
		_, err = conn.ExecContext(ctx, "refresh materialized view failure_mv")
		require.Error(t, err)
		require.Equal(t, "9223372036854775807", scalar("select v from failure_mv"))
		require.Equal(t, before, scalar("select concat(last_run,':',successful_sequence,':',source_physical,':',source_logical) from mo_catalog.mo_materialized_views where database_name='"+schema+"' and name='failure_mv'"))
		_, err = conn.ExecContext(ctx, "alter table facts add column unrelated int")
		require.Error(t, err, "dependency guard survives integration")
		old := scalar("select rel_id from mo_catalog.mo_tables where reldatabase='" + schema + "' and relname='precision_4'")
		exec("drop materialized view precision_4")
		require.Equal(t, "0", scalar("select count(*) from mo_catalog.mo_materialized_views where database_name='"+schema+"' and name='precision_4'"))
		exec("set div_precision_increment=10")
		exec("create materialized view precision_4 refresh manual as select a/3 as q from facts")
		exec("set div_precision_increment=0")
		exec("refresh materialized view precision_4")
		require.Equal(t, "0.3333333333", scalar("select q from precision_4"))
		require.NotEqual(t, old, scalar("select rel_id from mo_catalog.mo_tables where reldatabase='"+schema+"' and relname='precision_4'"))
		inventory, err := conn.QueryContext(ctx, "show table_number from mo_catalog")
		require.NoError(t, err)
		func() {
			defer inventory.Close()
			require.True(t, inventory.Next())
			var count int
			require.NoError(t, inventory.Scan(&count))
			require.Equal(t, 65, count, "main's 61 catalogs plus the four MV catalogs")
			require.False(t, inventory.Next())
			require.NoError(t, inventory.Err())
		}()

	})
}

// The existing database/sql types share this small interface; statements have a
// different call shape and are checked directly above.
type sqlQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}
