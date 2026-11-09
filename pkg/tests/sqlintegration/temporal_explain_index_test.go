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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// EXPLAIN and EXPLAIN ANALYZE must expose the same admitted access path for a
// selective temporal predicate; the original predicate remains the row oracle.
func TestTemporalExplainIndexParity(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		execWith := func(tb testing.TB, q string) {
			tb.Helper()
			_, err := conn.ExecContext(ctx, q)
			require.NoError(tb, err, q)
		}
		exec := func(q string) { execWith(t, q) }
		const schema = "temporal_explain_parity"
		exec("create database " + schema)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec("use " + schema)
		exec("set time_zone='+00:00'")
		sparseClock := time.Date(2026, time.April, 30, 23, 59, 59, 0, time.UTC).Unix()
		exec(fmt.Sprintf("set timestamp=%d", sparseClock))
		exec("create table events(id int primary key, d datetime(6), n bigint, key idx_d(d), key idx_n(n))")
		// Native layout statistics require more than 100,000 rows. Use the
		// first eligible cardinality and scatter dates across primary-key order,
		// so the index competes with overlapping base blocks.
		exec("insert into events select result, case when mod(result*8191,100001) < 99901 then date_add('2020-01-01 00:00:00', interval mod(result*8191,100001) second) else date_add(now(6), interval 1 day) end, mod(result*8191,100001) from generate_series(0,100000) g")
		var index string
		require.NoError(t, conn.QueryRowContext(ctx, "select index_table_name from mo_catalog.mo_indexes where name='idx_d' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='events') limit 1", schema).Scan(&index))
		exec("select mo_ctl('dn','flush','" + schema + ".events')")
		exec("select mo_ctl('dn','flush','" + schema + "." + index + "')")
		var eventsIntegerIndex string
		require.NoError(t, conn.QueryRowContext(ctx, "select index_table_name from mo_catalog.mo_indexes where name='idx_n' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='events') limit 1", schema).Scan(&eventsIntegerIndex))
		exec("select mo_ctl('dn','flush','" + schema + "." + eventsIntegerIndex + "')")
		exec("analyze table events(d,n) fullscan")
		query := "select id,d from events where d > now(6)"
		explain := func(prefix, query string) string {
			t.Helper()
			rows, err := conn.QueryContext(ctx, prefix+query)
			require.NoError(t, err)
			defer rows.Close()
			var out strings.Builder
			for rows.Next() {
				var line string
				require.NoError(t, rows.Scan(&line))
				out.WriteString(line)
				out.WriteByte('\n')
			}
			require.NoError(t, rows.Err())
			return out.String()
		}
		plain, analyzed := explain("explain ", query), explain("explain analyze ", query)
		require.Contains(t, analyzed, "Index Table Scan on events.idx_d", analyzed)
		require.Contains(t, plain, "Index Table Scan on events.idx_d", plain)
		readIDs := func(tb testing.TB, query func() (*sql.Rows, error)) map[int64]string {
			tb.Helper()
			rows, err := query()
			require.NoError(tb, err)
			defer rows.Close()
			columns, err := rows.Columns()
			require.NoError(tb, err)
			got := make(map[int64]string)
			for rows.Next() {
				var id int64
				var d string
				if len(columns) == 1 {
					require.NoError(tb, rows.Scan(&id))
				} else {
					require.NoError(tb, rows.Scan(&id, &d))
				}
				_, duplicate := got[id]
				require.False(tb, duplicate, "an index scan must not duplicate a row")
				got[id] = d
			}
			require.NoError(tb, rows.Err())
			return got
		}
		idsWith := func(tb testing.TB, query string) map[int64]string {
			tb.Helper()
			return readIDs(tb, func() (*sql.Rows, error) { return conn.QueryContext(ctx, query) })
		}
		ids := func(query string) map[int64]string { return idsWith(t, query) }
		// TIMESTAMP literals are folded in the session zone. A cached ordinary
		// or prepared plan must be rebound after that zone changes.
		exec("create table temporal_cache_events(id int primary key, d datetime(6), key idx_id_d(id,d))")
		exec("insert into temporal_cache_events values (1,'2025-12-31 23:30:00'), (2,'2026-01-01 01:00:00')")
		const cachedTemporal = "select id from temporal_cache_events where d < cast('2026-01-01 00:00:00' as timestamp(6))"
		t.Run("ordinary plan cache and time zone", func(t *testing.T) {
			for _, zone := range []string{"+00:00", "+08:00", "+00:00"} {
				execWith(t, "set time_zone='"+zone+"'")
				for range 2 {
					require.Equal(t, map[int64]string{1: ""}, idsWith(t, cachedTemporal), zone)
				}
			}
		})
		exec("set time_zone='+00:00'")
		_, err = conn.ExecContext(ctx, "set time_zone='not/a/zone'")
		require.Error(t, err)
		require.Equal(t, map[int64]string{1: ""}, ids(cachedTemporal))
		exec("set @temporal_cache_lower=0")
		for _, tc := range []struct{ name, statement, prepare, execute string }{
			{"without parameters", "temporal_cache_stmt", "prepare temporal_cache_stmt from 'select id from temporal_cache_events where d < cast(''2026-01-01 00:00:00'' as timestamp(6))'", "execute temporal_cache_stmt"},
			{"with parameters", "temporal_cache_param", "prepare temporal_cache_param from 'select id from temporal_cache_events where d < cast(''2026-01-01 00:00:00'' as timestamp(6)) and id > ?'", "execute temporal_cache_param using @temporal_cache_lower"},
		} {
			t.Run("prepared plan and time zone/"+tc.name, func(t *testing.T) {
				execWith(t, "set time_zone='+00:00'")
				execWith(t, tc.prepare)
				defer execWith(t, "deallocate prepare "+tc.statement)
				for _, zone := range []string{"+00:00", "+08:00", "+00:00"} {
					execWith(t, "set time_zone='"+zone+"'")
					for range 2 {
						require.Equal(t, map[int64]string{1: ""}, idsWith(t, tc.execute), zone)
					}
				}
				_, setErr := conn.ExecContext(ctx, "set time_zone='not/a/zone'")
				require.Error(t, setErr)
				require.Equal(t, map[int64]string{1: ""}, idsWith(t, tc.execute))
			})
		}
		// A different session can change the index definition without clearing
		// this session's cache. Schema versions must invalidate its old plan.
		require.Equal(t, map[int64]string{1: ""}, ids(cachedTemporal))
		require.Equal(t, map[int64]string{1: ""}, ids(cachedTemporal))
		version := func() uint32 {
			t.Helper()
			var v uint32
			require.NoError(t, conn.QueryRowContext(ctx, "select rel_version from mo_catalog.mo_tables where reldatabase='"+schema+"' and relname='temporal_cache_events'").Scan(&v))
			return v
		}
		ddlConn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer ddlConn.Close()
		_, err = ddlConn.ExecContext(ctx, "use "+schema)
		require.NoError(t, err)
		beforeVersion := version()
		_, err = ddlConn.ExecContext(ctx, "create index idx_temporal_d on temporal_cache_events(d)")
		require.NoError(t, err)
		indexVersion := version()
		require.Greater(t, indexVersion, beforeVersion)
		require.Equal(t, map[int64]string{1: ""}, ids(cachedTemporal))
		_, err = ddlConn.ExecContext(ctx, "drop index idx_temporal_d on temporal_cache_events")
		require.NoError(t, err)
		require.Greater(t, version(), indexVersion)
		require.Equal(t, map[int64]string{1: ""}, ids(cachedTemporal))
		expected := ids("select id,d from events ignore index(idx_d) where d > now(6)")
		require.Len(t, expected, 100)
		for id := range expected {
			require.GreaterOrEqual(t, (id*8191)%100001, int64(99901))
		}
		require.Equal(t, expected, ids(query))
		// The encoded key is needed by the residual even when only the PK is
		// projected. Exercise final remapping and actual index execution.
		const idOnly = "select id from events where d > now(6)"
		require.Contains(t, explain("explain analyze ", idOnly), "Index Table Scan on events.idx_d")
		require.Equal(t, ids("select id from events ignore index(idx_d) where d > now(6)"), ids(idOnly))

		// The same value distribution can need only one BASE block when dates
		// correlate with the PK. Object overlap alone cannot distinguish this
		// layout from events; AUTO must keep the cheaper physical scan.
		exec("create table ordered_events(id int primary key, d datetime(6), n bigint, key idx_d(d), key idx_n(n))")
		exec("insert into ordered_events select result, case when result < 99901 then date_add('2020-01-01 00:00:00', interval result second) else date_add(now(6), interval 1 day) end, result from generate_series(0,100000) g")
		var orderedIndex string
		require.NoError(t, conn.QueryRowContext(ctx, "select index_table_name from mo_catalog.mo_indexes where name='idx_d' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='ordered_events') limit 1", schema).Scan(&orderedIndex))
		exec("select mo_ctl('dn','flush','" + schema + ".ordered_events')")
		exec("select mo_ctl('dn','flush','" + schema + "." + orderedIndex + "')")
		var ordered_eventsIntegerIndex string
		require.NoError(t, conn.QueryRowContext(ctx, "select index_table_name from mo_catalog.mo_indexes where name='idx_n' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='ordered_events') limit 1", schema).Scan(&ordered_eventsIntegerIndex))
		exec("select mo_ctl('dn','flush','" + schema + "." + ordered_eventsIntegerIndex + "')")
		exec("analyze table ordered_events(d,n) fullscan")
		const orderedQuery = "select id,d from ordered_events where d > now(6)"
		orderedPlan := explain("explain analyze ", orderedQuery)
		require.Contains(t, orderedPlan, "Table Scan on "+schema+".ordered_events", orderedPlan)
		require.NotContains(t, orderedPlan, "Index Table Scan on ordered_events.idx_d", orderedPlan)
		orderedExpected := ids("select id,d from ordered_events ignore index(idx_d) where d > now(6)")
		require.Len(t, orderedExpected, 100)
		require.Equal(t, orderedExpected, ids(orderedQuery))
		for id := range orderedExpected {
			require.GreaterOrEqual(t, id, int64(99901))
		}

		// Ordinary integer costing uses the same cached-block observation. The
		// two layouts must return the complete independently derived row map.
		for _, table := range []string{"events", "ordered_events"} {
			t.Run("integer observed access/"+table, func(t *testing.T) {
				query := "select id,n from " + table + " where n >= 99901"
				want := make(map[int64]string)
				for id := int64(0); id <= 100000; id++ {
					n := id
					if table == "events" {
						n = id * 8191 % 100001
					}
					if n >= 99901 {
						want[id] = strconv.FormatInt(n, 10)
					}
				}
				require.Equal(t, want, idsWith(t, query))
				require.Equal(t, want, idsWith(t, "select id,n from "+table+" ignore index(idx_n) where n >= 99901"))
				physical := explain("explain analyze ", query)
				if table == "events" {
					require.Contains(t, physical, "Index Table Scan on events.idx_n", physical)
				} else {
					require.NotContains(t, physical, "Index Table Scan", physical)
					require.Contains(t, physical, "Table Scan on "+schema+".ordered_events", physical)
				}
			})
		}

		// A cast on the original DATETIME bound has not been proved safe for
		// eager evaluation. Keep this mixed-domain query on the ordinary path
		// and compare complete results against an explicit index-disabled scan.
		exec("create table reverse_events(id int primary key, ts timestamp(6), key idx_ts(ts))")
		exec("insert into reverse_events select id,cast(d as timestamp(6)) from events")
		var reverseIndex string
		require.NoError(t, conn.QueryRowContext(ctx, "select index_table_name from mo_catalog.mo_indexes where name='idx_ts' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='reverse_events') limit 1", schema).Scan(&reverseIndex))
		exec("select mo_ctl('dn','flush','" + schema + ".reverse_events')")
		exec("select mo_ctl('dn','flush','" + schema + "." + reverseIndex + "')")
		exec("analyze table reverse_events(ts) fullscan")
		const reverse = "select id,ts from reverse_events where ts > cast('2026-04-30 23:59:59' as datetime(6))"
		reversePlan := explain("explain analyze ", reverse)
		require.Contains(t, reversePlan, "Table Scan on "+schema+".reverse_events", reversePlan)
		require.NotContains(t, reversePlan, "Index Table Scan on reverse_events.idx_ts", reversePlan)
		reverseExpected := ids("select id,ts from reverse_events ignore index(idx_ts) where ts > cast('2026-04-30 23:59:59' as datetime(6))")
		require.Len(t, reverseExpected, 100)
		require.Equal(t, reverseExpected, ids(reverse))

		// The same dynamic SQL must remain eligible for a fresh cost decision
		// after a previously selective index choice becomes dense, and back.
		denseClock := time.Date(2019, time.January, 1, 0, 0, 0, 0, time.UTC).Unix()
		for _, clock := range []int64{sparseClock, denseClock, sparseClock} {
			exec(fmt.Sprintf("set timestamp=%d", clock))
			want := ids("select id,d from events ignore index(idx_d) where d > now(6)")
			require.Equal(t, want, ids(query))
			require.Equal(t, want, ids(query))
			path := explain("explain ", query)
			if clock == denseClock {
				require.Len(t, want, 100001)
				require.Contains(t, path, "Table Scan on "+schema+".events", path)
			} else {
				require.Len(t, want, 100)
				require.Contains(t, path, "Index Table Scan on events.idx_d", path)
			}
		}
		// A parameter-free prepared statement can be rebound after unrelated
		// DDL. It must remain executable and return fresh results on each side
		// of the selective-to-dense transition.
		exec(fmt.Sprintf("set timestamp=%d", sparseClock))
		exec("prepare temporal_prepared from 'select id,d from events where d > now(6)'")
		require.Equal(t, ids("select id,d from events ignore index(idx_d) where d > now(6)"), ids("execute temporal_prepared"))
		binaryPrepared, err := conn.PrepareContext(ctx, "select id,d from events where d > now(6) and id >= ?")
		require.NoError(t, err)
		defer binaryPrepared.Close()
		binaryIDs := func() map[int64]string {
			t.Helper()
			return readIDs(t, func() (*sql.Rows, error) { return binaryPrepared.QueryContext(ctx, int64(0)) })
		}
		require.Equal(t, ids("select id,d from events ignore index(idx_d) where d > now(6)"), binaryIDs())
		exec("create table unrelated_ddl(id int)")
		for _, clock := range []int64{sparseClock, denseClock, sparseClock} {
			exec(fmt.Sprintf("set timestamp=%d", clock))
			want := ids("select id,d from events ignore index(idx_d) where d > now(6)")
			require.Equal(t, want, ids("execute temporal_prepared"))
			require.Equal(t, want, ids("execute temporal_prepared"))
			require.Equal(t, want, binaryIDs(), "binary prepared execution must retain clock and schema behavior")
		}
		exec("deallocate prepare temporal_prepared")
		for _, zone := range []string{"+14:00", "-12:00", "America/New_York", "+00:00"} {
			exec("set time_zone='" + zone + "'")
			require.Equal(t, ids("select id,d from events ignore index(idx_d) where d > now(6)"), ids(query))
			if zone == "America/New_York" {
				path := explain("explain ", query)
				require.Contains(t, path, "Table Scan on "+schema+".events", path)
				require.NotContains(t, path, "Index Table Scan on events.idx_d", path)
			}
		}
		exec("analyze table events(d,n) fullscan")
		require.Equal(t, expected, ids(query))

		// Authorization is checked against the logical base table, including
		// when the explain plan contains its hidden covering-index relation.
		const role, user, password = "temporal_explain_reader", "temporal_explain_user", "TemporalTestPass"
		exec("create role " + role)
		defer cleanupSQLIntegration(t, cn, "drop user if exists "+user, "drop role if exists "+role)
		exec("create user " + user + " identified by '" + password + "' default role " + role)
		exec("grant connect on account * to " + role)
		exec("grant select on table " + schema + ".events to " + role)
		readerDB, err := sql.Open("mysql", fmt.Sprintf("sys#%s#%s:%s@tcp(127.0.0.1:%d)/", user, role, password, cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer readerDB.Close()
		reader, err := readerDB.Conn(ctx)
		require.NoError(t, err)
		defer reader.Close()
		_, err = reader.ExecContext(ctx, "set time_zone='+00:00'")
		require.NoError(t, err)
		_, err = reader.ExecContext(ctx, fmt.Sprintf("set timestamp=%d", sparseClock))
		require.NoError(t, err)
		func() {
			rows, err := reader.QueryContext(ctx, "explain select id,d from "+schema+".events where d > now(6)")
			require.NoError(t, err)
			defer rows.Close()
			var output strings.Builder
			for rows.Next() {
				var line string
				require.NoError(t, rows.Scan(&line))
				output.WriteString(line)
			}
			require.NoError(t, rows.Err())
			require.Contains(t, output.String(), "idx_d")
		}()
		readerPrepared, err := reader.PrepareContext(ctx, "select id,d from "+schema+".events where d > now(6) and id >= ?")
		require.NoError(t, err)
		defer readerPrepared.Close()
		require.Equal(t, expected, readIDs(t, func() (*sql.Rows, error) { return readerPrepared.QueryContext(ctx, int64(0)) }))
		exec("revoke select on table " + schema + ".events from " + role)
		rows, err := reader.QueryContext(ctx, "explain select id,d from "+schema+".events where d > now(6)")
		if rows != nil {
			defer rows.Close()
			if err == nil {
				require.False(t, rows.Next(), "an unauthorized user must not receive a plan row")
				err = rows.Err()
			}
		}
		require.Error(t, err, "an unauthorized user must not receive an explain plan")
		rows, err = readerPrepared.QueryContext(ctx, int64(0))
		if rows != nil {
			defer rows.Close()
			if err == nil {
				require.False(t, rows.Next(), "revocation must also reject an existing binary prepared handle")
				err = rows.Err()
			}
		}
		require.Error(t, err)
		exec("drop index idx_d on events")
		require.Equal(t, expected, ids(query), "a prior index choice must not survive index removal")
	})
}
