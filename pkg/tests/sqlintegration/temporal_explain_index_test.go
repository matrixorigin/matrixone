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
		exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
		const schema = "temporal_explain_parity"
		exec("create database " + schema)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec("use " + schema)
		exec("set time_zone='+00:00'")
		exec("create table events(id int primary key, d datetime(6), key idx_d(d))")
		exec("insert into events select result, case when result < 99900 then date_add('2020-01-01 00:00:00', interval result second) else date_add(now(6), interval 1 day) end from generate_series(0,99999) g")
		var index string
		require.NoError(t, conn.QueryRowContext(ctx, "select index_table_name from mo_catalog.mo_indexes where name='idx_d' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='events') limit 1", schema).Scan(&index))
		exec("select mo_ctl('dn','flush','" + schema + ".events')")
		exec("select mo_ctl('dn','flush','" + schema + "." + index + "')")
		exec("analyze table events(d) fullscan")
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
		ids := func(query string) map[int64]bool {
			t.Helper()
			rows, err := conn.QueryContext(ctx, query)
			require.NoError(t, err)
			defer rows.Close()
			got := make(map[int64]bool)
			for rows.Next() {
				var id int64
				var d string
				require.NoError(t, rows.Scan(&id, &d))
				got[id] = true
			}
			require.NoError(t, rows.Err())
			return got
		}
		expected := ids("select id,d from events ignore index(idx_d) where d > now(6)")
		require.Len(t, expected, 100)
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
	})
}
