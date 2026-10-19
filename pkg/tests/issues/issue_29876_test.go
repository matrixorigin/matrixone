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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

// Workload and teardown execute SQL through the same finite operation policy.
// Teardown gets a fresh background context, independent of a failed workload.
const issue29876SQLOperationTimeout = 2 * time.Minute

func execIssue29876SQL(parent context.Context, db *sql.DB, statement string) (sql.Result, error) {
	ctx, cancel := context.WithTimeout(parent, issue29876SQLOperationTimeout)
	defer cancel()
	return db.ExecContext(ctx, statement)
}

func cleanupIssue29876Resource(db *sql.DB, statement, verification string, args ...any) error {
	started := time.Now()
	if _, err := execIssue29876SQL(context.Background(), db, statement); err != nil {
		return fmt.Errorf("cleanup %q after %s: %w", statement, time.Since(started), err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), issue29876SQLOperationTimeout)
	defer cancel()
	var remaining uint64
	if err := db.QueryRowContext(ctx, verification, args...).Scan(&remaining); err != nil {
		return fmt.Errorf("verify cleanup %q: %w", statement, err)
	}
	if remaining != 0 {
		return fmt.Errorf("cleanup %q left %d catalog records", statement, remaining)
	}
	return nil
}

func TestIssue29876CrossDatabaseCorrelation(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		exec := func(t *testing.T, q string) sql.Result {
			t.Helper()
			r, err := execIssue29876SQL(t.Context(), db, q)
			require.NoError(t, err)
			return r
		}
		main, stage := testutils.GetDatabaseName(t), testutils.GetDatabaseName(t)+"_stage"
		for _, name := range []string{main, stage} {
			exec(t, "create database "+name)
			defer func() {
				assert.NoError(t, cleanupIssue29876Resource(db, "drop database if exists "+name,
					"select count(*) from mo_catalog.mo_database where account_id = 0 and datname = ?", name))
			}()
		}
		target, source := main+".records", stage+".records"
		for _, table := range []string{target, source} {
			exec(t, "create table "+table+"(id int primary key,v int)")
		}
		reset := func(t *testing.T) {
			exec(t, "delete from "+target)
			exec(t, "insert into "+target+" values(1,10),(2,20),(3,30)")
		}
		qualified := "exists(select 1 from " + source + " where " + target + ".id=" + source + ".id)"
		for _, tc := range []struct {
			name, values       string
			matched, remaining []int64
		}{
			{"matching", "(1,99),(4,44)", []int64{1}, []int64{2, 3}},
			{"disjoint", "(99,99)", nil, []int64{1, 2, 3}},
			{"empty", "", nil, []int64{1, 2, 3}},
		} {
			exec(t, "delete from "+source)
			if tc.values != "" {
				exec(t, "insert into "+source+" values "+tc.values)
			}
			for _, shape := range []struct{ name, selectFrom, deleteFrom, predicate string }{
				{"qualified", target, "from " + target, qualified},
				{"aliases", target + " t", "t from " + target + " t", "exists(select 1 from " + source + " s where t.id=s.id)"},
				{"in", target, "from " + target, "id in(select id from " + source + ")"},
				{"grouped_qualified", target, "from " + target, "exists(select 1 from " + source + " group by " + source + ".id having " + target + ".id=" + source + ".id)"},
				{"grouped_aliases", target + " t", "t from " + target + " t", "exists(select 1 from " + source + " s group by s.id having t.id=s.id)"},
			} {
				t.Run(tc.name+"/"+shape.name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), issue29876SQLOperationTimeout)
					defer cancel()
					reset(t)
					assert.Equal(t, tc.matched, queryInt64Rows(t, ctx, db, "select id from "+shape.selectFrom+" where "+shape.predicate+" order by id"))
					r, err := db.ExecContext(ctx, "delete "+shape.deleteFrom+" where "+shape.predicate)
					require.NoError(t, err)
					affected, err := r.RowsAffected()
					require.NoError(t, err)
					assert.Equal(t, int64(len(tc.matched)), affected)
					assert.Equal(t, tc.remaining, queryInt64Rows(t, ctx, db, "select id from "+target+" order by id"))
				})
			}
		}
		// Bound this ordered reuse scenario independently of the shape matrix.
		ctx, cancel := context.WithTimeout(t.Context(), issue29876SQLOperationTimeout)
		defer cancel()
		// A real incremental package carries only changed rows, not a full reload.
		reset(t)
		exec(t, "insert into "+source+" values(1,99),(4,44)")
		exec(t, "begin")
		exec(t, "delete from "+target+" where "+qualified)
		exec(t, "insert into "+target+" select * from "+source)
		exec(t, "commit")
		assert.Equal(t, []int64{1, 2, 3, 4}, queryInt64Rows(t, ctx, db, "select id from "+target+" order by id"))
		assert.Equal(t, []int64{99, 20, 30, 44}, queryInt64Rows(t, ctx, db, "select v from "+target+" order by id"))
		// UPDATE and rollback consume the same correlation without losing rows.
		reset(t)
		exec(t, "update "+target+" set v=v+1 where "+qualified)
		assert.Equal(t, []int64{11, 20, 30}, queryInt64Rows(t, ctx, db, "select v from "+target+" order by id"))
		exec(t, "begin")
		exec(t, "delete from "+target+" where "+qualified)
		exec(t, "rollback")
		assert.Equal(t, []int64{1, 2, 3}, queryInt64Rows(t, ctx, db, "select id from "+target+" order by id"))
		// Reusing a prepared statement must re-evaluate changed staging data.
		stmt, err := db.PrepareContext(ctx, "select id from "+target+" where "+qualified+" order by id")
		require.NoError(t, err)
		defer stmt.Close()
		for _, id := range []int64{1, 2} {
			exec(t, "delete from "+source)
			exec(t, fmt.Sprintf("insert into %s values(%d,99)", source, id))
			func() {
				rows, err := stmt.QueryContext(ctx)
				require.NoError(t, err)
				defer rows.Close()
				var ids []int64
				for rows.Next() {
					var got int64
					require.NoError(t, rows.Scan(&got))
					ids = append(ids, got)
				}
				require.NoError(t, rows.Err())
				assert.Equal(t, []int64{id}, ids)
			}()
		}
		// A schema-less derived table cannot capture an explicit outer namespace.
		assert.Equal(t, []int64{2}, queryInt64Rows(t, ctx, db, "select id from "+target+" where exists(select 1 from (select id from "+source+") records where "+target+".id=records.id) order by id"))
		// Views expose their own database, independent of the body source.
		exec(t, "create view "+main+".record_view as select * from "+target)
		exec(t, "create view "+stage+".record_view as select * from "+source)
		assert.Equal(t, []int64{2}, queryInt64Rows(t, ctx, db, "select id from "+main+".record_view where exists(select 1 from "+stage+".record_view where "+main+".record_view.id="+stage+".record_view.id) order by id"))

		// This correlated scalar shape is unsupported; GROUP BY reuse must not
		// hide the correlation and silently return the inner column instead.
		_, err = db.ExecContext(ctx, "select (select "+target+".id from "+source+" group by "+source+".id) from "+target+" order by id")
		require.ErrorContains(t, err, "wrapped correlated scalar projection cannot be safely decorrelated")

		// Nullable equality must not turn NULL into a matching row.
		exec(t, "update "+source+" set v=null")
		assert.Empty(t, queryInt64Rows(t, ctx, db, "select id from "+target+" where exists(select 1 from "+source+" where "+target+".v="+source+".v)"))

		// Invalid qualifiers must not bind a same-named local source or delete rows.
		_, err = db.ExecContext(ctx, "delete from "+target+" where exists(select 1 from "+source+" where absent_db.records.id="+source+".id)")
		assert.Error(t, err)
		assert.Equal(t, []int64{1, 2, 3}, queryInt64Rows(t, ctx, db, "select id from "+target+" order by id"))
		_, err = db.ExecContext(ctx, "select absent_db.records.id from "+target+" group by "+target+".id")
		assert.Error(t, err, "GROUP BY reuse cannot validate an invalid namespace")

		t.Run("published_views", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), issue29876SQLOperationTimeout)
			defer cancel()
			account, publication := strings.ToLower(main)+"_acct", strings.ToLower(main)+"_pub"
			exec(t, "create account "+account+" admin_name 'admin' identified by '111'")
			t.Cleanup(func() {
				assert.NoError(t, cleanupIssue29876Resource(db, "drop account if exists "+account,
					"select count(*) from mo_catalog.mo_account where account_name = ?", account))
			})
			exec(t, "use "+main)
			for _, view := range []struct{ name, body string }{
				{"plain_v", "select id from records where id=2"},
				{"qualified_v", "select " + target + ".id from records where id=2"},
				{"nested_v", "select " + main + ".qualified_v.id from qualified_v"},
				{"having_v", "select " + target + ".id from records having " + target + ".id=2"},
			} {
				exec(t, "create view "+main+"."+view.name+" as "+view.body)
			}
			exec(t, "create publication "+publication+" database "+main+" account "+account)
			t.Cleanup(func() {
				assert.NoError(t, cleanupIssue29876Resource(db, "drop publication if exists "+publication,
					"select count(*) from mo_catalog.mo_pubs where account_id = 0 and pub_name = ?", publication))
			})
			sub, err := sql.Open("mysql", fmt.Sprintf("%s#admin#accountadmin:111@tcp(127.0.0.1:%d)/", account, cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			defer sub.Close()
			sub.SetMaxOpenConns(1)
			_, err = sub.ExecContext(ctx, "create database subscribed from sys publication "+publication)
			require.NoError(t, err)
			for _, view := range []string{"plain_v", "qualified_v", "nested_v", "having_v"} {
				t.Run(view, func(t *testing.T) {
					query := "select subscribed." + view + ".id from subscribed." + view
					assert.Equal(t, []int64{2}, queryInt64Rows(t, ctx, sub, query))
				})
			}
			_, err = sub.ExecContext(ctx, "select "+target+".id from subscribed.records")
			assert.Error(t, err, "publisher qualification remains invalid in subscriber SQL")
		})
	})
}
