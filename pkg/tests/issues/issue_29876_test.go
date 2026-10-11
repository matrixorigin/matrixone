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
	"testing"
	"time"
)

func TestIssue29876CrossDatabaseCorrelation(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		exec := func(t *testing.T, q string) sql.Result {
			t.Helper()
			r, err := db.ExecContext(ctx, q)
			require.NoError(t, err)
			return r
		}
		main, stage := testutils.GetDatabaseName(t), testutils.GetDatabaseName(t)+"_stage"
		for _, name := range []string{main, stage} {
			exec(t, "create database "+name)
			defer func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, err := db.ExecContext(cleanup, "drop database if exists "+name)
				assert.NoError(t, err)
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
			} {
				t.Run(tc.name+"/"+shape.name, func(t *testing.T) {
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
		}
		// A schema-less derived table cannot capture an explicit outer namespace.
		assert.Equal(t, []int64{2}, queryInt64Rows(t, ctx, db, "select id from "+target+" where exists(select 1 from (select id from "+source+") records where "+target+".id=records.id) order by id"))
		// Views expose their own database, independent of the body source.
		exec(t, "create view "+main+".record_view as select * from "+target)
		exec(t, "create view "+stage+".record_view as select * from "+source)
		assert.Equal(t, []int64{2}, queryInt64Rows(t, ctx, db, "select id from "+main+".record_view where exists(select 1 from "+stage+".record_view where "+main+".record_view.id="+stage+".record_view.id) order by id"))

		// Nullable equality must not turn NULL into a matching row.
		exec(t, "update "+source+" set v=null")
		assert.Empty(t, queryInt64Rows(t, ctx, db, "select id from "+target+" where exists(select 1 from "+source+" where "+target+".v="+source+".v)"))

		// Invalid qualifiers must not bind a same-named local source or delete rows.
		_, err = db.ExecContext(ctx, "delete from "+target+" where exists(select 1 from "+source+" where absent_db.records.id="+source+".id)")
		assert.Error(t, err)
		assert.Equal(t, []int64{1, 2, 3}, queryInt64Rows(t, ctx, db, "select id from "+target+" order by id"))
	})
}
