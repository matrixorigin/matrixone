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

package dml

import (
	"context"
	"database/sql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestUUIDSemantics(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		db := openRetestSQLDB(t, c)
		defer db.Close()
		execSQLDB(t, ctx, db, "create database uuid_semantics")
		defer cleanupTestDatabases(t, db, "uuid_semantics")
		execSQLDB(t, ctx, db, "use uuid_semantics")
		const u = "018f0000-0000-7000-8000-000000000001"
		const lit = "cast('" + u + "' as uuid)"
		execSQLDB(t, ctx, db, "create table t(id int primary key,u uuid,key idx_u(u))")
		execSQLDB(t, ctx, db, "insert into t values(1,'"+u+"'),(2,null)")
		for _, predicate := range []string{"u=null", "null=u", "u<null", "u between null and u", "u in(null)"} {
			var n int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from t where "+predicate).Scan(&n), predicate)
			require.Zero(t, n, predicate)
		}
		var n int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from t where u<=>null").Scan(&n))
		require.Equal(t, 1, n)
		stmt, err := db.PrepareContext(ctx, "select count(*) from t where u=?")
		require.NoError(t, err)
		defer stmt.Close()
		for _, value := range []any{u, nil, u} {
			want := 1
			if value == nil {
				want = 0
			}
			require.NoError(t, stmt.QueryRowContext(ctx, value).Scan(&n))
			require.Equal(t, want, n)
		}
		for _, q := range []string{
			"select 'fallback' union all select " + lit,
			"select " + lit + " union all select 'fallback'",
			"select 'fallback' union select " + lit + " union select null",
		} {
			func() {
				rows, err := db.QueryContext(ctx, q)
				require.NoError(t, err, q)
				defer rows.Close()
				found := false
				for rows.Next() {
					var v sql.NullString
					require.NoError(t, rows.Scan(&v))
					if v.Valid && v.String != "fallback" {
						require.Equal(t, u, v.String, q)
						found = true
					}
				}
				require.NoError(t, rows.Err())
				require.True(t, found, q)
			}()
		}
		for _, expr := range []string{"if(true," + lit + ",'fallback')", "case when true then " + lit + " else 'fallback' end", "coalesce(" + lit + ",'fallback')", "least(" + lit + ",'fallback')", "greatest('000'," + lit + ")"} {
			var v string
			require.NoError(t, db.QueryRowContext(ctx, "select "+expr).Scan(&v), expr)
			require.Equal(t, u, v, expr)
		}
		execSQLDB(t, ctx, db, "create table mixed as select 'fallback' as v union all select "+lit)
		var v string
		require.NoError(t, db.QueryRowContext(ctx, "select v from mixed where v<>'fallback'").Scan(&v))
		require.Equal(t, u, v)
		var width int
		require.NoError(t, db.QueryRowContext(ctx, "select character_maximum_length from information_schema.columns where table_schema='uuid_semantics' and table_name='mixed' and column_name='v'").Scan(&width))
		require.Equal(t, 36, width)
		func() {
			rows, err := db.QueryContext(ctx, "select v from mixed")
			require.NoError(t, err)
			defer rows.Close()
			columns, err := rows.ColumnTypes()
			require.NoError(t, err)
			require.Equal(t, "VARCHAR", columns[0].DatabaseTypeName())
			for rows.Next() {
				require.NoError(t, rows.Scan(&v))
			}
			require.NoError(t, rows.Err())
		}()
		execSQLDB(t, ctx, db, "create table native as select "+lit+" as v")
		require.NoError(t, db.QueryRowContext(ctx, "select data_type from information_schema.columns where table_schema='uuid_semantics' and table_name='native' and column_name='v'").Scan(&v))
		require.Equal(t, "uuid", v)
		codec, err := db.PrepareContext(ctx, "select bin_to_uuid(uuid_to_bin(?,?),?)")
		require.NoError(t, err)
		defer codec.Close()
		for _, flag := range []any{nil, "abc", "1tail", 0, 1} {
			require.NoError(t, codec.QueryRowContext(ctx, u, flag, flag).Scan(&v))
			require.Equal(t, u, v)
		}
		for _, q := range []string{"select uuid_to_bin('invalid',null)", "select bin_to_uuid('short',null)", "select bin_to_uuid(repeat('x',15),null)", "select bin_to_uuid(repeat('x',17),null)", "select " + lit + "='invalid'"} {
			require.Error(t, db.QueryRowContext(ctx, q).Scan(&v), q)
		}
		require.NoError(t, db.QueryRowContext(ctx, "select bin_to_uuid(uuid_to_bin('"+u+"',null),null)").Scan(&v))
		require.Equal(t, u, v)
	})
}
