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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue31882ValuesSubqueryAndNull(t *testing.T) {
	embed.RunSingleCNBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		name := fmt.Sprintf("issue_31882_%d", time.Now().UnixNano())
		execSQLRequire(t, ctx, db, "create database "+name)
		defer execSQLMaybe(t, context.Background(), db, "drop database if exists "+name)
		table := name + ".t"
		execSQLRequire(t, ctx, db, "create table "+table+" (k int primary key, id int, a int)")
		for _, tc := range []struct {
			name, statement                    string
			firstID, firstA, secondID, secondA sql.NullInt64
		}{
			{"insert", "insert into %s values (1,1,(select 3)),(2,2,null)", sql.NullInt64{Int64: 1, Valid: true}, sql.NullInt64{Int64: 3, Valid: true}, sql.NullInt64{Int64: 2, Valid: true}, sql.NullInt64{}},
			{"insert-reversed", "insert into %s values (2,2,null),(1,1,(select 3))", sql.NullInt64{Int64: 1, Valid: true}, sql.NullInt64{Int64: 3, Valid: true}, sql.NullInt64{Int64: 2, Valid: true}, sql.NullInt64{}},
			{"null-subquery-row", "insert into %s values (1,null,(select 3)),(2,2,5)", sql.NullInt64{}, sql.NullInt64{Int64: 3, Valid: true}, sql.NullInt64{Int64: 2, Valid: true}, sql.NullInt64{Int64: 5, Valid: true}},
			{"null-subquery-row-last", "insert into %s values (2,2,5),(1,null,(select 3))", sql.NullInt64{}, sql.NullInt64{Int64: 3, Valid: true}, sql.NullInt64{Int64: 2, Valid: true}, sql.NullInt64{Int64: 5, Valid: true}},
			{"replace", "replace into %s values (1,1,(select 3)),(2,2,null)", sql.NullInt64{Int64: 1, Valid: true}, sql.NullInt64{Int64: 3, Valid: true}, sql.NullInt64{Int64: 2, Valid: true}, sql.NullInt64{}},
			{"replace-reversed", "replace into %s values (2,2,null),(1,1,(select 3))", sql.NullInt64{Int64: 1, Valid: true}, sql.NullInt64{Int64: 3, Valid: true}, sql.NullInt64{Int64: 2, Valid: true}, sql.NullInt64{}},
			{"replace-null-subquery-row", "replace into %s values (1,null,(select 3)),(2,2,5)", sql.NullInt64{}, sql.NullInt64{Int64: 3, Valid: true}, sql.NullInt64{Int64: 2, Valid: true}, sql.NullInt64{Int64: 5, Valid: true}},
			{"replace-null-subquery-row-last", "replace into %s values (2,2,5),(1,null,(select 3))", sql.NullInt64{}, sql.NullInt64{Int64: 3, Valid: true}, sql.NullInt64{Int64: 2, Valid: true}, sql.NullInt64{Int64: 5, Valid: true}},
			{"typed-control", "insert into %s values (1,1,(select 3)),(2,2,cast(null as signed))", sql.NullInt64{Int64: 1, Valid: true}, sql.NullInt64{Int64: 3, Valid: true}, sql.NullInt64{Int64: 2, Valid: true}, sql.NullInt64{}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				execSQLRequire(t, ctx, db, "truncate table "+table)
				execSQLRequire(t, ctx, db, fmt.Sprintf(tc.statement, table))
				rows, err := db.QueryContext(ctx, "select id,a from "+table+" order by k")
				require.NoError(t, err)
				defer rows.Close()
				var actual [][2]sql.NullInt64
				for rows.Next() {
					var id, a sql.NullInt64
					require.NoError(t, rows.Scan(&id, &a))
					actual = append(actual, [2]sql.NullInt64{id, a})
				}
				require.NoError(t, rows.Err())
				require.Equal(t, [][2]sql.NullInt64{{tc.firstID, tc.firstA}, {tc.secondID, tc.secondA}}, actual)
			})
		}

		execSQLRequire(t, ctx, db, "truncate table "+table)
		execSQLRequire(t, ctx, db, "insert into "+table+" values (1,null,(select 3))")
		var id sql.NullInt64
		var value int
		require.NoError(t, db.QueryRowContext(ctx, "select id,a from "+table+" where k=1").Scan(&id, &value))
		require.False(t, id.Valid)
		require.Equal(t, 3, value)

		execSQLRequire(t, ctx, db, "truncate table "+table)
		stmt, err := db.PrepareContext(ctx, "insert into "+table+" values (?,?,(select ?)),(?,?,?)")
		require.NoError(t, err)
		_, err = stmt.ExecContext(ctx, 1, 1, 3, 2, 2, nil)
		require.NoError(t, err)
		require.NoError(t, stmt.Close())
		var nullCount int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+table+" where k=2 and a is null").Scan(&nullCount))
		require.Equal(t, 1, nullCount)

		typedTable := name + ".typed"
		execSQLRequire(t, ctx, db, "create table "+typedTable+" (id int primary key, amount decimal(10,2), note varchar(25))")
		execSQLRequire(t, ctx, db, "insert into "+typedTable+" values (1,(select cast(3.50 as decimal(10,2))),'ok'),(2,null,null)")
		var matches int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+typedTable+" where (id=1 and amount=3.50 and note='ok') or (id=2 and amount is null and note is null)").Scan(&matches))
		require.Equal(t, 2, matches)

		vectorTable := name + ".vectors"
		execSQLRequire(t, ctx, db, "create table "+vectorTable+" (id int primary key, v vecf32(3))")
		execSQLRequire(t, ctx, db, "insert into "+vectorTable+" values (1,(select cast('[1,2,3]' as vecf32(3)))),(2,null)")
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+vectorTable+" where (id=1 and v is not null) or (id=2 and v is null)").Scan(&matches))
		require.Equal(t, 2, matches)

		notNullTable := name + ".required"
		execSQLRequire(t, ctx, db, "create table "+notNullTable+" (id int not null, a int)")
		_, err = db.ExecContext(ctx, "insert into "+notNullTable+" values (null,(select 3)),(2,5)")
		require.Error(t, err)
		require.NotContains(t, err.Error(), "panic")
		var count int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+notNullTable).Scan(&count))
		require.Zero(t, count)
	})
}
