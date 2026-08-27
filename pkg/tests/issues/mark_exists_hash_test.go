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

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestNullableExistsMarkKeepsHashEqualityAndBooleanResults(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()

		const database = "nullable_exists_mark"
		execSQLMaybe(t, ctx, db, "drop database if exists "+database)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+database)
		}()
		execSQLRequire(t, ctx, db, "create database "+database)
		execSQLRequire(t, ctx, db, "use "+database)
		execSQLRequire(t, ctx, db, "create table probe (id int)")
		execSQLRequire(t, ctx, db, "create table build_a (id int)")
		execSQLRequire(t, ctx, db, "create table build_b (id int)")
		execSQLRequire(t, ctx, db, "insert into probe values (null), (1), (2), (3)")
		execSQLRequire(t, ctx, db, "insert into build_a values (null), (2)")
		execSQLRequire(t, ctx, db, "insert into build_b values (null), (3)")

		const query = `select p.id,
			exists(select 1 from build_a a where a.id = p.id) as in_a,
			exists(select 1 from build_b b where b.id = p.id) as in_b,
			exists(select 1 from build_a a where a.id = p.id) or
				exists(select 1 from build_b b where b.id = p.id) as in_either,
			not exists(select 1 from build_a a where a.id = p.id) as not_in_a
			from probe p order by p.id`
		planText := explainSQL(t, ctx, db, "explain "+query)
		require.Contains(t, planText, "Join Type: MARK")
		require.NotContains(t, planText, "IS TRUE",
			"existential equality must remain visible to hash MARK lowering:\n%s", planText)

		rows, err := db.QueryContext(ctx, query)
		require.NoError(t, err)
		defer rows.Close()
		var got []string
		for rows.Next() {
			var id sql.NullInt64
			var inA, inB, inEither, notInA bool
			require.NoError(t, rows.Scan(&id, &inA, &inB, &inEither, &notInA))
			idText := "NULL"
			if id.Valid {
				idText = fmt.Sprint(id.Int64)
			}
			got = append(got, fmt.Sprintf("%s:%t:%t:%t:%t", idText, inA, inB, inEither, notInA))
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []string{
			"NULL:false:false:false:true",
			"1:false:false:false:true",
			"2:true:false:true:false",
			"3:false:true:true:true",
		}, got)
	})
}
