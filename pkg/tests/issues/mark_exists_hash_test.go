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
		execSQLRequire(t, ctx, db, "create table invalid_hll (payload varchar(20))")
		execSQLRequire(t, ctx, db, "insert into probe values (null), (1), (2), (3)")
		execSQLRequire(t, ctx, db, "insert into build_a values (null), (2)")
		execSQLRequire(t, ctx, db, "insert into build_b values (null), (3)")
		execSQLRequire(t, ctx, db, "insert into invalid_hll values ('bad')")

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

		readIDs := func(query string) []int64 {
			rows, queryErr := db.QueryContext(ctx, query)
			require.NoError(t, queryErr)
			defer rows.Close()
			var ids []int64
			for rows.Next() {
				var id int64
				require.NoError(t, rows.Scan(&id))
				ids = append(ids, id)
			}
			require.NoError(t, rows.Err())
			return ids
		}
		requireQueryError := func(query string) {
			rows, queryErr := db.QueryContext(ctx, query)
			if queryErr == nil {
				func() {
					defer rows.Close()
					for rows.Next() {
					}
					queryErr = rows.Err()
				}()
			}
			require.Error(t, queryErr)
		}

		const filteringOr = `select p.id from probe p where
			exists(select 1 from build_a a where a.id = p.id) or
			exists(select 1 from build_b b where b.id = p.id)
			order by p.id`
		orPlan := explainSQL(t, ctx, db, "explain "+filteringOr)
		require.Contains(t, orPlan, "Union All")
		require.Contains(t, orPlan, "SEMI")
		require.Equal(t, []int64{2, 3}, readIDs(filteringOr))
		require.Equal(t, readIDs(`select p.id from probe p where p.id in (
			select id from build_a union all select id from build_b) order by p.id`),
			readIDs(filteringOr))

		const dnfQuery = `select distinct p.id from probe p join build_a a on
			(a.id = p.id and p.id = 2) or (a.id = p.id and p.id = 3)
			order by p.id`
		require.Equal(t, readIDs(`select distinct p.id from probe p join build_a a
			on a.id = p.id and (p.id = 2 or p.id = 3) order by p.id`), readIDs(dnfQuery))

		// Keep fallible predicates on the legacy plan shapes. These inputs make
		// the errors observable in the historical plans; the new hash-key and
		// UNION rewrites must neither suppress nor relocate them.
		const fallibleExists = `select p.id from probe p where p.id < 0 and exists (
			select 1 from invalid_hll h
			where hll_cardinality(cast(h.payload as varbinary)) = p.id)`
		fallibleExistsPlan := explainSQL(t, ctx, db, "explain "+fallibleExists)
		require.Contains(t, fallibleExistsPlan, "IS TRUE")
		requireQueryError(fallibleExists)

		const fallibleOrExists = `select p.id from probe p where p.id < 0 and (
			exists(select 1 from invalid_hll h1
				where hll_cardinality(cast(h1.payload as varbinary)) = p.id) or
			exists(select 1 from invalid_hll h2
				where hll_cardinality(cast(h2.payload as varbinary)) = p.id))`
		fallibleOrPlan := explainSQL(t, ctx, db, "explain "+fallibleOrExists)
		require.NotContains(t, fallibleOrPlan, "Union All")
		requireQueryError(fallibleOrExists)

		const fallibleDNF = `select p.id from probe p join invalid_hll h on
			(hll_cardinality(cast(h.payload as varbinary)) = p.id and p.id = 1) or
			(hll_cardinality(cast(h.payload as varbinary)) = p.id and p.id = 2)
			where p.id < 0`
		require.Empty(t, readIDs(fallibleDNF))
	})
}
