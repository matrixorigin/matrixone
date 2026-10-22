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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestCTEEvaluationIdentity(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		defer cleanupSQLIntegration(t, cn, "drop database if exists cte_evaluation_identity")
		for _, q := range []string{"create database cte_evaluation_identity", "use cte_evaluation_identity",
			"create table t(k int)", "insert into t values(1),(2),(2),(null)",
			"create table special(e enum('b','a'),s set('b','a'))", "insert into special values('b','b'),('a','a')"} {
			execSQLRequire(t, ctx, db, q)
		}
		for _, tc := range []struct {
			name, query string
			want        int
		}{
			{"rand", "with r as(select rand()x) select count(*) from r a join r b on a.x=b.x", 1},
			{"uuid", "with r as(select uuid()x) select count(*) from r a join r b on a.x=b.x", 1},
			{"union", "with r as(select 1 k,rand()x union all select 2,rand()) select count(*) from r a join r b on a.k=b.k and a.x=b.x", 2},
			{"table bag and null", "with r as(select k,uuid()x from t) select count(*) from r a join r b on a.k<=>b.k and a.x=b.x", 4},
			{"special types", "with r as(select e,s,uuid()x from special) select count(*) from r a join r b on a.e=b.e and a.s=b.s and a.x=b.x", 2},
			{"special numeric", "with r as(select e,uuid()x from special) select sum(a.e+0) from r a join r b on a.x=b.x", 3},
			{"grouped", "with r as(select k,rand()x from t group by k) select count(*) from r a join r b on a.k<=>b.k and a.x=b.x", 3},
			{"recursive", "with recursive r(n,x) as(select 1,rand() union all select n+1,x from r where n<3) select count(*) from r a join r b on a.n=b.n and a.x=b.x", 3},
			{"recursive offset", "with recursive r(n,x) as(select 1,uuid() union all select n+1,x from r where n<5 limit 2 offset 1) select count(*) from r a join r b on a.n=b.n and a.x=b.x", 2},
			{"recursive distinct", "with recursive r(n,x) as(select 1,uuid() union distinct select n,x from r) select count(*) from r a join r b on a.x=b.x", 1},
			{"recursive predecessor", "with recursive r as(select uuid()x), seq(n,x) as(select 1,x from r union all select n+1,r.x from seq join r on seq.x=r.x where n<3) select count(*) from seq", 3},
			{"transitive outer", "with r as(select uuid()x),s as(select x from r) select count(*) from s a join s b on a.x=b.x join r c on a.x=c.x", 1},
			{"transitive inner", "with s as(with r as(select uuid()x) select x from r) select count(*) from s a join s b on a.x=b.x", 1},
			{"shadowed declarations", "with r as(select uuid()x) select count(*) from r a join (with r as(select uuid()x) select x from r) b on a.x<>b.x", 1},
			{"three readers", "with r as(select uuid()x) select count(*) from r a join r b on a.x=b.x join r c on b.x=c.x", 1},
			{"early consumers", "with r as(select k,uuid()x from t where k=1) select count(*) from (select x from r limit 1)a join (select x from r limit 1)b on a.x=b.x", 1},
			{"exists", "with r as(select uuid()x from t) select count(*) from r a where exists(select 1 from r b where b.x=a.x)", 4},
			{"empty", "with r as(select uuid()x from t where false) select count(*) from r a join r b on a.x=b.x", 0},
			{"statement stable control", "with r as(select now()x) select count(*) from r a join r b on a.x=b.x", 1},
			{"unused invalid", "with unused as(select missing_column from t) select 1", 1},
			{"unused recursive", "with recursive unused(n) as(select 1 union all select n+1 from unused) select 7", 7},
			{"correlated local", "select sum(n) from (select k,(with r as(select t.k,uuid()x) select count(*) from r a join r b on a.x=b.x and a.k<=>b.k) n from t)q", 4},
			{"correlated invocation generations", "select count(distinct x) from (select (with r as(select t.k k,uuid()x) select min(a.x) from r a join r b on a.x=b.x and a.k<=>b.k) x from t)q", 4},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var got int
				require.NoError(t, db.QueryRowContext(ctx, tc.query).Scan(&got), tc.query)
				require.Equal(t, tc.want, got, tc.query)
			})
		}
		t.Run("unsupported lifetime captures", func(t *testing.T) {
			for _, q := range []string{
				"with r as(select uuid()x) select (select max(x) from r group by t.k) from t",
				"select (with recursive base as(select uuid()x),r(n,x) as(select 1,x from base union all select n+1,base.x from r join base on r.x=base.x where n<t.k) select count(*) from r) from t",
			} {
				var unused int
				err := db.QueryRowContext(ctx, q).Scan(&unused)
				require.ErrorContains(t, err, "shared CTE capture across")
			}
		})
		t.Run("cached reader locking", func(t *testing.T) {
			for _, q := range []string{
				"with recursive r(n)as(select 1 union all select n+1 from r where n<3) select a.n from(select n from r)a join r b on a.n=b.n for update",
				"with r as(select k,uuid()x from t) select a.k from(select k from r)a join r b on a.k=b.k for update",
			} {
				var unused int
				err := db.QueryRowContext(ctx, q).Scan(&unused)
				require.ErrorContains(t, err, "FOR UPDATE on a materialized CTE")
			}
		})
		t.Run("visible schema", func(t *testing.T) {
			rows, err := db.QueryContext(ctx, "with r(key_col,identity_col)as(select k,uuid()from t) select a.* from r a join r b on a.identity_col=b.identity_col")
			require.NoError(t, err)
			defer rows.Close()
			columns, err := rows.Columns()
			require.NoError(t, err)
			require.Equal(t, []string{"key_col", "identity_col"}, columns)
			got := readWindowNullRows(t, rows)
			require.Len(t, got, 4, "private transport columns must not enter star output")
		})
		t.Run("hint cannot disable identity", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "set optimizer_hints='sharedComputation=1'")
			defer execSQLRequire(t, ctx, db, "set optimizer_hints=''")
			var n int
			require.NoError(t, db.QueryRowContext(ctx, "with r as(select uuid()x) select count(*) from r a join r b on a.x=b.x").Scan(&n))
			require.Equal(t, 1, n)
		})
		t.Run("prepared generation", func(t *testing.T) {
			stmt, err := db.PrepareContext(ctx, "with r as(select uuid()x) select a.x,b.x from r a join r b on a.x=b.x where ?")
			require.NoError(t, err)
			defer stmt.Close()
			var first string
			for _, enabled := range []bool{true, false, true} {
				var a, b string
				err := stmt.QueryRowContext(ctx, enabled).Scan(&a, &b)
				if !enabled {
					require.ErrorIs(t, err, sql.ErrNoRows)
					_, err = db.ExecContext(ctx, "select missing_column")
					require.Error(t, err)
					continue
				}
				require.NoError(t, err)
				require.Equal(t, a, b)
				if first == "" {
					first = a
				} else {
					require.NotEqual(t, first, a, "prepared executions must not reuse materialized rows")
				}
			}
		})
	})
}
