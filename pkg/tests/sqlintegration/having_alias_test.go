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

func TestHavingAliasCollision(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		defer cleanupSQLIntegration(t, cn, "drop database if exists having_alias_collision")
		for _, s := range []string{"create database having_alias_collision", "use having_alias_collision", "set session sql_mode='ONLY_FULL_GROUP_BY'", "create table g2(id int primary key,k int,v double,s varchar(20))", "insert into g2 values(1,NULL,10,'a'),(2,NULL,20,'b'),(3,1,30,NULL),(4,1,NULL,'c'),(5,2,40,'d'),(6,2,50,NULL)",
			"create table a(k int,v int)", "create table b(k int)",
			"insert into a values(1,10)", "insert into b values(2)"} {
			execSQLRequire(t, ctx, db, s)
		}
		for _, tc := range []struct {
			name, q string
			want    []string
		}{
			{"reported-query", "select k,sum(v) as s from g2 group by k having s=30", []string{"NULL:30", "1:30"}},
			{"renamed-control", "select k,sum(v) as sv from g2 group by k having sv=30", []string{"NULL:30", "1:30"}},
			{"explicit-control", "select k,sum(v) as s from g2 group by k having sum(v)=30", []string{"NULL:30", "1:30"}},
			{"arithmetic", "select k,sum(v) as s from g2 group by k having s*2>50", []string{"NULL:30", "1:30", "2:90"}},
			{"case", "select k,sum(v) as s from g2 group by k having case when s>50 then true else false end", []string{"2:90"}},
			{"scalar-alias", "select k,k+1 as s,sum(v) total from g2 group by k having s=2", []string{"1:2:30"}},
			{"grouped-source", "select k,sum(v) as k from g2 group by g2.k having k=1", []string{"1:30"}},
			{"unprojected-grouped-source", "select sum(v) as k from g2 group by k having k=1", []string{"30"}},
			{"aggregate-input-source", "select k,sum(v) as v from g2 group by k having count(v)=2", []string{"NULL:30", "2:90"}},
			{"self-alias-first", "select k,sum(v) as v from g2 group by k having v=30 and sum(v)=30", []string{"NULL:30", "1:30"}},
			{"source-aggregate-first", "select k,sum(v) as v from g2 group by k having sum(v)=30 and v=30", []string{"NULL:30", "1:30"}},
			{"computed-aggregate-alias", "select k,sum(v)+max(v) as s from g2 group by k having s=60", []string{"1:60"}},
			{"rollup-alias", "select k,sum(v) as s from g2 group by k with rollup having s>=90", []string{"2:90", "NULL:150"}},
			{"aggregate-without-group", "select sum(v) as s from g2 having s=150", []string{"150"}},
			{"empty-scalar-aggregate", "select sum(v) as s from g2 where false having s is null", []string{"NULL"}},
			{"all-null-group", "select k,sum(v) as s from g2 where k=1 and v is null group by k having s is null", []string{"1:NULL"}},
			{"correlated-group-source", "select sum(v) as k from g2 group by k having exists(select 1 where k=1)", []string{"30"}},
			{"correlated-qualified-control", "select sum(v) as k from g2 group by k having exists(select 1 where g2.k=1)", []string{"30"}},
			{"correlated-direct-output", "select k from g2 group by k having exists(select 1 where k=1)", []string{"1"}},
			{"local-subquery-shadows-group", "select sum(v) as k from g2 group by k having exists(select 1 from b where k=2)", []string{"30", "30", "90"}},
			{"using-group-left", "select sum(a.v) as k from a full outer join b using(k) group by k having k=1", []string{"10"}},
			{"using-group-right", "select sum(a.v) as k from a full outer join b using(k) group by k having k=2", []string{"NULL"}},
			{"using-group-explicit-control", "select sum(a.v) as k from a full outer join b using(k) group by k having coalesce(a.k,b.k)=1", []string{"10"}},
			{"using-compound-group-left", "select sum(a.v) as k from a full outer join b using(k) group by k+1 having k+1=2", []string{"10"}},
			{"using-compound-group-right", "select sum(a.v) as k from a full outer join b using(k) group by k+1 having k+1=3", []string{"NULL"}},
			{"using-chosen-arm-is-not-merged-group", "select sum(a.v) as k from a full outer join b using(k) group by a.k having k=10", []string{"10"}},
			{"subquery-source-scope", "select k,sum(v) as s from g2 group by k having s=30 and exists(select 1 from g2 z where s='a')", []string{"NULL:30", "1:30"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				rows, err := db.QueryContext(ctx, tc.q)
				require.NoError(t, err, tc.q)
				require.ElementsMatch(t, tc.want, readWindowNullRows(t, rows))
			})
		}
		t.Run("prepared-reuse", func(t *testing.T) {
			stmt, err := db.PrepareContext(ctx, "select k,sum(v) as s from g2 group by k having s=?")
			require.NoError(t, err)
			defer stmt.Close()
			for _, tc := range []struct {
				value int
				want  []string
			}{{30, []string{"NULL:30", "1:30"}}, {90, []string{"2:90"}}, {999, nil}, {30, []string{"NULL:30", "1:30"}}} {
				rows, err := stmt.QueryContext(ctx, tc.value)
				require.NoError(t, err)
				require.ElementsMatch(t, tc.want, readWindowNullRows(t, rows))
			}
		})
		t.Run("mode-off", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "set session sql_mode=''")
			rows, err := db.QueryContext(ctx, "select k,sum(v) as s from g2 group by k having s=30")
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"NULL:30", "1:30"}, readWindowNullRows(t, rows))
		})
	})
}
