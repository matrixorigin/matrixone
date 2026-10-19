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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// One source row is enough to distinguish a data NULL from a rollup NULL.
// The public result must lose that internal distinction at the query boundary,
// while an explicitly selected GROUPING value remains an ordinary SQL column.
func TestGroupingSetQueryBlockIsolation(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop database if exists grouping_scope")
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		for _, statement := range []string{
			"create database grouping_scope", "use grouping_scope",
			"create table small(g int,h int,v int)", "insert into small values (1,NULL,10)",
		} {
			execSQLRequire(t, ctx, db, statement)
		}
		check := func(t *testing.T, query string, want []string) {
			t.Helper()
			rows, err := db.QueryContext(ctx, query)
			require.NoError(t, err, query)
			require.ElementsMatch(t, want, readWindowNullRows(t, rows), query)
		}
		const inner = "select g,h,sum(v) total,grouping(g,h) mask from small group by g,h with rollup"
		const plain = "select g,h,count(*) n,sum(total) total from (" + inner + ") r group by g,h"
		// Expected rows are enumerated from the three inner rows, independently of
		// the implementation. Matching a materialized table alone could hide bugs.
		const rollup = "select g,h,count(*) n,sum(total) total,grouping(g,h) mask from (" + inner + ") r group by g,h with rollup"
		for _, tc := range []struct {
			name, query string
			want        []string
		}{
			{"inner-mask", inner, []string{"1:NULL:10:0", "1:NULL:10:1", "NULL:NULL:10:3"}},
			{"outer-group", plain, []string{"1:NULL:2:20", "NULL:NULL:1:10"}},
			{"outer-rollup", rollup, []string{"1:NULL:2:20:0", "1:NULL:2:20:1", "NULL:NULL:1:10:0", "NULL:NULL:1:10:1", "NULL:NULL:3:30:3"}},
			{"distinct", "select distinct g,h from (" + inner + ") r", []string{"1:NULL", "NULL:NULL"}},
			{"union", "select g,h from (" + inner + ") a union select g,h from (" + inner + ") b", []string{"1:NULL", "NULL:NULL"}},
			{"intersect", "select g,h from (" + inner + ") a intersect select g,h from (" + inner + ") b", []string{"1:NULL", "NULL:NULL"}},
			{"except-control", "select g,h from (" + inner + ") a except select g,h from (" + inner + ") b", nil},
			{"explicit-mask", "select g,h,mask,count(*) from (" + inner + ") r group by g,h,mask", []string{"1:NULL:0:1", "1:NULL:1:1", "NULL:NULL:3:1"}},
			{"cte", "with r as (" + inner + ") select g,h,count(*),sum(total) from r group by g,h", []string{"1:NULL:2:20", "NULL:NULL:1:10"}},
			{"single-set-outer", "select g,h,count(*),grouping(g,h) from (" + inner + ") r group by grouping sets ((g,h))", []string{"1:NULL:2:0", "NULL:NULL:1:0"}},
			{"window-rewrite", "select g,h,count(*),sum(rn),min(mask),max(mask) from (select g,h,grouping(g,h) mask,row_number() over(order by grouping(g,h)) rn from small group by g,h with rollup) r group by g,h", []string{"1:NULL:2:3:0:1", "NULL:NULL:1:3:3:3"}},
			{"single-key-mask", "select g,sum(total),grouping(g) from (select g,sum(v) total from small group by g with rollup) r group by g with rollup", []string{"1:10:0", "NULL:10:0", "NULL:20:1"}},
			{"same-block-hidden-order", "select g,h,sum(v) from small group by g,h with rollup having grouping(g,h)<>3 order by grouping(g,h),g,h limit 1", []string{"1:NULL:10"}},
			{"outer-filter-null", "select g,h,total,mask from (" + inner + ") r where g is null", []string{"NULL:NULL:10:3"}},
			{"outer-filter-not-null", "select g,h,total,mask from (" + inner + ") r where g is not null", []string{"1:NULL:10:0", "1:NULL:10:1"}},
			{"outer-having-mask", "select g,h,count(*),grouping(g,h) from (" + inner + ") r group by g,h with rollup having grouping(g,h)=0", []string{"1:NULL:2:0", "NULL:NULL:1:0"}},

			{"join-remapped-key-control", "select r.g,d.h,count(*),sum(total) from (" + inner + ") r left join small d on r.h <=> d.h group by r.g,d.h", []string{"1:NULL:2:20", "NULL:NULL:1:10"}},
			{"join-inherited-key", "select r.g,r.h,count(*),sum(total) from (" + inner + ") r left join small d on r.h <=> d.h group by r.g,r.h", []string{"1:NULL:2:20", "NULL:NULL:1:10"}},
		} {
			t.Run(tc.name, func(t *testing.T) { check(t, tc.query, tc.want) })
		}
		t.Run("static-computed-outputs", func(t *testing.T) {
			rt := runtime.ServiceRuntime(cn.ServiceID())
			old, existed := rt.GetGlobalVariables("optimizer_hints")
			rt.SetGlobalVariables("optimizer_hints", "sharedComputation=1")
			defer func() {
				if existed {
					rt.SetGlobalVariables("optimizer_hints", old)
				} else {
					rt.CompareAndDeleteGlobalVariables("optimizer_hints", "sharedComputation=1")
				}
			}()
			for _, tc := range []struct {
				name, query string
				want        []string
			}{
				{"filter", "select k,total from (select g+1 k,sum(v) total from small group by g with rollup) r where k=2", []string{"2:10"}},
				{"null-filter", "select k,total from (select g+1 k,sum(v) total from small group by g with rollup) r where k is null", []string{"NULL:10"}},
				{"mixed-keys", "select k,total from (select g+coalesce(h,0) k,sum(v) total from (select g,h,v from small union all select 1,2,20) s group by g,h with rollup) r where k=1", []string{"1:10", "1:30"}},
				{"constant-null-filter", "select * from (select NULL k,count(*) n from small group by g with rollup) r where k is not null", nil},
			} {
				t.Run(tc.name, func(t *testing.T) { check(t, tc.query, tc.want) })
			}
		})
		for _, family := range []struct {
			name, clause string
			nullCount    int
		}{
			{"cube", "cube(g,h)", 2}, {"sets", "grouping sets ((g,h),(g),())", 1},
		} {
			t.Run(family.name, func(t *testing.T) {
				body := "select g,h,sum(v) total,grouping(g,h) mask from small group by " + family.clause
				check(t, "select g,h,count(*),sum(total) from ("+body+") r group by g,h", []string{"1:NULL:2:20", fmt.Sprintf("NULL:NULL:%d:%d", family.nullCount, 10*family.nullCount)})
				check(t, "select distinct g,h from ("+body+") r", []string{"1:NULL", "NULL:NULL"})
			})
		}
		t.Run("view", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "create view rolled as "+inner)
			check(t, "select g,h,count(*),sum(total) from rolled group by g,h", []string{"1:NULL:2:20", "NULL:NULL:1:10"})
		})
		t.Run("ctas", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "create table persisted as "+plain)
			check(t, "select * from persisted", []string{"1:NULL:2:20", "NULL:NULL:1:10"})
		})
		t.Run("insert-select", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "create table inserted(g int,h int,n bigint,total bigint)")
			execSQLRequire(t, ctx, db, "insert into inserted "+plain)
			check(t, "select * from inserted", []string{"1:NULL:2:20", "NULL:NULL:1:10"})
		})
	})
}
