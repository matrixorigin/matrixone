// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// Review 6095032855 (P1): an unprojected ORDER BY column authored inside a
// pagination stage must keep its authored local binding - the WITH scope it
// lexically belongs to - instead of falling through the synthesized page
// wrapper to an enclosing query. Each variant runs on its own bounded,
// isolated connection; the one-winner contracts encode the exact rows.
const r6095WithAB = "with x as (select 1 as a, 2 as b union all select 2 as a, 1 as b)"

func TestReview6095AuthoredPaginationSortBindingWitness(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		rootCtx, rootCancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer rootCancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		setupDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?multiStatements=true", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer setupDB.Close()
		_, err = setupDB.ExecContext(rootCtx, "create database if not exists r6095_sort_db")
		require.NoError(t, err)
		// Bounded, error-surfacing cleanup is registered right after the
		// database exists; setup uses the pooled database handle and every
		// variant below keeps its own isolated connection.
		defer cleanupSQLIntegration(t, cn, "drop database if exists r6095_sort_db")

		paged := func(stmt string) string { return "select " + stmt + " from (select 0 as b) o" }
		probe := func(t *testing.T, name, stmt string, want []int) {
			t.Helper()
			pdb, pdbErr := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/r6095_sort_db?multiStatements=true", cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, pdbErr)
			defer pdb.Close()
			pconn, pconnErr := pdb.Conn(rootCtx)
			require.NoError(t, pconnErr)
			defer pconn.Close()
			pctx, pcancel := context.WithTimeout(rootCtx, 20*time.Second)
			defer pcancel()
			rowsQ, qerr := pconn.QueryContext(pctx, stmt)
			require.NoErrorf(t, qerr, "%s must resolve and execute cleanly (x.b must bind the authored WITH scope): %s", name, stmt)
			got := []int{}
			defer rowsQ.Close()
			for rowsQ.Next() {
				var v int
				require.NoError(t, rowsQ.Scan(&v))
				got = append(got, v)
			}
			require.NoErrorf(t, rowsQ.Err(), "%s must complete its result stream cleanly", name)
			require.Equalf(t, want, got, "%s must return exactly %v in order", name, want)
		}

		t.Run("page_sort_witness_local_b", func(t *testing.T) {
			// The reviewer's exact single-winner witness: local x.b orders
			// the page (b asc: 1 -> a=2 first), the final page keeps 2.
			probe(t, "page sort witness", paged("(("+r6095WithAB+" (select a from x) order by b limit 2) limit 1)"), []int{2})
		})
		t.Run("no_enclosing_local_b", func(t *testing.T) {
			// Without any enclosing b the page must still sort by authored
			// x.b, not fail with 1054 by looking outward for a missing name.
			probe(t, "no enclosing b", "select (("+r6095WithAB+" (select a from x) order by b limit 2) limit 1)", []int{2})
		})
		t.Run("single_page_control", func(t *testing.T) {
			// Without the final restrict page both locally-ordered rows
			// survive: [2, 1]. Runs as a derived-table subject so the two-row
			// page is a legal query result rather than a scalar-arity error.
			probe(t, "single page control", "select a from ((("+r6095WithAB+" (select a from x) order by b limit 2))) d", []int{2, 1})
		})
		t.Run("inner_order_by_control", func(t *testing.T) {
			// The inner-ORDER-BY spelling keeps its owned binding today.
			probe(t, "inner order by control", paged("(("+r6095WithAB+" (select a from x order by b limit 2)) limit 1)"), []int{2})
		})
		t.Run("projected_column_control", func(t *testing.T) {
			// A visible page projection of b binds within the page itself.
			// The page projects two columns, so it runs as a derived-table
			// subject rather than a scalar expression.
			probe(t, "projected column control", "select a from ((("+r6095WithAB+" (select a, b from x) order by b limit 2) limit 1)) d", []int{2})
		})
		t.Run("true_outer_case_zero_binding", func(t *testing.T) {
			// No local b exists: the ORDER BY's b must resolve the true outer
			// binding, and the outer value must determine exactly one
			// deterministic winner: case b=0 orders by a ascending -> [1].
			probe(t, "true outer b=0", "select ((with x as (select 1 as a union all select 2 as a) (select a from x) order by (case when b = 0 then a else -a end) limit 2) limit 1) from (select 0 as b) o", []int{1})
		})
		t.Run("true_outer_case_three_binding", func(t *testing.T) {
			// The same shape with a different outer value: case b!=0 orders
			// by -a, so [2] wins. Both deterministic winners can only come
			// from a genuine outer binding.
			probe(t, "true outer b=3", "select ((with x as (select 1 as a union all select 2 as a) (select a from x) order by (case when b = 0 then a else -a end) limit 2) limit 1) from (select 3 as b) o", []int{2})
		})
		t.Run("nested_with_shadowed_inner_wins", func(t *testing.T) {
			// A same-name enclosing CTE must not capture a nested paged
			// WITH-owning region whose page was adopted natively: the inner
			// declaration stays attached to its authored region and the
			// inner rows (b asc -> a=2 first) win through the outer page.
			probe(t, "nested shadowed derived", "select a from ((with x as (select 9 as a, 5 as b) (("+r6095WithAB+" (select a from x) order by b limit 2)) limit 1)) d", []int{2})
		})
		t.Run("nested_with_shadowed_scalar", func(t *testing.T) {
			probe(t, "nested shadowed scalar", "select ((with x as (select 9 as a, 5 as b) (("+r6095WithAB+" (select a from x) order by b limit 2)) limit 1)) from (select 0 as z) o", []int{2})
		})
		t.Run("nested_with_third_spelling", func(t *testing.T) {
			probe(t, "nested third spelling", "select a from ((with x as (select 9 as a, 5 as b) (("+r6095WithAB+" (select a from x) order by b limit 2))) limit 1) d2", []int{2})
		})
		t.Run("nested_with_isolated_keeps_inner", func(t *testing.T) {
			// The outer CTE is differently named: losing the inner
			// declaration errors on the missing relation instead.
			probe(t, "nested isolated inner-only", "select a from ((with ox as (select 9 as a, 5 as b) (("+r6095WithAB+" (select a from x) order by b limit 2)) limit 1)) d", []int{2})
		})
		t.Run("two_stage_multipage_legal", func(t *testing.T) {
			// A second page ordering the first page's projected column keeps
			// the ordinary multipage composition: inner [2,1] (local b asc),
			// then outer order-by-a limit 1 -> [1].
			probe(t, "two stage multipage", "select a from ((("+r6095WithAB+" (select a from x) order by b limit 2)) order by a limit 1) d3", []int{1})
		})
	})
}

// Review 6095032855 (P3): real engine validation of the two extended
// reference positions. A bare-END datetime column drives the time-window
// INTERVAL owner through an actual bucketed aggregate, and the CREATE
// INDEX INCLUDE spelling is exercised at the engine boundary, retaining
// the exact observed parser/engine split rather than asserting an
// unsupported combination.
func TestReview6095IntervalIncludeEngineWitness(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		rootCtx, rootCancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer rootCancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		setupDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?multiStatements=true", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer setupDB.Close()
		_, err = setupDB.ExecContext(rootCtx, "create database if not exists r6095_endref_db")
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop database if exists r6095_endref_db")
		execOn := func(stmt string) error {
			pdb, pdbErr := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/r6095_endref_db?multiStatements=true", cn.GetServiceConfig().CN.Frontend.Port))
			if pdbErr != nil {
				return pdbErr
			}
			defer pdb.Close()
			pconn, pconnErr := pdb.Conn(rootCtx)
			if pconnErr != nil {
				return pconnErr
			}
			defer pconn.Close()
			pctx, pcancel := context.WithTimeout(rootCtx, 20*time.Second)
			defer pcancel()
			_, execErr := pconn.ExecContext(pctx, stmt)
			return execErr
		}
		require.NoError(t, execOn("create table ev (end datetime, id int, v int)"))
		require.NoError(t, execOn("insert into ev values ('2026-01-01 00:00:00', 1, 10), ('2026-01-01 00:00:30', 1, 20)"))
		// The time-window owner interval(end, 1, minute) buckets both rows
		// into the same minute window for their common id.
		pdb, pdbErr := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/r6095_endref_db?multiStatements=true", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, pdbErr)
		defer pdb.Close()
		pconn, pconnErr := pdb.Conn(rootCtx)
		require.NoError(t, pconnErr)
		defer pconn.Close()
		pctx, pcancel := context.WithTimeout(rootCtx, 20*time.Second)
		defer pcancel()
		rowsQ, qerr := pconn.QueryContext(pctx, "select count(*) from ev group by id interval(end, 1, minute)")
		require.NoErrorf(t, qerr, "the bare-END interval owner must execute on the real engine")
		defer func() { require.NoError(t, rowsQ.Close()) }()
		require.Truef(t, rowsQ.Next(), "the single minute window must aggregate")
		var got int
		require.NoError(t, rowsQ.Scan(&got))
		require.Equal(t, 2, got, "both rows fall into the same one-minute window")
		require.False(t, rowsQ.Next())
		require.NoError(t, rowsQ.Err())
		// The supported native DDL must succeed outright: this
		// implementation's index INCLUDE accepts the admitted bare-END
		// column-reference position end to end.
		require.NoErrorf(t, execOn("create index ci_e on ev (id) include (end)"),
			"the bare-END include DDL must succeed on this implementation")
	})
}
