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

// Review 6094104086 (P1): a broadcast join whose probe side is a lazily
// admitted UNION pagination stream must complete on time with the correct
// row semantics for every affected join family - equality LEFT (missing-key
// preservation), INNER (matched only) and EXISTS semi joins - plus the
// passing non-equality LEFT control and a zero-row early-stop probe. Each
// variant runs on its own connection under its own bounded deadline with
// prompt result-set release, so one pre-fix run reports each boundary
// independently and a hung server side cannot poison sibling variants.
func TestReview6094BroadcastJoinLazyBranchOwnerWitness(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		rootCtx, rootCancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer rootCancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		setupDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?multiStatements=true", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer setupDB.Close()
		_, err = setupDB.ExecContext(rootCtx, "create database if not exists r6094_join_db")
		require.NoError(t, err)
		// Bounded, error-surfacing cleanup is registered right after the
		// database exists, so a later hang or stall cannot leave teardown
		// waiting on an unbounded statement. Setup runs on the pooled
		// database handle: no session-leased connection can outlive a
		// require-based early exit without its explicit Close.
		defer cleanupSQLIntegration(t, cn, "drop database if exists r6094_join_db")
		_, err = setupDB.ExecContext(rootCtx, "create table r6094_join_db.k (v int)")
		require.NoError(t, err)
		_, err = setupDB.ExecContext(rootCtx, "insert into r6094_join_db.k values (1), (3)")
		require.NoError(t, err)

		lazySource := "(with q as (select 1 as a union all select 2 as a) (select a from q limit 5) limit 3) p"

		probe := func(t *testing.T, name, stmt string, deadline time.Duration, want []int) {
			t.Helper()
			pdb, pdbErr := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/r6094_join_db?multiStatements=true", cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, pdbErr)
			defer pdb.Close()
			pconn, pconnErr := pdb.Conn(rootCtx)
			require.NoError(t, pconnErr)
			defer pconn.Close()
			started := time.Now()
			pctx, pcancel := context.WithTimeout(rootCtx, deadline)
			defer pcancel()
			rowsQ, qerr := pconn.QueryContext(pctx, stmt)
			if qerr != nil {
				t.Fatalf("%s never returned rows within its deadline: %v (elapsed %s)", name, qerr, time.Since(started))
			}
			got := []int{}
			defer rowsQ.Close()
			for rowsQ.Next() {
				var v int
				require.NoErrorf(t, rowsQ.Scan(&v), "%s must scan an integer projection", name)
				got = append(got, v)
			}
			require.NoErrorf(t, rowsQ.Err(), "%s must complete its result stream cleanly", name)
			require.Equalf(t, want, got, "%s must return exactly %v in order (elapsed %s)", name, want, time.Since(started))
		}

		t.Run("equality_left_missing_key", func(t *testing.T) {
			probe(t, "equality LEFT", "select p.a from "+lazySource+" left join k on p.a = k.v order by p.a", 20*time.Second, []int{1, 2})
		})
		t.Run("inner_matched_only", func(t *testing.T) {
			probe(t, "INNER hash", "select p.a from "+lazySource+" inner join k on p.a = k.v order by p.a", 20*time.Second, []int{1})
		})
		t.Run("exists_semi_matched_only", func(t *testing.T) {
			probe(t, "EXISTS semi", "select p.a from "+lazySource+" where exists (select 1 from k where k.v = p.a) order by p.a", 20*time.Second, []int{1})
		})
		t.Run("non_equality_left_control", func(t *testing.T) {
			probe(t, "non-equality LEFT control", "select p.a from "+lazySource+" left join k on p.a < k.v order by p.a", 20*time.Second, []int{1, 2})
		})
		t.Run("zero_row_early_stop", func(t *testing.T) {
			probe(t, "zero-row LEFT", "select p.a from (with q as (select 1 as a union all select 2 as a) (select a from q where 1 = 0 limit 5) limit 3) p left join k on p.a = k.v order by p.a", 20*time.Second, []int{})
		})
		t.Run("full_outer_both_sides", func(t *testing.T) {
			// FULL OUTER over the same lazy pagination probe: the matched
			// row, the left-unmatched page row and the build-side-unmatched
			// row must all survive with the build admitted as an ordinary
			// sibling outside the lazy branch scheduler.
			pdb, pdbErr := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/r6094_join_db?multiStatements=true", cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, pdbErr)
			defer pdb.Close()
			pconn, pconnErr := pdb.Conn(rootCtx)
			require.NoError(t, pconnErr)
			defer pconn.Close()
			started := time.Now()
			pctx, pcancel := context.WithTimeout(rootCtx, 20*time.Second)
			defer pcancel()
			rowsQ, qerr := pconn.QueryContext(pctx, "select coalesce(p.a, -1) as pa, coalesce(kk.v, -1) as kv from "+lazySource+" full join k as kk on p.a = kk.v order by pa desc, kv desc")
			if qerr != nil {
				t.Fatalf("FULL OUTER never returned rows within its deadline: %v (elapsed %s)", qerr, time.Since(started))
			}
			defer rowsQ.Close()
			var got [][2]int
			for rowsQ.Next() {
				var pa, kv int
				require.NoError(t, rowsQ.Scan(&pa, &kv))
				got = append(got, [2]int{pa, kv})
			}
			require.NoError(t, rowsQ.Err())
			require.Equal(t, [][2]int{{2, -1}, {1, 1}, {-1, 3}}, got,
				"FULL OUTER must return matched, left-unmatched and right-unmatched rows in order (elapsed %s)", time.Since(started))
		})
	})
}
