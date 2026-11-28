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
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// Review 6095032855 (glm235 follow-up): a VIEW whose owned inner pagination
// goes through the double-cloned adoption path (WITH-bearing region cloned,
// then cloned again to adopt its authored ORDER BY natively) must still
// freeze every inner wildcard projection at CREATE VIEW time. The stored
// definition must read back with creation-time names, and later
// schema growth of the physical table must not leak into the frozen view:
// the exact nested-WITH shadowing shape already proven for the adopted
// ORDER BY, combined with a concrete physical table and an inner wlidcard
// SELECT, driven through real CREATE VIEW, SHOW CREATE readback, and
// select-after-ALTER readback.
func TestReview6095DoubleCloneViewStarCaptureConsumer(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		rootCtx, rootCancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer rootCancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		setupDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?multiStatements=true", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer setupDB.Close()
		_, err = setupDB.ExecContext(rootCtx, "create database if not exists r6095_view_db")
		require.NoError(t, err)
		// Bounded, error-surfacing cleanup right after the database exists;
		// setup uses the pooled handle, scenarios below keep isolated conns.
		defer cleanupSQLIntegration(t, cn, "drop database if exists r6095_view_db")
		_, err = setupDB.ExecContext(rootCtx, "create table r6095_view_db.vbt (a int, b int)")
		require.NoError(t, err)
		_, err = setupDB.ExecContext(rootCtx, "insert into r6095_view_db.vbt values (1, 10), (2, 5)")
		require.NoError(t, err)

		dsn := func() string {
			return fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/r6095_view_db?multiStatements=true", cn.GetServiceConfig().CN.Frontend.Port)
		}
		execStmt := func(stmt string) error {
			pdb, pdbErr := sql.Open("mysql", dsn())
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
		rowSet := func(t *testing.T, name, stmt string, cols int) [][]string {
			t.Helper()
			pdb, pdbErr := sql.Open("mysql", dsn())
			require.NoError(t, pdbErr)
			defer pdb.Close()
			pconn, pconnErr := pdb.Conn(rootCtx)
			require.NoError(t, pconnErr)
			defer pconn.Close()
			pctx, pcancel := context.WithTimeout(rootCtx, 20*time.Second)
			defer pcancel()
			rowsQ, qerr := pconn.QueryContext(pctx, stmt)
			require.NoErrorf(t, qerr, "%s must execute cleanly: %s", name, stmt)
			defer func() { require.NoError(t, rowsQ.Close()) }()
			var got [][]string
			for rowsQ.Next() {
				vals := make([]sql.RawBytes, cols)
				ptrs := make([]any, cols)
				for i := range vals {
					ptrs[i] = &vals[i]
				}
				require.NoError(t, rowsQ.Scan(ptrs...))
				row := make([]string, cols)
				for i, v := range vals {
					row[i] = string(v)
				}
				got = append(got, row)
			}
			require.NoErrorf(t, rowsQ.Err(), "%s must complete its stream cleanly", name)
			return got
		}

		// The exact GLM234 nested-WITH shadowing shape, with the physical
		// table inside the inner CTE and an inner wildcard SELECT whose
		// local authored ORDER BY is adopted, followed by an enclosing page.
		viewDDL := "create view v_cap as select * from ((with ox as (select 9 as a, 5 as b) ((with x as (select * from vbt) (select * from x) order by b limit 2)) limit 1)) d"
		require.NoError(t, execStmt(viewDDL), "the double-cloned pagination view must be representable")

		// Authoritative stored-DDL readback: the inner wildcards must be
		// frozen to creation-time names, with no raw star surviving in the
		// owned inner page.
		defRows := rowSet(t, "SHOW CREATE VIEW", "show create view v_cap", 4)
		require.NotEmpty(t, defRows, "SHOW CREATE VIEW must read the stored definition back")
		stored := strings.ToLower(defRows[0][1])
		t.Logf("G235 stored definition: %s", stored)
		// The precise contract: no raw star survives anywhere in the stored
		// definition - not the top-level projection, not the CTE body, and
		// not the owned inner page SELECT that the double-cloned adoption
		// carries. (The growth control below stays as a behavior witness;
		// the expanded CTE body alone would shield growth leaks, so the
		// authoritative discriminator is the stored-definition readback.)
		require.NotContainsf(t, stored, "select * from", "every owned wildcard - top level, CTE body and the inner page - must be frozen to creation-time names")

		// Creation-time rows: the adopted local ORDER BY (b ascending over
		// x = vbt) picks (2,5) first, and the enclosing page keeps exactly
		// the frozen view output.
		viewRows := rowSet(t, "view rows", "select * from v_cap", 2)
		require.Equal(t, [][]string{{"2", "5"}}, viewRows, "the frozen view must expose exactly creation-time columns and the adopted-order winner")

		// Schema-growth readback control: a later added physical column must
		// not leak into the frozen view through any surviving wildcard.
		require.NoError(t, execStmt("alter table vbt add column w int"))
		growthRows := rowSet(t, "view rows after growth", "select * from v_cap", 2)
		require.Equal(t, [][]string{{"2", "5"}}, growthRows, "later physical-table growth must not change the frozen view's output")
	})
}
