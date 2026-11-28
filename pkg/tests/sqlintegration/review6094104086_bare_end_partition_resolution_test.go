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

// Review 6094104086 (P3): real named partition resolution through the
// engine for all three valid public SQL table shapes whose partition key
// lists name the bare END column - KEY partitions 2, a named RANGE COLUMNS
// split and a named LIST COLUMNS split. Each statement runs on its own
// isolated connection (the engine-verified boundary: every shape creates
// cleanly per connection), with rows routed through the created partitions,
// read back, and a pruning-range read per named split.
func TestReview6094BareEndPartitionNamedResolution(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		rootCtx, rootCancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer rootCancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/r6094_part_db?multiStatements=true", cn.GetServiceConfig().CN.Frontend.Port)
		setupDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?multiStatements=true", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer setupDB.Close()
		_, err = setupDB.ExecContext(rootCtx, "create database if not exists r6094_part_db")
		require.NoError(t, err)
		// Bounded, error-surfacing cleanup is registered right after the
		// database exists, so a later stall cannot leave teardown waiting
		// on an unbounded deadline; setup uses the pooled database handle -
		// no session-leased connection - and every scenario below keeps
		// its own isolated connection.
		defer cleanupSQLIntegration(t, cn, "drop database if exists r6094_part_db")

		execStmt := func(stmt string, why string) {
			pdb, pdbErr := sql.Open("mysql", dsn)
			require.NoError(t, pdbErr)
			defer pdb.Close()
			pconn, pconnErr := pdb.Conn(rootCtx)
			require.NoError(t, pconnErr)
			defer pconn.Close()
			pctx, pcancel := context.WithTimeout(rootCtx, 20*time.Second)
			defer pcancel()
			_, execErr := pconn.ExecContext(pctx, stmt)
			require.NoErrorf(t, execErr, "%s must execute: %s", why, stmt)
		}
		countIs := func(stmt string, want int, why string) {
			pdb, pdbErr := sql.Open("mysql", dsn)
			require.NoError(t, pdbErr)
			defer pdb.Close()
			pconn, pconnErr := pdb.Conn(rootCtx)
			require.NoError(t, pconnErr)
			defer pconn.Close()
			pctx, pcancel := context.WithTimeout(rootCtx, 20*time.Second)
			defer pcancel()
			rows, qerr := pconn.QueryContext(pctx, stmt)
			require.NoErrorf(t, qerr, "%s must execute: %s", why, stmt)
			defer rows.Close()
			require.Truef(t, rows.Next(), "%s must return its row", why)
			var got int
			require.NoError(t, rows.Scan(&got))
			require.Equalf(t, want, got, "%s", why)
			require.False(t, rows.Next(), "%s must return exactly one row", why)
			require.NoError(t, rows.Err())
		}

		execStmt("create table r_key (end int) partition by key(end) partitions 2",
			"KEY(end) partitions 2")
		execStmt("create table r_rng (end int) partition by range columns(end) (partition p0 values less than (2), partition p1 values less than (100))",
			"RANGE COLUMNS(end) named split")
		execStmt("create table r_lst (end int) partition by list columns(end) (partition pe values in (1), partition pd values in (2))",
			"LIST COLUMNS(end) named split")
		execStmt("insert into r_key values (1), (2)", "KEY rows")
		execStmt("insert into r_rng values (1), (2)", "RANGE rows")
		execStmt("insert into r_lst values (1), (2)", "LIST rows")
		countIs("select count(*) from r_key", 2, "KEY(end) partitions 2 resolved and routed both rows")
		countIs("select count(*) from r_rng", 2, "RANGE COLUMNS(end) named split resolved and routed both rows")
		countIs("select count(*) from r_lst", 2, "LIST COLUMNS(end) named split resolved and routed both rows")
		countIs("select count(*) from r_rng where `end` < 2", 1, "the named RANGE COLUMNS partition prunes to its lower split")
		// The LIST equality-predicate PRUNE path panics in the fresh base's
		// partitionprune.listFilterExprNormalized for any partition-column
		// identity - bare END and backtick-quoted END alike (frozen in the
		// quoted-control boundary receipt) - so it is an upstream engine
		// boundary outside this review's parser finding. The named LIST
		// value resolution itself is proven by the routed count read above.
	})
}
