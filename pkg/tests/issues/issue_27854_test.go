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
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestIssue27854RequiredVectorDomainStaysCoordinatorLocal(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)

		dbName := strings.ToLower(testutils.GetDatabaseName(t))
		execSQLRequire(t, ctx, db, "create database `"+dbName+"`")
		defer execSQLMaybe(t, ctx, db, "drop database if exists `"+dbName+"`")
		execSQLRequire(t, ctx, db, "use `"+dbName+"`")
		execSQLRequire(t, ctx, db, "set experimental_ivf_index = 1")
		vec := func(first int) string { return fmt.Sprintf("[%d%s]", first, strings.Repeat(",0", 31)) }
		execSQLRequire(t, ctx, db,
			"create table filtered_t(id int primary key, file_id varchar(20), v vecf32(32), key idx_file_id(file_id))")
		execSQLRequire(t, ctx, db,
			fmt.Sprintf("insert into filtered_t values (1,'file1','%s'),(2,'file1','%s'),(3,'file1','%s')", vec(1), vec(2), vec(3)))
		execSQLRequire(t, ctx, db,
			fmt.Sprintf("insert into filtered_t select result + 3, 'file1', '%s' from generate_series(1, 101) g", vec(100)))
		execSQLRequire(t, ctx, db,
			fmt.Sprintf("insert into filtered_t select result + 104, 'file2', '%s' from generate_series(1, 20) g", vec(0)))
		execSQLRequire(t, ctx, db,
			"create index filtered_idx using ivfflat on filtered_t(v) lists=1 op_type 'vector_l2_ops'")

		query := fmt.Sprintf("select id from filtered_t where file_id = 'file1' and "+
			"l2_distance(v,'%s') <= 3 order by l2_distance(v,'%s') "+
			"limit 10 by rank with option 'mode=pre'", vec(0), vec(0))
		execSQLRequire(t, ctx, db, "set session optimizer_hints = 'forceOneCN=1'")
		local := queryInt64Rows(t, ctx, db, query)
		require.Equal(t, []int64{1, 2, 3}, local)

		execSQLRequire(t, ctx, db, "set session optimizer_hints = ''")
		require.Equal(t, local, queryInt64Rows(t, ctx, db, query))

		emptyQuery := strings.Replace(query, "file_id = 'file1'", "file_id = 'missing'", 1)
		require.Empty(t, queryInt64Rows(t, ctx, db, emptyQuery),
			"an exact empty build domain must keep parallel reader cardinality without panicking")

		// Two explicit flushes make a tiny persisted multi-object fixture. Patch
		// only its cost statistics so DOP selection is deterministic, without a
		// large dataset or waiting for the background statistics interval.
		var entries string
		require.NoError(t, db.QueryRowContext(ctx, `select distinct i.index_table_name
			from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id=t.rel_id
			where t.reldatabase=? and t.relname='filtered_t' and i.name='filtered_idx'
			and i.algo_table_type='entries'`, dbName).Scan(&entries))
		flush := "select mo_ctl('dn','flush','" + dbName + "." + entries + "')"
		execSQLRequire(t, ctx, db, flush)
		execSQLRequire(t, ctx, db, fmt.Sprintf("insert into filtered_t values (125,'file1','%s')", vec(150)))
		execSQLRequire(t, ctx, db, flush)
		var count float64
		var objects int64
		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(
			"select table_cnt, accurate_object_number from table_stats('%s.%s','refresh','full') g", dbName, entries)).Scan(&count, &objects))
		require.Equal(t, float64(125), count)
		require.GreaterOrEqual(t, objects, int64(2), "the fixture must contain multiple real entries objects")
		patch := fmt.Sprintf(`{"table_cnt":125,"block_number":2,"accurate_object_number":2,"size_map":{"%s":16000}}`,
			catalog.SystemSI_IVFFLAT_TblCol_Entries_entry)
		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(
			"select table_cnt from table_stats('%s.%s','patch','%s') g", dbName, entries, patch)).Scan(&count))
		for _, dop := range []int{1, 2} {
			execSQLRequire(t, ctx, db, fmt.Sprintf("set @@max_dop=%d", dop))
			planText := strings.Join(querySingleStringColumn(t, ctx, db, "explain analyze "+query), "\n")
			require.Contains(t, planText, "Estimated Scan Rows: 125, Blocks: 2, Vector Bytes/Row: 128")
			require.Contains(t, planText, fmt.Sprintf("Planned DOP: %d", dop))
			require.Equal(t, local, queryInt64Rows(t, ctx, db, query))
			require.Empty(t, queryInt64Rows(t, ctx, db, emptyQuery))
		}
	})
}

func queryInt64Rows(t *testing.T, ctx context.Context, db *sql.DB, query string) []int64 {
	t.Helper()
	rows, err := db.QueryContext(ctx, query)
	require.NoError(t, err)
	defer rows.Close()
	var result []int64
	for rows.Next() {
		var value int64
		require.NoError(t, rows.Scan(&value))
		result = append(result, value)
	}
	require.NoError(t, rows.Err())
	return result
}
