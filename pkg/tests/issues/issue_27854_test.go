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

	"github.com/matrixorigin/matrixone/pkg/embed"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestIssue27854RequiredVectorDomainOnMultiCN(t *testing.T) {
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

		dbName := testutils.GetDatabaseName(t)
		execSQLRequire(t, ctx, db, "create database `"+dbName+"`")
		defer execSQLMaybe(t, ctx, db, "drop database if exists `"+dbName+"`")
		execSQLRequire(t, ctx, db, "use `"+dbName+"`")
		execSQLRequire(t, ctx, db, "set experimental_ivf_index = 1")
		execSQLRequire(t, ctx, db,
			"create table filtered_t(id int primary key, file_id varchar(20), v vecf32(3), key idx_file_id(file_id))")
		execSQLRequire(t, ctx, db,
			"insert into filtered_t values (1,'file1','[1,0,0]'),(2,'file1','[2,0,0]'),(3,'file1','[3,0,0]')")
		execSQLRequire(t, ctx, db,
			"insert into filtered_t select result + 3, 'file1', '[100,0,0]' from generate_series(1, 101) g")
		execSQLRequire(t, ctx, db,
			"insert into filtered_t select result + 104, 'file2', '[0,0,0]' from generate_series(1, 20) g")
		execSQLRequire(t, ctx, db,
			"create index filtered_idx using ivfflat on filtered_t(v) lists=1 op_type 'vector_l2_ops'")

		query := "select id from filtered_t where file_id = 'file1' and " +
			"l2_distance(v,'[0,0,0]') <= 3 order by l2_distance(v,'[0,0,0]') " +
			"limit 10 by rank with option 'mode=pre'"
		execSQLRequire(t, ctx, db, "set session optimizer_hints = 'forceOneCN=1'")
		local := queryInt64Rows(t, ctx, db, query)
		require.Equal(t, []int64{1, 2, 3}, local)

		t.Cleanup(func() { plan2.SetForceScanOnMultiCN(false) })
		plan2.SetForceScanOnMultiCN(true)
		execSQLRequire(t, ctx, db, "set session optimizer_hints = ''")
		physical, err := testutils.QueryTextResult(ctx, db, "explain phyplan "+query)
		require.NoError(t, err)
		require.Contains(t, strings.ToUpper(physical.ColumnName), "PHYPLAN ON MULTICN(")
		require.Equal(t, local, queryInt64Rows(t, ctx, db, query))

		emptyQuery := strings.Replace(query, "file_id = 'file1'", "file_id = 'missing'", 1)
		require.Empty(t, queryInt64Rows(t, ctx, db, emptyQuery),
			"an exact empty build domain must keep parallel reader cardinality without panicking")
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
