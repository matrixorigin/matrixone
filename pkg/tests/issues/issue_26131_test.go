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
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

const issue26131Q15 = `
with revenue0 as (
    select l_suppkey as supplier_no,
           sum(l_extendedprice * (1 - l_discount)) as total_revenue
    from lineitem
    where l_shipdate >= date '1995-12-01'
      and l_shipdate < date '1995-12-01' + interval '3' month
    group by l_suppkey
)
select s_suppkey, s_name, total_revenue
from supplier, revenue0
where s_suppkey = supplier_no
  and total_revenue = (select max(total_revenue) from revenue0)
order by s_suppkey`

func TestIssue26131Q15SharedCTEExecutesBothConsumers(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()

		const database = "issue_26131"
		execSQLMaybe(t, ctx, db, "drop database if exists "+database)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+database)
		}()
		execSQLRequire(t, ctx, db, "create database "+database)
		execSQLRequire(t, ctx, db, "use "+database)
		execSQLRequire(t, ctx, db, "create table supplier (s_suppkey int primary key, s_name varchar(32))")
		execSQLRequire(t, ctx, db, "create table lineitem (l_suppkey int, l_extendedprice decimal(15,2), l_discount decimal(15,2), l_shipdate date)")
		execSQLRequire(t, ctx, db, "insert into supplier select result, concat('supplier-', cast(result as char)) from generate_series(1,100) g")
		execSQLRequire(t, ctx, db, "insert into lineitem select mod(result - 1, 100) + 1, if(mod(result - 1, 100) + 1 = 42, 2, 1), 0, '1995-12-15' from generate_series(1,100000) g")
		execSQLRequire(t, ctx, db, "analyze table supplier, lineitem")

		planText := explainSQL(t, ctx, db, "explain "+issue26131Q15)
		require.GreaterOrEqual(t, strings.Count(planText, "Sink Scan"), 2,
			"the real plan must route both the join and scalar MAX consumers through the shared CTE")

		rows, err := db.QueryContext(ctx, issue26131Q15)
		require.NoError(t, err)
		defer rows.Close()
		require.True(t, rows.Next())
		var supplierKey int
		var supplierName, revenue string
		require.NoError(t, rows.Scan(&supplierKey, &supplierName, &revenue))
		require.Equal(t, 42, supplierKey)
		require.Equal(t, "supplier-42", supplierName)
		require.NotEmpty(t, revenue)
		require.False(t, rows.Next(), "both consumers must drain to one terminal result")
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	})
}

func explainSQL(t *testing.T, ctx context.Context, db *sql.DB, statement string) string {
	t.Helper()
	rows, err := db.QueryContext(ctx, statement)
	require.NoError(t, err)
	defer rows.Close()
	columns, err := rows.Columns()
	require.NoError(t, err)
	var lines []string
	for rows.Next() {
		values := make([]sql.RawBytes, len(columns))
		dest := make([]any, len(values))
		for i := range values {
			dest[i] = &values[i]
		}
		require.NoError(t, rows.Scan(dest...))
		for _, value := range values {
			lines = append(lines, string(value))
		}
	}
	require.NoError(t, rows.Err())
	return strings.Join(lines, "\n")
}
