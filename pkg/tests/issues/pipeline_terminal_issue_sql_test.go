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

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// This public-path complement checks recursive pipeline completion and the
// original error surviving spool cleanup. No large vector dataset is required.
func TestIssue24971RecursivePipelineTerminalSQL(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		var got int
		require.NoError(t, conn.QueryRowContext(ctx, "with recursive r(n) as (select 1 union all select n+1 from r where n<3) select max(n) from r").Scan(&got))
		require.Equal(t, 3, got)
		mustExec(t, ctx, conn, "set cte_max_recursion_depth=2")
		for range 2 {
			err = conn.QueryRowContext(ctx, "with recursive r(n) as (select 1 union all select n+1 from r where n<5) select max(n) from r").Scan(&got)
			require.ErrorContains(t, err, "recursive level out of range")
		}
		require.NoError(t, conn.QueryRowContext(ctx, "select 1").Scan(&got))
		require.Equal(t, 1, got)
	})
}

// A minimum q15-shaped CTE + join + nested MAX smoke test. The issue's missing
// fatal notification is injected by the separate receiver whitebox test; this
// case does not claim to reproduce the original 48-hour stability workload.
func TestIssue26202Q15ShapeSQL(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(2)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		name := strings.ToLower(testutils.GetDatabaseName(t))
		mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", name))
		defer func() {
			cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanCancel()
			_, err := db.ExecContext(cleanCtx, fmt.Sprintf("drop database if exists `%s`", name))
			require.NoError(t, err)
		}()
		mustExec(t, ctx, conn, fmt.Sprintf("use `%s`", name))
		mustExec(t, ctx, conn, "create table supplier(id int primary key, name varchar(16))")
		mustExec(t, ctx, conn, "create table lineitem(supplier_id int, price decimal(12,2), discount decimal(4,2), shipdate date)")
		mustExec(t, ctx, conn, "insert into supplier values (1,'a'),(2,'b')")
		mustExec(t, ctx, conn, "insert into lineitem values (1,100,0,'1996-01-02'),(1,50,0,'1996-02-02'),(2,200,0,'1996-03-02')")
		query := `with revenue as (
   select supplier_id,sum(price*(1-discount)) as total from lineitem
   where shipdate>=date '1996-01-01' and shipdate<date '1996-04-01' group by supplier_id
  ) select s.id,s.name,cast(r.total as decimal(12,2)) from supplier s join revenue r on s.id=r.supplier_id
  where r.total=(select max(total) from revenue) order by s.id`
		rows, err := conn.QueryContext(ctx, query)
		require.NoError(t, err)
		defer rows.Close()
		require.True(t, rows.Next())
		var id int
		var nameGot, total string
		require.NoError(t, rows.Scan(&id, &nameGot, &total))
		require.Equal(t, 2, id)
		require.Equal(t, "b", nameGot)
		require.Equal(t, "200.00", total)
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
	})
}
