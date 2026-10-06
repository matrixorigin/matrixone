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

package multicn

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

// A second CN is necessary: single-CN execution cannot observe an unencoded
// WINDOW or a JoinMap published on a different CN from its consumer.
func TestJoinCoordinatorStage(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	inventory := clusterservice.GetMOCluster(cn.ServiceID())
	refresher, ok := inventory.(clusterservice.AuthoritativeRefresher)
	require.True(t, ok)
	updater, ok := inventory.(clusterservice.CNWorkStateUpdaterWithContext)
	require.True(t, ok)
	var peer string
	waitWorkers := func(ctx context.Context, want int) {
		t.Helper()
		require.Eventually(t, func() bool {
			if refresher.Refresh(ctx) != nil {
				return false
			}
			count := 0
			inventory.GetCNService(clusterservice.NewSelector(), func(service metadata.CNService) bool {
				count++
				if service.ServiceID != cn.ServiceID() {
					peer = service.PipelineServiceAddress
				}
				return true
			})
			return count == want
		}, 30*time.Second, 100*time.Millisecond)
	}
	waitWorkers(ctx, 2)
	oldForce := plan.GetForceScanOnMultiCN()
	plan.SetForceScanOnMultiCN(true)
	t.Cleanup(func() { plan.SetForceScanOnMultiCN(oldForce) })
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	const schema = "join_stage_placement"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		require.NoError(t, updater.DebugUpdateCNWorkStateWithContext(cleanupCtx, cn.ServiceID(), int(metadata.WorkState_Working)))
		waitWorkers(cleanupCtx, 2)
		_, err := conn.ExecContext(cleanupCtx, "drop database if exists "+schema)
		require.NoError(t, err)
	})
	exec := func(statement string) {
		t.Helper()
		_, err := conn.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}
	for _, statement := range []string{
		"drop database if exists " + schema, "create database " + schema, "use " + schema,
		"create table a(id int,k int,x int)", "create table c(rid int,k int,v int)",
		"insert into a values(1,1,1),(2,1,1),(3,2,NULL),(4,NULL,0)",
		"insert into c values(11,1,1),(12,1,3),(13,2,NULL),(14,3,7)",
	} {
		exec(statement)
	}
	for _, table := range []string{"a", "c"} {
		exec("select mo_ctl('dn','flush','" + schema + "." + table + "')")
	}
	require.NoError(t, updater.DebugUpdateCNWorkStateWithContext(ctx, cn.ServiceID(), int(metadata.WorkState_Draining)))
	waitWorkers(ctx, 1)
	query := func(t *testing.T, statement string) string {
		t.Helper()
		rows, err := conn.QueryContext(ctx, statement)
		require.NoError(t, err, statement)
		defer rows.Close()
		cols, err := rows.Columns()
		require.NoError(t, err)
		var out strings.Builder
		for rows.Next() {
			values := make([]sql.NullString, len(cols))
			pointers := make([]any, len(cols))
			for i := range values {
				pointers[i] = &values[i]
			}
			require.NoError(t, rows.Scan(pointers...))
			for i, value := range values {
				if i != 0 {
					out.WriteByte('\t')
				}
				if value.Valid {
					out.WriteString(value.String)
				} else {
					out.WriteString("NULL")
				}
			}
			out.WriteByte('\n')
		}
		require.NoError(t, rows.Err())
		return out.String()
	}
	for _, tc := range []struct{ name, statement, want string }{
		{"local window probe", `select d.id,d.rn,c.v from (select id,k,row_number() over(order by id) as rn from a) d left join c on d.k=c.k order by d.id,c.v`, "1\t1\t1\n1\t1\t3\n2\t2\t1\n2\t2\t3\n3\t3\tNULL\n4\t4\tNULL\n"},
		{"local window build", `select a.id,d.v from a left join (select k,v,row_number() over(partition by k order by rid desc) as rn from c) d on a.k=d.k and d.rn=1 order by a.id`, "1\t3\n2\t3\n3\tNULL\n4\tNULL\n"},
		{"numeric cast provenance", `with d as (select distinct k,x,k is null as kn,coalesce(k,0) as kv,x is null as xn,coalesce(x,0) as xv from a), f as (select d.kn,d.kv,d.xn,d.xv,coalesce(max(c.v),0) as result from d join c on c.k=d.k where c.rid>10 group by d.kn,d.kv,d.xn,d.xv) select a.id,coalesce(f.result,0) from a left join f on (a.k is null)=f.kn and coalesce(a.k,0)=f.kv and (a.x is null)=f.xn and coalesce(a.x,0)=f.xv order by a.id`, "1\t3\n2\t3\n3\t0\n4\t0\n"},
		{"full outer unmatched build", `select a.id,c.rid from a full outer join c on a.k=c.k order by coalesce(a.id,999),c.rid`, "1\t11\n1\t12\n2\t11\n2\t12\n3\t13\n4\tNULL\nNULL\t14\n"},
		{"right unmatched build", `select a.id,c.rid from a right join c on a.k=c.k order by coalesce(a.id,999),c.rid`, "1\t11\n1\t12\n2\t11\n2\t12\n3\t13\nNULL\t14\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, query(t, tc.statement))
			require.Equal(t, strings.SplitN(tc.want, "\n", 2)[0]+"\n", query(t, tc.statement+" limit 1"))
			physical := query(t, "explain phyplan analyze "+tc.statement)
			require.NotEmpty(t, peer)
			require.Contains(t, physical, peer, "must execute a scan on the other CN")
		})
	}
	require.Equal(t, "", query(t, `select d.id from (select id,k,row_number() over(order by id) rn from a where id<0) d left join c on d.k=c.k`))
	// Reuse the same cluster with both workers admitted. Product probes remain
	// distributed while their broadcast producers have a common result owner.
	require.NoError(t, updater.DebugUpdateCNWorkStateWithContext(ctx, cn.ServiceID(), int(metadata.WorkState_Working)))
	waitWorkers(ctx, 2)
	exec("set max_dop=2")
	for _, tc := range []struct{ name, statement, want string }{
		{"broadcast product", `select count(*),sum(a.id*100+c.rid) from a cross join c`, "16\t4200\n"},
		{"empty broadcast product", `select count(*) from a cross join c where a.id<0`, "0\n"},
		{"nested product early stop", `select count(*) from (select a.id from a cross join c) d join a z on d.id=z.id where z.id<0`, "0\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for range 2 {
				require.Equal(t, tc.want, query(t, tc.statement))
			}
		})
	}
	physical := query(t, "explain phyplan analyze select a.id,c.rid from a cross join c")
	require.Contains(t, strings.ToLower(physical), "product")
	require.Contains(t, physical, peer, "must retain remote probe work")

	stmt, err := conn.PrepareContext(ctx, `select count(*),sum(a.id*100+c.rid) from a cross join c where a.id>?`)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stmt.Close()) })
	for _, threshold := range []int{0, 99, 0} {
		var count int64
		var sum sql.NullInt64
		require.NoError(t, stmt.QueryRowContext(ctx, threshold).Scan(&count, &sum))
		if threshold == 0 {
			require.EqualValues(t, 16, count)
			require.True(t, sum.Valid)
			require.EqualValues(t, 4200, sum.Int64)
		} else {
			require.Zero(t, count)
			require.False(t, sum.Valid)
		}
	}

}
