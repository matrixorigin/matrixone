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

package isolated

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestPointIndexSelectionAfterAnalyze(t *testing.T) {
	data := t.TempDir()
	t.Logf("embedded single-CN cluster pid=%d start=%s config and data=%s", os.Getpid(), time.Now().Format(time.RFC3339), data)
	cluster, err := embed.NewCluster(embed.WithTesting(), embed.WithDataPath(data), embed.WithCNCount(1))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	require.NoError(t, cluster.Start())
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx := t.Context()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	exec := func(stmt string) {
		t.Helper()
		_, err := conn.ExecContext(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	exec("create database top1_scattered")
	exec("use top1_scattered")
	exec(`create table runs (
		id bigint primary key, workspace_id int not null, workflow_id int not null,
		task_id int not null, created_at bigint not null, body varchar(256) not null,
		index ix_flow (workspace_id, workflow_id), index ix_task (task_id))`)
	exec(`insert into runs
		select result,
		       case when mod(result * 79, 150000) < 110469 then 1 else result end,
		       case when mod(result * 79, 150000) < 110469 then 1 else result end,
		       case when mod(result * 79, 150000) < 15665 then 7 else mod(result * 79, 150000) + 10 end,
		       mod(result * 173, 200003), repeat('x', 200)
		from generate_series(1, 150000) g`)
	exec("analyze table runs")

	query := func(hint string, key int) string {
		return fmt.Sprintf("select * from runs%s where workspace_id = 1 and workflow_id = 1 and task_id = %d order by created_at, id limit 1", hint, key)
	}
	for _, hint := range []string{"", " ignore index (ix_flow)"} {
		// The first EXPLAIN ANALYZE follows ANALYZE TABLE directly. Running a
		// SELECT or the narrow-index control first could warm the key columns.
		plan, err := testutils.QueryText(ctx, conn, "explain analyze "+query(hint, 100010))
		require.NoError(t, err)
		require.Contains(t, plan, "Sort Key: runs.created_at INTERNAL, runs.id INTERNAL")
		require.Contains(t, plan, "Limit: 1")
		for _, scan := range []string{"Table Scan on top1_scattered.runs", "Index Table Scan on runs.ix_task"} {
			pattern := regexp.QuoteMeta(scan) + `[^\n]*\n\s*Analyze:[^\n]*inputRows=1\b`
			require.Regexp(t, pattern, plan)
		}
	}
	for _, tc := range []struct{ task, id int }{{100010, 100000}, {7, 26590}} {
		for _, hint := range []string{"", " ignore index (ix_flow)"} {
			var id, workspace, workflow, task, created int
			var body string
			err := conn.QueryRowContext(ctx, query(hint, tc.task)).Scan(&id, &workspace, &workflow, &task, &created, &body)
			require.NoError(t, err)
			require.Equal(t, []int{tc.id, 1, 1, tc.task, tc.id * 173 % 200003}, []int{id, workspace, workflow, task, created})
			require.Equal(t, strings.Repeat("x", 200), body)
		}
	}
}
