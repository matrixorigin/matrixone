//go:build gpu

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package isolated

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Run with a GPU-built libmo on a supported Linux x86_64 NVIDIA host. This
// exercises the native full-source initializer, not just its generated SQL.
func TestAlterCopyGPUIndexSnapshotCDC(t *testing.T) {
	path, err := os.MkdirTemp("", "copy-gpu-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(path)) })
	cluster, err := embed.NewCluster(embed.WithTesting(), embed.WithDataPath(path), embed.WithCNCount(1))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	require.NoError(t, cluster.Start())
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?readTimeout=45s&writeTimeout=45s", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	db.SetMaxOpenConns(3)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	require.Eventually(t, func() bool {
		runtime, ok := iscp.GetExecutorRuntime(cn.ServiceID())
		return ok && runtime.IsRunning()
	}, 90*time.Second, 100*time.Millisecond)
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	for _, tc := range []struct {
		algo   string
		create string
	}{
		{"cagra", "create index ix using cagra on t(v) op_type 'vector_l2_ops' intermediate_graph_degree=8 graph_degree=4 ASYNC"},
		{"ivfpq", "create index ix using ivfpq on t(v) op_type 'vector_l2_ops' lists=2 m=2 bits_per_code=8 ASYNC"},
	} {
		t.Run(tc.algo, func(t *testing.T) {
			dbName := "copy_gpu_" + tc.algo
			exec := func(query string) {
				t.Helper()
				_, err := conn.ExecContext(ctx, query)
				require.NoError(t, err, query)
			}
			exec("create database " + dbName)
			t.Cleanup(func() {
				cleanupCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
				defer stop()
				_, err := db.ExecContext(cleanupCtx, "drop database if exists "+dbName)
				require.NoError(t, err)
			})
			exec("use " + dbName)
			exec("set experimental_" + tc.algo + "_index=1")
			exec("create table t(id bigint primary key, v vecf32(8))")
			var rows []string
			for i := 1; i <= 20; i++ {
				vector := strings.TrimSuffix(strings.Repeat(fmt.Sprintf("%d,", i), 8), ",")
				rows = append(rows, fmt.Sprintf("(%d,'[%s]')", i, vector))
			}
			rows = append(rows, "(21,'[105,105,105,105,105,105,105,105]')")
			exec("insert into t values " + strings.Join(rows, ","))
			var originalTable uint64
			require.NoError(t, conn.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='t'", dbName).Scan(&originalTable))
			exec(tc.create)

			type job struct {
				id        uint64
				state     int8
				watermark string
				status    iscp.JobStatus
				spec      iscp.JobSpec
			}
			readJob := func(tableID uint64) (job, error) {
				var j job
				var status, spec string
				err := db.QueryRowContext(ctx, "select job_id,job_state,watermark,job_status,job_spec from mo_catalog.mo_iscp_log where drop_at is null and job_name='index_ix' and table_id=?", tableID).Scan(&j.id, &j.state, &j.watermark, &status, &spec)
				if err == nil {
					err = json.Unmarshal([]byte(status), &j.status)
				}
				if err == nil {
					err = json.Unmarshal([]byte(spec), &j.spec)
				}
				return j, err
			}
			waitJob := func(tableID uint64, after types.TS) job {
				t.Helper()
				var j job
				require.EventuallyWithT(t, func(check *assert.CollectT) {
					var err error
					j, err = readJob(tableID)
					if !assert.NoError(check, err) {
						return
					}
					assert.Equal(check, int8(iscp.ISCPJobState_Completed), j.state)
					assert.Equal(check, int8(iscp.JobStage_Running), j.status.Stage)
					assert.Zero(check, j.status.ErrorCode, j.status.ErrorMsg)
					assert.NotEmpty(check, j.watermark)
					assert.NotEqual(check, "0-0", j.watermark)
					if j.watermark != "0-0" && j.watermark != "" {
						watermark := types.StringToTS(j.watermark)
						assert.True(check, watermark.GE(&after), "watermark %s has not reached %s", j.watermark, after.ToString())
					}
				}, 2*time.Minute, 100*time.Millisecond)
				return j
			}
			initialJob := waitJob(originalTable, types.TS{})
			exec("alter table t algorithm=copy,add column marker int default 0")
			var replacementTable uint64
			require.NoError(t, conn.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='t'", dbName).Scan(&replacementTable))
			require.NotEqual(t, originalTable, replacementTable, "COPY must replace the source relation")
			snapshotJob := waitJob(replacementTable, types.TS{})
			require.True(t, snapshotJob.spec.InitSQLCoversSnapshot)
			initSQL, err := base64.StdEncoding.DecodeString(snapshotJob.spec.InitSQL)
			require.NoError(t, err, "persisted ISCP InitSQL must be valid base64")
			require.Contains(t, strings.ToLower(string(initSQL)), tc.algo+"_create")
			require.NotZero(t, initialJob.id)
			require.NotZero(t, snapshotJob.id)

			var storage string
			require.NoError(t, conn.QueryRowContext(ctx, "select index_table_name from mo_catalog.mo_indexes where table_id=? and name='ix' and algo_table_type=?", replacementTable, tc.algo+"_index").Scan(&storage))
			storageSQL := "`" + dbName + "`.`" + strings.ReplaceAll(storage, "`", "``") + "`"
			var count int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+storageSQL+" where tag=0").Scan(&count))
			require.Positive(t, count, "native model must be published with snapshot progress")

			explain := func(query string) string {
				t.Helper()
				planRows, err := conn.QueryContext(ctx, "explain "+query)
				require.NoError(t, err)
				defer planRows.Close()
				var plan strings.Builder
				for planRows.Next() {
					var line string
					require.NoError(t, planRows.Scan(&line))
					plan.WriteString(line)
				}
				require.NoError(t, planRows.Err())
				return strings.ToLower(plan.String())
			}
			orderBy := " order by l2_distance(v,'[100,100,100,100,100,100,100,100]') limit 1"
			require.Contains(t, explain("select id from t"+orderBy), tc.algo+"_search", "the query must exercise the native index")
			require.NotContains(t, explain("select id from t ignore index(ix)"+orderBy), tc.algo+"_search", "the comparison must scan the source table")

			exec("insert into t(id,v) values (100,'[100,100,100,100,100,100,100,100]')")
			exec("update t set v='[500,500,500,500,500,500,500,500]' where id=5")
			exec("delete from t where id=21")
			writeEnd := types.BuildTS(time.Now().UnixNano(), 0)
			var laterJob job
			require.EventuallyWithT(t, func(check *assert.CollectT) {
				var err error
				laterJob, err = readJob(replacementTable)
				if !assert.NoError(check, err) {
					return
				}
				assert.Equal(check, int8(iscp.ISCPJobState_Completed), laterJob.state)
				assert.Equal(check, int8(iscp.JobStage_Running), laterJob.status.Stage)
				assert.Zero(check, laterJob.status.ErrorCode, laterJob.status.ErrorMsg)
				assert.Equal(check, snapshotJob.id, laterJob.id)
				assert.Greater(check, laterJob.status.LSN, snapshotJob.status.LSN)
				assert.NotEmpty(check, laterJob.watermark)
				if laterJob.watermark != "0-0" && laterJob.watermark != "" {
					watermark := types.StringToTS(laterJob.watermark)
					assert.True(check, watermark.GE(&writeEnd), "watermark %s has not reached %s", laterJob.watermark, writeEnd.ToString())
				}
				var tailRows int
				queryErr := conn.QueryRowContext(ctx, "select count(*) from "+storageSQL+" where tag=1").Scan(&tailRows)
				if assert.NoError(check, queryErr) {
					assert.Positive(check, tailRows, "CDC tail must be present after the COPY snapshot")
				}
			}, 2*time.Minute, 100*time.Millisecond)
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+storageSQL+" where tag=1").Scan(&count))
			require.Positive(t, count, "CDC must publish the post-COPY tail")
			for _, probe := range []struct {
				value int
				want  int64
			}{{100, 100}, {500, 5}, {105, 100}, {12, 12}} {
				vector := strings.TrimSuffix(strings.Repeat(fmt.Sprintf("%d,", probe.value), 8), ",")
				for _, hint := range []string{"", " ignore index(ix)"} {
					var id int64
					q := fmt.Sprintf("select id from t%s order by l2_distance(v,'[%s]') limit 1", hint, vector)
					require.NoError(t, conn.QueryRowContext(ctx, q).Scan(&id))
					require.Equal(t, probe.want, id, "probe %d with %q", probe.value, hint)
				}
			}
		})
	}
}
