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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
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
		{"cagra", "create index ix using cagra on t(v) op_type 'vector_l2_ops' intermediate_graph_degree=8 graph_degree=4 quantization 'float32' ASYNC"},
		{"ivfpq", "create index ix using ivfpq on t(v) op_type 'vector_l2_ops' lists=2 m=2 bits_per_code=4 kmeans_train_percent=100 quantization 'float32' ASYNC"},
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
			defer conn.ExecContext(ctx, "rollback")
			exec("use " + dbName)
			exec("set experimental_" + tc.algo + "_index=1")
			buildThreads := tc.algo + "_threads_build"
			exec("set session " + buildThreads + "=1")
			exec("create table t(id bigint primary key, v vecf32(8))")
			var rows []string
			for i := 1; i <= 20; i++ {
				vector := strings.TrimSuffix(strings.Repeat(fmt.Sprintf("%d,", i), 8), ",")
				rows = append(rows, fmt.Sprintf("(%d,'[%s]')", i, vector))
			}
			rows = append(rows, "(21,'[105,105,105,105,105,105,105,105]')", "(22,'[-1,-1,-1,-1,-1,-1,-1,-1]')")
			exec("insert into t values " + strings.Join(rows, ","))
			var originalTable uint64
			require.NoError(t, conn.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='t'", dbName).Scan(&originalTable))
			// Delete the exact nearest row after the initializer has acquired S1.
			// A second full-table snapshot cannot see this tombstone.
			fault.Enable()
			defer fault.Disable()
			const phase = "processInitSQLSnapshot"
			waitKey := objectio.ISCPExecutorFaultWaitKey(phase)
			waiters := waitKey + ":waiters"
			require.NoError(t, fault.AddFaultPoint(ctx, waitKey, ":::", "wait", 0, "", false))
			defer fault.RemoveFaultPoint(ctx, waitKey)
			require.NoError(t, fault.AddFaultPoint(ctx, waiters, ":::", "getwaiters", 0, waitKey, false))
			defer fault.RemoveFaultPoint(ctx, waiters)
			require.NoError(t, fault.AddFaultPoint(ctx, objectio.FJ_CDCExecutor, ":::", "echo", 0, phase, false))
			defer fault.RemoveFaultPoint(ctx, objectio.FJ_CDCExecutor)
			exec(tc.create)
			require.Eventually(t, func() bool {
				n, _, ok := fault.TriggerFault(waiters)
				return ok && n > 0
			}, time.Minute, 50*time.Millisecond)
			exec("delete from t where id=22")
			deleteEnd := types.BuildTS(time.Now().UnixNano(), 0)
			_, err = fault.RemoveFaultPoint(ctx, waitKey)
			require.NoError(t, err)

			type job struct {
				id        uint64
				state     int8
				watermark string
				status    iscp.JobStatus
				spec      iscp.JobSpec
			}
			type jobReader interface {
				QueryRowContext(context.Context, string, ...any) *sql.Row
			}
			readJob := func(reader jobReader, tableID uint64) (job, error) {
				var j job
				var status, spec string
				err := reader.QueryRowContext(ctx, "select job_id,job_state,watermark,job_status,job_spec from mo_catalog.mo_iscp_log where drop_at is null and job_name='index_ix' and table_id=?", tableID).Scan(&j.id, &j.state, &j.watermark, &status, &spec)
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
					j, err = readJob(db, tableID)
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
			initialJob := waitJob(originalTable, deleteEnd)
			deletedProbe := " order by l2_distance(v,'[-1,-1,-1,-1,-1,-1,-1,-1]') limit 1"
			for _, hint := range []string{"", " ignore index(ix)"} {
				q := "select id from t" + hint + deletedProbe
				if hint == "" {
					require.Contains(t, explain(q), tc.algo+"_search")
				} else {
					require.NotContains(t, explain(q), tc.algo+"_search")
				}
				var id int64
				require.NoError(t, conn.QueryRowContext(ctx, q).Scan(&id), q)
				require.EqualValues(t, 1, id, q)
			}
			require.True(t, initialJob.spec.InitSQLCoversSnapshot)
			exec("alter table t algorithm=copy,add column marker int default 0")
			var replacementTable uint64
			require.NoError(t, conn.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='t'", dbName).Scan(&replacementTable))
			require.NotEqual(t, originalTable, replacementTable, "COPY must replace the source relation")
			snapshotJob := waitJob(replacementTable, types.TS{})
			require.True(t, snapshotJob.spec.InitSQLCoversSnapshot)
			require.Empty(t, snapshotJob.spec.InitSQL)
			require.NotZero(t, initialJob.id)
			require.NotZero(t, snapshotJob.id)

			var storage string
			require.NoError(t, conn.QueryRowContext(ctx, "select index_table_name from mo_catalog.mo_indexes where table_id=? and name='ix' and algo_table_type=?", replacementTable, tc.algo+"_index").Scan(&storage))
			storageSQL := "`" + dbName + "`.`" + strings.ReplaceAll(storage, "`", "``") + "`"
			var count int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+storageSQL+" where tag=0").Scan(&count))
			require.Positive(t, count, "native model must be published with snapshot progress")

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
				laterJob, err = readJob(db, replacementTable)
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

			quantization := func() string {
				t.Helper()
				var raw string
				require.NoError(t, db.QueryRowContext(ctx, "select algo_params from mo_catalog.mo_indexes where table_id=? and name='ix' and algo_table_type=?", replacementTable, tc.algo+"_index").Scan(&raw))
				var params struct {
					Quantization string `json:"quantization"`
				}
				require.NoError(t, json.Unmarshal([]byte(raw), &params))
				return params.Quantization
			}
			reindex := func(format string, needsSnapshot bool) uint64 {
				t.Helper()
				exec("alter table t alter reindex ix " + tc.algo + " quantization '" + format + "'")
				j, err := readJob(conn, replacementTable)
				require.NoError(t, err)
				require.Greater(t, j.id, laterJob.id)
				require.Equal(t, needsSnapshot, j.spec.InitSQLCoversSnapshot)
				if needsSnapshot {
					require.Equal(t, int8(iscp.JobStage_Init), j.status.Stage)
				} else {
					require.Equal(t, int8(iscp.JobStage_Running), j.status.Stage)
					require.NotEmpty(t, j.watermark)
					require.NotEqual(t, "0-0", j.watermark)
				}
				return j.id
			}
			exec("begin")
			reindex("float32", true)
			exec("rollback")
			exec("begin")
			reindex("int8", false)
			exec("rollback")
			observer, err := db.Conn(ctx)
			require.NoError(t, err)
			defer observer.Close()
			_, err = observer.ExecContext(ctx, "use "+dbName)
			require.NoError(t, err)
			_, err = observer.ExecContext(ctx, "set experimental_"+tc.algo+"_index=1")
			require.NoError(t, err)
			privateDistance := " order by l2_distance(v,'[-100,-100,-100,-100,-100,-100,-100,-100]')"
			privateOrder := privateDistance + " limit 1"
			checkNeighbor := func(reader jobReader, query string, want int64) {
				t.Helper()
				var id int64
				require.NoError(t, reader.QueryRowContext(ctx, query).Scan(&id), query)
				require.Equal(t, want, id, query)
			}
			checkNeighbor(observer, "select id from t"+privateOrder, 1)
			exec("begin")
			exec("insert into t(id,v) values (999,'[-100,-100,-100,-100,-100,-100,-100,-100]')")
			reindex("int8", true)
			privateQuery := "select id from t" + privateOrder
			require.Contains(t, explain(privateQuery), tc.algo+"_search")
			checkNeighbor(conn, privateQuery, 999)
			exec("rollback")
			require.Equal(t, "float32", quantization())
			checkNeighbor(observer, "select id from t ignore index(ix)"+privateOrder, 1)
			checkNeighbor(observer, privateQuery, 1)
			unchanged, err := readJob(db, replacementTable)
			require.NoError(t, err)
			require.Equal(t, laterJob.id, unchanged.id)

			exec("set session " + buildThreads + "=2")
			exec("begin")
			exec("insert into t(id,v) values (999,'[-100,-100,-100,-100,-100,-100,-100,-100]')")
			jobID := reindex("int8", true)
			exec("alter table t rename to renamed")
			exec("insert into renamed(id,v) values (1000,'[-50,-50,-50,-50,-50,-50,-50,-50]')")
			exec("create table t(id bigint primary key, v vecf32(8))")
			exec("insert into t values " + strings.Join(rows, ","))
			exec("commit")
			require.Equal(t, "int8", quantization())
			checkNeighbor(observer, "select id from renamed"+privateOrder, 999)
			rebuilt := waitJob(replacementTable, types.TS{})
			require.Equal(t, jobID, rebuilt.id)
			require.Equal(t, "renamed", rebuilt.spec.SrcTable.TableName)
			settings, err := sqlexec.NewMetadataFromJson(string(rebuilt.spec.SnapshotBuildVars))
			require.NoError(t, err)
			threads, err := settings.ResolveVariableFunc(buildThreads, true, false)
			require.NoError(t, err)
			require.Equal(t, int64(2), threads)
			require.Contains(t, explain("select id from renamed"+privateDistance+" limit 2"), tc.algo+"_search")
			for _, hint := range []string{"", " ignore index(ix)"} {
				q := "select id from renamed" + hint + privateDistance + " limit 2"
				rows, err := observer.QueryContext(ctx, q)
				require.NoError(t, err)
				defer rows.Close()
				var ids []int64
				for rows.Next() {
					var id int64
					require.NoError(t, rows.Scan(&id), q)
					ids = append(ids, id)
				}
				require.NoError(t, rows.Err(), q)
				require.Equal(t, []int64{999, 1000}, ids, q)
			}
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+storageSQL+" where tag=0").Scan(&count))
			require.Positive(t, count)
			query := "select id from renamed" + orderBy
			require.Contains(t, explain(query), tc.algo+"_search")
			checkNeighbor(observer, query, 100)
		})
	}
}
