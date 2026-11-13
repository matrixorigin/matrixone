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

package sqlintegration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/stretchr/testify/require"
)

func TestMoIndexStatsSQL(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists index_stats_test", "drop user if exists index_stats_reader")
		exec := func(query string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, query)
			require.NoErrorf(t, err, "%s", query)
		}
		exec("create database index_stats_test")
		exec("use index_stats_test")
		for _, name := range []string{"plain_vectors", "quantized_vectors"} {
			exec("create table " + name + "(id bigint primary key, label int, embedding vecf64(1))")
			exec("insert into " + name + " values(1,1,'[-10]'),(2,2,'[10]')")
			quantization := ""
			if name == "quantized_vectors" {
				quantization = " quantization 'int8'"
			}
			exec("create index ivf using ivfflat on " + name + "(embedding) lists=1 op_type 'vector_l2_ops' kmeans_train_percent=100" + quantization)
			query := "select cast(json_extract(metrics,'$.vector_count') as bigint), " +
				"cast(json_extract(metrics,'$.center_mismatch_ratio') as double) " +
				"from mo_index_stats('index_stats_test','" + name + "','ivf') AS s where row_type='index'"
			var count int64
			var encoded string
			require.NoError(t, conn.QueryRowContext(ctx, "select metrics from mo_index_stats('index_stats_test','"+name+"','ivf') AS s where row_type='index'").Scan(&encoded))
			var summary map[string]any
			require.NoError(t, json.Unmarshal([]byte(encoded), &summary))
			require.Equal(t, float64(1), summary["lists"])
			require.Equal(t, "vector_l2_ops", summary["op_type"])
			require.Equal(t, "sync", summary["maintenance_mode"])
			require.Equal(t, float64(2), summary["avg_bucket_size"])
			require.Equal(t, float64(0), summary["empty_centroid_ratio"])
			require.Equal(t, float64(1), summary["max_bucket_size_multiple"])
			for _, key := range []string{"mean_residual_distance", "rms_residual_distance", "center_shift"} {
				require.NotContains(t, summary, key)
			}
			require.NotContains(t, summary, "async")
			require.NotContains(t, summary, "diagnosis")
			require.NotContains(t, summary, "status")
			options := summary["index_options"].(map[string]any)
			for _, key := range []string{"lists", "op_type", "quantization", "async"} {
				require.NotContains(t, options, key)
			}
			storage := summary["storage"].(map[string]any)
			require.NotContains(t, storage, "size_is_estimate")
			require.Equal(t, float64(4), storage["table_count"], "include the rejected-row table")
			require.Len(t, storage, 3, "storage exposes only aggregate size and counts")
			require.GreaterOrEqual(t, storage["object_count"].(float64), float64(0))
			require.GreaterOrEqual(t, storage["size_bytes"].(float64), float64(0))
			var mismatch float64
			var centerTable string
			require.NoError(t, conn.QueryRowContext(ctx, "select i.index_table_name from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id=t.rel_id where t.reldatabase='index_stats_test' and t.relname='"+name+"' and i.name='ivf' and i.algo_table_type='centroids'").Scan(&centerTable))
			readCenter := func() float64 {
				t.Helper()
				var encoded string
				require.NoError(t, conn.QueryRowContext(ctx, "select __mo_index_centroid from "+sqlquote.Ident(centerTable)+" order by __mo_index_centroid_version desc limit 1").Scan(&encoded))
				var values []float64
				require.NoError(t, json.Unmarshal([]byte(encoded), &values))
				require.Len(t, values, 1)
				return values[0]
			}
			center := readCenter()
			require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&count, &mismatch))
			require.Equal(t, int64(2), count)
			require.InDelta(t, center*center/(center*center+100), mismatch, 1e-10)
			x1, x2 := center+9, center+11
			exec(fmt.Sprintf("update %s set embedding='[%g]' where id=1", name, x1))
			exec(fmt.Sprintf("update %s set embedding='[%g]' where id=2", name, x2))
			require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&count, &mismatch))
			require.InDelta(t, 100.0/101, mismatch, 1e-10)
			oldMismatch := mismatch
			exec("alter table " + name + " alter reindex ivf ivfflat")
			require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&count, &mismatch))
			require.Equal(t, int64(2), count)
			newCenter := readCenter()
			wantShift := math.Abs((x1+x2)/2 - newCenter)
			wantMismatch := 2 * wantShift * wantShift / ((x1-newCenter)*(x1-newCenter) + (x2-newCenter)*(x2-newCenter))
			require.InDelta(t, wantMismatch, mismatch, 1e-10)
			require.Less(t, mismatch, oldMismatch)
			var version int64
			require.NoError(t, conn.QueryRowContext(ctx,
				"select cast(json_extract(metrics,'$.index_version') as bigint) from mo_index_stats('index_stats_test','"+name+"','ivf') AS s where row_type='index'").Scan(&version))
			require.Equal(t, int64(1), version)
		}
		exec("alter table plain_vectors alter reindex ivf ivfflat lists=2")
		exec("delete from plain_vectors where id=1")
		var count, empty, minBucket int64
		var emptyRatio, maxMultiple float64
		require.NoError(t, conn.QueryRowContext(ctx,
			"select cast(json_extract(metrics,'$.vector_count') as bigint),"+
				"cast(json_extract(metrics,'$.empty_centroid_count') as bigint),"+
				"cast(json_extract(metrics,'$.min_bucket_size') as bigint),"+
				"cast(json_extract(metrics,'$.empty_centroid_ratio') as double),"+
				"cast(json_extract(metrics,'$.max_bucket_size_multiple') as double) "+
				"from mo_index_stats('index_stats_test','plain_vectors','ivf') AS s where row_type='index'").Scan(&count, &empty, &minBucket, &emptyRatio, &maxMultiple))
		require.Equal(t, int64(1), count)
		require.Equal(t, int64(1), empty)
		require.Zero(t, minBucket)
		require.Equal(t, 0.5, emptyRatio)
		require.Equal(t, float64(2), maxMultiple)
		rows, err := conn.QueryContext(ctx, "select object_id,metrics "+
			"from mo_index_stats('index_stats_test','plain_vectors','ivf') AS s "+
			"where row_type='centroid' and json_extract(metrics,'$.center_mismatch_ratio') is not null "+
			"order by cast(json_extract(metrics,'$.center_mismatch_ratio') as double) desc limit 10")
		require.NoError(t, err)
		defer rows.Close()
		var ranked int
		for rows.Next() {
			var id, encoded string
			require.NoError(t, rows.Scan(&id, &encoded))
			var metrics map[string]any
			require.NoError(t, json.Unmarshal([]byte(encoded), &metrics))
			require.NotEmpty(t, id)
			require.Equal(t, float64(1), metrics["vector_count"])
			require.Contains(t, metrics, "center_mismatch_ratio")
			require.Equal(t, float64(2), metrics["bucket_size_multiple"])
			require.NotContains(t, metrics, "centroid_count")
			require.NotContains(t, metrics, "min_bucket_size")
			ranked++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 1, ranked, "empty buckets must be excluded by the missing JSON metric")
		exec("create index ordinary on plain_vectors(label)")
		var ordinaryJSON string
		require.NoError(t, conn.QueryRowContext(ctx, "select metrics from mo_index_stats('index_stats_test','plain_vectors','ordinary') AS s").Scan(&ordinaryJSON))
		var ordinary map[string]any
		require.NoError(t, json.Unmarshal([]byte(ordinaryJSON), &ordinary))
		require.Equal(t, float64(1), ordinary["entry_count"])
		require.Equal(t, float64(1), ordinary["cardinality"])
		rows, err = conn.QueryContext(ctx, "select * from mo_index_stats('index_stats_test','plain_vectors','missing') AS s")
		if rows != nil {
			defer rows.Close()
			require.NoError(t, rows.Err())
		}
		require.ErrorContains(t, err, "does not exist")
		exec("create user index_stats_reader identified by '111'")
		reader, err := sql.Open("mysql", fmt.Sprintf("index_stats_reader:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer reader.Close()
		rows, err = reader.QueryContext(ctx, "select * from mo_index_stats('index_stats_test','plain_vectors','ivf') AS s")
		if rows != nil {
			defer rows.Close()
			require.NoError(t, rows.Err())
		}
		require.Error(t, err, "diagnostics must require SELECT on the primary table")
		exec("grant select on table index_stats_test.plain_vectors to public")
		var summary string
		require.NoError(t, reader.QueryRowContext(ctx, "select metrics from mo_index_stats('index_stats_test','plain_vectors','ivf') AS s where row_type='index'").Scan(&summary))
	})
}

func TestMoIndexStatsAsyncSQL(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists index_stats_async")
		exec := func(query string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		exec("create database index_stats_async")
		exec("use index_stats_async")
		exec("create table vectors(id bigint primary key, v vecf64(1))")
		exec("insert into vectors values(1,'[-10]'),(2,'[10]')")
		exec("create index ivf using ivfflat on vectors(v) lists=1 kmeans_train_percent=100 async")
		read := func() (map[string]any, error) {
			var encoded string
			if err := conn.QueryRowContext(ctx, "select metrics from mo_index_stats('index_stats_async','vectors','ivf') AS s where row_type='index'").Scan(&encoded); err != nil {
				return nil, err
			}
			var metrics map[string]any
			err := json.Unmarshal([]byte(encoded), &metrics)
			return metrics, err
		}
		wait := func(count float64) {
			t.Helper()
			var last map[string]any
			var lastErr error
			require.Eventually(t, func() bool {
				last, lastErr = read()
				if lastErr != nil || last["vector_count"] != count {
					return false
				}
				maintenance, ok := last["async"].(map[string]any)
				return ok && maintenance["caught_up"] == true
			}, 45*time.Second, 100*time.Millisecond, "index must consume the source changes")
			require.NoError(t, lastErr, "%v", last)
		}
		wait(2)
		var executor *iscp.ISCPTaskExecutor
		require.Eventually(t, func() bool {
			var ok bool
			executor, ok = iscp.GetExecutorRuntime(cn.ServiceID())
			return ok
		}, 30*time.Second, 100*time.Millisecond)
		require.NoError(t, executor.Pause())
		paused := true
		defer func() {
			if paused {
				require.NoError(t, executor.Resume())
			}
		}()
		exec("insert into vectors values(3,'[30]')")
		metrics, err := read()
		require.NoError(t, err)
		require.Equal(t, "async", metrics["maintenance_mode"])
		require.Equal(t, float64(2), metrics["vector_count"], "report consumed index vectors")
		maintenance := metrics["async"].(map[string]any)
		require.Equal(t, false, maintenance["caught_up"])
		require.GreaterOrEqual(t, maintenance["lag_seconds"].(float64), float64(0))
		require.NotContains(t, metrics, "diagnosis")
		require.NotContains(t, maintenance, "status")
		exec("begin")
		exec("insert into vectors values(4,'[40]')")
		metrics, err = read()
		require.NoError(t, err)
		maintenance = metrics["async"].(map[string]any)
		require.Nil(t, maintenance["caught_up"], "transaction-local writes cannot be covered by ISCP")
		require.Equal(t, "source_commit_ts_unavailable", maintenance["status"])
		exec("rollback")
		require.NoError(t, executor.Resume())
		paused = false
		wait(3)
	})
}
