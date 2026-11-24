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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/diagnostic"
	"github.com/stretchr/testify/assert"
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
		for _, tc := range []struct{ name, columns, pk, rows, newRow string }{
			{"enum_keys", "s enum('red','blue','green','black')", "s", "('red','[1]'),('blue','[2]'),('green','[3]')", "('black','[4]')"},
			{"string_keys", "s varchar(20) collate utf8mb4_bin", "s", "('A','[1]'),('a','[2]'),('é','[3]')", "('e','[4]')"},
			{"tuple_keys", "s varchar(20) collate utf8mb4_bin,n bigint", "s,n", "('A',1,'[1]'),('A',2,'[2]'),('a',1,'[3]')", "('a',2,'[4]')"},
		} {
			exec("create table " + tc.name + "(" + tc.columns + ",v vecf32(1),primary key(" + tc.pk + "))")
			exec("insert into " + tc.name + " values " + tc.rows)
			exec("create index ivf using ivfflat on " + tc.name + "(v) lists=1")
			exec("insert into " + tc.name + " values " + tc.newRow)
			var oldRows, newRows, recall int64
			query := "select cast(json_extract(metrics,'$.drift.old_sample_count') as bigint)," +
				"cast(json_extract(metrics,'$.drift.new_sample_count') as bigint)," +
				"cast(json_extract(metrics,'$.analysis.recall.sample_count') as bigint) " +
				"from mo_index_stats('index_stats_test','" + tc.name + "','ivf','{\"sample_count\":4}') s where row_type='index'"
			require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&oldRows, &newRows, &recall), tc.name)
			require.Equal(t, int64(3), oldRows)
			require.Equal(t, int64(1), newRows)
			require.Equal(t, int64(4), recall)
		}

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
			if name == "quantized_vectors" {
				require.Equal(t, "center_stats_approximate_quantized", summary["status"])
			} else {
				require.NotContains(t, summary, "status")
			}
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
			expectedMismatch := func(center float64, x1, x2 float64) float64 {
				t.Helper()
				if name == "quantized_vectors" {
					var entries, meta string
					for role, target := range map[string]*string{"entries": &entries, "metadata": &meta} {
						require.NoError(t, conn.QueryRowContext(ctx, "select i.index_table_name from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id=t.rel_id where t.reldatabase='index_stats_test' and t.relname='"+name+"' and i.name='ivf' and i.algo_table_type='"+role+"'").Scan(target))
					}
					var lo, hi float64
					require.NoError(t, conn.QueryRowContext(ctx, "select cast(`__mo_index_val` as double) from "+sqlquote.Ident(meta)+" where `__mo_index_key`='quantize_min'").Scan(&lo))
					require.NoError(t, conn.QueryRowContext(ctx, "select cast(`__mo_index_val` as double) from "+sqlquote.Ident(meta)+" where `__mo_index_key`='quantize_max'").Scan(&hi))
					rows, err := conn.QueryContext(ctx, "select __mo_index_centroid_fk_entry from "+sqlquote.Ident(entries)+" where __mo_index_centroid_fk_version=(select cast(`__mo_index_val` as bigint) from "+sqlquote.Ident(meta)+" where `__mo_index_key`='version') order by __mo_index_pri_col")
					require.NoError(t, err)
					defer rows.Close()
					var values []float64
					for rows.Next() {
						var raw string
						require.NoError(t, rows.Scan(&raw))
						var v []float64
						require.NoError(t, json.Unmarshal([]byte(raw), &v))
						values = append(values, lo+(v[0]+128)*(hi-lo)/255)
					}
					require.NoError(t, rows.Err())
					require.Len(t, values, 2)
					x1, x2 = values[0], values[1]
				}
				shift := (x1+x2)/2 - center
				denominator := (x1-center)*(x1-center) + (x2-center)*(x2-center)
				if denominator == 0 {
					return 0
				}
				return 2 * shift * shift / denominator
			}
			center := readCenter()
			require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&count, &mismatch))
			require.Equal(t, int64(2), count)
			require.InDelta(t, expectedMismatch(center, -10, 10), mismatch, 1e-10)
			x1, x2 := center+9, center+11
			exec(fmt.Sprintf("update %s set embedding='[%g]' where id=1", name, x1))
			exec(fmt.Sprintf("update %s set embedding='[%g]' where id=2", name, x2))
			require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&count, &mismatch))
			require.InDelta(t, expectedMismatch(center, x1, x2), mismatch, 1e-10)
			oldMismatch := mismatch
			exec("alter table " + name + " alter reindex ivf ivfflat")
			require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&count, &mismatch))
			require.Equal(t, int64(2), count)
			newCenter := readCenter()
			wantMismatch := expectedMismatch(newCenter, x1, x2)
			require.InDelta(t, wantMismatch, mismatch, 1e-10)
			require.Less(t, mismatch, oldMismatch)
			var version int64
			require.NoError(t, conn.QueryRowContext(ctx,
				"select cast(json_extract(metrics,'$.index_version') as bigint) from mo_index_stats('index_stats_test','"+name+"','ivf') AS s where row_type='index'").Scan(&version))
			require.Equal(t, int64(1), version)
		}
		testIndexHealthSQL(t, ctx, conn, exec)
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
		err = conn.QueryRowContext(ctx, "select metrics from mo_index_stats('index_stats_test','plain_vectors','ordinary','{}') as s").Scan(&ordinaryJSON)
		require.ErrorContains(t, err, "does not support deep analysis")
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
		var denied string
		err = reader.QueryRowContext(ctx, "select metrics from mo_index_stats('index_stats_test','plain_vectors','ivf','{}') as s where row_type='index'").Scan(&denied)
		require.Error(t, err, "deep analysis must require source SELECT too")
		deniedErr := err.Error()
		for _, args := range []string{"'missing'", "'PRIMARY','{}'"} {
			err := reader.QueryRowContext(ctx, "select metrics from mo_index_stats('index_stats_test','plain_vectors',"+args+") as s where row_type='index'").Scan(&denied)
			require.EqualError(t, err, deniedErr)
		}
		exec("grant select on table index_stats_test.plain_vectors to public")
		var summary string
		require.NoError(t, reader.QueryRowContext(ctx, "select metrics from mo_index_stats('index_stats_test','plain_vectors','ivf') AS s where row_type='index'").Scan(&summary))
		require.NoError(t, reader.QueryRowContext(ctx, "select metrics from mo_index_stats('index_stats_test','plain_vectors','ivf','{}') as s where row_type='index'").Scan(&summary))
		exec("create temporary table plain_vectors(id bigint primary key, embedding vecf64(1))")
		var unavailable string
		err = conn.QueryRowContext(ctx, "select metrics from mo_index_stats('index_stats_test','plain_vectors','ivf') as s where row_type='index'").Scan(&unavailable)
		require.ErrorContains(t, err, "non-temporary, non-partitioned")
		exec("drop temporary table plain_vectors")
		exec("create table partitioned_vectors(id bigint primary key, embedding vecf64(1)) partition by hash(id) partitions 2")
		err = conn.QueryRowContext(ctx, "select metrics from mo_index_stats('index_stats_test','partitioned_vectors','ivf') as s where row_type='index'").Scan(&unavailable)
		require.ErrorContains(t, err, "non-temporary, non-partitioned")

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
		exec("insert into vectors values(1,'[-10]')")
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
		exec("create index ivf using ivfflat on vectors(v) lists=1 kmeans_train_percent=100 async")
		// Commit after registration but before the worker takes its training snapshot.
		exec("insert into vectors values(2,'[10]'),(5,NULL)")
		require.NoError(t, executor.Resume())
		paused = false
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
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				last, err := read()
				require.NoError(c, err)
				require.Equal(c, count, last["vector_count"])
				maintenance, ok := last["async"].(map[string]any)
				require.True(c, ok, "%v", last)
				require.Equal(c, true, maintenance["caught_up"], "%v", last)
			}, 45*time.Second, 100*time.Millisecond, "index must consume the source changes")
		}
		// The CDC index stores three entries, but only the two non-NULL vectors train the baseline.
		wait(3)
		built, err := read()
		require.NoError(t, err)
		require.Equal(t, float64(0), built["growth_ratio"])
		require.InDelta(t, 2.0/30, built["training_sample_ratio"], 1e-12)
		require.NoError(t, executor.Pause())
		paused = true
		exec("delete from vectors where id=1")
		lagging, err := read()
		require.NoError(t, err)
		drift := lagging["drift"].(map[string]any)
		require.Equal(t, float64(2), drift["old_sample_count"])
		require.Equal(t, float64(0), drift["new_sample_count"])
		exec("insert into vectors values(3,'[30]')")
		metrics, err := read()
		require.NoError(t, err)
		require.Equal(t, "async", metrics["maintenance_mode"])
		require.Equal(t, float64(3), metrics["vector_count"], "report consumed index entries")
		var deepJSON string
		require.NoError(t, conn.QueryRowContext(ctx, "select metrics from mo_index_stats('index_stats_async','vectors','ivf','{}') as s where row_type='index'").Scan(&deepJSON))
		var deep map[string]any
		require.NoError(t, json.Unmarshal([]byte(deepJSON), &deep))
		require.Equal(t, "index_not_caught_up", deep["analysis"].(map[string]any)["status"])
		maintenance := metrics["async"].(map[string]any)
		require.Equal(t, false, maintenance["caught_up"])
		require.GreaterOrEqual(t, maintenance["lag_seconds"].(float64), float64(0))
		require.NotContains(t, metrics, "diagnosis")
		require.NotContains(t, maintenance, "status")
		maintenanceBeforeCatchup := maintenance
		exec("create view ivf_health as select metrics from mo_index_stats('index_stats_async','vectors','ivf','{}') as s where row_type='index'")
		exec("create snapshot index_stats_lagging for account")
		defer cleanupSQLIntegration(t, cn, "drop snapshot index_stats_lagging")
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
		require.NoError(t, conn.QueryRowContext(ctx, "select metrics from mo_index_stats('index_stats_async','vectors','ivf','{}') as s where row_type='index'").Scan(&deepJSON))
		require.NoError(t, json.Unmarshal([]byte(deepJSON), &deep))
		require.NotContains(t, deep["analysis"], "status", "same-view durable progress permits coverage analysis")
		require.Contains(t, deep["analysis"], "recall")
		require.Equal(t, true, deep["async"].(map[string]any)["caught_up"])

		var historicalJSON string
		require.NoError(t, conn.QueryRowContext(ctx, "select metrics from ivf_health {snapshot='index_stats_lagging'}").Scan(&historicalJSON))
		var historical map[string]any
		require.NoError(t, json.Unmarshal([]byte(historicalJSON), &historical))
		require.Equal(t, maintenanceBeforeCatchup, historical["async"])
		require.Equal(t, "index_not_caught_up", historical["analysis"].(map[string]any)["status"])
	})
}

// Reuses the existing one-CN fixture. Two old clusters are followed by a remote
// third cluster straddling their old boundary: true neighbors cross old buckets.
func testIndexHealthSQL(t *testing.T, ctx context.Context, conn *sql.Conn, exec func(string)) {
	t.Helper()
	exec("create table health_vectors(id bigint primary key,v vecf64(2))")
	values := make([]string, 0, 32)
	for i := 0; i < 32; i++ {
		x := -10
		if i%2 == 1 {
			x = 10
		}
		values = append(values, fmt.Sprintf("(%d,'[%d,%g]')", i, x, float64((i/2)%2)*.02-.01))
	}
	exec("insert into health_vectors values" + strings.Join(values, ","))
	exec("create index ivf using ivfflat on health_vectors(v) lists=2 op_type 'vector_l2_ops' kmeans_train_percent=100")
	read := func(deep bool) map[string]any {
		t.Helper()
		args := ""
		if deep {
			args = `, '{"sample_count":256,"retrain":true}'`
		}
		var raw string
		require.NoError(t, conn.QueryRowContext(ctx, "select metrics from mo_index_stats('index_stats_test','health_vectors','ivf'"+args+") as s where row_type='index'").Scan(&raw))
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(raw), &m))
		return m
	}
	initial := read(true)
	require.Equal(t, 0., initial["growth_ratio"])
	require.NotContains(t, initial, "baseline_status")
	recallAtOne := func(m map[string]any) float64 {
		t.Helper()
		a := m["analysis"].(map[string]any)
		r := a["recall"].(map[string]any)
		require.NotContains(t, r, "status")
		curve := r["curve"].([]any)
		return curve[0].(map[string]any)["recall"].(float64)
	}
	require.Equal(t, 1., recallAtOne(initial))
	require.Equal(t, float64(32)/60, initial["training_sample_ratio"])
	values = values[:0]
	for i := 0; i < 96; i++ {
		x := -.01
		if i%2 == 1 {
			x = .01
		}
		values = append(values, fmt.Sprintf("(%d,'[%g,%g]')", i+32, x, 1000+float64(i/2)/10))
	}
	exec("insert into health_vectors values" + strings.Join(values, ","))
	normal := read(false)
	require.NotContains(t, normal, "analysis")
	require.Equal(t, 3., normal["growth_ratio"])
	drift := normal["drift"].(map[string]any)
	require.NotContains(t, drift, "status")
	require.Greater(t, drift["new_old_residual_ratio"].(float64), 100.)
	bad := read(true)
	analysis := bad["analysis"].(map[string]any)
	coverage := analysis["recall"].(map[string]any)
	require.Contains(t, []any{"below_target_at_probe_budget", "new_data_recall_drop"}, coverage["status"])
	require.Less(t, coverage["curve"].([]any)[0].(map[string]any)["recall"].(float64), .7)
	require.Equal(t, float64(96), coverage["new_query_count"])
	require.Equal(t, float64(32), coverage["old_query_count"])
	require.Greater(t, analysis["retrain"].(map[string]any)["sse_ratio"].(float64), 1.2)
	require.Len(t, bad["suggestions"], 2, "one REINDEX and one probe_limit suggestion")
	exec("alter table health_vectors alter reindex ivf ivfflat")
	healthy := read(true)
	require.Equal(t, 0., healthy["growth_ratio"])
	require.GreaterOrEqual(t, recallAtOne(healthy), .95)
	var meta, raw string
	require.NoError(t, conn.QueryRowContext(ctx, "select i.index_table_name from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id=t.rel_id where t.reldatabase='index_stats_test' and t.relname='health_vectors' and i.name='ivf' and i.algo_table_type='metadata'").Scan(&meta))
	require.NoError(t, conn.QueryRowContext(ctx, "select __mo_index_val from "+sqlquote.Ident(meta)+" where __mo_index_key='diagnostic_build'").Scan(&raw))
	base, status := diagnostic.DecodeBuild(raw, 1)
	require.Empty(t, status)
	require.Equal(t, int64(128), base.Rows)
	require.Equal(t, int64(128), base.TrainingSamples)
	require.True(t, base.Historical)
	base.IndexVersion = 0
	encoded, err := json.Marshal(base)
	require.NoError(t, err)
	exec("update " + sqlquote.Ident(meta) + " set __mo_index_val=" + sqlquote.String(string(encoded)) + " where __mo_index_key='diagnostic_build'")
	require.Equal(t, "baseline_stale", read(false)["baseline_status"])
	exec("delete from " + sqlquote.Ident(meta) + " where __mo_index_key='diagnostic_build'")
	legacy := read(false)
	require.Equal(t, "baseline_missing", legacy["baseline_status"])
	require.NotContains(t, legacy, "growth_ratio")
	require.NotContains(t, legacy, "drift")
	var invalid string
	for _, q := range []string{
		"select metrics from mo_index_stats('index_stats_test','health_vectors','ivf','{\"sample_count\":0}') as s",
		"select metrics from mo_index_stats('index_stats_test','health_vectors','ivf','', '{}') as s",
		"select metrics from mo_index_analyze('index_stats_test','health_vectors','ivf') as s",
	} {
		require.Error(t, conn.QueryRowContext(ctx, q).Scan(&invalid))
	}
}
