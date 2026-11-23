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

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestMoIndexStatsFulltextSQL(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists index_stats_fulltext", "drop user if exists fulltext_stats_reader")
		exec := func(query string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		exec("create database index_stats_fulltext")
		exec("use index_stats_fulltext")
		checkFulltextTailFrames(t, ctx, cn.ServiceID(), exec)
		exec("set experimental_fulltext2_index=1")
		exec("create user fulltext_stats_reader identified by '111'")
		for _, tc := range []struct {
			name, kind, options string
			version             int
			async               bool
		}{
			{"classic", "fulltext", "", 1, false},
			{"classic_async", "fulltext", " async", 1, true},
			{"v2", "fulltext2", "", 2, true},
		} {
			if !t.Run(tc.name, func(t *testing.T) {
				exec := func(query string) {
					t.Helper()
					_, err := conn.ExecContext(ctx, query)
					require.NoError(t, err, query)
				}
				exec("create table " + tc.name + "(id bigint primary key,body text)")
				exec("insert into " + tc.name + " values(1,'apple apple banana'),(2,'apple pear'),(3,'banana')")
				exec("create " + tc.kind + " index ft on " + tc.name + "(body)" + tc.options)
				query := "select index_type,row_type,object_id,metrics from mo_index_stats('index_stats_fulltext','" + tc.name + "','ft') as s"
				read := func() (map[string]any, error) {
					rows, err := conn.QueryContext(ctx, query)
					if err != nil {
						return nil, err
					}
					defer rows.Close()
					if !rows.Next() {
						return nil, fmt.Errorf("missing fulltext summary: %v", rows.Err())
					}
					var indexType, rowType, encoded string
					var object sql.NullString
					if err = rows.Scan(&indexType, &rowType, &object, &encoded); err != nil {
						return nil, err
					}
					require.Equal(t, "fulltext", indexType)
					require.Equal(t, "index", rowType)
					require.False(t, object.Valid)
					require.False(t, rows.Next(), "fulltext must never enumerate the vocabulary")
					if err = rows.Err(); err != nil {
						return nil, err
					}
					var metrics map[string]any
					err = json.Unmarshal([]byte(encoded), &metrics)
					return metrics, err
				}
				wait := func(docs, terms, mean float64) map[string]any {
					t.Helper()
					var metrics map[string]any
					var lastErr error
					ready := func() bool {
						metrics, lastErr = read()
						if lastErr != nil || metrics["document_count"] != docs || metrics["term_count"] != terms {
							return false
						}
						if tc.async {
							return metrics["async"].(map[string]any)["caught_up"] == true
						}
						return true
					}
					if tc.async {
						require.Eventually(t, ready, 45*time.Second, 100*time.Millisecond)
					} else {
						require.True(t, ready(), "%v %v", lastErr, metrics)
					}
					require.NoError(t, lastErr)
					require.InDelta(t, mean, metrics["avg_document_frequency"], 1e-12)
					return metrics
				}
				metrics := wait(3, 3, 5.0/3)
				require.Equal(t, float64(tc.version), metrics["version"])
				if tc.version == 2 {
					checkConcurrentStatsGeneration(t, ctx, db, conn)
					require.NotContains(t, metrics, "health_status")
					for _, key := range []string{"tail_chunk_count", "tail_frame_count", "tail_size_bytes"} {
						require.Contains(t, metrics, key)
					}
					require.Empty(t, metrics["suggestions"])
				} else {
					require.NotContains(t, metrics, "tail_chunk_count")
				}

				require.Equal(t, []any{"body"}, metrics["columns"])
				require.Equal(t, "ngram", metrics["parser"])
				require.Equal(t, float64(1), metrics["min_document_frequency"])
				require.Equal(t, float64(2), metrics["max_document_frequency"])
				for _, key := range []string{"centroid_count", "index_version", "lists", "diagnosis", "status"} {
					require.NotContains(t, metrics, key)
				}
				require.Len(t, metrics["storage"], 3)
				var executor *iscp.ISCPTaskExecutor
				paused := false
				if tc.async {
					require.Equal(t, "async", metrics["maintenance_mode"])
					require.Eventually(t, func() bool { var ok bool; executor, ok = iscp.GetExecutorRuntime(cn.ServiceID()); return ok }, 30*time.Second, 100*time.Millisecond)
					require.NoError(t, executor.Pause())
					paused = true
					defer func() {
						if paused {
							require.NoError(t, executor.Resume())
						}
					}()
				} else {
					require.Equal(t, "sync", metrics["maintenance_mode"])
					require.NotContains(t, metrics, "async")
				}
				exec("update " + tc.name + " set body='pear pear' where id=1")
				exec("delete from " + tc.name + " where id=3")
				if tc.async {
					metrics, err = read()
					require.NoError(t, err)
					require.Equal(t, float64(3), metrics["document_count"], "describe consumed index content")
					require.Equal(t, false, metrics["async"].(map[string]any)["caught_up"])
					require.NoError(t, executor.Resume())
					paused = false
				}
				metrics = wait(2, 2, 1.5)
				if tc.version == 2 {
					require.NotContains(t, metrics, "health_status")
					require.Greater(t, metrics["tail_chunk_count"].(float64), 0.)
					require.Greater(t, metrics["tail_frame_count"].(float64), 0.)
					require.Greater(t, metrics["tail_size_bytes"].(float64), 0.)
					require.InDelta(t, 1.0/3, metrics["base_dead_ratio"], 1e-12)
				}
				require.Equal(t, float64(1), metrics["min_document_frequency"])
				require.Equal(t, float64(2), metrics["max_document_frequency"])
				reader, err := sql.Open("mysql", fmt.Sprintf("fulltext_stats_reader:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
				require.NoError(t, err)
				defer reader.Close()
				rows, err := reader.QueryContext(ctx, query)
				if rows != nil {
					defer rows.Close()
					require.NoError(t, rows.Err())
				}
				require.Error(t, err, "require SELECT on the source")
				exec("grant select on table index_stats_fulltext." + tc.name + " to public")
				var encoded string
				require.NoError(t, reader.QueryRowContext(ctx, "select metrics from mo_index_stats('index_stats_fulltext','"+tc.name+"','ft') as s").Scan(&encoded))
				exec("delete from " + tc.name)
				metrics = wait(0, 0, 0)
				require.Equal(t, float64(0), metrics["min_document_frequency"])
				require.Equal(t, float64(0), metrics["max_document_frequency"])
				if tc.version == 2 {
					// Two compact-to-empty cycles reuse the first tail position with
					// different documents and vocabularies.
					exec("alter table v2 alter reindex ft fulltext2 merge force_sync")
					exec("insert into v2 values (10,'apple')")
					wait(1, 1, 1)
					exec("delete from v2 where true")
					wait(0, 0, 0)
					exec("alter table v2 alter reindex ft fulltext2 merge force_sync")
					exec("insert into v2 values (11,'apple banana'),(12,'pear')")
					wait(2, 3, 1)
				}
			}) {
				return
			}
		}
	})
}

func checkConcurrentStatsGeneration(t *testing.T, ctx context.Context, db *sql.DB, conn *sql.Conn) {
	t.Helper()
	var storage string
	err := conn.QueryRowContext(ctx, `SELECT i.index_table_name FROM mo_catalog.mo_indexes i
 JOIN mo_catalog.mo_tables t ON t.rel_id=i.table_id
 WHERE t.reldatabase='index_stats_fulltext' AND t.relname='v2' AND i.name='ft' AND i.algo_table_type='ftv2_index'`).Scan(&storage)
	require.NoError(t, err)
	veccache.Cache.RemoveAllGenerations(storage, "test")
	var calls errgroup.Group
	for range 4 {
		calls.Go(func() error {
			var docs int
			err := db.QueryRowContext(ctx, `SELECT CAST(json_extract(metrics,'$.document_count') AS SIGNED)
    FROM mo_index_stats('index_stats_fulltext','v2','ft') s`).Scan(&docs)
			if err == nil && docs != 3 {
				return fmt.Errorf("document count after concurrent load: got %d, want 3", docs)
			}
			return err
		})
	}
	require.NoError(t, calls.Wait())
	entries := 0
	for _, k := range veccache.Cache.Keys() {
		if strings.HasPrefix(k, storage+"@") {
			entries++
		}
	}
	require.Equal(t, 1, entries, "different autocommit snapshots must reuse the same diagnostic load")
}

func checkFulltextTailFrames(t *testing.T, ctx context.Context, service string, exec func(string)) {
	t.Helper()
	cfg := fulltext2.TableConfig{DbName: "index_stats_fulltext", IndexTable: "chunks", MetadataTable: "frames"}
	sp := sqlexec.NewSqlProcessWithContext(sqlexec.NewSqlContext(ctx, service, nil, 0, nil))
	// Hide the payload column to prove the complete-metadata query never projects it.
	exec("create table chunks(index_id varchar(64), chunk_id bigint, tag int, payload longblob)")
	exec("create table frames(index_id varchar(64), filesize bigint)")
	size := int64(vectorindex.MaxChunkSize)
	exec(fmt.Sprintf("insert into chunks values ('cdc_tail',1,1,repeat('a',%d)),('cdc_tail',2,1,'b')", size))
	exec(fmt.Sprintf("insert into frames values ('cdc_tail:1',%d)", size+1))
	usage, err := fulltext2.ReadTailUsage(sp, cfg)
	require.NoError(t, err)
	require.EqualValues(t, 2, usage.Chunks)
	require.Equal(t, size+1, usage.Bytes)
	require.NotNil(t, usage.Frames)
	require.EqualValues(t, 1, *usage.Frames)

	// A legacy chunk can coexist with newer frames that have metadata.
	exec("alter table chunks rename column payload to data")
	exec("insert into chunks values ('cdc_tail',3,1,'c')")
	usage, err = fulltext2.ReadTailUsage(sp, cfg)
	require.NoError(t, err)
	require.EqualValues(t, 3, usage.Chunks)
	require.Equal(t, size+2, usage.Bytes)
	require.Nil(t, usage.Frames)
}
