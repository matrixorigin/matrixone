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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/stretchr/testify/require"
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
			}) {
				return
			}
		}
	})
}
