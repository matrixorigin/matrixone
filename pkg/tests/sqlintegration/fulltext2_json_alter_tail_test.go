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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/stretchr/testify/require"
)

// The rebuilt generation must complete a subsequent, unindexed row without
// crossing the ALTER's schema boundary. Pause only the test fixture's ISCP
// executor after the rebuild, so the gap cannot disappear before the read.
func TestFulltext2JSONProbeAfterAlterTail(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const schema = "ft2_json_alter_tail"
		exec := func(statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		exec("create database " + schema)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		for _, statement := range []string{
			"use " + schema,
			"set experimental_fulltext2_index=1",
			"create table t(id bigint primary key,j json)",
			`insert into t values (1,'{"foo":"needle"}'),(2,'{"foo":"hay"}'),(3,'{"foo":"needle"}')`,
			"create fulltext2 index ftj on t(j) with parser json",
			"alter table t add column extra int default 0",
		} {
			exec(statement)
		}
		indexTable := func(kind string) string {
			t.Helper()
			var name string
			err := conn.QueryRowContext(ctx, `select index_table_name from mo_catalog.mo_indexes
				where name='ftj' and algo_table_type=? and table_id in
				(select rel_id from mo_catalog.mo_tables where reldatabase=database() and relname='t') limit 1`, kind).Scan(&name)
			require.NoError(t, err)
			require.NotEmpty(t, name)
			return name
		}
		meta, storage := indexTable("ftv2_meta"), indexTable("ftv2_index")
		metaQuery := fmt.Sprintf("select coalesce(max(build_ts),0) from `%s`.`%s`", schema, strings.ReplaceAll(meta, "`", "``"))
		var built int64
		require.Eventually(t, func() bool {
			return conn.QueryRowContext(ctx, metaQuery).Scan(&built) == nil && built > 0
		}, 90*time.Second, 100*time.Millisecond, "rebuilt index must have a committed generation")
		// A fresh build_ts does not by itself prove a single-schema interval. Bake
		// one post-rebuild write into the tail so the next gap starts in the
		// current schema, then stop the executor before creating that gap.
		chunkQuery := fmt.Sprintf("select coalesce(max(chunk_id),-1) from `%s`.`%s` where index_id='cdc_tail' and tag=1", schema, strings.ReplaceAll(storage, "`", "``"))
		var baseline, chunk int64
		require.NoError(t, conn.QueryRowContext(ctx, chunkQuery).Scan(&baseline))
		exec(`insert into t values (6,'{"foo":"hay"}',0)`)
		require.Eventually(t, func() bool {
			return conn.QueryRowContext(ctx, chunkQuery).Scan(&chunk) == nil && chunk > baseline
		}, 90*time.Second, 100*time.Millisecond, "post-rebuild write must reach the index")
		var executor *iscp.ISCPTaskExecutor
		require.Eventually(t, func() bool {
			var ok bool
			executor, ok = iscp.GetExecutorRuntime(cn.ServiceID())
			return ok
		}, 30*time.Second, 100*time.Millisecond, "ISCP executor must be ready")
		require.NoError(t, executor.Pause())
		defer func() {
			if err := executor.Resume(); err != nil {
				t.Errorf("resume fixture ISCP executor: %v", err)
			}
		}()
		require.NoError(t, conn.QueryRowContext(ctx, metaQuery).Scan(&built))
		require.Positive(t, built)
		exec(`insert into t values (4,'{"foo":"needle"}',9),(5,'{"foo":"hay"}',9)`)
		queryIDs := func(statement string) []int64 {
			t.Helper()
			rows, err := conn.QueryContext(ctx, statement)
			require.NoError(t, err, statement)
			defer rows.Close()
			var ids []int64
			for rows.Next() {
				var id int64
				require.NoError(t, rows.Scan(&id))
				ids = append(ids, id)
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			return ids
		}
		var until string
		require.NoError(t, conn.QueryRowContext(ctx, "select watermark from change_watermark() w").Scan(&until))
		changes := fmt.Sprintf("select c.id from table_changes('%s','t','%d-0','%s') c where c.change_type='insert' and json_extract_string(c.j,'$.foo')='needle' order by c.id", schema, built, until)
		require.Equal(t, []int64{4}, queryIDs(changes), "the gap must fit the rebuilt schema")
		const query = "select id from t where json_extract_string(j,'$.foo')='needle' order by id"
		rows, err := conn.QueryContext(ctx, "explain verbose "+query)
		require.NoError(t, err)
		defer rows.Close()
		var plan []string
		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line))
			plan = append(plan, line)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		explained := strings.Join(plan, "\n")
		require.Contains(t, explained, "Fulltext Index Scan")
		require.Contains(t, explained, "Fulltext Index: ftj")
		require.Contains(t, explained, "table_changes(", "the stopped index must require a tail")
		require.Equal(t, []int64{1, 3, 4}, queryIDs(query))
		var after int64
		require.NoError(t, conn.QueryRowContext(ctx, metaQuery).Scan(&after))
		require.Equal(t, built, after, "the new row must not have entered the index")
	})
}
