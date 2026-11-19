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
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestReplaceCompositeKeyPrunesFlushedObjects(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		schema := strings.ToLower(testutils.GetDatabaseName(t))
		exec := func(statement string) {
			t.Helper()
			_, err := db.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		exec("create database " + schema)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		target := schema + ".telemetry"
		exec("create table " + target + "(run_id varchar(64), sequence_id bigint unsigned, payload varchar(64), primary key(run_id,sequence_id))")
		inspect := func(qctx context.Context, command string) string {
			t.Helper()
			var result string
			require.NoError(t, db.QueryRowContext(qctx, "select mo_ctl('dn','inspect',?)", command).Scan(&result))
			require.NotContains(t, result, "run err:")
			return result
		}
		require.Contains(t, inspect(ctx, "merge show -t "+target), "auto merge: true")
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			require.Contains(t, inspect(cleanup, "merge switch on -t "+target), "merge enabled for table")
		}()
		require.Contains(t, inspect(ctx, "merge switch off -t "+target), "merge disabled for table")
		require.Contains(t, inspect(ctx, "merge show -t "+target), "auto merge: false")

		const rowsPerObject = 64
		const batchRows = 16
		metrics := regexp.MustCompile(`inputBlocks=(\d+) inputRows=(\d+)[^\n]* ReadSize=([^\n]*?) MemorySize=`)
		for object := range 16 {
			exec(fmt.Sprintf("insert into %s select 'current', result+%d, 'original' from generate_series(0,%d) g", target, object*rowsPerObject, rowsPerObject-1))
			exec("select mo_ctl('dn','flush','" + target + "')")
			service := cn.RawService().(cnservice.Service)
			frontier, _ := service.GetClock().Now()
			_, err := service.GetTxnClient().WaitLogTailAppliedAt(ctx, frontier)
			require.NoError(t, err)
			if object != 7 && object != 15 {
				continue
			}
			t.Run(fmt.Sprintf("objects_%d", object+1), func(t *testing.T) {
				var objects, persistedRows, maxLevel int64
				require.NoError(t, db.QueryRowContext(ctx, "select count(*),sum(rows_cnt),max(level) from metadata_scan('"+target+"','run_id') g").Scan(&objects, &persistedRows, &maxLevel))
				require.Equal(t, int64(object+1), objects)
				require.Equal(t, int64((object+1)*rowsPerObject), persistedRows)
				require.Zero(t, maxLevel, "the fixture must exercise unmerged TN-flushed objects")
				newKey := func(i int) int { return 10000 + i }
				for _, tc := range []struct {
					name, runID string
					key         func(int) int
					newRows     int
					maxBlocks   int
					duplicates  bool
					workspace   bool
				}{
					{name: "new_keys", runID: "current", key: newKey, newRows: batchRows},
					{name: "different_run", runID: "other", key: func(i int) int { return i }, newRows: batchRows},
					{name: "existing_keys", runID: "current", key: func(i int) int { return i }, maxBlocks: 1},
					{name: "mixed_keys", runID: "current", key: func(i int) int {
						if i < batchRows/2 {
							return i
						}
						return newKey(i)
					}, newRows: batchRows / 2, maxBlocks: 1},
					{name: "two_matching_objects", runID: "current", key: func(i int) int {
						if i < batchRows/2 {
							return i
						}
						return object*rowsPerObject + i
					}, maxBlocks: 2},
					{name: "duplicate_input", runID: "current", key: newKey, newRows: batchRows, duplicates: true},
					{name: "workspace_conflict", runID: "current", key: newKey, newRows: batchRows, workspace: true},
				} {
					t.Run(tc.name, func(t *testing.T) {
						tx, err := db.BeginTx(ctx, nil)
						require.NoError(t, err)
						defer tx.Rollback()
						if tc.workspace {
							_, err = tx.ExecContext(ctx, "insert into "+target+" values ('current',10000,'workspace')")
							require.NoError(t, err)
						}
						var values []string
						want := make(map[string]string, batchRows)
						for i := range batchRows {
							key := tc.key(i)
							if tc.duplicates {
								values = append(values, fmt.Sprintf("('%s',%d,'overwritten')", tc.runID, key))
							}
							values = append(values, fmt.Sprintf("('%s',%d,'replacement')", tc.runID, key))
							want[fmt.Sprintf("%s/%d", tc.runID, key)] = "replacement"
						}
						statement := "replace into " + target + " values " + strings.Join(values, ",")
						result, err := testutils.QueryTextResult(ctx, tx, "explain analyze "+statement)
						require.NoError(t, err)
						t.Log(result.Text)

						rows, err := tx.QueryContext(ctx, "select run_id,sequence_id,payload from "+target+" where payload<>'original'")
						require.NoError(t, err)
						defer rows.Close()
						got := make(map[string]string)
						for rows.Next() {
							var runID, payload string
							var sequence uint64
							require.NoError(t, rows.Scan(&runID, &sequence, &payload))
							key := fmt.Sprintf("%s/%d", runID, sequence)
							require.NotContains(t, got, key)
							got[key] = payload
						}
						require.NoError(t, rows.Err())
						require.NoError(t, rows.Close())
						require.Equal(t, want, got)
						var count int64
						require.NoError(t, tx.QueryRowContext(ctx, "select count(*) from "+target).Scan(&count))
						require.Equal(t, persistedRows+int64(tc.newRows), count)

						require.Contains(t, result.Text, "Table Scan on "+target)
						require.Contains(t, result.Text, "Runtime Filter Probe:")
						matches := metrics.FindAllStringSubmatch(result.Text, -1)
						require.Len(t, matches, 1, result.Text)
						blocks, err := strconv.Atoi(matches[0][1])
						require.NoError(t, err)
						inputRows, err := strconv.Atoi(matches[0][2])
						require.NoError(t, err)
						maxBlocks, maxRows := tc.maxBlocks, tc.maxBlocks*rowsPerObject
						if tc.workspace {
							// inputBlocks also counts the nonempty workspace batch;
							// use the scan's ReadSize to distinguish it from disk IO.
							maxBlocks++
							maxRows++
						}
						require.LessOrEqual(t, blocks, maxBlocks, "unrelated blocks must be pruned")
						require.LessOrEqual(t, inputRows, maxRows, "unrelated rows must not be scanned")
						if tc.maxBlocks == 0 {
							require.Equal(t, "0 bytes|0 bytes|0 bytes", matches[0][3], "new keys must not read persisted data")
						}
					})
				}
			})
		}
	})
}
