// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

func TestPreparedSchemaCommitBeforeMetadata(t *testing.T) {
	// This pessimistic two-CN scenario owns a disposable generation rather than
	// the shared optimistic one-CN fixture used by neighboring tests.
	require.NoError(t, embed.CloseSingleCNBaseClusterTests())
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2), embed.WithPreStart(func(op embed.ServiceOperator) {
		if op.ServiceType() == metadata.ServiceType_CN {
			op.Adjust(func(cfg *embed.ServiceConfig) { cfg.CN.Txn.Mode = txn.TxnMode_Pessimistic.String() })
		}
	}))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	t.Cleanup(cancel)
	open := func(index int) *sql.Conn {
		cn, err := cluster.GetCNService(index)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
		return conn
	}
	first, second := open(0), open(1)
	exec := func(t *testing.T, conn *sql.Conn, statement string) {
		t.Helper()
		_, err := conn.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}
	exec(t, first, "create database prepared_schema_commit")
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		_, err := first.ExecContext(cleanup, "rollback")
		require.NoError(t, err)
		_, err = first.ExecContext(cleanup, "drop database prepared_schema_commit")
		require.NoError(t, err)
	})
	exec(t, first, "use prepared_schema_commit")
	exec(t, second, "use prepared_schema_commit")
	for _, protocol := range []string{"text", "binary"} {
		for _, scenario := range []string{"committed_before_execute", "empty_after_binding", "rows_after_binding"} {
			t.Run(protocol+"/"+scenario, func(t *testing.T) {
				table := protocol + "_" + scenario
				exec(t, first, "create table "+table+" (a int primary key,b int)")
				exec(t, first, "begin")
				t.Cleanup(func() {
					cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
					defer stop()
					_, err := first.ExecContext(cleanup, "rollback")
					require.NoError(t, err)
				})
				exec(t, first, "alter table "+table+" modify b bigint")
				wantRows := scenario == "rows_after_binding"
				if wantRows {
					exec(t, first, "insert into "+table+" values (2,2147483648)")
				}
				statement := "select * from " + table + " where a > ? for update"
				var prepared *sql.Stmt
				if protocol == "text" {
					exec(t, second, "prepare schema_commit from "+statement)
					exec(t, second, "set @a=1")
				} else {
					prepared, err = second.PrepareContext(ctx, statement)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, prepared.Close()) })
				}
				queryCtx, stopQuery := context.WithCancel(ctx)
				defer stopQuery()
				type result struct {
					types []string
					rows  [][2]int64
					err   error
				}
				query := func() result {
					var rows *sql.Rows
					var err error
					if protocol == "text" {
						rows, err = second.QueryContext(queryCtx, "execute schema_commit using @a")
					} else {
						rows, err = prepared.QueryContext(queryCtx, 1)
					}
					if err != nil {
						return result{err: err}
					}
					defer rows.Close()
					columns, err := rows.ColumnTypes()
					if err != nil {
						return result{err: err}
					}
					out := result{}
					for _, column := range columns {
						out.types = append(out.types, column.DatabaseTypeName())
					}
					for rows.Next() {
						var row [2]int64
						if err := rows.Scan(&row[0], &row[1]); err != nil {
							out.err = err
							return out
						}
						out.rows = append(out.rows, row)
					}
					out.err = errors.Join(rows.Err(), rows.Close())
					return out
				}
				var got result
				if scenario == "committed_before_execute" {
					exec(t, first, "commit")
					got = query()
				} else {
					wasEnabled := fault.Status()
					fault.Enable()
					t.Cleanup(func() {
						_, err := fault.RemoveFaultPoint(context.Background(), "prepared-result-metadata-bound")
						require.NoError(t, err)
						_, err = fault.RemoveFaultPoint(context.Background(), "prepared-result-metadata-waiters")
						require.NoError(t, err)
						if !wasEnabled {
							fault.Disable()
						}
					})
					require.NoError(t, fault.AddFaultPoint(ctx, "prepared-result-metadata-bound", ":::", "wait", 0, "", false))
					require.NoError(t, fault.AddFaultPoint(ctx, "prepared-result-metadata-waiters", ":::", "getwaiters", 0, "prepared-result-metadata-bound", false))
					done := make(chan result, 1)
					go func() { done <- query() }()
					joined := false
					defer func() {
						_, _ = fault.RemoveFaultPoint(context.Background(), "prepared-result-metadata-bound")
						stopQuery()
						if !joined {
							select {
							case <-done:
							case <-ctx.Done():
								t.Errorf("query cleanup: %v", ctx.Err())
							}
						}
					}()
					require.Eventually(t, func() bool {
						waiters, _, exists := fault.TriggerFault("prepared-result-metadata-waiters")
						return exists && waiters == 1
					}, 5*time.Second, time.Millisecond)
					exec(t, first, "commit")
					_, err := fault.RemoveFaultPoint(ctx, "prepared-result-metadata-bound")
					require.NoError(t, err)
					select {
					case got = <-done:
						joined = true
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				require.NoError(t, got.err)
				require.Equal(t, []string{"INT", "BIGINT"}, got.types)
				if wantRows {
					require.Equal(t, [][2]int64{{2, 2147483648}}, got.rows)
				} else {
					require.Empty(t, got.rows)
				}
				if protocol == "text" {
					exec(t, second, "deallocate prepare schema_commit")
				}
				// A later statement cannot inherit a failed/unfinished metadata publisher.
				var value int
				require.NoError(t, second.QueryRowContext(ctx, "select 7").Scan(&value))
				require.Equal(t, 7, value)
			})
		}
	}
	// Saved metadata has the same publication boundary, including zero rows.
	exec(t, second, "set save_query_result=on")
	for _, limit := range []string{"0", "1"} {
		t.Run("saved_result/limit_"+limit, func(t *testing.T) {
			rows, err := second.QueryContext(ctx, "/* save_result */ select b from text_rows_after_binding limit "+limit)
			require.NoError(t, err)
			defer rows.Close()
			columns, err := rows.ColumnTypes()
			require.NoError(t, err)
			require.Len(t, columns, 1)
			require.Equal(t, "BIGINT", columns[0].DatabaseTypeName())
			count := 0
			for rows.Next() {
				var value int64
				require.NoError(t, rows.Scan(&value))
				require.Equal(t, int64(2147483648), value)
				count++
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			if limit == "0" {
				require.Zero(t, count)
			} else {
				require.Equal(t, 1, count)
			}
			var saved, generated int
			require.NoError(t, second.QueryRowContext(ctx,
				"select savedRowCount,queryRowCount from meta_scan(last_query_id()) as r").Scan(&saved, &generated))
			require.Equal(t, count, saved)
			require.Equal(t, count, generated)
			if count > 0 {
				// The persisted schema must also decode the saved BIGINT exactly.
				replayed, err := second.QueryContext(ctx, "select b from result_scan(last_query_id(-2)) as r")
				require.NoError(t, err)
				defer replayed.Close()
				columns, err := replayed.ColumnTypes()
				require.NoError(t, err)
				require.Len(t, columns, 1)
				require.Equal(t, "BIGINT", columns[0].DatabaseTypeName())
				require.True(t, replayed.Next())
				var value int64
				require.NoError(t, replayed.Scan(&value))
				require.Equal(t, int64(2147483648), value)
				require.False(t, replayed.Next())
				require.NoError(t, replayed.Err())
				require.NoError(t, replayed.Close())
			}
		})
	}
}
