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

package multicn

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestCloneDeleteOwnershipAfterCommitTwoCN(t *testing.T) {
	t.Cleanup(func() { require.NoError(t, embed.CloseBaseClusterTests()) })
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		writerCN, err := cluster.GetCNService(0)
		require.NoError(t, err)
		readerCN, err := cluster.GetCNService(1)
		require.NoError(t, err)

		open := func(cn embed.ServiceOperator) *sql.DB {
			t.Helper()
			dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port)
			db, err := sql.Open("mysql", dsn)
			require.NoError(t, err)
			return db
		}
		writer := open(writerCN)
		defer writer.Close()
		reader := open(readerCN)
		defer reader.Close()

		const schema = "clone_delete_ownership_two_cn"
		const source = schema + ".src"
		const clone = schema + ".rewritten"

		exec := func(statement string) {
			t.Helper()
			_, err := writer.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		checkIDs := func(t *testing.T, db *sql.DB, table string, want []int) {
			t.Helper()
			rows, err := db.QueryContext(ctx, "select id from "+table+" order by id")
			require.NoError(t, err, table)
			defer rows.Close()

			var got []int
			for rows.Next() {
				var id int
				require.NoError(t, rows.Scan(&id), table)
				got = append(got, id)
			}
			require.NoError(t, rows.Err(), table)
			require.Equal(t, want, got, table)
		}

		exec("create database " + schema)
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			if _, err := writer.ExecContext(cleanup, "drop database "+schema); err != nil {
				t.Errorf("drop regression database: %v", err)
			}
		}()
		exec("create table " + source + " (id int primary key)")
		exec("insert into " + source + " values (1),(2),(3),(4)")
		const flushSource = "select mo_ctl('dn','flush','" + source + "')"
		exec(flushSource)
		exec("delete from " + source + " where id=2")
		exec(flushSource)
		sourceIDs := []int{1, 3, 4}
		checkIDs(t, writer, source, sourceIDs)

		writerClient := writerCN.RawService().(cnservice.Service).GetTxnClient()
		before := writerClient.GetLatestCommitTS()
		tx, err := writer.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		_, err = tx.ExecContext(ctx, "create table "+clone+" clone "+source)
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, "delete from "+clone+" where id=3")
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		// Wait until the separate reader CN has applied the writer's commit.
		// Both sessions then read the committed tables through the SQL frontend.
		committed := writerClient.GetLatestCommitTS()
		require.True(t, before.Less(committed), "clone transaction must advance the writer's commit timestamp")
		readerClient := readerCN.RawService().(cnservice.Service).GetTxnClient()
		snapshot, err := readerClient.WaitLogTailAppliedAt(ctx, committed)
		require.NoError(t, err)
		require.True(t, committed.Less(snapshot), "reader must apply logtail through the clone commit")

		t.Run("writer CN", func(t *testing.T) {
			checkIDs(t, writer, source, sourceIDs)
			checkIDs(t, writer, clone, []int{1, 4})
		})
		t.Run("reader CN", func(t *testing.T) {
			checkIDs(t, reader, source, sourceIDs)
			checkIDs(t, reader, clone, []int{1, 4})
		})
	})
}
