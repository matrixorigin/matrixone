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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func BenchmarkCloneDatabaseManyTables(b *testing.B) {
	RunBaseClusterTests(b, func(cluster Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(b, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(b, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(b.Context(), 10*time.Minute)
		defer cancel()
		conn, err := db.Conn(ctx)
		require.NoError(b, err)
		defer conn.Close()
		cloneCN, err := cluster.GetCNService(1)
		require.NoError(b, err)
		cloneDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cloneCN.GetServiceConfig().CN.Frontend.Port))
		require.NoError(b, err)
		defer cloneDB.Close()
		cloneConn, err := cloneDB.Conn(ctx)
		require.NoError(b, err)
		defer cloneConn.Close()
		for _, storage := range []string{"empty", "unflushed", "flushed", "foreign_keys"} {
			cardinalities := []int{1, 20, 100}
			if storage == "empty" {
				cardinalities = append(cardinalities, 500, 1000)
			}
			for _, tables := range cardinalities {
				b.Run(fmt.Sprintf("%s/tables=%d", storage, tables), func(b *testing.B) {
					exec := func(statement string) {
						_, err := conn.ExecContext(ctx, statement)
						require.NoError(b, err, statement)
					}
					const source = "clone_benchmark_source"
					const target = "clone_benchmark_target"
					exec("drop database if exists " + target)
					exec("drop database if exists " + source)
					exec("create database " + source)
					defer func() {
						cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
						defer cleanupCancel()
						for _, name := range []string{target, source} {
							_, err := conn.ExecContext(cleanupCtx, "drop database if exists "+name)
							assert.NoError(b, err)
						}
					}()
					for table := range tables {
						definition := "id int primary key, v int"
						if storage == "foreign_keys" && table > 0 {
							definition += fmt.Sprintf(", foreign key (v) references %s.t%d(id)", source, table-1)
						}
						exec(fmt.Sprintf("create table %s.t%d (%s)", source, table, definition))
						if storage == "unflushed" || storage == "flushed" {
							exec(fmt.Sprintf("insert into %s.t%d values (1, 10)", source, table))
						}
						if storage == "flushed" {
							exec(fmt.Sprintf("select mo_ctl('dn','flush','%s.t%d')", source, table))
						}
					}
					b.ResetTimer()
					for range b.N {
						_, err := cloneConn.ExecContext(ctx, "create database "+target+" clone "+source)
						require.NoError(b, err)
						b.StopTimer()
						var count int
						err = conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase = '"+target+"'").Scan(&count)
						require.NoError(b, err)
						require.Equal(b, tables, count)
						exec("drop database " + target)
						b.StartTimer()
					}
					b.StopTimer()
				})
			}
		}
	})
}
