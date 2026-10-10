// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

// Own every service: close/reopen reloads the same durable data and must never
// stop the package's shared fixture. No session caches survive the restart.
func TestDatabaseDefaultsDurableRestart(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(1), embed.WithPreStart(func(service embed.ServiceOperator) {
		if service.ServiceType() == metadata.ServiceType_CN {
			service.Adjust(func(cfg *embed.ServiceConfig) { cfg.CN.Frontend.SkipCheckUser = false })
		}
	}))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	open := func() *sql.DB {
		t.Helper()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		require.NoError(t, waitSystemBootstrap(ctx, db))
		return db
	}
	db := open()
	exec := func(statement string) {
		t.Helper()
		_, err := db.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}
	exec("create database c03_durable collate utf8mb4_bin")
	exec("create table c03_durable.defaults(v varchar(8))")
	exec("alter table c03_durable.defaults default collate=utf8mb4_general_ci")
	exec("create table c03_durable.converted(id int primary key, v varchar(8), index iv(v)) collate=utf8mb4_bin")
	exec("insert into c03_durable.converted values (1,'ß'),(2,'ss')")
	exec("alter table c03_durable.converted convert to character set utf8mb4 collate utf8mb4_unicode_ci")
	exec("alter database c03_durable collate utf8mb4_unicode_ci")
	require.NoError(t, db.Close())
	require.NoError(t, cluster.Close())
	require.NoError(t, cluster.Start())
	db = open()
	exec("use c03_durable")
	var charset, rule string
	require.NoError(t, db.QueryRowContext(ctx, "select @@character_set_database,@@collation_database").Scan(&charset, &rule))
	require.Equal(t, "utf8mb4", charset)
	require.Equal(t, "utf8mb4_unicode_ci", rule)
	exec("create table inherited(v varchar(8))")
	exec("alter table defaults add column added varchar(8)")
	for column, expected := range map[string]string{"v": "utf8mb4_bin", "added": "utf8mb4_general_ci"} {
		require.NoError(t, db.QueryRowContext(ctx, "select collation_name from information_schema.columns where table_schema='c03_durable' and table_name='defaults' and column_name=?", column).Scan(&rule))
		require.Equal(t, expected, rule)
	}
	require.NoError(t, db.QueryRowContext(ctx, "select collation_name from information_schema.columns where table_schema='c03_durable' and table_name='inherited' and column_name='v'").Scan(&rule))
	require.Equal(t, "utf8mb4_unicode_ci", rule)
	var count int
	require.NoError(t, db.QueryRowContext(ctx, "select count(*) from converted force index(iv) where v='ss'").Scan(&count))
	require.Equal(t, 2, count)
	exec("drop database c03_durable")
}
