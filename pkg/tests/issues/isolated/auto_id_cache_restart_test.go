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

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

// This test owns its cluster because it stops every service and reloads the
// same data with a different immutable CN configuration. It must not stop the
// package's shared cluster or model rollout by mutating a running service.
func TestAutoIDCacheRestartAndDisabledNode(t *testing.T) {
	c, err := embed.StartTestCluster(embed.WithCNCount(1), embed.WithPreStart(func(service embed.ServiceOperator) {
		if service.ServiceType() == metadata.ServiceType_CN {
			service.Adjust(func(cfg *embed.ServiceConfig) {
				cfg.CN.Frontend.SkipCheckUser = false
				cfg.CN.AutoIncrement.EnableAutoIDCache = true
			})
		}
	}))
	if c != nil {
		t.Cleanup(func() { require.NoError(t, c.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	connect := func() *sql.DB {
		t.Helper()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		require.NoError(t, db.PingContext(ctx))
		require.NoError(t, waitSystemBootstrap(ctx, db))
		return db
	}
	db := connect()
	exec := func(statement string) {
		t.Helper()
		_, err := db.ExecContext(ctx, statement)
		require.NoErrorf(t, err, "SQL: %s", statement)
	}
	exec("create database ai_cache_recovery")
	exec("create table ai_cache_recovery.t(id bigint auto_increment primary key) auto_id_cache=1")
	exec("insert into ai_cache_recovery.t values(NULL)")
	restart := func(enabled bool) {
		t.Helper()
		require.NoError(t, db.Close())
		require.NoError(t, c.Close())
		cn.Adjust(func(cfg *embed.ServiceConfig) { cfg.CN.AutoIncrement.EnableAutoIDCache = enabled })
		require.NoError(t, c.Start())
		db = connect()
	}
	restart(false)
	var name, ddl string
	require.NoError(t, db.QueryRowContext(ctx, "show create table ai_cache_recovery.t").Scan(&name, &ddl))
	require.Contains(t, ddl, "AUTO_ID_CACHE=1", "disabled nodes must not falsify persisted metadata")
	for _, statement := range []string{
		"insert into ai_cache_recovery.t values(NULL)",
		"truncate table ai_cache_recovery.t",
		"create table ai_cache_recovery.rejected(id int auto_increment primary key) auto_id_cache=1",
		"create table ai_cache_recovery.rejected_like like ai_cache_recovery.t",
	} {
		_, err := db.ExecContext(ctx, statement)
		require.ErrorContains(t, err, "AUTO_ID_CACHE is disabled", statement)
	}
	var count int
	require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='ai_cache_recovery' and relname like 'rejected%'").Scan(&count))
	require.Zero(t, count, "DDL rejection must not publish a partial table")
	// Existing ALTER starting-value maintenance does not allocate a range or
	// load the CACHE policy. It remains available and must preserve metadata.
	exec("alter table ai_cache_recovery.t auto_increment=2")
	require.NoError(t, db.QueryRowContext(ctx, "show create table ai_cache_recovery.t").Scan(&name, &ddl))
	require.Contains(t, ddl, "AUTO_ID_CACHE=1")
	exec("create table ai_cache_recovery.default_policy(id int auto_increment primary key) auto_id_cache=0")
	exec("insert into ai_cache_recovery.default_policy values(NULL)")
	restart(true)
	require.NoError(t, db.QueryRowContext(ctx, "show create table ai_cache_recovery.t").Scan(&name, &ddl))
	require.Contains(t, ddl, "AUTO_ID_CACHE=1")
	require.NoError(t, db.QueryRowContext(ctx, "select internal_auto_increment('ai_cache_recovery','t')").Scan(&count))
	require.Equal(t, 2, count, "disabled attempts must not reserve IDs")
	exec("insert into ai_cache_recovery.t values(NULL)")
	require.NoError(t, db.QueryRowContext(ctx, "select max(id) from ai_cache_recovery.t").Scan(&count))
	require.Equal(t, 2, count)
	exec("drop database ai_cache_recovery")
}
