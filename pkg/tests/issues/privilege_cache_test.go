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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

func TestPrivilegeCacheTracksRemoteCatalogChanges(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
		defer cancel()
		open := func(index int, user string) *sql.Conn {
			cn, err := c.GetCNService(index)
			require.NoError(t, err)
			db, err := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", user, cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			return conn
		}
		admin := open(0, "dump")
		mustExec(t, ctx, admin, "create account auth_cache admin_name 'admin' identified by '111'")
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			mustExec(t, cleanup, admin, "drop account auth_cache")
		}()
		owner := open(0, "auth_cache#admin#accountadmin")
		for _, stmt := range []string{
			"create database app", "create table app.t(id int primary key)", "insert into app.t values (1)",
			"create role reader", "create user learner identified by '111' default role reader",
			"grant reader to learner", "grant connect on account * to reader",
			"grant select on table app.t to reader",
		} {
			mustExec(t, ctx, owner, stmt)
		}
		reader := open(1, "auth_cache#learner#reader")
		mustExec(t, ctx, reader, "set enable_privilege_cache=on")
		mustExec(t, ctx, reader, "prepare cached_read from 'select id from app.t where id=1'")
		prepared, err := reader.PrepareContext(ctx, "select id from app.t where id=?")
		require.NoError(t, err)
		defer prepared.Close()
		check := func(allowed bool) {
			t.Helper()
			var got int
			for _, query := range []string{"select id from app.t where id=1", "execute cached_read", "binary"} {
				var row *sql.Row
				if query == "binary" {
					row = prepared.QueryRowContext(ctx, 1)
				} else {
					row = reader.QueryRowContext(ctx, query)
				}
				err := row.Scan(&got)
				if allowed {
					require.NoError(t, err)
					require.Equal(t, 1, got)
				} else {
					require.ErrorContains(t, err, "privilege")
				}
			}
		}
		check(true)
		check(true)
		// A data write must not invalidate authorization catalog contents.
		cn, err := c.GetCNService(1)
		require.NoError(t, err)
		eng := cn.RawService().(cnservice.Service).GetEngine().(*disttae.Engine)
		var account uint32
		require.NoError(t, admin.QueryRowContext(ctx, "select account_id from mo_catalog.mo_account where account_name='auth_cache'").Scan(&account))
		version, _, err := eng.GetPrivilegeCacheVersion(ctx, account, timestamp.Timestamp{})
		require.NoError(t, err)
		require.True(t, version != (disttae.PrivilegeCacheVersion{}))
		mustExec(t, ctx, owner, "insert into app.t values (2)")
		current, _, err := eng.GetPrivilegeCacheVersion(ctx, account, timestamp.Timestamp{})
		require.NoError(t, err)
		require.True(t, version == current)
		canceled, stop := context.WithCancel(ctx)
		stop()
		current, _, err = eng.GetPrivilegeCacheVersion(canceled, account, timestamp.Timestamp{})
		require.ErrorIs(t, err, context.Canceled)
		require.True(t, current == (disttae.PrivilegeCacheVersion{}))
		mustExec(t, ctx, reader, "begin")
		check(true)
		mustExec(t, ctx, owner, "revoke select on table app.t from reader")
		check(false)
		mustExec(t, ctx, reader, "rollback")
		mustExec(t, ctx, owner, "grant select on table app.t to reader")
		check(true)
		mustExec(t, ctx, owner, "revoke reader from learner")
		check(false)
		mustExec(t, ctx, owner, "grant reader to learner")
		check(true)
		mustExec(t, ctx, reader, "set role public")
		check(false)
		mustExec(t, ctx, reader, "set role reader")
		check(true)
		mustExec(t, ctx, owner, "drop table app.t")
		mustExec(t, ctx, owner, "create table app.t(id int primary key)")
		mustExec(t, ctx, owner, "insert into app.t values (1)")
		check(false)
		mustExec(t, ctx, owner, "grant select on table app.t to reader")
		check(true)
		for _, setting := range []string{"set clear_privilege_cache=on", "set enable_privilege_cache=off", "set enable_privilege_cache=on"} {
			mustExec(t, ctx, reader, setting)
			check(true)
		}
	})
}
