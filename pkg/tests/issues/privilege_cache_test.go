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
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
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
		// Each protocol owns a distinct session cache: one query must not refresh
		// another protocol's cache and hide a missing execution-time check.
		readers := []*sql.Conn{open(1, "auth_cache#learner#reader"), open(1, "auth_cache#learner#reader"), open(1, "auth_cache#learner#reader")}
		allReadersExec := func(stmt string) {
			t.Helper()
			for _, reader := range readers {
				mustExec(t, ctx, reader, stmt)
			}
		}
		allReadersExec("set enable_privilege_cache=on")
		mustExec(t, ctx, readers[1], "prepare cached_read from 'select id from app.t where id=1'")
		prepared, err := readers[2].PrepareContext(ctx, "select id from app.t where id=?")
		require.NoError(t, err)
		defer prepared.Close()
		check := func(allowed bool) {
			t.Helper()
			var got int
			for i, query := range []string{"select id from app.t where id=1", "execute cached_read", "binary"} {
				var row *sql.Row
				if query == "binary" {
					row = prepared.QueryRowContext(ctx, 1)
				} else {
					row = readers[i].QueryRowContext(ctx, query)
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
		// A data write must not change authorization catalog contents. The
		// conservative version also includes physical generations/watermarks:
		// background catalog replay can change it without changing these rows.
		catalogContents := func() map[string][sha256.Size]byte {
			t.Helper()
			contents := make(map[string][sha256.Size]byte)
			for _, query := range []string{
				"select * from mo_catalog.mo_database where datname='app'",
				"select * from mo_catalog.mo_tables where reldatabase='app'",
				"select * from mo_catalog.mo_user", "select * from mo_catalog.mo_role",
				"select * from mo_catalog.mo_user_grant", "select * from mo_catalog.mo_role_grant",
				"select * from mo_catalog.mo_role_privs",
			} {
				rows, err := owner.QueryContext(ctx, query)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, rows.Close()) })
				columns, err := rows.Columns()
				require.NoError(t, err)
				var encodedRows []string
				for rows.Next() {
					values := make([]any, len(columns))
					pointers := make([]any, len(columns))
					for i := range values {
						pointers[i] = &values[i]
					}
					require.NoError(t, rows.Scan(pointers...))
					encoded, err := json.Marshal(values)
					require.NoError(t, err)
					encodedRows = append(encodedRows, string(encoded))
				}
				require.NoError(t, rows.Err())
				require.NoError(t, rows.Close())
				sort.Strings(encodedRows)
				encoded, err := json.Marshal(encodedRows)
				require.NoError(t, err)
				// Compare all values without printing credential catalog contents.
				contents[query] = sha256.Sum256(encoded)
			}
			return contents
		}
		cn, err := c.GetCNService(1)
		require.NoError(t, err)
		eng := cn.RawService().(cnservice.Service).GetEngine().(*disttae.Engine)
		var account uint32
		require.NoError(t, admin.QueryRowContext(ctx, "select account_id from mo_catalog.mo_account where account_name='auth_cache'").Scan(&account))
		version, _, err := eng.GetPrivilegeCacheVersion(ctx, account, timestamp.Timestamp{})
		require.NoError(t, err)
		require.True(t, version != (disttae.PrivilegeCacheVersion{}))
		beforeDataWrite := catalogContents()
		mustExec(t, ctx, owner, "insert into app.t values (2)")
		var dataRows int
		require.NoError(t, owner.QueryRowContext(ctx, "select count(*) from app.t").Scan(&dataRows))
		require.Equal(t, 2, dataRows)
		require.Equal(t, beforeDataWrite, catalogContents(), "ordinary DML must preserve authorization contents")
		check(true)
		canceled, stop := context.WithCancel(ctx)
		stop()
		current, _, err := eng.GetPrivilegeCacheVersion(canceled, account, timestamp.Timestamp{})
		require.ErrorIs(t, err, context.Canceled)
		require.True(t, current == (disttae.PrivilegeCacheVersion{}))
		allReadersExec("begin")
		check(true)
		mustExec(t, ctx, owner, "revoke select on table app.t from reader")
		require.NotEqual(t, beforeDataWrite, catalogContents(), "the contents oracle must observe a real grant change")
		check(false)
		allReadersExec("rollback")
		mustExec(t, ctx, owner, "grant select on table app.t to reader")
		check(true)
		mustExec(t, ctx, owner, "revoke reader from learner")
		check(false)
		mustExec(t, ctx, owner, "grant reader to learner")
		check(true)
		allReadersExec("set role public")
		check(false)
		allReadersExec("set role reader")
		check(true)
		mustExec(t, ctx, owner, "drop table app.t")
		mustExec(t, ctx, owner, "create table app.t(id int primary key)")
		mustExec(t, ctx, owner, "insert into app.t values (1)")
		check(false)
		mustExec(t, ctx, owner, "grant select on table app.t to reader")
		check(true)
		for _, setting := range []string{"set clear_privilege_cache=on", "set enable_privilege_cache=off", "set enable_privilege_cache=on"} {
			allReadersExec(setting)
			check(true)
		}
		consume := func(conn *sql.Conn, query string) error {
			rows, err := conn.QueryContext(ctx, query)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
			}
			return rows.Err()
		}
		t.Run("nested_authorization", func(t *testing.T) {
			// Each shell has its own warm cache before a remote revoke.
			for _, query := range []string{
				"set @nested_value=(select id from app.t)",
				"explain select id from app.t",
				"explain analyze select id from app.t",
				"explain phyplan select id from app.t",
				"explain analyze force execute nested_read",
				"execute nested_set",
				"execute nested_explain",
				"binary set",
			} {
				t.Run(query, func(t *testing.T) {
					conn := open(1, "auth_cache#learner#reader")
					mustExec(t, ctx, conn, "set enable_privilege_cache=on")
					var binary *sql.Stmt
					switch query {
					case "explain analyze force execute nested_read", "execute nested_explain":
						mustExec(t, ctx, conn, "prepare nested_read from 'select id from app.t'")
						if query == "execute nested_explain" {
							mustExec(t, ctx, conn, "prepare nested_explain from 'explain analyze force execute nested_read'")
						}
					case "execute nested_set":
						mustExec(t, ctx, conn, "prepare nested_set from 'set @nested_value=(select id from app.t)'")
					case "binary set":
						var err error
						binary, err = conn.PrepareContext(ctx, "set @nested_value=(select id from app.t)") //nolint:sqlclosecheck // closed by the deferred Close below; the run closure captures binary, which defeats the analyzer's tracking
						require.NoError(t, err)
						defer binary.Close()
					}
					run := func() error {
						if binary != nil {
							_, err := binary.ExecContext(ctx)
							return err
						}
						return consume(conn, query)
					}
					require.NoError(t, run())
					mustExec(t, ctx, conn, "set @nested_value=-1")
					mustExec(t, ctx, owner, "revoke select on table app.t from reader")
					defer mustExec(t, ctx, owner, "grant select on table app.t to reader")
					require.ErrorContains(t, run(), "privilege")
					var value int
					require.NoError(t, conn.QueryRowContext(ctx, "select @nested_value").Scan(&value))
					require.Equal(t, -1, value, "denied evaluation must not assign a value")
				})
			}
			conn := open(1, "auth_cache#learner#reader")
			require.ErrorContains(t, consume(conn, "explain analyze update app.t set id=7"), "privilege")
			var id int
			require.NoError(t, owner.QueryRowContext(ctx, "select id from app.t").Scan(&id))
			require.Equal(t, 1, id)
		})
		t.Run("grant_option_membership", func(t *testing.T) {
			mustExec(t, ctx, owner, "create role recipient")
			mustExec(t, ctx, owner, "grant select on table app.t to reader with grant option")
			conn := open(1, "auth_cache#learner#reader")
			const grant = "grant select on table app.t to recipient"
			mustExec(t, ctx, conn, grant)
			mustExec(t, ctx, owner, "revoke reader from learner")
			require.ErrorContains(t, consume(conn, grant), "privilege")
			mustExec(t, ctx, owner, "grant reader to learner")
			mustExec(t, ctx, conn, grant)
		})

		t.Run("catalog_fault", func(t *testing.T) {
			type control struct {
				query string
				conn  *sql.Conn
			}
			controls := make([]control, 0, 10)
			for _, user := range []string{"learner#reader", "admin#accountadmin"} {
				for _, query := range []string{"set clear_privilege_cache=on", "set enable_privilege_cache=off", "show warnings", "set clear_privilege_cache=1", "set @constant=1+1"} {
					conn := open(1, "auth_cache#"+user)
					mustExec(t, ctx, conn, "set enable_privilege_cache=on")
					require.NoError(t, consume(conn, "select id from app.t"))
					controls = append(controls, control{query, conn})
				}
			}
			check(true)
			if fault.Enable() {
				defer fault.Disable()
			}
			remove, err := objectio.InjectLogging(objectio.FJ_CNSubscribeTableFail, "mo_catalog", "mo_role_privs", 0, true)
			require.NoError(t, err)
			defer remove()
			for i, tc := range controls {
				t.Run(fmt.Sprintf("control_%d/%s", i, tc.query), func(t *testing.T) {
					require.NoError(t, consume(tc.conn, tc.query))
				})
			}
			// Actual grant consumers must fail closed despite a warm cache.
			for i, query := range []string{"select id from app.t where id=1", "execute cached_read", "binary"} {
				var id int
				var err error
				if query == "binary" {
					err = prepared.QueryRowContext(ctx, 1).Scan(&id)
				} else {
					err = readers[i].QueryRowContext(ctx, query).Scan(&id)
				}
				require.ErrorContains(t, err, "injected subscribe table err", query)
			}
		})
		check(true)

	})
}
