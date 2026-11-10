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
	"fmt"
	"testing"
	"time"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestShowTablesPrivilegeProtocolCompatibility(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn,
			"drop user if exists metadata_compat_user",
			"drop role if exists metadata_compat_outer, metadata_compat_middle, metadata_compat_inner",
			"drop database if exists metadata_compat_db",
		)
		rt := moruntime.ServiceRuntime(cn.ServiceID())
		original, existed := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
		require.True(t, existed)
		defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)

		port := cn.GetServiceConfig().CN.Frontend.Port
		admin, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer admin.Close()
		for _, statement := range []string{
			"create database metadata_compat_db",
			"create table metadata_compat_db.allowed(id int)",
			"create table metadata_compat_db.hidden(id int)",
			"create role metadata_compat_outer, metadata_compat_middle, metadata_compat_inner",
			"grant metadata_compat_inner to metadata_compat_middle",
			"grant metadata_compat_middle to metadata_compat_outer",
			"create user metadata_compat_user identified by '111' default role metadata_compat_outer",
			"grant connect on account * to metadata_compat_outer",
		} {
			execSQLRequire(t, ctx, admin, statement)
		}
		user, err := sql.Open("mysql", fmt.Sprintf(
			"sys#metadata_compat_user#metadata_compat_outer:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer user.Close()
		conn, err := user.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		_, err = conn.ExecContext(ctx, "set session enable_privilege_cache = off")
		require.NoError(t, err)

		const (
			listing      = "show tables from metadata_compat_db"
			grantDB      = "grant show tables on database metadata_compat_db to metadata_compat_inner"
			revokeDB     = "revoke show tables on database metadata_compat_db from metadata_compat_inner"
			grantObject  = "grant select on table metadata_compat_db.allowed to metadata_compat_outer"
			revokeObject = "revoke select on table metadata_compat_db.allowed from metadata_compat_outer"
		)
		query := func(t *testing.T, statement string) []string {
			t.Helper()
			rows, err := conn.QueryContext(ctx, statement)
			return readMetadataTableNames(t, rows, err)
		}
		for _, version := range []int64{defines.MORPCVersion40, defines.MORPCVersion41} {
			t.Run(fmt.Sprint(version), func(t *testing.T) {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
				execSQLRequire(t, ctx, admin, grantDB)
				execSQLRequire(t, ctx, admin, grantObject)
				require.Equal(t, []string{"allowed", "hidden"}, query(t, listing))
				require.Equal(t, []string{"allowed"}, query(t, listing+" like 'allowed'"))
				_, err := conn.ExecContext(ctx, "prepare metadata_compat_show from '"+listing+"'")
				require.NoError(t, err)
				defer func() {
					_, err := conn.ExecContext(ctx, "deallocate prepare metadata_compat_show")
					require.NoError(t, err)
				}()
				prepared, err := conn.PrepareContext(ctx, listing)
				require.NoError(t, err)
				defer prepared.Close()
				execSQLRequire(t, ctx, admin, revokeDB)
				for _, q := range []func() (*sql.Rows, error){
					func() (*sql.Rows, error) { return conn.QueryContext(ctx, listing) },
					func() (*sql.Rows, error) { return conn.QueryContext(ctx, "execute metadata_compat_show") },
					func() (*sql.Rows, error) { return prepared.QueryContext(ctx) },
				} {
					rows, err := q()
					if version < defines.MORPCVersion41 {
						requireMetadataPrivilegeDenied(t, rows, err)
					} else {
						require.Equal(t, []string{"allowed"}, readMetadataTableNames(t, rows, err))
					}
				}
				execSQLRequire(t, ctx, admin, grantDB)
				require.Equal(t, []string{"allowed", "hidden"}, query(t, "execute metadata_compat_show"))
				rows, err := prepared.QueryContext(ctx)
				require.Equal(t, []string{"allowed", "hidden"}, readMetadataTableNames(t, rows, err))
				execSQLRequire(t, ctx, admin, revokeDB)
				execSQLRequire(t, ctx, admin, revokeObject)
				rows, err = conn.QueryContext(ctx, listing)
				requireMetadataPrivilegeDenied(t, rows, err)
			})
		}

		// A handle prepared as an unfiltered compatibility listing must rebuild
		// before it can execute with object-only authorization after an upgrade.
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion40)
		execSQLRequire(t, ctx, admin, grantDB)
		execSQLRequire(t, ctx, admin, grantObject)
		prepared, err := conn.PrepareContext(ctx, listing)
		require.NoError(t, err)
		defer prepared.Close()
		execSQLRequire(t, ctx, admin, revokeDB)
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion41)
		rows, err := prepared.QueryContext(ctx)
		require.Equal(t, []string{"allowed"}, readMetadataTableNames(t, rows, err))
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion40)
		rows, err = prepared.QueryContext(ctx)
		requireMetadataPrivilegeDenied(t, rows, err)
		execSQLRequire(t, ctx, admin, grantDB)
		rows, err = prepared.QueryContext(ctx)
		require.Equal(t, []string{"allowed", "hidden"}, readMetadataTableNames(t, rows, err))
	})
}

func readMetadataTableNames(t *testing.T, rows *sql.Rows, err error) []string {
	t.Helper()
	require.NoError(t, err)
	defer rows.Close()
	names := []string{}
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	return names
}

func requireMetadataPrivilegeDenied(t *testing.T, rows *sql.Rows, err error) {
	t.Helper()
	if rows != nil {
		defer rows.Close()
	}
	require.ErrorContains(t, err, "do not have privilege")
}
