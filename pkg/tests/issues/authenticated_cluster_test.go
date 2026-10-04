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

package issues

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

const (
	authenticatedClusterHeartbeatTimeout = 15 * time.Second
	authenticatedClusterBackendTimeout   = 20 * time.Second
	authenticatedClusterStoreTimeout     = 60 * time.Second
)

func runAuthenticatedClusterTest(t *testing.T, fn func(embed.Cluster)) {
	t.Helper()
	embed.RunBaseClusterTests(t, fn)
}

func TestAuthenticatedTestsReuseBaseCluster(t *testing.T) {
	var baseCluster embed.Cluster
	var authenticatedCluster embed.Cluster

	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		baseCluster = c
	})
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		authenticatedCluster = c
	})

	require.Same(t, baseCluster, authenticatedCluster)

	// Validate the real bootstrap/catalog boundary before other shared scenarios
	// can modify grants. Reuse this fixture for a fresh ordinary account too.
	t.Run("initial privilege catalogs", func(t *testing.T) {
		checkInitialPrivilegeCatalogs(t, authenticatedCluster)
	})

	var cnCount, tnCount, logCount int
	authenticatedCluster.ForeachServices(func(svc embed.ServiceOperator) bool {
		cfg := svc.GetServiceConfig()
		require.Equal(t, authenticatedClusterBackendTimeout,
			cfg.HAKeeperClient.BackendReadTimeout.Duration)
		switch svc.ServiceType() {
		case metadata.ServiceType_CN:
			cnCount++
			require.False(t, cfg.CN.Frontend.SkipCheckUser)
			require.Equal(t, authenticatedClusterHeartbeatTimeout,
				cfg.CN.HAKeeper.HeatbeatTimeout.Duration)
			require.Less(t, cfg.CN.HAKeeper.HeatbeatTimeout.Duration,
				cfg.HAKeeperClient.BackendReadTimeout.Duration)
		case metadata.ServiceType_TN:
			tnCount++
			require.NotNil(t, cfg.TN_please_use_getTNServiceConfig)
			require.Equal(t, authenticatedClusterHeartbeatTimeout,
				cfg.TN_please_use_getTNServiceConfig.HAKeeper.HeatbeatTimeout.Duration)
			require.Less(t,
				cfg.TN_please_use_getTNServiceConfig.HAKeeper.HeatbeatTimeout.Duration,
				cfg.HAKeeperClient.BackendReadTimeout.Duration)
		case metadata.ServiceType_LOG:
			logCount++
			require.Equal(
				t,
				authenticatedClusterStoreTimeout,
				cfg.LogService.HAKeeperConfig.TNStoreTimeout.Duration,
			)
			require.Equal(
				t,
				authenticatedClusterStoreTimeout,
				cfg.LogService.HAKeeperConfig.CNStoreTimeout.Duration,
			)
			require.Less(t, authenticatedClusterHeartbeatTimeout,
				cfg.LogService.HAKeeperConfig.TNStoreTimeout.Duration)
			require.Less(t, authenticatedClusterHeartbeatTimeout,
				cfg.LogService.HAKeeperConfig.CNStoreTimeout.Duration)
			require.Less(t, cfg.HAKeeperClient.BackendReadTimeout.Duration,
				cfg.LogService.HAKeeperConfig.TNStoreTimeout.Duration)
		}
		return true
	})
	// The shared base cluster keeps two CNs; dedicated topology tests opt into
	// three CNs explicitly when that topology is part of the contract.
	require.Equal(t, 2, cnCount)
	require.Equal(t, 1, tnCount)
	require.Equal(t, 1, logCount)
}

func checkInitialPrivilegeCatalogs(t *testing.T, cluster embed.Cluster) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	open := func(user string) *sql.DB {
		db, err := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", user, cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		return db
	}
	type privilege struct {
		RoleID      int64
		RoleName    string
		ObjectType  string
		ObjectID    uint64
		PrivilegeID int64
		Level       string
		Operator    uint32
		Grant       bool
	}
	read := func(db *sql.DB, role string, roleID int64, userID uint32) map[string]privilege {
		rows, err := db.QueryContext(ctx, "select role_id,role_name,obj_type,obj_id,privilege_id,privilege_name,privilege_level,operation_user_id,with_grant_option,granted_time from mo_catalog.mo_role_privs where role_name = ?", role)
		require.NoError(t, err)
		defer rows.Close()
		result := make(map[string]privilege)
		for rows.Next() {
			var value privilege
			var name, granted string
			require.NoError(t, rows.Scan(&value.RoleID, &value.RoleName, &value.ObjectType, &value.ObjectID, &value.PrivilegeID, &name, &value.Level, &value.Operator, &value.Grant, &granted))
			require.Equal(t, roleID, value.RoleID)
			require.Equal(t, role, value.RoleName)
			require.Equal(t, userID, value.Operator)
			_, err = time.ParseInLocation("2006-01-02 15:04:05", granted, time.UTC)
			require.NoError(t, err)
			require.NotContains(t, result, name, "duplicate initial privilege")
			// Compare privilege contents independently of tenant-local principal IDs.
			value.RoleID, value.Operator, value.RoleName = 0, 0, ""
			result[name] = value
		}
		require.NoError(t, rows.Err())
		return result
	}
	sys := open("dump")
	systemAdmin := read(sys, "moadmin", 0, 0)
	systemPublic := read(sys, "public", 1, 0)
	require.Len(t, systemAdmin, 34)
	require.Len(t, systemPublic, 1)
	require.Contains(t, systemPublic, "connect")
	for _, name := range []string{"create account", "drop account", "alter account", "upgrade account"} {
		require.Contains(t, systemAdmin, name)
		delete(systemAdmin, name)
	}
	const account = "initial_privilege_catalog_check"
	execSQLRequire(t, ctx, sys, "create account "+account+" admin_name 'admin' identified by '111'")
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		execSQLMaybe(t, cleanup, sys, "drop account if exists "+account)
	}()
	admin := open(account + "#admin#accountadmin")
	require.Equal(t, systemAdmin, read(admin, "accountadmin", 2, 2))
	require.Equal(t, systemPublic, read(admin, "public", 1, 2))
	execSQLRequire(t, ctx, admin, "create database initial_privilege_probe")
	execSQLRequire(t, ctx, admin, "create table initial_privilege_probe.t(id int)")
	public := open(account + "#admin#public")
	_, err = public.ExecContext(ctx, "drop table initial_privilege_probe.t")
	require.ErrorContains(t, err, "do not have privilege")
	var value int
	require.NoError(t, public.QueryRowContext(ctx, "select 1").Scan(&value))
	require.Equal(t, 1, value)
}
