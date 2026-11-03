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

package showaccounts

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type showAccountStorage struct {
	databases int64
	tables    int64
	size      float64
	snapshot  float64
}

func TestShowAccountsStorageAggregation(t *testing.T) {
	// Disable periodic and correction work before startup so the forced and
	// aggregate reads use the same statistics.
	c, err := embed.StartTestCluster(embed.WithCNCount(1), embed.WithPreStart(func(s embed.ServiceOperator) {
		s.Adjust(func(cfg *embed.ServiceConfig) {
			// Keep the test cluster's TMP file service in its owned directory.
			for i := range cfg.FileServices {
				if strings.EqualFold(cfg.FileServices[i].Name, defines.TmpFileServiceName) {
					cfg.FileServices[i].DataDir = filepath.Join(cfg.DataDir, "tmp")
				}
			}
			if s.ServiceType() == metadata.ServiceType_CN {
				cfg.CN.Frontend.SkipCheckUser = false
				cfg.CN.SQLAddress = fmt.Sprintf("127.0.0.1:%d", cfg.CN.Frontend.Port)
				cfg.CN.Engine.Stats.DisableStatsTask = true
				cfg.CN.Engine.Stats.CorrectionDuration = 24 * time.Hour
			}
		})
	}))
	if c != nil {
		var dataDir string
		c.ForeachServices(func(s embed.ServiceOperator) bool {
			dataDir = s.GetServiceConfig().DataDir
			return false
		})
		t.Cleanup(func() {
			if err := c.Close(); err != nil {
				t.Errorf("close test cluster: %v", err)
				return
			}
			if filepath.Dir(dataDir) != os.TempDir() || !strings.HasPrefix(filepath.Base(dataDir), "mo-cluster-test-") {
				t.Errorf("unexpected test cluster directory %q", dataDir)
				return
			}
			assert.NoError(t, os.RemoveAll(dataDir))
		})
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	port := cn.GetServiceConfig().CN.Frontend.Port
	sys, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
	require.NoError(t, err)
	defer sys.Close()

	// Enabling reads does not enable the disabled periodic task.
	control := cn.RawService().(cnservice.Service).GetEngine().(interface{ HandleMoTableStatsCtl(string) string })
	require.Contains(t, control.HandleMoTableStatsCtl("echo_current_setting:true"), "move_on(false)")
	control.HandleMoTableStatsCtl("use_old_impl:false")
	require.Contains(t, control.HandleMoTableStatsCtl("echo_current_setting:true"), "use_old_impl(false), force_update(false)")

	read := func(db *sql.DB, query string) map[string]showAccountStorage {
		t.Helper()
		rows, err := db.QueryContext(ctx, query)
		require.NoError(t, err, query)
		defer rows.Close()
		cols, err := rows.Columns()
		require.NoError(t, err)
		for i := range cols {
			cols[i] = strings.ToLower(cols[i])
		}
		require.Subset(t, cols, []string{"account_name", "db_count", "tbl_count", "size", "snapshot_size"})
		result := make(map[string]showAccountStorage)
		for rows.Next() {
			var name string
			var account showAccountStorage
			dest := make([]any, len(cols))
			for i, col := range cols {
				dest[i] = new(any)
				switch col {
				case "account_name":
					dest[i] = &name
				case "db_count":
					dest[i] = &account.databases
				case "tbl_count":
					dest[i] = &account.tables
				case "size":
					dest[i] = &account.size
				case "snapshot_size":
					dest[i] = &account.snapshot
				}
			}
			require.NoError(t, rows.Scan(dest...))
			require.NotContains(t, result, name)
			result[name] = account
		}
		require.NoError(t, rows.Err())
		return result
	}
	exec := func(db *sql.DB, statement string) {
		t.Helper()
		_, err := db.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}

	const a, b = "acc31388a", "acc31388b"
	tenants := make(map[string]*sql.DB)
	for _, name := range []string{a, b} {
		exec(sys, "drop account if exists "+name)
		defer func(name string) {
			cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
			defer done()
			_, err := sys.ExecContext(cleanup, "drop account if exists "+name)
			assert.NoError(t, err)
		}(name)
		exec(sys, fmt.Sprintf("create account %s admin_name 'admin' identified by '111'", name))
		tenant, err := sql.Open("mysql", fmt.Sprintf("%s#admin#accountadmin:111@tcp(127.0.0.1:%d)/", name, port))
		require.NoError(t, err)
		defer tenant.Close()
		tenants[name] = tenant
		exec(tenant, "create database size_db")
		exec(tenant, "create table size_db.t(v int)")
		exec(tenant, "insert into size_db.t values (1), (2)")
	}

	forcedA := read(sys, "show accounts like '"+a+"'")
	forcedB := read(sys, "show accounts like '"+b+"'")
	require.Len(t, forcedA, 1)
	require.Len(t, forcedB, 1)
	require.Positive(t, forcedA[a].size)
	require.Positive(t, forcedB[b].size)
	combined := read(sys, "show accounts like 'acc31388%'")
	require.Equal(t, map[string]showAccountStorage{a: forcedA[a], b: forcedB[b]}, combined)

	// Check the aggregate independently against the public result.
	ids := make(map[string]uint64)
	rows, err := sys.QueryContext(ctx, "select account_name, account_id from mo_catalog.mo_account where account_name in ('acc31388a','acc31388b')")
	require.NoError(t, err)
	func() {
		defer rows.Close()
		for rows.Next() {
			var name string
			var id uint64
			require.NoError(t, rows.Scan(&name, &id))
			ids[name] = id
		}
		require.NoError(t, rows.Err())
	}()
	require.Len(t, ids, 2)
	reader := cn.RawService().(cnservice.Service).GetEngine().(interface {
		QueryAccountTableSizes(context.Context, []uint64, bool) (map[uint64]uint64, bool, error)
	})
	sizes, ok, err := reader.QueryAccountTableSizes(ctx, []uint64{ids[a], ids[b]}, false)
	require.NoError(t, err)
	require.True(t, ok)
	for _, name := range []string{a, b} {
		size, exists := sizes[ids[name]]
		require.True(t, exists)
		require.Equal(t, math.Round(float64(size)/1048576*1e6)/1e6, combined[name].size)
	}

	tenantView := read(tenants[a], "show accounts")
	require.Equal(t, map[string]showAccountStorage{a: forcedA[a]}, tenantView)
	exec(tenants[a], "drop table size_db.t")
	afterDrop := read(sys, "show accounts like '"+a+"'")
	require.Equal(t, forcedA[a].tables-1, afterDrop[a].tables)
	combined = read(sys, "show accounts like 'acc31388%'")
	require.Equal(t, map[string]showAccountStorage{a: afterDrop[a], b: forcedB[b]}, combined)
}
