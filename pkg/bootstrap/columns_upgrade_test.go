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

package bootstrap

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestColumnsUpgradeReachesExistingTenants(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		current := versions.Version{Version: "4.0.16", VersionOffset: 1, State: versions.StateReady}
		definition := sysview.InformationSchemaColumnsV109DDL()
		tenantVersion := "4.0.16"
		var queuedVersion, queuedRoute bool
		var writes []string
		failCreate := false
		injected := errors.New("injected COLUMNS create failure")
		op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
		op.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		op.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
		exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
			switch {
			case strings.HasPrefix(sql, "SELECT reldatabase, relname, account_id FROM mo_catalog.mo_tables"):
				return newBootstrapStringResult("mo_catalog"), nil
			case sql == "select version, version_offset, state from mo_version order by create_at desc limit 1":
				return recoveryVersionResult(t, current), nil
			case strings.HasPrefix(sql, "select state from mo_version"):
				return executor.Result{}, nil
			case strings.HasPrefix(sql, "select version from mo_version"):
				return newBootstrapStringResult("4.0.16"), nil
			case strings.HasPrefix(sql, "insert into mo_version"):
				require.Contains(t, sql, "'4.0.17', 1, 0")
				queuedVersion = true
			case strings.HasPrefix(sql, "insert into mo_upgrade"):
				require.Contains(t, sql, "'4.0.16'")
				require.Contains(t, sql, "'4.0.17'")
				queuedRoute = true
			case strings.HasPrefix(sql, "select create_version from mo_account"):
				return newBootstrapStringResult(tenantVersion), nil
			case strings.HasPrefix(sql, "SELECT tbl.rel_createsql"):
				return newBootstrapStringResult(definition), nil
			case sql == "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
				return newBootstrapStringResult(`{"result":"cn:118"}`), nil
			case sql == "DROP VIEW IF EXISTS information_schema.COLUMNS;":
				writes = append(writes, sql)
			case sql == sysview.InformationSchemaColumnsDDL:
				writes = append(writes, sql)
				if failCreate {
					return executor.Result{}, injected
				}
				definition = sql
			case sql == "update mo_account set create_version = '4.0.17' where account_id = 7":
				require.Equal(t, sysview.InformationSchemaColumnsDDL, definition,
					"tenant completion must follow the successful view refresh")
				tenantVersion = "4.0.17"
				return executor.Result{AffectedRows: 1}, nil
			default:
				return executor.Result{}, fmt.Errorf("unexpected upgrade SQL: %s", sql)
			}
			return executor.Result{}, nil
		}, op)
		s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec,
			func(s *service) { s.initUpgrade() })
		t.Cleanup(s.stopper.Stop)
		final := s.getFinalVersionHandle().Metadata()
		require.NoError(t, s.doCheckUpgrade(t.Context()))
		require.True(t, queuedVersion)
		require.True(t, queuedRoute, "a completed 4.0.16 cluster must queue the forward repair")
		require.Equal(t, "4.0.17", final.Version)
		require.Equal(t, "4.0.16", final.MinUpgradeVersion)
		require.Equal(t, uint32(1), final.VersionOffset)
		require.Equal(t, defines.MORPCVersion118, final.RequiredProtocolVersion)

		// Model a tenant created by an old CN after batch enumeration: the
		// cluster route has finished, but its persisted account version is old.
		current = final
		current.State = versions.StateReady
		failCreate = true
		fetchTenant := func() (int32, string, error) { return 7, "4.0.17", nil }
		upgraded, err := s.MaybeUpgradeTenant(t.Context(), fetchTenant, nil)
		require.ErrorIs(t, err, injected)
		require.False(t, upgraded)
		require.Equal(t, "4.0.16", tenantVersion)
		require.Empty(t, s.mu.tenants, "failed admission must remain retryable")
		failCreate = false
		writes = nil
		upgraded, err = s.MaybeUpgradeTenant(t.Context(), fetchTenant, nil)
		require.NoError(t, err)
		require.True(t, upgraded)
		require.Equal(t, []string{"DROP VIEW IF EXISTS information_schema.COLUMNS;", sysview.InformationSchemaColumnsDDL}, writes)
		require.Equal(t, "4.0.17", tenantVersion)
		require.Equal(t, sysview.InformationSchemaColumnsDDL, definition)
		writes = nil
		upgraded, err = s.MaybeUpgradeTenant(t.Context(), fetchTenant, nil)
		require.NoError(t, err)
		require.False(t, upgraded)
		require.Empty(t, writes, "a completed tenant must not be refreshed again")
	})
}
