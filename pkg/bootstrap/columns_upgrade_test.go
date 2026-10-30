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

package bootstrap

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_18"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestColumnsUpgradeAdmissionAndIdempotence(t *testing.T) {
	const protocolSQL = "SELECT mo_ctl('cn', 'GetProtocolVersion', '')"
	entry := versions.InformationSchemaColumnsUpgradeEntry()
	injected := errors.New("injected columns upgrade failure")
	for _, tc := range []struct {
		name       string
		definition string
		protocol   string
		failSQL    string
		wantDDL    []string
		wantErr    bool
	}{
		{name: "already current", definition: sysview.InformationSchemaColumnsDDL},
		{name: "legacy V58", definition: sysview.InformationSchemaColumnsV58DDL(), protocol: `{"result":"cn0:109,cn1:109"}`, wantDDL: []string{entry.UpgSql}},
		{name: "old protocol", definition: sysview.InformationSchemaColumnsV58DDL(), protocol: `{"result":"cn0:109,cn1:108"}`, wantErr: true},
		{name: "failed create", definition: sysview.InformationSchemaColumnsV58DDL(), protocol: `{"result":"cn0:109"}`, failSQL: entry.UpgSql, wantErr: true, wantDDL: []string{entry.UpgSql}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				operator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
				operator.EXPECT().TxnOptions().Return(pbtxn.TxnOptions{}).AnyTimes()
				mp := mpool.MustNewZero()
				defer func() { require.Zero(t, mp.CurrNB()) }()
				result := func(value string) executor.Result {
					r := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
					if value != "" {
						r.NewBatchWithRowCount(1)
						require.NoError(t, executor.AppendStringRows(r, 0, []string{value}))
					}
					return r.GetResult()
				}
				definition := tc.definition
				var ddl []string
				txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
					switch {
					case strings.HasPrefix(sql, "SELECT tbl.rel_createsql"):
						return result(definition), nil
					case sql == protocolSQL:
						return result(tc.protocol), nil
					default:
						ddl = append(ddl, sql)
						if sql == tc.failSQL {
							return executor.Result{}, injected
						}
						if sql == entry.UpgSql {
							definition = sysview.InformationSchemaColumnsDDL
						}
						return executor.Result{}, nil
					}
				}, operator)
				err := entry.Upgrade(txn, 7)
				if tc.failSQL != "" {
					require.ErrorIs(t, err, injected)
				} else if tc.wantErr {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, tc.wantDDL, ddl)

				if tc.wantErr && tc.failSQL == "" {
					return
				}
				if tc.failSQL != "" {
					// The owner transaction rolls back a failed entry. A retry sees the
					// persisted legacy view and performs the complete repair.
					definition, tc.failSQL, tc.protocol = sysview.InformationSchemaColumnsV58DDL(), "", `{"result":"cn0:109"}`
					ddl = nil
					require.NoError(t, entry.Upgrade(txn, 7))
					require.Equal(t, []string{entry.UpgSql}, ddl)
				}
				ddl = nil
				require.NoError(t, entry.Upgrade(txn, 7))
				require.Empty(t, ddl, "a completed refresh must be idempotent")
			})
		})
	}
}

// TestColumnsUpgradeReachesExistingTenants drives the retained 4.0.19 tenant
// hop (design 5.2, routes per D14-2; renumbered by revision 10 onto the
// consolidated ladder): a 4.0.7 account holding any pre-correction
// information_schema identity is rewritten to exactly the published
// identities through the per-account walk, which steps the upstream 4.0.18
// handle and the final vehicle; a current account is a no-op, a mid-hop DDL
// failure leaves the account marker untouched and retries cleanly.
func TestColumnsUpgradeReachesExistingTenants(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		frozen := v4_0_7.Handler.Metadata()
		current := versions.Version{Version: frozen.Version, VersionOffset: frozen.VersionOffset, State: versions.StateReady}
		columnsDefinition := sysview.InformationSchemaColumnsBinaryFamilyNullDDL()
		statisticsDefinition := sysview.InformationSchemaStatisticsDDL
		tenantVersion := "4.0.7"
		var queuedVersion, queuedRoute bool
		var writes []string
		failStatistics := false
		injected := errors.New("injected STATISTICS create failure")
		op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
		op.EXPECT().TxnOptions().Return(pbtxn.TxnOptions{}).AnyTimes()
		op.EXPECT().Txn().Return(pbtxn.TxnMeta{}).AnyTimes()
		exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
			switch {
			case strings.HasPrefix(sql, "SELECT reldatabase, relname, account_id FROM mo_catalog.mo_tables"):
				return newBootstrapStringResult("mo_catalog"), nil
			case sql == versions.LatestVersionSQL:
				return recoveryVersionResult(t, current), nil
			case strings.HasPrefix(sql, "select version from mo_version"):
				// The latest ready version that the route chains from.
				return newBootstrapStringResult(frozen.Version), nil
			case strings.HasPrefix(sql, "select state from mo_version"):
				// The queued forward version has no row yet; the checker
				// proceeds to enqueue it with its route.
				return executor.Result{}, nil
			case strings.HasPrefix(sql, "insert into mo_version"):
				// The registered forward version carries the two shared entries.
				require.Contains(t, sql, "'4.0.19', 2, 0")
				queuedVersion = true
			case strings.HasPrefix(sql, "insert into mo_upgrade"):
				require.Contains(t, sql, "'4.0.7'")
				require.Contains(t, sql, "'4.0.19'")
				queuedRoute = true
			case strings.HasPrefix(sql, "select create_version from mo_account"):
				return newBootstrapStringResult(tenantVersion), nil
			case strings.Contains(sql, "relname = 'COLUMNS'"), strings.Contains(sql, "relname = 'columns'"):
				return newBootstrapStringResult(columnsDefinition), nil
			case strings.Contains(sql, "relname = 'STATISTICS'"), strings.Contains(sql, "relname = 'statistics'"):
				return newBootstrapStringResult(statisticsDefinition), nil
			case strings.HasPrefix(sql, "select rel_id, rel_logical_id, relkind from mo_catalog.mo_tables"):
				return executor.Result{}, nil
			case sql == "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
				return newBootstrapStringResult(`{"result":"cn:118"}`), nil
			case sql == strings.Replace(sysview.InformationSchemaColumnsDDL, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1):
				writes = append(writes, sql)
				columnsDefinition = sysview.InformationSchemaColumnsDDL
			case sql == strings.Replace(sysview.InformationSchemaStatisticsPrefixSubPartDDL, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1):
				writes = append(writes, sql)
				if failStatistics {
					return executor.Result{}, injected
				}
				statisticsDefinition = sysview.InformationSchemaStatisticsPrefixSubPartDDL
			case strings.HasPrefix(sql, "update mo_account set create_version = '"):
				version := strings.TrimSuffix(
					strings.TrimPrefix(sql, "update mo_account set create_version = '"),
					"' where account_id = 7")
				if version == v4_0_18.Handler.Metadata().Version {
					// The walk also steps the upstream 4.0.18 handle; its
					// stamp stays inside the walking transaction, so the
					// durable account version only moves when the final
					// vehicle's stamp commits with the transaction.
					return executor.Result{AffectedRows: 1}, nil
				}
				if version == "4.0.19" {
					require.Equal(t, sysview.InformationSchemaColumnsDDL, columnsDefinition,
						"the completed tenant must carry the hop's published COLUMNS identity")
					require.Equal(t, sysview.InformationSchemaStatisticsPrefixSubPartDDL, statisticsDefinition,
						"the completed tenant must carry the hop's published STATISTICS identity")
				}
				tenantVersion = version
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
		require.True(t, queuedRoute, "a completed 4.0.7 cluster must queue the forward repair")
		require.Equal(t, "4.0.19", final.Version)
		require.Equal(t, "4.0.7", final.MinUpgradeVersion)
		require.Equal(t, uint32(2), final.VersionOffset)
		require.Equal(t, int64(defines.MORPCVersion118), final.RequiredProtocolVersion)

		// The cluster route has finished; the tenant's persisted account
		// version is authoritative. Every leg clears the completed-tenant
		// cache the way a fresh login on a new CN observes the account.
		current = final
		current.State = versions.StateReady
		fetchTenant := func() (int32, string, error) { return 7, tenantVersion, nil }
		runLeg := func(tenant, columns, statistics string) (bool, error) {
			delete(s.mu.tenants, 7)
			tenantVersion = tenant
			columnsDefinition = columns
			statisticsDefinition = statistics
			failStatistics = false
			writes = nil
			return s.MaybeUpgradeTenant(t.Context(), fetchTenant, nil)
		}

		// A 4.0.7 account holding the NULL-selector COLUMNS generation and
		// the SQL NULL SUB_PART STATISTICS is rewritten to exactly the
		// published identities.
		upgraded, err := runLeg("4.0.7",
			sysview.InformationSchemaColumnsBinaryFamilyNullDDL(), sysview.InformationSchemaStatisticsDDL)
		require.NoError(t, err)
		require.True(t, upgraded)
		require.Equal(t, 2, len(writes))
		require.Contains(t, writes, strings.Replace(sysview.InformationSchemaColumnsDDL, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1))
		require.Contains(t, writes, strings.Replace(sysview.InformationSchemaStatisticsPrefixSubPartDDL, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1))
		require.Equal(t, "4.0.19", tenantVersion)
		require.Equal(t, sysview.InformationSchemaColumnsDDL, columnsDefinition)
		require.Equal(t, sysview.InformationSchemaStatisticsPrefixSubPartDDL, statisticsDefinition)

		// The same holds for the pre-#31160 'binary'-selector generation.
		upgraded, err = runLeg("4.0.7",
			sysview.InformationSchemaColumnsV110DDL(), sysview.InformationSchemaStatisticsDDL)
		require.NoError(t, err)
		require.True(t, upgraded)
		require.Equal(t, 2, len(writes))
		require.Equal(t, "4.0.19", tenantVersion)

		// A tenant already holding both published identities is a no-op: a
		// completed refresh never churns the definitions (canonicalized
		// identity equality; unknown or edited text still fails the check).
		upgraded, err = runLeg("4.0.7",
			sysview.InformationSchemaColumnsDDL, sysview.InformationSchemaStatisticsPrefixSubPartDDL)
		require.NoError(t, err)
		require.True(t, upgraded)
		require.Empty(t, writes, "a current tenant must not be rewritten")
		require.Equal(t, "4.0.19", tenantVersion)

		// A mid-hop DDL failure stays visible to login: the account marker
		// is untouched, so the next pass re-enters and repairs the failed
		// leg. Model a tenant whose COLUMNS is current but whose STATISTICS
		// definition is still stale.
		delete(s.mu.tenants, 7)
		tenantVersion = "4.0.7"
		columnsDefinition = sysview.InformationSchemaColumnsDDL
		statisticsDefinition = sysview.InformationSchemaStatisticsDDL
		failStatistics = true
		writes = nil
		upgraded, err = s.MaybeUpgradeTenant(t.Context(), fetchTenant, nil)
		require.ErrorIs(t, err, injected)
		require.False(t, upgraded)
		require.Equal(t, "4.0.7", tenantVersion, "the failed hop must not advance the account marker")
		require.Equal(t, sysview.InformationSchemaStatisticsDDL, statisticsDefinition,
			"the failed create must not persist the new identity")
		require.Empty(t, s.mu.tenants, "failed hops must remain retryable")

		// The retry completes the tenant by refreshing the stale leg.
		failStatistics = false
		writes = nil
		upgraded, err = s.MaybeUpgradeTenant(t.Context(), fetchTenant, nil)
		require.NoError(t, err)
		require.True(t, upgraded)
		require.Equal(t, []string{
			strings.Replace(sysview.InformationSchemaStatisticsPrefixSubPartDDL, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1),
		}, writes)
		require.Equal(t, "4.0.19", tenantVersion)
		require.Equal(t, sysview.InformationSchemaColumnsDDL, columnsDefinition)
		require.Equal(t, sysview.InformationSchemaStatisticsPrefixSubPartDDL, statisticsDefinition)

		// A completed tenant is never refreshed again.
		writes = nil
		upgraded, err = s.MaybeUpgradeTenant(t.Context(), fetchTenant, nil)
		require.NoError(t, err)
		require.False(t, upgraded)
		require.Empty(t, writes, "a completed tenant must not be refreshed again")
	})
}
