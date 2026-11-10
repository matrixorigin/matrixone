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

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type inventoryUpgradeExecutor struct {
	executor.SQLExecutor
	t       *testing.T
	calls   int
	unknown int
	failure error
	active  bool
}

func (e *inventoryUpgradeExecutor) ExecTxn(ctx context.Context, work func(executor.TxnExecutor) error, opts executor.Options) error {
	require.False(e.t, e.active, "page owners must not overlap")
	e.active = true
	defer func() { e.active = false }()
	e.calls++
	require.True(e.t, opts.HasAccountID())
	require.Zero(e.t, opts.AccountID(), "account readiness is checked by the system owner")
	require.Nil(e.t, opts.Txn())
	if err := e.SQLExecutor.ExecTxn(ctx, work, opts); err != nil {
		return err
	}
	if e.calls == e.unknown {
		return e.failure // Unconfirmed owner, not proof of database rollback.
	}
	return nil
}

// Compose the actual component with the service account transaction owner.
// Real SQL covers full registered dispatch, caller rollback and manual entry.
func TestInventoryServiceTransactionOwner(t *testing.T) {
	for _, scenario := range []string{"complete", "already completed", "create failure", "page commit unknown"} {
		t.Run(scenario, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				version := "4.0.6"
				if scenario == "already completed" {
					version = "4.0.7"
				}
				var ddl, pages, ready int
				injected := errors.New("unconfirmed inventory owner")
				exec := &inventoryUpgradeExecutor{t: t, failure: injected}
				if scenario == "page commit unknown" {
					exec.unknown = 2
				}
				exec.SQLExecutor = executor.NewMemExecutor2(func(query string) (executor.Result, error) {
					switch {
					case query == "select create_version from mo_account where account_id = 7 for update":
						return newBootstrapStringResult(version), nil
					case strings.HasPrefix(query, "create table if not exists mo_catalog.mo_database_inventory"):
						ddl++
						if scenario == "create failure" {
							return executor.Result{}, injected
						}
						return executor.Result{}, nil
					case strings.HasPrefix(query, "select cast(dat_id as bigint unsigned)"):
						pages++
						require.Contains(t, query, "account_id=7 and dat_id>0")
						return executor.Result{}, nil
					case query == "update mo_account set create_version = '4.0.7' where account_id = 7":
						ready++
						version = "4.0.7"
						return executor.Result{AffectedRows: 1}, nil
					default:
						return executor.Result{}, fmt.Errorf("unexpected inventory owner query: %s", query)
					}
				}, &testTxnOperator{})
				s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec,
					func(s *service) { s.initUpgrade() })
				t.Cleanup(s.stopper.Stop)
				ctx := defines.AttachAccountId(t.Context(), 7)
				upgrade := versions.VersionUpgrade{FromVersion: "4.0.6", ToVersion: "4.0.7"}
				active, err := versions.UpgradeDatabaseInventory(ctx, 7, versions.DefaultCatalogUpgradeObjectBytes,
					func(_ string, _ uint64, _ bool, work func(executor.TxnExecutor) error) (bool, error) {
						return s.withPagedTenantAccount(ctx, upgrade, 7, work)
					})
				if active && err == nil {
					active, err = s.withPagedTenantAccount(ctx, upgrade, 7, func(tx executor.TxnExecutor) error {
						return versions.UpgradeTenantVersion(7, "4.0.7", tx)
					})
				}
				switch scenario {
				case "complete":
					require.NoError(t, err)
					require.True(t, active)
					require.Equal(t, 3, exec.calls)
					require.Equal(t, 1, ready)
					require.Equal(t, 1, ddl)
					require.Equal(t, 1, pages)
				case "already completed":
					require.NoError(t, err)
					require.False(t, active)
					require.Equal(t, 1, exec.calls)
					require.Zero(t, ddl+pages+ready)
				default:
					require.ErrorIs(t, err, injected)
					require.False(t, active)
					require.Zero(t, ready, "unconfirmed work cannot publish readiness")
					if scenario == "create failure" {
						require.Equal(t, 1, exec.calls)
						require.Zero(t, pages)
					} else {
						require.Equal(t, 2, exec.calls)
					}
				}
				require.Empty(t, s.mu.tenants, "only a confirmed outer recheck owns readiness caching")
			})
		})
	}
}

func TestInventoryUpgradeTargetMustBeRegistered(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		exec := &inventoryUpgradeExecutor{t: t}
		s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec,
			func(s *service) { s.initUpgrade() })
		t.Cleanup(s.stopper.Stop)
		for _, target := range []string{"4.0.8", "not_a_version", "4.0.6"} {
			changed, err := s.upgradePagedTenantOnDemand(t.Context(), 7, target)
			require.Error(t, err)
			require.False(t, changed)
		}
		require.Zero(t, exec.calls)
	})
}

func TestInventoryUpgradeRegisteredTarget(t *testing.T) {
	s := &service{}
	s.initUpgrade()
	h, err := s.registeredPagedTenantHandle("4.0.7")
	require.NoError(t, err)
	require.Same(t, v4_0_7.Handler, h)
	require.Same(t, h, s.getFinalVersionHandle())
	require.Equal(t, "4.0.6", h.Metadata().MinUpgradeVersion)
	require.Equal(t, int64(defines.MORPCVersion126), h.Metadata().RequiredProtocolVersion, "no new wire protocol gate")
}
