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
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type routineRepairTestHandle struct {
	VersionHandle
	err   error
	calls int
}

func (h *routineRepairTestHandle) HandleTenantUpgrade(context.Context, int32, executor.TxnExecutor) error {
	h.calls++
	return h.err
}

func TestRoutineRepairContinuesAfterFrozenCatalog(t *testing.T) {
	for _, manual := range []bool{false, true} {
		t.Run(fmt.Sprintf("manual=%v", manual), func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				tenantVersion := "4.0.7"
				injected := errors.New("repair failed")
				repair := &routineRepairTestHandle{VersionHandle: newTestVersionHandler("4.0.19", "4.0.7", versions.No, versions.Yes, 1), err: injected}
				reads := 0
				exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
					switch {
					case strings.HasPrefix(sql, "select create_version from mo_account"):
						reads++
						return newBootstrapStringResult(tenantVersion), nil
					case sql == versions.LatestVersionSQL:
						return statisticsLatestVersionResult(t, repair.Metadata(), versions.StateReady), nil
					case strings.HasPrefix(sql, "update mo_account set create_version = '4.0.19'"):
						tenantVersion = "4.0.19"
						return executor.Result{AffectedRows: 1}, nil
					default:
						return executor.Result{}, fmt.Errorf("unexpected upgrade SQL: %s", sql)
					}
				}, &testTxnOperator{})
				s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec, func(s *service) {
					s.handles = []VersionHandle{v4_0_7.Handler, repair}
					s.upgrade.finalVersionCompleted.Store(true)
				})
				t.Cleanup(s.stopper.Stop)
				run := func() error {
					if manual {
						return s.UpgradeOneTenant(t.Context(), 7)
					}
					_, err := s.MaybeUpgradeTenant(t.Context(), func() (int32, string, error) { return 7, "4.0.6", nil }, nil)
					return err
				}
				require.ErrorIs(t, run(), injected)
				require.Equal(t, "4.0.7", tenantVersion)
				require.False(t, s.mu.tenants[7], "failed repair must remain retryable")
				repair.err = nil
				require.NoError(t, run())
				require.Equal(t, "4.0.19", tenantVersion)
				require.True(t, s.mu.tenants[7])
				require.Equal(t, 2, repair.calls)
				completedReads := reads
				require.NoError(t, run())
				require.Equal(t, completedReads, reads, "only the committed final version may be cached")
			})
		})
	}
}
