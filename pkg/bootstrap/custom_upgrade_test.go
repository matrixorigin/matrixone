// Copyright 2024 Matrix Origin
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
	"context"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func Test_UpgradeOneTenant(t *testing.T) {
	runtime.RunTest("", func(rt runtime.Runtime) {
		wantSQL := "select create_version from mo_account where account_id = 2"
		wantErr := moerr.NewInternalErrorNoCtx("version lookup failed")
		var queries []string
		sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
			queries = append(queries, sql)
			return executor.Result{}, wantErr
		})
		b := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, sqlExecutor,
			func(s *service) {
				s.handles = append(s.handles,
					newTestVersionHandler("1.2.0", "1.1.0", versions.Yes, versions.No, 10),
					newTestVersionHandler("2.0.0", "1.2.0", versions.Yes, versions.No, 2))
			})
		defer b.Close()
		require.ErrorIs(t, b.UpgradeOneTenant(context.Background(), 2), wantErr)
		require.Equal(t, []string{wantSQL}, queries)
		require.False(t, b.mu.tenants[2])
	})
}

func TestUpgradeTenantRetry(t *testing.T) {
	for _, closeDuringBackoff := range []bool{false, true} {
		name := "failure then success"
		if closeDuringBackoff {
			name = "Close during backoff"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				runtime.RunTest("", func(rt runtime.Runtime) {
					const tenantID = int32(2)
					const currentVersion = "4.0.0"
					const offset = uint32(3)
					tenantSQL := "select create_version from mo_account where account_id = 2"
					accountSQL := "select account_id, account_name from mo_catalog.mo_account where account_name = 'tenant'"
					latestSQL := fmt.Sprintf("select version, version_offset, state from %s order by create_at desc limit 1", catalog.MOVersionTable)
					// The memory executor lends this fixture-owned lookup row once.
					accountRow := buildUpgradeTenantAccountRows([]int32{tenantID}, []string{"tenant"})
					defer accountRow.Close()
					attempts := make(chan struct{}, 3)
					called := 0
					sqlExecutor := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
						switch {
						case sql == tenantSQL:
							called++
							attempts <- struct{}{}
							if called == 1 || closeDuringBackoff {
								return executor.Result{}, moerr.NewInternalErrorNoCtx("version lookup failed")
							}
							return buildTenantVersionResult(currentVersion), nil
						case sql == accountSQL:
							return accountRow, nil
						case sql == latestSQL:
							return buildLatestVersionResult(currentVersion, offset, versions.StateReady), nil
						case strings.Contains(sql, "FROM mo_catalog.mo_tables tbl"):
							return buildExistsResult(), nil
						case strings.Contains(sql, "from mo_catalog.mo_upgrade") || strings.Contains(sql, "from mo_upgrade"):
							return executor.Result{}, nil
						default:
							return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
						}
					}, &testTxnOperator{})
					b := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, sqlExecutor,
						func(s *service) {
							s.handles = append(s.handles, newTestVersionHandler(currentVersion, currentVersion, versions.Yes, versions.Yes, offset))
							s.upgrade.finalVersionCompleted.Store(true)
						})
					defer b.Close()
					ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
					defer cancel()
					accepted, err := b.UpgradeTenant(ctx, "tenant", 3, false)
					require.NoError(t, err)
					require.True(t, accepted)
					select {
					case <-attempts:
					case <-ctx.Done():
						t.Fatal("tenant retry was not registered")
					}
					synctest.Wait()
					if !closeDuringBackoff {
						select {
						case <-attempts:
						case <-ctx.Done():
							t.Fatal("tenant retry did not succeed")
						}
						synctest.Wait()
					}
					closed := make(chan struct{})
					go func() { _ = b.Close(); close(closed) }()
					select {
					case <-closed:
					case <-time.After(100 * time.Millisecond):
						t.Error("bootstrap Close remained blocked behind upgrade retry backoff")
					}
					<-closed
					wantCalls := 2
					if closeDuringBackoff {
						wantCalls = 1
					}
					require.Equal(t, wantCalls, called)
					b.mu.RLock()
					completed := b.mu.tenants[tenantID]
					b.mu.RUnlock()
					require.Equal(t, !closeDuringBackoff, completed)
				})
			})
		})
	}
}

func Test_UpgradeOneTenant_SameVersionWithCurrentOffsetRunsTenantHandler(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			const (
				tenantID      = int32(2)
				currentVer    = "4.0.0"
				currentOffset = uint32(3)
			)

			h := newTestVersionHandler(currentVer, currentVer, versions.Yes, versions.Yes, currentOffset)
			txnOp := &testTxnOperator{}
			sqlExecutor := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
				switch {
				case sql == fmt.Sprintf("select create_version from mo_account where account_id = %d", tenantID):
					return buildTenantVersionResult(currentVer), nil
				case sql == fmt.Sprintf(`select version, version_offset, state from %s order by create_at desc limit 1`, catalog.MOVersionTable):
					return buildLatestVersionResult(currentVer, currentOffset, versions.StateReady), nil
				case strings.Contains(sql, catalog.MOUpgradeTable) &&
					strings.Contains(sql, fmt.Sprintf("where final_version = '%s' and final_version_offset = %d", currentVer, currentOffset)):
					return buildUpgradeVersionResult(1, versions.StateReady, currentVer, currentVer, currentOffset, 0, versions.Yes, versions.Yes, 1, 1), nil
				case sql == fmt.Sprintf("select create_version from mo_account where account_id = %d for update", tenantID):
					return buildTenantVersionResult(currentVer), nil
				case sql == fmt.Sprintf("update mo_account set create_version = '%s' where account_id = %d", currentVer, tenantID):
					return executor.Result{AffectedRows: 1}, nil
				default:
					return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
				}
			}, txnOp)

			b := newServiceForTest(
				sid,
				&memLocker{},
				clock.NewHLCClock(func() int64 { return 0 }, 0),
				nil,
				sqlExecutor,
				func(s *service) {
					s.handles = append(s.handles, h)
					s.upgrade.finalVersionCompleted.Store(true)
				},
			)

			require.NoError(t, b.UpgradeOneTenant(context.Background(), tenantID))
			require.Equal(t, uint64(1), h.callHandleTenantUpgrade.Load())
		},
	)
}

func Test_UpgradeOneTenant_SameVersionWithoutUpgradeEntriesNoops(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			const (
				tenantID      = int32(2)
				currentVer    = "4.0.0"
				currentOffset = uint32(3)
			)

			h := newTestVersionHandler(currentVer, currentVer, versions.Yes, versions.Yes, currentOffset)
			txnOp := &testTxnOperator{}
			sqlExecutor := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
				switch {
				case sql == fmt.Sprintf("select create_version from mo_account where account_id = %d", tenantID):
					return buildTenantVersionResult(currentVer), nil
				case sql == fmt.Sprintf(`select version, version_offset, state from %s order by create_at desc limit 1`, catalog.MOVersionTable):
					return buildLatestVersionResult(currentVer, currentOffset, versions.StateReady), nil
				case strings.Contains(sql, catalog.MOUpgradeTable) &&
					strings.Contains(sql, fmt.Sprintf("where final_version = '%s' and final_version_offset = %d", currentVer, currentOffset)):
					return executor.Result{}, nil
				default:
					return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
				}
			}, txnOp)

			b := newServiceForTest(
				sid,
				&memLocker{},
				clock.NewHLCClock(func() int64 { return 0 }, 0),
				nil,
				sqlExecutor,
				func(s *service) {
					s.handles = append(s.handles, h)
					s.upgrade.finalVersionCompleted.Store(true)
				},
			)

			require.NoError(t, b.UpgradeOneTenant(context.Background(), tenantID))
			require.Zero(t, h.callHandleTenantUpgrade.Load())
		},
	)
}

func buildTenantVersionResult(version string) executor.Result {
	memRes := executor.NewMemResult(
		[]types.Type{types.New(types.T_varchar, 50, 0)},
		mpool.MustNewZero(),
	)
	memRes.NewBatchWithRowCount(1)
	executor.AppendStringRows(memRes, 0, []string{version})
	return memRes.GetResult()
}

func buildLatestVersionResult(version string, offset uint32, state int32) executor.Result {
	memRes := executor.NewMemResult(
		[]types.Type{
			types.New(types.T_varchar, 50, 0),
			types.New(types.T_uint32, 32, 0),
			types.New(types.T_int32, 32, 0),
		},
		mpool.MustNewZero(),
	)
	memRes.NewBatchWithRowCount(1)
	executor.AppendStringRows(memRes, 0, []string{version})
	executor.AppendFixedRows(memRes, 1, []uint32{offset})
	executor.AppendFixedRows(memRes, 2, []int32{state})
	return memRes.GetResult()
}
