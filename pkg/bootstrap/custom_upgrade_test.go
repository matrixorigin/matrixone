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
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func Test_UpgradeOneTenant(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Expected no panic")
				}
			}()

			wantSql1 := "select create_version from mo_account where account_id = 2"

			sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				if wantSql1 == sql {
					time.Sleep(time.Second)
					return executor.Result{}, moerr.NewInternalErrorNoCtx("return error")
				}
				return executor.Result{}, nil
			})

			b := newServiceForTest(
				sid,
				&memLocker{},
				clock.NewHLCClock(func() int64 { return 0 }, 0),
				nil,
				sqlExecutor,
				func(s *service) {
					h1 := newTestVersionHandler("1.2.0", "1.1.0", versions.Yes, versions.No, 10)
					h2 := newTestVersionHandler("2.0.0", "1.2.0", versions.Yes, versions.No, 2)
					s.handles = append(s.handles, h1)
					s.handles = append(s.handles, h2)
				},
			)

			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
			defer cancel()

			err := b.UpgradeOneTenant(ctx, 2)
			assert.Error(t, err)
		},
	)
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
