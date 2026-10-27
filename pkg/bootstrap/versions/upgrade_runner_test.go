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

package versions

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestUpgradeEntryRunnersStopOnError(t *testing.T) {
	const sid = "upgrade-runner-test"
	runtime.RunTest(sid, func(rt runtime.Runtime) {
		op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
		op.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()
		tx := executor.NewMemTxnExecutor(func(string) (executor.Result, error) {
			t.Fatal("already-completed entries must not execute SQL")
			return executor.Result{}, nil
		}, op)
		fail := errors.New("upgrade check failed")
		for _, tc := range []struct {
			name        string
			tenant      uint32
			loggerCalls int
			run         func([]UpgradeEntry, func(string) *log.MOLogger, func() string) error
		}{
			{"tenant", 42, 2, func(entries []UpgradeEntry, logger func(string) *log.MOLogger, version func() string) error {
				return RunTenantUpgradeEntries(42, tx, entries, logger, version)
			}},
			{"historical_tenant", 42, 2, func(entries []UpgradeEntry, logger func(string) *log.MOLogger, version func() string) error {
				return RunHistoricalTenantUpgradeEntries(42, tx, entries, logger, version)
			}},
			{"cluster", catalog.System_Account, 2, func(entries []UpgradeEntry, logger func(string) *log.MOLogger, version func() string) error {
				return RunClusterUpgradeEntries(tx, entries, logger, version)
			}},
			{"tenant_with_logger", 42, 0, func(entries []UpgradeEntry, _ func(string) *log.MOLogger, version func() string) error {
				return RunTenantUpgradeEntriesWithLogger(42, tx, entries, rt.Logger(), version)
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var seen []string
				var loggerCalls, versionCalls int
				logger := func(got string) *log.MOLogger { require.Equal(t, sid, got); loggerCalls++; return rt.Logger() }
				version := func() string { versionCalls++; return "4.0.test" }
				require.NoError(t, tc.run(nil, logger, version))
				require.Zero(t, loggerCalls)
				require.Zero(t, versionCalls)
				entries := []UpgradeEntry{
					{TableName: "first", CheckFunc: func(_ executor.TxnExecutor, account uint32) (bool, error) {
						require.Equal(t, tc.tenant, account)
						seen = append(seen, "first")
						return true, nil
					}},
					{TableName: "failure", CheckFunc: func(_ executor.TxnExecutor, account uint32) (bool, error) {
						require.Equal(t, tc.tenant, account)
						seen = append(seen, "failure")
						return false, fail
					}},
					{TableName: "unreachable", CheckFunc: func(_ executor.TxnExecutor, _ uint32) (bool, error) {
						seen = append(seen, "unreachable")
						return true, nil
					}},
				}
				require.ErrorIs(t, tc.run(entries, logger, version), fail)
				require.Equal(t, []string{"first", "failure"}, seen)
				require.Equal(t, tc.loggerCalls, loggerCalls)
				require.Equal(t, 2, versionCalls)
			})
		}
	})
}
