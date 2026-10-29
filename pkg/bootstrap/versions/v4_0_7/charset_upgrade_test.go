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

package v4_0_7

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

type charsetScopedTxn struct {
	executor.TxnExecutor
	t         *testing.T
	accountID uint32
}

func (txn charsetScopedTxn) Exec(sql string, options executor.StatementOption) (executor.Result, error) {
	if sql != "SELECT mo_ctl('cn', 'GetProtocolVersion', '')" {
		require.True(txn.t, options.HasAccountID())
		require.Equal(txn.t, txn.accountID, options.AccountID())
	}
	return txn.TxnExecutor.Exec(sql, options)
}

func TestCharacterSetsUpgradeAdmissionAndRetry(t *testing.T) {
	entry := refreshInformationSchemaCharacterSets()
	const protocolSQL = "SELECT mo_ctl('cn', 'GetProtocolVersion', '')"
	checkSQL := sysview.InformationSchemaCharacterSetsCheckSQL()
	injected := errors.New("injected charset metadata upgrade failure")
	for _, tc := range []struct {
		name     string
		ready    bool
		protocol string
		failSQL  string
		wantErr  bool
		wantSQL  []string
	}{
		{name: "legacy utf8 maxlen three", protocol: `{"result":"cn0:106,cn1:106"}`,
			wantSQL: []string{checkSQL, protocolSQL, entry.PreSql, entry.UpgSql}},
		{name: "fresh canonical catalog", ready: true, wantSQL: []string{checkSQL}},
		{name: "failed check", failSQL: checkSQL, wantErr: true, wantSQL: []string{checkSQL}},
		{name: "mixed protocol", protocol: `{"result":"cn0:106,cn1:105"}`, wantErr: true,
			wantSQL: []string{checkSQL, protocolSQL}},
		{name: "failed protocol", failSQL: protocolSQL, wantErr: true,
			wantSQL: []string{checkSQL, protocolSQL}},
		{name: "failed delete", protocol: `{"result":"cn0:106"}`, failSQL: entry.PreSql, wantErr: true,
			wantSQL: []string{checkSQL, protocolSQL, entry.PreSql}},
		{name: "failed insert", protocol: `{"result":"cn0:106"}`, failSQL: entry.UpgSql, wantErr: true,
			wantSQL: []string{checkSQL, protocolSQL, entry.PreSql, entry.UpgSql}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				operator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
				operator.EXPECT().TxnOptions().Return(pbtxn.TxnOptions{}).AnyTimes()
				mp := mpool.MustNewZero()
				defer func() {
					require.Zero(t, mp.CurrNB())
					mpool.DeleteMPool(mp)
				}()
				result := func(value string) executor.Result {
					r := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
					if value != "" {
						r.NewBatchWithRowCount(1)
						require.NoError(t, executor.AppendStringRows(r, 0, []string{value}))
					}
					return r.GetResult()
				}
				var calls []string
				ready := tc.ready
				failSQL := tc.failSQL
				protocol := tc.protocol
				txn := charsetScopedTxn{t: t, accountID: 7, TxnExecutor: executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
					calls = append(calls, sql)
					if sql == failSQL {
						return executor.Result{}, injected
					}
					switch sql {
					case checkSQL:
						if ready {
							return result("1"), nil
						}
						return result(""), nil
					case protocolSQL:
						return result(protocol), nil
					case entry.PreSql:
						ready = false
					case entry.UpgSql:
						ready = true
					default:
						t.Fatalf("unexpected SQL: %s", sql)
					}
					return executor.Result{}, nil
				}, operator)}
				err := entry.Upgrade(txn, 7)
				if tc.failSQL != "" {
					require.ErrorIs(t, err, injected)
				} else if tc.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, tc.wantSQL, calls)
				if tc.wantErr {
					// The owner transaction rolls back failed entries. On retry the
					// handler must execute the repair, not treat MAXLEN=3 as ready.
					ready, failSQL, protocol = false, "", `{"result":"cn0:106"}`
					calls = nil
					require.NoError(t, entry.Upgrade(txn, 7))
					require.Equal(t, []string{checkSQL, protocolSQL, entry.PreSql, entry.UpgSql}, calls)
				}
				calls = nil
				require.NoError(t, entry.Upgrade(txn, 7))
				require.Equal(t, []string{checkSQL}, calls, "a completed refresh must be idempotent")
			})
		})
	}
}
