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
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

type collationAccountTxn struct {
	executor.TxnExecutor
	t *testing.T
}

func (tx collationAccountTxn) Exec(sql string, opts executor.StatementOption) (executor.Result, error) {
	if sql != "SELECT mo_ctl('cn', 'GetProtocolVersion', '')" {
		require.True(tx.t, opts.HasAccountID())
		require.Equal(tx.t, uint32(7), opts.AccountID())
	}
	return tx.TxnExecutor.Exec(sql, opts)
}

func TestCollationsUpgrade(t *testing.T) {
	entry := populateInformationSchemaCollations()
	injected := errors.New("injected migration failure")
	for _, tc := range []struct {
		name, fail           string
		current, oldProtocol bool
	}{
		{name: "stale then current"},
		{name: "already current", current: true},
		{name: "check fails", fail: "check"},
		{name: "protocol lookup fails", fail: "protocol"},
		{name: "mixed protocol", oldProtocol: true},
		{name: "delete fails", fail: "delete"},
		{name: "insert fails", fail: "insert"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
				op.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
				mp := mpool.MustNewZeroNoFixed()
				defer mpool.DeleteMPool(mp)
				current := tc.current
				var writes []string
				mem := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
					switch sql {
					case sysview.InformationSchemaCollationsCheckSQL():
						if tc.fail == "check" {
							return executor.Result{}, injected
						}
						if current {
							res := executor.NewMemResult([]types.Type{types.T_int64.ToType()}, mp)
							res.NewBatchWithRowCount(1)
							require.NoError(t, executor.AppendFixedRows(res, 0, []int64{1}))
							return res.GetResult(), nil
						}
					case "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
						if tc.fail == "protocol" {
							return executor.Result{}, injected
						}
						response := `{"result":"cn:118"}`
						if tc.oldProtocol {
							response = `{"result":"cn:118,old:117"}`
						}
						res := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
						res.NewBatchWithRowCount(1)
						require.NoError(t, executor.AppendStringRows(res, 0, []string{response}))
						return res.GetResult(), nil
					case "DELETE FROM information_schema.COLLATIONS":
						if tc.fail == "delete" {
							return executor.Result{}, injected
						}
						writes = append(writes, sql)
					case sysview.InformationSchemaCollationsData:
						if tc.fail == "insert" {
							return executor.Result{}, injected
						}
						writes = append(writes, sql)
						current = true
					default:
						t.Fatalf("unexpected SQL: %s", sql)
					}
					return executor.Result{}, nil
				}, op)
				tx := collationAccountTxn{TxnExecutor: mem, t: t}
				err := entry.Upgrade(tx, 7)
				switch {
				case tc.oldProtocol:
					require.ErrorContains(t, err, "version 117")
					require.Empty(t, writes)
				case tc.fail != "":
					require.ErrorIs(t, err, injected)
					if tc.fail == "insert" {
						require.Equal(t, []string{"DELETE FROM information_schema.COLLATIONS"}, writes)
					} else {
						require.Empty(t, writes)
					}
				default:
					require.NoError(t, err)
					if tc.current {
						require.Empty(t, writes)
					} else {
						require.Equal(t, []string{"DELETE FROM information_schema.COLLATIONS", sysview.InformationSchemaCollationsData}, writes)
					}
					writes = nil
					require.NoError(t, entry.Upgrade(tx, 7))
					require.Empty(t, writes)
				}
				require.Zero(t, mp.CurrNB(), "all check/protocol results must be closed")
			})
		})
	}
}
