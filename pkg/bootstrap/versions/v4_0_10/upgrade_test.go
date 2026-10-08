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

package v4_0_10

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestColumnsUpgradeMetadata(t *testing.T) {
	m := Handler.Metadata()
	require.Equal(t, "4.0.10", m.Version)
	require.Equal(t, "4.0.9", m.MinUpgradeVersion)
	require.Equal(t, versions.Yes, m.UpgradeTenant)
	require.Equal(t, versions.No, m.UpgradeCluster)
	require.Equal(t, defines.MORPCVersion100, m.RequiredProtocolVersion)
	require.Equal(t, uint32(1), m.VersionOffset)
}

// The migration must verify both the exact persisted definition and every CN's
// capability before its first DDL. A protocol response is a component-level
// oracle here, not a substitute for a real mixed-binary rollout test.
func TestColumnsUpgradeAdmission(t *testing.T) {
	const protocolSQL = "SELECT mo_ctl('cn', 'GetProtocolVersion', '')"
	entry := upgradeInformationSchemaColumns()
	injected := errors.New("injected upgrade failure")
	for _, tc := range []struct {
		name         string
		definition   string
		lowercase    bool
		protocol     string
		failSQL      string
		wantDDL      []string
		wantProtocol bool
		wantErr      bool
	}{
		{name: "all old", protocol: `{"result":"cn0:99,cn1:99"}`, wantProtocol: true, wantErr: true},
		{name: "mixed", protocol: `{"result":"cn0:100,cn1:99"}`, wantProtocol: true, wantErr: true},
		{name: "no response", wantProtocol: true, wantErr: true},
		{name: "RPC failure", failSQL: protocolSQL, wantProtocol: true, wantErr: true},
		{name: "missing catalog", protocol: `{"result":"cn0:100,cn1:100"}`, wantProtocol: true,
			wantDDL: []string{entry.PreSql, entry.UpgSql}},
		{name: "legacy lowercase", definition: sysview.InformationSchemaColumnsV58DDL(), lowercase: true,
			protocol: `{"result":"cn0:100,cn1:100"}`, wantProtocol: true,
			wantDDL: []string{entry.PreSql, entry.UpgSql}},
		{name: "marker is not exact readiness", definition: sysview.InformationSchemaColumnsDDL + " ",
			protocol: `{"result":"cn0:100,cn1:100"}`, wantProtocol: true,
			wantDDL: []string{entry.PreSql, entry.UpgSql}},
		{name: "canonical uppercase", definition: sysview.InformationSchemaColumnsDDL},
		{name: "canonical lowercase", definition: sysview.InformationSchemaColumnsDDL, lowercase: true},
		{name: "drop failure", protocol: `{"result":"cn0:100,cn1:100"}`, failSQL: entry.PreSql,
			wantProtocol: true, wantErr: true, wantDDL: []string{entry.PreSql}},
		{name: "create failure", protocol: `{"result":"cn0:100,cn1:100"}`, failSQL: entry.UpgSql,
			wantProtocol: true, wantErr: true, wantDDL: []string{entry.PreSql, entry.UpgSql}},
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
				var ddl []string
				var protocolCalls, definitionCalls int
				txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
					switch {
					case strings.HasPrefix(sql, "SELECT tbl.rel_createsql"):
						definitionCalls++
						if tc.lowercase && strings.Contains(sql, "tbl.relname = 'COLUMNS'") {
							return result(""), nil
						}
						return result(tc.definition), nil
					case sql == protocolSQL:
						protocolCalls++
						require.Empty(t, ddl, "capability check must precede all DDL")
					default:
						ddl = append(ddl, sql)
					}
					if sql == tc.failSQL {
						return executor.Result{}, injected
					}
					if sql == protocolSQL {
						return result(tc.protocol), nil
					}
					return executor.Result{}, nil
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
				require.Equal(t, tc.wantProtocol, protocolCalls == 1)
				if tc.lowercase || tc.definition == "" {
					require.Equal(t, 2, definitionCalls)
				} else {
					require.Equal(t, 1, definitionCalls)
				}
			})
		})
	}
}

func TestColumnsUpgradeLifecycle(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		ctx := context.Background()
		operator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
		operator.EXPECT().TxnOptions().Return(pbtxn.TxnOptions{}).AnyTimes()
		txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
			require.True(t, strings.HasPrefix(sql, "SELECT tbl.rel_createsql"), sql)
			mp := mpool.MustNewZero()
			r := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
			r.NewBatchWithRowCount(1)
			require.NoError(t, executor.AppendStringRows(r, 0, []string{sysview.InformationSchemaColumnsDDL}))
			return r.GetResult(), nil
		}, operator)
		require.NoError(t, Handler.Prepare(ctx, txn, true))
		require.NoError(t, Handler.HandleTenantUpgrade(ctx, int32(catalog.System_Account), txn))
		require.Error(t, Handler.HandleCreateFrameworkDeps(txn))
		injected := errors.New("injected")
		failed := executor.NewMemTxnExecutor(func(string) (executor.Result, error) { return executor.Result{}, injected }, operator)
		require.ErrorIs(t, Handler.HandleTenantUpgrade(ctx, 7, failed), injected)
	})
}
