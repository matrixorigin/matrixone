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

package v4_0_14

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
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
	require.Equal(t, "4.0.14", m.Version)
	require.Equal(t, "4.0.13", m.MinUpgradeVersion)
	require.Equal(t, versions.Yes, m.UpgradeTenant)
	require.Equal(t, versions.No, m.UpgradeCluster)
	require.Equal(t, defines.MORPCVersion109, m.RequiredProtocolVersion)
	require.Equal(t, uint32(1), m.VersionOffset)
	require.Len(t, tenantUpgEntries, 1)
	require.Equal(t, int64(defines.MORPCVersion109), tenantUpgEntries[0].RequiredProtocolVersion)
}

func TestColumnsUpgradeAdmissionAndIdempotence(t *testing.T) {
	const protocolSQL = "SELECT mo_ctl('cn', 'GetProtocolVersion', '')"
	entry := upgradeInformationSchemaColumns()
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
		{name: "legacy V58", definition: sysview.InformationSchemaColumnsV58DDL(), protocol: `{"result":"cn0:109,cn1:109"}`, wantDDL: []string{entry.PreSql, entry.UpgSql}},
		{name: "old protocol", definition: sysview.InformationSchemaColumnsV58DDL(), protocol: `{"result":"cn0:109,cn1:108"}`, wantErr: true},
		{name: "failed drop", definition: sysview.InformationSchemaColumnsV58DDL(), protocol: `{"result":"cn0:109"}`, failSQL: entry.PreSql, wantErr: true, wantDDL: []string{entry.PreSql}},
		{name: "failed create", definition: sysview.InformationSchemaColumnsV58DDL(), protocol: `{"result":"cn0:109"}`, failSQL: entry.UpgSql, wantErr: true, wantDDL: []string{entry.PreSql, entry.UpgSql}},
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
						if sql == entry.PreSql {
							definition = ""
						} else if sql == entry.UpgSql {
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
					require.Equal(t, []string{entry.PreSql, entry.UpgSql}, ddl)
				}
				ddl = nil
				require.NoError(t, entry.Upgrade(txn, 7))
				require.Empty(t, ddl, "a completed refresh must be idempotent")
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
		require.NoError(t, Handler.HandleTenantUpgrade(ctx, 1, txn))
		require.Error(t, Handler.HandleCreateFrameworkDeps(txn))

		injected := errors.New("injected")
		failed := executor.NewMemTxnExecutor(func(string) (executor.Result, error) { return executor.Result{}, injected }, operator)
		require.ErrorIs(t, Handler.HandleTenantUpgrade(ctx, 7, failed), injected)
	})
}
