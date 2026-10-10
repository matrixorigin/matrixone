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

package v4_0_14

import (
	"errors"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestDatabaseDefaultsUpgrade(t *testing.T) {
	metadata := Handler.Metadata()
	require.Equal(t, "4.0.14", metadata.Version)
	require.Equal(t, "4.0.13", metadata.MinUpgradeVersion)
	require.Equal(t, defines.MORPCVersion109, metadata.RequiredProtocolVersion)
	require.Equal(t, versions.Yes, metadata.UpgradeTenant)
	require.Equal(t, uint32(4), metadata.VersionOffset)
	injected := errors.New("catalog unavailable")
	for _, tc := range []struct {
		name                   string
		oldProtocol, lowercase bool
		fail                   string
	}{
		{name: "upgrade then idempotent"}, {name: "lowercase view", lowercase: true},
		{name: "mixed versions", oldProtocol: true}, {name: "table lookup fails", fail: "table"},
		{name: "view lookup fails", fail: "view"}, {name: "create table fails", fail: "create"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
				op.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
				mp := mpool.MustNewZeroNoFixed()
				defer mpool.DeleteMPool(mp)
				strResult := func(value string) executor.Result {
					r := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
					r.NewBatchWithRowCount(1)
					require.NoError(t, executor.AppendStringRows(r, 0, []string{value}))
					return r.GetResult()
				}
				var writes []string
				tableExists := false
				viewDefinition := sysview.InformationSchemaSchemataLegacyDDL
				columnsDefinition := sysview.InformationSchemaColumnsDDL
				tablesDefinition := sysview.InformationSchemaTablesDDL
				txnExec := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
					switch {
					case strings.HasPrefix(sql, "SELECT reldatabase, relname, account_id"):
						if tc.fail == "table" {
							return executor.Result{}, injected
						}
						if tableExists {
							return strResult(catalog.MODatabaseDefaults), nil
						}
						return executor.Result{}, nil
					case strings.HasPrefix(sql, "SELECT tbl.rel_createsql"):
						if tc.fail == "view" {
							return executor.Result{}, injected
						}
						if tc.lowercase && (strings.Contains(sql, "tbl.relname = 'SCHEMATA'") || strings.Contains(sql, "tbl.relname = 'COLUMNS'") || strings.Contains(sql, "tbl.relname = 'TABLES'")) {
							return executor.Result{}, nil
						}
						if strings.Contains(strings.ToLower(sql), "tbl.relname = 'columns'") {
							return strResult(columnsDefinition), nil
						}
						if strings.Contains(strings.ToLower(sql), "tbl.relname = 'tables'") {
							return strResult(tablesDefinition), nil
						}
						return strResult(viewDefinition), nil
					case sql == "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
						if tc.oldProtocol {
							return strResult(`{"method":"GETPROTOCOLVERSION","result":"cn-a:109,cn-b:108"}`), nil
						}
						return strResult(`{"method":"GETPROTOCOLVERSION","result":"cn-a:109,cn-b:109"}`), nil
					case sql == catalog.MoDatabaseDefaultsDDL:
						if tc.fail == "create" {
							return executor.Result{}, injected
						}
						tableExists = true
						writes = append(writes, sql)
					case sql == tenantUpgEntries[1].PreSql || sql == tenantUpgEntries[2].PreSql || sql == tenantUpgEntries[3].PreSql:
						require.True(t, tableExists)
						writes = append(writes, sql)
					case sql == sysview.InformationSchemaSchemataDDL:
						require.True(t, tableExists)
						viewDefinition = sql
						writes = append(writes, sql)
					case sql == sysview.InformationSchemaColumnsV109DDL:
						columnsDefinition = sql
						writes = append(writes, sql)
					case sql == sysview.InformationSchemaTablesV109DDL:
						tablesDefinition = sql
						writes = append(writes, sql)
					default:
						t.Fatalf("unexpected SQL: %s", sql)
					}
					return executor.Result{}, nil
				}, op)
				require.NoError(t, Handler.Prepare(t.Context(), txnExec, true))
				require.NoError(t, Handler.HandleClusterUpgrade(t.Context(), txnExec))
				require.Error(t, Handler.HandleCreateFrameworkDeps(txnExec))
				err := Handler.HandleTenantUpgrade(t.Context(), 7, txnExec)
				if tc.oldProtocol {
					require.ErrorContains(t, err, "version 108")
					require.Empty(t, writes)
				} else if tc.fail != "" {
					require.ErrorIs(t, err, injected)
				} else {
					require.NoError(t, err)
					require.Equal(t, []string{catalog.MoDatabaseDefaultsDDL, tenantUpgEntries[1].PreSql, sysview.InformationSchemaSchemataDDL, tenantUpgEntries[2].PreSql, sysview.InformationSchemaColumnsV109DDL, tenantUpgEntries[3].PreSql, sysview.InformationSchemaTablesV109DDL}, writes)
					writes = nil
					require.NoError(t, Handler.HandleTenantUpgrade(t.Context(), 7, txnExec))
					require.Empty(t, writes)
				}
				require.Zero(t, mp.CurrNB())
			})
		})
	}
}
