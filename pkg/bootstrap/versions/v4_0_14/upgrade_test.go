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
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
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
	require.Equal(t, defines.MORPCVersion110, m.RequiredProtocolVersion)
	require.Equal(t, uint32(1+len(v4_0_7.PythonRevisionUpgradeEntries())), m.VersionOffset)
	require.Len(t, tenantUpgEntries, 1+len(v4_0_7.PythonRevisionUpgradeEntries()))
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
		require.NoError(t, upgradeInformationSchemaColumns().Upgrade(txn, 1))
		require.Error(t, Handler.HandleCreateFrameworkDeps(txn))

		injected := errors.New("injected")
		failed := executor.NewMemTxnExecutor(func(string) (executor.Result, error) { return executor.Result{}, injected }, operator)
		require.ErrorIs(t, upgradeInformationSchemaColumns().Upgrade(failed, 7), injected)
	})
}

func TestPythonRevisionRepairUpgradePath(t *testing.T) {
	metadata := Handler.Metadata()
	require.Equal(t, "4.0.14", metadata.Version)
	require.Equal(t, "4.0.13", metadata.MinUpgradeVersion)
	require.Equal(t, versions.No, metadata.UpgradeCluster)
	require.Equal(t, versions.Yes, metadata.UpgradeTenant)
	require.Equal(t, int64(defines.MORPCVersion110), metadata.RequiredProtocolVersion)
	require.Equal(t, uint32(1+len(v4_0_7.PythonRevisionUpgradeEntries())), metadata.VersionOffset)
	require.True(t, metadata.CanDirectUpgrade("4.0.13"))
	require.False(t, metadata.CanDirectUpgrade("4.0.7"))
	require.Len(t, v4_0_7.PythonRevisionUpgradeEntries(), 9)
}

func TestPythonRevisionRepairRejectsOldProtocolBeforeCatalogAccess(t *testing.T) {
	for _, response := range []string{
		`{"result":"cn-a:109"}`,
		`{"result":"cn-a:110,cn-b:109"}`,
		`{"result":""}`,
		`invalid`,
	} {
		t.Run(response, func(t *testing.T) {
			var statements []string
			txn := executor.NewMemTxnExecutor(func(statement string) (executor.Result, error) {
				statements = append(statements, statement)
				require.Equal(t, "SELECT mo_ctl('cn', 'GetProtocolVersion', '')", statement)
				return repairStringResult(t, response), nil
			}, nil)
			err := Handler.HandleTenantUpgrade(context.Background(), 7, txn)
			require.ErrorContains(t, err, "upgrade requires all CNs to support protocol version 110")
			require.Equal(t, []string{"SELECT mo_ctl('cn', 'GetProtocolVersion', '')"}, statements)
		})
	}
}

func TestPythonRevisionRepairIsIdempotentForCatalogStates(t *testing.T) {
	for _, test := range []struct {
		name          string
		schemaOrigin  string
		columns       []string
		newIndex      bool
		legacyIndex   bool
		revisionTable bool
		wantFirstDDL  int
	}{
		{
			name:         "upstream_final_4.0.13_missed_catalog",
			schemaOrigin: "4.0.13",
			legacyIndex:  true,
			wantFirstDDL: 9,
		},
		{
			name:         "partial_revision_catalog",
			schemaOrigin: "4.0.8",
			columns: []string{
				"active_revision", "namespace_version", "canonical_input_descriptor",
				"return_descriptor", "signature_key_schema_version", "signature_fingerprint",
			},
			legacyIndex:  true,
			wantFirstDDL: 3,
		},
		{
			name:         "complete_revision_catalog",
			schemaOrigin: "4.0.8",
			columns: []string{
				"active_revision", "namespace_version", "canonical_input_descriptor",
				"return_descriptor", "signature_key_schema_version", "signature_fingerprint",
			},
			newIndex:      true,
			revisionTable: true,
			wantFirstDDL:  0,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				state := &pythonRevisionCatalogState{
					columns:       make(map[string]bool),
					newIndex:      test.newIndex,
					legacyIndex:   test.legacyIndex,
					revisionTable: test.revisionTable,
				}
				for _, column := range test.columns {
					state.columns[column] = true
				}
				txn := newRepairTxnExecutor(t, state)
				require.NoError(t, Handler.Prepare(context.Background(), txn, true))
				require.NoError(t, Handler.HandleTenantUpgrade(context.Background(), 7, txn))
				require.Len(t, state.ddl, test.wantFirstDDL,
					"repair of %s schema must apply only missing catalog objects", test.schemaOrigin)
				firstDDL := append([]string(nil), state.ddl...)
				require.NoError(t, Handler.HandleTenantUpgrade(context.Background(), 7, txn))
				require.Equal(t, firstDDL, state.ddl,
					"a restart/retry of %s schema must not repeat DDL", test.schemaOrigin)
			})
		})
	}
}

type pythonRevisionCatalogState struct {
	columns       map[string]bool
	newIndex      bool
	legacyIndex   bool
	revisionTable bool
	ddl           []string
}

func newRepairTxnExecutor(t *testing.T, state *pythonRevisionCatalogState) executor.TxnExecutor {
	t.Helper()
	txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	txnOperator.EXPECT().TxnOptions().Return(pbtxn.TxnOptions{}).AnyTimes()
	return executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		lower := strings.ToLower(sql)
		switch {
		case strings.Contains(lower, "select tbl.rel_createsql"):
			return repairStringResult(t, sysview.InformationSchemaColumnsDDL), nil
		case strings.Contains(lower, "from mo_catalog.mo_columns"):
			for column := range state.columns {
				if strings.Contains(sql, fmt.Sprintf("attname = '%s'", column)) {
					return repairColumnResult(t), nil
				}
			}
			return executor.Result{}, nil
		case strings.Contains(lower, "from `mo_catalog`.`mo_indexes`"):
			if strings.Contains(sql, "name_db_arg_types_descriptor") && state.newIndex {
				return repairStringResult(t, "name_db_arg_types_descriptor"), nil
			}
			if strings.Contains(sql, "name_db_arg_types'") && state.legacyIndex {
				return repairStringResult(t, "name_db_arg_types"), nil
			}
			return executor.Result{}, nil
		case strings.Contains(lower, "from mo_catalog.mo_tables"):
			if state.revisionTable && strings.Contains(sql, "relname = 'mo_function_revisions'") {
				return repairStringResult(t, "mo_function_revisions"), nil
			}
			return executor.Result{}, nil
		case sql == "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
			return repairStringResult(t, `{"method":"GETPROTOCOLVERSION","result":"cn-a:110"}`), nil
		case strings.Contains(lower, "alter table mo_catalog.mo_user_defined_function add column"):
			for column := range state.columns {
				if strings.Contains(sql, "add column "+column+" ") {
					return executor.Result{}, nil
				}
			}
			for _, column := range []string{
				"active_revision", "namespace_version", "canonical_input_descriptor",
				"return_descriptor", "signature_key_schema_version", "signature_fingerprint",
			} {
				if strings.Contains(sql, "add column "+column+" ") {
					state.columns[column] = true
					state.ddl = append(state.ddl, sql)
					return executor.Result{}, nil
				}
			}
		case strings.Contains(lower, "create unique index name_db_arg_types_descriptor"):
			state.newIndex = true
			state.ddl = append(state.ddl, sql)
			return executor.Result{}, nil
		case strings.Contains(lower, "drop index name_db_arg_types"):
			state.legacyIndex = false
			state.ddl = append(state.ddl, sql)
			return executor.Result{}, nil
		case strings.Contains(lower, "create table mo_catalog.mo_function_revisions"):
			state.revisionTable = true
			state.ddl = append(state.ddl, sql)
			return executor.Result{}, nil
		default:
			return executor.Result{}, fmt.Errorf("unexpected repair SQL: %s", sql)
		}
		return executor.Result{}, nil
	}, txnOperator)
}

func repairStringResult(t *testing.T, value string) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	result := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
	result.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendStringRows(result, 0, []string{value}))
	return result.GetResult()
}

func repairColumnResult(t *testing.T) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	result := executor.NewMemResult([]types.Type{
		types.T_varchar.ToType(), types.T_varchar.ToType(),
		types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType(),
		types.T_int32.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(),
	}, mp)
	result.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendStringRows(result, 0, []string{"BIGINT UNSIGNED"}))
	require.NoError(t, executor.AppendStringRows(result, 1, []string{"NO"}))
	require.NoError(t, executor.AppendFixedRows(result, 2, []int64{0}))
	require.NoError(t, executor.AppendFixedRows(result, 3, []int64{0}))
	require.NoError(t, executor.AppendFixedRows(result, 4, []int64{0}))
	require.NoError(t, executor.AppendFixedRows(result, 5, []int64{0}))
	require.NoError(t, executor.AppendFixedRows(result, 6, []int32{1}))
	require.NoError(t, executor.AppendStringRows(result, 7, []string{"0"}))
	require.NoError(t, executor.AppendStringRows(result, 8, []string{""}))
	require.NoError(t, executor.AppendStringRows(result, 9, []string{""}))
	return result.GetResult()
}
