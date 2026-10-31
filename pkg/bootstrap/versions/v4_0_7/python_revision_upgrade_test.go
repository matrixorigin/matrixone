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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestPythonRevisionRepairUpgradePath(t *testing.T) {
	metadata := Handler.Metadata()
	require.Equal(t, "4.0.7", metadata.Version)
	require.Equal(t, "4.0.6", metadata.MinUpgradeVersion)
	require.Equal(t, versions.Yes, metadata.UpgradeCluster)
	require.Equal(t, versions.Yes, metadata.UpgradeTenant)
	require.Equal(t, uint32(len(tenantUpgEntries)+len(clusterUpgEntries)), metadata.VersionOffset)
	require.True(t, metadata.CanDirectUpgrade("4.0.6"))
	require.False(t, metadata.CanDirectUpgrade("4.0.1"))
	require.Len(t, routineUpgEntries, 9)
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
			name:         "released_4.0.6_missing_routine_schema",
			schemaOrigin: "released 4.0.6",
			legacyIndex:  true,
			wantFirstDDL: 9,
		},
		{
			name:         "partial_revision_catalog",
			schemaOrigin: "interrupted current migration",
			columns: []string{
				"active_revision", "namespace_version", "canonical_input_descriptor",
				"return_descriptor", "signature_key_schema_version", "signature_fingerprint",
			},
			legacyIndex:  true,
			wantFirstDDL: 3,
		},
		{
			name:         "complete_revision_catalog",
			schemaOrigin: "interrupted current migration",
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
				for _, entry := range routineUpgEntries {
					require.NoError(t, entry.Upgrade(txn, 7))
				}
				require.Len(t, state.ddl, test.wantFirstDDL,
					"repair of %s schema must apply only missing catalog objects", test.schemaOrigin)
				firstDDL := append([]string(nil), state.ddl...)
				// Re-entering the full migration first visits the old signature entry.
				// The final descriptor index must complete it without recreate/drop churn.
				predecessor := addUserDefinedFunctionSignatureIndex()
				require.NoError(t, predecessor.Upgrade(txn, 7))
				for _, entry := range routineUpgEntries {
					require.NoError(t, entry.Upgrade(txn, 7))
				}
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
		require.NotContains(t, sql, "GetProtocolVersion")
		lower := strings.ToLower(sql)
		switch {
		case strings.HasPrefix(sql, "SHOW CREATE TABLE"):
			ddl := "create table mo_user_defined_function (name varchar(100), db varchar(100), arg_types varchar(255), canonical_input_descriptor varchar(255)"
			if state.legacyIndex {
				ddl += ", unique key name_db_arg_types(name, db, arg_types)"
			}
			if state.newIndex {
				ddl += ", unique key name_db_arg_types_descriptor(name, db, arg_types, canonical_input_descriptor)"
			}
			return newShowCreateTableResult(t, "mo_user_defined_function", ddl+")"), nil
		case strings.HasPrefix(sql, "select count(distinct i.id)"):
			return indexCatalogCounts(t, [3]int64{1, 1, 1}), nil
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
