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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type viewIdentityTxn struct {
	executor.TxnExecutor
	exec func(string, executor.StatementOption) (executor.Result, error)
}

func (t viewIdentityTxn) Exec(sql string, opts executor.StatementOption) (executor.Result, error) {
	return t.exec(sql, opts)
}

func TestSystemViewDefinitionRecognizesAuthoredFormsAndNameCase(t *testing.T) {
	const ddl = "CREATE VIEW information_schema.VIEWS AS select 1 as n"
	for _, stored := range []string{ddl, "CREATE OR REPLACE VIEW information_schema.VIEWS AS select 1 as n", "CREATE VIEW information_schema.VIEWS AS select 2 as n"} {
		for _, name := range []string{"VIEWS", "views"} {
			t.Run(name+"/"+stored, func(t *testing.T) {
				old := CheckViewDefinition
				CheckViewDefinition = func(_ executor.TxnExecutor, account uint32, schema, actual string) (bool, string, error) {
					require.Equal(t, uint32(7), account)
					require.Equal(t, "information_schema", schema)
					return actual == name, stored, nil
				}
				t.Cleanup(func() { CheckViewDefinition = old })
				entry := InformationSchemaViewUpgradeEntry("VIEWS", ddl, 0)
				ready, err := entry.CheckFunc(nil, 7)
				require.NoError(t, err)
				require.Equal(t, !strings.Contains(stored, "select 2"), ready)
			})
		}
	}
}

func TestLegacySystemViewPreservesIdentityAndStopsOnFailure(t *testing.T) {
	for _, tc := range []struct {
		name, kind, fail  string
		physical, logical uint64
		rows              int
		wantID            uint64
		wantErr           bool
	}{
		{name: "absent"},
		{name: "view", kind: catalog.SystemViewRel, physical: 12, logical: 5, rows: 1},
		{name: "table", kind: catalog.SystemOrdinaryRel, physical: 12, logical: 5, rows: 1, wantID: 5},
		{name: "legacy logical ID", kind: catalog.SystemOrdinaryRel, physical: 12, rows: 1, wantID: 12},
		{name: "unexpected kind", kind: "s", physical: 12, rows: 1, wantErr: true},
		{name: "zero physical ID", kind: catalog.SystemOrdinaryRel, rows: 1, wantErr: true},
		{name: "duplicate", kind: catalog.SystemOrdinaryRel, physical: 12, rows: 2, wantErr: true},
		{name: "read error", fail: "select", wantErr: true},
		{name: "drop error", kind: catalog.SystemOrdinaryRel, physical: 12, rows: 1, fail: "DROP", wantErr: true},
		{name: "create error", kind: catalog.SystemOrdinaryRel, physical: 12, rows: 1, fail: "CREATE", wantErr: true, wantID: 12},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer func() { require.Zero(t, mp.CurrNB()) }()
			failure := errors.New("injected statement failure")
			var writes []string
			tx := viewIdentityTxn{exec: func(sql string, opts executor.StatementOption) (executor.Result, error) {
				require.Equal(t, uint32(7), opts.AccountID())
				if strings.HasPrefix(sql, tc.fail) && tc.fail != "" {
					return executor.Result{}, failure
				}
				if strings.HasPrefix(sql, "select") {
					res := executor.NewMemResult([]types.Type{types.T_uint64.ToType(), types.T_uint64.ToType(), types.T_varchar.ToType()}, mp)
					if tc.rows > 0 {
						res.NewBatchWithRowCount(tc.rows)
						var phys, logical []uint64
						var kinds []string
						for i := 0; i < tc.rows; i++ {
							phys = append(phys, tc.physical)
							logical = append(logical, tc.logical)
							kinds = append(kinds, tc.kind)
						}
						require.NoError(t, executor.AppendFixedRows(res, 0, phys))
						require.NoError(t, executor.AppendFixedRows(res, 1, logical))
						require.NoError(t, executor.AppendStringRows(res, 2, kinds))
					}
					return res.GetResult(), nil
				}
				writes = append(writes, sql)
				if strings.HasPrefix(sql, "CREATE") {
					require.Equal(t, tc.wantID, opts.KeepLogicalId())
				}
				return executor.Result{}, nil
			}}
			entry := InformationSchemaViewUpgradeEntry("KEY_COLUMN_USAGE", "CREATE VIEW information_schema.KEY_COLUMN_USAGE AS select 1 as n", 0)
			entry.UpgType = RECONCILE_LEGACY_INFORMATION_SCHEMA_VIEW
			entry.CheckFunc = func(executor.TxnExecutor, uint32) (bool, error) { return false, nil }
			err := entry.UpgradeAfterProtocolCheck(tx, 7)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if tc.fail != "" {
				require.ErrorIs(t, err, failure)
			}
			if !tc.wantErr {
				if tc.kind == catalog.SystemOrdinaryRel {
					require.Equal(t, []string{"DROP TABLE `information_schema`.`KEY_COLUMN_USAGE`", entry.UpgSql}, writes)
				} else {
					require.Equal(t, []string{entry.UpgSql}, writes)
				}
			} else if tc.fail != "CREATE" {
				require.Empty(t, writes, "failed probe/drop cannot create a replacement")
			}
		})
	}
}
