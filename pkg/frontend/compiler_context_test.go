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

package frontend

import (
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

var _ plan2.ViewDependencyIdentityResolver = (*TxnCompilerContext)(nil)

func TestExecCtxWithRootSQLRestoresScopedValues(t *testing.T) {
	ses := &Session{}
	ses.SetSql("session SQL")
	execCtx := &ExecCtx{ses: ses}
	tcc := &TxnCompilerContext{execCtx: execCtx}
	wantErr := errors.New("stop")

	require.NoError(t, execCtx.withRootSQL("outer SQL", func() error {
		require.Equal(t, "outer SQL", tcc.GetRootSql())
		require.ErrorIs(t, execCtx.withRootSQL("inner SQL", func() error {
			require.Equal(t, "inner SQL", tcc.GetRootSql())
			return wantErr
		}), wantErr)
		require.Equal(t, "outer SQL", tcc.GetRootSql())
		return nil
	}))
	require.Equal(t, "session SQL", tcc.GetRootSql())
}

func TestExecCtxWithRootSQLRestoresAfterPanic(t *testing.T) {
	ses := &Session{}
	ses.SetSql("session SQL")
	execCtx := &ExecCtx{ses: ses}
	tcc := &TxnCompilerContext{execCtx: execCtx}

	require.PanicsWithValue(t, "boom", func() {
		_ = execCtx.withRootSQL("prepared SQL", func() error {
			require.Equal(t, "prepared SQL", tcc.GetRootSql())
			panic("boom")
		})
	})
	require.Equal(t, "session SQL", tcc.GetRootSql())
}

func TestExecCtxCloseClearsRootSQLOverride(t *testing.T) {
	rootSQL := "prepared SQL"
	execCtx := &ExecCtx{rootSQLOverride: &rootSQL}
	execCtx.Close()
	require.Nil(t, execCtx.rootSQLOverride)
}

func TestResolveViewDependencyAccount(t *testing.T) {
	ses := &Session{}
	ses.SetTenantInfo(&TenantInfo{TenantID: 7})
	ses.SetAccountId(7)
	tcc := &TxnCompilerContext{execCtx: &ExecCtx{ses: ses}}

	for _, test := range []struct {
		name     string
		obj      *pbplan.ObjectRef
		snapshot *pbplan.Snapshot
		want     uint32
	}{
		{name: "ordinary tenant table", obj: &pbplan.ObjectRef{SchemaName: "db", ObjName: "t"}, want: 7},
		{name: "snapshot tenant", obj: &pbplan.ObjectRef{SchemaName: "db", ObjName: "t"},
			snapshot: &pbplan.Snapshot{Tenant: &pbplan.SnapshotTenant{TenantID: 8}}, want: 8},
		{name: "subscription publisher", obj: &pbplan.ObjectRef{SchemaName: "db", ObjName: "t",
			PubInfo: &pbplan.PubInfo{TenantId: 9}}, want: 9},
		{name: "subscription overrides snapshot", obj: &pbplan.ObjectRef{SchemaName: "db", ObjName: "t",
			PubInfo: &pbplan.PubInfo{TenantId: 9}},
			snapshot: &pbplan.Snapshot{Tenant: &pbplan.SnapshotTenant{TenantID: 8}}, want: 9},
		{name: "cluster table", obj: &pbplan.ObjectRef{SchemaName: catalog.MO_CATALOG, ObjName: "cluster_table"}, want: 0},
		{name: "statement info", obj: &pbplan.ObjectRef{SchemaName: catalog.MO_SYSTEM, ObjName: catalog.MO_STATEMENT}, want: 0},
		{name: "system relation overrides publisher", obj: &pbplan.ObjectRef{SchemaName: catalog.MO_SYSTEM,
			ObjName: catalog.MO_STATEMENT, PubInfo: &pbplan.PubInfo{TenantId: 9}}, want: 0},
		{name: "metric", obj: &pbplan.ObjectRef{SchemaName: catalog.MO_SYSTEM_METRICS, ObjName: catalog.MO_METRIC}, want: 0},
		{name: "sql statement cu", obj: &pbplan.ObjectRef{SchemaName: catalog.MO_SYSTEM_METRICS, ObjName: catalog.MO_SQL_STMT_CU}, want: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := tcc.ResolveViewDependencyAccount(test.obj, &pbplan.TableDef{}, test.snapshot)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestGetConfig(t *testing.T) {
	tcc := &TxnCompilerContext{
		execCtx: &ExecCtx{
			ses: &Session{},
		},
	}

	tests := []struct {
		varName   string
		dbName    string
		tblName   string
		expected  string
		expectErr bool
	}{
		{
			varName:   "unique_check_on_autoincr",
			dbName:    "test_db",
			tblName:   "test_tbl",
			expected:  "None",
			expectErr: true,
		},
		{
			varName:  "unique_check_on_autoincr",
			dbName:   "mo_catalog",
			tblName:  "test_tbl",
			expected: "Check",
		},
		{
			varName:   "invalid_var",
			dbName:    "test_db",
			tblName:   "test_tbl",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.varName, func(t *testing.T) {
			val, err := tcc.GetConfig(tt.varName, tt.dbName, tt.tblName)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, val)
			}
			require.True(t, len(tcc.GetAccountName()) > 0)
		})
	}
}
