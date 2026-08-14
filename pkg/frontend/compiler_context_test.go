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
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

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

func TestGetIndexVisibilitiesAtSnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := defines.AttachAccountId(context.Background(), 1)
	result := mock_frontend.NewMockExecResult(ctrl)
	result.EXPECT().GetRowCount().Return(uint64(2)).AnyTimes()
	result.EXPECT().GetString(gomock.Any(), uint64(0), uint64(0)).Return("idx_visible", nil)
	result.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(1)).Return(int64(1), nil)
	result.EXPECT().GetString(gomock.Any(), uint64(1), uint64(0)).Return("IDX_INVISIBLE", nil)
	result.EXPECT().GetInt64(gomock.Any(), uint64(1), uint64(1)).Return(int64(0), nil)

	bh := mock_frontend.NewMockBackgroundExec(ctrl)
	bh.EXPECT().ClearExecResultSet()
	bh.EXPECT().Exec(gomock.Any(),
		"SELECT name, is_visible FROM mo_catalog.mo_indexes {MO_TS = 42} WHERE table_id = 99",
	).DoAndReturn(func(queryCtx context.Context, _ string) error {
		accountID, err := defines.GetAccountId(queryCtx)
		require.NoError(t, err)
		require.Equal(t, uint32(7), accountID)
		return nil
	})
	bh.EXPECT().GetExecResultSet().Return([]interface{}{result})

	got, err := getIndexVisibilities(ctx, bh, 99, &plan.Snapshot{
		TS:     &timestamp.Timestamp{PhysicalTime: 42},
		Tenant: &plan.SnapshotTenant{TenantID: 7},
	})
	require.NoError(t, err)
	require.Equal(t, map[string]bool{
		"idx_visible":   true,
		"idx_invisible": false,
	}, got)
}
