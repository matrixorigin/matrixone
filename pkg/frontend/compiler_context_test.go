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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestViewMetadataStatusRequiresPersistedCurrentRow(t *testing.T) {
	require.False(t, viewMetadataStatusIsCurrent(false, "", "", "", true))
	for _, status := range []string{
		catalog.ViewRefreshStatusPending,
		catalog.ViewRefreshStatusDiscovering,
		catalog.ViewRefreshStatusRunning,
		catalog.ViewRefreshStatusInvalid,
	} {
		require.False(t, viewMetadataStatusIsCurrent(true, status, "", "", true), status)
	}
	require.False(t, viewMetadataStatusIsCurrent(true, catalog.ViewRefreshStatusCurrent,
		catalog.ViewRefreshStatusRevalidateRequired, "", true))
	require.False(t, viewMetadataStatusIsCurrent(true, catalog.ViewRefreshStatusCurrent,
		catalog.ViewRefreshStatusRevalidateScan, "", true))
	require.True(t, viewMetadataStatusIsCurrent(true, catalog.ViewRefreshStatusCurrent,
		catalog.ViewRefreshStatusLegacyScan, "", true))
}

func TestViewMetadataStatusFailsClosedBeforeDisabledBarrierTick(t *testing.T) {
	require.True(t, viewMetadataStatusIsCurrent(false, "", "", catalog.ViewRefreshStatusLegacyScan, false))
	require.False(t, viewMetadataStatusIsCurrent(true, catalog.ViewRefreshStatusCurrent, "",
		catalog.ViewRefreshStatusActivated, false))
	require.True(t, viewMetadataStatusIsCurrent(true, catalog.ViewRefreshStatusCurrent, "",
		catalog.ViewRefreshStatusActivated, true))
	for _, status := range []string{
		catalog.ViewRefreshStatusRevalidateRequired,
		catalog.ViewRefreshStatusRevalidateScan,
	} {
		require.False(t, viewMetadataStatusIsCurrent(true, catalog.ViewRefreshStatusCurrent, "", status, true))
		require.False(t, viewMetadataStatusIsCurrent(true, catalog.ViewRefreshStatusCurrent, "", status, false))
	}
}

func TestViewMetadataCatalogReadinessFallbackIsTyped(t *testing.T) {
	missingTable := moerr.NewNoSuchTableNoCtx("mo_catalog", catalog.MO_VIEW_REFRESH)
	missingDatabase := moerr.NewBadDBNoCtx(catalog.MO_CATALOG)
	retryable := moerr.NewTxnNeedRetryNoCtx()
	require.True(t, ignoreViewMetadataCatalogReadinessError(missingTable, false))
	require.True(t, ignoreViewMetadataCatalogReadinessError(missingDatabase, false))
	require.False(t, ignoreViewMetadataCatalogReadinessError(missingTable, true))
	require.False(t, ignoreViewMetadataCatalogReadinessError(retryable, false))
}

func TestSystemViewsDoNotRequireRefreshState(t *testing.T) {
	tcc := &TxnCompilerContext{}
	for _, databaseName := range catalog.SystemDatabases {
		require.NoError(t, tcc.EnsureViewMetadataCurrent(databaseName, "system view", 0, 1))
	}
}

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
