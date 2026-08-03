// Copyright 2026 Matrix Origin
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
	"encoding/json"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func TestBuildPendingViewMetadataRetryQueryIsBounded(t *testing.T) {
	query := buildPendingViewMetadataRetryQuery(7, 42)
	require.Contains(t, query, "metadata_refresh_pending")
	require.Contains(t, query, "account_id > 7")
	require.Contains(t, query, "account_id = 7 and rel_id > 42")
	require.Contains(t, query, "limit 1024")
	require.NotContains(t, query, "limit 1025")
}

func TestLoadPendingViewMetadataRetryPageRotatesAfterFailures(t *testing.T) {
	resetPendingViewMetadataCursor(t)
	ctx := context.Background()
	bh := &backgroundExecTest{}
	bh.init()
	firstQuery := buildPendingViewMetadataRetryQuery(0, 0)
	secondQuery := buildPendingViewMetadataRetryQuery(7, 42)
	bh.sql2result[firstQuery] = newPendingViewMetadataRetryResult([][]interface{}{
		{uint64(7), uint64(41), uint64(1), "db", "v1", "{}"},
		{uint64(7), uint64(42), uint64(1), "db", "v2", "{}"},
	})
	bh.sql2result[secondQuery] = newPendingViewMetadataRetryResult(nil)

	results, err := loadPendingViewMetadataRetryPage(ctx, bh)
	require.NoError(t, err)
	require.Equal(t, uint64(2), results[0].GetRowCount())
	results, err = loadPendingViewMetadataRetryPage(ctx, bh)
	require.NoError(t, err)
	require.Equal(t, uint64(2), results[0].GetRowCount())
	require.Equal(t, []string{firstQuery, secondQuery, firstQuery}, bh.executedSQLs)
}

func TestRetryPendingViewMetadataSkipsMissingDefaultDatabase(t *testing.T) {
	resetPendingViewMetadataCursor(t)
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	bh := &backgroundExecTest{}
	bh.init()
	viewData, err := json.Marshal(plan2.ViewData{
		Stmt:            "create view db.v as select 1",
		DefaultDatabase: "missing_db",
	})
	require.NoError(t, err)
	query := buildPendingViewMetadataRetryQuery(0, 0)
	bh.sql2result[query] = newPendingViewMetadataRetryResult([][]interface{}{
		{uint64(7), uint64(42), uint64(1), "db", "v", string(viewData)},
	})
	bh.sql2err["use `missing_db`"] = moerr.NewBadDB(ctx, "missing_db")

	require.NoError(t, retryPendingViewMetadata(ctx, ses, bh))
}

func TestRetryPendingViewMetadataPropagatesMalformedCatalogRows(t *testing.T) {
	valid := []interface{}{uint64(7), uint64(42), uint64(1), "db", "v", "{}"}
	for column := range valid {
		t.Run([]string{"account", "view", "version", "database", "name", "definition"}[column], func(t *testing.T) {
			resetPendingViewMetadataCursor(t)
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			ses := newTestSession(t, ctrl)
			defer ses.Close()
			bh := &backgroundExecTest{}
			bh.init()
			row := append([]interface{}(nil), valid...)
			row[column] = struct{}{}
			bh.sql2result[buildPendingViewMetadataRetryQuery(0, 0)] =
				newPendingViewMetadataRetryResult([][]interface{}{row})

			require.Error(t, retryPendingViewMetadata(ctx, ses, bh))
		})
	}
}

func TestRetryPendingViewMetadataSQLModeAndErrors(t *testing.T) {
	tests := []struct {
		name        string
		viewData    plan2.ViewData
		execError   error
		wantError   bool
		wantSQLMode string
	}{
		{
			name:        "legacy sql mode",
			viewData:    plan2.ViewData{Stmt: "create view db.v as select 1"},
			wantSQLMode: plan2.LegacyViewParserSQLMode(),
		},
		{
			name: "saved sql mode",
			viewData: plan2.ViewData{
				Stmt:    "create view db.v as select 1",
				SQLMode: viewMetadataPtr("ANSI_QUOTES"),
			},
			wantSQLMode: "ANSI_QUOTES",
		},
		{
			name:      "skippable refresh error",
			viewData:  plan2.ViewData{Stmt: "create view db.v as select 1"},
			execError: moerr.NewNoSuchTable(context.Background(), "db", "t"),
		},
		{
			name:      "catalog refresh error",
			viewData:  plan2.ViewData{Stmt: "create view db.v as select 1"},
			execError: moerr.NewInternalErrorNoCtx("catalog write failed"),
			wantError: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resetPendingViewMetadataCursor(t)
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			ses := newTestSession(t, ctrl)
			defer ses.Close()
			bh := &backgroundExecTest{}
			bh.init()
			definition, err := json.Marshal(test.viewData)
			require.NoError(t, err)
			bh.sql2result[buildPendingViewMetadataRetryQuery(0, 0)] =
				newPendingViewMetadataRetryResult([][]interface{}{
					{uint64(7), uint64(42), uint64(1), "db", "v", string(definition)},
				})
			refreshSQL, err := compile.BuildViewMetadataRefreshSQL(ctx, 1, "db", "v", test.viewData)
			require.NoError(t, err)
			bh.sql2err[refreshSQL] = test.execError

			err = retryPendingViewMetadata(ctx, ses, bh)
			if test.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if test.wantSQLMode != "" {
				require.Equal(t, test.wantSQLMode, bh.parserSQLMode)
			}
		})
	}
}

func TestLoadPendingViewMetadataRetryPageErrorPaths(t *testing.T) {
	t.Run("query error", func(t *testing.T) {
		resetPendingViewMetadataCursor(t)
		ctx := context.Background()
		bh := &backgroundExecTest{}
		bh.init()
		query := buildPendingViewMetadataRetryQuery(0, 0)
		bh.sql2err[query] = moerr.NewInternalErrorNoCtx("query failed")

		_, err := loadPendingViewMetadataRetryPage(ctx, bh)
		require.Error(t, err)
	})

	t.Run("malformed cursor columns", func(t *testing.T) {
		for _, row := range [][]interface{}{
			{nil, uint64(42), uint64(1), "db", "v", "{}"},
			{uint64(7), nil, uint64(1), "db", "v", "{}"},
		} {
			resetPendingViewMetadataCursor(t)
			ctx := context.Background()
			bh := &backgroundExecTest{}
			bh.init()
			bh.sql2result[buildPendingViewMetadataRetryQuery(0, 0)] =
				newPendingViewMetadataRetryResult([][]interface{}{row})

			_, err := loadPendingViewMetadataRetryPage(ctx, bh)
			require.Error(t, err)
		}
	})

	t.Run("empty page resets cursor", func(t *testing.T) {
		resetPendingViewMetadataCursor(t)
		pendingViewMetadataCursor.Lock()
		pendingViewMetadataCursor.accountID = 7
		pendingViewMetadataCursor.viewID = 42
		pendingViewMetadataCursor.Unlock()
		ctx := context.Background()
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[buildPendingViewMetadataRetryQuery(7, 42)] = newPendingViewMetadataRetryResult(nil)
		bh.sql2result[buildPendingViewMetadataRetryQuery(0, 0)] = newPendingViewMetadataRetryResult(nil)

		results, err := loadPendingViewMetadataRetryPage(ctx, bh)
		require.NoError(t, err)
		require.False(t, execResultArrayHasData(results))
		pendingViewMetadataCursor.Lock()
		require.Zero(t, pendingViewMetadataCursor.accountID)
		require.Zero(t, pendingViewMetadataCursor.viewID)
		pendingViewMetadataCursor.Unlock()
	})
}

func TestHandleCreateFunctionRetriesPendingViewMetadata(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	bh := &backgroundExecTest{}
	bh.init()
	backgroundStub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer backgroundStub.Reset()
	initStub := gostub.StubFunc(&initFunctionFunc, nil)
	defer initStub.Reset()
	retried := false
	retryStub := gostub.Stub(&retryPendingViewMetadataFunc, func(context.Context, *Session, BackgroundExec) error {
		retried = true
		return nil
	})
	defer retryStub.Reset()

	err := handleCreateFunction(ses, &ExecCtx{reqCtx: ctx}, &tree.CreateFunction{})
	require.NoError(t, err)
	require.True(t, retried)
}

func resetPendingViewMetadataCursor(t *testing.T) {
	pendingViewMetadataCursor.Lock()
	oldAccountID := pendingViewMetadataCursor.accountID
	oldViewID := pendingViewMetadataCursor.viewID
	oldGeneration := pendingViewMetadataCursor.generation
	pendingViewMetadataCursor.accountID = 0
	pendingViewMetadataCursor.viewID = 0
	pendingViewMetadataCursor.generation = 0
	pendingViewMetadataCursor.Unlock()
	t.Cleanup(func() {
		pendingViewMetadataCursor.Lock()
		pendingViewMetadataCursor.accountID = oldAccountID
		pendingViewMetadataCursor.viewID = oldViewID
		pendingViewMetadataCursor.generation = oldGeneration
		pendingViewMetadataCursor.Unlock()
	})
}

func newPendingViewMetadataRetryResult(rows [][]interface{}) *MysqlResultSet {
	columns := []string{"account_id", "rel_id", "rel_version", "reldatabase", "relname", "viewdef"}
	result := &MysqlResultSet{}
	for _, name := range columns {
		column := &MysqlColumn{}
		column.SetName(name)
		column.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		result.AddColumn(column)
	}
	for _, row := range rows {
		result.AddRow(row)
	}
	return result
}

func viewMetadataPtr[T any](value T) *T {
	return &value
}
