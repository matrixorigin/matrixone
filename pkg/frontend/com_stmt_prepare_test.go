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

package frontend

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestBuildComStmtPrepareSQLPreservesStatementText(t *testing.T) {
	testCases := []struct {
		name    string
		sqlMode string
		sql     string
	}{
		{name: "explain analyze", sql: "explain analyze select 1"},
		{name: "quote", sql: "select 'single quote: '''"},
		{name: "backslash", sql: `select 'backslash: \\'`},
		{name: "no backslash escapes", sqlMode: "NO_BACKSLASH_ESCAPES", sql: `select 'backslash: \\'`},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wrappedSQL := buildComStmtPrepareSQL("__mo_stmt_1", tc.sql, tc.sqlMode)
			stmts, err := mysql.ParseWithSQLMode(context.Background(), wrappedSQL, 1, tc.sqlMode)
			require.NoError(t, err)
			require.Len(t, stmts, 1)

			prepare, ok := stmts[0].(*tree.PrepareString)
			require.True(t, ok)
			require.Equal(t, "__mo_stmt_1", string(prepare.Name))
			require.Equal(t, tc.sql, prepare.Sql)
			prepare.Free()
		})
	}
}

func TestComStmtPrepareRewritePolicyConsumedOnce(t *testing.T) {
	tests := []struct {
		name         string
		policy       *rewritePolicySnapshot
		wantRewrites []string
	}{
		{
			name: "single role rule",
			policy: &rewritePolicySnapshot{
				enabled:   true,
				roleRules: map[string]string{"src.t": "select * from src.t where role_keep = 1"},
			},
			wantRewrites: []string{"select * from src.t where role_keep = 1"},
		},
		{
			name: "stacked role and session rules",
			policy: &rewritePolicySnapshot{
				enabled:      true,
				roleRules:    map[string]string{"src.t": "select * from src.t where role_keep = 1"},
				sessionRules: map[string]string{"src.t": "select * from src.t where session_keep = 1"},
			},
			wantRewrites: []string{
				"select * from src.t where role_keep = 1",
				"select * from src.t where session_keep = 1",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			ses := newTestSession(t, ctrl)
			defer ses.Close()

			payload := "select * from src.t"
			input, err := buildComStmtPrepareInput(ctx, "__mo_stmt_1", payload, "", tc.policy)
			require.NoError(t, err)
			require.True(t, input.rewritePolicyMaterialized)
			materializedWrapper := input.getSql()

			stmts, err := mysql.Parse(ctx, materializedWrapper, 1)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			prepare, ok := stmts[0].(*tree.PrepareString)
			require.True(t, ok)
			defer prepare.Free()
			// The protocol payload stays raw. Only the outer wrapper carries the
			// materialized request policy until prepareStringStatement consumes it.
			require.Equal(t, payload, prepare.Sql)

			execCtx := newTestExecCtx(ctx, ctrl)
			execCtx.rewriteEnabled = true
			execCtx.sqlOfStmt = materializedWrapper
			rewritten, inner, _, err := prepareStringStatement(execCtx, ses, prepare.Sql)
			require.NoError(t, err)
			defer inner.Free()

			content, ok := leadingHintContent(rewritten)
			require.True(t, ok)
			chains, _, err := parsers.DecodeRewriteHint(ctx, content)
			require.NoError(t, err)
			require.Equal(t, tc.wantRewrites, chains["src.t"])
		})
	}
}
