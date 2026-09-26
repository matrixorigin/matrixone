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

package frontend

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestWhereAliasSessionPlanInvalidation(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	setPu("", config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil))
	ses := NewSession(ctx, "", &testMysqlWriter{}, nil)
	t.Cleanup(func() { ses.Close() })
	other := NewSession(ctx, "", &testMysqlWriter{}, nil)
	t.Cleanup(func() { other.Close() })
	value, err := ses.GetSessionSysVar("enable_where_alias")
	require.NoError(t, err)
	require.Equal(t, int8(0), value)
	initial := &trackedStatement{}
	ses.cachePlan("initial", []tree.Statement{initial}, []*plan.Plan{{}})
	require.NoError(t, ses.SetSessionSysVar(ctx, "enable_where_alias", int64(0)))
	require.True(t, ses.isCached("initial"), "setting the default is not a semantic change")
	require.Zero(t, initial.freed)
	require.Equal(t, ScopeSession, gSysVarsDefs["enable_where_alias"].Scope)
	require.False(t, gSysVarsDefs["enable_where_alias"].SetVarHintApplies)

	prepared := &PrepareStmt{}
	ses.prepareStmts["p"] = prepared
	for _, value := range []int64{1, 0} {
		stmt := &trackedStatement{}
		ses.cachePlan("cached-sql", []tree.Statement{stmt}, []*plan.Plan{{}})
		prepared.needsRebuild = false
		prepared.compileNeedsRebuild = false
		require.NoError(t, ses.SetSessionSysVar(ctx, "enable_where_alias", value))
		actual, err := ses.GetSessionSysVar("enable_where_alias")
		require.NoError(t, err)
		require.Equal(t, int8(value), actual)
		isolated, err := other.GetSessionSysVar("enable_where_alias")
		require.NoError(t, err)
		require.Equal(t, int8(0), isolated)
		require.False(t, ses.isCached("cached-sql"))
		require.Equal(t, 1, stmt.freed)
		require.True(t, prepared.needsRebuild)
		require.True(t, prepared.compileNeedsRebuild)

		stmt = &trackedStatement{}
		ses.cachePlan("cached-sql", []tree.Statement{stmt}, []*plan.Plan{{}})
		prepared.needsRebuild = false
		prepared.compileNeedsRebuild = false
		require.NoError(t, ses.SetSessionSysVar(ctx, "enable_where_alias", value))
		require.True(t, ses.isCached("cached-sql"))
		require.Zero(t, stmt.freed)
		require.False(t, prepared.needsRebuild)
		require.False(t, prepared.compileNeedsRebuild)
	}
	require.Error(t, ses.SetSessionSysVar(ctx, "enable_where_alias", "invalid"))
	require.True(t, ses.isCached("cached-sql"))
	value, err = ses.GetSessionSysVar("enable_where_alias")
	require.NoError(t, err)
	require.Equal(t, int8(0), value)
}
