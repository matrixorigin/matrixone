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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestViewRestoreDependencySortUsesCreationEnvironment(t *testing.T) {
	compiler := plan.NewMockCompilerContext(false, testutil.NewProcess(t))
	original := t.Context()
	compiler.SetContext(original)
	info := &tableInfo{dbName: "tpch", tblName: "restore_v", createSql: "create view tpch.restore_v as select 1 as id",
		viewDef: `{"DefaultDatabase":"creation_env","security_type":"INVOKER","lower_case_table_names":0}`}
	statements, err := parseViewCreateSQLForRestore(original, info, 1)
	require.NoError(t, err)
	defer freeStatements(statements)
	p, err := buildViewCreatePlanForRestore(compiler, info, statements[0])
	require.NoError(t, err)
	require.Same(t, original, compiler.GetContext())
	require.Equal(t, "tpch", compiler.DefaultDatabase())
	var definition plan.ViewData
	require.NoError(t, json.Unmarshal([]byte(p.GetDdl().GetCreateView().TableDef.ViewSql.View), &definition))
	require.Equal(t, "creation_env", definition.DefaultDatabase)
	require.Equal(t, "INVOKER", definition.SecurityType)
	info.viewDef = `{`
	_, err = buildViewCreatePlanForRestore(compiler, info, statements[0])
	require.Error(t, err)
	require.Same(t, original, compiler.GetContext(), "失败也必须撤销临时恢复上下文")
}

func TestCloneViewRestoresOnlyMappedCreationDatabase(t *testing.T) {
	for _, tc := range []struct {
		name, source, want string
	}{
		{"mapped", `"source"`, `"target"`},
		{"external", `"external"`, `"external"`},
		{"no USE", `""`, `""`},
		{"unknown", `null`, `null`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			definition := `{"DefaultDatabase":` + tc.source + `,"sql_mode":"ANSI_QUOTES","lower_case_table_names":0,"security_type":"INVOKER","future":{"x":[1]}}`
			info := &tableInfo{dbName: "source", tblName: "v", viewDef: definition,
				createSql: `create view source.v as select "x" from source.t`}
			key := genKey("source", "v")
			mapped, order, err := rewriteCloneViewInfos(map[string]*tableInfo{key: info}, []string{key}, "source", "target", 1)
			require.NoError(t, err)
			require.Equal(t, []string{genKey("target", "v")}, order)
			clone := mapped[order[0]]
			require.Equal(t, "target", clone.dbName)
			require.Contains(t, clone.createSql, "`x`", "ANSI_QUOTES must remain an identifier, not become a string literal")
			require.Contains(t, clone.createSql, "`target`.`t`")
			require.JSONEq(t, `{"DefaultDatabase":`+tc.want+`,"sql_mode":"ANSI_QUOTES","lower_case_table_names":0,"security_type":"INVOKER","future":{"x":[1]}}`, clone.viewDef)
			require.Equal(t, definition, info.viewDef, "clone must not mutate the source payload")
		})
	}
	for _, definition := range []string{"", `{}`, `null`, `{"future":1}`} {
		got, err := rebindCloneViewCreationDatabase(definition, "source", "target", 1)
		require.NoError(t, err)
		require.Equal(t, definition, got)
	}
	for _, definition := range []string{`{`, `{"DefaultDatabase":7}`} {
		_, err := rebindCloneViewCreationDatabase(definition, "source", "target", 1)
		require.Error(t, err)
	}
	for _, mode := range []int64{0, 1} {
		got, err := rebindCloneViewCreationDatabase(`{"DefaultDatabase":"SOURCE"}`, "source", "target", mode)
		require.NoError(t, err)
		if mode == 0 {
			require.JSONEq(t, `{"DefaultDatabase":"SOURCE"}`, got)
		} else {
			require.JSONEq(t, `{"DefaultDatabase":"target"}`, got)
		}
	}
}

func TestViewRestoreParserPreservesIdentifierCase(t *testing.T) {
	info := &tableInfo{dbName: "tpch", tblName: "MixedCase", createSql: "create view tpch.MixedCase as select 1",
		viewDef: `{"lower_case_table_names":0}`}
	statements, err := parseViewCreateSQLForRestore(t.Context(), info, 1)
	require.NoError(t, err)
	defer freeStatements(statements)
	require.Equal(t, "MixedCase", string(statements[0].(*tree.CreateView).Name.ObjectName))
	info.viewDef = `{"lower_case_table_names":3}`
	_, err = parseViewCreateSQLForRestore(t.Context(), info, 1)
	require.ErrorContains(t, err, "lower_case_table_names")

	// Reach the real background executor's parser-policy rejection before it
	// creates a transaction or accesses any engine service.
	parent := newFeatureLimitTestSession(t)
	back := &backExec{backSes: &backSession{}}
	back.backSes.upstream = parent
	ctx := plan.WithRestoredViewDefinition(defines.AttachAccountId(t.Context(), 0), info.dbName, info.tblName, info.viewDef, info.createSql)
	require.ErrorContains(t, back.ExecWithSQLMode(ctx, info.createSql, ""), "lower_case_table_names")
	require.EqualValues(t, -1, back.GetLastAffectedRows())

	// The ordinary parser still follows its caller's policy.
	ordinary, err := mysql.Parse(t.Context(), info.createSql, 1)
	require.NoError(t, err)
	defer freeStatements(ordinary)
	require.Equal(t, "mixedcase", string(ordinary[0].(*tree.CreateView).Name.ObjectName))
}
