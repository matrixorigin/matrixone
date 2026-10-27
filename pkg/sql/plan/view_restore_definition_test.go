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

package plan

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestPreserveRestoredViewDefinition(t *testing.T) {
	const original = `{"Stmt":"old SQL","DefaultDatabase":"source","definition":"old select","security_type":"INVOKER","sql_mode":"ANSI_QUOTES","lower_case_table_names":0,"check_option":"LOCAL","dependencies":[{"RelationID":7}],"required_protocol_version":100,"future_attribute":{"owner":"source","values":[1,null,"x"]}}`
	const generated = `{"Stmt":"new SQL","DefaultDatabase":"target","definition":"new select","security_type":"DEFINER","sql_mode":"","lower_case_table_names":1,"check_option":"NONE","dependencies":[{"RelationID":9}],"required_protocol_version":72}`
	ctx := WithRestoredViewDefinition(t.Context(), "target", "v", original, "")
	result, err := preserveRestoredViewDefinition(ctx, "target", "v", generated)
	require.NoError(t, err)
	require.JSONEq(t, `{"Stmt":"new SQL","DefaultDatabase":"source","definition":"new select","security_type":"INVOKER","sql_mode":"ANSI_QUOTES","lower_case_table_names":0,"check_option":"LOCAL","dependencies":[{"RelationID":9}],"required_protocol_version":100,"future_attribute":{"owner":"source","values":[1,null,"x"]}}`, result)
	for _, tc := range []struct {
		name, source, rebuilt, want string
	}{
		{"legacy fields stay absent", `{"Stmt":"old","opaque":true}`, `{"Stmt":"new","DefaultDatabase":"target","sql_mode":"ANSI_QUOTES","lower_case_table_names":1,"security_type":"DEFINER"}`, `{"Stmt":"new","opaque":true}`},
		{"empty payload stays unqualified", "", generated, `{"Stmt":"new SQL","definition":"new select","dependencies":[{"RelationID":9}],"required_protocol_version":72}`},
		{"known no USE stays empty", `{"DefaultDatabase":""}`, `{"DefaultDatabase":"target"}`, `{"DefaultDatabase":""}`},
		{"unknown default database stays null", `{"DefaultDatabase":null}`, `{"DefaultDatabase":"target"}`, `{"DefaultDatabase":null}`},
		{"missing rebuilt default database keeps source", `{"DefaultDatabase":"source"}`, `{}`, `{"DefaultDatabase":"source"}`},
		{"source protocol survives unmarked rebuild", `{"required_protocol_version":100}`, `{}`, `{"required_protocol_version":100}`},
		{"new protocol may rise", `{"required_protocol_version":72}`, `{"required_protocol_version":100}`, `{"required_protocol_version":100}`},
		{"equal protocol", `{"required_protocol_version":100}`, `{"required_protocol_version":100}`, `{"required_protocol_version":100}`},
		{"new protocol only", `{}`, `{"required_protocol_version":100}`, `{"required_protocol_version":100}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := WithRestoredViewDefinition(t.Context(), "target", "v", tc.source, "")
			result, err := preserveRestoredViewDefinition(ctx, "target", "v", tc.rebuilt)
			require.NoError(t, err)
			require.JSONEq(t, tc.want, result)
		})
	}
	for _, tc := range []struct {
		name, database, view string
		ctx                  context.Context
	}{
		{"nil context", "target", "v", nil},
		{"ordinary statement", "target", "v", t.Context()},
		{"another database", "other", "v", ctx},
		{"another view", "target", "other", ctx},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := preserveRestoredViewDefinition(tc.ctx, tc.database, tc.view, generated)
			require.NoError(t, err)
			require.Equal(t, generated, result, "恢复上下文不能改变其它 CREATE 或普通用户语句")
		})
	}
	for _, tc := range []struct{ name, source, rebuilt string }{
		{"malformed source", `{`, `{}`},
		{"null source", `null`, `{}`},
		{"malformed source field", `{"sql_mode":7}`, `{}`},
		{"malformed rebuilt", `{}`, `{`},
		{"malformed rebuilt field", `{}`, `{"required_protocol_version":"100"}`},
		{"null rebuilt", `{}`, `null`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := WithRestoredViewDefinition(t.Context(), "target", "v", tc.source, "")
			result, err := preserveRestoredViewDefinition(ctx, "target", "v", tc.rebuilt)
			require.Error(t, err)
			require.Empty(t, result)
		})
	}
}

func TestRestoredViewParserPolicyIsScoped(t *testing.T) {
	const replay = "create view v as select 1"
	for _, tc := range []struct {
		name, definition, sql string
		want                  int64
		invalid               bool
	}{
		{"case sensitive", `{"lower_case_table_names":0}`, replay, 0, false},
		{"lowercase", `{"lower_case_table_names":1}`, replay, 1, false},
		{"preserve names", `{"lower_case_table_names":2}`, replay, 2, false},
		{"legacy absent", `{}`, replay, 2, false},
		{"legacy null", `{"lower_case_table_names":null}`, replay, 2, false},
		{"another statement", `{"lower_case_table_names":0}`, "select 1", 2, false},
		{"negative mode", `{"lower_case_table_names":-1}`, replay, 0, true},
		{"unknown mode", `{"lower_case_table_names":3}`, replay, 0, true},
		{"invalid json", `{`, replay, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := WithRestoredViewDefinition(t.Context(), "tpch", "v", tc.definition, replay)
			got, err := RestoredViewLowerCaseTableNames(ctx, tc.sql, 2)
			if tc.invalid {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			}
		})
	}
	for _, ctx := range []context.Context{nil, t.Context(), WithRestoredViewDefinition(t.Context(), "tpch", "v", `{"lower_case_table_names":0}`, "")} {
		got, err := RestoredViewLowerCaseTableNames(ctx, replay, 2)
		require.NoError(t, err)
		require.EqualValues(t, 2, got)
	}
}

func TestRestoredViewCreationContext(t *testing.T) {
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	originalDatabase, originalMode := compiler.DefaultDatabase(), compiler.GetLowerCaseTableNames()
	for _, tc := range []struct {
		name, definition, wantDatabase string
		invalid                        bool
	}{
		{"creation database differs from view location", `{"DefaultDatabase":"creation_env","lower_case_table_names":0}`, "creation_env", false},
		{"known no USE", `{"DefaultDatabase":"","lower_case_table_names":0}`, "", false},
		{"legacy null", `{"DefaultDatabase":null,"lower_case_table_names":0}`, originalDatabase, false},
		{"legacy missing", `{"lower_case_table_names":0}`, originalDatabase, false},
		{"invalid json", `{`, "", true},
		{"null definition", `null`, "", true},
		{"invalid field", `{"DefaultDatabase":7}`, "", true},
		{"invalid mode", `{"lower_case_table_names":3}`, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiler.SetContext(WithRestoredViewDefinition(t.Context(), "target", "v", tc.definition, ""))
			owned, err := restoredViewCreationContext(compiler, "target", "v")
			if tc.invalid {
				require.Error(t, err)
				require.Nil(t, owned)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantDatabase, owned.DefaultDatabase())
			require.Zero(t, owned.GetLowerCaseTableNames())
			require.Equal(t, compiler.GetRootSql(), owned.GetRootSql())
			require.Equal(t, originalDatabase, compiler.DefaultDatabase())
			require.Equal(t, originalMode, compiler.GetLowerCaseTableNames())
		})
	}
	for _, ctx := range []context.Context{t.Context(), WithRestoredViewDefinition(t.Context(), "target", "other", `{}`, "")} {
		compiler.SetContext(ctx)
		owned, err := restoredViewCreationContext(compiler, "target", "v")
		require.NoError(t, err)
		require.Same(t, compiler, owned)
	}
}

func TestRestoredLegacyStarDoesNotBecomeQualifiedByReplay(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	def := f.addView(t, "root_v", "select * from nation")
	original := def.ViewSql.View
	var rebuilt ViewData
	require.NoError(t, json.Unmarshal([]byte(original), &rebuilt))
	rebuilt.Stmt = "create view root_v as select n_name from nation"
	generated, err := json.Marshal(rebuilt)
	require.NoError(t, err)
	ctx := WithRestoredViewDefinition(t.Context(), "tpch", "root_v", original, "create view root_v as select * from nation")
	def.ViewSql.View, err = preserveRestoredViewDefinition(ctx, "tpch", "root_v", string(generated))
	require.NoError(t, err)
	for _, disabled := range []bool{false, true} {
		r := f.request(t)
		r.memoDisabled = disabled
		result, err := r.Describe("tpch", "root_v", nil)
		result.Release()
		require.Nil(t, result)
		require.ErrorContains(t, err, "LEGACY_STAR_UNAVAILABLE")
		r.Close()
	}
}

func TestBuildCreateViewPreservesRestoredDefinition(t *testing.T) {
	for _, tc := range []struct {
		name, definition string
		wantError        bool
	}{
		{"saved invoker", `{"security_type":"INVOKER","sql_mode":"","lower_case_table_names":1,"future_attribute":{"retain":true}}`, false},
		{"legacy remains distinguishable", `{"future_attribute":{"retain":true}}`, false},
		{"empty legacy remains unqualified", ``, false},
		{"invalid payload", `{`, true},
		{"known no USE cannot inherit restore database", `{"DefaultDatabase":""}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
			compiler.SetContext(WithRestoredViewDefinition(t.Context(), "tpch", "restored_v", tc.definition, "create view restored_v as select n_name from nation"))
			statements, err := mysql.Parse(t.Context(), "create view restored_v as select n_name from nation", 1)
			require.NoError(t, err)
			defer statements[0].Free()
			p, err := BuildPlan(compiler, statements[0], false)
			if tc.wantError {
				require.Error(t, err)
				require.Nil(t, p)
				return
			}
			require.NoError(t, err)
			view := p.GetDdl().GetCreateView().TableDef
			require.Len(t, view.Cols, 1)
			var stored map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(view.ViewSql.View), &stored))
			if tc.definition == "" {
				require.NotContains(t, stored, "future_attribute")
			} else {
				require.JSONEq(t, `{"retain":true}`, string(stored["future_attribute"]))
			}
			require.NotContains(t, stored, "DefaultDatabase")
			if tc.name == "saved invoker" {
				require.JSONEq(t, `"INVOKER"`, string(stored["security_type"]))
			} else {
				require.NotContains(t, stored, "security_type")
				require.NotContains(t, stored, "sql_mode")
				require.NotContains(t, stored, "lower_case_table_names")
			}
		})
	}
}
