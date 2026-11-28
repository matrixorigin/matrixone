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
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
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

func TestViewRestoreExecutionParserUsesSavedPolicy(t *testing.T) {
	const sql = "create view tpch.MixedView as select 1"
	for _, tc := range []struct {
		name, definition, replay, want string
		sessionMode                    int64
		invalid                        bool
	}{
		{"saved lowercase against restore mode zero", `{"lower_case_table_names":1}`, sql, "mixedview", 0, false},
		{"saved case sensitive", `{"lower_case_table_names":0}`, sql, "MixedView", 1, false},
		{"saved preserve names", `{"lower_case_table_names":2}`, sql, "MixedView", 1, false},
		{"legacy absent", `{}`, sql, "MixedView", 0, false},
		{"legacy null", `{"lower_case_table_names":null}`, sql, "MixedView", 0, false},
		{"ordinary statement", "", "", "MixedView", 0, false},
		{"different SQL", `{"lower_case_table_names":1}`, "select 1", "MixedView", 0, false},
		{"invalid saved mode", `{"lower_case_table_names":3}`, sql, "", 0, true},
		{"malformed payload", `{`, sql, "", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Context(t.Context())
			if tc.definition != "" {
				ctx = plan.WithRestoredViewDefinition(ctx, "tpch", "mixedview", tc.definition, tc.replay)
			}
			ses := &wrapperParseSession{lowerCaseTableNames: tc.sessionMode}
			input := &UserInput{sql: sql, useParserSQLMode: true}
			execCtx := &ExecCtx{reqCtx: ctx, ses: ses, input: input}
			wrappers, err := GetComputationWrapperInBack(execCtx, "tpch", input, "", nil, testutil.NewProcess(t), ses)
			if tc.invalid {
				require.Error(t, err)
				require.Empty(t, wrappers)
				return
			}
			require.NoError(t, err)
			require.Len(t, wrappers, 1)
			defer wrappers[0].GetAst().Free()
			view := wrappers[0].GetAst().(*tree.CreateView)
			require.Equal(t, tc.want, string(view.Name.ObjectName), "assert the AST used for execution, not preliminary classification")
			require.Equal(t, tc.sessionMode, parserLowerCaseTableNames(ses), "restore must not mutate the session policy")
		})
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

// The authentic release-authored extra-grouping fixture bytes (captured from
// the real v4.2.9 server; verified marker-free) drive the actual frontend
// restore helpers.
const releaseVNestedViewData6066215544 = `{"Stmt":"create view pg200.v_nested as ((select count(*) as a from pg200.tw_src interval(ts, 1, minute))) interval(ts, 2, minute) limit 5","DefaultDatabase":"","sql_mode":"ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES","security_type":"DEFINER"}`

// pg200SeedCompiler binds only the fixture's own physical source identity and
// delegates everything else to the underlying mock compiler.
type pg200SeedCompiler struct {
	*plan.MockCompilerContext
}

func (c *pg200SeedCompiler) Resolve(schemaName, tableName string, snapshot *plan.Snapshot, resolution *plan.TableResolution) (*plan.ObjectRef, *plan.TableDef, error) {
	if strings.EqualFold(schemaName, "pg200") && strings.EqualFold(tableName, "tw_src") {
		def := &plan.TableDef{
			Name: "tw_src", DbName: "pg200", TblId: 4202, LogicalId: 4202,
			Cols: []*plan.ColDef{{Name: "ts", Typ: plan.Type{Id: int32(types.T_timestamp)}}},
		}
		return &plan.ObjectRef{DbName: "pg200", ObjName: "tw_src", Obj: 4202, SchemaName: "pg200"}, def, nil
	}
	return c.MockCompilerContext.Resolve(schemaName, tableName, snapshot, resolution)
}

func TestViewRestoreHelpersBindReleasedExtraGroupingFixture(t *testing.T) {
	inner := plan.NewMockCompilerContext(false, testutil.NewProcess(t))
	compiler := &pg200SeedCompiler{MockCompilerContext: inner}
	original := t.Context()
	compiler.SetContext(original)
	createSQL := "create view pg200.v_nested as ((select count(*) as a from pg200.tw_src interval(ts, 1, minute))) interval(ts, 2, minute) limit 5"
	info := &tableInfo{dbName: "pg200", tblName: "v_nested", createSql: createSQL, viewDef: releaseVNestedViewData6066215544}
	statements, err := parseViewCreateSQLForRestore(original, info, 1)
	require.NoErrorf(t, err, "the release-authored CREATE VIEW must parse through the frontend restore helper")
	defer freeStatements(statements)
	p, err := buildViewCreatePlanForRestore(compiler, info, statements[0])
	require.NoErrorf(t, err, "the actual frontend restore helper must plan the released extra-grouping fixture")
	require.NotNilf(t, p, "the restore helper must produce a real plan")
	// the helper owns and restores the compiler context.
	require.Same(t, original, compiler.GetContext(), "the restore helper must restore the original context ownership")
	// the emitted CREATE DDL persists the authored statement and the physical dependency identity.
	var emitted plan.ViewData
	require.NoErrorf(t, json.Unmarshal([]byte(p.GetDdl().GetCreateView().TableDef.ViewSql.View), &emitted), "the emitted CREATE DDL must decode")
	require.NotEmptyf(t, emitted.Stmt, "the emitted CREATE DDL must carry the authored statement")
	var sawSrc bool
	for _, dep := range emitted.Dependencies {
		if strings.EqualFold(dep.DatabaseName, "pg200") && strings.EqualFold(dep.RelationName, "tw_src") {
			require.Equalf(t, uint64(4202), dep.RelationID, "the emitted CREATE DDL must persist the physical RelationID")
			require.Equalf(t, uint64(4202), dep.LogicalID, "the emitted CREATE DDL must persist the physical LogicalID")
			sawSrc = true
		}
	}
	require.Truef(t, sawSrc, "the emitted CREATE DDL must bind the seeded pg200.tw_src dependency (refuse nil/empty)")
	// the planned view definition: exactly one visible BIGINT a.
	cols := p.GetDdl().GetCreateView().TableDef.Cols
	require.Lenf(t, cols, 1, "the planned view definition has exactly one visible output column")
	require.Truef(t, strings.EqualFold(cols[0].Name, "a"), "the planned output column is a")
	require.Equalf(t, int32(types.T_int64), cols[0].Typ.Id, "the planned output column a is BIGINT per the release readback")
}

func TestViewRestoreHelpersBindReleasedTripleWindowFixture(t *testing.T) {
	inner := plan.NewMockCompilerContext(false, testutil.NewProcess(t))
	compiler := &pg200SeedCompiler{MockCompilerContext: inner}
	original := t.Context()
	compiler.SetContext(original)
	createSQL := "create view pg200.v_triple as ((select count(*) as a from pg200.tw_src interval(ts, 1, minute)) interval(ts, 2, minute) order by a) interval(ts, 3, minute) limit 5"
	def := `{"Stmt":"` + createSQL + `","DefaultDatabase":"pg200","sql_mode":"","security_type":"DEFINER"}`
	info := &tableInfo{dbName: "pg200", tblName: "v_triple", createSql: createSQL, viewDef: def}
	statements, err := parseViewCreateSQLForRestore(original, info, 1)
	require.NoErrorf(t, err, "the reviewers' triple-window CREATE VIEW must parse through the frontend restore helper")
	defer freeStatements(statements)
	p, err := buildViewCreatePlanForRestore(compiler, info, statements[0])
	require.NoErrorf(t, err, "the actual frontend restore helper must plan the released triple-window fixture")
	require.NotNilf(t, p, "the restore helper must produce a real plan")
	require.Same(t, original, compiler.GetContext(), "the restore helper must restore the original context ownership")
	var emitted plan.ViewData
	require.NoErrorf(t, json.Unmarshal([]byte(p.GetDdl().GetCreateView().TableDef.ViewSql.View), &emitted), "the emitted CREATE DDL must carry ViewData")
	require.Equalf(t, createSQL, emitted.Stmt, "the emitted CREATE DDL must persist the authored statement exactly, unrewritten")
	require.NotContainsf(t, p.GetDdl().GetCreateView().TableDef.ViewSql.View, "cte_scope_protocol",
		"the restore must not promote the marker on the marker-free triple-window body")
	sawSrc := false
	for _, dep := range emitted.Dependencies {
		if strings.EqualFold(dep.DatabaseName, "pg200") && strings.EqualFold(dep.RelationName, "tw_src") {
			require.Equalf(t, uint64(4202), dep.RelationID, "the emitted CREATE DDL must persist the physical RelationID")
			require.Equalf(t, uint64(4202), dep.LogicalID, "the emitted CREATE DDL must persist the physical LogicalID")
			sawSrc = true
		}
	}
	require.Truef(t, sawSrc, "the emitted CREATE DDL must bind the seeded pg200.tw_src dependency")
	cols := p.GetDdl().GetCreateView().TableDef.Cols
	require.Lenf(t, cols, 1, "the planned triple-window view definition has exactly one visible output column")
	require.Truef(t, strings.EqualFold(cols[0].Name, "a"), "the planned output column is a")
	require.Equalf(t, int32(types.T_int64), cols[0].Typ.Id, "the planned output column a is BIGINT per the middle-window binding")
}
