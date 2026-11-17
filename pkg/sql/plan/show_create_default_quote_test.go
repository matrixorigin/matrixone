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

package plan

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	sqlkafka "github.com/matrixorigin/matrixone/pkg/sql/kafka"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// SHOW CREATE TABLE must print a string default so that replaying the
// output stores the same default (#31066).
func TestShowCreateTableStringDefaultRoundTrip(t *testing.T) {
	mock := NewMockOptimizer(false)
	const sourceSQL = "create table t_default_quote (" +
		"a varchar(10) default 'it''s', " +
		"b varchar(10) default 'x`y', " +
		"c varchar(10) default \"q\"\"r\", " +
		"d varchar(10) default 'a\\\\b', " +
		"e varchar(10) default '', " +
		"f binary(8) default 'a', " +
		"g varchar(10) default 'NULL', " +
		"h varchar(10) default NULL)"

	sourceDef, err := buildTestCreateTableStmt(mock, sourceSQL)
	require.NoError(t, err)

	showSQL, parsed, err := ConstructCreateTableSQL(&mock.ctxt, sourceDef, nil, false, nil)
	require.NoError(t, err)
	if parsed != nil {
		parsed.Free()
	}
	for _, want := range []string{
		"`a` varchar(10) DEFAULT 'it''s'",
		"`b` varchar(10) DEFAULT 'x`y'",
		"`c` varchar(10) DEFAULT 'q\"r'",
		"`d` varchar(10) DEFAULT 'a\\\\b'",
		"`e` varchar(10) DEFAULT ''",
		"`f` binary(8) DEFAULT 'a'",
		"`g` varchar(10) DEFAULT 'NULL'",
		"`h` varchar(10) DEFAULT NULL",
	} {
		require.True(t, strings.Contains(showSQL, want), "missing %q in\n%s", want, showSQL)
	}

	before := proto.Clone(sourceDef)
	for _, mode := range []string{"", "NO_BACKSLASH_ESCAPES", "ANSI_QUOTES", "ANSI_QUOTES,NO_BACKSLASH_ESCAPES"} {
		t.Run(mode, func(t *testing.T) {
			mock.ctxt.SetSqlModeOverride(mode)
			current := sourceDef
			for generation := 0; generation < 2; generation++ {
				ddl, parsed, err := ConstructCreateTableSQL(&mock.ctxt, current, nil, false, nil)
				require.NoError(t, err)
				// The display AST and a replay of the displayed text must
				// agree under the caller's SQL mode.
				require.NotNil(t, parsed)
				fromAST, err := BuildPlan(&mock.ctxt, parsed, false)
				parsed.Free()
				require.NoError(t, err)
				for i, col := range sourceDef.Cols {
					require.Equal(t, col.Default.OriginString, fromAST.GetDdl().GetCreateTable().GetTableDef().Cols[i].Default.OriginString, col.Name)
				}
				stmt, err := mysql.ParseOneWithSQLMode(t.Context(), ddl, 1, mode)
				require.NoError(t, err)
				rebuilt, err := BuildPlan(&mock.ctxt, stmt, false)
				stmt.Free()
				require.NoError(t, err)
				next := rebuilt.GetDdl().GetCreateTable().GetTableDef()
				for i, col := range sourceDef.Cols {
					require.Equal(t, col.Default.OriginString, next.Cols[i].Default.OriginString, col.Name)
					require.True(t, proto.Equal(col.Default.Expr, next.Cols[i].Default.Expr), col.Name)
				}
				current = next
			}
			_, err := ConstructCreateTableSQLForSchemaHash(sourceDef)
			require.NoError(t, err)
			require.True(t, proto.Equal(before, sourceDef), "rendering must not mutate shared metadata")
		})
	}
}

func TestShowCreateGeneratedAndSetReplayWithoutBackslashEscapes(t *testing.T) {
	mock := NewMockOptimizer(false)
	source, err := buildTestCreateTableStmt(mock, `create table show_mode_expression (
		v varchar(32),
		g varchar(64) generated always as (concat(v, 'a\\b')) stored,
		s set('line\nbreak','slash\\value','plain'))`)
	require.NoError(t, err)
	wantGenerated := FindColumn(source.Cols, "g").GeneratedCol
	wantSet := FindColumn(source.Cols, "s").Typ.Enumvalues
	require.Contains(t, wantSet, "line\nbreak")
	legacy, err := ConstructCreateTableSQLForSchemaHash(source)
	require.NoError(t, err)
	before := proto.Clone(source)

	mock.ctxt.SetSqlModeOverride("NO_BACKSLASH_ESCAPES")
	current := source
	for generation := 0; generation < 2; generation++ {
		ddl, parsed, err := ConstructCreateTableSQL(&mock.ctxt, current, nil, false, nil)
		require.NoError(t, err)
		if parsed != nil {
			parsed.Free()
		}
		replay, err := mysql.ParseOneWithSQLMode(t.Context(), ddl, 1, "NO_BACKSLASH_ESCAPES")
		require.NoError(t, err, ddl)
		built, err := BuildPlan(&mock.ctxt, replay, false)
		replay.Free()
		require.NoError(t, err)
		current = built.GetDdl().GetCreateTable().TableDef
		require.True(t, proto.Equal(wantGenerated.Expr, FindColumn(current.Cols, "g").GeneratedCol.Expr), ddl)
		require.Equal(t, wantSet, FindColumn(current.Cols, "s").Typ.Enumvalues, ddl)
	}
	legacyAfter, err := ConstructCreateTableSQLForSchemaHash(source)
	require.NoError(t, err)
	require.Equal(t, legacy, legacyAfter)
	require.True(t, proto.Equal(before, source))
}

func TestShowCreateExternalFieldsReplayWithoutBackslashEscapes(t *testing.T) {
	mock := NewMockOptimizer(false)
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		ScanType: tree.INFILE,
		Filepath: "/data/example.csv",
		Format:   tree.CSV,
		Option:   []string{"filepath", "/data/example.csv", "format", tree.CSV},
		Tail: &tree.TailParameter{
			Fields: &tree.Fields{
				Terminated: &tree.Terminated{Value: `\t`},
				EnclosedBy: &tree.EnclosedBy{Value: '\\'},
				EscapedBy:  &tree.EscapedBy{Value: '\\'},
			},
			Lines: &tree.Lines{StartingBy: `\start`, TerminatedBy: &tree.Terminated{Value: `\n`}},
		},
	}}
	encoded, err := json.Marshal(param)
	require.NoError(t, err)
	source := &plan.TableDef{
		Name: "show_mode_external", TableType: catalog.SystemExternalRel, Createsql: string(encoded),
		Cols: []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}, Default: &plan.Default{NullAbility: true}}},
	}
	mock.ctxt.SetSqlModeOverride("NO_BACKSLASH_ESCAPES")
	ddl, stmt, err := ConstructCreateTableSQL(&mock.ctxt, source, nil, false, nil)
	require.NoError(t, err)
	if stmt != nil {
		stmt.Free()
	}
	replay, err := mysql.ParseOneWithSQLMode(t.Context(), ddl, 1, "NO_BACKSLASH_ESCAPES")
	require.NoError(t, err, ddl)
	defer replay.Free()
	create, ok := replay.(*tree.CreateTable)
	require.True(t, ok)
	require.NotNil(t, create.Param)
	require.NotNil(t, create.Param.Tail)
	require.Equal(t, param.Tail.Fields.Terminated.Value, create.Param.Tail.Fields.Terminated.Value, ddl)
	require.Equal(t, param.Tail.Fields.EnclosedBy.Value, create.Param.Tail.Fields.EnclosedBy.Value, ddl)
	require.Equal(t, param.Tail.Fields.EscapedBy.Value, create.Param.Tail.Fields.EscapedBy.Value, ddl)
	require.Equal(t, param.Tail.Lines.StartingBy, create.Param.Tail.Lines.StartingBy, ddl)
	require.Equal(t, param.Tail.Lines.TerminatedBy.Value, create.Param.Tail.Lines.TerminatedBy.Value, ddl)
	built, err := BuildPlan(&mock.ctxt, replay, false)
	require.NoError(t, err)
	var createSQL string
	for _, def := range built.GetDdl().GetCreateTable().TableDef.Defs {
		if properties := def.GetProperties(); properties != nil {
			for _, property := range properties.Properties {
				if property.Key == catalog.SystemRelAttr_CreateSQL {
					createSQL = property.Value
				}
			}
		}
	}
	require.NotEmpty(t, createSQL)
	var rebuilt tree.ExternParam
	require.NoError(t, json.Unmarshal([]byte(createSQL), &rebuilt))
	require.Equal(t, param.Tail.Fields.Terminated.Value, rebuilt.Tail.Fields.Terminated.Value)
	require.Equal(t, param.Tail.Fields.EnclosedBy.Value, rebuilt.Tail.Fields.EnclosedBy.Value)
	require.Equal(t, param.Tail.Fields.EscapedBy.Value, rebuilt.Tail.Fields.EscapedBy.Value)
	require.Equal(t, param.Tail.Lines.StartingBy, rebuilt.Tail.Lines.StartingBy)
	require.Equal(t, param.Tail.Lines.TerminatedBy.Value, rebuilt.Tail.Lines.TerminatedBy.Value)

	// The empty separator must remain a string literal even with ANSI_QUOTES.
	param.Tail.Fields.Terminated.Value = ""
	encoded, err = json.Marshal(param)
	require.NoError(t, err)
	source.Createsql = string(encoded)
	mock.ctxt.SetSqlModeOverride("ANSI_QUOTES,NO_BACKSLASH_ESCAPES")
	ddl, stmt, err = ConstructCreateTableSQL(&mock.ctxt, source, nil, false, nil)
	require.NoError(t, err, ddl)
	if stmt != nil {
		stmt.Free()
	}
	require.Contains(t, ddl, "FIELDS TERMINATED BY ''")
}

func TestShowCreateExternalCommentReplaysInSessionMode(t *testing.T) {
	for _, marker := range []string{`#\t`, "plain"} {
		for _, mode := range []string{"", "NO_BACKSLASH_ESCAPES", "ANSI_QUOTES,NO_BACKSLASH_ESCAPES"} {
			t.Run(marker+"/"+mode, func(t *testing.T) {
				mock := NewMockOptimizer(false)
				param := &tree.ExternParam{ExParamConst: tree.ExParamConst{
					ScanType: tree.INFILE,
					Filepath: "/data/example.csv",
					Format:   tree.CSV,
					Option:   []string{"filepath", "/data/example.csv", "format", tree.CSV, "comment", marker},
				}}
				encoded, err := json.Marshal(param)
				require.NoError(t, err)
				source := &plan.TableDef{
					Name: "show_external_comment", TableType: catalog.SystemExternalRel, Createsql: string(encoded),
					Cols: []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}, Default: &plan.Default{NullAbility: true}}},
				}
				mock.ctxt.tables[source.Name] = source
				mock.ctxt.objects[source.Name] = &ObjectRef{SchemaName: mock.ctxt.DefaultDatabase(), ObjName: source.Name}
				mock.ctxt.SetSqlModeOverride(mode)

				show, err := mysql.ParseOneWithSQLMode(t.Context(), "show create table show_external_comment", 1, mode)
				require.NoError(t, err)
				shown, err := BuildPlan(&mock.ctxt, show, false)
				show.Free()
				require.NoError(t, err)
				query := shown.GetQuery()
				root := query.GetNodes()[query.GetSteps()[0]]
				ddl := root.ProjectList[1].GetLit().GetSval()
				replay, err := mysql.ParseOneWithSQLMode(t.Context(), ddl, 1, mode)
				require.NoError(t, err, ddl)
				create, ok := replay.(*tree.CreateTable)
				require.True(t, ok)
				require.Equal(t, marker, GetCSVComment(create.Param), ddl)
				replay.Free()
			})
		}
	}
}

func TestShowCreateKafkaSeparatorReplaysInSessionMode(t *testing.T) {
	for _, separator := range []string{`\`, ","} {
		for _, mode := range []string{"", "NO_BACKSLASH_ESCAPES", "ANSI_QUOTES,NO_BACKSLASH_ESCAPES"} {
			t.Run(separator+"/"+mode, func(t *testing.T) {
				mock := NewMockOptimizer(false)
				cfg := sqlkafka.Config{
					Brokers: "host.invalid:9092", Topic: "events", Group: "reader",
					Format: sqlkafka.FormatCSV, Separator: separator,
				}
				source := &plan.TableDef{
					Name: "kafka_mode", TableType: catalog.SystemExternalRel,
					FeatureFlag: features.KafkaExternal, Createsql: sqlkafka.BuildCreateSQLEnvelope(cfg),
					Cols: []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}, Default: &plan.Default{NullAbility: true}}},
				}
				mock.ctxt.tables[source.Name] = source
				mock.ctxt.objects[source.Name] = &ObjectRef{SchemaName: mock.ctxt.DefaultDatabase(), ObjName: source.Name}
				mock.ctxt.SetSqlModeOverride(mode)

				show, err := mysql.ParseOneWithSQLMode(t.Context(), "show create table kafka_mode", 1, mode)
				require.NoError(t, err)
				shown, err := BuildPlan(&mock.ctxt, show, false)
				show.Free()
				require.NoError(t, err)
				query := shown.GetQuery()
				root := query.GetNodes()[query.GetSteps()[0]]
				ddl := root.ProjectList[1].GetLit().GetSval()
				replay, err := mysql.ParseOneWithSQLMode(t.Context(), ddl, 1, mode)
				require.NoError(t, err, ddl)
				rebuilt, err := BuildPlan(&mock.ctxt, replay, false)
				replay.Free()
				require.NoError(t, err, ddl)
				var createSQL string
				for _, def := range rebuilt.GetDdl().GetCreateTable().TableDef.Defs {
					if properties := def.GetProperties(); properties != nil {
						for _, property := range properties.Properties {
							if property.Key == catalog.SystemRelAttr_CreateSQL {
								createSQL = property.Value
							}
						}
					}
				}
				require.NotEmpty(t, createSQL)
				recreated, found, err := sqlkafka.ParseCreateSQLEnvelope(t.Context(), createSQL)
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, cfg.Separator, recreated.Separator)
			})
		}
	}
}

func TestDefaultWithoutBackslashEscapesRejectsInvalidScalar(t *testing.T) {
	for _, origin := range []string{"'unterminated", "'a\\b', 2", "'a\\b' as alias", "'a\\b' from t", "'a\\b' where true", "'a\\b' limit 1"} {
		t.Run(origin, func(t *testing.T) {
			_, err := formatDefaultWithoutBackslashEscapes(origin)
			require.Error(t, err)
		})
	}
}

func TestShowCreateResultReplaysDefaultsInSessionMode(t *testing.T) {
	mock := NewMockOptimizer(false)
	source, err := buildTestCreateTableStmt(mock, `create table show_mode_src (
		id int, literal_tab varchar(32) default 'a\\t',
		double_slash varchar(32) default 'a\\\\b',
		trailing_slash varchar(32) default 'tail\\',
		quotes varchar(32) default 'it''s "quoted"',
		plain varchar(32) default 'plain')`)
	require.NoError(t, err)
	mock.ctxt.tables[source.Name] = source
	mock.ctxt.objects[source.Name] = &ObjectRef{SchemaName: mock.ctxt.DefaultDatabase(), ObjName: source.Name}

	for _, mode := range []string{"", "NO_BACKSLASH_ESCAPES", "ANSI_QUOTES", "ANSI_QUOTES,NO_BACKSLASH_ESCAPES"} {
		t.Run(mode, func(t *testing.T) {
			mock.ctxt.SetSqlModeOverride(mode)
			show, err := mysql.ParseOneWithSQLMode(t.Context(), "show create table show_mode_src", 1, mode)
			require.NoError(t, err)
			defer show.Free()
			shown, err := BuildPlan(&mock.ctxt, show, false)
			require.NoError(t, err)
			query := shown.GetQuery()
			require.Equal(t, []string{"Table", "Create Table"}, query.GetHeadings())
			root := query.GetNodes()[query.GetSteps()[0]]
			require.Len(t, root.GetProjectList(), 2)
			require.Equal(t, source.Name, root.ProjectList[0].GetLit().GetSval())
			ddl := root.ProjectList[1].GetLit().GetSval()
			replay, err := mysql.ParseOneWithSQLMode(t.Context(), ddl, 1, mode)
			require.NoError(t, err, "public DDL: %q", ddl)
			defer replay.Free()
			rebuilt, err := BuildPlan(&mock.ctxt, replay, false)
			require.NoError(t, err)
			for _, name := range []string{"literal_tab", "double_slash", "trailing_slash", "quotes", "plain"} {
				want := FindColumn(source.Cols, name)
				got := FindColumn(rebuilt.GetDdl().GetCreateTable().TableDef.Cols, name)
				require.NotNil(t, got, name)
				require.True(t, proto.Equal(want.Default.Expr, got.Default.Expr),
					"%s: source default %q, replay default %q", name, want.Default.OriginString, got.Default.OriginString)
			}
		})
	}
}

func TestShowCreateResultReplaysCommentsInSessionMode(t *testing.T) {
	mock := NewMockOptimizer(false)
	source, err := buildTestCreateTableStmt(mock, `create table show_comment_src (
		v text comment 'c\\"q\\" and it''s\nline'
	) comment='t\\path and it''s'`)
	require.NoError(t, err)
	require.Equal(t, "c\\\"q\\\" and it''s\nline", FindColumn(source.Cols, "v").Comment)
	comment := func(def *plan.TableDef) string {
		for _, tableDef := range def.Defs {
			if properties := tableDef.GetProperties(); properties != nil {
				for _, property := range properties.Properties {
					if property.Key == catalog.SystemRelAttr_Comment {
						return property.Value
					}
				}
			}
		}
		return ""
	}
	require.Equal(t, "t\\path and it''s", comment(source))
	mock.ctxt.tables[source.Name] = source
	mock.ctxt.objects[source.Name] = &ObjectRef{SchemaName: mock.ctxt.DefaultDatabase(), ObjName: source.Name}
	before := proto.Clone(source)
	legacy, err := ConstructCreateTableSQLForSchemaHash(source)
	require.NoError(t, err)
	require.Contains(t, legacy, `COMMENT 'c\"q\" and it''s`)

	for _, mode := range []string{"", "NO_BACKSLASH_ESCAPES", "ANSI_QUOTES", "ANSI_QUOTES,NO_BACKSLASH_ESCAPES"} {
		t.Run(mode, func(t *testing.T) {
			mock.ctxt.SetSqlModeOverride(mode)
			show, err := mysql.ParseOneWithSQLMode(t.Context(), "show create table show_comment_src", 1, mode)
			require.NoError(t, err)
			defer show.Free()
			shown, err := BuildPlan(&mock.ctxt, show, false)
			require.NoError(t, err)
			query := shown.GetQuery()
			root := query.GetNodes()[query.GetSteps()[0]]
			ddl := root.ProjectList[1].GetLit().GetSval()
			for generation := 0; generation < 2; generation++ {
				replay, err := mysql.ParseOneWithSQLMode(t.Context(), ddl, 1, mode)
				require.NoError(t, err, "public DDL: %q", ddl)
				rebuilt, err := BuildPlan(&mock.ctxt, replay, false)
				replay.Free()
				require.NoError(t, err)
				next := rebuilt.GetDdl().GetCreateTable().TableDef
				require.Equal(t, FindColumn(source.Cols, "v").Comment, FindColumn(next.Cols, "v").Comment)
				require.Equal(t, comment(source), comment(next))
				var parsed tree.Statement
				ddl, parsed, err = ConstructCreateTableSQL(&mock.ctxt, next, nil, false, nil)
				require.NoError(t, err)
				if parsed != nil {
					parsed.Free()
				}
			}
			require.True(t, proto.Equal(before, source), "rendering must not mutate shared metadata")
		})
	}
}

func TestCreateLikePreservesEnumAndDefaultInSessionMode(t *testing.T) {
	mock := NewMockOptimizer(false)
	source, err := buildTestCreateTableStmt(mock, `create table like_mode_src (
		id int, e enum('line\nbreak','cr\rend','nul\0byte','q''uote','plain'),
		v varchar(32) default 'a\\t')`)
	require.NoError(t, err)
	mock.ctxt.tables[source.Name] = source
	mock.ctxt.objects[source.Name] = &ObjectRef{SchemaName: mock.ctxt.DefaultDatabase(), ObjName: source.Name}
	before := proto.Clone(source)
	const wantDomain = "line\nbreak,cr\rend,nul\x00byte,q'uote,plain"
	require.Equal(t, wantDomain, FindColumn(source.Cols, "e").Typ.Enumvalues)

	for _, mode := range []string{"", "NO_BACKSLASH_ESCAPES", "ANSI_QUOTES,NO_BACKSLASH_ESCAPES"} {
		t.Run(mode, func(t *testing.T) {
			mock.ctxt.SetSqlModeOverride(mode)
			stmt, err := mysql.ParseOneWithSQLMode(t.Context(), "create table like_mode_dst like like_mode_src", 1, mode)
			require.NoError(t, err)
			defer stmt.Free()
			built, err := BuildPlan(&mock.ctxt, stmt, false)
			require.NoError(t, err)
			clone := built.GetDdl().GetCreateTable().TableDef
			require.Equal(t, wantDomain, FindColumn(clone.Cols, "e").Typ.Enumvalues)
			require.True(t, proto.Equal(FindColumn(source.Cols, "v").Default.Expr, FindColumn(clone.Cols, "v").Default.Expr))
			require.True(t, proto.Equal(before, source), "CREATE LIKE must not mutate the source")
		})
	}
}

func TestAlterCopyDefaultUsesInternalParserMode(t *testing.T) {
	for _, mode := range []string{"", "NO_BACKSLASH_ESCAPES", "ANSI_QUOTES", "ANSI_QUOTES,NO_BACKSLASH_ESCAPES"} {
		t.Run(mode, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			source, err := buildTestCreateTableStmt(mock, `create table mode_copy(id int primary key, v varchar(32) default 'old')`)
			require.NoError(t, err)
			mock.ctxt.tables["mode_copy"] = source
			mock.ctxt.objects["mode_copy"] = &ObjectRef{SchemaName: mock.ctxt.DefaultDatabase(), ObjName: source.Name}
			before := proto.Clone(source)
			mock.ctxt.SetSqlModeOverride(mode)
			literal := `'a\\b'`
			if strings.Contains(mode, "NO_BACKSLASH_ESCAPES") {
				literal = `'a\b'`
			}
			for _, clause := range []string{"add added varchar(32) default " + literal, "modify v varchar(32) default " + literal} {
				stmt, err := mysql.ParseOneWithSQLMode(t.Context(), "alter table mode_copy "+clause+", algorithm=copy", 1, mode)
				require.NoError(t, err)
				built, err := BuildPlan(&mock.ctxt, stmt, false)
				stmt.Free()
				require.NoError(t, err)
				alter := built.GetDdl().GetAlterTable()
				// Match the ordinary-mode parser used by the internal SQL executor.
				replay, err := mysql.ParseOne(t.Context(), alter.CreateTmpTableSql, 1)
				require.NoError(t, err, alter.CreateTmpTableSql)
				rebuilt, err := BuildPlan(&mock.ctxt, replay, false)
				replay.Free()
				require.NoError(t, err)
				for _, col := range alter.CopyTableDef.Cols {
					after := FindColumn(rebuilt.GetDdl().GetCreateTable().TableDef.Cols, col.Name)
					require.NotNil(t, after, col.Name)
					require.Equal(t, col.Default.OriginString, after.Default.OriginString, col.Name)
					require.True(t, proto.Equal(col.Default.Expr, after.Default.Expr), col.Name)
				}
				require.True(t, proto.Equal(before, source), "COPY planning must not rewrite source defaults")
			}
		})
	}
}
