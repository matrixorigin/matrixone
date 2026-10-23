// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

func TestNativePADStandaloneLegacyTableReconstruction(t *testing.T) {
	for _, ddl := range []string{
		"create table n (s varchar(8), g varchar(8) collate utf8mb4_general_ci) collate utf8mb4_bin",
		"create table n (s varchar(8) collate utf8mb4_bin) collate utf8mb4_general_ci",
		"create table n (s varchar(8) collate utf8mb4_unicode_ci unique)",
	} {
		ctx, build := nativePADDDLContext(t)
		ctx.SetContext(WithCollationBindingRevision(t.Context(), 0))
		created, err := build(ddl)
		require.NoError(t, err)
		source := created.GetDdl().GetCreateTable().TableDef
		before := proto.Clone(source)
		// There is no sidecar context when this SQL is handed to another server.
		text, _, err := ConstructCreateTableSQL(nil, source, nil, false, nil)
		require.NoError(t, err)
		for _, generation := range []uint32{0, 1} {
			ctx.SetContext(WithCollationBindingRevision(t.Context(), generation))
			rebuilt, err := build(text)
			require.NoError(t, err, text)
			target := rebuilt.GetDdl().GetCreateTable().TableDef
			require.Equal(t, source.DefaultCharset, target.DefaultCharset)
			require.Equal(t, source.CollationVersion, target.CollationVersion, text)
			for i, col := range source.Cols {
				if !col.Hidden {
					require.Equal(t, col.Typ, target.Cols[i].Typ, text)
				}
			}
		}
		require.Equal(t, before, source)
	}
}

func TestNativePADStandaloneViewRejectsLostProvenance(t *testing.T) {
	for _, source := range []struct {
		revision uint32
		query    string
		bad      bool
	}{
		{0, "'A' collate utf8mb4_general_ci = 'a'", true},
		{1, "'A' collate utf8mb4_general_ci = 'a'", true},
		{0, "'a' collate utf8mb4_bin", true},
		{1, "'words: collate utf8mb4_general_ci'", false},
		{1, "'A' collate utf8mb4_0900_ai_ci", false},
		{1, "'A' collate utf8mb4_unicode_ci", false},
	} {
		ctx, build := nativePADDDLContext(t)
		ctx.SetContext(WithCollationBindingRevision(t.Context(), source.revision))
		created, err := build("create view v as select " + source.query + " as s")
		require.NoError(t, err)
		table := created.GetDdl().GetCreateView().TableDef
		table.TableType = catalog.SystemViewRel
		ctx.tables["v"] = table
		ctx.objects["v"] = &ObjectRef{ObjName: "v", SchemaName: "tpch"}
		ctx.SetContext(WithCollationBindingRevision(t.Context(), 1))
		for _, sql := range []string{"show create view v", "show create table v"} {
			_, err = build(sql)
			if source.bad {
				require.ErrorContains(t, err, "standalone SQL", source.query)
			} else {
				require.NoError(t, err, source.query)
			}
		}
		if source.bad {
			// Context-free get_ddl/publication callers must not lose a bool
			// view's marker just because no output column has a native Type.
			_, _, err = ConstructCreateTableSQL(nil, table, nil, false, nil)
			require.ErrorContains(t, err, "standalone SQL", source.query)
		}
	}
}

func TestNativePADStandaloneLegacyComputedExpression(t *testing.T) {
	for _, source := range []struct {
		ddl string
		bad bool
	}{
		{"create table n (b bool default ('A' collate utf8mb4_general_ci = 'a'))", true},
		{"create table n (b bool generated always as ('A' collate utf8mb4_general_ci = 'a') stored)", true},
		{"create table n (a int, check ('A' collate utf8mb4_general_ci = 'a'))", true},
		{"create table n (s varchar(80) default ('words: collate utf8mb4_general_ci'))", false},
		{"create table n (s varchar(8) default ('A' collate utf8mb4_0900_ai_ci))", false},
	} {
		ctx, build := nativePADDDLContext(t)
		ctx.SetContext(WithCollationBindingRevision(t.Context(), 0))
		created, err := build(source.ddl)
		require.NoError(t, err)
		ctx.SetContext(WithCollationBindingRevision(t.Context(), 1))
		_, _, err = ConstructCreateTableSQL(nil, created.GetDdl().GetCreateTable().TableDef, nil, false, nil)
		if source.bad {
			require.ErrorContains(t, err, "standalone SQL", source.ddl)
		} else {
			require.NoError(t, err, source.ddl)
		}
	}
}

func TestNativePADStandaloneExternalMySQLNames(t *testing.T) {
	ctx, build := nativePADDDLContext(t)
	ctx.SetContext(WithCollationBindingRevision(t.Context(), 0))
	created, err := build("create table n (s varchar(8) collate utf8mb4_bin, g varchar(8) collate utf8mb4_general_ci) collate utf8mb4_bin")
	require.NoError(t, err)
	source := created.GetDdl().GetCreateTable().TableDef
	before := proto.Clone(source)
	text, err := ConstructLegacyCreateTableSQL(source, true)
	require.NoError(t, err)
	require.Contains(t, text, "`s` varchar(8) COLLATE utf8mb4_bin")
	require.Contains(t, text, "`g` varchar(8) COLLATE utf8mb4_general_ci")
	require.Contains(t, text, ") COLLATE=utf8mb4_bin")
	require.NotContains(t, text, "utf32_bin")
	require.NotContains(t, text, "0900")
	require.Equal(t, before, source)
	ctx.SetContext(WithCollationBindingRevision(t.Context(), 1))
	created, err = build("create table n (s varchar(8) collate utf8mb4_general_ci)")
	require.NoError(t, err)
	_, err = ConstructLegacyCreateTableSQL(created.GetDdl().GetCreateTable().TableDef, true)
	require.Error(t, err, "target dialect is not permission to export native metadata")
}

func TestNativePADStandaloneRestoreInspectionIsNotAuthority(t *testing.T) {
	ctx, build := nativePADDDLContext(t)
	created, err := build("create table n (s varchar(8) collate utf8mb4_general_ci)")
	require.NoError(t, err)
	ctx.tables["n"] = created.GetDdl().GetCreateTable().TableDef
	ctx.objects["n"] = &ObjectRef{ObjName: "n", SchemaName: "tpch"}
	created, err = build("create view v as select 'A' collate utf8mb4_general_ci = 'a' as b")
	require.NoError(t, err)
	view := created.GetDdl().GetCreateView().TableDef
	view.TableType = catalog.SystemViewRel
	ctx.tables["v"] = view
	ctx.objects["v"] = &ObjectRef{ObjName: "v", SchemaName: "tpch"}
	parent := WithCollationBindingRevision(defines.AttachAccountId(t.Context(), 42), 1)
	inspection := WithRestoreSchemaInspection(parent)
	ctx.SetContext(inspection)
	rt := moruntime.ServiceRuntime(ctx.GetProcess().GetService())
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion120)
	for _, sql := range []string{"show create table n", "show create table v", "show create view v"} {
		_, err = build(sql)
		require.NoError(t, err, "inspection is a read, not a writer")
		require.Same(t, inspection, ctx.GetContext())
	}
	_, err = build("create table other (s varchar(8) collate utf8mb4_general_ci)")
	require.Error(t, err, "inspection must not grant native authoring")
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion130)
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, defines.MORPCVersion120)
	for _, sql := range []string{"show create table n", "show create view v"} {
		_, err = build(sql)
		require.Error(t, err, "inspection must not bypass complete-source read admission")
		require.Same(t, inspection, ctx.GetContext())
	}
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, defines.MORPCVersion130)
	ctx.SetContext(parent)
	_, err = build("show create view v")
	require.ErrorContains(t, err, "standalone SQL", "private inspection must not leak to the caller")
	ctx.SetContext(WithRestoreSchemaInspection(WithCollationBindingRevision(parent, 0)))
	created, err = build("create table other (s varchar(8) collate utf8mb4_general_ci)")
	require.NoError(t, err)
	require.Zero(t, FindColumn(created.GetDdl().GetCreateTable().TableDef.Cols, "s").Typ.CollationVersion,
		"inspection does not install source Types or change caller generation")
	account, err := defines.GetAccountId(ctx.GetContext())
	require.NoError(t, err)
	require.Equal(t, uint32(42), account)
}

func TestNativePADStandaloneEscapedCollationName(t *testing.T) {
	for _, name := range []string{`utf8mb4_g\eneral_ci`, `utf8mb4_b\in`} {
		t.Run(name, func(t *testing.T) {
			ctx, build := nativePADDDLContext(t)
			ddl := "create table n(s varchar(8) collate '" + name + "')"
			for _, revision := range []uint32{0, 1} {
				ctx.SetContext(WithCollationBindingRevision(t.Context(), revision))
				p, err := build(ddl)
				require.NoError(t, err, "the parser must actually accept and decode this spelling")
				require.Equal(t, revision, p.GetDdl().GetCreateTable().TableDef.Cols[0].Typ.CollationVersion)
			}
			require.ErrorContains(t, RequireStandaloneLegacySQL(t.Context(), ddl), "standalone SQL")
		})
	}
	t.Run("legacy_view", func(t *testing.T) {
		ctx, build := nativePADDDLContext(t)
		ctx.SetContext(WithCollationBindingRevision(t.Context(), 0))
		p, err := build(`create view v as select 'A' collate 'utf8mb4_g\eneral_ci' = 'a' as b`)
		require.NoError(t, err)
		require.ErrorContains(t, RequireLegacySQLMetadata(t.Context(), p.GetDdl().GetCreateView().TableDef), "standalone SQL")
	})
	for _, sql := range []string{
		`create table n(s varchar(8) collate 'utf32_b\in')`,
		`create table n(s varchar(80) default 'collate utf8mb4_g\eneral_ci')`,
		`create table n(s int) /* collate utf8mb4_g\eneral_ci */`,
	} {
		require.NoError(t, RequireStandaloneLegacySQL(t.Context(), sql), sql)
	}
}

func TestNativePADStandaloneMalformedViewProvenance(t *testing.T) {
	for _, view := range []string{
		`{`,
		`{"Stmt":"select 1","collation_binding_revision":2,"required_protocol_version":124}`,
		`{"Stmt":"select 1","required_protocol_version":-1}`,
		`{"Stmt":"select 1","required_protocol_version":130}`,
	} {
		require.Error(t, RequireLegacySQLMetadata(t.Context(), &TableDef{ViewSql: &ViewDef{View: view}}), view)
	}
}

func TestNativePADStandaloneRawSQLFallback(t *testing.T) {
	for _, sql := range []string{
		"create table n(s varchar(8) collate utf8mb4_bin)",
		"create table n(i int) collate utf8mb4_general_ci",
		"alter table n convert to character set utf8mb4 collate utf8mb3_bin",
		"create table n(b bool default ((select 'A' collate utf8_general_ci) = 'a'))",
	} {
		require.ErrorContains(t, RequireStandaloneLegacySQL(t.Context(), sql), "standalone SQL", sql)
	}
	for _, sql := range []string{
		"create table n(s varchar(8)) collate utf32_bin",
		"create table n(s varchar(8) collate utf8mb4_0900_ai_ci)",
		"create table n(s varchar(8) collate utf8mb4_unicode_ci)",
		"create table n(s varchar(80) default 'collate utf8mb4_bin')",
		"create table n(s int) /* collate utf8mb4_general_ci */",
	} {
		require.NoError(t, RequireStandaloneLegacySQL(t.Context(), sql), sql)
	}
	require.Error(t, RequireStandaloneLegacySQL(t.Context(), "create table n(s int, collate utf8mb4_bin"))
}
