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
	"context"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestEnumCollationExactNames(t *testing.T) {
	for _, name := range []string{"utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_unicode_ci"} {
		require.True(t, exactEnumCollationName(name), name)
	}
	for _, name := range []string{"utf8mb4_0900_ai_ci", "utf8mb4_0900_bin", "utf8_bin", "utf8mb3_bin", "utf8_general_ci", "utf8mb3_general_ci", "utf8_unicode_ci", "utf8mb3_unicode_ci", "unknown"} {
		require.False(t, exactEnumCollationName(name), name)
	}
}

func TestStoredEnumValueBinding(t *testing.T) {
	target := planpb.Type{Id: int32(types.T_enum), Enumvalues: "A,a,", EnumCollation: types.EnumCollationUTF8Bin}
	binder := NewDefaultBinder(t.Context(), nil, nil, target, nil)
	for _, ordinal := range []uint16{0, 1, 2, 3} {
		expr, err := binder.BindExpr(&tree.StoredEnumValue{Ordinal: ordinal, EnumValues: target.Enumvalues, EnumCollation: target.EnumCollation}, 0, true)
		require.NoError(t, err)
		require.Equal(t, target, expr.Typ)
		require.Equal(t, uint32(ordinal), expr.GetLit().GetEnumVal())
	}
	for _, value := range []*tree.StoredEnumValue{
		{Ordinal: 4, EnumValues: target.Enumvalues, EnumCollation: target.EnumCollation},
		{Ordinal: 1, EnumValues: "a,A,", EnumCollation: target.EnumCollation},
		{Ordinal: 1, EnumValues: target.Enumvalues, EnumCollation: types.EnumCollationGeneralCI},
	} {
		_, err := binder.BindExpr(value, 0, true)
		require.Error(t, err)
	}
	expr, err := binder.BindExpr(&tree.StoredEnumValue{Ordinal: 0, EnumValues: target.Enumvalues, EnumCollation: target.EnumCollation}, 0, true)
	require.NoError(t, err)
	require.Zero(t, expr.GetLit().GetEnumVal())
	_, err = NewDefaultBinder(t.Context(), nil, nil, planpb.Type{Id: int32(types.T_uint16)}, nil).BindExpr(
		&tree.StoredEnumValue{Ordinal: 0, EnumValues: target.Enumvalues}, 0, true)
	require.Error(t, err)
}

func TestEnumUnsupportedCharsetDefault(t *testing.T) {
	for _, tc := range []struct {
		charset, collation, want string
	}{
		{"utf8", "", "utf8mb3_general_ci"},
		{"utf8mb3", "", "utf8mb3_general_ci"},
		{"utf32", "", "utf32_general_ci"},
		{"utf8", "utf8mb4_bin", "utf8mb3_bin"},
		{"utf8mb3", "utf8mb4_general_ci", "utf8mb3_general_ci"},
		{"utf32", "utf8mb4_bin", "utf32_bin"},
		{"utf8mb4", "utf8mb4_bin", ""},
		{"utf8mb4", "utf8_bin", "utf8_bin"},
		{"", "utf8_unicode_ci", "utf8_unicode_ci"},
	} {
		t.Run(tc.charset+"/"+tc.collation, func(t *testing.T) {
			var options []tree.TableOption
			if tc.charset != "" {
				options = append(options, &tree.TableOptionCharset{Charset: tc.charset})
			}
			if tc.collation != "" {
				options = append(options, &tree.TableOptionCollate{Collate: tc.collation})
			}
			require.Equal(t, tc.want, enumUnsupportedTableDefault(nil, options, false))
		})
	}
}

func TestEnumCollationParticipatesInTypeAndAlterEquality(t *testing.T) {
	bin := planpb.Type{Id: int32(types.T_enum), Enumvalues: "A,a", EnumCollation: types.EnumCollationUTF8Bin}
	ci := bin
	ci.EnumCollation = types.EnumCollationGeneralCI
	require.False(t, bin.SameCollation(ci))
	require.False(t, isSameColumnType(bin, ci))
	require.False(t, alterCopyKeyColumnValueUnchanged(&ColDef{Typ: bin}, &ColDef{Typ: ci}))
	require.True(t, alterCopyColumnTypeMayChangeValues(bin, ci))
	require.True(t, alterCopyForeignKeyColumnMayChangeValues(bin, ci))
}

func TestEnumUnsupportedTableDefaultAlter(t *testing.T) {
	databaseDefaultsProtocol(t, defines.MORPCVersion135)
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	source, err := buildTestCreateTableStmt(mock, "create table enum_target(i int) collate=utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	require.Equal(t, uint32(3), source.DefaultCharset)
	require.Equal(t, "utf8mb4_0900_ai_ci", source.EnumUnsupportedDefault)
	mock.ctxt.tables["enum_target"] = source
	mock.ctxt.objects["enum_target"] = &planpb.ObjectRef{SchemaName: "tpch", ObjName: "enum_target"}
	before, err := source.Marshal()
	require.NoError(t, err)
	_, err = buildSingleStmt(mock, t, "alter table enum_target add e enum('a','A')")
	require.ErrorContains(t, err, "cannot inherit unsupported table collation")
	after, err := source.Marshal()
	require.NoError(t, err)
	require.Equal(t, before, after)
	p, err := buildSingleStmt(mock, t, "alter table enum_target default collate=utf8mb4_general_ci, add e enum('a','b')")
	require.NoError(t, err)
	target := p.GetDdl().GetAlterTable().CopyTableDef
	require.NotNil(t, target)
	require.Empty(t, target.EnumUnsupportedDefault)
	require.Equal(t, types.EnumCollationGeneralCI, FindColumn(target.Cols, "e").Typ.EnumCollation)
}

func TestEnumUnsupportedTableDefaultRequiresAuthoringProtocol(t *testing.T) {
	databaseDefaultsProtocol(t, defines.MORPCVersion134)
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	source, err := buildTestCreateTableStmt(mock, "create table enum_target(i int) collate=utf8mb4_bin")
	require.NoError(t, err)
	mock.ctxt.tables["enum_target"] = source
	mock.ctxt.objects["enum_target"] = &planpb.ObjectRef{SchemaName: "tpch", ObjName: "enum_target"}
	before, err := source.Marshal()
	require.NoError(t, err)

	_, err = buildSingleStmt(mock, t, "alter table enum_target default collate=utf8mb4_general_ci")
	require.NoError(t, err, "existing table defaults remain usable at the old protocol")
	_, err = buildSingleStmt(mock, t, "alter table enum_target default collate=utf8mb4_0900_ai_ci")
	require.ErrorContains(t, err, "version 135")
	after, err := source.Marshal()
	require.NoError(t, err)
	require.Equal(t, before, after, "rejected authoring must not mutate the source schema")
}

func TestLegacyEnumShowReplayAndLike(t *testing.T) {
	databaseDefaultsProtocol(t, defines.MORPCVersion135)
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	source, err := buildTestCreateTableStmt(mock, "create table enum_source(e enum('A','a') collate __mo_enum_legacy_main_v1, v varchar(8) collate utf8mb4_general_ci) collate=__mo_table_legacy_main_v1")
	require.NoError(t, err)
	require.Zero(t, source.DefaultCharset)
	require.Zero(t, FindColumn(source.Cols, "e").Typ.EnumCollation)
	show, replay, err := ConstructCreateTableSQL(mock.CurrentContext(), source, nil, false, nil)
	require.NoError(t, err)
	require.Contains(t, show, legacyEnumCollationName)
	require.Contains(t, show, legacyTableCollationName)
	require.Contains(t, show, "COLLATE utf8mb4_general_ci")
	defer replay.Free()
	built, err := BuildPlan(mock.CurrentContext(), replay, false, nil)
	require.NoError(t, err)
	recreated := built.GetDdl().GetCreateTable().GetTableDef()
	require.Zero(t, recreated.DefaultCharset)
	require.Zero(t, FindColumn(recreated.Cols, "e").Typ.EnumCollation)
	require.Equal(t, uint32(types.CharsetUTF8), FindColumn(recreated.Cols, "v").Typ.Charset)

	mock.ctxt.tables["enum_source"] = source
	mock.ctxt.objects["enum_source"] = &planpb.ObjectRef{SchemaName: "tpch", ObjName: "enum_source"}
	copyPlan, err := buildSingleStmt(mock, t, "create table enum_copy like enum_source")
	require.NoError(t, err)
	clone := copyPlan.GetDdl().GetCreateTable().GetTableDef()
	require.Zero(t, clone.DefaultCharset)
	require.Zero(t, FindColumn(clone.Cols, "e").Typ.EnumCollation)
	mock.ctxt.tables["enum_copy"] = clone
	mock.ctxt.objects["enum_copy"] = &planpb.ObjectRef{SchemaName: "tpch", ObjName: "enum_copy"}
	alter, err := buildSingleStmt(mock, t, "alter table enum_copy add second_e enum('A','a')")
	require.NoError(t, err)
	require.Zero(t, FindColumn(alter.GetDdl().GetAlterTable().CopyTableDef.Cols, "second_e").Typ.EnumCollation)
}

func TestEnumRejectsEmptyCollation(t *testing.T) {
	databaseDefaultsProtocol(t, defines.MORPCVersion135)
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	for _, sql := range []string{
		"create table t(e enum('a') collate '')",
		"create table t(e enum('a') collate '') collate utf8mb4_bin",
		"create table t(e enum('a') collate '') collate utf8mb4_0900_ai_ci",
		"create table t(e enum('a') character set utf8mb4 collate __mo_enum_legacy_main_v1)",
		"create table t(v varchar(3) collate __mo_enum_legacy_main_v1)",
		"create table t(v int) collate=__mo_table_legacy_main_v1",
	} {
		_, err := buildSingleStmt(mock, t, sql)
		require.Error(t, err, sql)
	}
}

func TestEnumCollationProtocolFloor(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	defer rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCLatestVersion)
	current := &planpb.TableDef{Cols: []*planpb.ColDef{{Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "a,b", EnumCollation: types.EnumCollationUTF8Bin}}}}
	legacy := &planpb.TableDef{Cols: []*planpb.ColDef{{Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "a,b"}}}}
	marker := &planpb.TableDef{EnumUnsupportedDefault: "utf8mb4_0900_ai_ci", DefaultCharset: 3}
	fn := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.CAST_VALUE_TO_INDEX, 1)}, Args: make([]*planpb.Expr, 4),
	}}}
	for _, owner := range []any{current, marker, fn} {
		version, err := RequiredPersistedExpressionProtocolVersion(owner)
		require.NoError(t, err)
		require.Equal(t, defines.MORPCVersion135, version)
	}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion134)
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion134)
	require.NoError(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, legacy))
	require.ErrorContains(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, current), "version 135")
	require.ErrorContains(t, RequirePersistedExpressionProtocolForAuthoring(proc.Ctx, proc, marker), "version 135")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion135)
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion135)
	require.NoError(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, current))
	require.NoError(t, RequirePersistedExpressionProtocolForAuthoring(proc.Ctx, proc, marker))
}

func TestBuiltinEnumSchemasAtClosedAuthoringFloor(t *testing.T) {
	databaseDefaultsProtocol(t, defines.MORPCVersion135)
	setPlanTestGlobalVariable(t, "", moruntime.PersistedExpressionProtocolAuthoringFloor, int64(0))
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	for _, tc := range []struct {
		name, sql, column string
	}{
		{"snapshots", "create table mo_catalog.mo_snapshots (level enum('cluster','account','database','table') collate __mo_enum_legacy_main_v1)", "level"},
		{"role edges", sysview.MysqlRoleEdgesDDL, "with_admin_option"},
		{"collations", sysview.InformationSchemaCollationsDDL, "pad_attribute"},
		{"events", sysview.InformationSchemaEventsDDL, "interval_field"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			definition, err := buildTestCreateTableStmt(mock, tc.sql)
			require.NoError(t, err)
			column := FindColumn(definition.Cols, tc.column)
			require.NotNil(t, column)
			require.Zero(t, column.Typ.EnumCollation)
		})
	}
	_, err := buildTestCreateTableStmt(mock, "create table user_enum(e enum('a','A') collate utf8mb4_bin)")
	require.ErrorContains(t, err, "version 135", "the system DDL exception must not open the authoring gate")
}

func TestSystemBootstrapServerAliasDoesNotOpenEnumAuthoring(t *testing.T) {
	databaseDefaultsProtocol(t, defines.MORPCVersion135)
	setPlanTestGlobalVariable(t, "", moruntime.PersistedExpressionProtocolAuthoringFloor, int64(0))
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	mock.ctxt.SetContext(defines.AttachInternalExecutor(mock.ctxt.GetContext()))
	fallback := mock.ctxt
	fallback.ResolveVariableFunc = nil
	mock.ctxt.ResolveVariableFunc = func(name string, system, global bool) (any, error) {
		if name == "collation_server" {
			return "utf8mb4_0900_ai_ci", nil
		}
		return fallback.ResolveVariable(name, system, global)
	}
	table, err := buildTestCreateTableStmt(mock, "create table mo_catalog.bootstrap_test(i int)")
	require.NoError(t, err)
	require.Empty(t, table.EnumUnsupportedDefault)
	_, err = buildTestCreateTableStmt(mock, "create table mo_catalog.explicit_marker(i int) collate=utf8mb4_0900_ai_ci")
	require.ErrorContains(t, err, "version 135")
	_, err = buildTestCreateTableStmt(mock, "create table mo_catalog.explicit_enum(e enum('a') collate utf8mb4_bin)")
	require.ErrorContains(t, err, "version 135")
	_, err = buildTestCreateTableStmt(mock, "create table tpch.internal_user_table(i int)")
	require.ErrorContains(t, err, "version 135", "internal user-database DDL retains the server marker")
	mock.ctxt.SetContext(context.Background())
	_, err = buildTestCreateTableStmt(mock, "create table mo_catalog.user_system_table(i int)")
	require.ErrorContains(t, err, "version 135", "ordinary statements do not inherit the internal system-DDL exception")
}
