// Copyright 2021 - 2026 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestCreateTableLikePreservesTextCollationMetadata(t *testing.T) {
	mock := NewMockOptimizer(false)
	stmt, err := mysql.ParseOne(t.Context(), `create table source_t(
		bin_text varchar(10),
		general_text varchar(10) collate utf8mb4_general_ci,
		packed varchar(10)
	) collate utf8mb4_bin`, 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(mock.CurrentContext(), stmt, false)
	require.NoError(t, err)
	source := built.GetDdl().GetCreateTable().GetTableDef()
	FindColumn(source.Cols, "packed").Typ.Charset = uint32(types.CharsetBinary)
	mock.ctxt.tables["source_t"] = source

	likeStmt, err := mysql.ParseOne(t.Context(), "create table clone_t like source_t", 1)
	require.NoError(t, err)
	defer likeStmt.Free()
	clonePlan, err := BuildPlan(mock.CurrentContext(), likeStmt, false)
	require.NoError(t, err)
	clone := clonePlan.GetDdl().GetCreateTable().GetTableDef()
	require.Equal(t, uint32(types.CharsetUTF8MB4Bin), clone.DefaultCharset)
	require.Equal(t, uint32(types.CharsetUTF8MB4Bin), FindColumn(clone.Cols, "bin_text").Typ.Charset)
	require.Equal(t, uint32(types.CharsetUTF8), FindColumn(clone.Cols, "general_text").Typ.Charset)
	require.Equal(t, uint32(types.CharsetBinary), FindColumn(clone.Cols, "packed").Typ.Charset)
}

func TestCreateTableLikePreservesLegacyBytewiseTextBehavior(t *testing.T) {
	mock := NewMockOptimizer(false)
	stmt, err := mysql.ParseOne(t.Context(), `create table source_t(
		legacy_text varchar(10),
		general_text varchar(10) collate utf8mb4_general_ci
	)`, 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(mock.CurrentContext(), stmt, false)
	require.NoError(t, err)
	source := built.GetDdl().GetCreateTable().GetTableDef()
	source.DefaultCharset = uint32(types.CharsetLegacy)
	FindColumn(source.Cols, "legacy_text").Typ.Charset = uint32(types.CharsetLegacy)
	mock.ctxt.tables["source_t"] = source

	likeStmt, err := mysql.ParseOne(t.Context(), "create table clone_t like source_t", 1)
	require.NoError(t, err)
	defer likeStmt.Free()
	clonePlan, err := BuildPlan(mock.CurrentContext(), likeStmt, false)
	require.NoError(t, err)
	clone := clonePlan.GetDdl().GetCreateTable().GetTableDef()
	// The legacy identity is reconstructed as the explicit bytewise collation;
	// exact zero is not required, but its pre-upgrade ordering is.
	require.Equal(t, uint32(types.CharsetUTF8MB4Bin), clone.DefaultCharset)
	require.Equal(t, uint32(types.CharsetUTF8MB4Bin), FindColumn(clone.Cols, "legacy_text").Typ.Charset)
	require.Equal(t, uint32(types.CharsetUTF8), FindColumn(clone.Cols, "general_text").Typ.Charset)
}

func TestCreateTableLikePreservesCheckAcrossSQLModes(t *testing.T) {
	testCases := []struct {
		name       string
		sourceMode string
		likeMode   string
		createSQL  string
	}{
		{
			name:       "no backslash escapes to default",
			sourceMode: "NO_BACKSLASH_ESCAPES",
			likeMode:   "",
			createSQL:  `create table source_t(s varchar(10), check (s = 'a\nb'))`,
		},
		{
			name:       "default to no backslash escapes",
			sourceMode: "",
			likeMode:   "NO_BACKSLASH_ESCAPES",
			createSQL:  `create table source_t(s varchar(10), check (s = 'a\\nb'))`,
		},
		{
			name:       "no backslash escapes trailing backslash",
			sourceMode: "NO_BACKSLASH_ESCAPES",
			likeMode:   "",
			createSQL:  `create table source_t(s varchar(20), check (s = 'a\'))`,
		},
	}

	build := func(t *testing.T, mock *MockOptimizer, sql, mode string) *plan.TableDef {
		t.Helper()
		mock.ctxt.SetSqlModeOverride(mode)
		stmts, err := mysql.ParseWithSQLMode(t.Context(), sql, 1, mode)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		defer stmts[0].Free()
		built, err := BuildPlan(mock.CurrentContext(), stmts[0], false)
		require.NoError(t, err)
		return built.GetDdl().GetCreateTable().GetTableDef()
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			source := build(t, mock, tc.createSQL, tc.sourceMode)
			require.Len(t, source.Checks, 1)
			mock.ctxt.tables["source_t"] = source

			clone := build(t, mock, "create table clone_t like source_t", tc.likeMode)
			require.Len(t, clone.Checks, 1)
			require.Equal(t, source.Checks[0].Check, clone.Checks[0].Check)
			require.Equal(t, source.Checks[0].OriginSql, clone.Checks[0].OriginSql)
			require.NotSame(t, source.Checks[0].Check, clone.Checks[0].Check)
		})
	}
}

func TestCreateTableLikeRequiresCheckProtocol(t *testing.T) {
	mock := NewMockOptimizer(false)
	source := func() *plan.TableDef {
		stmt, err := mysql.ParseOne(t.Context(), "create table source_t(a int check (a > 0))", 1)
		require.NoError(t, err)
		defer stmt.Free()
		built, err := BuildPlan(mock.CurrentContext(), stmt, false)
		require.NoError(t, err)
		return built.GetDdl().GetCreateTable().GetTableDef()
	}()
	proc := mock.ctxt.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion6)
	defer func() {
		if ok {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	}()

	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "structured", true: "legacy"}[legacy], func(t *testing.T) {
			sourceDef := DeepCopyTableDef(source, true)
			if legacy {
				sourceDef.Checks = nil
				sourceDef.Createsql = "create table source_t(a int, constraint legacy_positive check (a > 0))"
			}
			mock.ctxt.tables["source_t"] = sourceDef

			stmt, err := mysql.ParseOne(t.Context(), "create table clone_t like source_t", 1)
			require.NoError(t, err)
			defer stmt.Free()
			_, err = BuildPlan(mock.CurrentContext(), stmt, false)
			require.ErrorContains(t, err, "protocol version 7")
		})
	}
}

func TestCreateTableLikePreservesLegacyCheck(t *testing.T) {
	for _, tc := range []struct {
		name          string
		baseSQL       string
		legacySQL     string
		checkName     string
		checkOrigin   string
		persistedText string
		wantAmbiguous bool
	}{
		{
			name:          "top level",
			baseSQL:       "create table source_t(a int)",
			legacySQL:     "create table source_t(a int, constraint legacy_positive check (a > 0))",
			checkName:     "legacy_positive",
			checkOrigin:   "`a` > 0",
			persistedText: "constraint legacy_positive check (`a` > 0) enforced",
		},
		{
			name:          "column level",
			baseSQL:       "create table source_t(a int)",
			legacySQL:     "create table source_t(a int check (a > 0))",
			checkName:     "__mo_chk_1",
			checkOrigin:   "`a` > 0",
			persistedText: "constraint __mo_chk_1 check (`a` > 0) enforced",
		},
		{
			name:          "comment before top level",
			baseSQL:       "create table source_t(a int)",
			legacySQL:     "create table source_t(a int, /* retained */ constraint c check (a > 0))",
			checkName:     "c",
			checkOrigin:   "`a` > 0",
			persistedText: "constraint c check (`a` > 0) enforced",
		},
		{
			name:          "no backslash escapes source",
			baseSQL:       "create table source_t(s varchar(10))",
			legacySQL:     `create table source_t(s varchar(10), check (s = 'a\nb'))`,
			wantAmbiguous: true,
		},
		{
			name:          "default source with escaped backslash",
			baseSQL:       "create table source_t(s varchar(10))",
			legacySQL:     `create table source_t(s varchar(10), check (s = 'a\\nb'))`,
			wantAmbiguous: true,
		},
		{
			name:          "pipes as concat source",
			baseSQL:       "create table source_t(s varchar(10))",
			legacySQL:     "create table source_t(s varchar(10), check (s = 'a' || 'b'))",
			wantAmbiguous: true,
		},
		{
			name:          "ansi quotes source",
			baseSQL:       "create table source_t(a int)",
			legacySQL:     `create table source_t(a int, check ("a" > 0))`,
			wantAmbiguous: true,
		},
		{
			name:          "real as float uses catalog column type",
			baseSQL:       "create table source_t(r float)",
			legacySQL:     "create table source_t(r real, check (r > 0))",
			checkName:     "__mo_chk_1",
			checkOrigin:   "`r` > 0",
			persistedText: "constraint __mo_chk_1 check (`r` > 0) enforced",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			stmt, err := mysql.ParseOne(t.Context(), tc.baseSQL, 1)
			require.NoError(t, err)
			defer stmt.Free()
			built, err := BuildPlan(mock.CurrentContext(), stmt, false)
			require.NoError(t, err)

			source := built.GetDdl().GetCreateTable().GetTableDef()
			require.Empty(t, source.Checks)
			source.Createsql = tc.legacySQL
			mock.ctxt.tables["source_t"] = source

			likeStmt, err := mysql.ParseOne(t.Context(), "create table clone_t like source_t", 1)
			require.NoError(t, err)
			defer likeStmt.Free()
			clonePlan, err := BuildPlan(mock.CurrentContext(), likeStmt, false)
			if tc.wantAmbiguous {
				require.ErrorContains(t, err, "ambiguous SQL mode")
				return
			}
			require.NoError(t, err)
			clone := clonePlan.GetDdl().GetCreateTable().GetTableDef()
			require.Len(t, clone.Checks, 1)
			require.Equal(t, tc.checkName, clone.Checks[0].Name)
			require.Equal(t, tc.checkOrigin, clone.Checks[0].OriginSql)
			var createSQL string
			for _, def := range clone.Defs {
				for _, property := range def.GetProperties().GetProperties() {
					if property.Key == catalog.SystemRelAttr_CreateSQL {
						createSQL = property.Value
					}
				}
			}
			require.Contains(t, createSQL, tc.persistedText)
		})
	}
}
