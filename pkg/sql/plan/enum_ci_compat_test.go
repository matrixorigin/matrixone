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
	"testing"

	"github.com/gogo/protobuf/proto"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestEnumOrdinaryTableCharsetCompatibility(t *testing.T) {
	mock := NewMockOptimizer(false)
	rt := moruntime.ServiceRuntime(mock.ctxt.GetProcess().GetService())
	previous, had := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion12)
	defer func() {
		if had {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
		}
	}()
	ordinary, err := buildTestCreateTableStmt(mock, "create table compat(i int) character set utf8 collate utf8mb4_bin")
	require.NoError(t, err)
	_, err = buildTestCreateTableStmt(mock, "create table empty_compat(i int) character set '' collate utf8mb4_bin")
	require.NoError(t, err, "ordinary tables preserve the historically ignored table charset")
	require.Equal(t, types.EnumCollationUTF8Bin, ordinary.DefaultCharset)
	show, _, err := ConstructCreateTableSQL(&mock.ctxt, ordinary, nil, false, nil)
	require.NoError(t, err)
	require.Contains(t, show, "COLLATE=utf8mb4_bin")
	mock.ctxt.tables["compat"] = ordinary
	stmt, err := mysql.ParseOne(t.Context(), "create table compat_copy like compat", 1)
	require.NoError(t, err)
	built, err := BuildPlan(mock.CurrentContext(), stmt, false)
	require.NoError(t, err)
	require.Equal(t, types.EnumCollationUTF8Bin, built.GetDdl().GetCreateTable().GetTableDef().DefaultCharset)
	stmt.Free()
	for _, sql := range []string{
		"create table bad(e enum('a')) character set utf8 collate utf8mb4_bin",
		"create table bad(i int,e enum('a') collate utf8mb4_bin) character set latin1 collate utf8mb4_bin",
	} {
		stmt, err = mysql.ParseOne(t.Context(), sql, 1)
		require.NoError(t, err)
		_, err = BuildPlan(mock.CurrentContext(), stmt, false)
		require.ErrorContains(t, err, "not valid for CHARACTER SET", sql)
		stmt.Free()
	}
	for _, sql := range []string{
		"create table bad(e enum('a') collate utf8mb4_bin) character set ''",
		"create table bad(e enum('a') collate utf8mb4_bin) character set '' collate utf8mb4_bin",
		"create table bad(e enum('a')) character set utf8mb4 character set ''",
	} {
		stmt, err = mysql.ParseOne(t.Context(), sql, 1)
		require.NoError(t, err)
		_, err = BuildPlan(mock.CurrentContext(), stmt, false)
		require.ErrorContains(t, err, "empty table CHARACTER SET for ENUM", sql)
		stmt.Free()
	}
	_, err = buildTestCreateTableStmt(mock, "create table last_nonempty(e enum('a')) character set '' character set utf8mb4")
	require.NoError(t, err)
}

func TestEnumAlterCharsetGuardPreservesLegacyNoops(t *testing.T) {
	mock := NewMockOptimizer(false)
	base, err := buildTestCreateTableStmt(mock, "create table compat(i int)")
	require.NoError(t, err)
	mock.ctxt.tables["compat"] = base
	mock.ctxt.objects["compat"] = &ObjectRef{SchemaName: "tpch", ObjName: "compat", Obj: 42001}
	for _, tc := range []struct {
		sql     string
		wantErr string
	}{
		{"alter table compat disable keys", ""},
		{"alter table compat enable keys", ""},
		{"alter table compat force", ""},
		{"alter table compat character set utf8mb4", "ALTER TABLE charset/collation defaults"},
		{"alter table compat character set utf8mb4 collate utf8mb4_bin", "ALTER TABLE charset/collation defaults"},
		{"alter table compat convert to character set utf8mb4", "ALTER TABLE charset/collation defaults"},
		{"alter table compat add e enum('a'), character set utf8mb4", "ALTER TABLE charset/collation defaults"},
		{"alter table compat modify i varchar(8) character set utf8mb4", "unsupport column definition <nil>"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			before := proto.Clone(base).(*TableDef)
			stmt, err := mysql.ParseOne(t.Context(), tc.sql, 1)
			require.NoError(t, err)
			_, err = BuildPlan(mock.CurrentContext(), stmt, false)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantErr)
			}
			require.True(t, proto.Equal(before, base), "planning must not mutate the original table")
			stmt.Free()
		})
	}
	stmt, err := mysql.ParseOne(t.Context(), "alter table compat character set utf8mb4", 1)
	require.NoError(t, err)
	stmt.(*tree.AlterTable).Options[0].(*tree.TableOptionCharset).LegacyAlterKind = tree.LegacyAlterNoopKind(255)
	_, err = BuildPlan(mock.CurrentContext(), stmt, false)
	require.ErrorContains(t, err, "ALTER TABLE charset/collation defaults")
	stmt.Free()
}
