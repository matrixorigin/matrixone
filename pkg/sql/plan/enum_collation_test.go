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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestEnumColumnCollationInheritance(t *testing.T) {
	for _, tc := range []struct {
		name, marker, explicit, charset string
		fallback, want                  uint32
		wantErr                         string
	}{
		{name: "legacy", want: types.EnumCollationLegacy},
		{name: "table", fallback: types.EnumCollationUTF8Bin, want: types.EnumCollationUTF8Bin},
		{name: "unsupported inherited", marker: "utf8mb4_unicode_ci", wantErr: "cannot inherit unsupported table collation"},
		{name: "column overrides marker", marker: "utf8mb4_unicode_ci", explicit: "utf8mb4_bin", want: types.EnumCollationUTF8Bin},
		{name: "column overrides table", fallback: types.EnumCollationUTF8Bin, explicit: "utf8mb4_general_ci", want: types.EnumCollationGeneralCI},
		{name: "unsupported column", explicit: "utf8mb4_unicode_ci", wantErr: "ENUM collation"},
		{name: "utf8mb3 collation", explicit: "utf8mb3_bin", wantErr: "ENUM collation"},
		{name: "utf8mb3 charset", charset: "utf8mb3", wantErr: "ENUM CHARACTER SET"},
		{name: "charset mismatch", charset: "latin1", explicit: "utf8mb4_bin", wantErr: "not valid for CHARACTER SET"},
		{name: "matching charset", charset: "utf8mb4", explicit: "utf8mb4_bin", want: types.EnumCollationUTF8Bin},
		{name: "charset cannot override marker", charset: "utf8mb4", marker: "utf8mb4_unicode_ci", wantErr: "cannot inherit unsupported table collation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := planpb.Type{Id: int32(types.T_enum), Enumvalues: "a,b"}
			var attrs []tree.ColumnAttribute
			if tc.charset != "" {
				attrs = append(attrs, tree.NewAttributeCharset(tc.charset))
			}
			if tc.explicit != "" {
				attrs = append(attrs, tree.NewAttributeCollate(tc.explicit))
			}
			err := applyEnumColumnCollation(context.Background(), &typ, attrs, tc.fallback, tc.marker)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, typ.EnumCollation)
		})
	}

	plain := planpb.Type{Id: int32(types.T_varchar)}
	require.NoError(t, applyEnumColumnCollation(context.Background(), &plain, nil, 0, "utf8mb4_unicode_ci"))
}

func TestEnumCollationProtocolFloor(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	current := &planpb.TableDef{Cols: []*planpb.ColDef{{Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "a,b", EnumCollation: types.EnumCollationUTF8Bin}}}}
	legacy := &planpb.TableDef{Cols: []*planpb.ColDef{{Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "a,b"}}}}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion11)
	require.NoError(t, RequireEnumCollationProtocol(proc, legacy))
	require.ErrorContains(t, RequireEnumCollationProtocol(proc, current), "protocol version 12")
	require.ErrorContains(t, RequireEnumCollationProtocol(nil, current), "protocol version 12")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion12)
	require.NoError(t, RequireEnumCollationProtocol(proc, current))
}

func TestCreateLikePreservesLegacyEnumUnderUnsupportedDefault(t *testing.T) {
	mock := NewMockOptimizer(false)
	proc := mock.ctxt.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, hadOld := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion12)
	defer func() {
		if hadOld {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		}
	}()
	source, err := buildTestCreateTableStmt(mock, "create table source_t(e enum('A','a'))")
	require.NoError(t, err)
	source.EnumUnsupportedDefault = "utf8mb4_unicode_ci"
	mock.ctxt.tables["source_t"] = source
	_, _, err = ConstructCreateTableSQL(mock.CurrentContext(), source, nil, false, nil)
	require.ErrorContains(t, err, "cannot replay a legacy ENUM")
	stmt, err := mysql.ParseOne(t.Context(), "create table clone_t like source_t", 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(mock.CurrentContext(), stmt, false)
	require.NoError(t, err)
	clone := built.GetDdl().GetCreateTable().GetTableDef()
	require.Equal(t, "utf8mb4_unicode_ci", clone.EnumUnsupportedDefault)
	require.Equal(t, types.EnumCollationLegacy, FindColumn(clone.Cols, "e").Typ.EnumCollation)
	require.Equal(t, types.EnumCollationLegacy, FindColumn(source.Cols, "e").Typ.EnumCollation)
}

func TestEnumRejectsEmptyCollation(t *testing.T) {
	mock := NewMockOptimizer(false)
	for _, sql := range []string{
		"create table t(e enum('a') collate '')",
		"create table t(e enum('a') collate '') collate utf8mb4_bin",
		"create table t(e enum('a')) collate ''",
		"create table t(i int) collate ''",
	} {
		stmt, err := mysql.ParseOne(t.Context(), sql, 1)
		require.NoError(t, err, sql)
		_, err = BuildPlan(mock.CurrentContext(), stmt, false)
		stmt.Free()
		require.Error(t, err, sql)
	}
}
