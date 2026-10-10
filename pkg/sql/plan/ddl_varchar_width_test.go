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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestDDLVarcharWidthCreate(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sql       string
		wantWidth int32
		wantOID   types.T
		wantError bool
	}{
		{name: "reported table", sql: "create table t(c bool, v varchar(65535))", wantError: true},
		{name: "boundary", sql: "create table t(v varchar(16383))", wantWidth: 16383, wantOID: types.T_varchar},
		{name: "above boundary", sql: "create table t(v varchar(16384))", wantError: true},
		{name: "table charset", sql: "create table t(v varchar(16384)) charset utf8mb4", wantError: true},
		{name: "table binary collation", sql: "create table t(v varchar(16384)) collate utf8mb4_bin", wantError: true},
		{name: "column charset", sql: "create table t(v varchar(16384) character set utf8mb4)", wantError: true},
		{name: "column collation", sql: "create table t(v varchar(16384) collate utf8mb4_bin)", wantError: true},
		{name: "compatibility alias", sql: "create table t(v varchar(16384) character set utf8mb3)", wantError: true},
		{name: "binary override", sql: "create table t(v varchar(65535) character set binary)", wantWidth: 65535, wantOID: types.T_varbinary},
		{name: "binary table", sql: "create table t(v varchar(65535)) charset binary", wantWidth: 65535, wantOID: types.T_varbinary},
		{name: "text overrides binary table", sql: "create table t(v varchar(16384) character set utf8mb4) charset binary", wantError: true},
		{name: "omitted width", sql: "create table t(v varchar)", wantWidth: 16383, wantOID: types.T_varchar},
		{name: "omitted binary width", sql: "create table t(v varchar) charset binary", wantWidth: 65535, wantOID: types.T_varbinary},
		{name: "zero width", sql: "create table t(v varchar(0))", wantWidth: 0, wantOID: types.T_varchar},
		{name: "explicit CTAS column", sql: "create table t(v varchar(16384)) as select 'a' as v", wantError: true},
		{name: "text unaffected", sql: "create table t(v text)", wantOID: types.T_text},
		{name: "varbinary unaffected", sql: "create table t(v varbinary(65535))", wantWidth: 65535, wantOID: types.T_varbinary},
	} {
		t.Run(tc.name, func(t *testing.T) {
			built, err := buildSingleStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, tc.sql)
			if tc.wantError {
				require.Error(t, err)
				var typed *moerr.Error
				require.ErrorAs(t, err, &typed)
				require.Equal(t, moerr.ER_TOO_BIG_FIELDLENGTH, typed.MySQLCode())
				require.Equal(t, "42000", typed.SqlState())
				require.Contains(t, typed.Error(), "'v' (max = 16383)")
				return
			}
			require.NoError(t, err)
			col := FindColumn(built.GetDdl().GetCreateTable().TableDef.Cols, "v")
			require.NotNil(t, col)
			require.Equal(t, int32(tc.wantOID), col.Typ.Id)
			require.Equal(t, tc.wantWidth, col.Typ.Width)
		})
	}
}

func TestDDLVarcharWidthAlter(t *testing.T) {
	for _, sql := range []string{
		"alter table t1 add column v varchar(16384)",
		"alter table t1 modify column b varchar(16384)",
		"alter table t1 change column b v varchar(16384)",
		"alter table t1 modify column b varchar(16384), add column v int",
	} {
		t.Run(sql, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			// Mock schemas predate explicit charset metadata. New columns must
			// still use the UTF-8 byte budget when inheriting that identity.
			require.Zero(t, mock.ctxt.tables["t1"].DefaultCharset)
			_, err := buildSingleStmt(mock, t, sql)
			require.Error(t, err)
			var typed *moerr.Error
			require.ErrorAs(t, err, &typed)
			require.Equal(t, moerr.ER_TOO_BIG_FIELDLENGTH, typed.MySQLCode())
		})
	}
}

func TestDDLVarcharWidthPreservesExistingSchema(t *testing.T) {
	for _, tc := range []struct {
		name    string
		charset uint32
	}{
		{name: "explicit charset", charset: uint32(types.CharsetUTF8)},
		{name: "legacy charset", charset: uint32(types.CharsetLegacy)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			charset := tc.charset
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			built, err := buildSingleStmt(mock, t, "create table source_t(v varchar(10))")
			require.NoError(t, err)
			source := built.GetDdl().GetCreateTable().TableDef
			source.DefaultCharset = charset
			col := FindColumn(source.Cols, "v")
			col.Typ.Width = types.MaxVarcharLen
			col.Typ.Charset = charset
			mock.ctxt.tables["source_t"] = source

			clone, err := buildSingleStmt(mock, t, "create table clone_t like source_t")
			require.NoError(t, err)
			require.Equal(t, int32(types.MaxVarcharLen), FindColumn(clone.GetDdl().GetCreateTable().TableDef.Cols, "v").Typ.Width)
		})
	}
}

func TestDDLVarcharWidthPreservesInternalDDL(t *testing.T) {
	for _, definition := range []string{"varchar(65535)", "varchar"} {
		t.Run(definition, func(t *testing.T) {
			stmt, err := mysql.ParseOne(t.Context(), "create table t(v "+definition+")", 1)
			require.NoError(t, err)
			defer stmt.Free()
			col := stmt.(*tree.CreateTable).Defs[0].(*tree.ColumnTableDef)
			ctx := context.WithValue(t.Context(), defines.InternalExecutorKey{}, true)
			typ, err := getColumnTypeFromAst(ctx, col, uint32(types.CharsetUTF8), nil)
			require.NoError(t, err)
			require.Equal(t, int32(types.MaxVarcharLen), typ.Width)
		})
	}
}

func TestDDLVarcharWidthReplayAdmission(t *testing.T) {
	original := &planpb.TableDef{Name: "source", Cols: []*ColDef{{
		Name: "v", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 65535, Charset: uint32(types.CharsetUTF8)},
	}}}
	target := DeepCopyTableDef(original, true)
	target.Name = "copy"
	replay := ddlReplayForTable(WithPersistedDDLReplay(t.Context(), original, target), "copy")
	for _, tc := range []struct {
		name      string
		column    string
		wantError bool
	}{
		{name: "unchanged", column: "v varchar(65535)"},
		{name: "changed width", column: "v varchar(65534)", wantError: true},
		{name: "new column", column: "extra varchar(65535)", wantError: true},
		{name: "changed charset", column: "v varchar(65535) collate utf8mb4_bin", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := mysql.ParseOne(t.Context(), "create table copy("+tc.column+")", 1)
			require.NoError(t, err)
			defer stmt.Free()
			col := stmt.(*tree.CreateTable).Defs[0].(*tree.ColumnTableDef)
			_, err = getColumnTypeFromAst(t.Context(), col, uint32(types.CharsetUTF8), replay)
			if tc.wantError {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTooBigFieldLength), err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDDLVarcharWidthDoesNotRestrictExpressions(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	built, err := buildSingleStmt(mock, t, "create table t as select cast('a' as varchar(65535)) as v")
	require.NoError(t, err)
	col := FindColumn(built.GetDdl().GetCreateTable().TableDef.Cols, "v")
	require.Equal(t, int32(types.T_varchar), col.Typ.Id)
	require.Equal(t, int32(types.MaxVarcharLen), col.Typ.Width)
	require.Equal(t, uint32(types.CharsetUTF8), col.Typ.Charset)
}
