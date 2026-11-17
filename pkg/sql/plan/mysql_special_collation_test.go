// Copyright 2026 Matrix Origin
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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestBuildCreateTablePreservesEnumSetCollation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		columnType  string
		tableOption string
		wantID      types.T
		wantCharset uint32
	}{
		{
			name:        "ENUM inherits table ci collation",
			columnType:  "enum('Red','GREEN')",
			tableOption: "collate utf8mb4_general_ci",
			wantID:      types.T_enum,
			wantCharset: uint32(types.CharsetUTF8),
		},
		{
			name:        "SET inherits table binary collation",
			columnType:  "set('a','b')",
			tableOption: "collate utf8mb4_bin",
			wantID:      types.T_uint64,
			wantCharset: uint32(types.CharsetUTF8MB4Bin),
		},
		{
			name:        "column collation overrides table collation",
			columnType:  "enum('Red','GREEN') collate utf8mb4_bin",
			tableOption: "collate utf8mb4_general_ci",
			wantID:      types.T_enum,
			wantCharset: uint32(types.CharsetUTF8MB4Bin),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := "create table t(v " + tc.columnType + ") " + tc.tableOption
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			built, err := BuildPlan(NewMockCompilerContext(false, newPlanTestProcess(t)), stmt, false, nil)
			require.NoError(t, err)
			column := built.GetDdl().GetCreateTable().GetTableDef().GetCols()[0]
			require.Equal(t, int32(tc.wantID), column.Typ.Id)
			require.NotEmpty(t, column.Typ.Enumvalues)
			require.Equal(t, tc.wantCharset, column.Typ.Charset)
		})
	}
}

func TestRecoverLegacyEnumSetCollation(t *testing.T) {
	newTable := func(createSQL string) *planpb.TableDef {
		return &planpb.TableDef{
			TblId:          71,
			LogicalId:      71,
			Name:           "legacy_types",
			TableType:      catalog.SystemOrdinaryRel,
			Createsql:      createSQL,
			DefaultCharset: uint32(types.CharsetUTF8),
			Cols: []*planpb.ColDef{
				{Name: "id", Seqnum: 0, Typ: planpb.Type{Id: int32(types.T_int32)}},
				{Name: "e", Seqnum: 1, Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN"}},
				{Name: "s", Seqnum: 2, Typ: planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d"}},
				{Name: "b", Seqnum: 3, Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN"}},
				{Name: "u", Seqnum: 4, Typ: planpb.Type{Id: int32(types.T_uint64)}},
			},
		}
	}

	t.Run("stored declarations recover ci and binary identities", func(t *testing.T) {
		table := newTable("CREATE TABLE legacy_types (id INT, e ENUM('Red','GREEN'), s SET('a','b','c','d'), b ENUM('Red','GREEN') COLLATE utf8mb4_bin, u BIGINT UNSIGNED) COLLATE utf8mb4_general_ci")
		originalEnum := table.Cols[1]
		RecoverLegacyEnumSetCollation(t.Context(), table)
		require.Equal(t, uint32(types.CharsetUTF8), table.Cols[1].Typ.Charset)
		require.Equal(t, uint32(types.CharsetUTF8), table.Cols[2].Typ.Charset)
		require.Equal(t, uint32(types.CharsetUTF8MB4Bin), table.Cols[3].Typ.Charset)
		require.Zero(t, table.Cols[4].Typ.Charset, "ordinary uint64 has no display-label collation")
		require.Zero(t, originalEnum.Typ.Charset, "recovery replaces column structs instead of mutating shared input")
	})

	t.Run("omitted identity inherits durable table default", func(t *testing.T) {
		table := newTable("CREATE TABLE legacy_types (id INT, e ENUM('Red','GREEN'), s SET('a','b','c','d'), b ENUM('Red','GREEN') COLLATE utf8mb4_general_ci, u BIGINT UNSIGNED)")
		table.DefaultCharset = uint32(types.CharsetUTF8MB4Bin)
		RecoverLegacyEnumSetCollation(t.Context(), table)
		require.Equal(t, uint32(types.CharsetUTF8MB4Bin), table.Cols[1].Typ.Charset)
		require.Equal(t, uint32(types.CharsetUTF8MB4Bin), table.Cols[2].Typ.Charset)
		require.Equal(t, uint32(types.CharsetUTF8), table.Cols[3].Typ.Charset)
	})

	t.Run("missing durable default remains legacy", func(t *testing.T) {
		table := newTable("CREATE TABLE legacy_types (id INT, e ENUM('Red','GREEN'), s SET('a','b','c','d'), b ENUM('Red','GREEN') COLLATE utf8mb4_general_ci, u BIGINT UNSIGNED)")
		table.DefaultCharset = uint32(types.CharsetLegacy)
		RecoverLegacyEnumSetCollation(t.Context(), table)
		require.Zero(t, table.Cols[1].Typ.Charset)
		require.Zero(t, table.Cols[2].Typ.Charset)
		require.Equal(t, uint32(types.CharsetUTF8), table.Cols[3].Typ.Charset)
	})

	t.Run("unicode identity retains its revision", func(t *testing.T) {
		table := newTable("CREATE TABLE legacy_types (id INT, e ENUM('Red','GREEN'), s SET('a','b','c','d'), b ENUM('Red','GREEN'), u BIGINT UNSIGNED) COLLATE utf8mb4_unicode_ci")
		RecoverLegacyEnumSetCollation(t.Context(), table)
		for _, index := range []int{1, 2, 3} {
			require.Equal(t, uint32(types.CharsetUTF8MB4UnicodeCI), table.Cols[index].Typ.Charset)
			require.Equal(t, uint32(types.CollationVersionV1), table.Cols[index].Typ.CollationVersion)
		}
	})
}

func TestUnsupportedLegacyEnumSetIdentityDoesNotBlockTinyText(t *testing.T) {
	for _, tc := range []struct {
		name      string
		createSQL string
	}{
		{
			name:      "unsupported column collation",
			createSQL: "CREATE TABLE legacy_types (t TINYTEXT, e ENUM('Red','GREEN') COLLATE utf8mb4_unknown)",
		},
		{
			name:      "unsupported table collation",
			createSQL: "CREATE TABLE legacy_types (t TINYTEXT, e ENUM('Red','GREEN')) COLLATE utf8mb4_unknown",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := &planpb.TableDef{
				TblId:          71,
				LogicalId:      71,
				Name:           "legacy_types",
				TableType:      catalog.SystemOrdinaryRel,
				Createsql:      tc.createSQL,
				DefaultCharset: uint32(types.CharsetUTF8),
				Cols: []*planpb.ColDef{
					{Name: "t", Seqnum: 0, Typ: planpb.Type{Id: int32(types.T_text)}},
					{Name: "e", Seqnum: 1, Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN"}},
				},
			}
			require.NoError(t, RecoverLegacyTinyText(t.Context(), table, nil))
			RecoverLegacyEnumSetCollation(t.Context(), table)
			require.Equal(t, int32(types.MaxTinyTextLen), table.Cols[0].Typ.Width)
			require.Zero(t, table.Cols[1].Typ.Charset)
		})
	}
}

func TestRecoverLegacyEnumSetCollationRejectsStaleEvidence(t *testing.T) {
	newTable := func() *planpb.TableDef {
		return &planpb.TableDef{
			TblId:          72,
			LogicalId:      72,
			Name:           "legacy_types",
			TableType:      catalog.SystemOrdinaryRel,
			Createsql:      "CREATE TABLE legacy_types (e ENUM('Red','GREEN')) COLLATE utf8mb4_general_ci",
			DefaultCharset: uint32(types.CharsetUTF8),
			Cols: []*planpb.ColDef{{
				Name: "e", Seqnum: 0,
				Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN"},
			}},
		}
	}

	t.Run("copied physical table", func(t *testing.T) {
		table := newTable()
		table.TblId = 73
		RecoverLegacyEnumSetCollation(t.Context(), table)
		require.Zero(t, table.Cols[0].Typ.Charset)
	})

	t.Run("changed labels", func(t *testing.T) {
		table := newTable()
		table.Cols[0].Typ.Enumvalues = "Red,BLUE"
		RecoverLegacyEnumSetCollation(t.Context(), table)
		require.Zero(t, table.Cols[0].Typ.Charset)
	})

	t.Run("changed physical type", func(t *testing.T) {
		table := newTable()
		table.Cols[0].Typ.Id = int32(types.T_uint64)
		RecoverLegacyEnumSetCollation(t.Context(), table)
		require.Zero(t, table.Cols[0].Typ.Charset)
	})

	t.Run("ambiguous sequence number", func(t *testing.T) {
		table := newTable()
		table.Cols = append(table.Cols, &planpb.ColDef{
			Name: "other", Seqnum: 0,
			Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN"},
		})
		RecoverLegacyEnumSetCollation(t.Context(), table)
		require.Zero(t, table.Cols[0].Typ.Charset)
		require.Zero(t, table.Cols[1].Typ.Charset)
	})

	t.Run("direct CREATE LIKE has no declaration evidence", func(t *testing.T) {
		table := newTable()
		table.Createsql = "CREATE TABLE legacy_types LIKE source_types"
		RecoverLegacyEnumSetCollation(t.Context(), table)
		require.Zero(t, table.Cols[0].Typ.Charset)
	})

	t.Run("current metadata is authoritative", func(t *testing.T) {
		table := newTable()
		table.Cols[0].Typ.Charset = uint32(types.CharsetUTF8MB4Bin)
		RecoverLegacyEnumSetCollation(t.Context(), table)
		require.Equal(t, uint32(types.CharsetUTF8MB4Bin), table.Cols[0].Typ.Charset)
	})
}

func TestUnicodeEnumSetInsertPlanAdmission(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	for _, index := range []int{1, 3} {
		typ := &mock.ctxt.tables["nation"].Cols[index].Typ
		typ.Charset = uint32(types.CharsetUTF8MB4UnicodeCI)
		typ.CollationVersion = uint32(types.CollationVersionV1)
	}
	mock.ctxt.tables["nation"].Cols[1].Typ.Id = int32(types.T_enum)
	mock.ctxt.tables["nation"].Cols[1].Typ.Enumvalues = "Red,GREEN"
	mock.ctxt.tables["nation"].Cols[3].Typ.Id = int32(types.T_uint64)
	mock.ctxt.tables["nation"].Cols[3].Typ.Enumvalues = "a,b,c,d"

	pl, err := runOneStmt(mock, t,
		"insert into nation(n_nationkey,n_name,n_regionkey,n_comment) values (1,'Red',1,'a,b')")
	require.NoError(t, err)
	require.NoError(t, planpb.RequireLegacyCollations(pl))
}

func TestLegacyTinyTextRecoveryIsIndependentOfAmbiguousEnumSetLabels(t *testing.T) {
	createSQL := legacyCacheFixtureSQL(`CREATE TABLE legacy_types (payload TINYTEXT, e ENUM('a\nb'), s SET('a\nb','c')) COLLATE utf8mb4_general_ci`)
	table := &planpb.TableDef{
		TblId:          74,
		LogicalId:      74,
		Name:           "legacy_types",
		TableType:      catalog.SystemOrdinaryRel,
		Createsql:      createSQL,
		DefaultCharset: uint32(types.CharsetUTF8),
		Cols: []*planpb.ColDef{
			{Name: "payload", Seqnum: 0, Typ: planpb.Type{Id: int32(types.T_text)}},
			{Name: "e", Seqnum: 1, Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "a\nb"}},
			{Name: "s", Seqnum: 2, Typ: planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a\nb,c"}},
		},
	}

	require.NoError(t, RecoverLegacyTinyText(t.Context(), table, nil))
	require.Equal(t, int32(types.MaxTinyTextLen), table.Cols[0].Typ.Width)
	RecoverLegacyEnumSetCollation(t.Context(), table)
	require.Zero(t, table.Cols[1].Typ.Charset, "ambiguous ENUM labels must remain fail-closed")
	require.Zero(t, table.Cols[2].Typ.Charset, "ambiguous SET labels must remain fail-closed")

	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		require.Equal(t, "sql_mode", name)
		return "STRICT_TRANS_TABLES", nil
	})
	for _, tc := range []struct {
		name      string
		length    int
		wantError bool
	}{
		{name: "255 bytes fit", length: types.MaxTinyTextLen},
		{name: "256 bytes rejected", length: types.MaxTinyTextLen + 1, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := makeTypeByPlan2Type(table.Cols[0].Typ)
			source := vector.NewVec(types.T_text.ToType())
			defer source.Free(proc.Mp())
			require.NoError(t, vector.AppendBytes(source, []byte(strings.Repeat("x", tc.length)), false, proc.Mp()))
			destination := vector.NewVec(target)
			defer destination.Free(proc.Mp())
			result := vector.NewFunctionResultWrapper(target, proc.Mp())
			defer result.Free()
			require.NoError(t, result.PreExtendAndReset(1))

			err := function.NewAssignCast([]*vector.Vector{source, destination}, result, proc, 1, nil)
			if tc.wantError {
				require.Error(t, err)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrCastWidthExceeded), "%v", err)
				return
			}
			require.NoError(t, err)
			value, isNull := vector.GenerateFunctionStrParameter(result.GetResultVector()).GetStrValue(0)
			require.False(t, isNull)
			require.Len(t, value, types.MaxTinyTextLen)
		})
	}
}

func TestEnumSetSubqueryComparisonUsesColumnCollation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		typ       planpb.Type
		query     string
		wantCIKey bool
	}{
		{
			name:      "ENUM scalar equality",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			query:     "select (select n_name from nation where n_nationkey=0) = 'RED'",
			wantCIKey: true,
		},
		{
			name:      "SET scalar equality",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			query:     "select (select n_name from nation where n_nationkey=0) = 'A,B'",
			wantCIKey: true,
		},
		{
			name:      "ENUM scalar NULL safe equality",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			query:     "select (select n_name from nation where n_nationkey=0) <=> null",
			wantCIKey: true,
		},
		{
			name:      "SET scalar NULL safe equality",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			query:     "select (select n_name from nation where n_nationkey=0) <=> null",
			wantCIKey: true,
		},
		{
			name:      "ENUM materialized IN",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			query:     "select 'RED' in (select n_name from nation)",
			wantCIKey: true,
		},
		{
			name:      "ENUM materialized NOT IN",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			query:     "select 'RED' not in (select n_name from nation)",
			wantCIKey: true,
		},
		{
			name:      "SET materialized IN",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			query:     "select 'A,B' in (select n_name from nation)",
			wantCIKey: true,
		},
		{
			name:      "SET materialized NOT IN",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			query:     "select 'A,B' not in (select n_name from nation)",
			wantCIKey: true,
		},
		{
			name:  "ENUM binary scalar control",
			typ:   planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8MB4Bin)},
			query: "select (select n_name from nation where n_nationkey=0) = 'RED'",
		},
		{
			name:  "SET binary scalar control",
			typ:   planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8MB4Bin)},
			query: "select (select n_name from nation where n_nationkey=0) = 'A,B'",
		},
		{
			name:  "ENUM binary IN control",
			typ:   planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8MB4Bin)},
			query: "select 'RED' in (select n_name from nation)",
		},
		{
			name:  "SET binary NOT IN control",
			typ:   planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8MB4Bin)},
			query: "select 'A,B' not in (select n_name from nation)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			mock.ctxt.tables["nation"].Cols[1].Typ = tc.typ
			pl, err := runOneStmt(mock, t, tc.query)
			require.NoError(t, err)
			key := findPlanFunctionExpr(pl, moMySQLSpecialGeneralCIKeyFun)
			if tc.wantCIKey {
				require.NotNil(t, key, "subquery output must retain ENUM/SET provenance: %s", pl.GetQuery().String())
			} else {
				require.Nil(t, key, "binary or non-special provenance must keep the original comparison")
			}
		})
	}
}

func TestEnumSetCollateSubqueryComparisonUsesColumnCollation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		typ       planpb.Type
		query     string
		wantCIKey bool
	}{
		{
			name:      "ENUM scalar explicit ci",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			query:     "select ((select n_name from nation where n_nationkey=0) collate utf8mb4_general_ci) = 'RED'",
			wantCIKey: true,
		},
		{
			name:      "SET scalar explicit ci",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			query:     "select ((select n_name from nation where n_nationkey=0) collate utf8mb4_general_ci) = 'A,B'",
			wantCIKey: true,
		},
		{
			name:      "ENUM derived explicit ci",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			query:     "select (d.e collate utf8mb4_general_ci) = 'RED' from (select n_name as e from nation) d",
			wantCIKey: true,
		},
		{
			name:      "SET derived explicit ci",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			query:     "select (d.s collate utf8mb4_general_ci) = 'A,B' from (select n_name as s from nation) d",
			wantCIKey: true,
		},
		{
			name:      "ENUM binary scalar explicit ci override",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8MB4Bin)},
			query:     "select ((select n_name from nation where n_nationkey=0) collate utf8mb4_general_ci) = 'RED'",
			wantCIKey: true,
		},
		{
			name:      "SET binary scalar explicit ci override",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8MB4Bin)},
			query:     "select ((select n_name from nation where n_nationkey=0) collate utf8mb4_general_ci) = 'A,B'",
			wantCIKey: true,
		},
		{
			name:      "ENUM binary derived explicit ci override",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8MB4Bin)},
			query:     "select (d.e collate utf8mb4_general_ci) = 'RED' from (select n_name as e from nation) d",
			wantCIKey: true,
		},
		{
			name:      "SET binary derived explicit ci override",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8MB4Bin)},
			query:     "select (d.s collate utf8mb4_general_ci) = 'A,B' from (select n_name as s from nation) d",
			wantCIKey: true,
		},
		{
			name:  "ENUM binary scalar explicit binary control",
			typ:   planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8MB4Bin)},
			query: "select ((select n_name from nation where n_nationkey=0) collate utf8mb4_bin) = 'RED'",
		},
		{
			name:  "SET binary derived explicit binary control",
			typ:   planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8MB4Bin)},
			query: "select (d.s collate utf8mb4_bin) = 'A,B' from (select n_name as s from nation) d",
		},
		{
			name:  "ENUM scalar arbitrary cast boundary",
			typ:   planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			query: "select (cast((select n_name from nation where n_nationkey=0) as char) collate utf8mb4_general_ci) = 'RED'",
		},
		{
			name:  "SET derived arbitrary cast boundary",
			typ:   planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			query: "select (cast(d.s as char) collate utf8mb4_general_ci) = 'A,B' from (select n_name as s from nation) d",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			mock.ctxt.tables["nation"].Cols[1].Typ = tc.typ
			pl, err := runOneStmt(mock, t, tc.query)
			require.NoError(t, err)
			key := findPlanFunctionExpr(pl, moMySQLSpecialGeneralCIKeyFun)
			if tc.wantCIKey {
				require.NotNil(t, key, "explicit COLLATE must retain scalar or derived ENUM/SET provenance: %s", pl.GetQuery().String())
			} else {
				require.Nil(t, key, "binary and arbitrary-CAST expressions must keep the original comparison")
			}
		})
	}
}

func TestEnumSetDerivedLeftSubqueryComparisonUsesColumnCollation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		typ       planpb.Type
		query     string
		wantCIKey bool
	}{
		{
			name:      "ENUM IN literal",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			query:     "select d.e in (select 'RED') from (select n_name as e from nation) d",
			wantCIKey: true,
		},
		{
			name:      "ENUM NOT IN literal",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			query:     "select d.e not in (select 'RED') from (select n_name as e from nation) d",
			wantCIKey: true,
		},
		{
			name:      "SET IN literal",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			query:     "select d.s in (select 'A,B') from (select n_name as s from nation) d",
			wantCIKey: true,
		},
		{
			name:      "SET NOT IN literal",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			query:     "select d.s not in (select 'A,B') from (select n_name as s from nation) d",
			wantCIKey: true,
		},
		{
			name:      "ENUM IN VARCHAR projection",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			query:     "select d.e in (select r.v from (select 'RED' as v) r) from (select n_name as e from nation) d",
			wantCIKey: true,
		},
		{
			name:      "SET NOT IN VARCHAR projection",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			query:     "select d.s not in (select r.v from (select 'A,B' as v) r) from (select n_name as s from nation) d",
			wantCIKey: true,
		},
		{
			name:      "ENUM IN NULL retains left identity",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			query:     "select d.e in (select null) from (select n_name as e from nation) d",
			wantCIKey: true,
		},
		{
			name:      "SET NOT IN NULL retains left identity",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			query:     "select d.s not in (select null) from (select n_name as s from nation) d",
			wantCIKey: true,
		},
		{
			name:  "ENUM binary IN literal control",
			typ:   planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8MB4Bin)},
			query: "select d.e in (select 'RED') from (select n_name as e from nation) d",
		},
		{
			name:  "SET binary NOT IN VARCHAR control",
			typ:   planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8MB4Bin)},
			query: "select d.s not in (select r.v from (select 'A,B' as v) r) from (select n_name as s from nation) d",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			mock.ctxt.tables["nation"].Cols[1].Typ = tc.typ
			pl, err := runOneStmt(mock, t, tc.query)
			require.NoError(t, err)
			key := findPlanFunctionExpr(pl, moMySQLSpecialGeneralCIKeyFun)
			if tc.wantCIKey {
				require.NotNil(t, key, "generated comparisons must resolve the outer left ENUM/SET provenance: %s", pl.GetQuery().String())
			} else {
				require.Nil(t, key, "binary derived operands must keep the original comparison")
			}
		})
	}
}

func TestEnumSetProjectedCollateComparisonUsesColumnCollation(t *testing.T) {
	ciEnum := planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)}
	ciSet := planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)}
	binaryEnum := ciEnum
	binaryEnum.Charset = uint32(types.CharsetUTF8MB4Bin)
	binarySet := ciSet
	binarySet.Charset = uint32(types.CharsetUTF8MB4Bin)
	for _, tc := range []struct {
		name      string
		typ       planpb.Type
		query     string
		wantCIKey bool
	}{
		{
			name:      "ENUM scalar projected ci",
			typ:       ciEnum,
			query:     "select (select n_name collate utf8mb4_general_ci from nation where n_nationkey=0) = 'RED'",
			wantCIKey: true,
		},
		{
			name:      "SET scalar projected ci",
			typ:       ciSet,
			query:     "select (select n_name collate utf8mb4_general_ci from nation where n_nationkey=0) = 'A,B'",
			wantCIKey: true,
		},
		{
			name:      "ENUM derived projected ci",
			typ:       ciEnum,
			query:     "select d.e = 'RED' from (select n_name collate utf8mb4_general_ci as e from nation) d",
			wantCIKey: true,
		},
		{
			name:      "SET derived projected ci",
			typ:       ciSet,
			query:     "select d.s = 'A,B' from (select n_name collate utf8mb4_general_ci as s from nation) d",
			wantCIKey: true,
		},
		{
			name:      "ENUM IN projected ci",
			typ:       ciEnum,
			query:     "select 'RED' in (select n_name collate utf8mb4_general_ci from nation)",
			wantCIKey: true,
		},
		{
			name:      "SET IN projected ci",
			typ:       ciSet,
			query:     "select 'A,B' in (select n_name collate utf8mb4_general_ci from nation)",
			wantCIKey: true,
		},
		{
			name:      "ENUM NOT IN projected ci",
			typ:       ciEnum,
			query:     "select 'RED' not in (select n_name collate utf8mb4_general_ci from nation)",
			wantCIKey: true,
		},
		{
			name:      "SET NOT IN projected ci",
			typ:       ciSet,
			query:     "select 'A,B' not in (select n_name collate utf8mb4_general_ci from nation)",
			wantCIKey: true,
		},
		{
			name:      "ENUM binary source scalar projected ci override",
			typ:       binaryEnum,
			query:     "select (select n_name collate utf8mb4_general_ci from nation where n_nationkey=0) = 'RED'",
			wantCIKey: true,
		},
		{
			name:      "SET binary source derived projected ci override",
			typ:       binarySet,
			query:     "select d.s = 'A,B' from (select n_name collate utf8mb4_general_ci as s from nation) d",
			wantCIKey: true,
		},
		{
			name:      "ENUM binary source IN projected ci override",
			typ:       binaryEnum,
			query:     "select 'RED' in (select n_name collate utf8mb4_general_ci from nation)",
			wantCIKey: true,
		},
		{
			name:      "SET binary source NOT IN projected ci override",
			typ:       binarySet,
			query:     "select 'A,B' not in (select n_name collate utf8mb4_general_ci from nation)",
			wantCIKey: true,
		},
		{
			name:  "ENUM scalar projected binary control",
			typ:   ciEnum,
			query: "select (select n_name collate utf8mb4_bin from nation where n_nationkey=0) = 'RED'",
		},
		{
			name:  "SET derived projected binary control",
			typ:   ciSet,
			query: "select d.s = 'A,B' from (select n_name collate utf8mb4_bin as s from nation) d",
		},
		{
			name:  "ENUM scalar projected arbitrary cast boundary",
			typ:   ciEnum,
			query: "select (select cast(n_name as char) collate utf8mb4_general_ci from nation where n_nationkey=0) = 'RED'",
		},
		{
			name:  "SET derived projected arbitrary cast boundary",
			typ:   ciSet,
			query: "select d.s = 'A,B' from (select cast(n_name as char) collate utf8mb4_general_ci as s from nation) d",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			mock.ctxt.tables["nation"].Cols[1].Typ = tc.typ
			pl, err := runOneStmt(mock, t, tc.query)
			require.NoError(t, err)
			key := findPlanFunctionExpr(pl, moMySQLSpecialGeneralCIKeyFun)
			if tc.wantCIKey {
				require.NotNil(t, key, "projected COLLATE must retain comparison provenance: %s", pl.GetQuery().String())
			} else {
				require.Nil(t, key, "binary and arbitrary-CAST projections must keep the original comparison")
			}
		})
	}
}

func TestEnumSetCTEAndSetOperationComparisonUsesColumnCollation(t *testing.T) {
	ciEnum := planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)}
	ciSet := planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)}
	binaryEnum := ciEnum
	binaryEnum.Charset = uint32(types.CharsetUTF8MB4Bin)
	binarySet := ciSet
	binarySet.Charset = uint32(types.CharsetUTF8MB4Bin)
	for _, tc := range []struct {
		name      string
		typ       planpb.Type
		regionTyp *planpb.Type
		query     string
		wantCIKey bool
	}{
		{
			name:      "materialized CTE ENUM projected ci",
			typ:       ciEnum,
			query:     "with d as (select n_name collate utf8mb4_general_ci as e, rand() as r from nation) select e = 'RED' from d",
			wantCIKey: true,
		},
		{
			name:      "materialized CTE SET projected ci",
			typ:       ciSet,
			query:     "with d as (select n_name collate utf8mb4_general_ci as s, rand() as r from nation) select s = 'A,B' from d",
			wantCIKey: true,
		},
		{
			name:      "materialized CTE ENUM binary source projected ci override",
			typ:       binaryEnum,
			query:     "with d as (select n_name collate utf8mb4_general_ci as e, rand() as r from nation) select e = 'RED' from d",
			wantCIKey: true,
		},
		{
			name:      "materialized CTE SET binary source projected ci override",
			typ:       binarySet,
			query:     "with d as (select n_name collate utf8mb4_general_ci as s, rand() as r from nation) select s = 'A,B' from d",
			wantCIKey: true,
		},
		{
			name:  "materialized CTE ENUM projected binary control",
			typ:   ciEnum,
			query: "with d as (select n_name collate utf8mb4_bin as e, rand() as r from nation) select e = 'RED' from d",
		},
		{
			name:  "materialized CTE SET arbitrary cast boundary",
			typ:   ciSet,
			query: "with d as (select cast(n_name as char) collate utf8mb4_general_ci as s, rand() as r from nation) select s = 'A,B' from d",
		},
		{
			name:      "UNION ALL ENUM projected ci",
			typ:       ciEnum,
			query:     "select d.e = 'RED' from (select n_name collate utf8mb4_general_ci as e from nation union all select null) d",
			wantCIKey: true,
		},
		{
			name:      "UNION ALL SET projected ci",
			typ:       ciSet,
			query:     "select d.s = 'A,B' from (select n_name collate utf8mb4_general_ci as s from nation union all select null) d",
			wantCIKey: true,
		},
		{
			name:      "UNION ALL ENUM binary source projected ci override",
			typ:       binaryEnum,
			query:     "select d.e = 'RED' from (select n_name collate utf8mb4_general_ci as e from nation union all select null) d",
			wantCIKey: true,
		},
		{
			name:      "UNION ALL SET binary source projected ci override",
			typ:       binarySet,
			query:     "select d.s = 'A,B' from (select n_name collate utf8mb4_general_ci as s from nation union all select null) d",
			wantCIKey: true,
		},
		{
			name:  "UNION ALL ENUM projected binary control",
			typ:   ciEnum,
			query: "select d.e = 'RED' from (select n_name collate utf8mb4_bin as e from nation union all select null) d",
		},
		{
			name:  "UNION ALL SET arbitrary cast boundary",
			typ:   ciSet,
			query: "select d.s = 'A,B' from (select cast(n_name as char) collate utf8mb4_general_ci as s from nation union all select null) d",
		},
		{
			name:      "UNION ALL ENUM compatible non-null branches",
			typ:       ciEnum,
			regionTyp: &ciEnum,
			query:     "select d.e = 'RED' from (select n_name as e from nation union all select r_name from region) d",
			wantCIKey: true,
		},
		{
			name:      "UNION ALL SET compatible non-null branches",
			typ:       ciSet,
			regionTyp: &ciSet,
			query:     "select d.s = 'A,B' from (select n_name as s from nation union all select r_name from region) d",
			wantCIKey: true,
		},
		{
			name:      "UNION ALL ENUM mixed source collations clear comparison provenance",
			typ:       ciEnum,
			regionTyp: &binaryEnum,
			query:     "select d.e = 'RED' from (select n_name as e from nation union all select r_name from region) d",
		},
		{
			name:      "UNION ALL SET mixed source collations clear comparison provenance",
			typ:       ciSet,
			regionTyp: &binarySet,
			query:     "select d.s = 'A,B' from (select n_name as s from nation union all select r_name from region) d",
		},
		{
			name:  "UNION ALL ordinary string boundary",
			typ:   ciEnum,
			query: "select d.e = 'RED' from (select n_name collate utf8mb4_general_ci as e from nation union all select 'GREEN') d",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			mock.ctxt.tables["nation"].Cols[1].Typ = tc.typ
			if tc.regionTyp != nil {
				mock.ctxt.tables["region"].Cols[1].Typ = *tc.regionTyp
			}
			pl, err := runOneStmt(mock, t, tc.query)
			require.NoError(t, err)
			key := findPlanFunctionExpr(pl, moMySQLSpecialGeneralCIKeyFun)
			if tc.wantCIKey {
				require.NotNil(t, key, "CTE and set-operation outputs must retain comparison provenance: %s", pl.GetQuery().String())
			} else {
				require.Nil(t, key, "binary, arbitrary-CAST and ordinary-string outputs must keep the original comparison")
			}
		})
	}
}

func TestEnumSetGroupedComparisonUsesColumnCollation(t *testing.T) {
	ciEnum := planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)}
	ciSet := planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)}
	binaryEnum := ciEnum
	binaryEnum.Charset = uint32(types.CharsetUTF8MB4Bin)
	binarySet := ciSet
	binarySet.Charset = uint32(types.CharsetUTF8MB4Bin)
	for _, tc := range []struct {
		name      string
		typ       planpb.Type
		query     string
		wantCIKey bool
	}{
		{
			name:      "grouped ENUM projected ci",
			typ:       ciEnum,
			query:     "select (n_name collate utf8mb4_general_ci) = 'RED' from nation group by n_name collate utf8mb4_general_ci",
			wantCIKey: true,
		},
		{
			name:      "grouped SET projected ci",
			typ:       ciSet,
			query:     "select (n_name collate utf8mb4_general_ci) = 'A,B' from nation group by n_name collate utf8mb4_general_ci",
			wantCIKey: true,
		},
		{
			name:      "grouped ENUM binary source projected ci override",
			typ:       binaryEnum,
			query:     "select (n_name collate utf8mb4_general_ci) = 'RED' from nation group by n_name collate utf8mb4_general_ci",
			wantCIKey: true,
		},
		{
			name:      "grouped SET binary source projected ci override",
			typ:       binarySet,
			query:     "select (n_name collate utf8mb4_general_ci) = 'A,B' from nation group by n_name collate utf8mb4_general_ci",
			wantCIKey: true,
		},
		{
			name:  "grouped ENUM projected binary control",
			typ:   ciEnum,
			query: "select (n_name collate utf8mb4_bin) = 'RED' from nation group by n_name collate utf8mb4_bin",
		},
		{
			name:  "grouped SET projected binary control",
			typ:   ciSet,
			query: "select (n_name collate utf8mb4_bin) = 'A,B' from nation group by n_name collate utf8mb4_bin",
		},
		{
			name:  "grouped ENUM arbitrary cast boundary",
			typ:   ciEnum,
			query: "select (cast(n_name as char) collate utf8mb4_general_ci) = 'RED' from nation group by cast(n_name as char) collate utf8mb4_general_ci",
		},
		{
			name:  "grouped SET arbitrary cast boundary",
			typ:   ciSet,
			query: "select (cast(n_name as char) collate utf8mb4_general_ci) = 'A,B' from nation group by cast(n_name as char) collate utf8mb4_general_ci",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			mock.ctxt.tables["nation"].Cols[1].Typ = tc.typ
			pl, err := runOneStmt(mock, t, tc.query)
			require.NoError(t, err)
			key := findPlanFunctionExpr(pl, moMySQLSpecialGeneralCIKeyFun)
			if tc.wantCIKey {
				require.NotNil(t, key, "group references must retain comparison provenance: %s", pl.GetQuery().String())
			} else {
				require.Nil(t, key, "binary and arbitrary-CAST group expressions must keep the original comparison")
			}
		})
	}
}

func TestEnumSetCorrelatedDerivedComparisonUsesColumnCollation(t *testing.T) {
	ciEnum := planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)}
	ciSet := planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)}
	binaryEnum := ciEnum
	binaryEnum.Charset = uint32(types.CharsetUTF8MB4Bin)
	binarySet := ciSet
	binarySet.Charset = uint32(types.CharsetUTF8MB4Bin)
	for _, tc := range []struct {
		name      string
		typ       planpb.Type
		query     string
		wantCIKey bool
	}{
		{
			name:      "ENUM direct correlated equality",
			typ:       ciEnum,
			query:     "select n.n_nationkey from nation n where exists (select 1 where n.n_name = 'RED')",
			wantCIKey: true,
		},
		{
			name:      "SET direct correlated equality",
			typ:       ciSet,
			query:     "select n.n_nationkey from nation n where exists (select 1 where n.n_name = 'A,B')",
			wantCIKey: true,
		},
		{
			name:      "ENUM derived correlated equality",
			typ:       ciEnum,
			query:     "select d.id from (select n_nationkey as id, n_name as e from nation) d where exists (select 1 where d.e = 'RED')",
			wantCIKey: true,
		},
		{
			name:      "SET derived correlated equality",
			typ:       ciSet,
			query:     "select d.id from (select n_nationkey as id, n_name as s from nation) d where exists (select 1 where d.s = 'A,B')",
			wantCIKey: true,
		},
		{
			name:      "ENUM derived correlated NULL safe equality",
			typ:       ciEnum,
			query:     "select d.id from (select n_nationkey as id, n_name as e from nation) d where exists (select 1 where d.e <=> null)",
			wantCIKey: true,
		},
		{
			name:      "SET derived correlated NULL safe equality",
			typ:       ciSet,
			query:     "select d.id from (select n_nationkey as id, n_name as s from nation) d where exists (select 1 where d.s <=> null)",
			wantCIKey: true,
		},
		{
			name:      "ENUM nested scope derived correlated equality",
			typ:       ciEnum,
			query:     "select d.id from (select n_nationkey as id, n_name as e from nation) d where exists (select 1 from region r where exists (select 1 where d.e = 'RED'))",
			wantCIKey: true,
		},
		{
			name:      "SET nested scope derived correlated equality",
			typ:       ciSet,
			query:     "select d.id from (select n_nationkey as id, n_name as s from nation) d where exists (select 1 from region r where exists (select 1 where d.s = 'A,B'))",
			wantCIKey: true,
		},
		{
			name:  "ENUM binary derived correlated control",
			typ:   binaryEnum,
			query: "select d.id from (select n_nationkey as id, n_name as e from nation) d where exists (select 1 where d.e = 'RED')",
		},
		{
			name:  "SET binary nested scope derived correlated control",
			typ:   binarySet,
			query: "select d.id from (select n_nationkey as id, n_name as s from nation) d where exists (select 1 from region r where exists (select 1 where d.s = 'A,B'))",
		},
		{
			name:  "ENUM arbitrary cast derived correlated boundary",
			typ:   ciEnum,
			query: "select d.id from (select n_nationkey as id, cast(n_name as char) as e from nation) d where exists (select 1 where d.e = 'RED')",
		},
		{
			name:  "SET arbitrary cast derived correlated boundary",
			typ:   ciSet,
			query: "select d.id from (select n_nationkey as id, cast(n_name as char) as s from nation) d where exists (select 1 where d.s = 'A,B')",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			mock.ctxt.tables["nation"].Cols[1].Typ = tc.typ
			pl, err := runOneStmt(mock, t, tc.query)
			require.NoError(t, err)
			key := findPlanFunctionExpr(pl, moMySQLSpecialGeneralCIKeyFun)
			if tc.wantCIKey {
				require.NotNil(t, key, "correlated references must resolve their owning scope: %s", pl.GetQuery().String())
			} else {
				require.Nil(t, key, "binary and arbitrary-CAST correlated operands must keep the original comparison")
			}
		})
	}
}

func TestEnumSetStringComparisonUsesColumnCollation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		typ       planpb.Type
		predicate string
		values    []uint64
		want      []bool
		wantNull  []bool
	}{
		{
			name:      "ENUM ci equality",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name = 'RED'",
			values:    []uint64{1, 2, 0},
			want:      []bool{true, false, false},
		},
		{
			name:      "ENUM ci IN",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name in ('red', 'green')",
			values:    []uint64{1, 2, 0},
			want:      []bool{true, true, false},
		},
		{
			name:      "ENUM ci mixed IN keeps label semantics",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name in ('RED', 2)",
			values:    []uint64{1, 2, 0},
			want:      []bool{true, false, false},
		},
		{
			name:      "ENUM numeric IN keeps ordinals",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name in (1, 2)",
			values:    []uint64{1, 2, 0},
			want:      []bool{true, true, false},
		},
		{
			name:      "ENUM ci NOT IN",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name not in ('red', 'green')",
			values:    []uint64{1, 2, 0},
			want:      []bool{false, false, false},
		},
		{
			name:      "ENUM ci inequality",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name != 'RED'",
			values:    []uint64{1, 2, 0},
			want:      []bool{false, true, false},
		},
		{
			name:      "ENUM ci null safe equality",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name <=> 'RED'",
			values:    []uint64{1, 2, 0},
			want:      []bool{true, false, false},
			wantNull:  []bool{false, false, false},
		},
		{
			name:      "ENUM null safe equality with NULL",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name <=> NULL",
			values:    []uint64{1, 2, 0},
			want:      []bool{false, false, true},
			wantNull:  []bool{false, false, false},
		},
		{
			name:      "ENUM ci trailing space padding",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name = 'RED '",
			values:    []uint64{1, 2, 0},
			want:      []bool{true, false, false},
		},
		{
			name:      "ENUM unicode ci equality",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8MB4UnicodeCI), CollationVersion: uint32(types.CollationVersionV1)},
			predicate: "n_name = 'RED'",
			values:    []uint64{1, 2, 0},
			want:      []bool{true, false, false},
		},
		{
			name:      "ENUM explicit binary COLLATE overrides ci",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name collate utf8mb4_bin = 'RED'",
			values:    []uint64{1, 2, 0},
			want:      []bool{false, false, false},
		},
		{
			name:      "ENUM ci LIKE",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name like 'r%'",
			values:    []uint64{1, 2, 0},
			want:      []bool{true, false, false},
		},
		{
			name:      "ENUM binary equality rejects case variant",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8MB4Bin)},
			predicate: "n_name = 'RED'",
			values:    []uint64{1, 2, 0},
			want:      []bool{false, false, false},
		},
		{
			name:      "ENUM binary equality accepts exact label",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8MB4Bin)},
			predicate: "n_name = 'Red'",
			values:    []uint64{1, 2, 0},
			want:      []bool{true, false, false},
		},
		{
			name:      "ENUM explicit ci COLLATE overrides binary",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8MB4Bin)},
			predicate: "n_name collate utf8mb4_general_ci = 'RED'",
			values:    []uint64{1, 2, 0},
			want:      []bool{true, false, false},
		},
		{
			name:      "SET unicode ci equality",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8MB4UnicodeCI), CollationVersion: uint32(types.CollationVersionV1)},
			predicate: "n_name = 'A,B'",
			values:    []uint64{3, 1, 0},
			want:      []bool{true, false, false},
		},
		{
			name:      "SET ci equality",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name = 'A,B'",
			values:    []uint64{3, 1, 0},
			want:      []bool{true, false, false},
		},
		{
			name:      "SET ci IN",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name in ('A,B', 'D')",
			values:    []uint64{3, 8, 1, 0},
			want:      []bool{true, true, false, false},
		},
		{
			name:      "SET ci mixed IN keeps label semantics",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name in ('A,B', 4)",
			values:    []uint64{3, 4, 0},
			want:      []bool{true, false, false},
		},
		{
			name:      "SET numeric IN keeps bitmaps",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name in (1, 3)",
			values:    []uint64{3, 1, 0},
			want:      []bool{true, true, false},
		},
		{
			name:      "SET ci NOT IN",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name not in ('A,B', 'D')",
			values:    []uint64{3, 8, 1, 0},
			want:      []bool{false, false, true, false},
		},
		{
			name:      "SET ci inequality",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name != 'A,B'",
			values:    []uint64{3, 1, 0},
			want:      []bool{false, true, false},
		},
		{
			name:      "SET ci null safe equality",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name <=> 'A,B'",
			values:    []uint64{3, 1, 0},
			want:      []bool{true, false, false},
			wantNull:  []bool{false, false, false},
		},
		{
			name:      "SET null safe equality with NULL",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name <=> NULL",
			values:    []uint64{3, 1, 0},
			want:      []bool{false, false, true},
			wantNull:  []bool{false, false, false},
		},
		{
			name:      "SET empty label equality",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name = ''",
			values:    []uint64{0, 1, 0},
			want:      []bool{true, false, false},
		},
		{
			name:      "SET member order remains significant",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name = 'B,A'",
			values:    []uint64{3, 1, 0},
			want:      []bool{false, false, false},
		},
		{
			name:      "SET ci LIKE",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name like 'A,%'",
			values:    []uint64{3, 1, 0},
			want:      []bool{true, false, false},
		},
		{
			name:      "SET binary equality rejects case variant",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8MB4Bin)},
			predicate: "n_name = 'A,B'",
			values:    []uint64{3, 1, 0},
			want:      []bool{false, false, false},
		},
		{
			name:      "ENUM numeric comparison keeps ordinal",
			typ:       planpb.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name = 1",
			values:    []uint64{1, 2, 0},
			want:      []bool{true, false, false},
		},
		{
			name:      "SET numeric comparison keeps bitmap",
			typ:       planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c,d", Charset: uint32(types.CharsetUTF8)},
			predicate: "n_name = 3",
			values:    []uint64{3, 1, 0},
			want:      []bool{true, false, false},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			mock := NewMockOptimizer(false, proc)
			mock.ctxt.tables["nation"].Cols[1].Typ = tc.typ
			pl, err := runOneExprStmt(mock, t, "select "+tc.predicate+" from nation")
			require.NoError(t, err)
			nodes := pl.GetQuery().Nodes
			expr := DeepCopyExpr(nodes[len(nodes)-1].ProjectList[0])
			setComparisonInputRelPos(expr)

			input := batch.NewWithSize(3)
			defer input.Clean(proc.Mp())
			input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
			input.Vecs[1] = vector.NewVec(makeTypeByPlan2Type(tc.typ))
			input.Vecs[2] = vector.NewVec(types.T_uint64.ToType())
			for i, value := range tc.values {
				isNull := i == len(tc.values)-1
				require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i), false, proc.Mp()))
				if tc.typ.Id == int32(types.T_enum) {
					require.NoError(t, vector.AppendFixed(input.Vecs[1], types.Enum(value), isNull, proc.Mp()))
				} else {
					require.NoError(t, vector.AppendFixed(input.Vecs[1], value, isNull, proc.Mp()))
				}
				require.NoError(t, vector.AppendFixed(input.Vecs[2], value, isNull, proc.Mp()))
			}
			input.SetRowCount(len(tc.values))

			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			wantNull := tc.wantNull
			if wantNull == nil {
				wantNull = make([]bool, len(tc.values))
				wantNull[len(wantNull)-1] = true
			}
			require.Len(t, tc.want, len(tc.values))
			require.Len(t, wantNull, len(tc.values))
			for i, want := range tc.want {
				require.Equal(t, wantNull[i], result.GetNulls().Contains(uint64(i)), "row %d NULL", i)
				if !wantNull[i] {
					require.Equal(t, want, vector.GetFixedAtNoTypeCheck[bool](result, i), "row %d", i)
				}
			}
		})
	}
}
