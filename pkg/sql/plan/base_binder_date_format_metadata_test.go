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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestMySQLDateFormatWidth(t *testing.T) {
	for _, test := range []struct {
		name   string
		format string
		width  int32
	}{
		{name: "date_format", format: "%W %M %Y", width: 134},
		{name: "date_format", format: "%a %b %D %j", width: 74},
		{name: "date_format", format: "%H|%r|%T|%f", width: 35},
		{name: "date_format", format: "%U%u%V%v%y%m%d%h%I%i%l%p%S%s%c%e", width: 53},
		{name: "date_format", format: "%U|%u|%V", width: 29},
		{name: "date_format", format: "%%-%q-%", width: 5},
		{name: "date_format", format: strings.Repeat("%W", 2048), width: types.MaxVarcharLen},
		{name: "time_format", format: "%H", width: 11},
		{name: "time_format", format: "%k", width: 11},
		{name: "time_format", format: "%T", width: 17},
		{name: "time_format", format: "%i", width: 3},
		{name: "time_format", format: "%H|%r|%T|%f", width: 47},
	} {
		require.Equal(t, test.width, mysqlDateFormatWidth(test.name, test.format), test.name+" "+test.format)
	}
}

func TestBindDateFormatMetadata(t *testing.T) {
	dateType := types.T_datetime.ToType()
	dateArg := &planpb.Expr{
		Typ:  makePlan2Type(&dateType),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}},
	}
	dateArg.Typ.NotNullable = true

	literal, err := BindFuncExprImplByPlanExpr(context.Background(), "date_format", []*planpb.Expr{
		dateArg,
		makePlan2StringConstExprWithType("%W %M %Y"),
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_varchar), literal.Typ.Id)
	require.Equal(t, int32(134), literal.Typ.Width)
	require.False(t, literal.Typ.NotNullable)

	dynamicPatternType := types.New(types.T_varchar, 12, 0)
	dynamic, err := BindFuncExprImplByPlanExpr(context.Background(), "date_format", []*planpb.Expr{
		dateArg,
		{
			Typ:  makePlan2Type(&dynamicPatternType),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1}},
		},
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.MaxVarcharLen), dynamic.Typ.Width)

	timeType := types.T_time.ToType()
	timeArg := &planpb.Expr{
		Typ:  makePlan2Type(&timeType),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}},
	}
	for _, test := range []struct {
		format string
		width  int32
	}{
		{format: "%H", width: 11},
		{format: "%T", width: 17},
		{format: "%i", width: 3},
	} {
		literal, err = BindFuncExprImplByPlanExpr(context.Background(), "time_format", []*planpb.Expr{
			timeArg,
			makePlan2StringConstExprWithType(test.format),
		})
		require.NoError(t, err)
		require.Equal(t, test.width, literal.Typ.Width, test.format)
	}
}

func TestBuildCTASDateFormatMetadataAndHeading(t *testing.T) {
	ctx := newDateFormatCompilerContext()

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"create table time02 as select date_format(col2, '%W %M %Y') from time01", 1)
	require.NoError(t, err)
	defer stmt.Free()

	logicPlan, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	wantName := "date_format(col2, '%W %M %Y')"

	var visible []*planpb.ColDef
	for _, col := range logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols() {
		if !col.Hidden {
			visible = append(visible, col)
		}
	}
	require.Len(t, visible, 1)
	require.Equal(t, wantName, visible[0].Name)
	require.Equal(t, int32(types.T_varchar), visible[0].Typ.Id)
	require.Equal(t, int32(134), visible[0].Typ.Width)
	require.False(t, visible[0].Typ.NotNullable)
	require.NotNil(t, visible[0].Default)
	require.True(t, visible[0].Default.NullAbility)
}

func TestBuildCTASPreservesNestedDateFormatHeading(t *testing.T) {
	ctx := newDateFormatCompilerContext()

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"create table time02 as select concat(date_format(col2, '%M'), 'X'), concat(date_format(col2, '%m'), 'X') from time01", 1)
	require.NoError(t, err)
	defer stmt.Free()

	logicPlan, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)

	var visible []*planpb.ColDef
	for _, col := range logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols() {
		if !col.Hidden {
			visible = append(visible, col)
		}
	}
	require.Len(t, visible, 2)
	require.Equal(t, "concat(date_format(col2, '%M'), 'X')", visible[0].Name)
	require.Equal(t, "concat(date_format(col2, '%m'), 'X')", visible[1].Name)
}

func TestBuildCTASLowercasesApostropheInQuotedAlias(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"create table t as select 1 as `A'B`", 1)
	require.NoError(t, err)
	defer stmt.Free()

	logicPlan, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)

	var visible []*planpb.ColDef
	for _, col := range logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols() {
		if !col.Hidden {
			visible = append(visible, col)
		}
	}
	require.Len(t, visible, 1)
	require.Equal(t, "a'b", visible[0].Name)
}

func newDateFormatCompilerContext() *MockCompilerContext {
	ctx := NewMockCompilerContext(false)
	datetime := planpb.Type{Id: int32(types.T_datetime)}
	ctx.tables["time01"] = &planpb.TableDef{
		TblId:     1001,
		Name:      "time01",
		DbName:    "tpch",
		TableType: catalog.SystemOrdinaryRel,
		Cols: []*planpb.ColDef{
			{
				Name:       "col2",
				OriginName: "col2",
				Typ:        datetime,
				Default:    &planpb.Default{NullAbility: true},
			},
			{
				Name:       "fmt_col",
				OriginName: "fmt_col",
				Typ:        makePlan2Type(&types.Type{Oid: types.T_varchar, Width: 12}),
				Default:    &planpb.Default{NullAbility: true},
			},
			{
				Name:       "D'X",
				OriginName: "D'X",
				Typ:        datetime,
				Default:    &planpb.Default{NullAbility: true},
			},
		},
	}
	ctx.objects["time01"] = &planpb.ObjectRef{ObjName: "time01", SchemaName: "tpch"}
	return ctx
}

func requireCTASColumnName(t *testing.T, ctx *MockCompilerContext, sql, want string) {
	t.Helper()
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()

	logicPlan, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	var visible []*planpb.ColDef
	for _, col := range logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols() {
		if !col.Hidden {
			visible = append(visible, col)
		}
	}
	require.Len(t, visible, 1)
	require.Equal(t, want, visible[0].Name)
}

func TestBuildCTASPreservesDateFormatHeadingThroughDerivedStars(t *testing.T) {
	for _, sql := range []string{
		"create table time02 as with c as (select date_format(col2, '%M') from time01) select * from c",
		"create table time02 as select * from (select date_format(col2, '%M') from time01) c",
	} {
		t.Run(sql, func(t *testing.T) {
			requireCTASColumnName(t, newDateFormatCompilerContext(), sql,
				"date_format(col2, '%M')")
		})
	}
}

func TestBuildCTASPreservesDateFormatHeadingThroughJoinUsingStar(t *testing.T) {
	requireCTASColumnName(t, newDateFormatCompilerContext(),
		"create table time02 as select * from (select date_format(col2, '%M') from time01) a "+
			"join (select date_format(col2, '%M') from time01) b using (`date_format(col2, '%M')`)",
		"date_format(col2, '%M')")
}

func TestBuildCTASUsesSafeHeadingForMismatchedFullJoinUsing(t *testing.T) {
	requireCTASColumnName(t, newDateFormatCompilerContext(),
		"create table time02 as select * from (select date_format(col2, '%M') from time01) a "+
			"full outer join (select date_format(col2, '%m') from time01) b "+
			"using (`date_format(col2, '%M')`)",
		"date_format(col2, '%m')")
}

func TestBuildCTASPreservesDateFormatHeadingThroughRollupWindowRewrite(t *testing.T) {
	ctx := newDateFormatCompilerContext()
	sql := "create table time02 as select date_format(col2, '%M'), date_format(col2, '%m'), row_number() over () from time01 group by col2 with rollup"
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()

	logicPlan, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	var visible []*planpb.ColDef
	for _, col := range logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols() {
		if !col.Hidden {
			visible = append(visible, col)
		}
	}
	require.Len(t, visible, 3)
	require.Equal(t, "date_format(col2, '%M')", visible[0].Name)
	require.Equal(t, "date_format(col2, '%m')", visible[1].Name)
}

func TestBuildCTASDynamicDateFormatLowercasesIdentifiers(t *testing.T) {
	requireCTASColumnName(t, newDateFormatCompilerContext(),
		"create table time02 as select date_format(`D'X`, fmt_col) from time01",
		"date_format(d'x, fmt_col)")
}

func TestBuildCTASLowercasesApostropheInSourceIdentifier(t *testing.T) {
	requireCTASColumnName(t, newDateFormatCompilerContext(),
		"create table time02 as select date_format(`D'X`, '%M') from time01",
		"date_format(d'x, '%M')")
}

func TestHeadingProvenanceIsLazyAndSparse(t *testing.T) {
	ctx := NewBindContext(nil, nil)
	require.Nil(t, ctx.headingProvenance)
	require.Nil(t, ctx.generatedHeadingProvenance)
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false), false, false)
	require.Nil(t, builder.headingProvenanceByNode)

	for i := 0; i < 1000; i++ {
		ctx.appendHeading("ordinary", headingProvenance{})
	}
	require.Nil(t, ctx.headingProvenance)

	ctx.appendHeading("date_format(col2, '%M')", headingProvenance{
		parts: []headingPart{
			{text: "date_format(col2, "},
			{text: "'%M'", literal: true},
			{text: ")"},
		},
	})
	require.Len(t, ctx.headingProvenance, 1)
	_, ok := ctx.headingProvenance[1000]
	require.True(t, ok)
}

func BenchmarkHeadingProvenanceStorage(b *testing.B) {
	b.Run("ordinary", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ctx := NewBindContext(nil, nil)
			for j := 0; j < 1000; j++ {
				ctx.appendHeading("ordinary", headingProvenance{})
			}
		}
	})

	b.Run("date_format", func(b *testing.B) {
		provenance := headingProvenance{
			parts: []headingPart{
				{text: "date_format(col2, "},
				{text: "'%M'", literal: true},
				{text: ")"},
			},
		}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ctx := NewBindContext(nil, nil)
			ctx.appendHeading("date_format(col2, '%M')", provenance)
		}
	})
}
