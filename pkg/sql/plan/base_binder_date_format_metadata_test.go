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
		format string
		width  int32
	}{
		{format: "%W %M %Y", width: 134},
		{format: "%a %b %D %j", width: 74},
		{format: "%H|%r|%T|%f", width: 35},
		{format: "%U%u%V%v%y%m%d%h%I%i%l%p%S%s%c%e", width: 32},
		{format: "%%-%q-%", width: 5},
		{format: strings.Repeat("%W", 2048), width: types.MaxVarcharLen},
	} {
		require.Equal(t, test.width, mysqlDateFormatWidth(test.format), test.format)
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
}

func TestBuildCTASDateFormatMetadataAndHeading(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	ctx.tables["time01"] = &planpb.TableDef{
		TblId:     1001,
		Name:      "time01",
		DbName:    "tpch",
		TableType: catalog.SystemOrdinaryRel,
		Cols: []*planpb.ColDef{
			{
				Name:       "col2",
				OriginName: "col2",
				Typ:        planpb.Type{Id: int32(types.T_datetime)},
				Default:    &planpb.Default{NullAbility: true},
			},
		},
	}
	ctx.objects["time01"] = &planpb.ObjectRef{ObjName: "time01", SchemaName: "tpch"}

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
