// Copyright 2024 Matrix Origin
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
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestBindTimestampAddFSPByUnit(t *testing.T) {
	ctx := context.Background()

	temporalExpr := func(oid types.T, scale int32) *plan.Expr {
		return &plan.Expr{
			Expr: &plan.Expr_Col{Col: &plan.ColRef{}},
			Typ:  plan.Type{Id: int32(oid), Scale: scale},
		}
	}

	tests := []struct {
		name      string
		unit      string
		inputType types.T
		inputFSP  int32
		resultOID types.T
		resultFSP int32
	}{
		{"datetime0_second", "SECOND", types.T_datetime, 0, types.T_datetime, 0},
		{"datetime3_second", "SECOND", types.T_datetime, 3, types.T_datetime, 3},
		{"datetime6_second", "SECOND", types.T_datetime, 6, types.T_datetime, 6},
		{"datetime0_microsecond", "MICROSECOND", types.T_datetime, 0, types.T_datetime, 6},
		{"datetime3_microsecond", "MICROSECOND", types.T_datetime, 3, types.T_datetime, 6},
		{"timestamp0_second", "SECOND", types.T_timestamp, 0, types.T_datetime, 0},
		{"timestamp3_second", "SECOND", types.T_timestamp, 3, types.T_datetime, 3},
		{"timestamp6_second", "SECOND", types.T_timestamp, 6, types.T_datetime, 6},
		{"timestamp0_microsecond", "MICROSECOND", types.T_timestamp, 0, types.T_datetime, 6},
		{"timestamp3_microsecond", "MICROSECOND", types.T_timestamp, 3, types.T_datetime, 6},
		{"date_second", "SECOND", types.T_date, 0, types.T_datetime, 0},
		{"date_microsecond", "MICROSECOND", types.T_date, 0, types.T_datetime, 6},
		{"date_day", "DAY", types.T_date, 0, types.T_date, 0},
		{"date_week", "WEEK", types.T_date, 0, types.T_date, 0},
		{"date_month", "MONTH", types.T_date, 0, types.T_date, 0},
		{"date_quarter", "QUARTER", types.T_date, 0, types.T_date, 0},
		{"date_year", "YEAR", types.T_date, 0, types.T_date, 0},
		{"date_hour", "HOUR", types.T_date, 0, types.T_datetime, 0},
		{"date_minute", "MINUTE", types.T_date, 0, types.T_datetime, 0},
		{"char_string", "SECOND", types.T_char, 0, types.T_varchar, 0},
		{"datetime_unknown_unit", "UNKNOWN", types.T_datetime, 3, types.T_datetime, 6},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := BindFuncExprImplByPlanExpr(ctx, "timestampadd", []*plan.Expr{
				MakePlan2StringConstExprWithType(tc.unit), MakePlan2Int64ConstExprWithType(1), temporalExpr(tc.inputType, tc.inputFSP),
			})
			require.NoError(t, err)
			require.Equal(t, int32(tc.resultOID), expr.Typ.Id)
			require.Equal(t, tc.resultFSP, expr.Typ.Scale)
			wantWidth := tc.resultFSP
			if tc.resultOID == types.T_varchar {
				wantWidth = 65535
			}
			require.Equal(t, wantWidth, expr.Typ.Width)
			columns := GetResultColumnsFromPlan(&plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
				StmtType: plan.Query_SELECT, Steps: []int32{0}, Headings: []string{"added"},
				Nodes: []*plan.Node{{NodeType: plan.Node_PROJECT, ProjectList: []*plan.Expr{expr}}},
			}}})
			require.Len(t, columns, 1)
			require.Equal(t, "added", columns[0].Name)
			require.Equal(t, int32(tc.resultOID), columns[0].Typ.Id)
			require.Equal(t, tc.resultFSP, columns[0].Typ.Scale)
			require.Equal(t, wantWidth, columns[0].Typ.Width)
		})
	}
}

func TestBindTemporalMetadataForTimestampArithmetic(t *testing.T) {
	ctx := context.Background()
	timestamp := &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{}}, Typ: plan.Type{
		Id: int32(types.T_timestamp), Scale: 6,
	}}
	convertTz, err := BindFuncExprImplByPlanExpr(ctx, "convert_tz", []*plan.Expr{
		timestamp, makeStringConst("+00:00"), makeStringConst("+08:00"),
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_datetime), convertTz.Typ.Id)
	require.Equal(t, int32(6), convertTz.Typ.Scale)
}

func TestBindTimestampAddFractionalSecond(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		unit       string
		source     types.T
		normalized bool
	}{
		{"SECOND", types.T_bool, false}, {"SECOND", types.T_binary, false}, {"SECOND", types.T_varbinary, false}, {"SECOND", types.T_blob, false}, {"SECOND", types.T_decimal64, true}, {"SECOND", types.T_decimal128, true}, {"SECOND", types.T_decimal256, true}, {"SECOND", types.T_float64, true}, {"SECOND", types.T_varchar, true}, {"SECOND", types.T_int64, false}, {"DAY", types.T_decimal64, false}, {"MICROSECOND", types.T_decimal64, false},
	} {
		t.Run(tc.unit+tc.source.String(), func(t *testing.T) {
			src := &plan.Expr{Typ: plan.Type{Id: int32(tc.source), Width: 18, Scale: 7}, Expr: &plan.Expr_Col{Col: &plan.ColRef{}}}
			date := &plan.Expr{Typ: plan.Type{Id: int32(types.T_datetime)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}}
			expr, err := BindFuncExprImplByPlanExpr(ctx, "timestampadd", []*plan.Expr{MakePlan2StringConstExprWithType(tc.unit), src, date})
			require.NoError(t, err)
			if tc.normalized {
				require.Equal(t, "MICROSECOND", expr.GetF().Args[0].GetLit().GetSval())
				require.Equal(t, "timestampadd_second_interval", expr.GetF().Args[1].GetF().Func.ObjName)
				require.Equal(t, int32(6), expr.Typ.Scale)
			} else {
				require.Equal(t, tc.unit, expr.GetF().Args[0].GetLit().GetSval())
			}
		})
	}
}

func TestPreparedTimestampAddSecondSourceReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	prepared := buildPreparedAggregatePlan(t, `select timestampadd(second,?,cast("2024-01-01 00:00:00" as datetime(6)))`)
	original := DeepCopyPlan(prepared.Plan)
	for _, tc := range []struct {
		value ParamValue
		want  string
		null  bool
	}{
		{ParamValue{Value: "1.5", SourceType: types.New(types.T_decimal64, 2, 1), HasSourceType: true}, "2024-01-01 00:00:01.500000", false},
		{ParamValue{Value: "-1.5", SourceType: types.New(types.T_decimal128, 20, 1), HasSourceType: true}, "2023-12-31 23:59:58.500000", false},
		{ParamValue{Value: 1.000001, SourceType: types.T_float64.ToType(), HasSourceType: true}, "2024-01-01 00:00:01.000001", false},
		{ParamValue{Value: "1.5", RuntimeType: types.T_float64.ToType(), HasRuntimeType: true, IsBinaryProtocol: true}, "2024-01-01 00:00:01.500000", false},
		{ParamValue{Value: "-1.5", RuntimeType: types.T_text.ToType(), HasRuntimeType: true, IsBinaryProtocol: true}, "2023-12-31 23:59:58.500000", false},
		{ParamValue{SourceType: types.New(types.T_decimal64, 2, 1), HasSourceType: true}, "", true},
		{ParamValue{Value: "0.0000009", SourceType: types.New(types.T_decimal64, 8, 7), HasSourceType: true}, "2024-01-01 00:00:00.000000", false},
	} {
		func() {
			filled, err := FillValuesOfParamsInPlan(context.Background(), prepared.Plan, []any{tc.value})
			require.NoError(t, err)
			expr := findPlanFunctionExpr(filled, "timestampadd")
			require.NotNil(t, expr)
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			require.Equal(t, tc.null, result.GetNulls().Contains(0))
			if !tc.null {
				require.Equal(t, tc.want, vector.GetFixedAtWithTypeCheck[types.Datetime](result, 0).String2(6))
			}
			require.True(t, proto.Equal(original, prepared.Plan))
		}()
	}
}

func TestTimestampAddSecondDefaultSource(t *testing.T) {
	proc := testutil.NewProcess(t)
	stmt, err := parsers.ParseOne(proc.Ctx, dialect.MYSQL, "select timestampadd(second,1.5,cast('2024-01-01' as datetime))", 1)
	require.NoError(t, err)
	defer stmt.Free()
	ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	expr, err := NewDefaultBinder(proc.Ctx, nil, nil, plan.Type{Id: int32(types.T_datetime), Scale: 6}, nil).BindExpr(ast, 0, false)
	require.NoError(t, err)
	result, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
	require.NoError(t, err)
	defer free()
	require.Equal(t, "2024-01-01 00:00:01.500000", vector.GetFixedAtWithTypeCheck[types.Datetime](result, 0).String2(6))
}

func TestTimestampAddSecondBinarySources(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		input string
		want  string
		null  bool
	}{
		{"IF(TRUE,TRUE,1.5)", "2024-01-01 00:00:01.000000", false},
		{"IFF(FALSE,1.5,FALSE)", "2024-01-01 00:00:00.000000", false},
		{"CASE WHEN TRUE THEN TRUE ELSE 1.5 END", "2024-01-01 00:00:01.000000", false},
		{"CASE 1 WHEN 1 THEN TRUE ELSE 1.5 END", "2024-01-01 00:00:01.000000", false},
		{"NULLIF(TRUE,FALSE)", "2024-01-01 00:00:01.000000", false},
		{"NULLIF(TRUE,TRUE)", "", true},
		{"IF(FALSE,TRUE,1.5)", "2024-01-01 00:00:01.500000", false},
		{"CAST('000000001' AS BINARY)", "2024-01-01 00:00:01.000000", false},
		{"IF(TRUE,CAST('000000001' AS BINARY),'2')", "2024-01-01 00:00:01.000000", false},
		{"CAST('1.5' AS BINARY)", "2024-01-01 00:00:01.000000", false},
		{"IF(TRUE,CAST('1.5' AS BINARY),1.5)", "2024-01-01 00:00:01.000000", false},
		{"IF(FALSE,CAST('1.5' AS BINARY),1.5)", "2024-01-01 00:00:01.500000", false},
		{"IF(TRUE,CAST(CAST('1.5' AS BINARY) AS CHAR),1.5)", "2024-01-01 00:00:01.500000", false},
		{"0x31", "2024-01-01 00:00:49.000000", false},
		{"X'31'", "2024-01-01 00:00:49.000000", false},
		{"B'110001'", "2024-01-01 00:00:49.000000", false},
		{"0x01", "2024-01-01 00:00:01.000000", false},
		{"0x000031", "2024-01-01 00:00:49.000000", false},
		{"(0x31)", "2024-01-01 00:00:49.000000", false},
		{"+0x31", "2024-01-01 00:00:49.000000", false},
		{"-0x31", "2023-12-31 23:59:11.000000", false},
		{"IF(TRUE,0x31,0x32)", "2024-01-01 00:00:49.000000", false},
		{"IF(TRUE,0x31,1.5)", "2024-01-01 00:00:49.000000", false},
		{"IF(FALSE,0x31,1.5)", "2024-01-01 00:00:01.500000", false},
		{"IFF(FALSE,0x31,1.5)", "2024-01-01 00:00:01.500000", false},
		{"IF(TRUE,CASE WHEN FALSE THEN 1.5 ELSE 0x31 END,0x01)", "2024-01-01 00:00:49.000000", false},
		{"CASE WHEN TRUE THEN 0x31 ELSE 0x32 END", "2024-01-01 00:00:49.000000", false},
		{"CASE 2 WHEN 1 THEN 0x31 ELSE 1.5 END", "2024-01-01 00:00:01.500000", false},
		{"CASE WHEN FALSE THEN 0x31 END", "", true},
		{"NULLIF(0x31,0x32)", "2024-01-01 00:00:49.000000", false},
		{"NULLIF(0x31,0x31)", "", true},
		{"CAST(0x31 AS CHAR)", "2024-01-01 00:00:01.000000", false},
		{"IF(TRUE,CAST(0x31 AS CHAR),1.5)", "2024-01-01 00:00:01.000000", false},
		{"IFNULL(0x31,0x32)", "2024-01-01 00:00:01.000000", false},
		{"COALESCE(0x31,0x32)", "2024-01-01 00:00:01.000000", false},
		{"'1.5'", "2024-01-01 00:00:01.500000", false},
		{"_binary '1.5'", "2024-01-01 00:00:01.500000", false},
		{"IF(TRUE,_binary '1.5',1.5)", "2024-01-01 00:00:01.500000", false},
		{"IF(TRUE,_binary '000000001.5','2')", "2024-01-01 00:00:01.500000", false},
	} {
		t.Run(tc.input, func(t *testing.T) {
			stmt, err := parsers.ParseOne(proc.Ctx, dialect.MYSQL, fmt.Sprintf("select timestampadd(second,%s,cast('2024-01-01' as datetime(6)))", tc.input), 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			expr, err := NewDefaultBinder(proc.Ctx, nil, nil, plan.Type{Id: int32(types.T_datetime), Scale: 6}, nil).BindExpr(ast, 0, false)
			require.NoError(t, err)
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			require.Equal(t, tc.null, result.GetNulls().Contains(0))
			if !tc.null {
				require.Equal(t, tc.want, vector.GetFixedAtWithTypeCheck[types.Datetime](result, 0).String2(6))
			}
		})
	}
}

func TestBindTimestampAddSecondBinaryPlanLiteral(t *testing.T) {
	for _, form := range []plan.StringLiteralForm{plan.StringLiteralForm_STRING_LITERAL_NONE, plan.StringLiteralForm_STRING_LITERAL_HEX, plan.StringLiteralForm_STRING_LITERAL_BIT} {
		source := makePlan2StringConstExprWithType("1", true)
		source.GetLit().LiteralForm = form
		date := &plan.Expr{Typ: plan.Type{Id: int32(types.T_datetime)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{}}}
		expr, err := BindFuncExprImplByPlanExpr(context.Background(), "timestampadd", []*plan.Expr{MakePlan2StringConstExprWithType("SECOND"), source, date})
		require.NoError(t, err)
		require.Equal(t, "SECOND", expr.GetF().Args[0].GetLit().GetSval())
		require.Equal(t, int32(types.T_int64), expr.GetF().Args[1].Typ.Id)
		require.Equal(t, int32(0), expr.Typ.Scale)
		features, err := plan.RequiredRemoteExpressionFeatures(expr)
		require.NoError(t, err)
		require.False(t, features.TimestampAddSecondIntervals)
	}
}

func TestPreparedTimestampAddSecondBinarySelection(t *testing.T) {
	proc := testutil.NewProcess(t)
	prepared := buildPreparedAggregatePlan(t, `select timestampadd(second,if(?,0x31,1.5),cast("2024-01-01" as datetime(6)))`)
	original := DeepCopyPlan(prepared.Plan)
	for _, tc := range []struct {
		chooseHex bool
		want      string
	}{{true, "2024-01-01 00:00:49.000000"}, {false, "2024-01-01 00:00:01.500000"}, {true, "2024-01-01 00:00:49.000000"}} {
		func() {
			filled, err := FillValuesOfParamsInPlan(proc.Ctx, prepared.Plan, []any{tc.chooseHex})
			require.NoError(t, err)
			expr := findPlanFunctionExpr(filled, "timestampadd")
			require.NotNil(t, expr)
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			require.Equal(t, tc.want, vector.GetFixedAtWithTypeCheck[types.Datetime](result, 0).String2(6))
			require.True(t, proto.Equal(original, prepared.Plan))
		}()
	}
}

func TestTimestampAddSecondStoredSelection(t *testing.T) {
	for _, storage := range []plan.Type{{Id: int32(types.T_enum), Enumvalues: "label,1.5"}, {Id: int32(types.T_uint64), Enumvalues: "label,1.5"}} {
		for _, tc := range []struct {
			input string
			want  int64
			null  bool
		}{
			{"n_name", 1000000, false},
			{"IF(TRUE,n_name,1.5)", 1000000, false},
			{"IF(FALSE,n_name,1.5)", 1500000, false},
			{"IFF(TRUE,n_name,1.5)", 1000000, false},
			{"CASE WHEN TRUE THEN n_name ELSE 1.5 END", 1000000, false},
			{"CASE 2 WHEN 1 THEN 1.5 ELSE n_name END", 1000000, false},
			{"NULLIF(n_name,'other')", 1000000, false},
			{"NULLIF(n_name,'label')", 0, true},
			{"IF(TRUE,CASE WHEN TRUE THEN n_name ELSE 2.5 END,1.5)", 1000000, false},
			{"IF(TRUE,CAST(n_name AS CHAR),1.5)", 0, false},
		} {
			t.Run(fmt.Sprint(storage.Id, "/", tc.input), func(t *testing.T) {
				proc := testutil.NewProcess(t)
				mock := NewMockOptimizer(false, proc)
				mock.ctxt.tables["nation"].Cols[1].Typ = storage
				pl, err := runOneExprStmt(mock, t, "select timestampdiff(microsecond,cast('2024-01-01' as datetime(6)),timestampadd(second,"+tc.input+",cast('2024-01-01' as datetime(6)))) from nation")
				require.NoError(t, err)
				expr := pl.GetQuery().Nodes[1].ProjectList[0]
				input := batch.NewWithSize(1)
				defer input.Clean(proc.Mp())
				typ := types.T(storage.Id).ToType()
				input.Vecs[0] = vector.NewVec(typ)
				if typ.Oid == types.T_enum {
					require.NoError(t, vector.AppendFixed(input.Vecs[0], types.Enum(1), false, proc.Mp()))
				} else {
					require.NoError(t, vector.AppendFixed(input.Vecs[0], uint64(1), false, proc.Mp()))
				}
				input.SetRowCount(1)
				executor, err := colexec.NewExpressionExecutor(proc, expr)
				require.NoError(t, err)
				defer executor.Free()
				result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
				require.NoError(t, err)
				require.Equal(t, tc.null, result.IsNull(0))
				if !tc.null {
					require.Equal(t, tc.want, vector.GetFixedAtWithTypeCheck[int64](result, 0))
				}
			})
		}
	}
}

func TestTimestampAddSecondStoredIntegerDomains(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, oid := range []types.T{types.T_bool, types.T_binary, types.T_varbinary, types.T_blob} {
		t.Run(oid.String(), func(t *testing.T) {
			typ := types.New(oid, 9, 0)
			stmt, err := parsers.ParseOne(proc.Ctx, dialect.MYSQL, "select timestampadd(second,if(p,v,1.5),cast('2024-01-01' as datetime(6)))", 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			expr, err := NewGeneratedColBinder(proc.Ctx, []string{"p", "v"}, []plan.Type{{Id: int32(types.T_bool)}, makePlan2Type(&typ)}).BindExpr(ast, 0, false)
			require.NoError(t, err)
			input := batch.NewWithSize(2)
			defer input.Clean(proc.Mp())
			input.Vecs[0] = vector.NewVec(types.T_bool.ToType())
			require.NoError(t, vector.AppendFixedList(input.Vecs[0], []bool{true, false, true}, nil, proc.Mp()))
			input.Vecs[1] = vector.NewVec(typ)
			if oid == types.T_bool {
				require.NoError(t, vector.AppendFixedList(input.Vecs[1], []bool{true, false, false}, []bool{false, false, true}, proc.Mp()))
			} else {
				for i, value := range []string{"000000001", "1.5", ""} {
					require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(value), i == 2, proc.Mp()))
				}
			}
			input.SetRowCount(3)
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, "2024-01-01 00:00:01.000000", vector.GetFixedAtWithTypeCheck[types.Datetime](result, 0).String2(6))
			require.Equal(t, "2024-01-01 00:00:01.500000", vector.GetFixedAtWithTypeCheck[types.Datetime](result, 1).String2(6))
			require.True(t, result.IsNull(2))
		})
	}
}

func TestPreparedTimestampAddSecondIntegerSource(t *testing.T) {
	proc := testutil.NewProcess(t)
	prepared := buildPreparedAggregatePlan(t, `select timestampadd(second,if(true,?,1.5),cast("2024-01-01" as datetime(6)))`)
	original := DeepCopyPlan(prepared.Plan)
	for _, tc := range []struct {
		source ParamValue
		want   string
	}{
		{ParamValue{Value: true, SourceType: types.T_bool.ToType(), HasSourceType: true}, "2024-01-01 00:00:01.000000"},
		{ParamValue{Value: "000000001", SourceType: types.New(types.T_varbinary, 9, 0), HasSourceType: true}, "2024-01-01 00:00:01.000000"},
		{ParamValue{Value: "1.5", SourceType: types.New(types.T_varbinary, 9, 0), HasSourceType: true}, "2024-01-01 00:00:01.000000"},
		{ParamValue{Value: "1.5", SourceType: types.New(types.T_decimal64, 2, 1), HasSourceType: true}, "2024-01-01 00:00:01.500000"},
	} {
		t.Run(tc.source.SourceType.String(), func(t *testing.T) {
			filled, err := FillValuesOfParamsInPlan(proc.Ctx, prepared.Plan, []any{tc.source})
			require.NoError(t, err)
			expr := findPlanFunctionExpr(filled, "timestampadd")
			require.NotNil(t, expr)
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			require.Equal(t, tc.want, vector.GetFixedAtWithTypeCheck[types.Datetime](result, 0).String2(6))
			require.True(t, proto.Equal(original, prepared.Plan))
		})
	}
}
