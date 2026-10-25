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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestPreparedTemporalResultContracts(t *testing.T) {
	for _, tc := range []struct {
		name, expression, input, want string
		source                        types.Type
		result                        types.T
		fsp                           int32
	}{
		{"integer epoch", "from_unixtime(?)", "1704067200", "2024-01-01 00:00:00.000000", types.T_int64.ToType(), types.T_datetime, 6},
		{"decimal epoch", "from_unixtime(?)", "1704067200.123456", "2024-01-01 00:00:00.123456", types.New(types.T_decimal64, 16, 6), types.T_datetime, 6},
		{"float epoch", "from_unixtime(?)", "1704067200.125", "2024-01-01 00:00:00.125000", types.T_float64.ToType(), types.T_datetime, 6},
		{"decimal epoch scale3", "from_unixtime(?)", "1704067200.123", "2024-01-01 00:00:00.123000", types.New(types.T_decimal64, 13, 3), types.T_datetime, 6},
		{"date add numeric marker", "date_add(?, interval 1 day)", "20240131", "1970-08-24", types.T_int64.ToType(), types.T_date, 0},
		{"date add typed marker", "date_add(?, interval 1 day)", "2024-01-31", "2024-02-01", types.T_date.ToType(), types.T_date, 0},
		{"date add calendar boundary", "date_add(?, interval 0 day)", "9999-12-31 23:59:59.999999", "9999-12-31 23:59:59.999999", types.T_varchar.ToType(), types.T_datetime, 6},
		{"date add calendar unit", "date_add(?, interval 1 day)", "2024-01-31 12:34:56.123456", "2024-02-01 12:34:56.123456", types.T_varchar.ToType(), types.T_datetime, 6},
		{"date sub calendar unit", "date_sub(?, interval 1 month)", "2024-01-31 12:34:56.123456", "2023-12-31 12:34:56.123456", types.T_varchar.ToType(), types.T_datetime, 6},
		{"date add year month numeric", "date_add(?, interval 1 year_month)", "2024-01-31 12:34:56.123456", "2024-02-29 12:34:56.123456", types.T_varchar.ToType(), types.T_datetime, 6},
		{"date add year month string", "date_add(?, interval '1' year_month)", "2024-01-31 12:34:56.123456", "2024-02-29 12:34:56.123456", types.T_varchar.ToType(), types.T_datetime, 6},
		{"date sub year month numeric", "date_sub(?, interval 1 year_month)", "2024-01-31 12:34:56.123456", "2023-12-31 12:34:56.123456", types.T_varchar.ToType(), types.T_datetime, 6},
		{"date sub year month string", "date_sub(?, interval '1' year_month)", "2024-01-31 12:34:56.123456", "2023-12-31 12:34:56.123456", types.T_varchar.ToType(), types.T_datetime, 6},
		{"date add date string", "date_add(?, interval 1 day)", "2024-02-28", "2024-02-29", types.T_varchar.ToType(), types.T_date, 0},
		{"date add compact date", "date_add(?, interval 1 day)", "20240228", "2024-02-29", types.T_varchar.ToType(), types.T_date, 0},
		{"date add compact datetime", "date_add(?, interval 1 day)", "20240228233000.999999", "2024-02-29 23:30:00.999999", types.T_varchar.ToType(), types.T_datetime, 6},
		{"date add compact tab", "date_add(?, interval 1 day)", "\t20240228233000.999999\t", "2024-02-29 23:30:00.999999", types.T_varchar.ToType(), types.T_datetime, 6},
		{"date sub compact newline", "date_sub(?, interval 1 month)", "\n20240228233000.999999\n", "2024-01-28 23:30:00.999999", types.T_varchar.ToType(), types.T_datetime, 6},
		{"date add compact date whitespace", "date_add(?, interval 1 day)", " \t20240228\n", "2024-02-29", types.T_varchar.ToType(), types.T_date, 0},
		{"date add ISO midnight", "date_add(?, interval 1 day)", "2024-02-28T00:00:00", "2024-02-29 00:00:00.000000", types.T_varchar.ToType(), types.T_datetime, 6},
		{"date add typed datetime", "date_add(?, interval 1 month)", "2024-02-28 23:30:00.999", "2024-03-28 23:30:00.999000", types.New(types.T_datetime, 3, 3), types.T_datetime, 6},
		{"interval operator numeric marker", "? + interval 1 day", "20240131", "1970-08-24", types.T_int64.ToType(), types.T_date, 0},
		{"interval operator calendar add", "? + interval 1 day", "2024-01-31 12:34:56.123456", "2024-02-01 12:34:56.123456", types.T_varchar.ToType(), types.T_datetime, 6},
		{"interval operator calendar reverse", "interval 1 day + ?", "2024-01-31 12:34:56.123456", "2024-02-01 12:34:56.123456", types.T_varchar.ToType(), types.T_datetime, 6},
		{"interval operator calendar subtract", "? - interval 1 month", "2024-01-31 12:34:56.123456", "2023-12-31 12:34:56.123456", types.T_varchar.ToType(), types.T_datetime, 6},
		{"interval operator explicit datetime", "cast(? as datetime(6)) + interval 1 day", "2024-01-31 12:34:56.123456", "2024-02-01 12:34:56.123456", types.T_varchar.ToType(), types.T_datetime, 6},
		{"interval operator time add", "? + interval 1 hour", "2024-01-31 12:34:56.123456", "2024-01-31 13:34:56.123456", types.T_varchar.ToType(), types.T_datetime, 6},
		{"date add explicit date", "date_add(cast(? as date), interval 1 hour)", "2024-01-31 12:34:56.123456", "2024-01-31 01:00:00", types.T_varchar.ToType(), types.T_datetime, 0},
		{"date add time unit", "date_add(?, interval 1 hour)", "2024-01-31 12:34:56.123456", "2024-01-31 13:34:56.123456", types.T_varchar.ToType(), types.T_datetime, 6},
		{"timestampadd microsecond", "timestampadd(microsecond, 1, ?)", "2024-01-31 12:34:56.000001", "2024-01-31 12:34:56.000002", types.T_varchar.ToType(), types.T_datetime, 6},
		{"timestampadd day", "timestampadd(day, 1, ?)", "2024-01-31 12:34:56.123456", "2024-02-01 12:34:56.123456", types.T_varchar.ToType(), types.T_datetime, 6},
		{"timestampadd hour typed scale3", "timestampadd(hour, 1, ?)", "2024-01-31 12:34:56.123", "2024-01-31 13:34:56.123000", types.New(types.T_datetime, 3, 3), types.T_datetime, 6},
		{"timestampadd explicit date", "timestampadd(day, 1, cast(? as date))", "2024-01-31 12:34:56.123456", "2024-02-01", types.T_varchar.ToType(), types.T_date, 0},
		{"timestamp typed scale3", "timestamp(?)", "2024-02-29 12:34:56.123", "2024-02-29 12:34:56.123", types.New(types.T_datetime, 3, 3), types.T_datetime, 3},
		{"timestamp dynamic string", "timestamp(?)", "2024-02-29 12:34:56.123", "2024-02-29 12:34:56.123000", types.T_varchar.ToType(), types.T_datetime, 6},
		{"timestamp datetime", "timestamp(?)", "2024-02-29 12:34:56.123456", "2024-02-29 12:34:56.123456", types.New(types.T_datetime, 6, 6), types.T_datetime, 6},
		{"addtime string rollover", "addtime(?, '02:00:00.000001')", "2024-02-29 23:30:00.123456", "01:30:00.123457", types.T_varchar.ToType(), types.T_time, 6},
		{"addtime genuine duration", "addtime(?, '02:00:00.000001')", "23:30:00.123456", "25:30:00.123457", types.T_varchar.ToType(), types.T_time, 6},
		{"subtime genuine duration", "subtime(?, '02:00:00.000001')", "00:30:00.123456", "-01:29:59.876545", types.T_varchar.ToType(), types.T_time, 6},
		{"addtime negative calendar rollover", "addtime(?, '-02:00:00.000001')", "2024-02-29 00:30:00.123456", "22:30:00.123455", types.T_varchar.ToType(), types.T_time, 6},
		{"addtime explicit datetime", "addtime(cast(? as datetime(6)), '02:00:00.000001')", "2024-02-29 23:30:00.123456", "2024-03-01 01:30:00.123457", types.T_varchar.ToType(), types.T_datetime, 6},
		{"addtime typed rollover", "addtime(?, '02:00:00.000001')", "2024-02-29 23:30:00.123456", "01:30:00.123457", types.New(types.T_datetime, 6, 6), types.T_time, 6},
		{"subtime typed rollover", "subtime(?, '02:00:00.000001')", "2024-02-29 00:30:00.123456", "22:30:00.123455", types.New(types.T_datetime, 6, 6), types.T_time, 6},
	} {
		for _, ctas := range []bool{false, true} {
			name := "select/" + tc.name
			if ctas {
				name = "ctas/" + tc.name
			}
			t.Run(name, func(t *testing.T) {
				mock := NewMockOptimizer(false, newPlanTestProcess(t))
				proc := mock.ctxt.GetProcess()
				proc.GetSessionInfo().TimeZone = time.UTC
				params := vector.NewVec(types.T_text.ToType())
				defer func() {
					proc.SetPrepareParams(nil)
					params.Free(proc.Mp())
				}()
				require.NoError(t, vector.AppendBytes(params, []byte(tc.input), false, proc.Mp()))
				value := ParamValue{Value: tc.input, SourceType: tc.source, HasSourceType: true}
				if tc.source.Oid.IsInteger() {
					value.PrepareParamKind = vector.PrepareParamInteger
				} else if tc.source.IsDecimal() {
					value.PrepareParamKind = vector.PrepareParamDecimal
				}
				sql := "select " + tc.expression + " as v"
				if ctas {
					sql = "create table temporal_result as " + sql
				}
				stmt, err := mysql.ParseOne(context.Background(), sql, 1)
				require.NoError(t, err)
				defer stmt.Free()
				var query *planpb.Query
				if ctas {
					prepared, err := runOneStmt(mock, t, "prepare temporal_result from '"+strings.ReplaceAll(sql, "'", "''")+"'")
					require.NoError(t, err)
					original := prepared.GetDcl().GetPrepare().Plan
					if strings.HasPrefix(tc.expression, "from_unixtime") {
						originalExpr := findPlanFunctionExpr(&Plan{Plan: &planpb.Plan_Query{Query: original.GetDdl().Query}}, "from_unixtime")
						require.NotNil(t, originalExpr)
						require.True(t, preparedExprContainsParam(originalExpr))
						t.Logf("unfilled FROM_UNIXTIME type: %s", originalExpr.Typ.String())
					}
					proc.SetPrepareParams(params)
					filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), original, []any{value})
					require.NoError(t, err)
					require.NoError(t, RefreshPreparedCTASInferredColumns(context.Background(), filled, original, stmt))
					query = filled.GetDdl().Query
					column := filled.GetDdl().GetCreateTable().TableDef.Cols[0]
					assertTemporalResultType(t, column.Typ, tc.result, tc.fsp)
				} else {
					proc.SetPrepareParams(params)
					bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, []PreparedSourceBinding{{Position: 0, Type: tc.source}}, []any{value})
					require.NoError(t, err)
					query = bound.Plan.GetQuery()
				}
				expr := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList[0]
				assertTemporalResultType(t, expr.Typ, tc.result, tc.fsp)
				executor, err := colexec.NewExpressionExecutor(proc, expr)
				require.NoError(t, err)
				defer executor.Free()
				result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
				require.NoError(t, err)
				require.False(t, result.IsNull(0))
				require.Equal(t, tc.result, result.GetType().Oid)
				var got string
				switch tc.result {
				case types.T_datetime:
					got = vector.GetFixedAtWithTypeCheck[types.Datetime](result, 0).String2(tc.fsp)
				case types.T_date:
					got = vector.GetFixedAtWithTypeCheck[types.Date](result, 0).String()
				case types.T_time:
					got = vector.GetFixedAtWithTypeCheck[types.Time](result, 0).String2(tc.fsp)
				}
				require.Equal(t, tc.want, got)
			})
		}
	}
}

func assertTemporalResultType(t *testing.T, got planpb.Type, oid types.T, fsp int32) {
	t.Helper()
	require.Equal(t, oid, types.T(got.Id), got.String())
	require.Equal(t, fsp, got.Scale, got.String())
	require.Equal(t, fsp, got.Width, got.String())
}

func TestPreparedTemporalWriteConsumers(t *testing.T) {
	for _, tc := range []struct {
		name, expression, input, want string
		source                        types.Type
	}{
		{"epoch decimal", "from_unixtime(?)", "1704067200.123", "2024-01-01 00:00:00.123000", types.New(types.T_decimal64, 13, 3)},
		{"date hour", "date_add(?, interval 1 hour)", "2024-02-28", "2024-02-28 01:00:00.000000", types.T_date.ToType()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			proc := mock.ctxt.GetProcess()
			proc.GetSessionInfo().TimeZone = time.UTC
			table := mock.ctxt.tables["nation"]
			rowID := table.Cols[len(table.Cols)-1]
			table.Cols = append(table.Cols[:1], rowID)
			table.Cols[0].Typ = planpb.Type{Id: int32(types.T_datetime), Width: 6, Scale: 6}
			table.Cols[0].Default = nil
			table.Name2ColIndex = map[string]int32{"n_nationkey": 0, rowID.Name: 1}
			prepared, err := runOneStmt(mock, t, "prepare p from 'insert into nation(n_nationkey) select "+tc.expression+"'")
			require.NoError(t, err)
			params := vector.NewVec(types.T_text.ToType())
			defer params.Free(proc.Mp())
			defer proc.SetPrepareParams(nil)
			require.NoError(t, vector.AppendBytes(params, []byte(tc.input), false, proc.Mp()))
			proc.SetPrepareParams(params)
			value := ParamValue{Value: tc.input, SourceType: tc.source, HasSourceType: true}
			if tc.source.IsDecimal() {
				value.PrepareParamKind = vector.PrepareParamDecimal
			}
			filled, _, err := FillValuesOfParamsInPlanWithSpecializationPreservingDMLWrites(context.Background(), prepared.GetDcl().GetPrepare().Plan, []any{value})
			require.NoError(t, err)
			originalQuery := prepared.GetDcl().GetPrepare().Plan.GetQuery()
			targets := preparedDMLWriteExpressions(originalQuery)
			targetCount := 0
			for id, node := range originalQuery.Nodes {
				for slot, original := range node.ProjectList {
					if target := targets[original]; target.Id != 0 {
						got := filled.GetQuery().Nodes[id].ProjectList[slot].Typ
						require.Equal(t, target.Id, got.Id)
						require.Equal(t, target.Width, got.Width)
						require.Equal(t, target.Scale, got.Scale)
						targetCount++
					}
				}
			}
			require.Positive(t, targetCount, "must retain real positional writer inputs")
			name := "from_unixtime"
			if tc.name == "date hour" {
				name = "date_add"
			}
			expr := findPlanFunctionExpr(filled, name)
			require.NotNil(t, expr)
			target := types.New(types.T_datetime, 6, 6)
			expr, err = appendCastBeforeExpr(context.Background(), expr, makePlan2Type(&target))
			require.NoError(t, err)
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			require.Equal(t, tc.want, vector.GetFixedAtWithTypeCheck[types.Datetime](result, 0).String2(6), expr.String())
		})
	}
}

func TestPreparedSharedProjectionKeepsWriterDestination(t *testing.T) {
	source := &Expr{Typ: planpb.Type{Id: int32(types.T_date)}, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
	destination := DeepCopyExpr(source)
	destination.Typ = planpb.Type{Id: int32(types.T_datetime), Width: 6, Scale: 6}
	// The first writer sees source only below its final projection. The second
	// writer consumes the shared source projection directly, so a traversal
	// must promote that existing entry to an authoritative destination type.
	query := &planpb.Query{StmtType: planpb.Query_INSERT, Steps: []int32{3, 4}, Nodes: []*planpb.Node{
		{NodeType: planpb.Node_VALUE_SCAN},
		{NodeType: planpb.Node_PROJECT, Children: []int32{0}, ProjectList: []*Expr{source}},
		{NodeType: planpb.Node_PROJECT, Children: []int32{1}, ProjectList: []*Expr{destination}},
		{NodeType: planpb.Node_INSERT, Children: []int32{2}},
		{NodeType: planpb.Node_INSERT, Children: []int32{1}},
	}}
	targets := preparedDMLWriteExpressions(query)
	require.Equal(t, source.Typ, targets[source])
	require.Equal(t, destination.Typ, targets[destination])
}

func TestPreparedCalendarSelectHistory(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	ctx := &mock.ctxt
	originalContext := ctx.GetContext()
	defer ctx.SetContext(originalContext)
	stmt, err := mysql.ParseOne(context.Background(), "select date_add(?, interval 1 day) as a, date_sub(?, interval 1 month) as b", 1)
	require.NoError(t, err)
	defer stmt.Free()
	var history *PreparedTemporalTypes
	for _, tc := range []struct {
		values   []any
		results  []types.T
		revision uint64
	}{
		{[]any{"2024-02-28", "2024-02-28"}, []types.T{types.T_date, types.T_date}, 0},
		{[]any{"2024-02-28T00:00:00", "2024-02-28"}, []types.T{types.T_datetime, types.T_date}, 1},
		{[]any{"2024-02-28", "20240228000000"}, []types.T{types.T_datetime, types.T_datetime}, 2},
		{[]any{nil, "2024-02-28"}, []types.T{types.T_datetime, types.T_datetime}, 2},
	} {
		ctx.SetContext(WithPreparedTemporalTypes(originalContext, history))
		old := history
		var oldFlags []bool
		if old != nil {
			oldFlags = append([]bool(nil), old.datetime...)
		}
		bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_varchar.ToType()}, {Position: 1, Type: types.T_varchar.ToType()}}
		params := vector.NewVec(types.T_text.ToType())
		func() {
			defer params.Free(ctx.GetProcess().Mp())
			defer ctx.GetProcess().SetPrepareParams(nil)
			values := make([]any, len(tc.values))
			for i, v := range tc.values {
				text, _ := v.(string)
				require.NoError(t, vector.AppendBytes(params, []byte(text), v == nil, ctx.GetProcess().Mp()))
				values[i] = ParamValue{Value: v, SourceType: types.T_varchar.ToType(), HasSourceType: true}
			}
			ctx.GetProcess().SetPrepareParams(params)
			bound, err := BuildPreparedExecutionPlan(ctx, stmt, bindings, values)
			require.NoError(t, err)
			require.True(t, bound.ValueDependent, "DATE and unknown strings must also bypass type-only caching")
			q := bound.Plan.GetQuery()
			for i, expr := range q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList {
				fsp := int32(0)
				if tc.results[i] == types.T_datetime {
					fsp = 6
				}
				assertTemporalResultType(t, expr.Typ, tc.results[i], fsp)
			}
			history = bound.TemporalTypes
			require.Equal(t, tc.revision, history.Revision())
			if old != nil {
				require.Equal(t, oldFlags, old.datetime, "a retry may still hold the prior snapshot")
			}
		}()
	}
	// History is a SELECT consumer hint, never a new source domain or DML rule.
	ctxNoSelect := withPreparedSourceBindings(WithPreparedTemporalTypes(context.Background(), history), []PreparedSourceBinding{{Position: 0, Type: types.T_varchar.ToType()}}, []any{"2024-02-28"})
	typ := types.T_varchar.ToType()
	marker := &Expr{Typ: makePlan2Type(&typ), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
	require.Equal(t, types.T_date, preparedCalendarMarkerResult(ctxNoSelect, marker, true).Oid)
	require.Equal(t, uint64(2), history.Revision())
}

func TestPreparedTemporalMarkerReuse(t *testing.T) {
	for _, expression := range []string{"timestamp(?)", "date_add(?, interval 1 hour)", "addtime(?, '02:00:00.000001')"} {
		t.Run(expression, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			proc := mock.ctxt.GetProcess()
			proc.GetSessionInfo().TimeZone = time.UTC
			sql := "create table temporal_reuse as select " + expression + " as v"
			stmt, err := mysql.ParseOne(context.Background(), sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			prepared, err := runOneStmt(mock, t, "prepare temporal_reuse from '"+strings.ReplaceAll(sql, "'", "''")+"'")
			require.NoError(t, err)
			original := prepared.GetDcl().GetPrepare().Plan
			before, err := original.Marshal()
			require.NoError(t, err)
			for _, value := range []any{"2024-02-29 23:30:00.123456", "invalid", nil, "2024-02-29 23:30:00.123456"} {
				func() {
					param := ParamValue{Value: value, SourceType: types.T_varchar.ToType(), HasSourceType: true}
					filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), original, []any{param})
					require.NoError(t, err)
					require.NoError(t, RefreshPreparedCTASInferredColumns(context.Background(), filled, original, stmt))
					query := filled.GetDdl().Query
					expr := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList[0]
					wantType := types.T_datetime
					if strings.HasPrefix(expression, "addtime") {
						wantType = types.T_time
					}
					assertTemporalResultType(t, expr.Typ, wantType, 6)
					if strings.HasPrefix(expression, "timestamp") {
						require.Equal(t, function.EncodeOverloadID(function.TIMESTAMP, 9), expr.GetF().Func.Obj)
					}
					executor, err := colexec.NewExpressionExecutor(proc, expr)
					require.NoError(t, err)
					defer executor.Free()
					result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
					require.NoError(t, err)
					invalid := value == nil || value == "invalid"
					require.Equal(t, invalid, result.IsNull(0))
					if !invalid {
						var got, want string
						switch {
						case wantType == types.T_time:
							got = vector.GetFixedAtWithTypeCheck[types.Time](result, 0).String2(6)
							want = "01:30:00.123457"
						case strings.HasPrefix(expression, "date_add"):
							got = vector.GetFixedAtWithTypeCheck[types.Datetime](result, 0).String2(6)
							want = "2024-03-01 00:30:00.123456"
						default:
							got = vector.GetFixedAtWithTypeCheck[types.Datetime](result, 0).String2(6)
							want = "2024-02-29 23:30:00.123456"
						}
						require.Equal(t, want, got)
					}
					after, err := original.Marshal()
					require.NoError(t, err)
					require.Equal(t, before, after, "EXECUTE must not mutate the cached temporal template")
				}()
			}
		})
	}
}

func TestPreparedCalendarUnitWithCountMarker(t *testing.T) {
	for _, tc := range []struct {
		expression string
		oid        types.T
		fsp        int32
		want       string
	}{
		{"date_add(?, interval ? year_month)", types.T_datetime, 6, "2024-02-29 12:34:56.123456"},
		{"date_sub(?, interval ? year_month)", types.T_datetime, 6, "2023-12-31 12:34:56.123456"},
		{"date_add(?, interval ? day)", types.T_datetime, 6, "2024-02-01 12:34:56.123456"},
		{"? + interval ? day", types.T_datetime, 6, "2024-02-01 12:34:56.123456"},
		{"date_add(?, interval ? hour)", types.T_datetime, 6, "2024-01-31 13:34:56.123456"},
		{"timestampadd(day, ?, ?)", types.T_datetime, 6, "2024-02-01 12:34:56.123456"},
		{"timestampadd(microsecond, ?, ?)", types.T_datetime, 6, "2024-01-31 12:34:56.123457"},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			sql := "create table temporal_count as select " + tc.expression + " as v"
			stmt, err := mysql.ParseOne(context.Background(), sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			prepared, err := runOneStmt(mock, t, "prepare temporal_count from '"+sql+"'")
			require.NoError(t, err)
			date := ParamValue{Value: "2024-01-31 12:34:56.123456", SourceType: types.T_varchar.ToType(), HasSourceType: true}
			sourceDates := []ParamValue{date}
			if strings.HasPrefix(tc.expression, "date_") {
				sourceDates = append(sourceDates, ParamValue{Value: date.Value, IsBinaryProtocol: true})
			}
			for _, date := range sourceDates {
				for _, countType := range []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()} {
					count := ParamValue{Value: "1", SourceType: countType, HasSourceType: true}
					values := []any{date, count}
					if strings.HasPrefix(tc.expression, "timestampadd") {
						values = []any{count, date}
					}
					filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), prepared.GetDcl().GetPrepare().Plan, values)
					require.NoError(t, err)
					require.NoError(t, RefreshPreparedCTASInferredColumns(context.Background(), filled, prepared.GetDcl().GetPrepare().Plan, stmt))
					query := filled.GetDdl().Query
					expr := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList[0]
					assertTemporalResultType(t, expr.Typ, tc.oid, tc.fsp)
					func() {
						executor, err := colexec.NewExpressionExecutor(mock.ctxt.GetProcess(), expr)
						require.NoError(t, err)
						defer executor.Free()
						result, err := executor.Eval(mock.ctxt.GetProcess(), []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
						require.NoError(t, err)
						require.False(t, result.IsNull(0))
						var got string
						if tc.oid == types.T_date {
							got = vector.GetFixedAtWithTypeCheck[types.Date](result, 0).String()
						} else {
							got = vector.GetFixedAtWithTypeCheck[types.Datetime](result, 0).String2(tc.fsp)
						}
						require.Equal(t, tc.want, got)
					}()
				}
			}
		})
	}
}
