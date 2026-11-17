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
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestTemporalNumericConsumers(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	secToTime, err := types.ParseTime("02:50:03", 0)
	require.NoError(t, err)
	for _, tc := range []struct {
		name     string
		expr     string
		wantUint uint64
		wantTime *types.Time
	}{
		{name: "date bit and", expr: "date'2024-01-01' & 3", wantUint: 1},
		{name: "datetime bit or", expr: "cast('2024-01-01 10:00:00' as datetime) | 1", wantUint: 20240101100001},
		{name: "time bit xor", expr: "time'01:02:03' ^ 1", wantUint: 10202},
		{name: "date shift left", expr: "date'2024-01-01' << 1", wantUint: 40480202},
		{name: "time shift right", expr: "time'01:02:03' >> 1", wantUint: 5101},
		{name: "date bit count", expr: "bit_count(date'2024-01-01')", wantUint: 14},
		{name: "datetime bit count", expr: "bit_count(cast('2024-01-01 10:00:00' as datetime))", wantUint: 15},
		{name: "time bit count", expr: "bit_count(time'01:02:03')", wantUint: 10},
		{name: "time sec to time", expr: "sec_to_time(time'01:02:03')", wantTime: &secToTime},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select "+tc.expr, 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			bound, err := NewDefaultBinder(ctx, nil, nil, planpb.Type{}, nil).BindExpr(ast, 0, false)
			require.NoError(t, err)
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, bound, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			if tc.wantTime != nil {
				require.Equal(t, types.T_time, result.GetType().Oid)
				require.Equal(t, *tc.wantTime, vector.GetFixedAtNoTypeCheck[types.Time](result, 0))
			} else {
				require.Equal(t, types.T_uint64, result.GetType().Oid)
				require.Equal(t, tc.wantUint, vector.GetFixedAtNoTypeCheck[uint64](result, 0))
			}
		})
	}
}

func TestTemporalNumericConsumerColumns(t *testing.T) {
	date, err := types.ParseDateCast("2024-01-01")
	require.NoError(t, err)
	datetime, err := types.ParseDatetime("2024-01-01 10:00:00", 6)
	require.NoError(t, err)
	carryDatetime, err := types.ParseDatetime("2024-02-29 23:59:59.500000", 6)
	require.NoError(t, err)
	clockTime, err := types.ParseTime("01:02:03.000000", 6)
	require.NoError(t, err)
	negativeSubsecond, err := types.ParseTime("-00:00:00.500000", 6)
	require.NoError(t, err)
	negativeTime, err := types.ParseTime("-12:00:00.500000", 6)
	require.NoError(t, err)
	negativeSecondInteger := int64(-120001)
	negativeSubsecondInteger := int64(-1)

	for _, tc := range []struct {
		name         string
		function     string
		typ          types.Type
		values       any
		nulls        []bool
		constant     int64
		sourceArg    int
		zoneOffset   int
		want         []uint64
		wantTemporal bool
	}{
		{
			name: "date left bit and", function: "&", typ: types.T_date.ToType(),
			values: []types.Date{date, types.ZeroDate, 0, 0}, nulls: []bool{false, false, false, true},
			constant: 3, sourceArg: 0, want: []uint64{1, 0, 1, 0}, wantTemporal: true,
		},
		{
			name: "date right bit and", function: "&", typ: types.T_date.ToType(),
			values: []types.Date{date, types.ZeroDate, 0, 0}, nulls: []bool{false, false, false, true},
			constant: 3, sourceArg: 1, want: []uint64{1, 0, 1, 0}, wantTemporal: true,
		},
		{
			name: "date bit count", function: "bit_count", typ: types.T_date.ToType(),
			values: []types.Date{date, types.ZeroDate, 0, 0}, nulls: []bool{false, false, false, true},
			sourceArg: 0, want: []uint64{14, 0, 9, 0}, wantTemporal: true,
		},
		{
			name: "date shift left", function: "<<", typ: types.T_date.ToType(),
			values: []types.Date{date, types.ZeroDate, 0, 0}, nulls: []bool{false, false, false, true},
			constant: 1, sourceArg: 0, want: []uint64{40480202, 0, 20202, 0}, wantTemporal: true,
		},
		{
			name: "datetime bit or", function: "|", typ: types.T_datetime.ToTypeWithScale(6),
			values: []types.Datetime{datetime, carryDatetime, 0}, nulls: []bool{false, false, true},
			constant: 1, sourceArg: 0, want: []uint64{20240101100001, 20240301000001, 0}, wantTemporal: true,
		},
		{
			name: "datetime bit count", function: "bit_count", typ: types.T_datetime.ToTypeWithScale(6),
			values: []types.Datetime{datetime, carryDatetime, 0}, nulls: []bool{false, false, true},
			sourceArg: 0, want: []uint64{15, 20, 0}, wantTemporal: true,
		},
		{
			name: "signed fractional time bit xor", function: "^", typ: types.T_time.ToTypeWithScale(6),
			values: []types.Time{clockTime, negativeSubsecond, negativeTime, 0}, nulls: []bool{false, false, false, true},
			constant: 1, sourceArg: 0,
			want: []uint64{10202, uint64(negativeSubsecondInteger) ^ 1, uint64(negativeSecondInteger) ^ 1, 0}, wantTemporal: true,
		},
		{
			name: "signed fractional time bit count", function: "bit_count", typ: types.T_time.ToTypeWithScale(6),
			values: []types.Time{clockTime, negativeSubsecond, negativeTime, 0}, nulls: []bool{false, false, false, true},
			sourceArg: 0, want: []uint64{10, 64, 57, 0}, wantTemporal: true,
		},
		{
			name: "timestamp bit or UTC", function: "|", typ: types.T_timestamp.ToTypeWithScale(6),
			values: []types.Timestamp{datetime.ToTimestamp(time.UTC), carryDatetime.ToTimestamp(time.UTC), 0}, nulls: []bool{false, false, true},
			constant: 0, sourceArg: 0, want: []uint64{20240101100000, 20240301000000, 0}, wantTemporal: true,
		},
		{
			name: "timestamp bit count UTC", function: "bit_count", typ: types.T_timestamp.ToTypeWithScale(6),
			values: []types.Timestamp{datetime.ToTimestamp(time.UTC), carryDatetime.ToTimestamp(time.UTC), 0}, nulls: []bool{false, false, true},
			sourceArg: 0, want: []uint64{15, 20, 0}, wantTemporal: true,
		},
		{
			name: "timestamp bit or UTC plus eight", function: "|", typ: types.T_timestamp.ToTypeWithScale(6),
			values: []types.Timestamp{datetime.ToTimestamp(time.UTC), carryDatetime.ToTimestamp(time.UTC), 0}, nulls: []bool{false, false, true},
			constant: 0, sourceArg: 0, zoneOffset: 8 * 3600,
			want: []uint64{20240101180000, 20240301080000, 0}, wantTemporal: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.GetSessionInfo().TimeZone = time.FixedZone("test", tc.zoneOffset)
			source := temporalNumericColumn(tc.typ)
			args := []*Expr{source}
			if tc.function != "bit_count" {
				constant := makePlan2Int64ConstExprWithType(tc.constant)
				if tc.sourceArg == 0 {
					args = append(args, constant)
				} else {
					args = []*Expr{constant, source}
				}
			}
			bound, err := BindFuncExprImplByPlanExpr(proc.Ctx, tc.function, args)
			require.NoError(t, err)
			cast := bound.GetF().Args[tc.sourceArg].GetF()
			require.NotNil(t, cast)
			id, overload := function.DecodeOverloadID(cast.Func.Obj)
			require.Equal(t, int32(function.CAST), id)
			require.Equal(t, function.TemporalIntegerArgumentCastOverload, overload)
			require.NotNil(t, cast.Args[0].GetCol())

			input := newTemporalNumericColumnBatch(t, proc, tc.typ, tc.values, tc.nulls)
			defer input.Clean(proc.Mp())
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, bound, []*batch.Batch{input})
			require.NoError(t, err)
			defer free()
			require.Equal(t, types.T_uint64, result.GetType().Oid)
			for row, want := range tc.want {
				require.Equal(t, tc.nulls[row], result.IsNull(uint64(row)), "row %d", row)
				if !tc.nulls[row] {
					require.Equal(t, want, vector.GetFixedAtNoTypeCheck[uint64](result, row), "row %d", row)
				}
			}
		})
	}
}

func TestPreparedTemporalNumericConsumers(t *testing.T) {
	proc := testutil.NewProcess(t)
	prepared, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
		`prepare temporal_numeric_prepared from 'select ? & 3, 3 & ?, bit_count(?), sec_to_time(?)'`)
	require.NoError(t, err)
	original := prepared.GetDcl().GetPrepare().Plan
	snapshot := proto.Clone(original)
	firstTime, err := types.ParseTime("02:50:03.500000", 6)
	require.NoError(t, err)
	secondTime, err := types.ParseTime("00:00:01.500000", 6)
	require.NoError(t, err)

	for _, tc := range []struct {
		name     string
		values   []any
		wantUint []uint64
		wantTime types.Time
	}{
		{
			name: "native temporal parameters",
			values: []any{
				ParamValue{Value: "2024-01-01", RuntimeType: types.T_date.ToType(), HasRuntimeType: true, IsBinaryProtocol: true},
				ParamValue{Value: "2024-01-01", RuntimeType: types.T_date.ToType(), HasRuntimeType: true, IsBinaryProtocol: true},
				ParamValue{Value: "2024-01-01 10:00:00", RuntimeType: types.T_datetime.ToType(), HasRuntimeType: true, IsBinaryProtocol: true},
				ParamValue{Value: "01:02:03.500000", RuntimeType: types.T_time.ToTypeWithScale(6), HasRuntimeType: true, IsBinaryProtocol: true},
			},
			wantUint: []uint64{1, 1, 15}, wantTime: firstTime,
		},
		{
			name: "template reuse with new temporal parameters",
			values: []any{
				ParamValue{Value: "2024-01-02", RuntimeType: types.T_date.ToType(), HasRuntimeType: true, IsBinaryProtocol: true},
				ParamValue{Value: "2024-01-02", RuntimeType: types.T_date.ToType(), HasRuntimeType: true, IsBinaryProtocol: true},
				ParamValue{Value: "2024-01-02 00:00:00", RuntimeType: types.T_datetime.ToType(), HasRuntimeType: true, IsBinaryProtocol: true},
				ParamValue{Value: "00:00:01.500000", RuntimeType: types.T_time.ToTypeWithScale(6), HasRuntimeType: true, IsBinaryProtocol: true},
			},
			wantUint: []uint64{2, 2, 14}, wantTime: secondTime,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bound, changed, err := FillValuesOfParamsInPlanWithSpecialization(proc.Ctx, original, tc.values)
			require.NoError(t, err)
			require.True(t, changed)
			query := bound.GetQuery()
			root := query.Nodes[query.Steps[0]]
			require.Len(t, root.ProjectList, 4)
			for row, want := range tc.wantUint {
				result, free, err := colexec.GetReadonlyResultFromExpression(proc, root.ProjectList[row], []*batch.Batch{batch.EmptyForConstFoldBatch})
				require.NoError(t, err)
				gotType := result.GetType().Oid
				got := vector.GetFixedAtNoTypeCheck[uint64](result, 0)
				free()
				require.Equal(t, types.T_uint64, gotType)
				require.Equal(t, want, got)
			}
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, root.ProjectList[3], []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			gotScale := result.GetType().Scale
			gotTime := vector.GetFixedAtNoTypeCheck[types.Time](result, 0)
			free()
			require.Equal(t, int32(6), gotScale)
			require.Equal(t, tc.wantTime, gotTime)
			require.True(t, proto.Equal(snapshot, original), "execution specialization must leave the template unchanged")
		})
	}
}

type temporalNumericWarningSink struct {
	codes    []uint16
	messages []string
}

func (s *temporalNumericWarningSink) AppendWarningDiagnostic(code uint16, message string) {
	s.codes = append(s.codes, code)
	s.messages = append(s.messages, message)
}

func TestSecToTimeTemporalColumns(t *testing.T) {
	date, err := types.ParseDateCast("2024-01-01")
	require.NoError(t, err)
	datetime, err := types.ParseDatetime("2024-01-01 10:00:00.500000", 6)
	require.NoError(t, err)
	clockTime, err := types.ParseTime("01:02:03.500000", 6)
	require.NoError(t, err)
	negativeSubsecond, err := types.ParseTime("-00:00:00.500000", 6)
	require.NoError(t, err)
	negativeTime, err := types.ParseTime("-12:00:00.500000", 6)
	require.NoError(t, err)
	positiveResult, err := types.ParseTime("02:50:03.500000", 6)
	require.NoError(t, err)
	negativeSubsecondResult, err := types.ParseTime("-00:00:00.500000", 6)
	require.NoError(t, err)
	negativeResult, err := types.ParseTime("-33:20:00.500000", 6)
	require.NoError(t, err)

	for _, tc := range []struct {
		name         string
		typ          types.Type
		values       any
		nulls        []bool
		want         []types.Time
		wantScale    int32
		wantWarnings int
	}{
		{
			name: "date saturation and zero sentinels", typ: types.T_date.ToType(),
			values: []types.Date{date, types.ZeroDate, 0, 0}, nulls: []bool{false, false, false, true},
			want:      []types.Time{types.MySQLTimeFunctionMaxForScale(0), 0, types.Time(10101 * types.MicroSecsPerSec), 0},
			wantScale: 0, wantWarnings: 1,
		},
		{
			name: "fractional datetime saturation", typ: types.T_datetime.ToTypeWithScale(6),
			values: []types.Datetime{datetime, types.ZeroDatetime, 0}, nulls: []bool{false, false, true},
			want: []types.Time{types.MySQLTimeFunctionMax, 0, 0}, wantScale: 6, wantWarnings: 1,
		},
		{
			name: "signed fractional time", typ: types.T_time.ToTypeWithScale(6),
			values: []types.Time{clockTime, negativeSubsecond, negativeTime, 0, 0}, nulls: []bool{false, false, false, false, true},
			want: []types.Time{positiveResult, negativeSubsecondResult, negativeResult, 0, 0}, wantScale: 6,
		},
		{
			name: "timestamp saturation", typ: types.T_timestamp.ToTypeWithScale(6),
			values: []types.Timestamp{datetime.ToTimestamp(time.UTC), types.ZeroTimestamp, 0}, nulls: []bool{false, false, true},
			want: []types.Time{types.MySQLTimeFunctionMax, 0, 0}, wantScale: 6, wantWarnings: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.GetSessionInfo().TimeZone = time.UTC
			sink := &temporalNumericWarningSink{}
			proc.WarningSink = sink
			bound, err := BindFuncExprImplByPlanExpr(proc.Ctx, "sec_to_time", []*Expr{temporalNumericColumn(tc.typ)})
			require.NoError(t, err)
			require.Equal(t, types.T_time, types.T(bound.Typ.Id))
			require.Equal(t, tc.wantScale, bound.Typ.Scale)
			input := newTemporalNumericColumnBatch(t, proc, tc.typ, tc.values, tc.nulls)
			defer input.Clean(proc.Mp())
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, bound, []*batch.Batch{input})
			require.NoError(t, err)
			defer free()
			for row, want := range tc.want {
				require.Equal(t, tc.nulls[row], result.IsNull(uint64(row)), "row %d", row)
				if !tc.nulls[row] {
					require.Equal(t, want, vector.GetFixedAtNoTypeCheck[types.Time](result, row), "row %d", row)
				}
			}
			require.Len(t, sink.codes, tc.wantWarnings)
			for _, code := range sink.codes {
				require.Equal(t, uint16(moerr.ER_TRUNCATED_WRONG_VALUE), code)
			}
			for _, message := range sink.messages {
				require.Contains(t, message, "Truncated incorrect time value")
			}
		})
	}
}

func temporalNumericColumn(typ types.Type) *Expr {
	return &Expr{Typ: typ.PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
}

func newTemporalNumericColumnBatch(t *testing.T, proc *process.Process, typ types.Type, values any, nulls []bool) *batch.Batch {
	t.Helper()
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(typ)
	var err error
	switch values := values.(type) {
	case []types.Date:
		err = vector.AppendFixedList(input.Vecs[0], values, nulls, proc.Mp())
	case []types.Datetime:
		err = vector.AppendFixedList(input.Vecs[0], values, nulls, proc.Mp())
	case []types.Time:
		err = vector.AppendFixedList(input.Vecs[0], values, nulls, proc.Mp())
	case []types.Timestamp:
		err = vector.AppendFixedList(input.Vecs[0], values, nulls, proc.Mp())
	default:
		t.Fatalf("unsupported temporal test values %T", values)
	}
	require.NoError(t, err)
	input.SetRowCount(len(nulls))
	return input
}
