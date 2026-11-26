// Copyright 2021 Matrix Origin
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

package compile

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type remoteWarningSession struct {
	warnings []struct {
		code uint16
		msg  string
	}
	totalWarnings uint64
}

func (*remoteWarningSession) GetTempTable(string, string) (string, bool) { return "", false }
func (*remoteWarningSession) AddTempTable(string, string, string)        {}
func (*remoteWarningSession) RemoveTempTable(string, string)             {}
func (*remoteWarningSession) RemoveTempTableByRealName(string)           {}
func (*remoteWarningSession) GetSqlModeNoAutoValueOnZero() (bool, bool)  { return false, false }
func (s *remoteWarningSession) AppendWarningDiagnostic(code uint16, msg string) {
	s.totalWarnings++
	s.warnings = append(s.warnings, struct {
		code uint16
		msg  string
	}{code: code, msg: msg})
}
func (s *remoteWarningSession) AppendWarningBatch(total uint64, codes []uint16, messages []string) {
	s.totalWarnings += total
	for i := 0; i < len(codes) && i < len(messages); i++ {
		s.warnings = append(s.warnings, struct {
			code uint16
			msg  string
		}{code: codes[i], msg: messages[i]})
	}
}

func TestRemoteNumericCastWarningAppearsAtExecution(t *testing.T) {
	proc := testutil.NewProcess(t)
	session := &remoteWarningSession{}
	proc.Session = session
	proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		if name == "s" && !system {
			return "12abc", nil
		}
		return nil, moerr.NewInternalErrorNoCtx("variable not found")
	})

	variable := makeTestVarExpr("s")
	variable.GetV().System = false
	targetType := types.T_float64.ToType()
	cast, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "cast", []*plan.Expr{
		variable,
		{
			Typ:  plan2.MakePlan2Type(&targetType),
			Expr: &plan.Expr_T{T: &plan.TargetType{}},
		},
	})
	require.NoError(t, err)

	folded, err := foldVarExprsInExprInPlace(cast, proc)
	require.NoError(t, err)
	require.True(t, folded)
	require.Empty(t, session.warnings)

	input := batch.NewWithSize(1)
	dummy := vector.NewVec(types.T_int64.ToType(), proc.Mp())
	require.NoError(t, vector.AppendFixed(dummy, int64(1), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(dummy, int64(2), false, proc.Mp()))
	defer dummy.Free(proc.Mp())
	input.Vecs[0] = dummy
	input.SetRowCount(2)
	vec, free, err := colexec.GetReadonlyResultFromExpression(proc, cast, []*batch.Batch{input})
	require.NoError(t, err)
	require.Equal(t, 2, vec.Length())
	free()
	require.Len(t, session.warnings, 2)
	require.Equal(t, moerr.ER_TRUNCATED_WRONG_VALUE, session.warnings[0].code)
	require.Contains(t, session.warnings[0].msg, "12abc")
}

func TestMixedTemporalConditionalPreservesFractionalValues(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.TimeZone = time.UTC
	condition := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	timestamp, err := types.ParseTimestamp(time.UTC, "2024-01-02 12:34:56.123456", 6)
	require.NoError(t, err)
	datetime, err := types.ParseDatetime("2024-01-02 12:34:56.654", 3)
	require.NoError(t, err)
	value := func(oid types.T, value int64, scale int32) *plan.Expr {
		result := &plan.Expr{
			Typ:  plan.Type{Id: int32(oid), Width: scale, Scale: scale},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{}},
		}
		switch oid {
		case types.T_timestamp:
			result.GetLit().Value = &plan.Literal_Timestampval{Timestampval: value}
		case types.T_datetime:
			result.GetLit().Value = &plan.Literal_Datetimeval{Datetimeval: value}
		}
		return result
	}
	expr, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "if", []*plan.Expr{
		condition,
		value(types.T_timestamp, int64(timestamp), 6),
		value(types.T_datetime, int64(datetime), 3),
	})
	require.NoError(t, err)
	require.Equal(t, types.T_datetime, types.T(expr.Typ.Id))
	require.Equal(t, int32(6), expr.Typ.Scale)

	executor, err := colexec.NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	defer executor.Free()
	input := batch.NewWithSize(1)
	conditions := vector.NewVec(types.T_bool.ToType(), proc.Mp())
	require.NoError(t, vector.AppendFixed(conditions, true, false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(conditions, false, false, proc.Mp()))
	input.Vecs[0] = conditions
	input.SetRowCount(2)
	defer conditions.Free(proc.Mp())

	result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, types.T_datetime, result.GetType().Oid)
	require.Equal(t, int32(6), result.GetType().Scale)
	require.Equal(t, "2024-01-02 12:34:56.123456",
		types.Datetime(vector.GetFixedAtNoTypeCheck[types.Datetime](result, 0)).String2(6))
	require.Equal(t, "2024-01-02 12:34:56.654000",
		types.Datetime(vector.GetFixedAtNoTypeCheck[types.Datetime](result, 1)).String2(6))
}

func TestRemoteSecToTimeConversionWarningsRemainBounded(t *testing.T) {
	proc := testutil.NewProcess(t)
	collector := &remoteWarningCollector{maxRetained: 3}
	proc.Session = collector
	sourceType := types.T_varchar.ToType()
	source := &plan.Expr{
		Typ:  plan2.MakePlan2Type(&sourceType),
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}},
	}
	expr, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "sec_to_time", []*plan.Expr{source})
	require.NoError(t, err)
	executor, err := colexec.NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	defer executor.Free()

	input := batch.NewWithSize(1)
	const longWarningRows = 64
	longInvalid := []byte(strings.Repeat("x", 65535))
	inputValues := make([][]byte, 0, longWarningRows+2)
	inputValues = append(inputValues, longInvalid, []byte("foo"), []byte("3020400x"))
	for range longWarningRows - 1 {
		inputValues = append(inputValues, longInvalid)
	}
	values := testutil.MakeVarlenaVector(inputValues, nil, sourceType, proc.Mp())
	defer values.Free(proc.Mp())
	input.Vecs[0] = values
	input.SetRowCount(len(inputValues))
	result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, len(inputValues), result.Length())

	total, retained := collector.SnapshotWarnings()
	// The collector observes every bad row while retaining only its fixed-size
	// SHOW WARNINGS payload. The first retained record is the long value, so the
	// message-size assertion below also covers source-side rendering.
	require.Equal(t, uint64(longWarningRows+3), total)
	require.Len(t, retained, 3)
	for _, warning := range retained {
		require.Equal(t, moerr.ER_TRUNCATED_WRONG_VALUE, warning.Code)
		require.LessOrEqual(t, len(warning.Message), len("Truncated incorrect DECIMAL value: ''")+128)
	}
	require.Len(t, retained[0].Message, len("Truncated incorrect DECIMAL value: ''")+128)

	// The terminal envelope preserves the complete count and the bounded SHOW
	// WARNINGS payload for the initiating session.
	data, err := json.Marshal(remoteTerminalEnvelope{WarningCount: total, WarningDiagnostics: retained})
	require.NoError(t, err)
	initiatingSession := &remoteWarningSession{}
	require.NoError(t, (&messageSenderOnClient{warningSink: initiatingSession}).dealRemoteTerminal(data))
	require.Equal(t, uint64(longWarningRows+3), initiatingSession.totalWarnings)
	require.Len(t, initiatingSession.warnings, 3)
}

func TestRemoteWarningCollectorConfiguredRetentionPrefixAndZero(t *testing.T) {
	for _, tc := range []struct {
		name  string
		limit int
		want  int
	}{
		{name: "zero", limit: 0, want: 0},
		{name: "prefix", limit: 3, want: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			collector := &remoteWarningCollector{
				maxRetained:    tc.limit,
				maxRetainedSet: true,
			}
			collector.AppendWarningBatch(5,
				[]uint16{1, 2, 3, 4, 5},
				[]string{"one", "two", "three", "four", "five"})
			total, warnings := collector.SnapshotWarnings()
			require.Equal(t, uint64(5), total)
			require.Len(t, warnings, tc.want)
			for i := range warnings {
				require.Equal(t, uint16(i+1), warnings[i].Code)
			}
		})
	}
}

func TestRemoteWarningCollectorRespectsProcessNarrowedBudget(t *testing.T) {
	proc := &process.Process{Base: &process.BaseProcess{
		Lim: process.Limitation{Size: 1024},
	}}
	collector := &remoteWarningCollector{
		maxRetained:    int(^uint16(0)),
		maxRetainedSet: true,
		warningBudget:  process.WarningDiagnosticBudgetForProcess(proc),
	}
	proc.Session = collector

	longMessage := strings.Repeat("x", 2000)
	collector.AppendWarningDiagnostic(1000, longMessage)
	collector.AppendWarningDiagnostic(1001, "later")

	require.Equal(t, uint64(1024), collector.warningBudget.Limit())
	require.Equal(t, uint64(2), collector.warningCount)
	require.Empty(t, collector.warnings)
	require.LessOrEqual(t, collector.warningChargeBytes, uint64(1024))
	collector.closeWarnings(false)
	require.Zero(t, collector.warningBudget.Used())
}

func TestRemoteNumericCastWarningCountIsIndependentOfBatching(t *testing.T) {
	buildCast := func(proc *process.Process) *plan.Expr {
		proc.Session = &remoteWarningSession{}
		proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
			if name == "s" && !system {
				return "12abc", nil
			}
			return nil, moerr.NewInternalErrorNoCtx("variable not found")
		})

		variable := makeTestVarExpr("s")
		variable.GetV().System = false
		targetType := types.T_float64.ToType()
		cast, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "cast", []*plan.Expr{
			variable,
			{
				Typ:  plan2.MakePlan2Type(&targetType),
				Expr: &plan.Expr_T{T: &plan.TargetType{}},
			},
		})
		require.NoError(t, err)
		folded, err := foldVarExprsInExprInPlace(cast, proc)
		require.NoError(t, err)
		require.True(t, folded)
		return cast
	}

	run := func(layout []int) uint64 {
		proc := testutil.NewProcess(t)
		cast := buildCast(proc)
		executor, err := colexec.NewExpressionExecutor(proc, cast)
		require.NoError(t, err)
		defer executor.Free()

		for _, rows := range layout {
			input := batch.NewWithSize(1)
			input.Vecs[0] = testutil.MakeInt64Vector(make([]int64, rows), nil, proc.Mp())
			input.SetRowCount(rows)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, rows, result.Length())
			input.Vecs[0].Free(proc.Mp())
		}
		session := proc.Session.(*remoteWarningSession)
		return session.totalWarnings
	}

	// A single two-row batch and two one-row batches represent the same
	// logical result. Constant folding must not make their diagnostics differ.
	require.Equal(t, uint64(2), run([]int{2}))
	require.Equal(t, uint64(2), run([]int{1, 1}))
	require.Equal(t, uint64(0), run([]int{0}))
}

func TestRemoteNumericCoercionWarningsFollowEvaluatedRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	session := &remoteWarningSession{}
	proc.Session = session
	sourceType := types.T_text.ToType()
	source := &plan.Expr{
		Typ:  plan2.MakePlan2Type(&sourceType),
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}},
	}
	targetType := types.T_float64.ToType()
	cast, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "cast", []*plan.Expr{
		source,
		{
			Typ:  plan2.MakePlan2Type(&targetType),
			Expr: &plan.Expr_T{T: &plan.TargetType{}},
		},
	})
	require.NoError(t, err)

	input := batch.NewWithSize(1)
	values := testutil.MakeVarlenaVector(
		[][]byte{[]byte("12abc"), []byte("12abc")}, nil,
		types.T_text.ToType(), proc.Mp())
	defer values.Free(proc.Mp())
	input.Vecs[0] = values
	input.SetRowCount(2)
	vec, free, err := colexec.GetReadonlyResultFromExpression(proc, cast, []*batch.Batch{input})
	require.NoError(t, err)
	require.Equal(t, 2, vec.Length())
	free()
	require.Len(t, session.warnings, 2)

	session.warnings = nil
	input.SetRowCount(0)
	vec, free, err = colexec.GetReadonlyResultFromExpression(proc, cast, []*batch.Batch{input})
	require.NoError(t, err)
	require.Equal(t, 0, vec.Length())
	free()
	require.Empty(t, session.warnings)
}

func TestFoldVarExprRemoteNumericCastSkipsUnselectedBranchWarning(t *testing.T) {
	proc := testutil.NewProcess(t)
	session := &remoteWarningSession{}
	proc.Session = session
	proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		if name == "s" && !system {
			return "12abc", nil
		}
		return nil, moerr.NewInternalErrorNoCtx("variable not found")
	})

	variable := makeTestVarExpr("s")
	variable.GetV().System = false
	targetType := types.T_float64.ToType()
	cast, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "cast", []*plan.Expr{
		variable,
		{
			Typ:  plan2.MakePlan2Type(&targetType),
			Expr: &plan.Expr_T{T: &plan.TargetType{}},
		},
	})
	require.NoError(t, err)
	zeroType := types.T_int64.ToType()
	zero := &plan.Expr{
		Typ: plan2.MakePlan2Type(&zeroType),
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_I64Val{I64Val: 0},
		}},
	}
	condition := makeTestConstBoolExpr(false)
	iff, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "if", []*plan.Expr{
		condition, cast, zero,
	})
	require.NoError(t, err)

	folded, err := foldVarExprsInExprInPlace(iff, proc)
	require.NoError(t, err)
	require.True(t, folded)
	input := batch.NewWithSize(1)
	input.SetRowCount(2)
	vec, free, err := colexec.GetReadonlyResultFromExpression(proc, iff, []*batch.Batch{input})
	require.NoError(t, err)
	require.Equal(t, 2, vec.Length())
	free()
	require.Empty(t, session.warnings)
}

func TestRemoteTerminalWarningsAreForwardedToInitiatingSession(t *testing.T) {
	session := &remoteWarningSession{}
	sender := &messageSenderOnClient{warningSink: session}
	data, err := json.Marshal(remoteTerminalEnvelope{
		WarningCount: 2,
		WarningDiagnostics: []remoteWarningDiagnostic{
			{Code: moerr.ER_TRUNCATED_WRONG_VALUE, Message: "first"},
			{Code: moerr.ER_TRUNCATED_WRONG_VALUE, Message: "second"},
		},
	})
	require.NoError(t, err)
	require.NoError(t, sender.dealRemoteTerminal(data))
	require.Len(t, session.warnings, 2)
	require.Equal(t, uint64(2), session.totalWarnings)
	require.Equal(t, "first", session.warnings[0].msg)
	require.Equal(t, "second", session.warnings[1].msg)
}

func TestRemoteTerminalWarningsStayWithCapturedAttemptSink(t *testing.T) {
	proc := &process.Process{Base: &process.BaseProcess{}}
	oldSink := &remoteWarningCollector{}
	newSink := &remoteWarningCollector{}
	proc.Session = &remoteWarningSession{}
	proc.WarningSink = oldSink
	sender := &messageSenderOnClient{proc: proc, warningSink: oldSink}
	data, err := json.Marshal(remoteTerminalEnvelope{
		WarningCount: 1,
		WarningDiagnostics: []remoteWarningDiagnostic{
			{Code: 1062, Message: "duplicate"},
		},
	})
	require.NoError(t, err)

	// A retry replaces the process's current sink, but the old remote sender
	// must continue to target the sink captured for its original attempt.
	proc.WarningSink = newSink
	require.NoError(t, sender.dealRemoteTerminal(data))
	total, records := oldSink.SnapshotWarnings()
	require.Equal(t, uint64(1), total)
	require.Len(t, records, 1)
	newTotal, newRecords := newSink.SnapshotWarnings()
	require.Zero(t, newTotal)
	require.Empty(t, newRecords)

	// Once the failed attempt is sealed, a late terminal is rejected by the
	// captured collector rather than falling through to Session.
	oldSink.closeWarnings(false)
	late := &messageSenderOnClient{warningSink: oldSink}
	require.NoError(t, late.dealRemoteTerminal(data))
	total, records = oldSink.SnapshotWarnings()
	require.Equal(t, uint64(0), total)
	require.Empty(t, records)
}

func TestRemoteWarningCollectorBoundsRetention(t *testing.T) {
	collector := &remoteWarningCollector{maxRetained: 3}
	for i := 0; i < 1000; i++ {
		collector.AppendWarningDiagnostic(1292, "truncated")
	}

	total, retained := collector.SnapshotWarnings()
	require.Equal(t, uint64(1000), total)
	require.Len(t, retained, 3)

	data, err := json.Marshal(remoteTerminalEnvelope{
		WarningCount:       total,
		WarningDiagnostics: retained,
	})
	require.NoError(t, err)
	require.Less(t, len(data), 1024)
}

func TestRemoteWarningCollectorLegacyPayloadFallsBackTo64(t *testing.T) {
	collector := &remoteWarningCollector{}
	for i := 0; i < remoteWarningRetentionLimit+1; i++ {
		collector.AppendWarningDiagnostic(1292, "legacy")
	}
	total, retained := collector.SnapshotWarnings()
	require.Equal(t, uint64(remoteWarningRetentionLimit+1), total)
	require.Len(t, retained, remoteWarningRetentionLimit)
}

func TestRemoteTerminalEnvelopeByteBudgetCapsMaxCapacity(t *testing.T) {
	const total = 65535
	message := strings.Repeat("x", process.WarningDiagnosticMaxMessageBytes)
	warnings := make([]remoteWarningDiagnostic, total)
	for i := range warnings {
		warnings[i] = remoteWarningDiagnostic{Code: uint16(i), Message: message}
	}

	data, err := marshalRemoteTerminalEnvelope(remoteTerminalEnvelope{
		WarningCount:       total,
		WarningDiagnostics: warnings,
	}, 64*1024)
	require.NoError(t, err)
	require.LessOrEqual(t, len(data), 64*1024)

	var envelope remoteTerminalEnvelope
	require.NoError(t, json.Unmarshal(data, &envelope))
	require.Equal(t, uint64(total), envelope.WarningCount)
	require.NotEmpty(t, envelope.WarningDiagnostics)
	require.Less(t, len(envelope.WarningDiagnostics), total)
	for i, warning := range envelope.WarningDiagnostics {
		require.Equal(t, uint16(i), warning.Code)
		require.Equal(t, message, warning.Message)
	}
}

func TestRemoteWarningCollectorMergesDescendantCountsAndRecords(t *testing.T) {
	collector := &remoteWarningCollector{maxRetained: 2}
	collector.AppendWarningBatch(100, []uint16{1, 2, 3}, []string{"a", "b", "c"})
	collector.AppendWarningBatch(50, []uint16{4}, []string{"d"})

	total, retained := collector.SnapshotWarnings()
	require.Equal(t, uint64(150), total)
	require.Len(t, retained, 2)
	require.Equal(t, uint16(1), retained[0].Code)
	require.Equal(t, uint16(2), retained[1].Code)
}

func TestRemoteWarningCollectorSaturatesCount(t *testing.T) {
	collector := &remoteWarningCollector{}
	collector.AppendWarningBatch(^uint64(0)-1, nil, nil)
	collector.AppendWarningBatch(2, nil, nil)
	total, retained := collector.SnapshotWarnings()
	require.Equal(t, ^uint64(0), total)
	require.Empty(t, retained)
}

func TestRemoteWarningCollectorBoundsMessageBytes(t *testing.T) {
	collector := &remoteWarningCollector{}
	collector.AppendWarningDiagnostic(1292, strings.Repeat("界", process.WarningDiagnosticMaxMessageBytes*2))
	_, retained := collector.SnapshotWarnings()
	require.Len(t, retained, 1)
	require.LessOrEqual(t, len(retained[0].Message), process.WarningDiagnosticMaxMessageBytes)
	require.Contains(t, retained[0].Message, "truncated")
}

func TestRemoteWarningCollectorMaxErrorCountUsesStatementBudget(t *testing.T) {
	collector := &remoteWarningCollector{
		maxRetained:    int(^uint16(0)),
		maxRetainedSet: true,
		warningBudget:  process.NewWarningDiagnosticBudget(process.WarningDiagnosticMaxBytes),
	}
	longMessage := strings.Repeat("x", process.WarningDiagnosticMaxMessageBytes*2)
	for i := 0; i < int(^uint16(0)); i++ {
		collector.AppendWarningDiagnostic(1292, longMessage)
	}

	total, retained := collector.SnapshotWarnings()
	require.Equal(t, uint64(^uint16(0)), total)
	require.NotEmpty(t, retained)
	require.Less(t, len(retained), int(^uint16(0)))
	require.LessOrEqual(t, collector.warningBytes, process.WarningDiagnosticMaxBytes)
	require.LessOrEqual(t, collector.warningChargeBytes, uint64(process.WarningDiagnosticMaxBytes))
	require.LessOrEqual(t, collector.warningBudget.Used(), uint64(process.WarningDiagnosticMaxBytes))
	collector.closeWarnings(false)
	require.Zero(t, collector.warningBudget.Used())
}

func TestRemoteWarningCollectorRejectsUnderchargedSameBudgetTransfer(t *testing.T) {
	messages := []string{"first remote warning", "later remote warning"}
	firstCharge := process.WarningDiagnosticRecordBytes(messages[0])
	accounted := firstCharge + process.WarningDiagnosticRecordBytes(messages[1])
	sourceCharge := accounted - 1
	otherCharge := uint64(7)
	budget := process.NewWarningDiagnosticBudget(otherCharge + sourceCharge)
	require.True(t, budget.Reserve(otherCharge))
	require.True(t, budget.Reserve(sourceCharge))
	collector := &remoteWarningCollector{
		maxRetained:    1,
		maxRetainedSet: true,
		warningBudget:  budget,
	}

	require.True(t, process.AppendWarningBatchToSinkOwned(
		collector, 2, []uint16{1292, 1292}, messages, budget, sourceCharge))
	total, retained := collector.SnapshotWarnings()
	require.Equal(t, uint64(2), total)
	require.Len(t, retained, 1)
	require.Equal(t, messages[0], retained[0].Message)
	require.Equal(t, otherCharge+firstCharge, budget.Used())

	collector.closeWarnings(false)
	require.Equal(t, otherCharge, budget.Used())
	budget.Release(otherCharge)
	require.Zero(t, budget.Used())
}

func TestRemoteWarningCollectorDoesNotRetainMoreRecordsThanTotal(t *testing.T) {
	collector := &remoteWarningCollector{}
	collector.AppendWarningBatch(1, []uint16{1292, 1292}, []string{"first", "second"})
	total, retained := collector.SnapshotWarnings()
	require.Equal(t, uint64(1), total)
	require.Len(t, retained, 1)
	collector.AppendWarningDiagnostic(1292, "next")
	total, retained = collector.SnapshotWarnings()
	require.Equal(t, uint64(2), total)
	require.Len(t, retained, 2)
	collector.AppendWarningBatch(0, []uint16{1292}, []string{"ignored"})
	total, retained = collector.SnapshotWarnings()
	require.Equal(t, uint64(2), total)
	require.Len(t, retained, 2)
}

func TestScopeContainsVarExpr(t *testing.T) {
	scope := newScope(Normal)
	proj := projection.NewArgument()
	proj.ProjectList = []*plan.Expr{makeTestVarExpr("sql_mode")}
	f := filter.NewArgument()
	f.FilterExprs = []*plan.Expr{makeTestConstBoolExpr(true)}
	f.AppendChild(proj)
	scope.setRootOperator(f)

	require.True(t, scopeContainsVarExpr(scope))
}

func TestAssertFilterRemoteRoundTrip(t *testing.T) {
	ctx := &scopeContext{
		id:     1,
		root:   &scopeContext{},
		parent: &scopeContext{},
	}
	proc := testutil.NewProcess(t)
	original := filter.NewArgument()
	original.FilterExprs = []*plan.Expr{makeTestConstBoolExpr(true)}
	original.IsAssert = true

	_, wire, err := convertToPipelineInstruction(original, proc, ctx, 1)
	require.NoError(t, err)
	require.True(t, wire.FilterIsAssert)

	payload, err := wire.Marshal()
	require.NoError(t, err)
	wireRoundTrip := new(pipeline.Instruction)
	require.NoError(t, wireRoundTrip.Unmarshal(payload))
	require.True(t, wireRoundTrip.FilterIsAssert)

	restored, err := convertToVmOperator(wireRoundTrip, ctx, nil)
	require.NoError(t, err)
	require.True(t, restored.(*filter.Filter).IsAssert)
}

func TestAggregateConfigTypeRemoteRoundTrip(t *testing.T) {
	expected := aggexec.MakeAggFunctionExpression(
		aggexec.AggIdOfGroupConcat,
		true,
		[]*plan.Expr{makeTestVarExpr("value")},
		[]byte{1, 2, 3},
		plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
	)

	wire := convertToPipelineAggregates([]aggexec.AggFuncExecExpression{expected})
	require.Len(t, wire, 1)
	require.Equal(
		t,
		plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
		wire[0].ConfigType,
	)

	actual := convertToAggregates(wire)
	require.Len(t, actual, 1)
	require.Equal(t, expected.GetExtraConfig(), actual[0].GetExtraConfig())
	require.Equal(t, expected.GetConfigType(), actual[0].GetConfigType())
}

func TestScopeContainsVarExprInAggArguments(t *testing.T) {
	scope := newScope(Normal)
	op := group.NewArgument()
	op.Aggs = []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(function.AggSumOverloadID, false, []*plan.Expr{makeTestVarExpr("sql_mode")}, nil),
	}
	scope.setRootOperator(op)

	require.True(t, scopeContainsVarExpr(scope))
}

func TestScopeContainsVarExprInLockRows(t *testing.T) {
	scope := newScope(Normal)
	op := lockop.NewArgumentByEngine(nil)
	op.AddLockTarget(1, nil, 0, types.T_int64.ToType(), -1, -1, makeTestVarExpr("sql_mode"), false)
	scope.setRootOperator(op)

	require.True(t, scopeContainsVarExpr(scope))
}

func TestFoldVarExprsInScope(t *testing.T) {
	proc := newResolveVariableProcess(t, "STRICT_TRANS_TABLES")
	scope := newScope(Normal)
	scope.DataSource = &Source{
		FilterList: []*plan.Expr{makeTestVarExpr("sql_mode")},
	}

	folded, err := foldVarExprsInScope(scope, proc)
	require.NoError(t, err)
	require.True(t, folded)
	require.False(t, scopeContainsVarExpr(scope))

	lit, ok := scope.DataSource.FilterList[0].Expr.(*plan.Expr_Lit)
	require.True(t, ok)
	require.Equal(t, "STRICT_TRANS_TABLES", lit.Lit.GetSval())
}

func TestFoldVarExprsInScopeUsesPrivateExprCopies(t *testing.T) {
	shared := makeTestVarExpr("sql_mode")
	proc1 := newResolveVariableProcess(t, "ANSI")
	proc2 := newResolveVariableProcess(t, "TRADITIONAL")

	scope1 := newScope(Normal)
	proj1 := projection.NewArgument()
	proj1.ProjectList = []*plan.Expr{shared}
	scope1.setRootOperator(proj1)

	scope2 := newScope(Normal)
	proj2 := projection.NewArgument()
	proj2.ProjectList = []*plan.Expr{shared}
	scope2.setRootOperator(proj2)

	folded, err := foldVarExprsInScope(scope1, proc1)
	require.NoError(t, err)
	require.True(t, folded)
	folded, err = foldVarExprsInScope(scope2, proc2)
	require.NoError(t, err)
	require.True(t, folded)

	require.IsType(t, &plan.Expr_V{}, shared.Expr)
	require.NotSame(t, shared, proj1.ProjectList[0])
	require.NotSame(t, shared, proj2.ProjectList[0])
	require.NotSame(t, proj1.ProjectList[0], proj2.ProjectList[0])
	require.Equal(t, "ANSI", proj1.ProjectList[0].GetLit().GetSval())
	require.Equal(t, "TRADITIONAL", proj2.ProjectList[0].GetLit().GetSval())
}

func TestFoldVarExprsInRemoteRunScopeDoesNotMutateReusableScope(t *testing.T) {
	shared := makeTestVarExpr("sql_mode")
	proc1 := newResolveVariableProcess(t, "ANSI")
	proc2 := newResolveVariableProcess(t, "TRADITIONAL")

	scope := newScope(Remote)
	proj := projection.NewArgument()
	proj.ProjectList = []*plan.Expr{shared}
	scope.setRootOperator(proj)

	remoteScope1, folded, err := foldVarExprsInRemoteRunScope(scope, proc1)
	require.NoError(t, err)
	require.True(t, folded)
	remoteProj1 := remoteScope1.RootOp.(*projection.Projection)
	require.Equal(t, "ANSI", remoteProj1.ProjectList[0].GetLit().GetSval())

	require.True(t, scopeContainsVarExpr(scope))
	require.IsType(t, &plan.Expr_V{}, shared.Expr)
	require.Same(t, shared, proj.ProjectList[0])

	remoteScope2, folded, err := foldVarExprsInRemoteRunScope(scope, proc2)
	require.NoError(t, err)
	require.True(t, folded)
	remoteProj2 := remoteScope2.RootOp.(*projection.Projection)
	require.Equal(t, "TRADITIONAL", remoteProj2.ProjectList[0].GetLit().GetSval())

	require.NotSame(t, scope, remoteScope1)
	require.NotSame(t, scope.RootOp, remoteScope1.RootOp)
	require.NotSame(t, remoteScope1.RootOp, remoteScope2.RootOp)
	require.NotSame(t, remoteProj1.ProjectList[0], remoteProj2.ProjectList[0])
	require.True(t, scopeContainsVarExpr(scope))
}

func TestCopyBlockFiltersForRemoteRunUsesAdmittedSubset(t *testing.T) {
	admitted := plan2.MakePlan2Int64ConstExprWithType(7)
	staleFold := plan2.MakePlan2Int64ConstExprWithType(8)
	scope := &Scope{DataSource: &Source{
		node:               &plan.Node{BlockFilterList: []*plan.Expr{admitted}},
		remoteBlockFilters: []*plan.Expr{admitted},
		BlockFilterList:    []*plan.Expr{staleFold},
	}}

	remote := copyBlockFiltersForRemoteRun(scope)
	require.NotSame(t, scope, remote)
	require.Equal(t, int64(7), remote.DataSource.BlockFilterList[0].GetLit().GetI64Val())
	require.NotSame(t, admitted, remote.DataSource.BlockFilterList[0])
	require.Same(t, staleFold, scope.DataSource.BlockFilterList[0])

	scope.DataSource.remoteBlockFilters = []*plan.Expr{}
	remote = copyBlockFiltersForRemoteRun(scope)
	require.NotSame(t, scope, remote)
	require.Empty(t, remote.DataSource.BlockFilterList)
	require.Same(t, staleFold, scope.DataSource.BlockFilterList[0])
}

func TestFoldRemoteJSONVariablePreservesTypeAndBinding(t *testing.T) {
	proc := testutil.NewProcess(t)
	var value any
	proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		require.Equal(t, "json_value", name)
		require.False(t, system)
		return value, nil
	})
	variable := makeTestVarExprWithType("json_value", types.T_json.ToType())
	variable.GetV().System = false
	scope := newScope(Remote)
	proj := projection.NewArgument()
	proj.ProjectList = []*plan.Expr{variable}
	scope.setRootOperator(proj)

	worker := testutil.NewProcess(t)
	worker.SetResolveVariableFunc(nil)
	input := batch.NewWithSize(0)
	input.SetRowCount(3)
	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{"object", `{"a":1}`, `{"a": 1}`},
		{"array", `[2,true,null]`, `[2, true, null]`},
		{"string", `"true"`, `"true"`},
		{"boolean", `true`, `true`},
		{"number", `12.34`, `12.34`},
		{"nested_float", `{"a":[1.0,18446744073709551615]}`, `{"a": [1.0, 18446744073709551615]}`},
		{"json_null", `null`, `null`},
		{"sql_null", nil, ""},
		{"rebound_object", `{"a":2}`, `{"a": 2}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value = tc.value
			remote, changed, err := foldVarExprsInRemoteRunScope(scope, proc)
			require.NoError(t, err)
			require.True(t, changed, "JSON variable must be folded for remote execution")
			require.False(t, scopeContainsVarExpr(remote))
			require.True(t, scopeContainsVarExpr(scope), "reusable scope must retain the variable")
			require.Same(t, variable, proj.ProjectList[0])
			expr := remote.RootOp.(*projection.Projection).ProjectList[0]
			require.Equal(t, int32(types.T_json), expr.Typ.Id)
			require.Equal(t, uint32(types.StringSourceUserVariable)+1, expr.GetLit().StringSource)

			wire, err := expr.Marshal()
			require.NoError(t, err)
			decoded := new(plan.Expr)
			require.NoError(t, decoded.Unmarshal(wire))
			exec, err := colexec.NewExpressionExecutor(worker, decoded)
			require.NoError(t, err)
			defer exec.Free()
			vec, err := exec.Eval(worker, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, types.T_json, vec.GetType().Oid)
			require.True(t, vec.IsConst())
			require.Equal(t, 3, vec.Length())
			if tc.value == nil {
				require.True(t, vec.IsConstNull())
			} else {
				require.False(t, vec.IsConstNull())
				require.Equal(t, tc.want, types.DecodeJson(vec.GetBytesAt(0)).String())
				expected, err := types.ParseStringToByteJson(tc.value.(string))
				require.NoError(t, err)
				encoded, err := types.EncodeJson(expected)
				require.NoError(t, err)
				require.Equal(t, encoded, vec.GetBytesAt(0), "binary JSON numeric tags must survive transport")
			}
		})
	}
}

func TestRemoteUserVariableFoldPreservesStringSource(t *testing.T) {
	makeProc := func(value any) *process.Process {
		proc := testutil.NewProcess(t)
		proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
			if name == "remote_value" && !system {
				return value, nil
			}
			return nil, moerr.NewInternalErrorNoCtx("variable not found")
		})
		return proc
	}
	makeVariable := func() *plan.Expr {
		expr := makeTestVarExpr("remote_value")
		expr.GetV().System = false
		return expr
	}
	foldRoundTrip := func(proc *process.Process) *plan.Literal {
		expr := makeVariable()
		folded, err := foldVarExprsInExprInPlace(expr, proc)
		require.NoError(t, err)
		require.True(t, folded)

		payload, err := expr.Marshal()
		require.NoError(t, err)
		decoded := new(plan.Expr)
		require.NoError(t, decoded.Unmarshal(payload))
		return decoded.GetLit()
	}

	first := foldRoundTrip(makeProc("first"))
	require.Equal(t, "first", first.GetSval())
	require.Equal(t, uint32(types.StringSourceUserVariable)+1, first.GetStringSource())

	// A NULL value retains its source across the same scalar and wire boundary.
	null := foldRoundTrip(makeProc(nil))
	require.True(t, null.GetIsnull())
	require.Equal(t, uint32(types.StringSourceUserVariable)+1, null.GetStringSource())

	// Reusing the expression shape with a fresh process is equivalent to a fresh scope.
	second := foldRoundTrip(makeProc("second"))
	require.Equal(t, "second", second.GetSval())
	require.Equal(t, first.GetStringSource(), second.GetStringSource())
}

func TestFoldVarExprsInHiddenExpressionsUsePrivateCopies(t *testing.T) {
	proc := newResolveVariableProcess(t, "ANSI")
	sharedAggExpr := makeTestVarExpr("sql_mode")
	sharedLockRows := makeTestVarExpr("sql_mode")

	scope := newScope(Normal)
	groupOp := group.NewArgument()
	groupOp.Aggs = []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(function.AggSumOverloadID, false, []*plan.Expr{sharedAggExpr}, nil),
	}
	lockOp := lockop.NewArgumentByEngine(nil)
	lockOp.AddLockTarget(2, nil, 0, types.T_int64.ToType(), -1, -1, nil, false)
	lockOp.AddLockTarget(1, nil, 0, types.T_int64.ToType(), -1, -1, sharedLockRows, false)
	groupOp.AppendChild(lockOp)
	scope.setRootOperator(groupOp)

	rewriteCalls := 0
	rewritten, err := lockOp.RewriteLockRowsExpressions(func(expr *plan.Expr) (*plan.Expr, bool, error) {
		require.NotNil(t, expr)
		rewriteCalls++
		return expr, false, nil
	})
	require.NoError(t, err)
	require.False(t, rewritten)
	require.Equal(t, 1, rewriteCalls)

	folded, err := foldVarExprsInScope(scope, proc)
	require.NoError(t, err)
	require.True(t, folded)

	aggArg := groupOp.Aggs[0].GetArgExpressions()[0]
	lockRows := lockOp.GetLockRowsExpressions()[0]
	require.IsType(t, &plan.Expr_V{}, sharedAggExpr.Expr)
	require.IsType(t, &plan.Expr_V{}, sharedLockRows.Expr)
	require.NotSame(t, sharedAggExpr, aggArg)
	require.NotSame(t, sharedLockRows, lockRows)
	require.Equal(t, "ANSI", aggArg.GetLit().GetSval())
	require.Equal(t, "ANSI", lockRows.GetLit().GetSval())
}

func TestScopeContainsVarExprInSource(t *testing.T) {
	scope := newScope(Normal)
	scope.DataSource = &Source{
		FilterList: []*plan.Expr{makeTestVarExpr("sql_mode")},
	}

	require.True(t, scopeContainsVarExpr(scope))
}

func TestScopeContainsVarExprReturnsFalseWithoutVar(t *testing.T) {
	scope := newScope(Normal)
	f := filter.NewArgument()
	f.FilterExprs = []*plan.Expr{makeTestConstBoolExpr(true)}
	scope.setRootOperator(f)

	require.False(t, scopeContainsVarExpr(scope))
}

func makeTestVarExpr(name string) *plan.Expr {
	typ := types.T_text.ToType()
	return makeTestVarExprWithType(name, typ)
}

func makeTestVarExprWithType(name string, typ types.Type) *plan.Expr {
	return &plan.Expr{
		Typ: plan2.MakePlan2Type(&typ),
		Expr: &plan.Expr_V{
			V: &plan.VarRef{
				Name:   name,
				System: true,
			},
		},
	}
}

func newResolveVariableProcess(t *testing.T, sqlMode string) *process.Process {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		if name == "sql_mode" {
			return sqlMode, nil
		}
		if name == "default_week_format" {
			return int64(0), nil
		}
		return nil, moerr.NewInternalErrorNoCtx("variable not found")
	})
	return proc
}

func makeTestConstBoolExpr(v bool) *plan.Expr {
	typ := types.T_bool.ToType()
	return &plan.Expr{
		Typ: plan2.MakePlan2Type(&typ),
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Bval{Bval: v},
			},
		},
	}
}

// Reflection must inspect expression owners without boxing unrelated mutable
// state. Interface() on a struct copies private fields, even if field traversal
// would otherwise skip them.
func TestVarExprTraversalDoesNotCopySharedPrivateState(t *testing.T) {
	shared := &struct {
		privateCounter int
		Expr           *plan.Expr
	}{Expr: makeTestConstBoolExpr(true)}
	owner := &struct{ State any }{State: shared}
	started, stop, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		shared.privateCounter++
		close(started)
		for {
			select {
			case <-stop:
				return
			default:
				shared.privateCounter++
			}
		}
	}()
	<-started
	func() {
		defer func() { close(stop); <-done }()
		for i := 0; i < 1000; i++ {
			require.False(t, containsVarExprInValue(reflect.ValueOf(owner), nil))
			folded, err := foldVarExprsInValue(reflect.ValueOf(owner), nil, nil)
			require.NoError(t, err)
			require.False(t, folded)
		}
	}()
	// Skipping a non-getter's boxing must still traverse its exported Expr.
	shared.Expr = makeTestVarExpr("sql_mode")
	require.True(t, containsVarExprInValue(reflect.ValueOf(owner), nil))
	folded, err := foldVarExprsInValue(reflect.ValueOf(owner), nil, newResolveVariableProcess(t, "ANSI"))
	require.NoError(t, err)
	require.True(t, folded)
	require.Equal(t, "ANSI", owner.State.(*struct {
		privateCounter int
		Expr           *plan.Expr
	}).Expr.GetLit().GetSval())
}
