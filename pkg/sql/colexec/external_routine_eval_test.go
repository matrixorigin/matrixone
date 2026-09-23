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

package colexec

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/udf/python"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type externalRoutineTestExecutor struct {
	vector   *vector.Vector
	evalSeen *int
	err      error
}

func (e *externalRoutineTestExecutor) Eval(_ *process.Process, _ []*batch.Batch, _ []bool) (*vector.Vector, error) {
	if e.evalSeen != nil {
		*e.evalSeen++
	}
	return e.vector, e.err
}

func (e *externalRoutineTestExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, selectList []bool) (*vector.Vector, error) {
	return e.Eval(proc, batches, selectList)
}

func (e *externalRoutineTestExecutor) ResetForNextQuery() {}
func (e *externalRoutineTestExecutor) Free()              {}
func (e *externalRoutineTestExecutor) IsColumnExpr() bool { return true }
func (e *externalRoutineTestExecutor) TypeName() string   { return "test input" }

type externalRoutineCaptureRuntime struct {
	invocation  *udf.Invocation
	invocations []udf.Invocation
}

type externalRoutineEmptySuccessRuntime struct{}

type externalRoutineErrorRuntime struct{ err error }

func (r *externalRoutineEmptySuccessRuntime) Language() string { return udf.LanguagePython }

func (r *externalRoutineEmptySuccessRuntime) Execute(_ context.Context, _ *udf.Invocation, _ vector.FunctionResultWrapper, _ *mpool.MPool) error {
	return nil
}

func (r *externalRoutineErrorRuntime) Language() string { return udf.LanguagePython }

func (r *externalRoutineErrorRuntime) Execute(_ context.Context, _ *udf.Invocation, _ vector.FunctionResultWrapper, _ *mpool.MPool) error {
	return r.err
}

func (r *externalRoutineCaptureRuntime) Language() string { return udf.LanguagePython }

func (r *externalRoutineCaptureRuntime) Execute(_ context.Context, invocation *udf.Invocation, result vector.FunctionResultWrapper, mp *mpool.MPool) error {
	r.invocation = invocation
	r.invocations = append(r.invocations, *invocation)
	output := result.(*vector.FunctionResult[int64])
	input := invocation.Inputs[0]
	values := vector.MustFixedColNoTypeCheck[int64](input)
	for row, value := range values[:invocation.Length] {
		if err := output.Append(value+1, input.IsNull(uint64(row))); err != nil {
			return err
		}
	}
	return nil
}

func TestExternalRoutineEvalReusesGroupWithNewEpoch(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.SetQueryId("reentry-runtime-query")
	proc.GetSessionInfo().TimeZone = time.FixedZone("FixedZone", 8*60*60)
	proc.GetSessionInfo().User = "root"
	proc.Base.StmtProfile = &process.StmtProfile{}
	proc.GetStmtProfile().SetQueryStart(time.Date(2026, 9, 10, 1, 2, 3, 4000, time.UTC))

	input := vector.NewVec(types.T_int64.ToType())
	defer input.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(input, int64(1), false, proc.Mp()))
	inputBatch := batch.NewWithSize(0)
	inputBatch.SetRowCount(1)

	runtime := &externalRoutineCaptureRuntime{}
	proc.Base.UdfService = runtime
	evaluator, err := newExternalRoutineEval(proc, testExternalRoutineCall(t, "SCALAR", udf.NullCallHandler), []ExpressionExecutor{
		&externalRoutineTestExecutor{vector: input},
	}, nil)
	require.NoError(t, err)
	defer evaluator.Free()

	_, err = evaluator.Eval(proc, []*batch.Batch{inputBatch}, []bool{true})
	require.NoError(t, err)
	_, err = evaluator.Eval(proc, []*batch.Batch{inputBatch}, []bool{true})
	require.NoError(t, err)
	require.Len(t, runtime.invocations, 2)
	require.Equal(t, runtime.invocations[0].Tuple.GroupID, runtime.invocations[1].Tuple.GroupID)
	require.Equal(t, uint64(1), runtime.invocations[0].Tuple.GroupEpoch)
	require.Equal(t, uint64(2), runtime.invocations[1].Tuple.GroupEpoch)
	require.NotEqual(t, runtime.invocations[0].Tuple.InvocationID, runtime.invocations[1].Tuple.InvocationID)

	evaluator.ResetForNextQuery()
	_, err = evaluator.Eval(proc, []*batch.Batch{inputBatch}, []bool{true})
	require.NoError(t, err)
	require.Len(t, runtime.invocations, 3)
	require.NotEqual(t, runtime.invocations[1].Tuple.GroupID, runtime.invocations[2].Tuple.GroupID)
	require.Equal(t, uint64(1), runtime.invocations[2].Tuple.GroupEpoch)
}

func TestExternalRoutineEvalOwnsSelectionAndStrictNullGuard(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetQueryId("runtime-query")
	proc.GetSessionInfo().TimeZone = time.FixedZone("FixedZone", 8*60*60)
	proc.GetSessionInfo().User = "alice"
	proc.GetSessionInfo().Database = "app"
	proc.GetSessionInfo().Role = "writer"
	proc.GetSessionInfo().SqlMode = "STRICT_TRANS_TABLES,ANSI"
	proc.Base.StmtProfile = &process.StmtProfile{}
	proc.GetStmtProfile().SetQueryStart(time.Date(2026, 9, 10, 1, 2, 3, 4000, time.FixedZone("input", 8*60*60)))

	mp := proc.Mp()
	input := vector.NewVec(types.T_int64.ToType())
	defer input.Free(mp)
	require.NoError(t, vector.AppendFixed(input, int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(input, int64(0), true, mp))
	require.NoError(t, vector.AppendFixed(input, int64(3), false, mp))

	runtime := &externalRoutineCaptureRuntime{}
	proc.Base.UdfService = runtime
	call := testExternalRoutineCall(t, "SCALAR", udf.NullReturnNull)
	evaluator, err := newExternalRoutineEval(proc, call, []ExpressionExecutor{
		&externalRoutineTestExecutor{vector: input},
	}, nil)
	require.NoError(t, err)
	defer evaluator.Free()

	inputBatch := batch.NewWithSize(0)
	inputBatch.SetRowCount(3)
	result, err := evaluator.Eval(proc, []*batch.Batch{inputBatch}, []bool{true, true, true})
	require.NoError(t, err)
	require.Equal(t, 2, runtime.invocation.Length, "the parent evaluator removes strict NULL rows before dispatch")
	require.Equal(t, []int64{2, 0, 4}, vector.MustFixedColNoTypeCheck[int64](result))
	require.True(t, result.IsNull(1), "the skipped row is scattered back as NULL")
	require.Equal(t, "runtime-query", runtime.invocation.Context["statement_id"])
	require.Equal(t, "[\"ANSI\",\"STRICT_TRANS_TABLES\"]", runtime.invocation.Context["sql_mode"])
	require.Equal(t, "+480", runtime.invocation.Context["session_timezone_offset_minutes"])
	require.Equal(t, "alice", runtime.invocation.Context["current_user"])
}

func TestExternalRoutineEvalRejectsSelectionWithWrongRowDomain(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	input := vector.NewVec(types.T_int64.ToType())
	defer input.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(input, int64(1), false, proc.Mp()))
	inputBatch := batch.NewWithSize(0)
	inputBatch.SetRowCount(1)
	evaluator, err := newExternalRoutineEval(proc, testExternalRoutineCall(t, "SCALAR", udf.NullCallHandler), []ExpressionExecutor{
		&externalRoutineTestExecutor{vector: input},
	}, nil)
	require.NoError(t, err)
	defer evaluator.Free()

	_, err = evaluator.Eval(proc, []*batch.Batch{inputBatch}, []bool{true, false})
	require.ErrorContains(t, err, "selection has 2 rows, expected 1")
}

func TestExternalRoutineEvalRejectsSelectionForEmptyRowDomain(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	input := vector.NewVec(types.T_int64.ToType())
	defer input.Free(proc.Mp())
	evaluator, err := newExternalRoutineEval(proc, testExternalRoutineCall(t, "SCALAR", udf.NullCallHandler), []ExpressionExecutor{
		&externalRoutineTestExecutor{vector: input},
	}, nil)
	require.NoError(t, err)
	defer evaluator.Free()

	emptyBatch := batch.NewWithSize(0)
	emptyBatch.SetRowCount(0)
	_, err = evaluator.Eval(proc, []*batch.Batch{emptyBatch}, []bool{true})
	require.ErrorContains(t, err, "selection has 1 rows, expected 0")
}

func TestNewExpressionExecutorDispatchesTypedRoutineCall(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.SetQueryId("physical-external-routine-query")
	proc.GetSessionInfo().TimeZone = time.FixedZone("FixedZone", 8*60*60)
	proc.GetSessionInfo().User = "root"
	proc.Base.StmtProfile = &process.StmtProfile{}
	proc.GetStmtProfile().SetQueryStart(time.Date(2026, 9, 10, 1, 2, 3, 4000, time.UTC))

	input := vector.NewVec(types.T_int64.ToType())
	for _, value := range []int64{1, 2, 3} {
		require.NoError(t, vector.AppendFixed(input, value, false, proc.Mp()))
	}
	inputBatch := batch.NewWithSize(1)
	inputBatch.Vecs[0] = input
	inputBatch.SetRowCount(3)
	defer inputBatch.Clean(proc.Mp())

	runtime := &externalRoutineCaptureRuntime{}
	proc.Base.UdfService = runtime
	call := testExternalRoutineCall(t, "SCALAR", udf.NullCallHandler)
	expression := &planpb.Expr{
		Typ: call.ReturnType,
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Args: []*planpb.Expr{{
				Typ:  *call.ArgumentTypes[0],
				Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}},
			}},
			RoutineCall: call,
		}},
	}

	executor, err := NewExpressionExecutor(proc, expression)
	require.NoError(t, err)
	defer executor.Free()
	result, err := executor.Eval(proc, []*batch.Batch{inputBatch}, []bool{true, false, true})
	require.NoError(t, err)
	require.Len(t, runtime.invocations, 1)
	require.Equal(t, 2, runtime.invocation.Length)
	require.Equal(t, []int64{2, 0, 4}, vector.MustFixedColNoTypeCheck[int64](result))
	require.True(t, result.IsNull(1))
}

func TestExternalRoutineEvalRequiresRowDomainForZeroArgumentVector(t *testing.T) {
	rows, err := externalRoutineRowCount("VECTOR", nil)
	require.Zero(t, rows)
	require.ErrorContains(t, err, "requires an input batch with num_rows")

	rows, err = externalRoutineRowCount("VECTOR", []*batch.Batch{nil})
	require.Zero(t, rows)
	require.ErrorContains(t, err, "input batch 0 is nil")
}

func TestExternalRoutineRowCountValidatesAllJoinDomains(t *testing.T) {
	first := batch.NewWithSize(0)
	first.SetRowCount(3)
	second := batch.NewWithSize(0)
	second.SetRowCount(3)
	short := batch.NewWithSize(0)
	short.SetRowCount(2)
	for _, tc := range []struct {
		name    string
		mode    string
		batches []*batch.Batch
		want    int
		err     string
	}{
		{name: "aligned join row domains", mode: "SCALAR", batches: []*batch.Batch{first, second}, want: 3},
		{name: "scalar without explicit batch has one row", mode: "SCALAR", want: 1},
		{name: "empty vector batch carries zero rows", mode: "VECTOR", batches: []*batch.Batch{{}}, err: ""},
		{name: "nil later relation", mode: "SCALAR", batches: []*batch.Batch{first, nil}, err: "input batch 1 is nil"},
		{name: "unequal join cardinality", mode: "SCALAR", batches: []*batch.Batch{first, short}, err: "has 2 rows, expected 3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := externalRoutineRowCount(tc.mode, tc.batches)
			if tc.err != "" {
				require.Zero(t, rows)
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, rows)
		})
	}
}

func TestExternalRoutineEvalAcceptsAlignedJoinBatches(t *testing.T) {
	first := batch.NewWithSize(0)
	first.SetRowCount(1)
	second := batch.NewWithSize(0)
	second.SetRowCount(1)

	rows, err := externalRoutineRowCount("SCALAR", []*batch.Batch{first, second})
	require.NoError(t, err)
	require.Equal(t, 1, rows)

	second.SetRowCount(2)
	rows, err = externalRoutineRowCount("SCALAR", []*batch.Batch{first, second})
	require.Zero(t, rows)
	require.ErrorContains(t, err, "expected 1 for the logical row domain")
}

func TestExternalRoutineEvalRejectsMissingReturnDescriptor(t *testing.T) {
	call := testExternalRoutineCall(t, "SCALAR", udf.NullCallHandler)
	call.ReturnType = planpb.Type{}

	require.NotPanics(t, func() {
		err := validateRoutineCall(call)
		require.ErrorContains(t, err, "no return descriptor")
	})
}

func TestExternalRoutineEvalRejectsUnsupportedTypeBeforeResultAllocation(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	call := testExternalRoutineCall(t, "SCALAR", udf.NullCallHandler)
	call.ReturnType = planpb.Type{Id: int32(types.T_decimal256), Width: 76, Scale: 2}
	input := vector.NewVec(types.T_int64.ToType())
	defer input.Free(proc.Mp())

	require.NotPanics(t, func() {
		_, err := newExternalRoutineEval(proc, call, []ExpressionExecutor{
			&externalRoutineTestExecutor{vector: input},
		}, nil)
		require.ErrorContains(t, err, "typed Python return descriptor is unsupported")
	})
}

func TestExternalRoutineEvalRejectsExecutionContextInPlan(t *testing.T) {
	call := testExternalRoutineCall(t, "SCALAR", udf.NullCallHandler)
	call.Context = map[string]string{"current_user": "attacker"}

	err := validateRoutineCall(call)
	require.ErrorContains(t, err, "contains execution context")
}

func TestExternalRoutineEvalRejectsAccountBeforeEvaluatingParameters(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.SetQueryId("account-boundary-query")
	proc.GetSessionInfo().TimeZone = time.FixedZone("FixedZone", 8*60*60)
	proc.Base.StmtProfile = &process.StmtProfile{}
	proc.GetStmtProfile().SetQueryStart(time.Date(2026, 9, 10, 1, 2, 3, 4000, time.UTC))

	input := vector.NewVec(types.T_int64.ToType())
	defer input.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(input, int64(1), false, proc.Mp()))
	seen := 0
	call := testExternalRoutineCall(t, "SCALAR", udf.NullCallHandler)
	call.FunctionRef.AccountId = 99
	evaluator, err := newExternalRoutineEval(proc, call, []ExpressionExecutor{
		&externalRoutineTestExecutor{vector: input, evalSeen: &seen},
	}, nil)
	require.NoError(t, err)
	defer evaluator.Free()

	inputBatch := batch.NewWithSize(0)
	inputBatch.SetRowCount(1)
	_, err = evaluator.Eval(proc, []*batch.Batch{inputBatch}, []bool{true})
	require.ErrorContains(t, err, "FunctionRef account does not match")
	require.Zero(t, seen)
}

func TestMaskedNestedExternalRoutineIsCompactedBeforeOuterEval(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.SetQueryId("nested-runtime-query")
	proc.GetSessionInfo().TimeZone = time.FixedZone("FixedZone", 8*60*60)
	proc.GetSessionInfo().User = "alice"
	proc.GetSessionInfo().Database = "app"
	proc.GetSessionInfo().SqlMode = "STRICT_TRANS_TABLES"
	proc.Base.StmtProfile = &process.StmtProfile{}
	proc.GetStmtProfile().SetQueryStart(time.Date(2026, 9, 10, 1, 2, 3, 4000, time.FixedZone("input", 8*60*60)))

	input := vector.NewVec(types.T_int64.ToType())
	for _, value := range []int64{1, 2, 3} {
		require.NoError(t, vector.AppendFixed(input, value, false, proc.Mp()))
	}
	inputBatch := batch.NewWithSize(1)
	inputBatch.Vecs[0] = input
	inputBatch.SetRowCount(3)
	defer inputBatch.Clean(proc.Mp())

	runtime := &externalRoutineCaptureRuntime{}
	proc.Base.UdfService = runtime
	external, err := newExternalRoutineEval(proc, testExternalRoutineCall(t, "SCALAR", udf.NullCallHandler), []ExpressionExecutor{
		&externalRoutineTestExecutor{vector: input},
	}, nil)
	require.NoError(t, err)

	outer := NewFunctionExpressionExecutor()
	require.NoError(t, outer.Init(proc, 1, types.T_int64.ToType()))
	defer outer.Free()
	outer.evalFn = func(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, _ *function.FunctionSelectList) error {
		values := vector.GenerateFunctionFixedTypeParameter[int64](parameters[0])
		output := vector.MustFunctionResult[int64](result)
		for row := 0; row < length; row++ {
			value, isNull := values.GetValue(uint64(row))
			if err := output.Append(value, isNull); err != nil {
				return err
			}
		}
		return nil
	}
	outer.SetParameter(0, external)

	result, err := outer.Eval(proc, []*batch.Batch{inputBatch}, []bool{false, true, true})
	require.NoError(t, err)
	require.Equal(t, []int64{0, 3, 4}, vector.MustFixedColNoTypeCheck[int64](result))
	require.True(t, result.IsNull(0))
}

func TestBuildRoutineContextRejectsUnstableTimezone(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = time.Local
	_, err := buildRoutineContext(proc)
	if time.Local.String() == "Local" {
		if _, _, resolveErr := resolveSystemTimezone(); resolveErr != nil {
			require.ErrorContains(t, err, "stable IANA identity")
		} else {
			require.NoError(t, err)
		}
	} else {
		require.NoError(t, err)
	}
}

func TestBuildRoutineContextRejectsSubMinuteFixedTimezone(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.FixedZone("FixedZone", 8*60*60+30)

	_, err := buildRoutineContext(proc)
	require.ErrorContains(t, err, "whole number of minutes")
}

func TestBuildRoutineContextRejectsMissingStatementInputs(t *testing.T) {
	if _, err := buildRoutineContext(nil); err == nil {
		t.Fatal("nil process must not produce an invocation context")
	}
	if _, err := buildRoutineContext(&process.Process{}); err == nil {
		t.Fatal("process without Base must not produce an invocation context")
	}
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.Base.StmtProfile = &process.StmtProfile{}
	proc.GetSessionInfo().TimeZone = time.UTC
	proc.Base.UnixTime = 0
	_, err := buildRoutineContext(proc)
	require.ErrorContains(t, err, "statement timestamp is unavailable")

	proc.Base.UnixTime = time.Now().UnixNano()
	proc.GetSessionInfo().TimeZone = nil
	_, err = buildRoutineContext(proc)
	require.ErrorContains(t, err, "session timezone is unavailable")
}

func TestResolveSystemTimezoneUsesExplicitStableZone(t *testing.T) {
	t.Setenv("TZ", "posix/UTC")
	name, location, err := resolveSystemTimezone()
	require.NoError(t, err)
	require.Equal(t, "UTC", name)
	require.Equal(t, "UTC", location.String())
}

func TestBuildRoutineContextUsesStableStatementAndSessionSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name        string
		sessionMode string
		resolve     func() (interface{}, error)
		wantMode    string
	}{
		{
			name:        "resolved SQL mode overrides session fallback",
			sessionMode: "ANSI",
			resolve:     func() (interface{}, error) { return "strict_trans_tables, ansi,STRICT_TRANS_TABLES", nil },
			wantMode:    `["ANSI","STRICT_TRANS_TABLES"]`,
		},
		{
			name:        "resolver error keeps session SQL mode",
			sessionMode: "ansi, strict_trans_tables",
			resolve:     func() (interface{}, error) { return nil, errors.New("variable unavailable") },
			wantMode:    `["ANSI","STRICT_TRANS_TABLES"]`,
		},
		{
			name:        "non-string resolution keeps session SQL mode",
			sessionMode: "ANSI",
			resolve:     func() (interface{}, error) { return int64(1), nil },
			wantMode:    `["ANSI"]`,
		},
		{
			name:        "empty sentinel becomes empty mode list",
			sessionMode: process.EmptySqlModeSentinel,
			wantMode:    `[]`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			proc.SetQueryId("")
			proc.GetSessionInfo().TimeZone = time.UTC
			proc.GetSessionInfo().User = "root"
			proc.GetSessionInfo().Database = ""
			proc.GetSessionInfo().Role = ""
			proc.GetSessionInfo().SqlMode = tc.sessionMode
			proc.Base.StmtProfile = &process.StmtProfile{}
			proc.Base.UnixTime = time.Date(2026, 9, 23, 1, 2, 3, 4000, time.UTC).UnixNano()
			if tc.resolve != nil {
				proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
					require.Equal(t, "sql_mode", name)
					require.True(t, system)
					require.False(t, global)
					return tc.resolve()
				})
			}

			values, err := buildRoutineContext(proc)
			require.NoError(t, err)
			require.NotContains(t, values, "statement_id")
			require.Equal(t, strconv.FormatInt(time.Unix(0, proc.Base.UnixTime).UTC().UnixMicro(), 10), values["statement_timestamp_utc"])
			require.Equal(t, "IANA", values["session_timezone_kind"])
			require.Equal(t, "UTC", values["session_timezone_name"])
			require.NotEmpty(t, values["session_timezone_tzdb_version"])
			require.Equal(t, tc.wantMode, values["sql_mode"])
			require.NotContains(t, values, "current_database")
			require.NotContains(t, values, "current_role")
		})
	}
}

func testExternalRoutineCall(t *testing.T, mode, nullPolicy string) *planpb.RoutineCall {
	t.Helper()
	descriptor, err := function.NewPythonTypeDescriptor(types.T_int64.ToType())
	require.NoError(t, err)
	body, err := json.Marshal(function.PythonRoutineBody{
		DefinitionSchemaVersion: udf.PythonDefinitionSchemaVersion,
		Handler:                 "add",
		Source:                  "def add(ctx, value): return value + 1",
		Mode:                    mode,
		NullPolicy:              nullPolicy,
		ABIContract:             udf.PythonABIContract,
		AdapterVersion:          udf.PythonAdapterVersion,
		ArtifactDigest:          udf.PythonInlineArtifactDigest("add", "def add(ctx, value): return value + 1"),
		EnvironmentDigest:       func() string { digest, _ := udf.PythonEnvironmentDigest(); return digest }(),
		SDKVersion:              udf.PythonSDKVersion,
		ArgTypes:                []function.PythonTypeDescriptor{descriptor},
		ReturnType:              &descriptor,
	})
	require.NoError(t, err)
	routine := &function.Udf{
		FunctionID:       100,
		AccountID:        0,
		DatabaseID:       2,
		Revision:         7,
		NamespaceVersion: 9,
		Language:         udf.LanguagePython,
		Body:             string(body),
		PythonArgTypes:   []function.PythonTypeDescriptor{descriptor},
		PythonReturnType: &descriptor,
	}
	call, err := routine.GetRoutineCall()
	require.NoError(t, err)
	call.CallsiteId = "python/test"
	return call
}

func TestExternalRoutineEvalRejectsSourceInExecutablePlan(t *testing.T) {
	call := testExternalRoutineCall(t, python.ModeScalar, udf.NullCallHandler)
	call.GetPython().Source = "def add(ctx, value): return value + 1"
	require.ErrorContains(t, validateRoutineCall(call), "plan contains source")
}

func TestExternalRoutineEvalRequiresCanonicalLanguage(t *testing.T) {
	call := testExternalRoutineCall(t, python.ModeScalar, udf.NullCallHandler)
	call.Language = "PYTHON"
	require.ErrorContains(t, validateRoutineCall(call), "unsupported typed routine call contract")
}

func TestExternalRoutineEvalRejectsSuccessfulRuntimeWithoutResultRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.SetQueryId("missing-result-query")
	proc.GetSessionInfo().TimeZone = time.UTC
	proc.GetSessionInfo().User = "root"
	proc.Base.StmtProfile = &process.StmtProfile{}
	proc.GetStmtProfile().SetQueryStart(time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC))

	input := vector.NewVec(types.T_int64.ToType())
	defer input.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(input, int64(10), false, proc.Mp()))
	bat := batch.NewWithSize(0)
	bat.SetRowCount(1)
	proc.Base.UdfService = &externalRoutineEmptySuccessRuntime{}
	evaluator, err := newExternalRoutineEval(
		proc,
		testExternalRoutineCall(t, python.ModeScalar, udf.NullCallHandler),
		[]ExpressionExecutor{&externalRoutineTestExecutor{vector: input}},
		nil,
	)
	require.NoError(t, err)
	defer evaluator.Free()

	_, err = evaluator.Eval(proc, []*batch.Batch{bat}, []bool{true})
	require.ErrorContains(t, err, "runtime produced 0 result rows, expected 1")
}

func TestExternalRoutineEvalPropagatesParameterAndRuntimeFailures(t *testing.T) {
	for _, tc := range []struct {
		name             string
		parameterFailure bool
		runtimeFailure   bool
		want             string
	}{
		{name: "parameter evaluation fails", parameterFailure: true, want: "parameter expression failed"},
		{name: "runtime is disabled", want: "runtime is not enabled"},
		{name: "runtime execution fails", runtimeFailure: true, want: "handler process was cancelled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			proc.SetQueryId("error-path-query")
			proc.GetSessionInfo().TimeZone = time.UTC
			proc.GetSessionInfo().User = "root"
			proc.Base.StmtProfile = &process.StmtProfile{}
			proc.GetStmtProfile().SetQueryStart(time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC))
			input := vector.NewVec(types.T_int64.ToType())
			defer input.Free(proc.Mp())
			require.NoError(t, vector.AppendFixed(input, int64(10), false, proc.Mp()))
			parameter := &externalRoutineTestExecutor{vector: input}
			if tc.parameterFailure {
				parameter.err = errors.New(tc.want)
			} else if tc.runtimeFailure {
				proc.Base.UdfService = &externalRoutineErrorRuntime{err: errors.New(tc.want)}
			}
			evaluator, err := newExternalRoutineEval(
				proc, testExternalRoutineCall(t, python.ModeScalar, udf.NullCallHandler),
				[]ExpressionExecutor{parameter}, nil,
			)
			require.NoError(t, err)
			defer evaluator.Free()
			bat := batch.NewWithSize(0)
			bat.SetRowCount(1)
			_, err = evaluator.Eval(proc, []*batch.Batch{bat}, []bool{true})
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestExternalRoutineTransfersResultOwnership(t *testing.T) {
	for _, mode := range []string{"SCALAR", "VECTOR"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			proc.SetQueryId("result-ownership")
			proc.GetSessionInfo().TimeZone = time.UTC
			proc.GetSessionInfo().User = "root"
			proc.Base.StmtProfile = &process.StmtProfile{}
			proc.GetStmtProfile().SetQueryStart(time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC))
			input := vector.NewVec(types.T_int64.ToType())
			defer input.Free(proc.Mp())
			require.NoError(t, vector.AppendFixed(input, int64(10), false, proc.Mp()))
			require.NoError(t, vector.AppendFixed(input, int64(20), false, proc.Mp()))
			bat := batch.NewWithSize(0)
			bat.SetRowCount(2)
			proc.Base.UdfService = &externalRoutineCaptureRuntime{}
			evaluator, err := newExternalRoutineEval(proc, testExternalRoutineCall(t, mode, udf.NullCallHandler), []ExpressionExecutor{&externalRoutineTestExecutor{vector: input}}, nil)
			require.NoError(t, err)
			defer evaluator.Free()
			owned, err := evaluator.EvalWithoutResultReusing(proc, []*batch.Batch{bat}, []bool{true, false})
			require.NoError(t, err)
			// Only free when ownership actually moved, so the pre-fix counterexample
			// reports the alias without double freeing the evaluator's vector.
			if owned != evaluator.result.GetResultVector() {
				defer owned.Free(proc.Mp())
			}
			require.NotSame(t, owned, evaluator.result.GetResultVector())
			_, err = evaluator.Eval(proc, []*batch.Batch{bat}, nil)
			require.NoError(t, err)
			evaluator.Free()
			require.Equal(t, 2, owned.Length())
			require.Equal(t, int64(11), vector.GetFixedAtNoTypeCheck[int64](owned, 0))
			require.True(t, owned.IsNull(1))
		})
	}
}

func TestExternalRoutineRejectsDamagedDefinitionBeforeArguments(t *testing.T) {
	for _, damage := range []string{"precision", "fingerprint", "argument-type"} {
		t.Run(damage, func(t *testing.T) {
			call := testExternalRoutineCall(t, "SCALAR", udf.NullCallHandler)
			switch damage {
			case "precision":
				call.ReturnType = planpb.Type{Id: int32(types.T_decimal64), Width: 19, Scale: 2}
			case "fingerprint":
				call.GetPython().DefinitionFingerprint = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			case "argument-type":
				call.ArgumentTypes[0].Id = int32(types.T_int32)
			}
			require.Error(t, validateRoutineCall(call))
		})
	}
}

func TestValidateRoutineCallRejectsIncompleteTypedContracts(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*planpb.RoutineCall)
		want   string
	}{
		{name: "missing FunctionRef", mutate: func(call *planpb.RoutineCall) { call.FunctionRef = nil }, want: "no exact FunctionRef"},
		{name: "zero function identity", mutate: func(call *planpb.RoutineCall) { call.FunctionRef.FunctionId = 0 }, want: "no exact FunctionRef"},
		{name: "missing return descriptor", mutate: func(call *planpb.RoutineCall) { call.ReturnType.Id = int32(types.T_any) }, want: "no return descriptor"},
		{name: "unsupported contract version", mutate: func(call *planpb.RoutineCall) { call.ContractVersion++ }, want: "unsupported typed routine call contract"},
		{name: "non-volatile function", mutate: func(call *planpb.RoutineCall) { call.Volatility = "IMMUTABLE" }, want: "must be VOLATILE"},
		{name: "function cannot error", mutate: func(call *planpb.RoutineCall) { call.MayError = false }, want: "semantic contract is not supported"},
		{name: "unsupported security contract", mutate: func(call *planpb.RoutineCall) { call.SecurityMode = "DEFINER" }, want: "semantic contract is not supported"},
		{name: "leakproof function", mutate: func(call *planpb.RoutineCall) { call.Leakproof = true }, want: "semantic contract is not supported"},
		{name: "invalid callsite newline", mutate: func(call *planpb.RoutineCall) { call.CallsiteId = "python/test\nother" }, want: "valid callsite id"},
		{name: "execution context in plan", mutate: func(call *planpb.RoutineCall) { call.Context = map[string]string{"statement_id": "stale"} }, want: "contains execution context"},
		{name: "missing Python implementation", mutate: func(call *planpb.RoutineCall) { call.Implementation = nil }, want: "no Python implementation"},
		{name: "source in plan", mutate: func(call *planpb.RoutineCall) { call.GetPython().Source = "def add(ctx, value): return value" }, want: "plan contains source"},
		{name: "unsupported SDK contract", mutate: func(call *planpb.RoutineCall) { call.GetPython().SdkVersion = "future" }, want: "implementation contract is not supported"},
		{name: "unsupported mode", mutate: func(call *planpb.RoutineCall) { call.GetPython().Mode = "BATCH" }, want: "unsupported typed call mode"},
		{name: "unsupported NULL policy", mutate: func(call *planpb.RoutineCall) { call.GetPython().NullPolicy = "DEFAULT" }, want: "unsupported typed NULL policy"},
		{name: "NULL policy mismatch", mutate: func(call *planpb.RoutineCall) { call.NullPolicy = udf.NullReturnNull }, want: "NULL policy does not match"},
		{name: "malformed artifact digest", mutate: func(call *planpb.RoutineCall) { call.GetPython().ArtifactDigest = "not-a-digest" }, want: "invalid artifact/environment digest"},
		{name: "malformed definition fingerprint", mutate: func(call *planpb.RoutineCall) { call.GetPython().DefinitionFingerprint = "not-a-digest" }, want: "invalid definition fingerprint"},
		{name: "nil argument descriptor", mutate: func(call *planpb.RoutineCall) { call.ArgumentTypes[0] = nil }, want: "argument 0 is nil"},
		{name: "unsupported argument type", mutate: func(call *planpb.RoutineCall) { call.ArgumentTypes[0].Id = int32(types.T_any) }, want: "argument 0 is unsupported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			call := testExternalRoutineCall(t, python.ModeScalar, udf.NullCallHandler)
			tc.mutate(call)
			require.ErrorContains(t, validateRoutineCall(call), tc.want)
		})
	}
}

func TestValidateExternalRoutineInputsChecksRowAndTypeContracts(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	int64Rows := vector.NewVec(types.T_int64.ToType())
	defer int64Rows.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(int64Rows, int64(1), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(int64Rows, int64(2), false, proc.Mp()))
	shortRows := vector.NewVec(types.T_int64.ToType())
	defer shortRows.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(shortRows, int64(1), false, proc.Mp()))
	int32Rows := vector.NewVec(types.T_int32.ToType())
	defer int32Rows.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(int32Rows, int32(1), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(int32Rows, int32(2), false, proc.Mp()))
	emptyRows := vector.NewVec(types.T_int64.ToType())
	defer emptyRows.Free(proc.Mp())

	for _, tc := range []struct {
		name    string
		inputs  []*vector.Vector
		rows    int
		mutate  func(*planpb.RoutineCall)
		wantErr string
	}{
		{name: "valid aligned input", inputs: []*vector.Vector{int64Rows}, rows: 2},
		{name: "argument count mismatch", inputs: nil, rows: 2, wantErr: "evaluated input count 0"},
		{name: "missing input vector", inputs: []*vector.Vector{nil}, rows: 1, wantErr: "has no type"},
		{name: "empty input vector", inputs: []*vector.Vector{emptyRows}, rows: 1, wantErr: "has 0 rows"},
		{name: "short non-constant input", inputs: []*vector.Vector{shortRows}, rows: 2, wantErr: "expected at least 2"},
		{name: "physical SQL type mismatch", inputs: []*vector.Vector{int32Rows}, rows: 2, wantErr: "does not match the frozen descriptor"},
		{name: "unsupported frozen descriptor", inputs: []*vector.Vector{int64Rows}, rows: 2, mutate: func(call *planpb.RoutineCall) { call.ArgumentTypes[0].Id = int32(types.T_enum) }, wantErr: "invalid argument descriptor 0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			call := testExternalRoutineCall(t, python.ModeScalar, udf.NullCallHandler)
			if tc.mutate != nil {
				tc.mutate(call)
			}
			err := validateExternalRoutineInputs(tc.inputs, call, tc.rows)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestExternalRoutineSkipsArgumentsForEmptySelection(t *testing.T) {
	for _, mode := range []string{"SCALAR", "VECTOR"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			input := vector.NewVec(types.T_int64.ToType())
			defer input.Free(proc.Mp())
			require.NoError(t, vector.AppendFixed(input, int64(1), false, proc.Mp()))
			seen := 0
			evaluator, err := newExternalRoutineEval(proc, testExternalRoutineCall(t, mode, udf.NullCallHandler), []ExpressionExecutor{&externalRoutineTestExecutor{vector: input, evalSeen: &seen}}, nil)
			require.NoError(t, err)
			defer evaluator.Free()
			bat := batch.NewWithSize(0)
			bat.SetRowCount(1)
			result, err := evaluator.Eval(proc, []*batch.Batch{bat}, []bool{false})
			require.NoError(t, err)
			require.True(t, result.IsNull(0))
			require.Zero(t, seen, "an unselected call must not evaluate any arguments")
		})
	}
}
