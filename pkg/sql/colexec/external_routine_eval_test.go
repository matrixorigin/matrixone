// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package colexec

import (
	"context"
	"encoding/json"
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
}

func (e *externalRoutineTestExecutor) Eval(_ *process.Process, _ []*batch.Batch, _ []bool) (*vector.Vector, error) {
	if e.evalSeen != nil {
		*e.evalSeen++
	}
	return e.vector, nil
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
