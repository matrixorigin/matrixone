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
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// bindConstMathTextExpr binds the direct TEXT overload of a two-argument
// numeric function over constant TEXT and constant INT64 literals, mirroring
// the plan expression built for a const compose such as
// round(cast('18446744073709551615' as text), -1).
func bindConstMathTextExpr(
	t *testing.T, proc *process.Process,
	fnName, textArg string, digits int64,
) *plan.Expr {
	t.Helper()
	fn, err := function.GetFunctionByName(
		proc.Ctx, fnName, []types.Type{types.T_text.ToType(), types.T_int64.ToType()},
	)
	require.NoError(t, err)
	_, shouldCast := fn.ShouldDoImplicitTypeCast()
	require.False(t, shouldCast,
		"%s(TEXT, INT64) must bind the direct TEXT overload, not a by-cast match", fnName)

	textTyp := types.T_text.ToType()
	retType := fn.GetReturnType()
	return &plan.Expr{
		Typ: plan.Type{
			Id: int32(retType.Oid), Width: retType.Width, Scale: retType.Scale,
		},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: fnName},
			Args: []*plan.Expr{
				{
					Typ: plan.Type{Id: int32(textTyp.Oid), Width: textTyp.Width},
					Expr: &plan.Expr_Lit{Lit: &plan.Literal{
						Value: &plan.Literal_Sval{Sval: textArg},
					}},
				},
				{
					Typ: plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Lit{Lit: &plan.Literal{
						Value: &plan.Literal_I64Val{I64Val: digits},
					}},
				},
			},
		}},
	}
}

// catchPanic runs fn and returns the panic value it terminated with, or nil
// when fn returned normally (a t.Fatalf inside fn also surfaces as nil
// because runtime.Goexit is not a panic).
func catchPanic(fn func()) (recovered any) {
	defer func() {
		recovered = recover()
	}()
	fn()
	return nil
}

// countingStubExecutor records how often the borrow seam closes the
// borrowing through Free, without allocating a real executor tree.
type countingStubExecutor struct {
	freeCount int
}

func (s *countingStubExecutor) Eval(
	*process.Process, []*batch.Batch, []bool,
) (*vector.Vector, error) {
	return nil, nil
}

func (s *countingStubExecutor) EvalWithoutResultReusing(
	*process.Process, []*batch.Batch, []bool,
) (*vector.Vector, error) {
	return nil, nil
}

func (s *countingStubExecutor) ResetForNextQuery() {}

func (s *countingStubExecutor) Free() { s.freeCount++ }

func (s *countingStubExecutor) IsColumnExpr() bool { return false }

func (s *countingStubExecutor) TypeName() string { return "countingStubExecutor" }

// TestEvalBorrowedExecutorClosesBorrowOnNonSuccessExits pins the contract of
// evalBorrowedExecutor: a successful evaluation leaves the executor alive for
// its caller to free, while a returned error or a panning evaluation frees
// the executor exactly once, and the original panic value is re-raised
// unchanged.
func TestEvalBorrowedExecutorClosesBorrowOnNonSuccessExits(t *testing.T) {
	t.Run("success keeps the executor for its caller", func(t *testing.T) {
		stub := &countingStubExecutor{}
		result, err := evalBorrowedExecutor(stub, func() (*vector.Vector, error) {
			return nil, nil
		})
		require.NoError(t, err)
		require.Nil(t, result)
		require.Zero(t, stub.freeCount,
			"a successful borrow must hand the executor to the caller, not free it")
	})

	t.Run("returned error frees the executor once", func(t *testing.T) {
		stub := &countingStubExecutor{}
		evalErr := errors.New("evaluate failed")
		result, err := evalBorrowedExecutor(stub, func() (*vector.Vector, error) {
			return nil, evalErr
		})
		require.ErrorIs(t, err, evalErr)
		require.Nil(t, result)
		require.Equal(t, 1, stub.freeCount)
	})

	t.Run("panic frees the executor once and re-raises the same error", func(t *testing.T) {
		stub := &countingStubExecutor{}
		sentinel := errors.New("panic sentinel")
		caught := catchPanic(func() {
			_, _ = evalBorrowedExecutor(stub, func() (*vector.Vector, error) {
				panic(sentinel)
			})
		})
		caughtErr, ok := caught.(error)
		require.True(t, ok, "the re-raised panic value must keep its error type, got %#v", caught)
		require.ErrorIs(t, caughtErr, sentinel,
			"the re-raised panic value must be the original one")
		require.Equal(t, 1, stub.freeCount)
	})

	t.Run("panic re-raises non-error values unchanged too", func(t *testing.T) {
		stub := &countingStubExecutor{}
		caught := catchPanic(func() {
			_, _ = evalBorrowedExecutor(stub, func() (*vector.Vector, error) {
				panic("raw string panic")
			})
		})
		require.Equal(t, "raw string panic", caught)
		require.Equal(t, 1, stub.freeCount)
	})
}

// TestBorrowedExecutorIsFreedWhenEvaluationPanics drives the real
// constant-fold borrow seam with the round(TEXT, INT64) overload added by
// PR #32438: the round core reports its out-of-range result by panicking with
// moerr.NewOutOfRangeNoCtx (func_binary.go roundUint64), and before the fix
// that panic unwound past every executor Free. The leak was invisible except
// under the reuse checker, where the Go finalizer turned it into a whole-CN
// 'missing free' panic during the next workload (the PR's compose-CI crash).
// The seam must free the borrowed executor and still propagate the original
// moerr so the SQL error contract stays intact; the definitive missing-free
// proof is the service-level compose-parity BVT rerun with the reuse checker
// enabled, recorded with the fix's validation evidence.
func TestBorrowedExecutorIsFreedWhenEvaluationPanics(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	const overflowText = "18446744073709551615"

	t.Run("readonly borrow propagates the round out-of-range moerr", func(t *testing.T) {
		caught := catchPanic(func() {
			expr := bindConstMathTextExpr(t, proc, "round", overflowText, -1)
			_, _, err := GetReadonlyResultFromExpression(
				proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch},
			)
			t.Fatalf("expected the out-of-range panic to propagate, got return with err=%v", err)
		})
		require.NotNil(t, caught, "the round core must terminate by panicking")
		caughtErr, ok := caught.(error)
		require.True(t, ok, "round must panic with a moerr error, got %#v", caught)
		require.True(t, moerr.IsMoErrCode(caughtErr, moerr.ErrOutOfRange),
			"the propagated panic must be the round core's out-of-range moerr, got %v", caughtErr)
		require.ErrorContains(t, caughtErr, "uint64")
		require.ErrorContains(t, caughtErr, "ROUND")
	})

	t.Run("writable borrow propagates the round out-of-range moerr", func(t *testing.T) {
		caught := catchPanic(func() {
			expr := bindConstMathTextExpr(t, proc, "round", overflowText, -1)
			_, err := GetWritableResultFromNoColumnExpression(proc, expr)
			t.Fatalf("expected the out-of-range panic to propagate, got return with err=%v", err)
		})
		require.NotNil(t, caught, "the round core must terminate by panicking")
		caughtErr, ok := caught.(error)
		require.True(t, ok, "round must panic with a moerr error, got %#v", caught)
		require.True(t, moerr.IsMoErrCode(caughtErr, moerr.ErrOutOfRange),
			"the propagated panic must be the round core's out-of-range moerr, got %v", caughtErr)
	})

	t.Run("borrow stays usable after a panicking evaluation", func(t *testing.T) {
		// Success controls: the same seam must keep handing out correct
		// values and ownership after the panicking borrows above, which also
		// proves the freed executor tree leaves pool state healthy.
		expr := bindConstMathTextExpr(t, proc, "round", "9223372036854775808", 0)
		vec, free, err := GetReadonlyResultFromExpression(
			proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch},
		)
		require.NoError(t, err)
		require.Equal(t, types.Decimal128{B0_63: 9223372036854775808, B64_127: 0},
			vector.MustFixedColWithTypeCheck[types.Decimal128](vec)[0])
		free()

		expr = bindConstMathTextExpr(t, proc, "round", "-1", 0)
		vec, err = GetWritableResultFromNoColumnExpression(proc, expr)
		require.NoError(t, err)
		defer vec.Free(proc.Mp())
		require.Equal(t, types.Decimal128{B0_63: ^uint64(0), B64_127: ^uint64(0)},
			vector.MustFixedColWithTypeCheck[types.Decimal128](vec)[0])
	})
}
