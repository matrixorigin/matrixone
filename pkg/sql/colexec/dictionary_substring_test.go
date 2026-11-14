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

package colexec

import (
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestScopedSubstringMembershipLifetime(t *testing.T) {
	proc, allocation, _ := dependentTestAllocation(t)
	input := batch.NewWithSize(2)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	balance := types.New(types.T_decimal64, 15, 2)
	phone := types.New(types.T_char, 15, 0)
	input.Vecs[0] = vector.NewVec(balance)
	input.Vecs[1] = vector.NewVec(phone)
	require.NoError(t, vector.AppendFixedList(input.Vecs[0], []types.Decimal64{100, 0, 100, 100}, nil, proc.Mp()))
	for _, s := range []string{"13-567-890-1234", "17-567-890-1234", "31-567-890-1234"} {
		require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(s), false, proc.Mp()))
	}
	require.NoError(t, vector.AppendBytes(input.Vecs[1], nil, true, proc.Mp()))
	input.SetRowCount(4)
	col := func(i int32, typ types.Type) *plan.Expr {
		return &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: i}}}
	}
	integer := func(n int64) *plan.Expr {
		return &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: n}}}}
	}
	zero := &plan.Expr{Typ: balance.PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Decimal64Val{Decimal64Val: &plan.Decimal64{A: 0}}}}}
	guard, e := NewExpressionExecutorWithAllocation(proc, bindTestFunction(t, proc, ">", col(0, balance), zero), allocation)
	require.NoError(t, e)
	t.Cleanup(guard.Free)
	substring := bindTestFunction(t, proc, "substring", col(1, phone), integer(1), integer(2))
	payload := vector.NewVec(types.MustTypeFromPlan(substring.Typ))
	require.NoError(t, vector.AppendBytes(payload, []byte("13"), false, proc.Mp()))
	data, e := payload.MarshalBinary()
	payload.Free(proc.Mp())
	require.NoError(t, e)
	list := &plan.Expr{Typ: substring.Typ, Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: 1, Data: data}}}
	member, e := NewExpressionExecutorWithAllocation(proc, bindTestFunction(t, proc, "in", substring, list), allocation)
	require.NoError(t, e)
	t.Cleanup(member.Free)
	root := member.(*FunctionExpressionExecutor)
	sub := root.parameterExecutor[0].(*FunctionExpressionExecutor)
	require.True(t, predicateBorrowsInputs(root))
	require.NotNil(t, sub.dictionaryInput)
	require.Nil(t, sub.resetFn)
	require.Nil(t, sub.freeFn)
	frame := &DictionaryInputFrame{}
	t.Cleanup(func() { frame.Free(proc.Mp()) })
	var rows []int64
	var parameter vector.FunctionParameterWrapper[bool]
	kept, all, e := SelectPredicateRows(proc, input, nil, []ExpressionExecutor{guard, member}, allocation, &rows, &parameter, nil, nil, frame)
	require.NoError(t, e)
	require.False(t, all)
	require.Equal(t, []int64{0}, kept)
	require.Zero(t, frame.Gathers)
	require.Empty(t, frame.predicateVectors)
	require.Nil(t, sub.parameterResults[0])
	direct := &DictionaryInputFrame{}
	t.Cleanup(func() { direct.Free(proc.Mp()) })
	direct.Begin(input, allocation, proc.Mp())
	selected, borrowed, e := direct.predicateBatch(proc, input, nil, root, []int64{2, 0, 2, 3}, allocation)
	require.NoError(t, e)
	require.True(t, borrowed)
	require.True(t, selected.Vecs[1].IsDictionary())
	direct.Begin(selected, allocation, proc.Mp())
	direct.Bind([]ExpressionExecutor{member})
	out, e := member.Eval(proc, []*batch.Batch{selected}, nil)
	require.NoError(t, e)
	require.Equal(t, []bool{false, true, false}, vector.MustFixedColNoTypeCheck[bool](out)[:3])
	require.True(t, out.IsNull(3))
	require.Zero(t, direct.Gathers)
	direct.End()
	require.Nil(t, sub.parameterResults[0])
	input.Vecs[1].CleanOnlyData()
	require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte("99-567-890-1234"), false, proc.Mp()))
	require.Equal(t, "13", sub.resultVector.GetResultVector().GetStringAt(1))
	require.Equal(t, []bool{false, true, false}, vector.MustFixedColNoTypeCheck[bool](out)[:3])
	require.True(t, out.IsNull(3))
}

func TestScopedSubstringDomainsAndOwnedOutput(t *testing.T) {
	for _, oid := range []types.T{types.T_char, types.T_varchar} {
		for _, arity := range []int{2, 3} {
			t.Run(oid.String()+"/"+strconv.Itoa(arity), func(t *testing.T) {
				proc, allocation, _ := dependentTestAllocation(t)
				typ := types.New(oid, 64, 0)
				input := batch.NewWithSize(1)
				t.Cleanup(func() { input.Clean(proc.Mp()) })
				input.Vecs[0] = vector.NewVec(typ)
				suffix := strings.Repeat("x", 40)
				for range 2 {
					require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte("a你好b"+suffix), false, proc.Mp()))
				}
				require.NoError(t, vector.AppendBytes(input.Vecs[0], nil, true, proc.Mp()))
				require.NoError(t, input.Vecs[0].SetRuntimeStringDomainsWithMP([]types.RuntimeStringDomain{types.RuntimeStringText, types.RuntimeStringBinary, types.RuntimeStringText}, proc.Mp()))
				input.SetRowCount(3)
				column := &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
				integer := func(n int64) *plan.Expr {
					return &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: n}}}}
				}
				args := []*plan.Expr{column, integer(2)}
				if arity == 3 {
					args = append(args, integer(1))
				}
				executor, err := NewExpressionExecutorWithAllocation(proc, bindTestFunction(t, proc, "substring", args...), allocation)
				require.NoError(t, err)
				t.Cleanup(executor.Free)
				require.True(t, predicateBorrowsInputs(executor))
				frame := &DictionaryInputFrame{}
				t.Cleanup(func() { frame.Free(proc.Mp()) })
				frame.Begin(input, allocation, proc.Mp())
				selected, borrowed, err := frame.predicateBatch(proc, input, nil, executor, []int64{1, 0, 1, 2}, allocation)
				require.NoError(t, err)
				require.True(t, borrowed)
				require.True(t, selected.HasDictionary())
				frame.Begin(selected, allocation, proc.Mp())
				frame.Bind([]ExpressionExecutor{executor})
				out, err := executor.Eval(proc, []*batch.Batch{selected}, nil)
				require.NoError(t, err)
				require.Zero(t, frame.Gathers)
				binary, text := []byte("你好b"+suffix), []byte("你好b"+suffix)
				if arity == 3 {
					binary, text = []byte{0xe4}, []byte("你")
				}
				check := func() {
					t.Helper()
					for row, want := range [][]byte{binary, text, binary} {
						require.Equal(t, want, out.GetBytesAt(row))
						require.Equal(t, row != 1, out.GetIsBinaryStringAt(row))
					}
					require.True(t, out.IsNull(3))
				}
				check()
				frame.End()
				require.Nil(t, executor.(*FunctionExpressionExecutor).parameterResults[0])
				input.Vecs[0].CleanOnlyData()
				require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte("new generation"), false, proc.Mp()))
				check()
			})
		}
	}
}

func TestScopedSubstringUnprovedShapeKeepsBank(t *testing.T) {
	for _, shape := range []string{"column-bound", "blob-overload"} {
		t.Run(shape, func(t *testing.T) {
			proc, allocation, _ := dependentTestAllocation(t)
			typ := types.T_varchar.ToType()
			if shape == "blob-overload" {
				typ = types.T_blob.ToType()
			}
			input := batch.NewWithSize(2)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			input.Vecs[0], input.Vecs[1] = vector.NewVec(typ), vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte("abcdef"), false, proc.Mp()))
			require.NoError(t, vector.AppendFixed(input.Vecs[1], int64(2), false, proc.Mp()))
			input.SetRowCount(1)
			integer := func(n int64) *plan.Expr {
				return &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: n}}}}
			}
			start := integer(2)
			if shape == "column-bound" {
				start = &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}}
			}
			column := &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
			executor, err := NewExpressionExecutorWithAllocation(proc, bindTestFunction(t, proc, "substring", column, start, integer(3)), allocation)
			require.NoError(t, err)
			t.Cleanup(executor.Free)
			require.False(t, predicateBorrowsInputs(executor))
			frame := &DictionaryInputFrame{}
			t.Cleanup(func() { frame.Free(proc.Mp()) })
			frame.Begin(input, allocation, proc.Mp())
			selected, borrowed, err := frame.predicateBatch(proc, input, nil, executor, []int64{0}, allocation)
			require.NoError(t, err)
			require.True(t, borrowed)
			require.False(t, selected.HasDictionary())
			require.Positive(t, frame.Gathers)
			out, err := executor.Eval(proc, []*batch.Batch{selected}, nil)
			require.NoError(t, err)
			require.Equal(t, "bcd", out.GetStringAt(0))
			frame.End()
		})
	}
}

func TestScopedSubstringMetadataCapacityFallback(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	t.Cleanup(proc.Free)
	registry, err := mpool.NewAllocationAccountRegistry(1, 32)
	require.NoError(t, err)
	account, err := registry.Open(112)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, account.Snapshot().Used)
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
	})
	allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2, 3, 4)
	require.NoError(t, err)
	typ := types.New(types.T_char, 15, 0)
	input := batch.NewWithSize(1)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	input.Vecs[0] = vector.NewVec(typ)
	require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte("abc"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(input.Vecs[0], nil, true, proc.Mp()))
	input.SetRowCount(2)
	column := &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	integer := func(n int64) *plan.Expr {
		return &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: n}}}}
	}
	executor, err := NewExpressionExecutorWithAllocation(proc, bindTestFunction(t, proc, "substring", column, integer(1), integer(2)), allocation)
	require.NoError(t, err)
	t.Cleanup(executor.Free)
	require.True(t, predicateBorrowsInputs(executor))
	frame := &DictionaryInputFrame{}
	t.Cleanup(func() { frame.Free(proc.Mp()) })
	frame.Begin(input, allocation, proc.Mp())
	stale, err := vector.NewOffHeapVecWithTypeAndAllocation(typ, allocation)
	require.NoError(t, err)
	frame.predicateVectors = []*vector.Vector{stale}
	require.NoError(t, stale.PreExtend(4, proc.Mp()))
	require.EqualValues(t, 112, account.Snapshot().Used)
	// Scoped NULL metadata cannot fit until the optional old bank yields.
	selected, borrowed, err := frame.predicateBatch(proc, input, nil, executor, []int64{1}, allocation)
	require.NoError(t, err)
	require.False(t, borrowed)
	t.Cleanup(func() { selected.Clean(proc.Mp()) })
	require.False(t, selected.HasDictionary())
	require.True(t, selected.Vecs[0].IsNull(0))
	require.Nil(t, frame.predicateVectors[0])
	out, err := executor.Eval(proc, []*batch.Batch{selected}, nil)
	require.NoError(t, err)
	require.True(t, out.IsNull(0))
}
