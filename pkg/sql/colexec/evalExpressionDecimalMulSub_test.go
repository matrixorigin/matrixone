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

package colexec

import (
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"strconv"
	"strings"
	"testing"
)

func newTestDecimalAggregateExecutor(p *process.Process, e *plan.Expr) (ExpressionExecutor, error) {
	return newTestDecimalAggregateExecutorWithAllocation(p, e, nil)
}
func newTestDecimalAggregateExecutorWithAllocation(p *process.Process, e *plan.Expr, selection *vector.AllocationAccountSelection) (ExpressionExecutor, error) {
	ex, err := NewExpressionExecutorWithAllocation(p, e, selection)
	if err != nil {
		return nil, err
	}
	return FuseDecimalAggregateArgument(e, ex), nil
}

func mulSubTestPlan(t testing.TB, p *process.Process, commuted bool) *plan.Expr {
	t.Helper()
	narrow := types.New(types.T_decimal64, 15, 2)
	column := func(i int32) *plan.Expr {
		return &plan.Expr{Typ: narrow.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: i}}}
	}
	cast := func(src *plan.Expr, typ types.Type) *plan.Expr {
		// Resolve overload zero through the production cast registry.
		f, err := function.GetFunctionByNameWithOverload(p.Ctx, "cast", []types.Type{types.MustTypeFromPlan(src.Typ), typ}, 0)
		require.NoError(t, err)
		return &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "cast", Obj: f.GetEncodedOverloadID()}, Args: []*plan.Expr{src, {Typ: typ.PlanType(), Expr: &plan.Expr_T{T: &plan.TargetType{}}}}}}}
	}
	c := &plan.Expr{Typ: types.New(types.T_decimal128, 19, 0).PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Decimal128Val{Decimal128Val: &plan.Decimal128{A: 1}}}}}
	x := cast(column(0), types.New(types.T_decimal128, 15, 2))
	y := cast(column(1), types.New(types.T_decimal128, 15, 2))
	sub := bindTestFunction(t, p, "-", c, y)
	root := bindTestFunction(t, p, "*", x, sub)
	if commuted {
		root = bindTestFunction(t, p, "*", sub, x)
	}
	return root
}

func TestDecimalMulSubExpression(t *testing.T) {
	for _, commuted := range []bool{false, true} {
		p := testutil.NewProcess(t)
		defer p.Free()
		ex, err := newTestDecimalAggregateExecutor(p, mulSubTestPlan(t, p, commuted))
		require.NoError(t, err)
		defer ex.Free()
		fused, ok := ex.(*decimalMulSubExecutor)
		require.True(t, ok)
		input := batch.NewWithSize(2)
		defer input.Clean(p.Mp())
		negative := int64(-30000)
		for i, values := range [][]types.Decimal64{{10000, 20000, types.Decimal64(negative)}, {5, 10, 20}} {
			input.Vecs[i] = vector.NewVec(types.New(types.T_decimal64, 15, 2))
			require.NoError(t, vector.AppendFixedList(input.Vecs[i], values, nil, p.Mp()))
		}
		input.SetRowCount(3)
		for _, mask := range [][]bool{nil, {false, true, true}} {
			got, err := ex.Eval(p, []*batch.Batch{input}, mask)
			require.NoError(t, err)
			snapshot := append([]types.Decimal128(nil), vector.MustFixedColNoTypeCheck[types.Decimal128](got)...)
			want, err := fused.root.Eval(p, []*batch.Batch{input}, mask)
			require.NoError(t, err)
			require.Equal(t, *want.GetType(), *got.GetType())
			require.Equal(t, vector.MustFixedColNoTypeCheck[types.Decimal128](want), snapshot)
			// Independent expected rows, including non-prefix selection under a parent.
			for i, s := range []string{"95.0000", "180.0000", "-240.0000"} {
				if mask == nil || mask[i] {
					require.Equal(t, s, vector.GetFixedAtNoTypeCheck[types.Decimal128](got, i).Format(4))
				}
			}
		}
		// The non-NULL fast route did not allocate the skipped cast/subtraction results.
		ex.ResetForNextQuery()
		got, err := ex.Eval(p, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.Equal(t, int32(4), got.GetType().Scale)
		nulls.Add(input.Vecs[1].GetNulls(), 1)
		got, err = ex.Eval(p, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.True(t, got.IsNull(1))
	}
}

func TestDecimalMulSubGenericParentsAndWritableOwnership(t *testing.T) {
	p := testutil.NewProcess(t)
	defer p.Free()
	expr := mulSubTestPlan(t, p, false)
	// Root-only discovery must not recurse into ordinary parent expressions.
	parent := bindTestFunction(t, p, "+", expr, expr)
	ex, err := newTestDecimalAggregateExecutor(p, parent)
	require.NoError(t, err)
	defer ex.Free()
	require.IsType(t, &FunctionExpressionExecutor{}, ex)
	for _, child := range ex.(*FunctionExpressionExecutor).parameterExecutor {
		require.IsType(t, &FunctionExpressionExecutor{}, child)
	}
	input := batch.NewWithSize(2)
	defer input.Clean(p.Mp())
	for i, values := range [][]types.Decimal64{{10000, 20000, 30000}, {5, 10, 20}} {
		input.Vecs[i] = vector.NewVec(types.New(types.T_decimal64, 15, 2))
		require.NoError(t, vector.AppendFixedList(input.Vecs[i], values, nil, p.Mp()))
	}
	input.SetRowCount(3)
	got, err := ex.Eval(p, []*batch.Batch{input}, []bool{false, true, true})
	require.NoError(t, err)
	require.Equal(t, "360.0000", vector.GetFixedAtNoTypeCheck[types.Decimal128](got, 1).Format(4))
	require.Equal(t, "480.0000", vector.GetFixedAtNoTypeCheck[types.Decimal128](got, 2).Format(4))
	owned, err := GetWritableResultFromExpression(p, expr, []*batch.Batch{input})
	require.NoError(t, err)
	defer owned.Free(p.Mp())
	require.Equal(t, "95.0000", vector.GetFixedAtNoTypeCheck[types.Decimal128](owned, 0).Format(4))
}

func TestDecimalMulSubAdmissionAndAllocationFailure(t *testing.T) {
	p := testutil.NewProcess(t)
	defer p.Free()
	expression := mulSubTestPlan(t, p, false)
	// Explicit casts retain their normal evaluator, even when physically widening.
	explicit := proto.Clone(expression).(*plan.Expr)
	explicit.GetF().Args[0].GetF().SyntaxExplicitCast = true
	rejected, err := newTestDecimalAggregateExecutor(p, explicit)
	require.NoError(t, err)
	_, specialized := rejected.(*decimalMulSubExecutor)
	require.False(t, specialized)
	rejected.Free()

	for _, tc := range []struct {
		name   string
		target types.Type
		want   bool
	}{
		{"lossless literal cast", types.New(types.T_decimal128, 21, 2), true},
		{"narrow literal cast", types.New(types.T_decimal128, 18, 0), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bound := proto.Clone(expression).(*plan.Expr)
			sub := bound.GetF().Args[1].GetF()
			sub.Args[0] = bindTestCast(t, p, 0, sub.Args[0], tc.target)
			candidate, err := newTestDecimalAggregateExecutor(p, bound)
			require.NoError(t, err)
			defer candidate.Free()
			_, specialized := candidate.(*decimalMulSubExecutor)
			require.Equal(t, tc.want, specialized)
		})
	}
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.Open(4 << 10)
	require.NoError(t, err)
	defer func() {
		require.Zero(t, account.Snapshot().Used)
		account.Seal()
		_, err := registry.Finalize(account)
		require.NoError(t, err)
	}()
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)
	ex, err := newTestDecimalAggregateExecutorWithAllocation(p, expression, selection)
	require.NoError(t, err)
	defer ex.Free()
	fused := ex.(*decimalMulSubExecutor)
	input := batch.NewWithSize(2)
	defer input.Clean(p.Mp())
	for i := range input.Vecs {
		input.Vecs[i] = vector.NewVec(types.New(types.T_decimal64, 15, 2))
		require.NoError(t, vector.AppendFixedList(input.Vecs[i], make([]types.Decimal64, 8192), nil, p.Mp()))
	}
	input.SetRowCount(8192)
	_, err = ex.Eval(p, []*batch.Batch{input}, nil)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.LessOrEqual(t, account.Snapshot().Used, account.Snapshot().Limit)
	for _, v := range input.Vecs {
		v.SetLength(1)
	}
	input.SetRowCount(1)
	out, err := ex.Eval(p, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Same(t, selection, out.AllocationAccountSelection())
	// Actual avoided intermediates are distinct from a timing assertion.
	for _, child := range fused.root.parameterExecutor {
		if v := child.(*FunctionExpressionExecutor).resultVector.GetResultVector(); v != nil {
			require.Zero(t, v.Length())
		}
	}
	for _, v := range input.Vecs {
		v.SetLength(0)
	}
	input.SetRowCount(0)
	out, err = ex.Eval(p, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Zero(t, out.Length())
}

func BenchmarkDecimalMulSubExpression(b *testing.B) {
	for _, n := range []int{0, 1, 256, 8192} {
		for _, mode := range []string{"generic", "fused", "generic_null", "fused_null", "generic_const", "fused_const", "generic_mask", "fused_mask"} {
			b.Run(strconv.Itoa(n)+"/"+mode, func(b *testing.B) {
				p := testutil.NewProcess(b)
				defer p.Free()
				ex, err := newTestDecimalAggregateExecutor(p, mulSubTestPlan(b, p, false))
				require.NoError(b, err)
				defer ex.Free()
				root := ex.(*decimalMulSubExecutor).root
				input := batch.NewWithSize(2)
				defer input.Clean(p.Mp())
				for i := range input.Vecs {
					input.Vecs[i] = vector.NewVec(types.New(types.T_decimal64, 15, 2))
					values := make([]types.Decimal64, n)
					for j := range values {
						if i == 0 {
							values[j] = 123456
						} else {
							values[j] = 5
						}
					}
					require.NoError(b, vector.AppendFixedList(input.Vecs[i], values, nil, p.Mp()))
				}
				if strings.HasSuffix(mode, "_const") {
					for i, v := range input.Vecs {
						v.Free(p.Mp())
						value := types.Decimal64(5)
						if i == 0 {
							value = 123456
						}
						input.Vecs[i], err = vector.NewConstFixed(types.New(types.T_decimal64, 15, 2), value, n, p.Mp())
						require.NoError(b, err)
					}
				}
				input.SetRowCount(n)
				if strings.HasSuffix(mode, "_null") && n > 0 {
					nulls.Add(input.Vecs[1].GetNulls(), 0)
				}
				var mask []bool
				if strings.HasSuffix(mode, "_mask") {
					mask = make([]bool, n)
					for i := range mask {
						mask[i] = i%2 == 0
					}
				}
				var executor ExpressionExecutor = ex
				if strings.HasPrefix(mode, "generic") {
					executor = root
				}
				_, err = executor.Eval(p, []*batch.Batch{input}, mask)
				require.NoError(b, err)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					_, err = executor.Eval(p, []*batch.Batch{input}, mask)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// Materialized physical metadata can differ from the bound column domain.
// Retain generic cast validation when skipping it would narrow the source.
func TestDecimalMulSubPhysicalWidthFallback(t *testing.T) {
	p := testutil.NewProcess(t)
	defer p.Free()
	ex, err := newTestDecimalAggregateExecutor(p, mulSubTestPlan(t, p, false))
	require.NoError(t, err)
	defer ex.Free()
	input := batch.NewWithSize(2)
	defer input.Clean(p.Mp())
	input.Vecs[0] = vector.NewVec(types.New(types.T_decimal64, 18, 2))
	require.NoError(t, vector.AppendFixed(input.Vecs[0], types.Decimal64(999999999999999999), false, p.Mp()))
	input.Vecs[1] = vector.NewVec(types.New(types.T_decimal64, 15, 2))
	require.NoError(t, vector.AppendFixed(input.Vecs[1], types.Decimal64(5), false, p.Mp()))
	input.SetRowCount(1)
	_, err = ex.Eval(p, []*batch.Batch{input}, nil)
	require.Error(t, err, "the DECIMAL(15,2) widening target cannot accept the physical DECIMAL(18,2) value")
}

func TestDecimalMulSubRepresentations(t *testing.T) {
	for _, shape := range []string{"x_const", "y_const", "both_const", "constant_null", "flat_null", "borrowed_null"} {
		t.Run(shape, func(t *testing.T) {
			p := testutil.NewProcess(t)
			defer p.Free()
			ex, err := newTestDecimalAggregateExecutor(p, mulSubTestPlan(t, p, false))
			require.NoError(t, err)
			defer ex.Free()
			root := ex.(*decimalMulSubExecutor).root
			input := batch.NewWithSize(2)
			defer input.Clean(p.Mp())
			input.SetRowCount(3)
			for i, values := range [][]types.Decimal64{{10000, 20000, 30000}, {5, 10, 20}} {
				typ := types.New(types.T_decimal64, 15, 2)
				constant := shape == "both_const" || shape == "x_const" && i == 0 || shape == "y_const" && i == 1
				if constant {
					input.Vecs[i], err = vector.NewConstFixed(typ, values[0], 3, p.Mp())
					require.NoError(t, err)
				} else {
					input.Vecs[i] = vector.NewVec(typ)
					require.NoError(t, vector.AppendFixedList(input.Vecs[i], values, nil, p.Mp()))
				}
			}
			if shape == "constant_null" {
				input.Vecs[1].Free(p.Mp())
				input.Vecs[1] = vector.NewConstNull(types.New(types.T_decimal64, 15, 2), 3, p.Mp())
			}
			if shape == "flat_null" {
				nulls.Add(input.Vecs[1].GetNulls(), 1)
			}
			if shape == "borrowed_null" {
				validity := []byte{0b101}
				lease, err := vector.NewRefCountedBufferLease(validity, 1, nil)
				require.NoError(t, err)
				require.NoError(t, input.Vecs[1].GetNulls().InstallBorrowedValidity(validity, 0, 3, 1, lease))
				lease.Release()
			}
			for _, mask := range [][]bool{nil, {false, true, true}, {false, false, false}} {
				got, err := ex.Eval(p, []*batch.Batch{input}, mask)
				require.NoError(t, err)
				if shape == "borrowed_null" && mask == nil {
					require.True(t, input.Vecs[1].GetNulls().HasBorrowedValidity())
				}
				values := append([]types.Decimal128(nil), vector.MustFixedColNoTypeCheck[types.Decimal128](got)...)
				isNull := make([]bool, 3)
				for i := range isNull {
					isNull[i] = got.IsNull(uint64(i))
				}
				want, err := root.Eval(p, []*batch.Batch{input}, mask)
				require.NoError(t, err)
				for i := range isNull {
					require.Equal(t, want.IsNull(uint64(i)), isNull[i])
					if !isNull[i] {
						require.Equal(t, vector.GetFixedAtNoTypeCheck[types.Decimal128](want, i), values[i])
					}
				}
			}
		})
	}
}

func TestDecimalAggregateAdmissionIsolation(t *testing.T) {
	p := testutil.NewProcess(t)
	defer p.Free()
	expression := mulSubTestPlan(t, p, false)
	generic, err := NewExpressionExecutor(p, expression)
	require.NoError(t, err)
	require.IsType(t, &FunctionExpressionExecutor{}, generic)
	generic.Free()
	for _, shape := range []string{"shared_root", "shared_sub", "shared_column", "unknown_width", "parameter", "variable", "fallible_literal", "outer_subtraction"} {
		t.Run(shape, func(t *testing.T) {
			e := proto.Clone(expression).(*plan.Expr)
			switch shape {
			case "shared_root":
				e.AuxId = -1
			case "shared_sub":
				e.GetF().Args[1].AuxId = -1
			case "shared_column":
				e.GetF().Args[0].GetF().Args[0].AuxId = -1
			case "unknown_width":
				e.GetF().Args[0].GetF().Args[0].Typ.Width = 0
			case "parameter":
				e.GetF().Args[1].GetF().Args[0].Expr = &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}
			case "variable":
				e.GetF().Args[1].GetF().Args[0].Expr = &plan.Expr_V{V: &plan.VarRef{Name: "constant"}}
			case "fallible_literal":
				sub := e.GetF().Args[1].GetF()
				sub.Args[0] = bindTestCast(t, p, 0, sub.Args[0], types.New(types.T_decimal128, 1, 0))
			case "outer_subtraction":
				e = bindTestFunction(t, p, "-", e, e)
			}
			ex, err := newTestDecimalAggregateExecutor(p, e)
			require.NoError(t, err)
			defer ex.Free()
			_, fused := ex.(*decimalMulSubExecutor)
			require.False(t, fused)
		})
	}
}
