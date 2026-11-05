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

package top

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// 与 main 的回归保持同一契约，只覆盖本分支已有的内存与 spill 路径。
func TestTopTypedNullLayout(t *testing.T) {
	// spill 按 map 遍历读取批次，输入次序不保证读取次序；空 payload 的确定性复用由 Vector UT 覆盖。
	for _, tt := range []struct {
		name  string
		typ   types.Type
		limit int64
		input []any
		want  []any
	}{
		{"ordinary_then_null", types.T_int64.ToType(), 2, []any{int64(2), nil}, []any{int64(2), nil}},
		{"null_then_ordinary", types.T_int64.ToType(), 2, []any{nil, int64(2)}, []any{int64(2), nil}},
		{"heap_replacement", types.T_int64.ToType(), 1, []any{int64(1), nil, int64(2)}, []any{int64(2)}},
		{"varchar", types.T_varchar.ToType(), 2, []any{"b", nil}, []any{"b", nil}},
		{"spill_ordinary_then_null", types.T_int64.ToType(), int64(topSpillThreshold + 1), []any{int64(2), nil}, []any{int64(2), nil}},
		{"spill_null_then_ordinary", types.T_int64.ToType(), int64(topSpillThreshold + 1), []any{nil, int64(2)}, []any{int64(2), nil}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tc, child := newTopLayoutFixture(t, tt.typ, tt.limit)
			for _, value := range tt.input {
				appendTopLayoutBatch(t, tc, child, value)
				child.WithBatchs([]*batch.Batch{batch.EmptyBatch})
			}
			require.NoError(t, tc.arg.Prepare(tc.proc))
			require.Equal(t, tt.limit > int64(topSpillThreshold), tc.arg.ctr.spilling)
			require.Equal(t, tt.want, collectTopLayoutRows(t, tc))
		})
	}
}

func TestTopRepeatedOrderKey(t *testing.T) {
	for _, mode := range []struct {
		name  string
		limit int64
	}{
		{"resident", 1},
		{"spill", int64(topSpillThreshold + 1)},
	} {
		for _, tt := range []struct {
			name          string
			first, second plan.OrderBySpec_OrderByFlag
			input, want   []any
		}{
			{"ascending", plan.OrderBySpec_ASC, plan.OrderBySpec_DESC,
				[]any{int64(2), int64(1)}, []any{int64(1), int64(2)}},
			{"descending", plan.OrderBySpec_DESC, plan.OrderBySpec_ASC,
				[]any{int64(1), int64(2)}, []any{int64(2), int64(1)}},
			{"ascending_null", plan.OrderBySpec_ASC, plan.OrderBySpec_DESC,
				[]any{int64(2), nil, int64(1)}, []any{nil, int64(1), int64(2)}},
			{"descending_null", plan.OrderBySpec_DESC, plan.OrderBySpec_ASC,
				[]any{nil, int64(1), int64(2)}, []any{int64(2), int64(1), nil}},
			{"nulls_first", plan.OrderBySpec_DESC | plan.OrderBySpec_NULLS_FIRST, plan.OrderBySpec_DESC | plan.OrderBySpec_NULLS_LAST,
				[]any{int64(1), nil, int64(2)}, []any{nil, int64(2), int64(1)}},
			{"nulls_last", plan.OrderBySpec_ASC | plan.OrderBySpec_NULLS_LAST, plan.OrderBySpec_ASC | plan.OrderBySpec_NULLS_FIRST,
				[]any{nil, int64(2), int64(1)}, []any{int64(1), int64(2), nil}},
		} {
			t.Run(mode.name+"/"+tt.name, func(t *testing.T) {
				tc, child := newTopLayoutFixture(t, types.T_int64.ToType(), mode.limit)
				tc.arg.Fs[0].Flag = tt.first
				tc.arg.Fs = append(tc.arg.Fs, &plan.OrderBySpec{Expr: tc.arg.Fs[0].Expr, Flag: tt.second})
				for _, value := range tt.input {
					appendTopLayoutBatch(t, tc, child, value)
				}
				require.NoError(t, tc.arg.Prepare(tc.proc))
				require.Equal(t, mode.limit > int64(topSpillThreshold), tc.arg.ctr.spilling)
				want := tt.want
				if mode.limit == 1 {
					want = want[:1]
				}
				require.Equal(t, want, collectTopLayoutRows(t, tc))
			})
		}
	}
}

func TestTopTypedNullLayoutComputedKey(t *testing.T) {
	tc, child := newTopLayoutFixture(t, types.T_int64.ToType(), 2)
	computed, err := plan2.BindFuncExprImplByPlanExpr(tc.proc.Ctx, "+", []*plan.Expr{
		tc.arg.Fs[0].Expr, plan2.MakePlan2Int64ConstExprWithType(1),
	})
	require.NoError(t, err)
	tc.arg.Fs = append([]*plan.OrderBySpec{{Expr: computed, Flag: plan.OrderBySpec_DESC}}, tc.arg.Fs...)
	appendTopLayoutBatch(t, tc, child, int64(2))
	appendTopLayoutBatch(t, tc, child, nil)
	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.Equal(t, []any{int64(2), nil}, collectTopLayoutRows(t, tc))
}

func TestTopTypedNullLayoutResetAndReuse(t *testing.T) {
	tc, child := newTopLayoutFixture(t, types.T_int64.ToType(), 2)
	for generation, input := range [][]any{{int64(2), nil}, {nil, int64(2)}} {
		if generation > 0 {
			child.Free(tc.proc, false, nil)
			tc.arg.Reset(tc.proc, false, nil)
		}
		for _, value := range input {
			appendTopLayoutBatch(t, tc, child, value)
		}
		require.NoError(t, tc.arg.Prepare(tc.proc))
		require.Equal(t, []any{int64(2), nil}, collectTopLayoutRows(t, tc))
	}
}

func TestTopTypedNullLayoutPartialEvalError(t *testing.T) {
	tc, child := newTopLayoutFixture(t, types.T_int64.ToType(), 2)
	tc.arg.Fs = append(tc.arg.Fs, &plan.OrderBySpec{Expr: tc.arg.Fs[0].Expr})
	appendTopLayoutBatch(t, tc, child, nil)
	require.NoError(t, tc.arg.Prepare(tc.proc))
	tc.arg.ctr.executorsForOrderColumn[1].Free()
	tc.arg.ctr.executorsForOrderColumn[1] = topLayoutErrorExecutor{}
	_, err := vm.Exec(tc.arg, tc.proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, tc.arg.ctr.bat)
}

func newTopLayoutFixture(t *testing.T, typ types.Type, limit int64) (testCase, *colexec.MockOperator) {
	t.Helper()
	expr := newExpression(0)
	expr.Typ = plan.Type{Id: int32(typ.Oid), Width: typ.Width, Scale: typ.Scale}
	tc := newTestCase(t, mpool.MustNewZero(), []types.Type{typ}, limit,
		[]*plan.OrderBySpec{{Expr: expr, Flag: plan.OrderBySpec_DESC}})
	child := colexec.NewMockOperator()
	tc.arg.AppendChild(child)
	t.Cleanup(func() {
		var spillPath string
		if tc.arg.ctr.spillFile != nil {
			spillPath = tc.arg.ctr.spillFile.Name()
		}
		tc.arg.Free(tc.proc, false, nil)
		child.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Zero(t, tc.proc.Mp().CurrNB())
		if spillPath != "" {
			require.NoFileExists(t, spillPath)
		}
	})
	return tc, child
}

func appendTopLayoutBatch(t *testing.T, tc testCase, child *colexec.MockOperator, value any) {
	t.Helper()
	bat := batch.NewWithSize(1)
	// 先转交清理责任，后续分配或断言失败也不会遗留向量。
	child.WithBatchs([]*batch.Batch{bat})
	if value == nil {
		bat.Vecs[0] = vector.NewConstNull(tc.types[0], 1, tc.proc.Mp())
	} else {
		bat.Vecs[0] = vector.NewVec(tc.types[0])
		switch v := value.(type) {
		case int64:
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], v, false, tc.proc.Mp()))
		case string:
			require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(v), false, tc.proc.Mp()))
		default:
			t.Fatalf("unsupported test value %T", value)
		}
	}
	bat.SetRowCount(1)
}

func collectTopLayoutRows(t *testing.T, tc testCase) []any {
	t.Helper()
	batches := tc.arg.GetChildren(0).(*colexec.MockOperator).GetBatchs()
	inputs := make([]*vector.Vector, 0, len(batches))
	constNull := make([]bool, 0, len(batches))
	for _, bat := range batches {
		if bat == batch.EmptyBatch {
			continue
		}
		inputs = append(inputs, bat.Vecs[0])
		constNull = append(constNull, bat.Vecs[0].IsConstNull())
	}
	var got []any
	for {
		result, err := vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		if result.Batch == nil || result.Status == vm.ExecStop {
			break
		}
		require.Len(t, result.Batch.Vecs, 1)
		vec := result.Batch.Vecs[0]
		require.Equal(t, tc.types[0].Oid, vec.GetType().Oid)
		for row := 0; row < result.Batch.RowCount(); row++ {
			if vec.IsNull(uint64(row)) {
				got = append(got, nil)
			} else if tc.types[0].Oid == types.T_int64 {
				got = append(got, vector.MustFixedColWithTypeCheck[int64](vec)[row])
			} else {
				got = append(got, string(vec.GetBytesAt(row)))
			}
		}
	}
	index := 0
	for _, bat := range batches {
		if bat == batch.EmptyBatch {
			continue
		}
		require.Len(t, bat.Vecs, 1)
		require.Same(t, inputs[index], bat.Vecs[0])
		require.Equal(t, tc.types[0].Oid, bat.Vecs[0].GetType().Oid)
		require.Equal(t, constNull[index], bat.Vecs[0].IsConstNull())
		index++
	}
	return got
}

type topLayoutErrorExecutor struct{}

func (topLayoutErrorExecutor) Eval(*process.Process, []*batch.Batch, []bool) (*vector.Vector, error) {
	return nil, context.Canceled
}

func (topLayoutErrorExecutor) EvalWithoutResultReusing(*process.Process, []*batch.Batch, []bool) (*vector.Vector, error) {
	return nil, context.Canceled
}

func (topLayoutErrorExecutor) ResetForNextQuery() {}
func (topLayoutErrorExecutor) Free()              {}
func (topLayoutErrorExecutor) IsColumnExpr() bool { return false }
func (topLayoutErrorExecutor) TypeName() string   { return "top layout test error" }
