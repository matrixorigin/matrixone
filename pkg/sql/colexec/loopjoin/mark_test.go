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

package loopjoin

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func markEquality(t *testing.T, typ types.T, op string) *plan.Expr {
	t.Helper()
	fr, err := function.GetFunctionByName(context.Background(), op, []types.Type{typ.ToType(), typ.ToType()})
	require.NoError(t, err)
	return &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: fr.GetEncodedOverloadID(), ObjName: op},
		Args: []*plan.Expr{
			{Typ: plan.Type{Id: int32(typ)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}},
			{Typ: plan.Type{Id: int32(typ)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 0}}},
		},
	}}}
}

func TestMarkHashThreeValuedLogicAndReset(t *testing.T) {
	for _, tc := range []struct {
		name     string
		build    [][]int32
		nullLast bool
		want     []bool
		wantNull []bool
	}{
		{"empty", nil, false, []bool{false, false, false}, []bool{false, false, false}},
		{"empty batch", [][]int32{{}}, false, []bool{false, false, false}, []bool{false, false, false}},
		{"duplicates", [][]int32{{1}, {1}}, false, []bool{true, false, false}, []bool{false, false, true}},
		{"null only", [][]int32{{0}}, true, []bool{false, false, false}, []bool{true, true, true}},
		{"match beats null", [][]int32{{1}, {0}}, true, []bool{true, false, false}, []bool{false, true, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			m, err := newMarkHash(proc, markEquality(t, types.T_int32, "="))
			require.NoError(t, err)
			require.NotNil(t, m)
			join := &LoopJoin{LeftTypes: []types.Type{types.T_int32.ToType()}, JoinType: plan.Node_MARK,
				ResultCols: []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(-1, 0)}}
			join.ctr.markHash = m
			for generation := 0; generation < 2; generation++ {
				var bats []*batch.Batch
				for i, values := range tc.build {
					bat := makeInt32LoopJoinBatch(proc.Mp(), values)
					if tc.nullLast && i == len(tc.build)-1 {
						nulls.Add(bat.Vecs[0].GetNulls(), uint64(len(values)-1))
					}
					bats = append(bats, bat)
				}
				jm := markTestMap(t, proc, bats)
				require.NoError(t, m.build(proc, jm))
				probe := makeInt32LoopJoinBatch(proc.Mp(), []int32{1, 2, 0})
				nulls.Add(probe.Vecs[0].GetNulls(), 2)
				join.ctr.inBat = probe
				join.ctr.probeIdx = 0
				join.resetResultBat()
				result := vm.NewCallResult()
				require.NoError(t, join.ctr.probeMarkHash(join, proc, &result))
				require.Nil(t, join.ctr.inBat)
				require.Equal(t, 3, result.Batch.RowCount())
				require.Equal(t, tc.want, vector.MustFixedColNoTypeCheck[bool](result.Batch.Vecs[1]))
				for i, expected := range tc.wantNull {
					require.Equal(t, expected, result.Batch.Vecs[1].GetNulls().Contains(uint64(i)), "row %d", i)
				}
				join.Reset(proc, false, nil)
				require.Nil(t, m.iter)
				require.False(t, m.hasNull)
				require.False(t, m.hasRows)
				probe.Clean(proc.Mp())
				jm.Free()
			}
			join.Free(proc, false, nil)
			join.Free(proc, false, nil)
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

func TestMarkHashEligibility(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	for _, tc := range []struct {
		typ      types.T
		op       string
		eligible bool
	}{
		{types.T_int32, "=", true}, {types.T_varchar, "=", true},
		{types.T_float64, "=", true}, {types.T_array_float64, "=", false}, {types.T_int32, "<", false},
	} {
		m, err := newMarkHash(proc, markEquality(t, tc.typ, tc.op))
		require.NoError(t, err)
		require.Equal(t, tc.eligible, m != nil)
		if m != nil {
			m.free()
		}
	}
	scaledFloat := markEquality(t, types.T_float32, "=")
	for _, arg := range scaledFloat.GetF().Args {
		arg.Typ.Scale = 2
	}
	require.Nil(t, MarkHashKeys(scaledFloat), "scaled float32 equality rounds its operands")
	cond := markEquality(t, types.T_int32, "=")
	cond.GetF().Args[1].GetCol().RelPos = 0
	m, err := newMarkHash(proc, cond)
	require.NoError(t, err)
	require.Nil(t, m, "both keys depend on probe input")
	cond.GetF().Args[1].GetCol().RelPos = 1
	cond.GetF().Args[0], cond.GetF().Args[1] = cond.GetF().Args[1], cond.GetF().Args[0]
	m, err = newMarkHash(proc, cond)
	require.NoError(t, err)
	require.NotNil(t, m, "reversed equality")
	m.free()
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

type cancelMarkKey struct {
	colexec.ExpressionExecutor
	cancel context.CancelFunc
}

func (key cancelMarkKey) Eval(proc *process.Process, bats []*batch.Batch, sel []bool) (*vector.Vector, error) {
	vec, err := key.ExpressionExecutor.Eval(proc, bats, sel)
	key.cancel()
	return vec, err
}

func TestMarkHashCancellationCleanup(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	m, err := newMarkHash(proc, markEquality(t, types.T_int32, "="))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(proc.Ctx)
	defer cancel()
	proc.Ctx = ctx
	m.keys[1] = cancelMarkKey{m.keys[1], cancel}
	bat := makeInt32LoopJoinBatch(proc.Mp(), []int32{1})
	jm := markTestMap(t, proc, []*batch.Batch{bat})
	require.ErrorIs(t, m.build(proc, jm), context.Canceled)
	require.NotNil(t, m.iter, "failure after receiving shared index")
	m.free()
	m.free()
	require.NotNil(t, jm.NewIterator(), "probe cleanup must not free the shared index")
	jm.Free()
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}
