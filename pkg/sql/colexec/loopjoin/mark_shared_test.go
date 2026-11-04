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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func markTestMap(t *testing.T, proc *process.Process, bats []*batch.Batch, keys ...*vector.Vector) *message.JoinMap {
	t.Helper()
	h, err := hashmap.NewStrHashMap(false, proc.Mp())
	require.NoError(t, err)
	it := h.NewIterator()
	rows := 0
	hasNull := false
	for i, bat := range bats {
		key := bat.Vecs[0]
		if len(keys) != 0 {
			key = keys[i]
		}
		rows += bat.RowCount()
		hasNull = hasNull || key.IsConstNull() || key.GetNulls().Any()
		for start := 0; start < bat.RowCount(); start += hashmap.UnitLimit {
			_, _, err = it.Insert(start, min(hashmap.UnitLimit, bat.RowCount()-start), []*vector.Vector{key})
			require.NoError(t, err)
		}
	}
	jm := message.NewJoinMap(message.GroupSels{}, nil, h, nil, bats, proc.Mp())
	jm.IncRef(1)
	jm.SetRowCount(int64(rows))
	jm.SetHasNullKey(hasNull)
	return jm
}

func TestMarkHashSharesReadOnlyIndex(t *testing.T) {
	owner := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	bat := makeInt32LoopJoinBatch(owner.Mp(), []int32{1, 0})
	nulls.Add(bat.Vecs[0].GetNulls(), 1)
	jm := markTestMap(t, owner, []*batch.Batch{bat})
	result := make(chan error, 2)
	var procs []*process.Process
	for i := 0; i < 2; i++ {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		procs = append(procs, proc)
		m, err := newMarkHash(proc, markEquality(t, types.T_int32, "="))
		require.NoError(t, err)
		go func() {
			var err error
			defer func() { m.free(); result <- err }()
			if err = m.build(proc, jm); err != nil {
				return
			}
			for j := 0; j < 1024; j++ {
				vs, zs := m.iter.Find(0, 2, bat.Vecs)
				if !m.hasRows || !m.hasNull || vs[0] == 0 || zs[0] == 0 || zs[1] != 0 {
					err = fmt.Errorf("invalid shared MARK lookup")
					return
				}
			}
			// Reset only this consumer: the index must remain alive for the other.
			m.reset()
		}()
	}
	for range procs {
		require.NoError(t, <-result)
	}
	for _, proc := range procs {
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	}
	values, zs := jm.NewIterator().Find(0, 2, bat.Vecs)
	require.NotZero(t, values[0])
	require.Zero(t, zs[1])
	jm.Free()
	owner.Free()
	require.Zero(t, owner.Mp().CurrNB())
}

func TestMarkHashSideLocalCastsAndGeneralBuild(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	cond := markEquality(t, types.T_int64, "=")
	fr, err := function.GetFunctionByName(context.Background(), "cast", []types.Type{types.T_int32.ToType(), types.T_int64.ToType()})
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: int32(i), ColPos: 0}}}
		target := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_T{T: &plan.TargetType{}}}
		cond.GetF().Args[i] = &plan.Expr{Typ: target.Typ, Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: fr.GetEncodedOverloadID(), ObjName: "cast"}, Args: []*plan.Expr{col, target},
		}}}
	}
	m, err := newMarkHash(proc, cond)
	require.NoError(t, err)
	require.NotNil(t, m)
	bat := makeInt32LoopJoinBatch(proc.Mp(), []int32{1, 0})
	nulls.Add(bat.Vecs[0].GetNulls(), 1)
	buildKey, err := colexec.NewExpressionExecutor(proc, MarkHashKeys(cond)[1])
	require.NoError(t, err)
	vec, err := buildKey.Eval(proc, []*batch.Batch{bat, bat}, nil)
	require.NoError(t, err)
	jm := markTestMap(t, proc, []*batch.Batch{bat}, vec)
	buildKey.Free()
	require.NoError(t, m.build(proc, jm))
	probe, err := m.key.Eval(proc, []*batch.Batch{bat, bat}, nil)
	require.NoError(t, err)
	values, zs := m.iter.Find(0, 2, []*vector.Vector{probe})
	require.NotZero(t, values[0])
	require.Zero(t, zs[1])
	require.True(t, m.hasNull)
	m.free()
	jm.Free()

	m, err = newMarkHash(proc, markEquality(t, types.T_int32, "="))
	require.NoError(t, err)
	jm = message.NewJoinMap(message.GroupSels{}, nil, nil, nil, nil, proc.Mp())
	jm.IncRef(1)
	require.NoError(t, m.build(proc, jm))
	require.Nil(t, m.iter, "a general loop build has no index")
	m.free()
	jm.Free()
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}
