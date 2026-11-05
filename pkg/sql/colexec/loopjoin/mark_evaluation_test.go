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
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestMarkPreservesLateExpressionError(t *testing.T) {
	for _, threshold := range []int64{32 << 20, 100001} {
		for _, match := range []bool{true, false} {
			t.Run(fmt.Sprintf("threshold=%d/match=%t", threshold, match), func(t *testing.T) {
				mp := mpool.MustNewZero()
				proc := testutil.NewProcessWithMPool(t, "", mp)
				proc.SetMessageBoard(message.NewMessageBoard())
				sourceMP := mpool.MustNewZero()
				var inputs []*batch.Batch
				var build *hashbuild.HashBuild
				var join *LoopJoin
				t.Cleanup(func() {
					if join != nil {
						join.Free(proc, false, nil)
					}
					if build != nil {
						build.Free(proc, false, nil)
					}
					for _, b := range inputs {
						b.Clean(sourceMP)
					}
					proc.GetMessageBoard().Reset()
					proc.Free()
					require.Zero(t, mp.CurrNB())
					require.Zero(t, mp.AllocatedBytes())
					require.Zero(t, sourceMP.CurrNB())
					require.Zero(t, sourceMP.AllocatedBytes())
				})
				makeBatch := func(v []int64) *batch.Batch {
					b := batch.NewWithSize(1)
					inputs = append(inputs, b)
					b.Vecs[0] = testutil.MakeInt64Vector(v, nil, sourceMP)
					b.SetRowCount(len(v))
					return b
				}
				first := makeBatch(make([]int64, colexec.DefaultBatchSize))
				last := makeBatch([]int64{math.MaxInt64})
				probeValue := int64(1)
				if !match {
					probeValue = 2
				}
				probe := makeBatch([]int64{probeValue})
				cond := markEquality(t, types.T_int64, "=")
				fn, err := function.GetFunctionByName(context.Background(), "+", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()})
				require.NoError(t, err)
				plus := func(side int32) *plan.Expr {
					return &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: "+"}, Args: []*plan.Expr{
						{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: side, ColPos: 0}}},
						{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}}},
					}}}}
				}
				cond.GetF().Args[1] = plus(1)
				// Use the same eligibility decision as the compiler, not a synthetic map.
				indexed := MarkHashKeys(cond) != nil
				build = &hashbuild.HashBuild{NeedHashMap: indexed, OptionalIndex: indexed, NeedBatches: true, Conditions: []*plan.Expr{plus(0)}, JoinMapTag: 301, JoinMapRefCnt: 1, SpillThreshold: threshold}
				build.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, last}))
				join = &LoopJoin{JoinType: plan.Node_MARK, NonEqCond: cond, JoinMapTag: 301, LeftTypes: []types.Type{types.T_int64.ToType()}, ResultCols: []colexec.ResultPos{colexec.NewResultPos(-1, 0)}}
				join.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probe}))
				require.NoError(t, build.Prepare(proc))
				require.NoError(t, join.Prepare(proc))
				_, err = vm.Exec(build, proc)
				require.NoError(t, err, "building must not evaluate the later overflowing key")
				res, err := vm.Exec(join, proc)
				if match {
					require.NoError(t, err)
					require.Equal(t, []bool{true}, vector.MustFixedColNoTypeCheck[bool](res.Batch.Vecs[0]))
					require.False(t, res.Batch.Vecs[0].GetNulls().Any())
				} else {
					require.Error(t, err, "the general evaluator must still report a reached overflow")
				}
			})
		}
	}
}

func TestMarkProbeScratchMultipleWorkers(t *testing.T) {
	for _, limit := range []int64{2 << 20, 64 << 20} {
		t.Run(fmt.Sprintf("cap=%d", limit), func(t *testing.T) {
			mp, err := mpool.NewMPool("mark probe scratch", limit, mpool.NoFixed)
			require.NoError(t, err)
			owner := testutil.NewProcessWithMPool(t, "", mp)
			owner.SetMessageBoard(message.NewMessageBoard())
			sourceMP := mpool.MustNewZero()
			source := batch.NewWithSize(1)
			probe := batch.NewWithSize(1)
			var build *hashbuild.HashBuild
			var joins []*LoopJoin
			var workers []*process.Process
			t.Cleanup(func() {
				for i, j := range joins {
					j.Free(workers[i], false, nil)
					workers[i].Free()
				}
				if build != nil {
					build.Free(owner, false, nil)
				}
				owner.GetMessageBoard().Reset()
				owner.Free()
				source.Clean(sourceMP)
				probe.Clean(sourceMP)
				require.Zero(t, mp.CurrNB())
				require.Zero(t, mp.AllocatedBytes())
				require.Zero(t, sourceMP.CurrNB())
				require.Zero(t, sourceMP.AllocatedBytes())
			})
			source.Vecs[0] = testutil.MakeVarcharVector([]string{"present"}, nil, sourceMP)
			source.SetRowCount(1)
			// A successful first window followed by wide keys also tests rollback of
			// partially assembled output if scratch allocation forces fallback.
			values := make([]string, colexec.DefaultBatchSize+1)
			for i := range values {
				values[i] = "present"
				if i >= hashmap.UnitLimit && i < 2*hashmap.UnitLimit {
					values[i] = strings.Repeat("x", 65535)
				}
			}
			probe.Vecs[0] = testutil.MakeVarcharVector(values, []uint64{2*hashmap.UnitLimit - 1}, sourceMP)
			probe.SetRowCount(len(values))
			build = &hashbuild.HashBuild{NeedHashMap: true, NeedBatches: true, OptionalIndex: true, Conditions: []*plan.Expr{{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}}, JoinMapTag: 302, JoinMapRefCnt: 4}
			build.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{source}))
			require.NoError(t, build.Prepare(owner))
			_, err = vm.Exec(build, owner)
			require.NoError(t, err)
			for i := 0; i < 4; i++ {
				proc := testutil.NewProcessWithMPool(t, "", mp)
				proc.SetMessageBoard(owner.GetMessageBoard())
				workers = append(workers, proc)
				j := &LoopJoin{JoinType: plan.Node_MARK, NonEqCond: markEquality(t, types.T_varchar, "="), JoinMapTag: 302, LeftTypes: []types.Type{types.T_varchar.ToType()}, ResultCols: []colexec.ResultPos{colexec.NewResultPos(-1, 0)}}
				joins = append(joins, j)
				j.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probe}))
				require.NoError(t, j.Prepare(proc))
			}
			type answer struct {
				result vm.CallResult
				err    error
			}
			runWorkers := func() []answer {
				results := make([]chan answer, 4)
				for i, j := range joins {
					results[i] = make(chan answer, 1)
					go func(i int, j *LoopJoin) { r, e := vm.Exec(j, workers[i]); results[i] <- answer{r, e} }(i, j)
				}
				// Drain every worker before assertions/cleanup, including cancellation.
				answers := make([]answer, 4)
				for i := range results {
					answers[i] = <-results[i]
				}
				return answers
			}
			answers := runWorkers()
			retained := 0
			for i, a := range answers {
				require.NoError(t, a.err)
				require.NotNil(t, joins[i].ctr.markHash.iter, "real shared optional index was admitted")
				require.Equal(t, colexec.DefaultBatchSize, a.result.Batch.RowCount())
				actual := vector.MustFixedColNoTypeCheck[bool](a.result.Batch.Vecs[0])
				for row := range actual {
					require.Equal(t, row < hashmap.UnitLimit || row >= 2*hashmap.UnitLimit, actual[row])
					require.Equal(t, row == 2*hashmap.UnitLimit-1, a.result.Batch.Vecs[0].GetNulls().Contains(uint64(row)))
				}
				retained += hashmap.StrIteratorCapacity(joins[i].ctr.markHash.iter)
				// A fallback must not lose probeIdx or reuse a cleared key vector at
				// the next Call boundary, after the normal output batch is full.
				last, err := vm.Exec(joins[i], workers[i])
				require.NoError(t, err)
				require.Equal(t, []bool{true}, vector.MustFixedColNoTypeCheck[bool](last.Batch.Vecs[0]))
				require.False(t, last.Batch.Vecs[0].GetNulls().Any())
			}
			require.Zero(t, retained, "lookup must not retain uncharged Go-heap key buffers across workers")
			require.LessOrEqual(t, mp.Stats().HighWaterMark.Load(), limit)
			t.Logf("workers=4 cap=%d peak=%d retained key buffers=%d", limit, mp.Stats().HighWaterMark.Load(), retained)
			reset := func(input *batch.Batch) {
				for i, j := range joins {
					j.Reset(workers[i], false, nil)
				}
				build.Reset(owner, false, nil)
				owner.GetMessageBoard().Reset()
				build.SetChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{source}), 0)
				require.NoError(t, build.Prepare(owner))
				for i, j := range joins {
					j.SetChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}), 0)
					require.NoError(t, j.Prepare(workers[i]))
				}
				_, err := vm.Exec(build, owner)
				require.NoError(t, err)
			}
			if limit == 2<<20 {
				reset(probe)
				old := workers[0].Ctx
				ctx, cancel := context.WithCancel(old)
				t.Cleanup(cancel)
				workers[0].Ctx = ctx
				calls := 0
				joins[0].ctr.expr = cancelMarkKey{joins[0].ctr.expr, func() { calls++; cancel() }}
				answers = runWorkers()
				require.ErrorIs(t, answers[0].err, context.Canceled)
				require.Nil(t, answers[0].result.Batch)
				require.Equal(t, 1, calls)
				require.NotNil(t, joins[0].ctr.markHash.iter, "cancellation is in probe-scratch fallback, not build fallback")
				for _, a := range answers[1:] {
					require.NoError(t, a.err)
				}
				workers[0].Ctx = old
				joins[0].ctr.expr = joins[0].ctr.expr.(cancelMarkKey).ExpressionExecutor
			}
			reset(source)
			for i, a := range runWorkers() {
				require.NoError(t, a.err)
				require.NotNil(t, joins[i].ctr.markHash.iter)
				require.Equal(t, []bool{true}, vector.MustFixedColNoTypeCheck[bool](a.result.Batch.Vecs[0]))
				require.False(t, a.result.Batch.Vecs[0].GetNulls().Any())
				require.Zero(t, hashmap.StrIteratorCapacity(joins[i].ctr.markHash.iter))
			}
		})
	}
}
