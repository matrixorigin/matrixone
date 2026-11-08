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

package hashjoin

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func uniqueLeftProbeFixture(t *testing.T, residual bool) (*HashJoin, *hashbuild.HashBuild, *process.Process, *mpool.AllocationAccount) {
	t.Helper()
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())
	key := newExpr(0, types.T_int64.ToType())
	arg := &HashJoin{JoinType: plan.Node_LEFT, NumCPU: 1, HashOnPK: true, JoinMapTag: 94001,
		LeftTypes:  []types.Type{types.T_int64.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_float64.ToType()},
		RightTypes: []types.Type{types.T_int64.ToType()},
		ResultCols: []colexec.ResultPos{{Rel: 0, Pos: 0}, {Rel: 0, Pos: 1}, {Rel: 0, Pos: 2}, {Rel: 0, Pos: 3}, {Rel: 1, Pos: 0}, {Rel: 0, Pos: 1}},
		EqConds:    [][]*plan.Expr{{key}, {key}}}
	if residual {
		arg.NonEqCond = &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}}}
	}
	build := &hashbuild.HashBuild{NeedHashMap: true, Conditions: []*plan.Expr{key}, HashOnPK: true,
		NeedBatches: true, JoinMapTag: arg.JoinMapTag, JoinMapRefCnt: 1}
	registry, err := mpool.NewAllocationAccountRegistry(1, 256)
	require.NoError(t, err)
	account, err := registry.Open(16 << 20)
	require.NoError(t, err)
	require.NoError(t, arg.SetAllocationAccount(account))
	require.NoError(t, build.SetAllocationAccount(account))
	t.Cleanup(func() {
		arg.Reset(proc, false, nil)
		build.Reset(proc, false, nil)
		arg.Free(proc, false, nil)
		build.Free(proc, false, nil)
		for _, op := range []vm.Operator{arg, build} {
			for _, child := range op.GetOperatorBase().Children {
				child.Free(proc, false, nil)
			}
		}
		proc.Free()
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, proc.Mp().CurrNB())
	})
	return arg, build, proc, account
}

// Both the optimized shape and a residual-condition control must satisfy the
// same independent row oracle, including after result reuse and source release.
func TestUniqueLeftProbeCopiesOwnedColumns(t *testing.T) {
	for _, tc := range []struct {
		count   int
		storage string
	}{
		{8191, "plain"}, {8192, "plain"}, {8193, "plain"}, {33, "dead"}, {33, "window"},
	} {
		for _, residual := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/%s/residual=%t", tc.count, tc.storage, residual), func(t *testing.T) {
				arg, build, proc, _ := uniqueLeftProbeFixture(t, residual)
				makeProbe := func(count int) *batch.Batch {
					b := batch.NewWithSize(4)
					for i, typ := range arg.LeftTypes {
						b.Vecs[i] = vector.NewVec(typ)
					}
					b.Vecs[2].Free(proc.Mp())
					var err error
					b.Vecs[2], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte("constant"), count, proc.Mp())
					require.NoError(t, err)
					b.Vecs[2].GetGrouping().Add(1)
					for row := range count {
						require.NoError(t, vector.AppendFixed(b.Vecs[0], int64(row%4+1), row%7 == 0, proc.Mp()))
						value := strings.Repeat(fmt.Sprintf("%02d", row%13), 20)
						require.NoError(t, vector.AppendBytes(b.Vecs[1], []byte(value), row%5 == 0, proc.Mp()))
						require.NoError(t, vector.AppendFixed(b.Vecs[3], math.Copysign(0, float64(row%2)-0.5), row%11 == 0, proc.Mp()))
						if row%3 == 0 {
							b.Vecs[1].GetGrouping().Add(uint64(row))
						}
					}
					if tc.storage == "dead" {
						require.NoError(t, vector.SetBytesAt(b.Vecs[1], 1, []byte(strings.Repeat("dead", 16384)), proc.Mp()))
						require.NoError(t, vector.SetBytesAt(b.Vecs[1], 1, []byte(strings.Repeat("01", 20)), proc.Mp()))
						require.True(t, b.Vecs[1].VarlenaAreaIsDisjoint())
					}
					if tc.storage == "window" {
						parent := vector.NewVec(types.T_varchar.ToType())
						require.NoError(t, vector.AppendBytes(parent, []byte(strings.Repeat("unused", 16384)), false, proc.Mp()))
						for row := range count {
							require.NoError(t, parent.UnionOne(b.Vecs[1], int64(row), proc.Mp()))
						}
						window, err := parent.Window(1, count+1)
						require.NoError(t, err)
						b.Vecs[1].Free(proc.Mp())
						b.Vecs[1] = window
						t.Cleanup(func() { parent.Free(proc.Mp()) })
					}
					b.SetRowCount(count)
					return b
				}
				probe := makeProbe(tc.count)
				next := makeProbe(3)
				arg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probe, next}))
				right := batch.NewWithSize(1)
				right.Vecs[0] = testutil.MakeInt64Vector([]int64{2, 4}, nil, proc.Mp())
				right.SetRowCount(2)
				build.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{right}))
				require.NoError(t, arg.Prepare(proc))
				require.NoError(t, build.Prepare(proc))
				_, err := build.Call(proc)
				require.NoError(t, err)
				seen, batchIndex := 0, 0
				for {
					out, err := arg.Call(proc)
					require.NoError(t, err)
					if out.Batch == nil {
						break
					}
					if out.Batch.RowCount() == 0 {
						continue
					}
					if seen == tc.count {
						seen = 0
						batchIndex++
					}
					source := probe
					if batchIndex == 1 {
						source = next
					}
					// An entire optimized probe is copied before Call returns.
					// Releasing its bytes must not invalidate any returned column.
					if !residual && tc.count <= colexec.DefaultBatchSize && batchIndex == 0 {
						source.CleanOnlyData()
					}
					for row := range out.Batch.RowCount() {
						i := seen + row
						for col, period := range map[int]int{0: 7, 1: 5, 3: 11, 5: 5} {
							require.Equal(t, i%period == 0, out.Batch.Vecs[col].IsNull(uint64(row)))
						}
						if i%7 != 0 {
							require.Equal(t, int64(i%4+1), vector.GetFixedAtNoTypeCheck[int64](out.Batch.Vecs[0], row))
						}
						if i%5 != 0 {
							expected := strings.Repeat(fmt.Sprintf("%02d", i%13), 20)
							require.Equal(t, expected, string(out.Batch.Vecs[1].GetBytesAt(row)))
							require.Equal(t, expected, string(out.Batch.Vecs[5].GetBytesAt(row)))
						}
						require.Equal(t, "constant", string(out.Batch.Vecs[2].GetBytesAt(row)))
						require.Equal(t, i == 1, out.Batch.Vecs[2].GetGrouping().Contains(uint64(row)))
						require.Equal(t, i%3 == 0, out.Batch.Vecs[1].GetGrouping().Contains(uint64(row)))
						if i%11 != 0 {
							require.Equal(t, math.Float64bits(math.Copysign(0, float64(i%2)-0.5)), math.Float64bits(vector.GetFixedAtNoTypeCheck[float64](out.Batch.Vecs[3], row)))
						}
						matched := i%7 != 0 && (i%4+1)%2 == 0
						require.Equal(t, !matched, out.Batch.Vecs[4].IsNull(uint64(row)))
						if matched {
							require.Equal(t, int64(i%4+1), vector.GetFixedAtNoTypeCheck[int64](out.Batch.Vecs[4], row))
						}
					}
					seen += out.Batch.RowCount()
				}
				require.Equal(t, 1, batchIndex)
				require.Equal(t, 3, seen)
			})
		}
	}
}

// Cancel at a real completed column boundary, without timer/scheduler coupling.
type cancelAfterProbeColumn struct {
	context.Context
	arg    *HashJoin
	cancel context.CancelCauseFunc
	cause  error
}

func (c *cancelAfterProbeColumn) Err() error {
	if c.arg.ctr.resBat != nil && c.arg.ctr.resBat.Vecs[0].Length() > 0 {
		c.cancel(c.cause)
	}
	return c.Context.Err()
}

func TestUniqueLeftProbePartialCopyDoesNotPublish(t *testing.T) {
	for _, stop := range []string{"admission", "cancel"} {
		t.Run(stop, func(t *testing.T) {
			arg, build, proc, account := uniqueLeftProbeFixture(t, false)
			probe := batch.NewWithSize(4)
			for i, typ := range arg.LeftTypes {
				probe.Vecs[i] = vector.NewVec(typ)
			}
			for range 32 {
				require.NoError(t, vector.AppendFixed(probe.Vecs[0], int64(2), false, proc.Mp()))
				require.NoError(t, vector.AppendBytes(probe.Vecs[1], []byte(strings.Repeat("x", 4096)), false, proc.Mp()))
				require.NoError(t, vector.AppendBytes(probe.Vecs[2], []byte("constant"), false, proc.Mp()))
				require.NoError(t, vector.AppendFixed(probe.Vecs[3], 0.0, false, proc.Mp()))
			}
			probe.SetRowCount(32)
			arg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probe}))
			right := batch.NewWithSize(1)
			right.Vecs[0] = testutil.MakeInt64Vector([]int64{2}, nil, proc.Mp())
			right.SetRowCount(1)
			build.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{right}))
			require.NoError(t, arg.Prepare(proc))
			require.NoError(t, build.Prepare(proc))
			_, err := build.Call(proc)
			require.NoError(t, err)
			var filler []byte
			defer func() {
				if filler != nil {
					proc.Mp().Free(filler)
				}
			}()
			cause := moerr.NewInternalErrorNoCtx("cancel after first probe column")
			if stop == "admission" {
				filler, err = proc.Mp().AllocAccounted(int(account.Snapshot().Limit-account.Snapshot().Used-32768), account, mpool.AllocationOwnerMin, 1)
				require.NoError(t, err)
			} else {
				ctx, cancel := context.WithCancelCause(proc.Ctx)
				defer cancel(nil)
				proc.Ctx = &cancelAfterProbeColumn{Context: ctx, arg: arg, cancel: cancel, cause: cause}
			}
			out, err := arg.Call(proc)
			require.Error(t, err)
			require.Nil(t, out.Batch)
			require.Equal(t, 32, arg.ctr.resBat.Vecs[0].Length(), "failure occurs after the first real column copy")
			require.Zero(t, arg.ctr.resBat.RowCount(), "partial vectors remain private")
			if stop == "admission" {
				require.True(t, mpool.AllocationFailureReasonOf(err) == mpool.AllocationFailureCapacity || moerr.IsMoErrCode(err, moerr.ErrOOM), "%v", err)
			} else {
				require.Same(t, cause, err)
			}
			arg.Reset(proc, true, err)
			build.Reset(proc, true, err)
			if filler != nil {
				proc.Mp().Free(filler)
				filler = nil
			}
			require.Zero(t, account.Snapshot().Used)
		})
	}
}
