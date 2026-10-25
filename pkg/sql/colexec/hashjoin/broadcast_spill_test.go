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
	"errors"
	"fmt"
	"os"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestBroadcastSpillSingleFinalizer(t *testing.T) {
	for _, mode := range []struct {
		name  string
		typ   plan.Node_JoinType
		right bool
	}{{"full", plan.Node_OUTER, true}, {"right semi", plan.Node_SEMI, true}, {"right anti", plan.Node_ANTI, true}, {"left", plan.Node_LEFT, false}} {
		for _, emptyFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/empty-first=%t", mode.name, emptyFirst), func(t *testing.T) {
				proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
				defer proc.Free()
				proc.SetMessageBoard(message.NewMessageBoard())
				right := []oracleJoinRow{{1, 10, false}, {1, 11, false}, {2, 12, false}}
				lefts := [][]oracleJoinRow{{{1, 20, false}}, {{3, 21, false}}}
				if emptyFirst {
					lefts[0] = nil
				}
				rightBat := skewTestBatch(proc, right)
				defer rightBat.Clean(proc.Mp())
				joins := [2]*HashJoin{}
				channel := make(chan *bitmap.Bitmap, 2)
				for i := range joins {
					arg := &HashJoin{JoinType: mode.typ, IsRightJoin: mode.right, LeftTypes: []types.Type{types.T_int32.ToType(), types.T_int32.ToType()}, RightTypes: []types.Type{types.T_int32.ToType(), types.T_int32.ToType()}, EqConds: [][]*plan.Expr{{newExpr(0, types.T_int32.ToType())}, {newExpr(0, types.T_int32.ToType())}}, ResultCols: []colexec.ResultPos{colexec.NewResultPos(0, 1), colexec.NewResultPos(1, 1)}, JoinMapTag: 81, NumCPU: 2, IsMerger: i == 0, Channel: channel, SpillThreshold: 2, SpillScopeID: 1, SpillExpectedScopes: []uint32{1}}
					arg.ParallelID = int32(i)
					if mode.typ == plan.Node_SEMI || mode.typ == plan.Node_ANTI {
						arg.ResultCols = []colexec.ResultPos{colexec.NewResultPos(1, 1)}
					}
					input := skewTestBatch(proc, lefts[i])
					defer input.Clean(proc.Mp())
					resetChildrenWithBatch(arg, input)
					joins[i] = arg
					defer arg.Free(proc, false, nil)
				}
				require.NoError(t, joins[0].SealSpillScope(proc, []int32{0, 1}))
				proc.Base.ImportMemory = &process.ImportMemory{UnitBytes: 1, JoinInputBytes: 8 << 20}
				for _, arg := range joins {
					require.NoError(t, arg.Prepare(proc))
				}
				build := &hashbuild.HashBuild{NeedHashMap: true, NeedBatches: true, NeedAllocateSels: true, CanSpill: true, Conditions: joins[0].EqConds[1], JoinMapTag: 81, JoinMapRefCnt: 2, SpillThreshold: 2, SpillExpectedScopes: []uint32{1}}
				resetHashBuildChildrenWithBatch(build, rightBat)
				require.NoError(t, build.Prepare(proc))
				defer build.Free(proc, false, nil)
				_, err := build.Call(proc)
				require.NoError(t, err)
				first, err := joins[0].Call(proc)
				require.NoError(t, err)
				require.Nil(t, first.Batch)
				require.Equal(t, vm.ExecStop, first.Status)
				pipeline.New(0, nil, joins[0]).Cleanup(proc.NewContextChildProc(0), false, false, nil)
				require.Empty(t, channel, "spill Reset cannot send to the old bitmap protocol")
				actual := []string{}
				for calls := 0; calls < 100; calls++ {
					result, err := joins[1].Call(proc)
					require.NoError(t, err)
					if result.Batch == nil {
						break
					}
					for row := 0; row < result.Batch.RowCount(); row++ {
						values := []string{}
						for _, vec := range result.Batch.Vecs {
							index := row
							if vec.IsConst() {
								index = 0
							}
							if vec.IsConstNull() || vec.GetNulls().Contains(uint64(index)) {
								values = append(values, "NULL")
							} else {
								values = append(values, fmt.Sprint(vector.MustFixedColNoTypeCheck[int32](vec)[index]))
							}
						}
						actual = append(actual, fmt.Sprint(values))
					}
					require.Less(t, calls, 99)
				}
				combined := append(append([]oracleJoinRow(nil), lefts[0]...), lefts[1]...)
				expected, wantError := skewOracle(joins[1], combined, right, false)
				require.False(t, wantError)
				sort.Strings(actual)
				require.Equal(t, expected, actual)
				joins[1].Reset(proc, false, nil)
				build.Reset(proc, false, nil)
				require.Empty(t, channel, "the last registrant is a logical finalizer even when ParallelID != 0")
			})
		}
	}
}

func TestBroadcastSpillFreeWithoutResetClosesClaim(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	group := message.NewJoinSpillGroup()
	epoch := group.Seal(proc.Ctx, []uint64{1})
	file, err := os.CreateTemp(t.TempDir(), "claimed-build")
	require.NoError(t, err)
	require.NoError(t, group.PublishBuild([]*os.File{file}))
	work, err := group.Register(epoch, 1, nil)
	require.NoError(t, err)
	require.NotNil(t, work)
	join := &HashJoin{SpillGroup: group, SpillEpoch: epoch}
	join.ctr.spillWork = work
	join.ctr.spillGeneration = true
	join.ctr.spillParticipant = spillParticipantFinalizer
	reader, err := work.Build.Reader(0)
	require.NoError(t, err)
	defer reader.Close()
	join.Free(proc, true, errors.New("probe failed before Reset"))
	require.Error(t, work.Ctx.Err())
	_, err = file.Stat()
	require.NoError(t, err, "Free cannot close a file still owned by a reader")
	require.NoError(t, reader.Close())
	_, err = file.Stat()
	require.Error(t, err, "Free retires the work and the last reader closes its FD")
	join.Free(proc, true, nil)
}

func TestBroadcastParticipantTemplateAndEpoch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	proc.SetMessageBoard(message.NewMessageBoard())
	template := &HashJoin{JoinMapTag: 93, SpillScopeID: 1, SpillExpectedScopes: []uint32{1}}
	require.NoError(t, template.SealSpillScope(proc, []int32{0, 1}))
	group, epoch := template.SpillGroup, template.SpillEpoch
	publish := func() *os.File {
		f, err := os.CreateTemp(t.TempDir(), "generation")
		require.NoError(t, err)
		t.Cleanup(func() { _ = f.Close() })
		require.NoError(t, group.PublishBuild([]*os.File{f}))
		return f
	}
	file := publish()
	template.Reset(proc, false, nil)
	template.Free(proc, false, nil)
	template.Free(proc, true, errors.New("nonexecuting template has no claim"))
	_, err := file.Stat()
	require.NoError(t, err, "sealed template cannot retire actual participants")
	work, err := group.Register(epoch, uint64(1)<<32, nil)
	require.NoError(t, err)
	require.Nil(t, work)
	work, err = group.Register(epoch, uint64(1)<<32|1, nil)
	require.NoError(t, err)
	require.NotNil(t, work)
	work.Close()

	// The actual rearms against a new epoch; its previous Reset/Free is terminal.
	actual := &HashJoin{JoinMapTag: 93, SpillScopeID: 1, SpillExpectedScopes: []uint32{1}}
	next := group.BeginScopes(proc.Ctx, []uint32{1})
	require.Greater(t, next, epoch)
	require.NoError(t, actual.ArmSpillParticipant(proc))
	file = publish()
	actual.ctr.state = End
	actual.ctr.spillParticipant = spillParticipantRegistered
	actual.Reset(proc, false, nil)
	actual.Free(proc, false, nil)
	require.Equal(t, spillParticipantRetired, actual.ctr.spillParticipant)
	template.Reset(proc, true, errors.New("stale template"))
	template.Free(proc, true, nil)
	_, err = file.Stat()
	require.NoError(t, err, "retired and stale nonowners cannot abort the next generation")
	work, err = group.Register(next, uint64(1)<<32, nil)
	require.NoError(t, err)
	require.NotNil(t, work)
	work.Close()

	newEpoch := group.BeginScopes(proc.Ctx, []uint32{1})
	require.NoError(t, actual.ArmSpillParticipant(proc))
	require.Equal(t, newEpoch, actual.SpillEpoch)
	require.Equal(t, spillParticipantExpected, actual.ctr.spillParticipant)
	actual.Free(proc, true, errors.New("new actual failed"))
	require.Error(t, group.SealScope(newEpoch, 1, []int32{0}), "rearmed actual failure still aborts")
}

func TestBroadcastParticipantPrepareFailure(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	proc.SetMessageBoard(message.NewMessageBoard())
	budget := rscthrottler.NewMemThrottler(t.Name(), .9, rscthrottler.WithConstLimit(1<<30))
	working, err := budget.(rscthrottler.WorkingSetThrottler).ReserveWorkingSetCandidatesUntilFit(proc.Ctx, []rscthrottler.WorkingSetSpec{{UnitBytes: 1, Inputs: []rscthrottler.InputSlotSpec{{Name: "source"}, {Name: "output", Count: 1, Bytes: 1}, {Name: "join", Count: 2, Bytes: 1}}, IOBytes: 1}})
	require.NoError(t, err)
	working.Close() // deterministic failure of the first actual Prepare lease
	proc.Base.ImportMemory = &process.ImportMemory{WorkingSet: working, UnitBytes: 1}
	join := &HashJoin{JoinMapTag: 94, SpillScopeID: 1, SpillExpectedScopes: []uint32{1}}
	defer join.Free(proc, true, nil)
	group, err := proc.GetMessageBoard().BroadcastSpillGroup(94, []uint32{1})
	require.NoError(t, err)
	file, err := os.CreateTemp(t.TempDir(), "unregistered")
	require.NoError(t, err)
	defer file.Close()
	require.NoError(t, group.PublishBuild([]*os.File{file}))
	err = join.Prepare(proc)
	require.Error(t, err)
	require.Equal(t, spillParticipantExpected, join.ctr.spillParticipant, "claim precedes every Prepare allocation")
	pipeline.New(0, nil, join).Cleanup(proc.NewContextChildProc(0), true, false, err)
	require.Equal(t, spillParticipantRetired, join.ctr.spillParticipant)
	_, err = file.Stat()
	require.Error(t, err, "actual that cannot register retires the collected source")
	_, err = group.Register(group.Epoch(), uint64(1)<<32, nil)
	require.Error(t, err, "late actual observes terminal state instead of waiting")
}

func TestBroadcastParticipantMemoryCleanup(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty-build=%t", empty), func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			proc.SetMessageBoard(message.NewMessageBoard())
			var right []oracleJoinRow
			if !empty {
				right = []oracleJoinRow{{1, 7, false}}
			}
			bat := skewTestBatch(proc, right)
			defer bat.Clean(proc.Mp())
			joins := [2]*HashJoin{}
			channel := make(chan *bitmap.Bitmap, 2)
			for i := range joins {
				join := &HashJoin{JoinType: plan.Node_INNER, JoinMapTag: 95, SpillScopeID: 1, SpillExpectedScopes: []uint32{1}, EqConds: [][]*plan.Expr{{newExpr(0, types.T_int32.ToType())}, {newExpr(0, types.T_int32.ToType())}}, NumCPU: 2, IsMerger: i == 0, Channel: channel}
				join.ParallelID = int32(i)
				join.AppendChild(colexec.NewMockOperator()) // no probe rows
				defer join.Free(proc, false, nil)
				joins[i] = join
			}
			require.NoError(t, joins[0].SealSpillScope(proc, []int32{0, 1}))
			for _, join := range joins {
				require.NoError(t, join.Prepare(proc))
			}
			build := &hashbuild.HashBuild{NeedHashMap: true, NeedBatches: true, NeedAllocateSels: true, CanSpill: true, Conditions: joins[0].EqConds[1], JoinMapTag: 95, JoinMapRefCnt: 2, SpillThreshold: 1 << 30, SpillExpectedScopes: []uint32{1}}
			resetHashBuildChildrenWithBatch(build, bat)
			defer build.Free(proc, false, nil)
			require.NoError(t, build.Prepare(proc))
			_, err := build.Call(proc)
			require.NoError(t, err)
			var released atomic.Int32
			jm, err := message.ReceiveJoinMap(95, false, 0, proc.GetMessageBoard(), proc.Ctx)
			require.NoError(t, err)
			if jm != nil {
				jm.SetMemoryRelease(func() { released.Add(1) })
			}
			for i, join := range joins {
				result, err := join.Call(proc)
				require.NoError(t, err)
				require.Equal(t, vm.ExecStop, result.Status)
				pipeline.New(0, nil, join).Cleanup(proc.NewContextChildProc(0), false, false, nil)
				join.Reset(proc, false, nil)
				join.Free(proc, false, nil)
				if i == 0 {
					require.NoError(t, join.SpillGroup.SealScope(join.SpillEpoch, 1, []int32{0, 1}), "first normal completion cannot abort/finish the group")
					require.Zero(t, released.Load())
				}
			}
			if !empty {
				require.EqualValues(t, 1, released.Load(), "two actuals retire the normal map exactly once")
			}
			require.Error(t, joins[0].SpillGroup.SealScope(joins[0].SpillEpoch, 1, []int32{0, 1}), "all normal/empty actuals finish the generation")
			build.Reset(proc, false, nil)
		})
	}
}
