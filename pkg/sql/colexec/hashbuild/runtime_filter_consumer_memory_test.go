// Copyright 2026 Matrix Origin
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

package hashbuild

import (
	"math"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type testRuntimeFilterConsumerThrottler struct {
	grant    bool
	acquired []int64
	released int64
}

func (t *testRuntimeFilterConsumerThrottler) Refresh()    {}
func (t *testRuntimeFilterConsumerThrottler) PrintUsage() {}
func (t *testRuntimeFilterConsumerThrottler) Available() int64 {
	return 0
}
func (t *testRuntimeFilterConsumerThrottler) Acquire(size int64) (int64, bool) {
	t.acquired = append(t.acquired, size)
	return 0, t.grant
}
func (t *testRuntimeFilterConsumerThrottler) Release(size int64) int64 {
	t.released += size
	return 0
}

func installRuntimeFilterConsumerThrottler(
	t *testing.T,
	proc *process.Process,
	throttler *testRuntimeFilterConsumerThrottler,
) {
	t.Helper()
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, existed := rt.GetGlobalVariables(moruntime.CNMemoryThrottler)
	rt.SetGlobalVariables(moruntime.CNMemoryThrottler, throttler)
	t.Cleanup(func() {
		if existed {
			rt.SetGlobalVariables(moruntime.CNMemoryThrottler, previous)
			return
		}
		rt.CompareAndDeleteGlobalVariables(moruntime.CNMemoryThrottler, throttler)
	})
}

func TestRuntimeFilterConsumerMemoryBound(t *testing.T) {
	bound, ok := runtimeFilterConsumerMemoryBound(1_024, 10)
	require.True(t, ok)
	require.Equal(t, int64(1_024*2+10*16+64<<10), bound)

	_, ok = runtimeFilterConsumerMemoryBound(-1, 1)
	require.False(t, ok)
	_, ok = runtimeFilterConsumerMemoryBound(math.MaxInt, math.MaxInt)
	require.False(t, ok)
}

func TestMembershipRuntimeFilterConsumerMemoryAdmission(t *testing.T) {
	for _, test := range []struct {
		name    string
		grant   bool
		wantTyp int32
	}{
		{name: "granted", grant: true, wantTyp: message.RuntimeFilter_UNIQUEJOINKEYS},
		{name: "rejected", grant: false, wantTyp: message.RuntimeFilter_PASS},
	} {
		t.Run(test.name, func(t *testing.T) {
			tc := newTestCase(t, []bool{false}, []types.Type{types.T_int64.ToType()},
				[]*plan.Expr{newExpr(0, types.T_int64.ToType())})
			throttler := &testRuntimeFilterConsumerThrottler{grant: test.grant}
			installRuntimeFilterConsumerThrottler(t, tc.proc, throttler)
			tc.arg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{
				Tag:                 201,
				UseMembershipFilter: true,
			}
			tc.arg.OpAnalyzer = process.NewAnalyzer(0, false, false, "hash build")
			budget := process.MustNewHashBuildBudget(64<<20, 64<<20)
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			installTestHashBuildBudget(t, tc.arg, generation)
			tc.arg.ctr.hashmapBuilder.InputBatchRowCount = 4
			tc.arg.ctr.hashmapBuilder.UniqueJoinKeys = []*vector.Vector{
				testutil.MakeInt64Vector([]int64{1, 2, 3, 4}, nil, tc.proc.Mp()),
			}

			require.NoError(t, tc.arg.handleRuntimeFilter(tc.proc))
			require.Len(t, throttler.acquired, 1)

			receiver := message.NewMessageReceiver(
				[]int32{tc.arg.RuntimeFilterSpec.Tag},
				message.AddrBroadCastOnCurrentCN(),
				tc.proc.GetMessageBoard())
			msgs, done, err := receiver.ReceiveMessage(false, tc.proc.Ctx)
			require.NoError(t, err)
			require.False(t, done)
			require.Len(t, msgs, 1)
			runtimeFilter := msgs[0].(message.RuntimeFilterMessage)
			require.Equal(t, test.wantTyp, runtimeFilter.Typ)

			if test.grant {
				require.NotEmpty(t, runtimeFilter.Data)
				require.Zero(t, throttler.released)
				runtimeFilter.Destroy()
				require.Equal(t, throttler.acquired[0], throttler.released)
			} else {
				require.Empty(t, runtimeFilter.Data)
				require.Zero(t, generation.Used())
				require.Equal(t, int64(1), tc.arg.OpAnalyzer.GetOpStats().ExtraStats["HashBuildRuntimeFilterConsumerMemoryFallbacks"])
			}

			tc.arg.Free(tc.proc, false, nil)
			tc.proc.GetMessageBoard().CloseAndDrain()
			generation.Close()
			tc.proc.Free()
			require.Zero(t, tc.proc.Mp().CurrNB())
		})
	}
}
