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
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
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
	throttler rscthrottler.RSCThrottler,
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

type boundedRuntimeFilterConsumerThrottler struct {
	mu   sync.Mutex
	cap  int64
	used int64
	peak int64
}

func (t *boundedRuntimeFilterConsumerThrottler) Refresh()    {}
func (t *boundedRuntimeFilterConsumerThrottler) PrintUsage() {}
func (t *boundedRuntimeFilterConsumerThrottler) Available() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.cap - t.used
}
func (t *boundedRuntimeFilterConsumerThrottler) Acquire(size int64) (int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if size < 0 || size > t.cap-t.used {
		return t.cap - t.used, false
	}
	t.used += size
	t.peak = max(t.peak, t.used)
	return t.cap - t.used, true
}
func (t *boundedRuntimeFilterConsumerThrottler) Release(size int64) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.used -= size
	return t.cap - t.used
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

// TestMembershipRuntimeFilterHundredConcurrentTenMillionKeys models the
// production shape from #26720 without allocating 100 physical 10M vectors.
// Every successful reservation remains live while all 100 queries attempt
// admission, exactly matching the concurrent filter-build pressure. The
// bounded CN admits only the requests it can cover; handleRuntimeFilter's
// rejection test above proves the remaining requests publish PASS.
func TestMembershipRuntimeFilterHundredConcurrentTenMillionKeys(t *testing.T) {
	const (
		queries       = 100
		cardinality   = 10_000_000
		payloadBytes  = 80 << 20 // serialized 10M int64 keys
		cnConsumerCap = int64(2 << 30)
	)

	proc := testutil.NewProc(t)
	defer proc.Free()
	throttler := &boundedRuntimeFilterConsumerThrottler{cap: cnConsumerCap}
	installRuntimeFilterConsumerThrottler(t, proc, throttler)

	requested, ok := runtimeFilterConsumerMemoryBound(payloadBytes, cardinality)
	require.True(t, ok)
	var releases []func()
	for range queries {
		release, gotRequested, granted := reserveRuntimeFilterConsumerMemory(
			proc, payloadBytes, cardinality)
		require.Equal(t, requested, gotRequested)
		if granted {
			releases = append(releases, release)
		}
	}

	require.Equal(t, int(cnConsumerCap/requested), len(releases))
	require.Less(t, len(releases), queries)
	require.LessOrEqual(t, throttler.peak, cnConsumerCap)
	for _, release := range releases {
		release()
	}
	require.Zero(t, throttler.used)
}
