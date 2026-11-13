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

package iscp

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestSelectRowBudgetTo(t *testing.T) {
	makeProbe := func(commits map[int64]int) func(context.Context, types.TS, types.TS, int) (int, error) {
		return func(_ context.Context, from, to types.TS, target int) (int, error) {
			var rows int
			for physical, count := range commits {
				ts := types.BuildTS(physical, 0)
				if ts.GE(&from) && ts.LE(&to) {
					rows += count
				}
			}
			return rows, nil
		}
	}
	for _, tc := range []struct {
		name    string
		commits map[int64]int
		minTo   int64
		maxTo   int64
		rows    int
	}{
		{"full-range", map[int64]int{10: 3, 20: 4}, 30, 30, 7},
		{"leave-later-commit", map[int64]int{10: 3, 20: 4, 30: 6}, 20, 29, 7},
		{"oversized-first-commit", map[int64]int{10: 11, 20: 2}, 10, 10, 11},
		{"empty-leading-gap", map[int64]int{25: 4, 30: 8}, 25, 29, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected, rows, err := selectRowBudgetTo(
				context.Background(), types.BuildTS(1, 0), types.BuildTS(30, 0), 10,
				makeProbe(tc.commits),
			)
			require.NoError(t, err)
			require.GreaterOrEqual(t, selected.Physical(), tc.minTo)
			require.LessOrEqual(t, selected.Physical(), tc.maxTo)
			require.Equal(t, tc.rows, rows)
		})
	}
}

func TestRowBudgetRequiresIndependentJob(t *testing.T) {
	ordinary := &JobSpec{ConsumerInfo: ConsumerInfo{ConsumerType: int8(ConsumerType_CNConsumer)}, TriggerSpec: TriggerSpec{JobType: TriggerType_AlwaysUpdate}}
	budgeted := &JobSpec{ConsumerInfo: ConsumerInfo{ConsumerType: int8(ConsumerType_CNConsumer)}, TriggerSpec: TriggerSpec{JobType: TriggerType_AlwaysUpdate, TargetRows: 100}}
	target, err := rowBudgetForJobs([]*JobSpec{ordinary})
	require.NoError(t, err)
	require.Zero(t, target)
	target, err = rowBudgetForJobs([]*JobSpec{budgeted})
	require.NoError(t, err)
	require.Equal(t, 100, target)
	legacyIndex := &JobSpec{ConsumerInfo: ConsumerInfo{ConsumerType: int8(ConsumerType_IndexSync)}, TriggerSpec: TriggerSpec{JobType: TriggerType_Default}}
	target, err = rowBudgetForJobs([]*JobSpec{legacyIndex})
	require.NoError(t, err)
	require.Zero(t, target)
	target, err = rowBudgetForJobs([]*JobSpec{legacyIndex, legacyIndex})
	require.NoError(t, err)
	require.Zero(t, target)
	configuredIndex := &JobSpec{ConsumerInfo: ConsumerInfo{ConsumerType: int8(ConsumerType_IndexSync)},
		TriggerSpec: TriggerSpec{JobType: TriggerType_AlwaysUpdate, TargetRows: 100_000}}
	target, err = rowBudgetForJobs([]*JobSpec{configuredIndex})
	require.NoError(t, err)
	require.Equal(t, 100_000, target)
	_, err = rowBudgetForJobs([]*JobSpec{legacyIndex, configuredIndex})
	require.ErrorContains(t, err, "mixed ISCP row budgets")
	_, err = rowBudgetForJobs([]*JobSpec{ordinary, budgeted})
	require.ErrorContains(t, err, "mixed ISCP row budgets")
}

func TestRowBudgetTwoIterationsKeepDeferredPhysicalCommit(t *testing.T) {
	upper := types.BuildTS(30, 0)
	table := NewTableEntry(newRuntimeTestExecutor(), 1, 2, 3, "db", "table")
	spec := &JobSpec{ConsumerInfo: ConsumerInfo{ConsumerType: int8(ConsumerType_IndexSync)},
		TriggerSpec: TriggerSpec{JobType: TriggerType_Default, TargetRows: 100_000}}
	_, err := table.AddOrUpdateSinker(context.Background(), "index", spec,
		&JobStatus{Stage: JobStage_Running}, 1,
		types.BuildTS(19, math.MaxUint32), ISCPJobState_Completed, 0)
	require.NoError(t, err)
	commits := map[types.TS]int{
		types.BuildTS(20, 0): 60_000,
		types.BuildTS(30, 0): 60_000,
		types.BuildTS(30, 1): 10_000, // outside the requested upper bound
	}
	probe := func(_ context.Context, from, to types.TS, _ int) (int, error) {
		require.False(t, to.GT(&upper), "probe exceeded the scheduled upper bound")
		rows := 0
		for ts, count := range commits {
			if ts.GE(&from) && ts.LE(&to) {
				rows += count
			}
		}
		return rows, nil
	}

	iterations, _ := table.getCandidate()
	require.Len(t, iterations, 1)
	first := iterations[0]
	require.Equal(t, types.BuildTS(20, 0), first.fromTS)
	first.toTS = upper // the source snapshot bounds this scheduling attempt
	firstRows, err := selectRowBudgetForIteration(context.Background(), first, 100_000, probe)
	require.NoError(t, err)
	require.Equal(t, 60_000, firstRows)
	require.Equal(t, types.BuildTS(29, math.MaxUint32), first.toTS)
	require.NoError(t, table.UpdateWatermark(first))

	// Schedule from the published watermark. The deferred commit has a zero
	// logical component, and the next snapshot can include a later commit.
	iterations, _ = table.getCandidate()
	require.Len(t, iterations, 1)
	second := iterations[0]
	require.Equal(t, types.BuildTS(30, 0), second.fromTS)
	upper = types.BuildTS(30, 2)
	second.toTS = upper
	secondRows, err := selectRowBudgetForIteration(context.Background(), second, 100_000, probe)
	require.NoError(t, err)
	require.Equal(t, upper, second.toTS)
	require.Equal(t, 70_000, secondRows)
	require.NoError(t, table.UpdateWatermark(second))
	watermark, ok := table.GetWatermark("index")
	require.True(t, ok)
	require.Equal(t, upper, watermark)
	require.Equal(t, 130_000, firstRows+secondRows)
}

func TestRowBudgetProbeFailurePreservesRetryRange(t *testing.T) {
	from, to := types.BuildTS(20, 0), types.BuildTS(30, 0)
	iter := &IterationContext{fromTS: from, toTS: to}
	wantErr := errors.New("transient source read")
	calls := 0
	failingProbe := func(_ context.Context, _, _ types.TS, _ int) (int, error) {
		calls++
		if calls == 2 {
			return 0, wantErr
		}
		return 120_000, nil
	}
	_, err := selectRowBudgetForIteration(context.Background(), iter, 100_000, failingProbe)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, to, iter.toTS)

	healthyProbe := func(_ context.Context, probeFrom, probeTo types.TS, _ int) (int, error) {
		rows := 0
		for _, ts := range []types.TS{from, to} {
			if ts.GE(&probeFrom) && ts.LE(&probeTo) {
				rows += 60_000
			}
		}
		return rows, nil
	}
	rows, err := selectRowBudgetForIteration(context.Background(), iter, 100_000, healthyProbe)
	require.NoError(t, err)
	require.Equal(t, 60_000, rows)
	require.Equal(t, types.BuildTS(29, math.MaxUint32), iter.toTS)
}

func TestSelectRowBudgetToKeepsLogicalTimestampWhole(t *testing.T) {
	commits := map[types.TS]int{
		types.BuildTS(10, 1): 4,
		types.BuildTS(10, 2): 5,
		types.BuildTS(10, 3): 8,
	}
	probe := func(_ context.Context, from, to types.TS, _ int) (int, error) {
		rows := 0
		for ts, count := range commits {
			if ts.GE(&from) && ts.LE(&to) {
				rows += count
			}
		}
		return rows, nil
	}
	selected, rows, err := selectRowBudgetTo(
		context.Background(), types.BuildTS(10, 1), types.BuildTS(10, 4), 10, probe)
	require.NoError(t, err)
	require.Equal(t, types.BuildTS(10, 2), selected)
	require.Equal(t, 9, rows)
}

func TestProbeChangeRowsUsesRawStreamingTail(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	step := 0
	collect := func(ctx context.Context, _, _ types.TS) (engine.ChangesHandle, error) {
		require.True(t, engine.CollectChangesPreserveAllVersionsFromContext(ctx))
		require.True(t, engine.CollectChangesDisableQuickFromContext(ctx))
		return &iterationChangesHandle{next: func(context.Context, *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
			step++
			switch step {
			case 1:
				return tailGroupBatch(mp, 1, 1), nil, engine.ChangesHandle_Tail_wip, nil
			case 2:
				return nil, tailGroupBatch(mp, 2, 2), engine.ChangesHandle_Tail_done, nil
			default:
				return nil, nil, engine.ChangesHandle_Tail_done, nil
			}
		}}, nil
	}
	rows, err := probeChangeRows(context.Background(), types.BuildTS(1, 0), types.BuildTS(3, 0), 1, mp, collect)
	require.NoError(t, err)
	require.Equal(t, 2, rows)
	require.Zero(t, mp.CurrNB())
}

func TestProbeChangeRowsCleansErrorBatch(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	want := errors.New("source failed")
	collect := func(context.Context, types.TS, types.TS) (engine.ChangesHandle, error) {
		return &iterationChangesHandle{next: func(context.Context, *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
			return tailGroupBatch(mp, 1, 1), nil, engine.ChangesHandle_Tail_done, want
		}}, nil
	}
	_, err := probeChangeRows(context.Background(), types.BuildTS(1, 0), types.BuildTS(3, 0), 10, mp, collect)
	require.ErrorIs(t, err, want)
	require.Zero(t, mp.CurrNB())
}

func TestProbeChangeRowsStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	_, err := probeChangeRows(ctx, types.BuildTS(1, 0), types.BuildTS(3, 0), 10, nil,
		func(context.Context, types.TS, types.TS) (engine.ChangesHandle, error) {
			called = true
			return nil, nil
		})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, called)
}
