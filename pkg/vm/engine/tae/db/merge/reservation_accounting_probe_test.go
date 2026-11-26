// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merge

import (
	"context"
	"fmt"
	"iter"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This deterministic controller implements the production default Acquire
// contract: a refusal leaves reservations unchanged. No host pressure or large
// allocations are needed to exercise scheduler admission.
type reservationProbeDecision struct {
	ask     int64
	granted bool
}

type reservationProbeRSC struct {
	limit     int64
	held      int64
	decisions []reservationProbeDecision
	releases  []int64
}

func (*reservationProbeRSC) Refresh()    {}
func (*reservationProbeRSC) PrintUsage() {}
func (r *reservationProbeRSC) Available() int64 {
	return max(int64(0), r.limit-r.held)
}
func (r *reservationProbeRSC) Acquire(ask int64) (int64, bool) {
	granted := ask <= r.Available()
	r.decisions = append(r.decisions, reservationProbeDecision{ask, granted})
	if granted {
		r.held += ask
	}
	return r.Available(), granted
}
func (r *reservationProbeRSC) Release(size int64) int64 {
	r.releases = append(r.releases, size)
	r.held = max(int64(0), r.held-size)
	return r.Available()
}

// A tiny MergeTable implementation delegates all identity/lifecycle behavior to
// the existing catalog fixture and supplies valid visible object metadata.
type reservationProbeTable struct {
	catalog.MergeTable
	stats []*objectio.ObjectStats
}

type reservationProbeItem struct{ stats *objectio.ObjectStats }

func (i reservationProbeItem) GetCreatedAt() types.TS                { return types.BuildTS(1, 0) }
func (i reservationProbeItem) GetObjectStats() *objectio.ObjectStats { return i.stats }
func (p *reservationProbeTable) IterDataItem() iter.Seq[catalog.MergeDataItem] {
	return func(yield func(catalog.MergeDataItem) bool) {
		for _, stat := range p.stats {
			if !yield(reservationProbeItem{stat}) {
				return
			}
		}
	}
}

type reservationProbeExecutor struct {
	delayed  *delayedCompletionExecutor
	rejectAt int
	attempts int
}

func (e *reservationProbeExecutor) ExecuteFor(table catalog.MergeTable, task mergeTask) bool {
	e.attempts++
	if e.attempts == e.rejectAt {
		return false // normal executor scheduling failure; no completion follows
	}
	return e.delayed.ExecuteFor(table, task)
}

func reservationProbeKey(task mergeTask) string {
	keys := make([]string, 0, len(task.objs))
	for _, obj := range task.objs {
		keys = append(keys, fmt.Sprintf("%x", obj.ObjectName()))
	}
	sort.Strings(keys)
	return strings.Join(keys, "/")
}

// This ledger owns individual successful grants. Unlike the implementation's
// scalar Release(size), completion can remove only that task's own grant.
type reservationProbeLedger map[string]int64

func (l reservationProbeLedger) total() int64 {
	var total int64
	for _, size := range l {
		total += size
	}
	return total
}

func TestMergeReservationReachableBudgetProbe(t *testing.T) {
	// OriginSize is a scalar in ObjectStats, not a payload allocation. With
	// three 64MiB logical objects per task the estimate exceeds 500MiB,
	// allowing the production handleMainLoop admission guard to pass first.
	runMergeReservationAccountingProbe(t, 64*common.Const1MBytes, true)
}

func runMergeReservationAccountingProbe(t *testing.T, originSize uint32, checkGlobalGuard bool) {
	for _, path := range []string{"assigned_tasks", "two_Ln_clusters"} {
		t.Run(path, func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				ample    bool
				rejectAt int
			}{
				{name: "tight_budget"},
				{name: "sufficient_budget", ample: true},
				{name: "schedule_failure", ample: true, rejectAt: 2},
			} {
				t.Run(tc.name, func(t *testing.T) {
					table := &reservationProbeTable{MergeTable: promotionTable(8901)}
					// Six metadata-only objects: two disjoint key clusters, each
					// containing the minimum default overlap depth of three.
					for cluster := range 2 {
						for range 3 {
							lo := int32(cluster*100 + 1)
							table.stats = append(table.stats,
								newTestObjectStats(t, lo, lo+1, originSize, 2, 1, nil, 0))
						}
					}
					clock := newFakeClock()
					trigger := NewMMsgTaskTrigger(table)
					if path == "assigned_tasks" {
						// Assigned triggers are used by ioVacuumCheck and are an
						// existing package seam. The tasks have disjoint objects.
						trigger.WithAssignedTasks([]mergeTask{
							{objs: table.stats[:3], level: 1, note: "cluster one"},
							{objs: table.stats[3:], level: 1, note: "cluster two"},
						})
					} else {
						trigger.WithLn(1, 1, DefaultOverlapOpts.Clone())
					}
					// Estimate uses metadata only; the size never allocates a
					// corresponding physical object or merge input payload.
					estimate := int64(mergesort.EstimateMergeSizeWithBlockRows(IterStats(table.stats[:3]), table.GetBlockMaxRows()))
					require.Positive(t, estimate)
					if !checkGlobalGuard {
						require.Less(t, estimate, int64(1024))
					}
					budget := estimate * 3 / 2
					if tc.ample {
						budget = estimate * 2
					}
					rc := &reservationProbeRSC{limit: budget}
					if checkGlobalGuard {
						require.GreaterOrEqual(t, rc.Available(), int64(500*common.Const1MBytes),
							"initial budget must pass handleMainLoop's global admission guard")
						if !tc.ample {
							require.Less(t, budget-estimate, int64(500*common.Const1MBytes),
								"the budget falls below the global guard after the first grant inside this doSched")
						}
					}
					gathered := newLaunchPad(clock).gatherByTrigger(
						context.Background(), trigger, clock.Now(), rc)
					require.Len(t, gathered, 2, "both independent tasks fit the same initial budget")
					seen := make(map[string]struct{})
					for _, task := range gathered {
						require.Len(t, task.objs, 3, "gather must not trim this fixture")
						require.Equal(t, estimate, int64(task.eSize))
						require.True(t, resourceAvailable(estimate, rc))
						for _, obj := range task.objs {
							key := fmt.Sprintf("%x", obj.ObjectName())
							_, duplicate := seen[key]
							require.False(t, duplicate, "executor scopes must be disjoint")
							seen[key] = struct{}{}
						}
					}
					require.Len(t, seen, 6)
					require.Zero(t, rc.held, "gather is only an availability check")
					delayed := &delayedCompletionExecutor{tasks: make(chan mergeTask, 2)}
					executor := &reservationProbeExecutor{delayed: delayed, rejectAt: tc.rejectAt}
					sched := NewMergeScheduler(time.Hour,
						&dummyCatalogSource{initTables: []catalog.MergeTable{table}}, executor, clock)
					sched.PatchTestRscController(rc)
					// Run the real single-threaded scheduler transition directly.
					// No service, worker goroutine, wall-clock sleep, or fake
					// operator state is involved.
					sched.handleTaskTrigger(nil, trigger)
					supp := sched.supps[table.ID()]
					require.False(t, sched.allPaused)
					require.False(t, supp.paused)
					require.False(t, table.HasDropCommitted())
					require.False(t, supp.todo.readyAt.After(clock.Now()))
					require.Zero(t, supp.mergingTaskCnt.Load())
					require.LessOrEqual(t, len(sched.msgChan), 50)
					sched.doSched(nil, supp.todo)
					accepted := make([]mergeTask, 0, len(delayed.tasks))
					for len(delayed.tasks) > 0 {
						accepted = append(accepted, <-delayed.tasks)
					}
					// Completion is immediate and deterministic, but register
					// cleanup before any assertion that can end the test.
					t.Cleanup(func() {
						for _, task := range accepted {
							task.doneCB.OnExecDone(nil)
						}
					})
					require.Len(t, rc.decisions, 2)
					require.True(t, rc.decisions[0].granted)
					require.Equal(t, estimate, rc.decisions[0].ask)
					require.Equal(t, estimate, rc.decisions[1].ask)
					wantAccepted := 2
					if !tc.ample || tc.rejectAt != 0 {
						wantAccepted = 1
					}
					require.Equal(t, wantAccepted, len(accepted))
					wantAttempts := wantAccepted
					if tc.rejectAt != 0 {
						wantAttempts++
					}
					require.Equal(t, wantAttempts, executor.attempts)
					require.Equal(t, tc.ample, rc.decisions[1].granted)
					if tc.rejectAt != 0 {
						require.Equal(t, []int64{estimate}, rc.releases)
					} else {
						require.Empty(t, rc.releases)
					}
					ledger := make(reservationProbeLedger)
					for _, task := range accepted {
						ledger[reservationProbeKey(task)] = int64(task.eSize)
					}
					require.Equal(t, ledger.total(), rc.held)
					require.Equal(t, int64(len(accepted)), supp.mergingTaskCnt.Load())

					// Finish the second accepted task first. If its Acquire was
					// denied, it owns no grant and cannot release the first's.
					for i := len(accepted) - 1; i >= 0; i-- {
						task := accepted[i]
						before := rc.held
						delete(ledger, reservationProbeKey(task))
						task.doneCB.OnExecDone(nil)
						t.Logf("completed_accepted_task=%d held_before=%d held_after=%d independent_ledger=%d pending=%d",
							i+1, before, rc.held, ledger.total(), supp.mergingTaskCnt.Load())
						assert.Equal(t, ledger.total(), rc.held,
							"completion must release only the completing task's successful grant")
					}
					require.Zero(t, supp.mergingTaskCnt.Load())
					require.Zero(t, rc.held)
					require.Empty(t, ledger)
				})
			}
		})
	}
}

func TestMergeReservationContinuesAfterDenial(t *testing.T) {
	table := &reservationProbeTable{MergeTable: promotionTable(8902)}
	for cluster := range 3 {
		size := uint32(64 * common.Const1MBytes)
		if cluster == 2 {
			size /= 4
		}
		for range 3 {
			lo := int32(cluster*100 + 1)
			table.stats = append(table.stats, newTestObjectStats(t, lo, lo+1, size, 2, 1, nil, 0))
		}
	}
	large := int64(mergesort.EstimateMergeSizeWithBlockRows(IterStats(table.stats[:3]), table.GetBlockMaxRows()))
	small := int64(mergesort.EstimateMergeSizeWithBlockRows(IterStats(table.stats[6:]), table.GetBlockMaxRows()))
	require.Less(t, small, large/2)
	clock := newFakeClock()
	rc := &reservationProbeRSC{limit: large * 3 / 2}
	executor := &delayedCompletionExecutor{tasks: make(chan mergeTask, 3)}
	sched := NewMergeScheduler(5*time.Second, &dummyCatalogSource{initTables: []catalog.MergeTable{table}}, executor, clock)
	sched.PatchTestRscController(rc)
	trigger := NewMMsgTaskTrigger(table).WithByUser(true).WithAssignedTasks([]mergeTask{
		{objs: table.stats[:3]}, {objs: table.stats[3:6]}, {objs: table.stats[6:]},
	})
	sched.handleTaskTrigger(nil, trigger)
	supp := sched.supps[table.ID()]
	sched.doSched(nil, supp.todo)
	require.Len(t, executor.tasks, 2)
	first, second := <-executor.tasks, <-executor.tasks
	t.Cleanup(func() {
		first.doneCB.OnExecDone(nil)
		second.doneCB.OnExecDone(nil)
	})
	require.Len(t, rc.decisions, 3)
	require.True(t, rc.decisions[0].granted)
	require.False(t, rc.decisions[1].granted)
	require.True(t, rc.decisions[2].granted)
	require.Equal(t, large+small, rc.held)
	second.doneCB.OnExecDone(nil)
	second.doneCB.OnExecDone(nil)
	require.Equal(t, large, rc.held)
	require.Equal(t, []int64{small}, rc.releases)
	require.Equal(t, int64(1), supp.mergingTaskCnt.Load())
	first.doneCB.OnExecDone(nil)
	require.Zero(t, rc.held)
	require.Equal(t, []int64{small, large}, rc.releases)

	clock.Advance(5 * time.Second)
	sched.doSched(nil, supp.todo)
	require.Len(t, executor.tasks, 1, "already submitted candidates must not run again")
	retried := <-executor.tasks
	t.Cleanup(func() { retried.doneCB.OnExecDone(nil) })
	require.Equal(t, table.stats[3:6], retried.objs)
	require.Equal(t, large, rc.held)
	retried.doneCB.OnExecDone(nil)
	require.Zero(t, rc.held)
	require.Empty(t, supp.triggers)
}

type inlineCompletionExecutor struct {
	tasks           []mergeTask
	rc              *reservationProbeRSC
	attempts        int
	heldAtEntry     []int64
	releasesAtEntry [][]int64
}

func (e *inlineCompletionExecutor) ExecuteFor(_ catalog.MergeTable, task mergeTask) bool {
	e.attempts++
	e.heldAtEntry = append(e.heldAtEntry, e.rc.held)
	e.releasesAtEntry = append(e.releasesAtEntry, append([]int64(nil), e.rc.releases...))
	if e.attempts == 2 {
		return false
	}
	e.tasks = append(e.tasks, task)
	if e.attempts == 3 {
		task.doneCB.OnExecDone(nil)
	}
	return true
}

func TestMergeReservationInlineCompletion(t *testing.T) {
	const blockRows = uint32(16384)
	table := &reservationProbeTable{MergeTable: mergeTableWithBlockRows(8903, blockRows)}
	for cluster := range 3 {
		for range 3 {
			lo := int32(cluster*100 + 1)
			table.stats = append(table.stats, newTestObjectStats(t, lo, lo+1, 64*common.Const1MBytes, blockRows, 1, nil, 0))
		}
	}
	size := int64(mergesort.EstimateMergeSizeWithBlockRows(IterStats(table.stats[:3]), table.GetBlockMaxRows()))
	defaultSize := mergesort.EstimateMergeSizeWithBlockRows(IterStats(table.stats[:3]), objectio.BlockMaxRows)
	require.Greater(t, size, int64(defaultSize))
	rc := &reservationProbeRSC{limit: size * 2}
	executor := &inlineCompletionExecutor{rc: rc}
	clock := newFakeClock()
	sched := NewMergeScheduler(time.Hour, &dummyCatalogSource{initTables: []catalog.MergeTable{table}}, executor, clock)
	sched.PatchTestRscController(rc)
	trigger := NewMMsgTaskTrigger(table).WithAssignedTasks([]mergeTask{
		{objs: table.stats[:3]}, {objs: table.stats[3:6]}, {objs: table.stats[6:]},
	})
	sched.handleTaskTrigger(nil, trigger)
	supp := sched.supps[table.ID()]
	sched.doSched(nil, supp.todo)
	t.Cleanup(func() {
		for _, task := range executor.tasks {
			task.doneCB.OnExecDone(nil)
		}
	})
	require.Len(t, executor.tasks, 2)
	require.Equal(t, []int64{size, size * 2, size * 2}, executor.heldAtEntry)
	require.Equal(t, []int64{size}, executor.releasesAtEntry[2])
	require.Equal(t, size, rc.held)
	require.Equal(t, []int64{size, size}, rc.releases)
	require.Equal(t, int64(1), supp.mergingTaskCnt.Load())
	executor.tasks[1].doneCB.OnExecDone(nil)
	require.Equal(t, size, rc.held)
	require.Equal(t, []int64{size, size}, rc.releases)
	executor.tasks[0].doneCB.OnExecDone(nil)
	require.Zero(t, rc.held)
	require.Equal(t, []int64{size, size, size}, rc.releases)
}

// The simulated RSS and reservations let gathering and aggregate admission
// see different available budgets without relying on the test host's memory.
type manualMergeAdmissionProbe struct {
	*testRscController
	budget    int64
	decisions chan reservationProbeDecision
}

func (r *manualMergeAdmissionProbe) Acquire(ask int64) (int64, bool) {
	available := max(0, r.budget-r.reserved.Load())
	granted := ask <= available
	if granted {
		r.reserved.Add(ask)
		available -= ask
	}
	select {
	case r.decisions <- reservationProbeDecision{ask: ask, granted: granted}:
	default:
	}
	return available, granted
}

func TestManualMergeAfterReservationDenial(t *testing.T) {
	for _, tc := range []struct {
		name    string
		held    int64
		expires bool
	}{
		{name: "budget_available"},
		{name: "retry_after_release", held: 3 * common.Const1GBytes},
		{name: "bounded_retry", held: 3 * common.Const1GBytes, expires: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := promotionTable(8904)
			objs := []*objectio.ObjectStats{
				newTestObjectStats(t, 1, 3, 300*common.Const1MBytes, 2, 1, nil, 0),
				newTestObjectStats(t, 2, 4, 300*common.Const1MBytes, 2, 1, nil, 0),
			}
			const interval = 5 * time.Second
			clock := newFakeClock()
			rc := &manualMergeAdmissionProbe{
				testRscController: newTestRscController(6 * common.Const1GBytes),
				budget:            4 * common.Const1GBytes,
				decisions:         make(chan reservationProbeDecision, 4),
			}
			rc.reserved.Store(tc.held)
			estimate := int64(mergesort.EstimateMergeSizeWithBlockRows(
				IterStats(objs), table.GetBlockMaxRows()))
			require.True(t, resourceAvailable(estimate, rc), "both objects must pass gathering")
			if tc.held != 0 {
				require.Greater(t, estimate, rc.budget-tc.held, "final admission must deny the full task")
			}

			executor := &delayedCompletionExecutor{tasks: make(chan mergeTask, 2)}
			sched := NewMergeScheduler(interval, &dummyCatalogSource{initTables: []catalog.MergeTable{table}}, executor, clock)
			sched.PatchTestRscController(rc)
			sched.Start()
			t.Cleanup(sched.Stop)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			query := func() *QueryAnswer {
				answer, err := sched.Query(ctx, table)
				require.NoError(t, err)
				return answer
			}
			sched.PauseTable(table)
			require.False(t, query().AutoMergeOn)
			require.NoError(t, sched.SendTrigger(NewMMsgTaskTrigger(table).WithByUser(true).
				WithAssignedTasks(NewMergeTaskFromSpecObjects(objs, 1))))

			select {
			case decision := <-rc.decisions:
				require.Equal(t, reservationProbeDecision{ask: estimate, granted: tc.held == 0}, decision)
			case <-ctx.Done():
				t.Fatal("manual merge did not reach admission")
			}
			if tc.held != 0 {
				require.Zero(t, query().DataMergeCnt)
				require.Empty(t, executor.tasks)
				require.Equal(t, tc.held, rc.reserved.Load())
				if tc.expires {
					require.NotEmpty(t, query().Triggers, "denied manual work should remain visible")
					clock.Advance(2 * time.Minute)
					require.Eventually(t, func() bool {
						answer, err := sched.Query(ctx, table)
						return err == nil && answer.Triggers == ""
					}, time.Second, time.Millisecond)
					require.Empty(t, rc.decisions, "expired work must not attempt admission again")
					require.Empty(t, executor.tasks)
					require.Equal(t, tc.held, rc.reserved.Load())
					return
				}
				rc.Release(tc.held)
				clock.Advance(interval)
				query()
			}

			select {
			case task := <-executor.tasks:
				require.Equal(t, objs, task.objs)
				require.Equal(t, estimate, rc.reserved.Load())
				task.doneCB.OnExecDone(nil)
			case <-ctx.Done():
				t.Fatal("queued manual merge was lost after reservation capacity returned")
			}
			clock.Advance(interval)
			answer := query()
			require.EqualValues(t, 1, answer.DataMergeCnt)
			require.Zero(t, answer.PendingMergeCnt)
			require.Empty(t, answer.Triggers)
			require.Empty(t, executor.tasks)
			require.Zero(t, rc.reserved.Load())
		})
	}
}

func TestManualMergeRetryWakesWhenBusyTaskCompletes(t *testing.T) {
	for _, phase := range []string{"running", "restart", "stopped"} {
		t.Run(phase, func(t *testing.T) {
			runManualMergeRetryCompletion(t, phase)
		})
	}
}

func runManualMergeRetryCompletion(t *testing.T, completionPhase string) {
	table := promotionTable(8905)
	firstObjs := []*objectio.ObjectStats{
		newTestObjectStats(t, 1, 2, 300*common.Const1MBytes, 2, 1, nil, 0),
		newTestObjectStats(t, 2, 3, 300*common.Const1MBytes, 2, 1, nil, 0),
	}
	secondObjs := []*objectio.ObjectStats{
		newTestObjectStats(t, 3, 4, 300*common.Const1MBytes, 2, 1, nil, 0),
		newTestObjectStats(t, 4, 5, 300*common.Const1MBytes, 2, 1, nil, 0),
	}
	estimate := int64(mergesort.EstimateMergeSizeWithBlockRows(
		IterStats(firstObjs), table.GetBlockMaxRows()))
	rc := &manualMergeAdmissionProbe{
		testRscController: newTestRscController(6 * common.Const1GBytes),
		budget:            estimate,
		decisions:         make(chan reservationProbeDecision, 4),
	}
	clock := newFakeClock()
	executor := &delayedCompletionExecutor{tasks: make(chan mergeTask, 2)}
	sched := NewMergeScheduler(100*time.Second, &dummyCatalogSource{
		initTables: []catalog.MergeTable{table},
	}, executor, clock)
	sched.PatchTestRscController(rc)
	sched.Start()
	t.Cleanup(sched.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	query := func() *QueryAnswer {
		answer, err := sched.Query(ctx, table)
		require.NoError(t, err)
		return answer
	}
	sched.PauseTable(table)
	require.False(t, query().AutoMergeOn)
	tasks := append(NewMergeTaskFromSpecObjects(firstObjs, 1),
		NewMergeTaskFromSpecObjects(secondObjs, 1)...)
	require.NoError(t, sched.SendTrigger(NewMMsgTaskTrigger(table).
		WithByUser(true).WithAssignedTasks(tasks)))
	for _, granted := range []bool{true, false} {
		select {
		case decision := <-rc.decisions:
			require.Equal(t, reservationProbeDecision{ask: estimate, granted: granted}, decision)
		case <-ctx.Done():
			t.Fatal("initial manual merge did not reach both admissions")
		}
	}
	first := <-executor.tasks
	t.Cleanup(func() { first.doneCB.OnExecDone(nil) })
	require.Equal(t, firstObjs, first.objs)
	require.NotEmpty(t, query().Triggers)

	// The first retry fires while the table is busy. Keep the next check
	// inside the manual retry window even if the completion wake is lost.
	clock.Advance(15 * time.Second)
	require.Eventually(t, func() bool {
		due := query().NextCheckDue
		return due > 0 && due <= 15*time.Second
	}, time.Second, time.Millisecond)
	switch completionPhase {
	case "running":
		clock.Advance(time.Second)
		first.doneCB.OnExecDone(nil)
	case "restart":
		oldGeneration := sched.generation.Load()
		sched.Stop()
		sched.Start()
		require.NotSame(t, oldGeneration, sched.generation.Load())
		query() // wait until the new loop has rearmed retained retries
		clock.Advance(time.Second)
		first.doneCB.OnExecDone(nil)
	case "stopped":
		sched.Stop()
		first.doneCB.OnExecDone(nil)
		sched.Start()
	}
	select {
	case second := <-executor.tasks:
		t.Cleanup(func() { second.doneCB.OnExecDone(nil) })
		require.Equal(t, secondObjs, second.objs)
		second.doneCB.OnExecDone(nil)
	case <-ctx.Done():
		t.Fatal("completion did not wake the denied manual merge")
	}
	answer := query()
	require.EqualValues(t, 2, answer.DataMergeCnt)
	require.Zero(t, answer.PendingMergeCnt)
	require.Empty(t, answer.Triggers)
	require.Zero(t, rc.reserved.Load())
}

func TestManualMergeRetrySurvivesTablePause(t *testing.T) {
	table := promotionTable(8906)
	clock := newFakeClock()
	rc := &reservationProbeRSC{limit: 100}
	executor := &delayedCompletionExecutor{tasks: make(chan mergeTask, 1)}
	sched := NewMergeScheduler(100*time.Second,
		&dummyCatalogSource{initTables: []catalog.MergeTable{table}}, executor, clock)
	sched.PatchTestRscController(rc)
	supp := sched.supps[table.ID()]
	retry := NewMMsgTaskTrigger(table).WithByUser(true)
	retry.retryUntil = clock.Now().Add(manualMergeRetryWindow)
	retry.assigns = []mergeTask{{eSize: 40, note: "manual retry"}}
	background := NewMMsgTaskTrigger(table).WithAssignedTasks([]mergeTask{{eSize: 1}})
	supp.triggers = append(supp.triggers, retry, background)
	sched.pq.Update(supp.todo, clock.Now().Add(5*time.Second))

	sched.handleSwitch(MMsgSwitch{Table: table, On: false})
	require.True(t, supp.paused)
	require.Equal(t, 5*time.Second, supp.todo.readyAt.Sub(clock.Now()))
	clock.Advance(5 * time.Second)
	sched.doSched(nil, supp.todo)
	require.Empty(t, executor.tasks, "the paused table must skip its background trigger")
	require.LessOrEqual(t, supp.todo.readyAt.Sub(clock.Now()), 15*time.Second,
		"the background trigger must not bury an earlier manual retry")
	clock.Advance(15 * time.Second)
	sched.doSched(nil, supp.todo)
	require.Len(t, executor.tasks, 1)
	task := <-executor.tasks
	t.Cleanup(func() { task.doneCB.OnExecDone(nil) })
	require.Equal(t, "manual retry", task.note)
	require.Equal(t, int64(40), rc.held)
	task.doneCB.OnExecDone(nil)
	require.Zero(t, rc.held)
	require.Empty(t, supp.triggers)

	sched.handleSwitch(MMsgSwitch{Table: table, On: true})
	sched.handleSwitch(MMsgSwitch{Table: table, On: false})
	require.Equal(t, 24*time.Hour, supp.todo.readyAt.Sub(clock.Now()),
		"a paused table without manual work retains the ordinary long delay")
}

func TestManualMergeRetriesAreFairAcrossRequests(t *testing.T) {
	table := promotionTable(8907)
	clock := newFakeClock()
	rc := &reservationProbeRSC{limit: 60}
	executor := &delayedCompletionExecutor{tasks: make(chan mergeTask, 2)}
	sched := NewMergeScheduler(100*time.Second,
		&dummyCatalogSource{initTables: []catalog.MergeTable{table}}, executor, clock)
	sched.PatchTestRscController(rc)
	supp := sched.supps[table.ID()]
	policy := NewMMsgTaskTrigger(table).WithExpire(clock.Now().Add(2 * time.Minute))
	small := NewMMsgTaskTrigger(table).WithByUser(true)
	small.retryUntil = clock.Now().Add(manualMergeRetryWindow)
	small.assigns = []mergeTask{{eSize: 40, note: "earlier small request"}}
	large := NewMMsgTaskTrigger(table).WithByUser(true)
	large.retryUntil = small.retryUntil
	large.assigns = []mergeTask{{eSize: 80, note: "later large request"}}
	supp.triggers = []*MMsgTaskTrigger{policy, small, large}

	sched.doSched(nil, supp.todo)
	require.Equal(t, []reservationProbeDecision{{ask: 80, granted: false}}, rc.decisions)
	require.Same(t, policy, supp.triggers[0])
	require.Same(t, small, supp.triggers[len(supp.triggers)-1],
		"the earlier request must be selected after the later request is denied")
	clock.Advance(15 * time.Second)
	sched.doSched(nil, supp.todo)
	require.Equal(t, []reservationProbeDecision{
		{ask: 80, granted: false}, {ask: 40, granted: true},
	}, rc.decisions)
	require.Len(t, executor.tasks, 1)
	first := <-executor.tasks
	t.Cleanup(func() { first.doneCB.OnExecDone(nil) })
	require.Equal(t, "earlier small request", first.note)
	first.doneCB.OnExecDone(nil)
	rc.limit = 100
	clock.Advance(15 * time.Second)
	sched.doSched(nil, supp.todo)
	require.Len(t, executor.tasks, 1)
	second := <-executor.tasks
	t.Cleanup(func() { second.doneCB.OnExecDone(nil) })
	require.Equal(t, "later large request", second.note)
	second.doneCB.OnExecDone(nil)
	require.Equal(t, []reservationProbeDecision{
		{ask: 80, granted: false}, {ask: 40, granted: true}, {ask: 80, granted: true},
	}, rc.decisions)
	require.Same(t, policy, supp.triggers[0])
	require.Zero(t, rc.held)
}

func TestManualMergeRetryRechecksAfterMemoryPressure(t *testing.T) {
	table := promotionTable(8908)
	clock := newFakeClock()
	rc := newTestRscController(400 * common.Const1MBytes)
	executor := &delayedCompletionExecutor{tasks: make(chan mergeTask, 1)}
	sched := NewMergeScheduler(100*time.Second,
		&dummyCatalogSource{initTables: []catalog.MergeTable{table}}, executor, clock)
	sched.PatchTestRscController(rc)
	supp := sched.supps[table.ID()]
	retry := NewMMsgTaskTrigger(table).WithByUser(true)
	retry.retryUntil = clock.Now().Add(manualMergeRetryWindow)
	retry.assigns = []mergeTask{{eSize: 40 * common.Const1MBytes, note: "pressure retry"}}
	supp.triggers = append(supp.triggers, retry)
	sched.pq.Update(supp.todo, clock.Now())
	sched.Start()
	t.Cleanup(sched.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	answer, err := sched.Query(ctx, table)
	require.NoError(t, err)
	require.NotEmpty(t, answer.Triggers)
	require.Empty(t, executor.tasks)
	rc.limit.Store(600 * common.Const1MBytes)
	clock.Advance(15 * time.Second)
	select {
	case task := <-executor.tasks:
		t.Cleanup(func() { task.doneCB.OnExecDone(nil) })
		require.Equal(t, "pressure retry", task.note)
		task.doneCB.OnExecDone(nil)
	case <-ctx.Done():
		t.Fatal("low-memory gate did not recheck the queued manual retry")
	}
	require.Zero(t, rc.reserved.Load())
}
