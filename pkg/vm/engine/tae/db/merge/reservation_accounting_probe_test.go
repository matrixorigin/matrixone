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
					estimate := int64(mergesort.EstimateMergeSize(IterStats(table.stats[:3])))
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
	large := int64(mergesort.EstimateMergeSize(IterStats(table.stats[:3])))
	small := int64(mergesort.EstimateMergeSize(IterStats(table.stats[6:])))
	require.Less(t, small, large/2)
	clock := newFakeClock()
	rc := &reservationProbeRSC{limit: large * 3 / 2}
	executor := &delayedCompletionExecutor{tasks: make(chan mergeTask, 3)}
	sched := NewMergeScheduler(time.Hour, &dummyCatalogSource{initTables: []catalog.MergeTable{table}}, executor, clock)
	sched.PatchTestRscController(rc)
	trigger := NewMMsgTaskTrigger(table).WithAssignedTasks([]mergeTask{
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
	table := &reservationProbeTable{MergeTable: promotionTable(8903)}
	for cluster := range 3 {
		for range 3 {
			lo := int32(cluster*100 + 1)
			table.stats = append(table.stats, newTestObjectStats(t, lo, lo+1, 64*common.Const1MBytes, 2, 1, nil, 0))
		}
	}
	size := int64(mergesort.EstimateMergeSize(IterStats(table.stats[:3])))
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
