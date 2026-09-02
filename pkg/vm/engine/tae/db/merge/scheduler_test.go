// Copyright 2024 Matrix Origin
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
	"container/heap"
	"context"
	"iter"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

type dummyExecutor struct{}

func (e *dummyExecutor) ExecuteFor(table catalog.MergeTable, task mergeTask) bool {
	if task.doneCB != nil {
		task.doneCB.OnExecDone(nil)
	}
	return true
}

type recordingExecutor struct {
	executed chan uint64
	overflow atomic.Bool
}

func (e *recordingExecutor) ExecuteFor(table catalog.MergeTable, task mergeTask) bool {
	if task.doneCB != nil {
		task.doneCB.OnExecDone(nil)
	}
	select {
	case e.executed <- table.ID():
	default:
		e.overflow.Store(true)
	}
	return true
}

type delayedCompletionExecutor struct {
	tasks chan mergeTask
}

func (e *delayedCompletionExecutor) ExecuteFor(_ catalog.MergeTable, task mergeTask) bool {
	e.tasks <- task
	return true
}

type dummyCatalogSource struct {
	settingsFn func() (*batch.Batch, func())
	initTables []catalog.MergeTable
}

func (c *dummyCatalogSource) InitSource() iter.Seq[catalog.MergeTable] {
	return func(yield func(catalog.MergeTable) bool) {
		for _, table := range c.initTables {
			if !yield(table) {
				return
			}
		}
	}
}

func (c *dummyCatalogSource) SetMergeNotifier(catalog.MergeNotifierOnCatalog) {}

var oneGoodOneBad = func() (*batch.Batch, func()) {
	bat := batch.New([]string{"account_id", "tid", "version", "settings"})
	// first row with bad settings leading to parse error
	bat.Vecs[0] = vector.NewVec(types.T_uint32.ToType())
	vector.AppendFixed[uint32](bat.Vecs[0], 0, false, common.MergeAllocator)
	bat.Vecs[1] = vector.NewVec(types.T_uint64.ToType())
	vector.AppendFixed[uint64](bat.Vecs[1], 1000, false, common.MergeAllocator)
	bat.Vecs[2] = vector.NewVec(types.T_uint32.ToType())
	vector.AppendFixed[uint32](bat.Vecs[2], 0, false, common.MergeAllocator)
	bat.Vecs[3] = vector.NewVec(types.T_json.ToType())
	json, _ := types.ParseStringToByteJson(`{"bad_settings": 100}`)
	vector.AppendByteJson(bat.Vecs[3], json, false, common.MergeAllocator)

	// second row with good default settings
	vector.AppendFixed[uint32](bat.Vecs[0], 0, false, common.MergeAllocator)
	vector.AppendFixed[uint64](bat.Vecs[1], 1001, false, common.MergeAllocator)
	vector.AppendFixed[uint32](bat.Vecs[2], 0, false, common.MergeAllocator)
	json, _ = types.ParseStringToByteJson(DefaultMergeSettings.String())
	vector.AppendByteJson(bat.Vecs[3], json, false, common.MergeAllocator)

	bat.SetRowCount(2)

	return bat, func() { bat.Clean(common.MergeAllocator) }
}

func (c *dummyCatalogSource) GetMergeSettingsBatchFn() func() (*batch.Batch, func()) {
	return c.settingsFn
}

// schedulerTestHangTimeout bounds every synchronization wait in this file. A
// wait that exceeds it is a hang, not CI load; keep every guard site on this
// one constant so they cannot drift apart.
const schedulerTestHangTimeout = 10 * time.Second

// waitOrFatal waits for one signal on ch or fails the test at the shared hang
// guard.
func waitOrFatal(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	timer := time.NewTimer(schedulerTestHangTimeout)
	defer timer.Stop()
	select {
	case <-ch:
	case <-timer.C:
		t.Fatal(msg)
	}
}

func requireQuery(
	t *testing.T,
	sched *MergeScheduler,
	table catalog.MergeTable,
) *QueryAnswer {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), schedulerTestHangTimeout)
	defer cancel()
	answer, err := sched.Query(ctx, table)
	require.NoError(t, err)
	return answer
}

type droppedMergeTable struct {
	catalog.MergeTable
	checked chan struct{}
	once    sync.Once
}

func (t *droppedMergeTable) HasDropCommitted() bool {
	if t.checked != nil {
		t.once.Do(func() { close(t.checked) })
	}
	return true
}

func TestHandleTaskTriggerNilPointerFixed(t *testing.T) {
	// Setup: Create a MergeScheduler with empty supps map
	scheduler := &MergeScheduler{
		supps:   make(map[uint64]*todoSupporter),
		msgChan: make(chan *MMsg, 4096),
		clock:   NewStdClock(),
	}

	db := catalog.MockDBEntryWithAccInfo(1, 999)
	table := catalog.MockTableEntryWithDB(db, 1000)
	mockTable := catalog.ToMergeTable(table)

	// Create a mock table with ID that doesn't exist in supps map
	// Create a trigger message with vacuum set
	msg := &MMsgTaskTrigger{
		table:  mockTable,
		vacuum: &VacuumOpts{},
	}

	// After the fix: This should NOT panic
	// The early nil check should return gracefully
	require.NotPanics(t, func() {
		scheduler.handleTaskTrigger(nil, msg)
	}, "Should not panic after moving nil check before vacuum check")
}

func TestDoSchedNilSupporter(t *testing.T) {
	scheduler := &MergeScheduler{
		supps: make(map[uint64]*todoSupporter),
		clock: NewStdClock(),
	}

	db := catalog.MockDBEntryWithAccInfo(1, 999)
	table := catalog.MockTableEntryWithDB(db, 1000)
	mockTable := catalog.ToMergeTable(table)

	require.NotPanics(t, func() {
		scheduler.doSched(nil, &todoItem{table: mockTable})
	})

	todo := &todoItem{table: mockTable, readyAt: scheduler.clock.Now()}
	heap.Push(&scheduler.pq, todo)

	require.NotPanics(t, func() {
		scheduler.doSched(nil, todo)
	})
	require.Equal(t, 0, scheduler.pq.Len())
}

func TestScheduler(t *testing.T) {

	newTestTable := func(did, tid uint64) catalog.MergeTable {
		db := catalog.MockDBEntryWithAccInfo(did, tid)
		table := catalog.MockTableEntryWithDB(db, tid)
		return catalog.ToMergeTable(table)
	}

	dropped := &droppedMergeTable{
		MergeTable: newTestTable(1, 1003),
		checked:    make(chan struct{}),
	}
	tables := []catalog.MergeTable{
		newTestTable(1, 1001),
		newTestTable(1, 1002),
		dropped,
	}

	dummySource := &dummyCatalogSource{
		settingsFn: oneGoodOneBad,
		initTables: tables,
	}

	t1002TaskCnt := bigDataTaskCntThreshold + 1
	executor := &recordingExecutor{executed: make(chan uint64, t1002TaskCnt+2)}
	sched := NewMergeScheduler(
		1*time.Millisecond,
		dummySource,
		executor,
		NewStdClock(),
	)
	// Admission is part of scheduler behavior, but host memory pressure is not
	// an input to this unit test. A deterministic controller keeps the test from
	// silently changing meaning with the CI runner's cgroup state.
	sched.PatchTestRscController(newSimRscController(16 * common.Const1GBytes))

	sched.Start()
	defer sched.Stop()

	{
		// switch on/off
		sched.PauseTable(tables[0])
		answer := requireQuery(t, sched, tables[0])
		require.Equal(t, answer.AutoMergeOn, false)
		sched.ResumeTable(tables[0])
		answer = requireQuery(t, sched, tables[0])
		require.Equal(t, answer.AutoMergeOn, true)
		// next check due will be 1s later because of the resume
		require.Greater(t, answer.NextCheckDue, 900*time.Millisecond)

		sched.PauseAll()
		answer = requireQuery(t, sched, nil)
		require.Equal(t, answer.GlobalAutoMergeOn, false)
		sched.ResumeAll()
		answer = requireQuery(t, sched, nil)
		require.Equal(t, answer.GlobalAutoMergeOn, true)
	}

	{
		// mock object create events to bring table1001 ahead
		for i := 0; i < 6; i++ {
			sched.OnCreateNonAppendObject(tables[0])
		}
		answer := requireQuery(t, sched, tables[0])
		require.Less(t, answer.NextCheckDue, 500*time.Millisecond)
	}

	t1004 := newTestTable(1, 1004)
	{
		// create new table
		sched.OnCreateTableCommit(t1004)
		answer := requireQuery(t, sched, t1004)
		require.Equal(t, answer.AutoMergeOn, true)

		sched.PauseTable(t1004)
		trigger := NewMMsgTaskTrigger(t1004).WithByUser(true)
		trigger.WithAssignedTasks([]mergeTask{
			{
				objs: []*objectio.ObjectStats{
					newTestObjectStats(t, 1, 2, 300*common.Const1MBytes, 1000, 1, nil, 0),
				},
				note:  "assgined task 1",
				level: 1,
			},
		})
		sched.SendTrigger(trigger)

		// paused table will not user forced merge task
		// assert: answer.DataMergeCnt == 1

	}

	{
		// make merge task
		trigger := NewMMsgTaskTrigger(tables[1])
		assigns := []mergeTask{}
		for i := 0; i < t1002TaskCnt; i++ {
			assigns = append(assigns, mergeTask{
				objs: []*objectio.ObjectStats{
					newTestObjectStats(t, 1, 2, 300*common.Const1MBytes, 1000, 1, nil, 0),
				},
				note:  "assgined task 1",
				level: 1,
			})
		}
		trigger.WithAssignedTasks(assigns)
		sched.SendTrigger(trigger)

		// assert: answer.DataMergeCnt == taskCnt & answer.BigDataAcc == 1
	}

	{
		// manual vacuum
		{
			// error
			opts := NewVacuumOpts()
			opts.testInject = &vacuumTestInject{
				err: moerr.NewInternalError(context.Background(), "test"),
			}
			trigger := NewMMsgTaskTrigger(tables[0]).WithVacuumCheck(opts)
			sched.SendTrigger(trigger)
		}

		opts2 := NewVacuumOpts()
		// Keep HollowTopK above the injected task count: a full HollowTopK arms a
		// wall-clock 10s vacuum recheck whose persistent inject would emit an
		// extra ExecuteFor event into the exactly-sized drain below under CI
		// stalls. The recheck path has its own fake-clock test.
		opts2.HollowTopK = 2
		opts2.testInject = &vacuumTestInject{
			compactTask: []mergeTask{
				{
					objs: []*objectio.ObjectStats{
						newTestObjectStats(t, 1, 2, 30*common.Const1MBytes, 1000, 1, nil, 0),
					},
					note:  "test",
					level: 1,
				},
			},
			tombstoneVacPercent: 0.6,
		}
		trigger := NewMMsgTaskTrigger(tables[0]).WithVacuumCheck(opts2)
		sched.SendTrigger(trigger)

		// assert: answer.DataMergeCnt == 1
	}

	{
		// test policy patch
		// Keep the patch alive beyond the test's hang guard. This block verifies
		// policy composition, not expiration; expiration has a separate fake-clock
		// test below.
		policyPatchExpiry := sched.clock.Now().Add(time.Hour)

		trigger := NewMMsgTaskTrigger(tables[0])
		trigger.WithL0(DefaultLayerZeroOpts.Clone().WithToleranceDegressionCurve(20, 1, 10*time.Second, [4]float64{0, 0, 0, 0}))
		trigger.WithLn(-1, 10, DefaultOverlapOpts.Clone().WithMinPointDepthPerCluster(4))
		trigger.WithTombstone(DefaultTombstoneOpts.Clone().WithL2Count(10))
		trigger.WithVacuumCheck(DefaultVacuumOpts.Clone().WithHollowTopK(20))
		trigger.WithExpire(policyPatchExpiry)
		sched.SendTrigger(trigger)

		answer := requireQuery(t, sched, tables[0])
		require.Contains(t, answer.Triggers, "L2C: 10")

		// merge existing patch
		sched.SendTrigger(
			NewMMsgTaskTrigger(tables[0]).
				WithExpire(policyPatchExpiry).
				WithTombstone(DefaultTombstoneOpts.Clone().WithL2Count(100)),
		)

		answer = requireQuery(t, sched, tables[0])
		require.Contains(t, answer.Triggers, "L2C: 100")
	}

	{
		// Wait on the executor boundary, not elapsed time. Receiving these events
		// proves that both scheduler loops, resource admission, and completion
		// accounting have handled every trigger under test.
		expected := map[uint64]int{
			t1004.ID():     1,
			tables[1].ID(): t1002TaskCnt,
			tables[0].ID(): 1,
		}
		remaining := 0
		for _, count := range expected {
			remaining += count
		}
		timer := time.NewTimer(schedulerTestHangTimeout)
		defer timer.Stop()
		for remaining > 0 {
			select {
			case tableID := <-executor.executed:
				require.Positive(t, expected[tableID], "unexpected merge for table %d", tableID)
				expected[tableID]--
				remaining--
			case <-timer.C:
				t.Fatalf("merge scheduler did not execute all tasks: remaining=%v", expected)
			}
		}
		require.False(t, executor.overflow.Load(), "merge executor observation buffer overflowed")

		answer := requireQuery(t, sched, t1004)
		require.Equal(t, answer.DataMergeCnt, 1)

		answer = requireQuery(t, sched, tables[1])
		require.Equal(t, answer.DataMergeCnt, t1002TaskCnt)
		require.Equal(t, answer.VaccumTrigCount, 1)

		answer = requireQuery(t, sched, tables[0])
		require.Equal(t, answer.DataMergeCnt, 1)
	}

	{
		// dropped table will be removed from scheduler
		waitOrFatal(t, dropped.checked,
			"merge scheduler did not inspect the dropped table")
		answer := requireQuery(t, sched, tables[2])
		require.Equal(t, answer.NotExists, true)
	}

}

func TestVacuumRecheckArmsOnFullHollowTopKUsingInjectedClock(t *testing.T) {
	clock := newFakeClock()
	db := catalog.MockDBEntryWithAccInfo(1, 1001)
	table := catalog.ToMergeTable(catalog.MockTableEntryWithDB(db, 1001))
	sched := NewMergeScheduler(
		time.Hour,
		&dummyCatalogSource{initTables: []catalog.MergeTable{table}},
		&dummyExecutor{},
		clock,
	)
	generation := newMergeSchedulerGeneration()

	newOpts := func(hollowTopK int) *VacuumOpts {
		opts := NewVacuumOpts()
		opts.HollowTopK = hollowTopK
		opts.testInject = &vacuumTestInject{
			compactTask: []mergeTask{{
				objs: []*objectio.ObjectStats{
					newTestObjectStats(t, 1, 2, 30*common.Const1MBytes, 1000, 1, nil, 0),
				},
				note:  "test",
				level: 1,
			}},
		}
		return opts
	}

	// A partially hollow table (tasks < HollowTopK) sends only the compact
	// trigger and must not arm the recheck.
	sched.ioVacuumCheck(generation, MMsgVacuumCheck{Table: table, opts: newOpts(2)})
	require.Len(t, sched.msgChan, 1)
	clock.Advance(time.Minute)
	require.Never(t, func() bool { return len(sched.msgChan) > 1 },
		50*time.Millisecond, time.Millisecond,
		"a partially hollow table must not schedule a vacuum recheck")

	// A fully hollow table (tasks == HollowTopK) arms one recheck that fires
	// only after the injected clock crosses the 10s deadline.
	sched.ioVacuumCheck(generation, MMsgVacuumCheck{Table: table, opts: newOpts(1)})
	require.Len(t, sched.msgChan, 2)
	clock.Advance(10*time.Second - time.Nanosecond)
	require.Never(t, func() bool { return len(sched.msgChan) > 2 },
		50*time.Millisecond, time.Millisecond,
		"the vacuum recheck must not fire before its deadline")
	clock.Advance(time.Nanosecond)
	require.Eventually(t, func() bool { return len(sched.msgChan) == 3 },
		schedulerTestHangTimeout, time.Millisecond,
		"the vacuum recheck must fire once the injected clock crosses the deadline")
	<-sched.msgChan
	<-sched.msgChan
	recheck := <-sched.msgChan
	require.Equal(t, MMsgKindTrigger, recheck.Kind)
	trigger := recheck.Value.(*MMsgTaskTrigger)
	require.NotNil(t, trigger.vacuum, "the recheck must carry a vacuum check")
}

func TestSchedulerPolicyPatchExpirationUsesInjectedClock(t *testing.T) {
	clock := newFakeClock()
	db := catalog.MockDBEntryWithAccInfo(1, 1001)
	table := catalog.ToMergeTable(catalog.MockTableEntryWithDB(db, 1001))
	sched := NewMergeScheduler(
		time.Hour,
		&dummyCatalogSource{initTables: []catalog.MergeTable{table}},
		&dummyExecutor{},
		clock,
	)
	sched.PatchTestRscController(newSimRscController(16 * common.Const1GBytes))

	expiresAt := clock.Now().Add(time.Minute)
	sched.handleTaskTrigger(nil, NewMMsgTaskTrigger(table).
		WithExpire(expiresAt).
		WithTombstone(DefaultTombstoneOpts.Clone().WithL2Count(10)))
	sched.handleTaskTrigger(nil, NewMMsgTaskTrigger(table).
		WithExpire(expiresAt).
		WithTombstone(DefaultTombstoneOpts.Clone().WithL2Count(100)))

	supp := sched.supps[table.ID()]
	require.NotNil(t, supp)
	require.Len(t, supp.triggers, 1, "policy updates must merge in place")
	require.Equal(t, 100, supp.triggers[0].tomb.L2Count)

	// Expiration is strict: a patch remains valid at its deadline and is removed
	// only after the injected clock moves past it.
	clock.Advance(time.Minute)
	sched.doSched(nil, supp.todo)
	require.Len(t, supp.triggers, 1)

	clock.Advance(time.Nanosecond)
	sched.doSched(nil, supp.todo)
	require.Empty(t, supp.triggers)
}

type blockingMergeTable struct {
	catalog.MergeTable
	item catalog.MergeTombstoneItem
}

func (t *blockingMergeTable) IterTombstoneItem() iter.Seq[catalog.MergeTombstoneItem] {
	return func(yield func(catalog.MergeTombstoneItem) bool) {
		yield(t.item)
	}
}

type blockingMergeTombstoneItem struct {
	stats       *objectio.ObjectStats
	createdAt   types.TS
	enteredOnce sync.Once
	entered     chan struct{}
	release     chan struct{}
}

func (i *blockingMergeTombstoneItem) GetCreatedAt() types.TS {
	return i.createdAt
}

func (i *blockingMergeTombstoneItem) GetObjectStats() *objectio.ObjectStats {
	return i.stats
}

func (i *blockingMergeTombstoneItem) ForeachRowid(
	context.Context,
	any,
	func(types.Rowid, bool, int) error,
) error {
	i.enteredOnce.Do(func() {
		close(i.entered)
	})
	<-i.release
	return nil
}

func (i *blockingMergeTombstoneItem) MakeBufferBatch() (any, func()) {
	return struct{}{}, func() {}
}

func TestQueryAndStopBoundedWhenIOQueueFull(t *testing.T) {
	db := catalog.MockDBEntryWithAccInfo(1, 1001)
	baseTable := catalog.ToMergeTable(catalog.MockTableEntryWithDB(db, 1001))
	item := &blockingMergeTombstoneItem{
		stats: newTestObjectStats(
			t,
			1,
			2,
			2*common.DefaultMaxOsizeObjBytes,
			1,
			0,
			nil,
			0,
		),
		createdAt: types.BuildTS(time.Now().Add(-time.Hour).UnixNano(), 0),
		entered:   make(chan struct{}),
		release:   make(chan struct{}),
	}
	table := &blockingMergeTable{
		MergeTable: baseTable,
		item:       item,
	}
	source := &dummyCatalogSource{initTables: []catalog.MergeTable{table}}
	sched := NewMergeScheduler(
		time.Hour,
		source,
		&dummyExecutor{},
		NewStdClock(),
	)
	sched.Start()
	generation := sched.generation.Load()

	var releaseOnce sync.Once
	releaseIO := func() {
		releaseOnce.Do(func() {
			close(item.release)
		})
	}
	t.Cleanup(func() {
		releaseIO()
		sched.Stop()
	})

	require.NoError(t, sched.SendTrigger(
		NewMMsgTaskTrigger(table).WithVacuumCheck(DefaultVacuumOpts),
	))
	select {
	case <-item.entered:
	case <-time.After(time.Second):
		t.Fatal("vacuum I/O did not start")
	}

	for i := 0; i <= cap(generation.ioChan); i++ {
		require.NoError(t, sched.SendTrigger(
			NewMMsgTaskTrigger(table).WithVacuumCheck(DefaultVacuumOpts),
		))
	}
	require.Eventually(t, func() bool {
		return len(generation.ioChan) == cap(generation.ioChan)
	}, time.Second, time.Millisecond)

	queryCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := sched.Query(queryCtx, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	for len(sched.msgChan) < cap(sched.msgChan) {
		sched.msgChan <- &MMsg{
			Kind: MMsgKindTrigger,
			Value: NewMMsgTaskTrigger(table).
				WithVacuumCheck(DefaultVacuumOpts),
		}
	}
	sendCtx, cancelSend := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelSend()
	_, err = sched.Query(sendCtx, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	stopDone := make(chan struct{})
	go func() {
		sched.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler stop blocked behind the full I/O queue")
	}
	releaseIO()
}

func TestStoppedGenerationIOCannotCrossRestart(t *testing.T) {
	source := &dummyCatalogSource{}
	sched := NewMergeScheduler(
		time.Hour,
		source,
		&dummyExecutor{},
		NewStdClock(),
	)
	sched.Start()
	stoppedGeneration := sched.generation.Load()
	sched.Stop()

	sched.Start()
	t.Cleanup(sched.Stop)
	currentGeneration := sched.generation.Load()
	require.NotSame(t, stoppedGeneration, currentGeneration)
	require.NotEqual(t, stoppedGeneration.ioChan, currentGeneration.ioChan)

	staleMsg := &MMsg{Kind: MMsgKindVacuumCheck}
	for range cap(stoppedGeneration.ioChan) * 4 {
		require.False(t, sched.sendIOForGeneration(stoppedGeneration, staleMsg))
	}
	require.Empty(t, stoppedGeneration.ioChan)
	require.Empty(t, currentGeneration.ioChan)

	// Simulate the exact race where an old callback passed its stop check and
	// completes the send only after Stop returned and a new generation started.
	// Its message stays on the stopped generation's private queue.
	var staleIOProcessed atomic.Bool
	stoppedGeneration.ioChan <- &MMsg{
		Kind: MMsgKindConfigBootstrap,
		Value: MMsgConfigBootstrap{
			ReadSettingsBatch: func() (*batch.Batch, func()) {
				staleIOProcessed.Store(true)
				return nil, func() {}
			},
		},
		generation: stoppedGeneration,
	}

	staleAnswer := make(chan *QueryAnswer, 1)
	sched.msgChan <- &MMsg{
		Kind:       MMsgKindQuery,
		Value:      MMsgQuery{Answer: staleAnswer},
		generation: stoppedGeneration,
	}
	_, err := sched.Query(context.Background(), nil)
	require.NoError(t, err)
	require.Empty(t, staleAnswer)
	require.Never(t, staleIOProcessed.Load, 50*time.Millisecond, time.Millisecond)
	require.Empty(t, currentGeneration.ioChan)
}

func TestMergeCompletionAccountingSurvivesRestart(t *testing.T) {
	db := catalog.MockDBEntryWithAccInfo(1, 1001)
	table := catalog.ToMergeTable(catalog.MockTableEntryWithDB(db, 1001))
	source := &dummyCatalogSource{initTables: []catalog.MergeTable{table}}
	executor := &delayedCompletionExecutor{tasks: make(chan mergeTask, 1)}
	rc := newSimRscController(common.Const1GBytes)
	sched := NewMergeScheduler(time.Hour, source, executor, NewStdClock())
	sched.PatchTestRscController(rc)
	sched.Start()
	t.Cleanup(sched.Stop)

	initialAvailable := rc.Available()
	require.NoError(t, sched.SendTrigger(
		NewMMsgTaskTrigger(table).WithAssignedTasks([]mergeTask{{
			objs: []*objectio.ObjectStats{
				newTestObjectStats(
					t,
					1,
					2,
					8*common.Const1MBytes,
					1000,
					1,
					nil,
					0,
				),
			},
			note: "delayed completion across restart",
		}}),
	))

	var admitted mergeTask
	select {
	case admitted = <-executor.tasks:
	case <-time.After(time.Second):
		t.Fatal("merge task was not admitted")
	}
	require.Positive(t, admitted.eSize)
	answer, err := sched.Query(context.Background(), table)
	require.NoError(t, err)
	require.Equal(t, 1, answer.PendingMergeCnt)
	require.Equal(t, initialAvailable-int64(admitted.eSize), rc.Available())

	sched.Stop()
	sched.Start()

	admitted.doneCB.OnExecDone(nil)
	answer, err = sched.Query(context.Background(), table)
	require.NoError(t, err)
	require.Zero(t, answer.PendingMergeCnt)
	require.Equal(t, initialAvailable, rc.Available())

	// Completion observers are allowed to be notified only once. A duplicate
	// notification must not underflow the task count or release memory twice.
	admitted.doneCB.OnExecDone(nil)
	answer, err = sched.Query(context.Background(), table)
	require.NoError(t, err)
	require.Zero(t, answer.PendingMergeCnt)
	require.Equal(t, initialAvailable, rc.Available())
}

func TestTaskObserverAdmissionAndCompletionExactlyOnce(t *testing.T) {
	var calls atomic.Int64
	observer := &taskObserver{f: func() {
		calls.Add(1)
	}}

	const workers = 100
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if i%2 == 0 {
				observer.Admit()
			} else {
				observer.OnExecDone(nil)
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int64(1), calls.Load())
}

func TestLaunchPad(t *testing.T) {
	pad := newLaunchPad(NewStdClock())
	cata := catalog.MockCatalog(nil)
	db := catalog.MockDBEntryWithAccInfo(1, 1000)
	table := catalog.MockTableEntryWithDB(db, 1001)

	{
		// add objects to table

		//l0
		create := types.TS{}
		catalog.MockCreatedObjectEntry2List(table, cata, false, create.Next())
		catalog.MockCreatedObjectEntry2List(table, cata, false, create.Next())

		//l1
		for i := 0; i < 2; i++ {
			entry := catalog.MockCreatedObjectEntry2List(table, cata, false, create.Next())
			entry.SetLevel(1)
			zm := index.NewZM(types.T_int32.ToType().Oid, 0)
			zm.Update(int32(1))
			zm.Update(int32(2))
			objectio.SetObjectStatsSortKeyZoneMap(entry.GetObjectStats(), zm)
		}

		//l2
		for i := 0; i < 3; i++ {
			entry := catalog.MockCreatedObjectEntry2List(table, cata, false, create.Next())
			entry.SetLevel(2)
			zm := index.NewZM(types.T_int32.ToType().Oid, 0)
			zm.Update(int32(1))
			zm.Update(int32(2))
			objectio.SetObjectStatsSortKeyZoneMap(entry.GetObjectStats(), zm)
		}

		// l3 no zm to cause ln task error
		for i := 0; i < 3; i++ {
			entry := catalog.MockCreatedObjectEntry2List(table, cata, false, create.Next())
			entry.SetLevel(3)
		}

		// tombstone
		create = types.TS{}
		catalog.MockCreatedObjectEntry2List(table, cata, true, create.Next())
		catalog.MockCreatedObjectEntry2List(table, cata, true, create.Next())
	}

	trigger := DefaultTrigger.Clone()
	trigger.table = catalog.ToMergeTable(table)
	{
		trigger.assigns = []mergeTask{
			{
				objs: []*objectio.ObjectStats{
					newTestObjectStats(t, 1, 2, 40*common.Const1MBytes, 1000, 1, nil, 0),
					newTestObjectStats(t, 1, 2, 40*common.Const1MBytes, 1000, 1, nil, 0),
					newTestObjectStats(t, 1, 2, 40*common.Const1MBytes, 1000, 1, nil, 0),
					newTestObjectStats(t, 1, 2, 40*common.Const1MBytes, 1000, 1, nil, 0),
				},
				note: "test1",
			},
			{
				objs: []*objectio.ObjectStats{},
				note: "test2 should be removed",
			},
		}
		// prevent ln and l0 task
		trigger.startlv = 0
		trigger.endlv = 10
		trigger.l0.End = 5
		trigger.ln.MinPointDepthPerCluster = 5
	}

	pad.InitWithTrigger(trigger, time.Now())

	require.Equal(t, 2, len(pad.leveledObjects[0]))
	require.Equal(t, 2, len(pad.leveledObjects[1]))
	require.Equal(t, 3, len(pad.leveledObjects[2]))

	rc := rscthrottler.NewMemThrottler("TestLaunchPad", 1,
		rscthrottler.WithAllowOutOfLimitAcquire(),
		rscthrottler.WithConstLimit(100*common.Const1MBytes),
	)

	tasks := pad.gatherByTrigger(context.Background(), trigger, time.Now(), rc)
	require.Equal(t, 1, len(tasks))
	require.Contains(t, tasks[0].note, "test1")
	require.Contains(t, tasks[0].note, "reduce")

}

func TestXxx(t *testing.T) {
	now := time.Now().AddDate(0, 0, 3)
	t.Log(now.UnixNano(), " // ", now)
}
