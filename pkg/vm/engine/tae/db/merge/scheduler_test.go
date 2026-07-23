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

func requireQuery(
	t *testing.T,
	sched *MergeScheduler,
	table catalog.MergeTable,
) *QueryAnswer {
	t.Helper()
	answer, err := sched.Query(context.Background(), table)
	require.NoError(t, err)
	return answer
}

type droppedMergeTable struct {
	catalog.MergeTable
}

func (t *droppedMergeTable) HasDropCommitted() bool {
	return true
}

func TestHandleTaskTriggerNilPointerFixed(t *testing.T) {
	// Setup: Create a MergeScheduler with empty supps map
	scheduler := &MergeScheduler{
		supps:   make(map[uint64]*todoSupporter),
		msgChan: make(chan *MMsg, 4096),
		ioChan:  make(chan *MMsg, 256),
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
		scheduler.handleTaskTrigger(msg)
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
		scheduler.doSched(&todoItem{table: mockTable})
	})

	todo := &todoItem{table: mockTable, readyAt: scheduler.clock.Now()}
	heap.Push(&scheduler.pq, todo)

	require.NotPanics(t, func() {
		scheduler.doSched(todo)
	})
	require.Equal(t, 0, scheduler.pq.Len())
}

func TestScheduler(t *testing.T) {

	newTestTable := func(did, tid uint64) catalog.MergeTable {
		db := catalog.MockDBEntryWithAccInfo(did, tid)
		table := catalog.MockTableEntryWithDB(db, tid)
		return catalog.ToMergeTable(table)
	}

	tables := []catalog.MergeTable{
		newTestTable(1, 1001),
		newTestTable(1, 1002),
		&droppedMergeTable{MergeTable: newTestTable(1, 1003)},
	}

	dummySource := &dummyCatalogSource{
		settingsFn: oneGoodOneBad,
		initTables: tables,
	}

	sched := NewMergeScheduler(
		1*time.Millisecond,
		dummySource,
		&dummyExecutor{},
		NewStdClock(),
	)

	sched.Start()
	defer sched.Stop()

	time.Sleep(3 * time.Millisecond)

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

	t1002TaskCnt := bigDataTaskCntThreshold + 1
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
		opts2.HollowTopK = 1
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

		trigger := NewMMsgTaskTrigger(tables[0])
		trigger.WithL0(DefaultLayerZeroOpts.Clone().WithToleranceDegressionCurve(20, 1, 10*time.Second, [4]float64{0, 0, 0, 0}))
		trigger.WithLn(-1, 10, DefaultOverlapOpts.Clone().WithMinPointDepthPerCluster(4))
		trigger.WithTombstone(DefaultTombstoneOpts.Clone().WithL2Count(10))
		trigger.WithVacuumCheck(DefaultVacuumOpts.Clone().WithHollowTopK(20))
		trigger.WithExpire(time.Now().Add(50 * time.Millisecond))
		sched.SendTrigger(trigger)

		answer := requireQuery(t, sched, tables[0])
		require.Contains(t, answer.Triggers, "L2C: 10")

		// merge existing patch
		sched.SendTrigger(
			NewMMsgTaskTrigger(tables[0]).
				WithExpire(time.Now().Add(25 * time.Millisecond)).
				WithTombstone(DefaultTombstoneOpts.Clone().WithL2Count(100)),
		)

		answer = requireQuery(t, sched, tables[0])
		require.Contains(t, answer.Triggers, "L2C: 100")
	}

	{

		var answer *QueryAnswer

		for i := 0; i < 100; i++ {
			answer = requireQuery(t, sched, t1004)
			if answer.DataMergeCnt == 1 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		require.Equal(t, answer.DataMergeCnt, 1)

		for i := 0; i < 100; i++ {
			answer = requireQuery(t, sched, tables[1])
			if answer.DataMergeCnt == t1002TaskCnt {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		require.Equal(t, answer.DataMergeCnt, t1002TaskCnt)
		require.Equal(t, answer.VaccumTrigCount, 1)

		for i := 0; i < 100; i++ {
			answer = requireQuery(t, sched, tables[0])
			if answer.DataMergeCnt == 1 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		require.Equal(t, answer.DataMergeCnt, 1)
	}

	{
		// dropped table will be removed from scheduler
		answer := requireQuery(t, sched, tables[2])
		require.Equal(t, answer.NotExists, true)
	}

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

	for i := 0; i <= cap(sched.ioChan); i++ {
		require.NoError(t, sched.SendTrigger(
			NewMMsgTaskTrigger(table).WithVacuumCheck(DefaultVacuumOpts),
		))
	}
	require.Eventually(t, func() bool {
		return len(sched.ioChan) == cap(sched.ioChan)
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

func TestClosedGenerationNeverEnqueuesIO(t *testing.T) {
	sched := &MergeScheduler{
		ioChan: make(chan *MMsg, 256),
	}
	stopCh := make(chan struct{})
	close(stopCh)
	msg := &MMsg{Kind: MMsgKindVacuumCheck}

	for range cap(sched.ioChan) * 4 {
		require.False(t, sched.sendIOForGeneration(&stopCh, msg))
	}
	require.Empty(t, sched.ioChan)
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
