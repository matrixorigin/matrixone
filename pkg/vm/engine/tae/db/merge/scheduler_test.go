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
	"context"
	"iter"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

var noDefaultSettings = func() (*batch.Batch, func()) { return nil, nil }

func (c *dummyCatalogSource) GetMergeSettingsBatchFn() func() (*batch.Batch, func()) {
	return c.settingsFn
}

type droppedMergeTable struct {
	catalog.MergeTable
}

func (t *droppedMergeTable) HasDropCommitted() bool {
	return true
}

func TestScheduler(t *testing.T) {

	newTestTable := func(did, tid uint64) catalog.MergeTable {
		db := catalog.MockDBEntryWithAccInfo(1, tid)
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
		// dropped table will be removed from scheduler
		answer := sched.Query(tables[2])
		require.Equal(t, answer.NotExists, true)
	}

	{
		// switch on/off
		sched.PauseTable(tables[0])
		answer := sched.Query(tables[0])
		require.Equal(t, answer.AutoMergeOn, false)
		sched.ResumeTable(tables[0])
		answer = sched.Query(tables[0])
		require.Equal(t, answer.AutoMergeOn, true)
		// next check due will be 1s later because of the resume
		require.Greater(t, answer.NextCheckDue, 900*time.Millisecond)

		sched.PauseAll()
		answer = sched.Query(nil)
		require.Equal(t, answer.GlobalAutoMergeOn, false)
		sched.ResumeAll()
		answer = sched.Query(nil)
		require.Equal(t, answer.GlobalAutoMergeOn, true)
	}

	{
		// mock object create events to bring table1001 ahead
		for i := 0; i < 6; i++ {
			sched.OnCreateNonAppendObject(tables[0])
		}
		answer := sched.Query(tables[0])
		require.Less(t, answer.NextCheckDue, 500*time.Millisecond)
	}

	t1004 := newTestTable(1, 1004)
	{
		// create new table
		sched.OnCreateTableCommit(t1004)
		answer := sched.Query(t1004)
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

		answer := sched.Query(tables[0])
		require.Contains(t, answer.Triggers, "L2C: 10")

		// merge existing patch
		sched.SendTrigger(
			NewMMsgTaskTrigger(tables[0]).
				WithExpire(time.Now().Add(25 * time.Millisecond)).
				WithTombstone(DefaultTombstoneOpts.Clone().WithL2Count(100)),
		)

		answer = sched.Query(tables[0])
		require.Contains(t, answer.Triggers, "L2C: 100")
	}

	{

		var answer *QueryAnswer

		for i := 0; i < 100; i++ {
			answer = sched.Query(t1004)
			if answer.DataMergeCnt == 1 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		require.Equal(t, answer.DataMergeCnt, 1)

		for i := 0; i < 100; i++ {
			answer = sched.Query(tables[1])
			if answer.DataMergeCnt == t1002TaskCnt {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		require.Equal(t, answer.DataMergeCnt, t1002TaskCnt)
		require.Equal(t, answer.VaccumTrigCount, 1)

		for i := 0; i < 100; i++ {
			answer = sched.Query(tables[0])
			if answer.DataMergeCnt == 1 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		require.Equal(t, answer.DataMergeCnt, 1)
	}

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

	rc := new(resourceController)
	rc.setMemLimit(100 * common.Const1MBytes)
	rc.refresh()
	tasks := pad.gatherByTrigger(context.Background(), trigger, time.Now(), rc)
	require.Equal(t, 1, len(tasks))
	require.Contains(t, tasks[0].note, "test1")
	require.Contains(t, tasks[0].note, "reduce")

}

func TestXxx(t *testing.T) {
	now := time.Now().AddDate(0, 0, 3)
	t.Log(now.UnixNano(), " // ", now)
}
