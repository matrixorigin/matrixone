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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCoverage_mergeTask_String(t *testing.T) {
	task := mergeTask{
		isTombstone: true,
		level:       2,
		note:        "test task",
		objs:        make([]*objectio.ObjectStats, 3),
		oSize:       1024 * 1024,
	}

	s := task.String()
	assert.Contains(t, s, "isTombstone: true")
	assert.Contains(t, s, "level: 2")
	assert.Contains(t, s, "note: test task")
	assert.Contains(t, s, "objs: 3")
}

func TestCoverage_mergeTask_String_Empty(t *testing.T) {
	task := mergeTask{}
	s := task.String()
	assert.Contains(t, s, "isTombstone: false")
	assert.Contains(t, s, "level: 0")
	assert.Contains(t, s, "objs: 0")
}

func TestCoverage_MMsgTaskTrigger_IsEmptyTrigger(t *testing.T) {
	t.Run("empty trigger", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{}
		assert.True(t, trigger.IsEmptyTrigger())
	})

	t.Run("with l0", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{l0: &LayerZeroOpts{}}
		assert.False(t, trigger.IsEmptyTrigger())
	})

	t.Run("with ln", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{ln: &OverlapOpts{}}
		assert.False(t, trigger.IsEmptyTrigger())
	})

	t.Run("with tomb", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{tomb: &TombstoneOpts{}}
		assert.False(t, trigger.IsEmptyTrigger())
	})

	t.Run("with vacuum", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{vacuum: &VacuumOpts{}}
		assert.False(t, trigger.IsEmptyTrigger())
	})

	t.Run("with assigns", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{assigns: []mergeTask{{}}}
		assert.False(t, trigger.IsEmptyTrigger())
	})
}

func TestCoverage_MMsgTaskTrigger_String(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{}
		s := trigger.String()
		assert.Equal(t, "", s)
	})

	t.Run("with expire", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{
			expire: time.Now().Add(10 * time.Second),
		}
		s := trigger.String()
		assert.Contains(t, s, "expire")
	})

	t.Run("with l0", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{
			l0: DefaultLayerZeroOpts,
		}
		s := trigger.String()
		assert.NotEmpty(t, s)
	})

	t.Run("with ln", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{
			ln:      DefaultOverlapOpts,
			startlv: 1,
			endlv:   3,
		}
		s := trigger.String()
		assert.Contains(t, s, "(1->3)")
	})

	t.Run("with tomb", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{
			tomb: DefaultTombstoneOpts,
		}
		s := trigger.String()
		assert.NotEmpty(t, s)
	})

	t.Run("with vacuum", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{
			vacuum: DefaultVacuumOpts,
		}
		s := trigger.String()
		assert.NotEmpty(t, s)
	})

	t.Run("with assigns", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{
			assigns: []mergeTask{{}, {}},
		}
		s := trigger.String()
		assert.Contains(t, s, "assigns: 2")
	})

	t.Run("with all fields", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{
			expire:  time.Now().Add(10 * time.Second),
			l0:      DefaultLayerZeroOpts,
			ln:      DefaultOverlapOpts,
			startlv: 1,
			endlv:   5,
			tomb:    DefaultTombstoneOpts,
			vacuum:  DefaultVacuumOpts,
			assigns: []mergeTask{{}},
		}
		s := trigger.String()
		assert.Contains(t, s, "expire")
		assert.Contains(t, s, " || ")
		assert.Contains(t, s, "assigns: 1")
	})
}

func TestCoverage_MMsgTaskTrigger_Clone(t *testing.T) {
	original := DefaultTrigger.Clone()
	original.byUser = true
	original.expire = time.Now().Add(5 * time.Minute)
	original.assigns = []mergeTask{{note: "test"}}

	cloned := original.Clone()

	assert.Equal(t, original.byUser, cloned.byUser)
	assert.Equal(t, original.expire, cloned.expire)
	assert.Equal(t, original.startlv, cloned.startlv)
	assert.Equal(t, original.endlv, cloned.endlv)
	assert.Equal(t, len(original.assigns), len(cloned.assigns))

	// Verify independence
	cloned.byUser = false
	assert.True(t, original.byUser)
}

func TestCoverage_MMsgTaskTrigger_WithBuilders(t *testing.T) {
	t.Run("WithByUser", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{}
		result := trigger.WithByUser(true)
		assert.True(t, result.byUser)
		assert.Same(t, trigger, result)
	})

	t.Run("WithExpire", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{}
		exp := time.Now().Add(time.Hour)
		result := trigger.WithExpire(exp)
		assert.Equal(t, exp, result.expire)
	})

	t.Run("WithL0", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{}
		opts := &LayerZeroOpts{}
		result := trigger.WithL0(opts)
		assert.Same(t, opts, result.l0)
	})

	t.Run("WithHandleBigOld", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{}
		result := trigger.WithHandleBigOld(true)
		assert.True(t, result.handleBigOld)
	})

	t.Run("WithLn clamps startlv", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{}
		opts := &OverlapOpts{}
		result := trigger.WithLn(0, 5, opts)
		assert.Equal(t, 1, result.startlv, "startlv should be clamped to 1")
		assert.Equal(t, 5, result.endlv)
	})

	t.Run("WithLn clamps endlv", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{}
		opts := &OverlapOpts{}
		result := trigger.WithLn(2, MAX_LV+10, opts)
		assert.Equal(t, 2, result.startlv)
		assert.Equal(t, MAX_LV, result.endlv, "endlv should be clamped to MAX_LV")
	})

	t.Run("WithTombstone", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{}
		opts := &TombstoneOpts{}
		result := trigger.WithTombstone(opts)
		assert.Same(t, opts, result.tomb)
	})

	t.Run("WithVacuumCheck", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{}
		opts := &VacuumOpts{}
		result := trigger.WithVacuumCheck(opts)
		assert.Same(t, opts, result.vacuum)
	})

	t.Run("WithAssignedTasks", func(t *testing.T) {
		trigger := &MMsgTaskTrigger{}
		tasks := []mergeTask{{note: "task1"}, {note: "task2"}}
		result := trigger.WithAssignedTasks(tasks)
		assert.Equal(t, 2, len(result.assigns))
	})
}

func TestCoverage_NewMMsgTaskTrigger(t *testing.T) {
	trigger := NewMMsgTaskTrigger(nil)
	assert.NotNil(t, trigger)
	assert.Nil(t, trigger.table)
	assert.Equal(t, -1, trigger.startlv)
	assert.Equal(t, -1, trigger.endlv)
}

func TestCoverage_MMsgTaskTrigger_Merge(t *testing.T) {
	t.Run("merge expire", func(t *testing.T) {
		base := &MMsgTaskTrigger{}
		exp := time.Now().Add(time.Hour)
		other := &MMsgTaskTrigger{expire: exp}
		base.Merge(other)
		assert.Equal(t, exp, base.expire)
	})

	t.Run("merge l0", func(t *testing.T) {
		base := &MMsgTaskTrigger{}
		l0 := &LayerZeroOpts{}
		other := &MMsgTaskTrigger{l0: l0}
		base.Merge(other)
		assert.Same(t, l0, base.l0)
	})

	t.Run("merge ln", func(t *testing.T) {
		base := &MMsgTaskTrigger{}
		ln := &OverlapOpts{}
		other := &MMsgTaskTrigger{ln: ln, startlv: 2, endlv: 4}
		base.Merge(other)
		assert.Same(t, ln, base.ln)
		assert.Equal(t, 2, base.startlv)
		assert.Equal(t, 4, base.endlv)
	})

	t.Run("merge tomb", func(t *testing.T) {
		base := &MMsgTaskTrigger{}
		tomb := &TombstoneOpts{}
		other := &MMsgTaskTrigger{tomb: tomb}
		base.Merge(other)
		assert.Same(t, tomb, base.tomb)
	})

	t.Run("merge vacuum", func(t *testing.T) {
		base := &MMsgTaskTrigger{}
		vacuum := &VacuumOpts{}
		other := &MMsgTaskTrigger{vacuum: vacuum}
		base.Merge(other)
		assert.Same(t, vacuum, base.vacuum)
	})

	t.Run("nil fields not merged", func(t *testing.T) {
		l0 := &LayerZeroOpts{}
		base := &MMsgTaskTrigger{l0: l0}
		other := &MMsgTaskTrigger{}
		base.Merge(other)
		assert.Same(t, l0, base.l0, "l0 should not be overwritten by nil")
	})
}

func TestCoverage_NewMergeTaskFromSpecObjects(t *testing.T) {
	t.Run("with objects", func(t *testing.T) {
		obj1 := objectio.NewObjectStats()
		obj2 := objectio.NewObjectStats()
		objs := []*objectio.ObjectStats{obj1, obj2}

		tasks := NewMergeTaskFromSpecObjects(objs, 3)
		require.Len(t, tasks, 1)
		assert.Equal(t, int8(3), tasks[0].level)
		assert.False(t, tasks[0].isTombstone)
		assert.Equal(t, "user specified objects", tasks[0].note)
		assert.Len(t, tasks[0].objs, 2)
	})

	t.Run("empty objects", func(t *testing.T) {
		tasks := NewMergeTaskFromSpecObjects(nil, 0)
		require.Len(t, tasks, 1)
		assert.Nil(t, tasks[0].objs)
	})
}

func TestCoverage_todoPQ(t *testing.T) {
	t.Run("Len", func(t *testing.T) {
		pq := todoPQ{}
		assert.Equal(t, 0, pq.Len())

		pq = append(pq, &todoItem{})
		assert.Equal(t, 1, pq.Len())
	})

	t.Run("Less", func(t *testing.T) {
		now := time.Now()
		pq := todoPQ{
			{readyAt: now},
			{readyAt: now.Add(time.Second)},
		}
		assert.True(t, pq.Less(0, 1))
		assert.False(t, pq.Less(1, 0))
	})

	t.Run("Swap", func(t *testing.T) {
		item0 := &todoItem{index: 0, readyAt: time.Now()}
		item1 := &todoItem{index: 1, readyAt: time.Now().Add(time.Second)}
		pq := todoPQ{item0, item1}

		pq.Swap(0, 1)
		assert.Equal(t, 0, item1.index)
		assert.Equal(t, 1, item0.index)
		assert.Same(t, item1, pq[0])
		assert.Same(t, item0, pq[1])
	})

	t.Run("Push and Pop", func(t *testing.T) {
		pq := &todoPQ{}
		item := &todoItem{readyAt: time.Now()}
		pq.Push(item)
		assert.Equal(t, 1, pq.Len())
		assert.Equal(t, 0, item.index)

		popped := pq.Pop().(*todoItem)
		assert.Same(t, item, popped)
		assert.Equal(t, -1, popped.index)
		assert.Equal(t, 0, pq.Len())
	})

	t.Run("Peek", func(t *testing.T) {
		now := time.Now()
		pq := &todoPQ{
			{readyAt: now, index: 0},
		}
		assert.Equal(t, now, pq.Peek().readyAt)
	})
}

func TestCoverage_taskObserver(t *testing.T) {
	called := false
	obs := &taskObserver{
		f: func() { called = true },
	}
	obs.OnExecDone(nil)
	assert.True(t, called)
}

func TestCoverage_CNActiveObjectsString(t *testing.T) {
	sched := &MergeScheduler{}
	assert.Equal(t, "", sched.CNActiveObjectsString())
}

func TestCoverage_RemoveCNActiveObjects(t *testing.T) {
	sched := &MergeScheduler{}
	// Should not panic
	sched.RemoveCNActiveObjects(nil)
	sched.RemoveCNActiveObjects([]objectio.ObjectId{})
}

func TestCoverage_PruneCNActiveObjects(t *testing.T) {
	sched := &MergeScheduler{}
	// Should not panic
	sched.PruneCNActiveObjects(0, time.Second)
}

func TestCoverage_MMsgKind_Constants(t *testing.T) {
	assert.Equal(t, MMsgKind(0), MMsgKindSwitch)
	assert.Equal(t, MMsgKind(1), MMsgKindQuery)
	assert.Equal(t, MMsgKind(2), MMsgKindTableChange)
	assert.Equal(t, MMsgKind(3), MMsgKindTrigger)
	assert.Equal(t, MMsgKind(4), MMsgKindConfig)
	assert.Equal(t, MMsgKind(5), MMsgKindVacuumCheck)
	assert.Equal(t, MMsgKind(6), MMsgKindConfigBootstrap)
}

func TestCoverage_DefaultTrigger(t *testing.T) {
	assert.NotNil(t, DefaultTrigger)
	assert.NotNil(t, DefaultTrigger.l0)
	assert.NotNil(t, DefaultTrigger.ln)
	assert.NotNil(t, DefaultTrigger.tomb)
	assert.NotNil(t, DefaultTrigger.vacuum)
	assert.Equal(t, 1, DefaultTrigger.startlv)
	assert.Equal(t, MAX_LV, DefaultTrigger.endlv)
}

func TestCoverage_QueryAnswer(t *testing.T) {
	qa := &QueryAnswer{
		GlobalAutoMergeOn: true,
		MsgQueueLen:       10,
		AutoMergeOn:       true,
		NextCheckDue:      time.Second,
		DataMergeCnt:      5,
		TombstoneMergeCnt: 2,
		PendingMergeCnt:   3,
		VaccumTrigCount:   1,
		NotExists:         false,
	}
	assert.True(t, qa.GlobalAutoMergeOn)
	assert.Equal(t, 10, qa.MsgQueueLen)
}

func TestCoverage_mergeTask_StringVerbose(t *testing.T) {
	objs := make([]*objectio.ObjectStats, 5)
	for i := range objs {
		objs[i] = objectio.NewObjectStats()
		objectio.SetObjectStatsSize(objs[i], uint32(i+1)*1024)
	}
	task := mergeTask{
		isTombstone: false,
		level:       1,
		note:        fmt.Sprintf("compact %d objects", len(objs)),
		objs:        objs,
		oSize:       15360,
		eSize:       12000,
	}
	s := task.String()
	assert.Contains(t, s, "objs: 5")
	assert.Contains(t, s, "level: 1")
	assert.Contains(t, s, "compact 5 objects")
	assert.Contains(t, s, common.HumanReadableBytes(15360))
}
