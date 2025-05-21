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
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"go.uber.org/zap"
)

const (
	bigDataTaskCntThreshold   = 3
	objectOpsTriggerThreshold = 5
)

type mergeTask struct {
	objs        []*objectio.ObjectStats
	kind        taskHostKind
	isTombstone bool
	level       int8
	note        string
	oSize       int
	eSize       int
	doneCB      *taskObserver
}

func (r *mergeTask) String() string {
	return fmt.Sprintf(
		"mergeTask{isTombstone: %v, level: %d, note: %s, objs: %v, oSize: %s}",
		r.isTombstone, r.level, r.note, len(r.objs), common.HumanReadableBytes(r.oSize),
	)
}

type MergeScheduler struct {
	// determine the checking order of tables
	pq todoPQ
	// record the status of tables, facilitate the control of table pq
	supps map[uint64]*todoSupporter
	// fallback to check the status of the priority queue
	heartbeat *time.Ticker

	// control flow
	allPaused bool
	stopCh    atomic.Pointer[chan struct{}]
	stopRecv  chan struct{}
	stopped   atomic.Bool

	msgChan chan *MMsg
	ioChan  chan MMsgVacuumCheck

	pad            *launchPad
	defaultTrigger *MMsgTaskTrigger

	baseInterval time.Duration
	rc           *resourceController
	executor     MergeTaskExecutor
}

func NewMergeScheduler(
	baseInterval time.Duration,
	cata catalog.CatalogEventSource,
	executor MergeTaskExecutor,
) *MergeScheduler {
	sched := &MergeScheduler{
		rc:           new(resourceController),
		baseInterval: baseInterval,
		executor:     executor,

		supps:     make(map[uint64]*todoSupporter),
		heartbeat: time.NewTicker(time.Second * 10),

		stopRecv: make(chan struct{}, 1),
		msgChan:  make(chan *MMsg, 4096),
		ioChan:   make(chan MMsgVacuumCheck, 1024),

		pad:            newLaunchPad(),
		defaultTrigger: DefaultTrigger.Clone(),
	}

	sched.stopped.Store(true)

	// init priority queue
	for table := range cata.InitSource() {
		sched.handleAddTable(table)
	}
	cata.SetMergeNotifier(sched)

	sched.rc.refresh()
	return sched

}

func (a *MergeScheduler) Stop() {
	if a.stopped.CompareAndSwap(false, true) {
		ch := a.stopCh.Load()
		if ch != nil {
			close(*ch)
		}
		<-a.stopRecv
	}
}

func (a *MergeScheduler) Start() {
	if a.stopped.CompareAndSwap(true, false) {
		ch := make(chan struct{})
		a.stopCh.Store(&ch)
		a.heartbeat.Reset(time.Second * 10)
		go a.handleMainLoop()
		go a.handleIOLoop()
	}
}

// region: on events

func (a *MergeScheduler) OnCreateTableCommit(table catalog.MergeTable) {
	a.msgChan <- &MMsg{
		Kind: MMsgKindTableChange,
		Value: MMsgTableChange{
			Table:  table,
			Create: true,
		},
	}
}

func (a *MergeScheduler) OnCreateNonAppendObject(table catalog.MergeTable) {
	a.msgChan <- &MMsg{
		Kind: MMsgKindTableChange,
		Value: MMsgTableChange{
			Table:     table,
			ObjChange: true,
		},
	}
}

func (a *MergeScheduler) OnMergeDone(table catalog.MergeTable, esize int) {
	a.msgChan <- &MMsg{
		Kind: MMsgKindTableChange,
		Value: MMsgTableChange{
			Table:    table,
			DoneTask: true,
			EstSize:  esize,
		},
	}
}

func (a *MergeScheduler) taskObserverFactory(
	t catalog.MergeTable,
	size int,
) *taskObserver {
	return &taskObserver{f: func() { a.OnMergeDone(t, size) }}
}

type taskObserver struct {
	f func()
}

func (o *taskObserver) OnExecDone(_ any) {
	o.f()
}

// legacy interface for big tombstone write
func (a *MergeScheduler) StopMerge(tblEntry *catalog.TableEntry, reentrant bool, rt *dbutils.Runtime) error {
	c := new(engine.ConstraintDef)
	binary := tblEntry.GetLastestSchema(false).Constraint
	err := c.UnmarshalBinary(binary)
	if err != nil {
		return err
	}
	indexTableNames := make([]string, 0, len(c.Cts))
	for _, constraint := range c.Cts {
		if indices, ok := constraint.(*engine.IndexDef); ok {
			for _, index := range indices.Indexes {
				indexTableNames = append(indexTableNames, index.IndexTableName)
			}
		}
	}

	return rt.LockMergeService.LockFromUser(
		tblEntry.GetID(),
		tblEntry.GetLastestSchema(false).Name,
		reentrant,
		indexTableNames...,
	)
}

func (a *MergeScheduler) StartMerge(rt *dbutils.Runtime, tblID uint64, reentrant bool) error {
	return rt.LockMergeService.UnlockFromUser(tblID, reentrant)
}

func (a *MergeScheduler) CNActiveObjectsString() string { return "" }

func (a *MergeScheduler) RemoveCNActiveObjects(ids []objectio.ObjectId) {}

func (a *MergeScheduler) PruneCNActiveObjects(id uint64, ago time.Duration) {}

// region: msg def & sender

type MMsgKind int

const (
	MMsgKindSwitch MMsgKind = iota
	MMsgKindQuery
	MMsgKindTableChange
	MMsgKindTrigger
)

type MMsgSwitch struct {
	Table catalog.MergeTable
	On    bool
}

type QueryAnswer struct {
	// global status
	GlobalAutoMergeOn bool
	MsgQueueLen       int

	// table status
	AutoMergeOn       bool
	NextCheckDue      time.Duration
	DataMergeCnt      int
	TombstoneMergeCnt int
	PendingMergeCnt   int
	BigDataAcc        int
	Triggers          string

	NotExists bool
}

type MMsgQuery struct {
	Table  catalog.MergeTable
	Answer chan *QueryAnswer
}

type MMsgTableChange struct {
	Table     catalog.MergeTable
	Create    bool
	ObjChange bool
	DoneTask  bool
	EstSize   int
}

type MMsgVacuumCheck struct {
	Table catalog.MergeTable
	opts  *VacuumOpts
}

var DefaultTrigger = &MMsgTaskTrigger{
	l0:      DefaultLayerZeroOpts,
	startlv: 1,
	endlv:   MAX_LV,
	ln:      DefaultOverlapOpts,
	tomb:    DefaultTombstoneOpts,
	vacuum:  DefaultVacuumOpts,
}

type MMsgTaskTrigger struct {
	expire time.Time
	byUser bool

	table catalog.MergeTable

	// l0
	l0           *LayerZeroOpts
	handleBigOld bool

	// ln
	startlv int
	endlv   int
	ln      *OverlapOpts

	// tombstone
	tomb *TombstoneOpts

	// vacuum
	vacuum *VacuumOpts

	// assigned tasks
	assigns []mergeTask
}

func (b *MMsgTaskTrigger) Clone() *MMsgTaskTrigger {
	return &MMsgTaskTrigger{
		expire:       b.expire,
		byUser:       b.byUser,
		table:        b.table,
		l0:           b.l0.Clone(),
		handleBigOld: b.handleBigOld,
		startlv:      b.startlv,
		endlv:        b.endlv,
		ln:           b.ln.Clone(),
		tomb:         b.tomb.Clone(),
		vacuum:       b.vacuum.Clone(),
		assigns:      slices.Clone(b.assigns),
	}
}

func (b *MMsgTaskTrigger) IsEmptyTrigger() bool {
	return b.l0 == nil &&
		b.ln == nil &&
		b.tomb == nil &&
		b.vacuum == nil &&
		len(b.assigns) == 0
}

func (b *MMsgTaskTrigger) String() string {
	var buf bytes.Buffer
	if !b.expire.IsZero() {
		buf.WriteString(fmt.Sprintf("expire{%s}", time.Until(b.expire).Round(time.Second)))
	}
	if b.l0 != nil {
		if buf.Len() > 0 {
			buf.WriteString(" || ")
		}
		buf.WriteString(b.l0.String())
	}
	if b.ln != nil {
		if buf.Len() > 0 {
			buf.WriteString(" || ")
		}
		buf.WriteString(b.ln.String())
		buf.WriteString(fmt.Sprintf("(%d->%d)", b.startlv, b.endlv))
	}
	if b.tomb != nil {
		if buf.Len() > 0 {
			buf.WriteString(" || ")
		}
		buf.WriteString(b.tomb.String())
	}
	if b.vacuum != nil {
		if buf.Len() > 0 {
			buf.WriteString(" || ")
		}
		buf.WriteString(b.vacuum.String())
	}
	if len(b.assigns) > 0 {
		if buf.Len() > 0 {
			buf.WriteString(" || ")
		}
		buf.WriteString(fmt.Sprintf("assigns: %v", len(b.assigns)))
	}
	return buf.String()
}

func (b *MMsgTaskTrigger) WithByUser(byUser bool) *MMsgTaskTrigger {
	b.byUser = byUser
	return b
}

func (b *MMsgTaskTrigger) WithExpire(expire time.Time) *MMsgTaskTrigger {
	b.expire = expire
	return b
}

func NewMMsgTaskTrigger(table catalog.MergeTable) *MMsgTaskTrigger {
	return &MMsgTaskTrigger{table: table, startlv: -1, endlv: -1}
}

func (b *MMsgTaskTrigger) WithL0(opts *LayerZeroOpts) *MMsgTaskTrigger {
	b.l0 = opts
	return b
}

func (b *MMsgTaskTrigger) WithHandleBigOld(handleBigOld bool) *MMsgTaskTrigger {
	b.handleBigOld = handleBigOld
	return b
}

func (b *MMsgTaskTrigger) WithLn(
	startlv int,
	endlv int,
	opts *OverlapOpts,
) *MMsgTaskTrigger {
	if startlv < 1 {
		startlv = 1
	}
	if endlv > MAX_LV {
		endlv = MAX_LV
	}
	b.ln = opts
	b.startlv = startlv
	b.endlv = endlv
	return b
}

func (b *MMsgTaskTrigger) WithTombstone(opts *TombstoneOpts) *MMsgTaskTrigger {
	b.tomb = opts
	return b
}

func (b *MMsgTaskTrigger) WithVacuumCheck(opts *VacuumOpts) *MMsgTaskTrigger {
	b.vacuum = opts
	return b
}

func (b *MMsgTaskTrigger) WithAssignedTasks(tasks []mergeTask) *MMsgTaskTrigger {
	b.assigns = tasks
	return b
}

func (b *MMsgTaskTrigger) Merge(o *MMsgTaskTrigger) *MMsgTaskTrigger {
	if !o.expire.IsZero() {
		b.expire = o.expire
	}
	if o.l0 != nil {
		b.l0 = o.l0
	}
	if o.ln != nil {
		b.startlv = o.startlv
		b.endlv = o.endlv
		b.ln = o.ln
	}
	if o.tomb != nil {
		b.tomb = o.tomb
	}
	if o.vacuum != nil {
		b.vacuum = o.vacuum
	}
	return b
}

type MMsg struct {
	Kind  MMsgKind
	Value any
}

type todoItem struct {
	index   int
	readyAt time.Time
	table   catalog.MergeTable
}

func (a *MergeScheduler) Query(table catalog.MergeTable) *QueryAnswer {
	answer := make(chan *QueryAnswer)
	a.msgChan <- &MMsg{
		Kind:  MMsgKindQuery,
		Value: MMsgQuery{Table: table, Answer: answer},
	}
	return <-answer
}

func (a *MergeScheduler) PauseAll() {
	a.msgChan <- &MMsg{
		Kind: MMsgKindSwitch,
		Value: MMsgSwitch{
			On:    false,
			Table: nil,
		},
	}
}

func (a *MergeScheduler) ResumeAll() {
	a.msgChan <- &MMsg{
		Kind: MMsgKindSwitch,
		Value: MMsgSwitch{
			On:    true,
			Table: nil,
		},
	}
}

func (a *MergeScheduler) PauseTable(table catalog.MergeTable) {
	a.msgChan <- &MMsg{
		Kind: MMsgKindSwitch,
		Value: MMsgSwitch{
			On:    false,
			Table: table,
		},
	}
}

func (a *MergeScheduler) ResumeTable(table catalog.MergeTable) {
	a.msgChan <- &MMsg{
		Kind: MMsgKindSwitch,
		Value: MMsgSwitch{
			On:    true,
			Table: table,
		},
	}
}

func (a *MergeScheduler) SendTrigger(trigger *MMsgTaskTrigger) error {
	a.msgChan <- &MMsg{
		Kind:  MMsgKindTrigger,
		Value: trigger,
	}
	return nil
}

// region: priority queue
type todoPQ []*todoItem

func (pq todoPQ) Len() int { return len(pq) }

func (pq todoPQ) Less(i, j int) bool {
	return pq[i].readyAt.Before(pq[j].readyAt)
}

func (pq todoPQ) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *todoPQ) Push(x any) {
	n := len(*pq)
	item := x.(*todoItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *todoPQ) Pop() any {
	n := len(*pq)
	item := (*pq)[n-1]
	item.index = -1
	*pq = (*pq)[0 : n-1]
	return item
}

func (pq *todoPQ) Peek() *todoItem {
	return (*pq)[0]
}

func (pq *todoPQ) Update(item *todoItem, ready time.Time) {
	item.readyAt = ready
	heap.Fix(pq, item.index)
}

type todoSupporter struct {
	mergingTaskCnt         int
	bigTaskCnt             int
	objectOperations       int
	totalDataMergeCnt      int
	totalTombstoneMergeCnt int
	paused                 bool
	nextDue                time.Duration
	lastMergeTime          time.Time
	todo                   *todoItem
	triggers               []*MMsgTaskTrigger
}

func (m *todoSupporter) DoneTask() {
	m.mergingTaskCnt--
	if m.mergingTaskCnt < 0 {
		logutil.Error("MergeExecutorEvent",
			zap.String("event", "mergingTaskCnt < 0"),
			zap.String("table", m.todo.table.GetNameDesc()),
		)
		m.mergingTaskCnt = 0
	}
}

func (m *todoSupporter) NextAfter() time.Time {
	return time.Now().Add(m.nextDue)
}

func (a *MergeScheduler) handleIOLoop() {
	stopCh := *a.stopCh.Load()
	for {
		select {
		case <-stopCh:
			return
		case msg := <-a.ioChan:

			stats, err := CalculateVacuumStats(context.Background(),
				msg.Table,
				msg.opts,
			)
			if err != nil {
				logutil.Warn("MergeExecutorEvent",
					zap.String("warn", "calculate vacuum stats"),
					zap.String("table", msg.Table.GetNameDesc()),
					zap.Error(err),
				)
				continue
			}

			compactTasks := GatherCompactTasks(context.Background(), stats)
			if len(compactTasks) > 0 {
				a.SendTrigger(
					NewMMsgTaskTrigger(msg.Table).
						WithAssignedTasks(compactTasks),
				)
			}

			if stats.DelVacuumPercent > 0.5 {
				oneshotOpts := DefaultTombstoneOpts.Clone().WithOneShot(true)
				a.SendTrigger(
					NewMMsgTaskTrigger(msg.Table).
						WithTombstone(oneshotOpts),
				)
			}

			logutil.Info("MergeExecutorEvent",
				zap.String("event", "vacuum check"),
				zap.String("table", msg.Table.GetNameDesc()),
				zap.String("tombstoneVacuum", fmt.Sprintf("%.2g", stats.DelVacuumPercent)),
				zap.String("dataVacuum", fmt.Sprintf("%.2g", stats.DataVacuumPercent)),
				zap.Duration("maxAge", stats.MaxCreateAgo.Round(time.Second)),
				zap.Int("compactThreshold", stats.DataVacuumScoreToCompact),
				zap.Int("compact", len(compactTasks)),
			)
		}
	}
}

func (a *MergeScheduler) handleMainLoop() {
	var nextReadyAtTimer = time.NewTimer(time.Hour * 24)
	never := make(<-chan time.Time)

	stopCh := *a.stopCh.Load()

	for {

		now := time.Now()
		nextReadyAt := never

		if !a.allPaused {
			// handle tasks in the priority queue
			for a.pq.Len() > 0 {
				if len(a.msgChan) > 50 {
					logutil.Info("MergeExecutorEvent",
						zap.String("event", "handle msg first"),
						zap.Int("msgLen", len(a.msgChan)),
					)
					// handle msg first, because it can change the priority queue
					break
				}
				if a.rc.availableMem() < 500*common.Const1MBytes {
					logutil.Info("MergeExecutorEvent",
						zap.String("event", "pause due to OOM alert"),
						zap.Int64("availableMem", a.rc.availableMem()),
					)
					// let's pause for a while to avoid OOM
					break
				}
				todo := a.pq.Peek()
				if todo.readyAt.After(now) {
					break
				}
				// DO NOT pop the task from the priority queue,
				// because the task may be updated
				a.doSched(todo)
			}

			// set the timer for the next task
			if a.pq.Len() > 0 {
				if nextReadyAtTimer != nil {
					nextReadyAtTimer.Stop()
				}
				next := a.pq.Peek().readyAt.Sub(now)
				if next > 0 {
					nextReadyAtTimer.Reset(next)
					nextReadyAt = nextReadyAtTimer.C
				}
			}
		}

		select {
		case <-stopCh:
			// stop the loop
			a.heartbeat.Stop()
			a.stopRecv <- struct{}{}
			return
		case <-nextReadyAt:
			// continue the loop
		case <-a.heartbeat.C:
			a.rc.printMemUsage()
			a.rc.refresh()
			// continue the loop
		case msg := <-a.msgChan:
			a.dispatchMsg(msg)
			drained := false
			for !drained {
				select {
				case msg := <-a.msgChan:
					a.dispatchMsg(msg)
				default:
					drained = true
				}
			}
		}
	}
}

// region: handle msg

func (a *MergeScheduler) dispatchMsg(msg *MMsg) {
	switch msg.Kind {
	case MMsgKindSwitch:
		a.handleSwitch(msg.Value.(MMsgSwitch))
	case MMsgKindQuery:
		a.handleQuery(msg.Value.(MMsgQuery))
	case MMsgKindTrigger:
		a.handleTaskTrigger(msg.Value.(*MMsgTaskTrigger))
	case MMsgKindTableChange:
		tableChange := msg.Value.(MMsgTableChange)
		if tableChange.Create {
			a.handleAddTable(tableChange.Table)
		} else if tableChange.ObjChange {
			a.handleObjectOps(tableChange.Table)
		} else if tableChange.DoneTask {
			a.handleMergeDone(tableChange.Table, tableChange.EstSize)
		}
	}
}

func (a *MergeScheduler) handleTaskTrigger(msg *MMsgTaskTrigger) {
	if msg.vacuum != nil {
		a.ioChan <- MMsgVacuumCheck{
			Table: msg.table,
			opts:  msg.vacuum,
		}
	}

	supp := a.supps[msg.table.ID()]
	if supp == nil {
		// TODO(aptend): Add table to the priority queue?
		return
	}

	if msg.IsEmptyTrigger() {
		// just go ahead with all default actions
		a.pq.Update(supp.todo, time.Now())
		return
	}

	if !msg.expire.IsZero() {
		// this is a policy patch
		if len(supp.triggers) == 0 {
			base := a.defaultTrigger.Clone().Merge(msg)
			supp.triggers = append(supp.triggers, base)
		} else if supp.triggers[0].expire.IsZero() {
			// have a disposable trigger to do, put the new trigger in front of it
			base := a.defaultTrigger.Clone().Merge(msg)
			supp.triggers = append([]*MMsgTaskTrigger{base}, supp.triggers...)
		} else {
			// there is patch already, merge it in place
			supp.triggers[0] = supp.triggers[0].Merge(msg)
		}
		logutil.Info("MergeExecutorEvent",
			zap.String("event", "patch trigger"),
			zap.String("table", msg.table.GetNameDesc()),
			zap.Any("trigger", supp.triggers[0]),
		)
	} else {
		supp.triggers = append(supp.triggers, msg)
	}

	// exec the task immediately
	a.pq.Update(supp.todo, time.Now())
}

func (a *MergeScheduler) handleSwitch(msg MMsgSwitch) {
	if msg.Table == nil {
		if msg.On {
			logutil.Info("MergeExecutorEvent", zap.String("event", "resume all"))
			a.allPaused = false
		} else {
			logutil.Info("MergeExecutorEvent", zap.String("event", "pause all"))
			a.allPaused = true
		}
	} else {
		supp := a.supps[msg.Table.ID()]
		if supp == nil {
			return
		}
		if msg.On && supp.paused {
			logutil.Info("MergeExecutorEvent",
				zap.String("event", "resume table"),
				zap.String("table", msg.Table.GetNameDesc()),
			)
			supp.paused = false
			a.pq.Update(supp.todo, time.Now().Add(time.Second*1))
		} else if !msg.On && !supp.paused {
			logutil.Info("MergeExecutorEvent",
				zap.String("event", "pause table"),
				zap.String("table", msg.Table.GetNameDesc()),
			)
			supp.paused = true
			a.pq.Update(supp.todo, time.Now().Add(time.Hour*24))
		}
	}
}

func (a *MergeScheduler) handleQuery(msg MMsgQuery) {
	answer := &QueryAnswer{
		GlobalAutoMergeOn: !a.allPaused,
		MsgQueueLen:       len(a.msgChan),
	}

	if msg.Table != nil {
		supp := a.supps[msg.Table.ID()]
		if supp != nil {
			answer.AutoMergeOn = !supp.paused
			answer.NextCheckDue = time.Until(supp.todo.readyAt)
			answer.DataMergeCnt = supp.totalDataMergeCnt
			answer.TombstoneMergeCnt = supp.totalTombstoneMergeCnt
			answer.PendingMergeCnt = supp.mergingTaskCnt
			answer.BigDataAcc = supp.bigTaskCnt
			if len(supp.triggers) > 0 {
				answer.Triggers = fmt.Sprintf("%v", supp.triggers)
			}
		} else {
			answer.NotExists = true
		}
	}
	msg.Answer <- answer
}

func (a *MergeScheduler) handleAddTable(table catalog.MergeTable) {
	todo := &todoItem{
		table:   table,
		readyAt: time.Now().Add(a.baseInterval),
	}
	a.supps[table.ID()] = &todoSupporter{
		todo:    todo,
		nextDue: a.baseInterval,
	}
	heap.Push(&a.pq, todo)
}

func (a *MergeScheduler) handleObjectOps(table catalog.MergeTable) {
	if supp := a.supps[table.ID()]; supp != nil {
		supp.objectOperations++
		if supp.objectOperations > objectOpsTriggerThreshold {
			supp.objectOperations = 0
			base := a.baseInterval
			// bring the table to the top of the priority queue
			nextEvent := time.Now().Add(base)
			if supp.nextDue > base || supp.todo.readyAt.After(nextEvent) {
				supp.nextDue = base
				a.pq.Update(supp.todo, nextEvent)
			}
		}
	}
}

func (a *MergeScheduler) handleMergeDone(table catalog.MergeTable, esz int) {
	if supp := a.supps[table.ID()]; supp != nil {
		supp.DoneTask()
	}
	a.rc.releaseResources(int64(esz))
}

// region: schedule

func (a *MergeScheduler) doSched(todo *todoItem) {
	// this table is dropped
	if todo.table.HasDropCommitted() {
		delete(a.supps, todo.table.ID())
		heap.Pop(&a.pq)
		logutil.Info("MergeExecutorEvent",
			zap.String("event", "remove table"),
			zap.String("table", todo.table.GetNameDesc()),
		)
		return
	}

	supp := a.supps[todo.table.ID()]

	now := time.Now()

	// this table is merging, postpone the task
	if supp.mergingTaskCnt > 0 {
		a.pq.Update(todo, now.Add(a.baseInterval/2))
		return
	}

	trigger := a.defaultTrigger
	trigger.table = todo.table

	// remove expired triggers
	for len(supp.triggers) > 0 {
		last := supp.triggers[len(supp.triggers)-1]
		// trigger for once
		if last.expire.IsZero() {
			supp.triggers = supp.triggers[:len(supp.triggers)-1]
			trigger = last
			break
		}
		if last.expire.Before(now) {
			supp.triggers = supp.triggers[:len(supp.triggers)-1]
		} else {
			trigger = last
			break
		}
	}

	// this table is paused, postpone tasks if it triggered by inner codes.
	// User triggerd disposal actions is allowed to run even if the table is paused.
	if supp.paused && !trigger.byUser {
		a.pq.Update(todo, now.Add(time.Hour*24))
		return
	}

	// Gather tasks

	tasks := a.pad.gatherByTrigger(
		context.Background(),
		trigger,
		supp.lastMergeTime,
		a.rc,
	)

	afterGather := time.Now()
	// Schedule tasks
	for _, task := range tasks {
		task.doneCB = a.taskObserverFactory(todo.table, task.eSize)
		if a.executor.ExecuteFor(todo.table, task) {
			a.rc.reserveResources(int64(task.eSize))
			if task.isTombstone {
				supp.totalTombstoneMergeCnt++
			} else {
				supp.totalDataMergeCnt++
			}
			supp.mergingTaskCnt++
			if !task.isTombstone && task.oSize > 200*common.Const1MBytes {
				supp.bigTaskCnt++
			}
			supp.lastMergeTime = afterGather
		}
	}

	// Postprocess tasks: issue new vacuum task or adjust the next due time
	if supp.bigTaskCnt >= bigDataTaskCntThreshold {
		a.ioChan <- MMsgVacuumCheck{
			Table: todo.table,
			opts:  DefaultVacuumOpts,
		}
		supp.bigTaskCnt = supp.bigTaskCnt % bigDataTaskCntThreshold
	}

	if len(tasks) == 0 {
		// This table has no task to do, lower the priority of it
		if !trigger.byUser {
			supp.nextDue = supp.nextDue * 2
		}
	} else {
		supp.nextDue = a.baseInterval
	}
	a.pq.Update(todo, afterGather.Add(supp.nextDue))

}

type launchPad struct {
	leveledObjects [MAX_LV_COUNT][]*objectio.ObjectStats
	tombstoneStats []*objectio.ObjectStats
	smallTombstone []*objectio.ObjectStats
	bigTombstone   []*objectio.ObjectStats

	table         catalog.MergeTable
	lastMergeTime time.Duration

	revisedResults []mergeTask
}

func newLaunchPad() *launchPad {
	p := &launchPad{
		leveledObjects: [MAX_LV_COUNT][]*objectio.ObjectStats{},
		tombstoneStats: make([]*objectio.ObjectStats, 0),
		smallTombstone: make([]*objectio.ObjectStats, 0),
		bigTombstone:   make([]*objectio.ObjectStats, 0),
		revisedResults: make([]mergeTask, 0),
	}
	for i := range p.leveledObjects {
		p.leveledObjects[i] = make([]*objectio.ObjectStats, 0)
	}
	return p
}

func (p *launchPad) Reset() {
	for i := range p.leveledObjects {
		p.leveledObjects[i] = p.leveledObjects[i][:0]
	}
	p.smallTombstone = p.smallTombstone[:0]
	p.bigTombstone = p.bigTombstone[:0]
	p.tombstoneStats = p.tombstoneStats[:0]
	p.revisedResults = p.revisedResults[:0]
	p.table = nil
}

var ReleaseDate int64 = 1747559040461945825 //  2025-05-18 17:04:00.461945825 +0800 CST

func (p *launchPad) InitWithTrigger(trigger *MMsgTaskTrigger, lastMergeTime time.Time) {
	p.table = trigger.table
	p.lastMergeTime = time.Since(lastMergeTime)
	if p.lastMergeTime > TenYears {
		// avoid busy merge when the system is just started
		p.lastMergeTime = 30 * time.Minute * time.Duration(rand.Intn(9)+1) / 10
	}

	checkCreateTime := trigger.table.IsSpecialBigTable() && !trigger.handleBigOld

	if trigger.l0 != nil || trigger.ln != nil {
		for item := range p.table.IterDataItem() {
			stat := item.GetObjectStats()
			lv := stat.GetLevel()
			if checkCreateTime &&
				item.GetCreatedAt().Physical() < ReleaseDate &&
				stat.OriginSize() > common.DefaultMinOsizeQualifiedBytes {
				continue
			}
			p.leveledObjects[lv] = append(p.leveledObjects[lv], stat)
		}
	}
	if trigger.tomb != nil {
		for item := range p.table.IterTombstoneItem() {
			stat := item.GetObjectStats()
			p.tombstoneStats = append(p.tombstoneStats, stat)
		}
	}
}

func (p *launchPad) gatherTombstoneTasks(ctx context.Context,
	tombstoneOpts *TombstoneOpts,
	rc *resourceController,
) {
	tasks := GatherTombstoneTasks(ctx, IterStats(p.tombstoneStats), tombstoneOpts, p.lastMergeTime)
	p.revisedResults = append(p.revisedResults, controlTaskMemInPlace(tasks, rc, 1)...)
}

func (p *launchPad) gatherLnTasks(ctx context.Context,
	lnOpts *OverlapOpts,
	startlv int,
	endlv int,
	rc *resourceController,
) {
	if startlv < 1 {
		startlv = 1
	}
	if endlv > MAX_LV {
		endlv = MAX_LV
	}
	for i := startlv; i <= endlv; i++ {
		if len(p.leveledObjects[i]) <= 2 {
			continue
		}
		overlapTasks, err := GatherOverlapMergeTasks(ctx, p.leveledObjects[i], lnOpts, int8(i))
		if err != nil {
			logutil.Warn("MergeExecutorEvent",
				zap.String("warn", "GatherOverlapMergeTasks failed"),
				zap.String("table", p.table.GetNameDesc()),
				zap.Error(err),
			)
		} else {
			p.revisedResults = append(p.revisedResults,
				controlTaskMemInPlace(overlapTasks, rc, 2)...)
		}
	}
}

func (p *launchPad) gatherL0Tasks(ctx context.Context,
	l0Opts *LayerZeroOpts,
	rc *resourceController,
) {
	l0Tasks := GatherLayerZeroMergeTasks(ctx, p.leveledObjects[0], p.lastMergeTime, l0Opts)
	p.revisedResults = append(p.revisedResults,
		controlTaskMemInPlace(l0Tasks, rc, 2)...)
}

func (p *launchPad) gatherByTrigger(ctx context.Context,
	trigger *MMsgTaskTrigger,
	lastMergeTime time.Time,
	rc *resourceController,
) []mergeTask {
	p.Reset()
	p.InitWithTrigger(trigger, lastMergeTime)
	if trigger.l0 != nil {
		p.gatherL0Tasks(ctx, trigger.l0, rc)
	}
	if trigger.ln != nil {
		p.gatherLnTasks(ctx, trigger.ln, trigger.startlv, trigger.endlv, rc)
	}
	if trigger.tomb != nil {
		p.gatherTombstoneTasks(ctx, trigger.tomb, rc)
	}
	if len(trigger.assigns) > 0 {
		p.revisedResults = append(p.revisedResults,
			controlTaskMemInPlace(trigger.assigns, rc, 1)...)
	}
	return p.revisedResults
}

func sumOsize(objs []*objectio.ObjectStats) int {
	sum := 0
	for _, obj := range objs {
		sum += int(obj.OriginSize())
	}
	return sum
}

func controlTaskMemInPlace(tasks []mergeTask, rc *resourceController, deleteLessThan int) []mergeTask {
	if len(tasks) == 0 {
		return tasks
	}
	for i := range tasks {
		task := &tasks[i]
		original := len(task.objs)
		estSize := mergesort.EstimateMergeSize(IterStats(task.objs))
		for ; !rc.resourceAvailable(int64(estSize)) && len(task.objs) > 1; estSize = mergesort.EstimateMergeSize(IterStats(task.objs)) {
			task.objs = task.objs[:len(task.objs)/2]
		}
		if original-len(task.objs) > 0 {
			task.note = task.note + fmt.Sprintf("(reduce %d)", original-len(task.objs))
		}

		task.oSize = sumOsize(task.objs)
		task.eSize = estSize
	}
	tasks = slices.DeleteFunc(tasks, func(task mergeTask) bool {
		return len(task.objs) < deleteLessThan
	})
	return tasks
}
