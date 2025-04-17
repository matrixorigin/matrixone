package merge

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"go.uber.org/zap"
)

type mergeTask struct {
	objs        []*objectio.ObjectStats
	kind        taskHostKind
	isTombstone bool
	level       int8
	note        string
	oSize       int
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
	stopCh    chan struct{}
	stopRecv  chan struct{}
	stopped   atomic.Bool

	msgChan chan *MMsg
	ioChan  chan MMsgVacuumCheck

	pad *launchPad

	rt       *dbutils.Runtime
	catalog  *catalog.Catalog
	rc       *resourceController
	executor *executor
}

func NewMergeScheduler(
	rt *dbutils.Runtime,
	cata *catalog.Catalog,
	cnSched *CNMergeScheduler,
) *MergeScheduler {
	sched := &MergeScheduler{
		rt:       rt,
		catalog:  cata,
		rc:       new(resourceController),
		executor: newMergeExecutor(rt, cnSched),

		supps:     make(map[uint64]*todoSupporter),
		heartbeat: time.NewTicker(time.Second * 10),

		stopCh:   make(chan struct{}),
		stopRecv: make(chan struct{}, 1),
		msgChan:  make(chan *MMsg, 4096),
		ioChan:   make(chan MMsgVacuumCheck, 1024),

		pad: newLaunchPad(),
	}

	sched.stopped.Store(true)
	cata.SetMergeNotifier(sched)
	// init priority queue
	p := new(catalog.LoopProcessor)
	p.TableFn = func(table *catalog.TableEntry) error {
		if table.IsActive() {
			sched.handleAddTable(table)
		}
		return moerr.GetOkStopCurrRecur()
	}
	cata.RecurLoop(p)

	sched.rc.refresh()
	return sched

}

func (a *MergeScheduler) Stop() {
	if a.stopped.CompareAndSwap(false, true) {
		close(a.stopCh)
		<-a.stopRecv
	}
}

func (a *MergeScheduler) Start() {
	if a.stopped.CompareAndSwap(true, false) {
		a.heartbeat.Reset(time.Second * 10)
		go a.handleMainLoop()
		go a.handleIOLoop()
	}
}

// region: on events

func (a *MergeScheduler) OnCreateTableCommit(table *catalog.TableEntry) {
	a.msgChan <- &MMsg{
		Kind: MMsgKindTableChange,
		Value: MMsgTableChange{
			Table:  table,
			Create: true,
		},
	}
}

func (a *MergeScheduler) OnCreateNonAppendObject(table *catalog.TableEntry) {
	a.msgChan <- &MMsg{
		Kind: MMsgKindTableChange,
		Value: MMsgTableChange{
			Table:     table,
			ObjChange: true,
		},
	}
}

func (a *MergeScheduler) OnMergeDone(table *catalog.TableEntry) {
	a.msgChan <- &MMsg{
		Kind: MMsgKindTableChange,
		Value: MMsgTableChange{
			Table:    table,
			DoneTask: true,
		},
	}
}

type taskObserver struct {
	f func()
}

func (o *taskObserver) OnExecDone(_ any) {
	o.f()
}

func (a *MergeScheduler) makeTaskObserver(table *catalog.TableEntry) *taskObserver {
	return &taskObserver{f: func() { a.OnMergeDone(table) }}
}

// legacy interface for big tombstone write
func (a *MergeScheduler) StopMerge(tblEntry *catalog.TableEntry, reentrant bool) error {
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

	tblName := tblEntry.GetLastestSchema(false).Name
	return a.rt.LockMergeService.LockFromUser(tblEntry.GetID(), tblName, reentrant, indexTableNames...)
}

func (a *MergeScheduler) StartMerge(tblID uint64, reentrant bool) error {
	return a.rt.LockMergeService.UnlockFromUser(tblID, reentrant)
}

func (a *MergeScheduler) CNActiveObjectsString() string {
	return a.executor.cnSched.activeObjsString()
}

func (a *MergeScheduler) RemoveCNActiveObjects(ids []objectio.ObjectId) {
	a.executor.cnSched.removeActiveObject(ids)
}

func (a *MergeScheduler) PruneCNActiveObjects(id uint64, ago time.Duration) {
	a.executor.cnSched.prune(id, ago)
}

// region: msg def & sender

type MMsgKind int

const (
	MMsgKindSwitch MMsgKind = iota
	MMsgKindQuery
	MMsgKindTableChange
	MMsgKindTrigger
)

type MMsgSwitch struct {
	Table *catalog.TableEntry
	On    bool
}

type QueryAnswer struct {
	// global status
	GlobalAutoMergeOn bool
	MsgQueueLen       int

	// table status
	AutoMergeOn     bool
	NextCheckDue    time.Duration
	MergedCnt       int
	PendingMergeCnt int
	BigDataAcc      int
	Triggers        string
}

type MMsgQuery struct {
	Table  *catalog.TableEntry
	Answer chan *QueryAnswer
}

type MMsgTableChange struct {
	Table     *catalog.TableEntry
	Create    bool
	ObjChange bool
	DoneTask  bool
}

type MMsgVacuumCheck struct {
	Table *catalog.TableEntry
	opts  *VacuumOpts
}

var DefaultTrigger = &MMsgTaskTrigger{
	l0:      DefaultLayerZeroOpts,
	startlv: 1,
	endlv:   6,
	ln:      DefaultOverlapOpts,
	tomb:    DefaultTombstoneOpts,
	vacuum:  DefaultVacuumOpts,
}

type MMsgTaskTrigger struct {
	expire time.Time
	byUser bool

	table *catalog.TableEntry

	// l0
	l0 *LayerZeroOpts

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

func (b *MMsgTaskTrigger) String() string {
	var buf bytes.Buffer
	if b.l0 != nil {
		buf.WriteString(b.l0.String())
	}
	if b.ln != nil {
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(b.ln.String())
		buf.WriteString(fmt.Sprintf("(%d->%d)", b.startlv, b.endlv))
	}
	if b.tomb != nil {
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(b.tomb.String())
	}
	if b.vacuum != nil {
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(b.vacuum.String())
	}
	if len(b.assigns) > 0 {
		if buf.Len() > 0 {
			buf.WriteString(" ")
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

func NewMMsgTaskTrigger(table *catalog.TableEntry) *MMsgTaskTrigger {
	return &MMsgTaskTrigger{table: table, startlv: -1, endlv: -1}
}

func (b *MMsgTaskTrigger) WithL0(opts *LayerZeroOpts) *MMsgTaskTrigger {
	b.l0 = opts
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
	if endlv > 7 {
		endlv = 7
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

type MMsg struct {
	Kind  MMsgKind
	Value any
}

type todoItem struct {
	index   int
	readyAt time.Time
	table   *catalog.TableEntry
}

func (a *MergeScheduler) Query(table *catalog.TableEntry) *QueryAnswer {
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

func (a *MergeScheduler) PauseTable(table *catalog.TableEntry) {
	a.msgChan <- &MMsg{
		Kind: MMsgKindSwitch,
		Value: MMsgSwitch{
			On:    false,
			Table: table,
		},
	}
}

func (a *MergeScheduler) ResumeTable(table *catalog.TableEntry) {
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
	mergingTaskCnt   int
	bigTaskCnt       int
	objectOperations int
	totalMergeCnt    int
	paused           bool
	nextDue          time.Duration
	todo             *todoItem
	observer         *taskObserver
	triggers         []*MMsgTaskTrigger
}

func (m *todoSupporter) AddMergingTaskCnt(cnt int) {
	m.mergingTaskCnt += cnt
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

func (m *todoSupporter) AddObjectOperations(cnt int) {
	m.objectOperations += cnt
}

func (m *todoSupporter) NextAfter() time.Time {
	return time.Now().Add(m.nextDue)
}

func (a *MergeScheduler) handleIOLoop() {
	for {
		select {
		case <-a.stopCh:
			return
		case msg := <-a.ioChan:

			stats, err := CalculateVacuumStats(context.Background(), msg.Table, msg.opts)
			if err != nil {
				logutil.Warn("MergeExecutorEvent",
					zap.String("warn", "calculate vacuum stats"),
					zap.String("table", msg.Table.GetNameDesc()),
					zap.Error(err),
				)
				continue
			}
			logutil.Info("MergeExecutorEvent",
				zap.String("event", "vacuum check"),
				zap.String("table", msg.Table.GetNameDesc()),
				zap.Float64("tombstoneVacuum", stats.DelVacuumPercent),
				zap.Float64("dataVacuum", stats.DataVacuumPercent),
				zap.Duration("maxAge", stats.MaxCreateAgo),
			)
			compactTasks := GatherCompactTasks(context.Background(), stats, a.rc)
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
		}
	}
}

func (a *MergeScheduler) handleMainLoop() {
	var nextReadyAtTimer = time.NewTimer(time.Hour * 24)
	never := make(<-chan time.Time)

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
		case <-a.stopCh:
			// stop the loop
			a.heartbeat.Stop()
			a.stopRecv <- struct{}{}
			return
		case <-nextReadyAt:
			// continue the loop
		case <-a.heartbeat.C:
			a.rc.printStats()
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
			a.handleMergeDone(tableChange.Table)
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

	supp := a.supps[msg.table.ID]
	if supp == nil {
		// TODO(aptend): Add table to the priority queue?
		return
	}

	supp.triggers = append(supp.triggers, msg)
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
		supp := a.supps[msg.Table.ID]
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
		supp := a.supps[msg.Table.ID]
		if supp != nil {
			answer.AutoMergeOn = !supp.paused
			answer.NextCheckDue = supp.todo.readyAt.Sub(time.Now())
			answer.MergedCnt = supp.totalMergeCnt
			answer.PendingMergeCnt = supp.mergingTaskCnt
			answer.BigDataAcc = supp.bigTaskCnt
			if len(supp.triggers) > 0 {
				answer.Triggers = fmt.Sprintf("%v", supp.triggers)
			}
		}
	}
	msg.Answer <- answer
}

func (a *MergeScheduler) handleAddTable(table *catalog.TableEntry) {
	todo := &todoItem{
		table:   table,
		readyAt: time.Now().Add(a.rt.Options.CheckpointCfg.ScanInterval),
	}
	a.supps[table.ID] = &todoSupporter{
		todo:     todo,
		nextDue:  a.rt.Options.CheckpointCfg.ScanInterval,
		observer: a.makeTaskObserver(table),
	}
	heap.Push(&a.pq, todo)
}

func (a *MergeScheduler) handleObjectOps(table *catalog.TableEntry) {
	if supp := a.supps[table.ID]; supp != nil {
		supp.objectOperations++
		if supp.objectOperations > 5 {
			supp.objectOperations = 0
			base := a.rt.Options.CheckpointCfg.ScanInterval
			// bring the table to the top of the priority queue
			nextEvent := time.Now().Add(base)
			if supp.nextDue > base || supp.todo.readyAt.After(nextEvent) {
				supp.nextDue = base
				a.pq.Update(supp.todo, nextEvent)
			}
		}
	}
}

func (a *MergeScheduler) handleMergeDone(table *catalog.TableEntry) {
	if supp := a.supps[table.ID]; supp != nil {
		supp.DoneTask()
	}
}

// region: schedule

func (a *MergeScheduler) doSched(todo *todoItem) {
	// this table is dropped
	if todo.table.HasDropCommitted() {
		delete(a.supps, todo.table.ID)
		a.pq.Pop()
		return
	}

	supp := a.supps[todo.table.ID]

	// this table is merging, postpone the task
	if supp.mergingTaskCnt > 0 {
		dur := a.rt.Options.CheckpointCfg.ScanInterval
		a.pq.Update(todo, time.Now().Add(dur/2))
		return
	}

	trigger := DefaultTrigger
	trigger.table = todo.table

	// mannual actions take higher priority than auto merge
	if len(supp.triggers) > 0 {
		trigger = supp.triggers[len(supp.triggers)-1]
	}

	// this table is paused, postpone tasks if it triggered by inner codes
	if supp.paused && !trigger.byUser {
		a.pq.Update(todo, time.Now().Add(time.Hour*24))
		return
	}

	// remove expired triggers
	if len(supp.triggers) > 0 {
		if trigger.expire.Before(time.Now()) {
			supp.triggers = supp.triggers[:len(supp.triggers)-1]
		}
	}

	// Gather tasks
	tasks := a.pad.gatherByTrigger(context.Background(), trigger, a.rc)

	// Schedule tasks
	for _, task := range tasks {
		task.doneCB = supp.observer
		if a.executor.executeFor(todo.table, task) {
			supp.totalMergeCnt++
			supp.mergingTaskCnt++
			if !task.isTombstone && task.oSize > 200*common.Const1MBytes {
				supp.bigTaskCnt++
			}
		}
	}

	// Postprocess tasks: issue new vacuum task or adjust the next due time
	if supp.bigTaskCnt > 3 {
		a.ioChan <- MMsgVacuumCheck{
			Table: todo.table,
			opts:  DefaultVacuumOpts,
		}
		supp.bigTaskCnt = 0
	}

	if len(tasks) == 0 {
		// This table has no task to do, lower the priority of it
		if !trigger.byUser {
			supp.nextDue = supp.nextDue * 2
		}
	} else {
		supp.nextDue = a.rt.Options.CheckpointCfg.ScanInterval
	}
	a.pq.Update(todo, time.Now().Add(supp.nextDue))
}

type launchPad struct {
	leveledObjects [8][]*objectio.ObjectStats
	tombstoneStats []*objectio.ObjectStats
	smallTombstone []*objectio.ObjectStats
	bigTombstone   []*objectio.ObjectStats

	table         *catalog.TableEntry
	lastMergeTime time.Duration

	revisedResults []mergeTask
}

func newLaunchPad() *launchPad {
	p := &launchPad{
		leveledObjects: [8][]*objectio.ObjectStats{},
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

func (p *launchPad) AddDataObjectStats(stat *objectio.ObjectStats) {
	lv := stat.GetLevel()
	p.leveledObjects[lv] = append(p.leveledObjects[lv], stat)
}

func (p *launchPad) AddTombstoneObjectStats(stat *objectio.ObjectStats) {
	p.tombstoneStats = append(p.tombstoneStats, stat)
}

func (p *launchPad) InitWithTable(trigger *MMsgTaskTrigger) {
	p.table = trigger.table
	lastMergeTime := p.table.Stats.GetLastMergeTime()
	p.lastMergeTime = time.Since(lastMergeTime)
	readTxn := txnbase.MockTxnReaderWithNow()
	if trigger.l0 != nil || trigger.ln != nil {
		dataIt := p.table.MakeDataVisibleObjectIt(readTxn)
		for dataIt.Next() {
			item := dataIt.Item()
			if !ObjectValid(item) {
				continue
			}
			stat := item.GetObjectStats()
			p.leveledObjects[stat.GetLevel()] = append(p.leveledObjects[stat.GetLevel()], stat)
		}
	}
	if trigger.tomb != nil {
		tombstoneIt := p.table.MakeTombstoneVisibleObjectIt(readTxn)
		for tombstoneIt.Next() {
			item := tombstoneIt.Item()
			if !ObjectValid(item) {
				continue
			}
			stat := item.GetObjectStats()
			p.tombstoneStats = append(p.tombstoneStats, stat)
		}
	}
}

func (p *launchPad) gatherTombstoneTasks(ctx context.Context,
	tombstoneOpts *TombstoneOpts,
	rc *resourceController,
) {
	tasks := GatherTombstoneTasks(ctx, IterStats(p.tombstoneStats), tombstoneOpts)
	p.revisedResults = append(p.revisedResults, tasks...)
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
	if endlv > 6 {
		endlv = 6
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
	l0Tasks, err := GatherLayerZeroMergeTasks(ctx, p.leveledObjects[0], p.lastMergeTime, l0Opts)
	if err != nil {
		logutil.Warn("MergeExecutorEvent",
			zap.String("warn", "GatherLayerZeroMergeTasks failed"),
			zap.String("table", p.table.GetNameDesc()),
			zap.Error(err),
		)
	} else {
		p.revisedResults = append(p.revisedResults,
			controlTaskMemInPlace(l0Tasks, rc, 2)...)
	}
}

func (p *launchPad) gatherByTrigger(ctx context.Context,
	trigger *MMsgTaskTrigger,
	rc *resourceController,
) []mergeTask {
	p.Reset()
	p.InitWithTable(trigger)
	if trigger.l0 != nil {
		p.gatherL0Tasks(ctx, trigger.l0, rc)
	}
	if trigger.ln != nil {
		p.gatherLnTasks(ctx, trigger.ln, trigger.startlv, trigger.endlv, rc)
	}
	if trigger.tomb != nil {
		p.gatherTombstoneTasks(ctx, trigger.tomb, rc)
	}
	if trigger.assigns != nil {
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
		rc.reserveResources(int64(estSize))
		task.oSize = sumOsize(task.objs)
	}
	slices.DeleteFunc(tasks, func(task mergeTask) bool {
		return len(task.objs) < deleteLessThan
	})
	return tasks
}
