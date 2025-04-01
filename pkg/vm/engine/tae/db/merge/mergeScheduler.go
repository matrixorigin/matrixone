package merge

import (
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
	doneCB      *taskObserver
}

func (r *mergeTask) String() string {
	return fmt.Sprintf("mergeTask{isTombstone: %v, level: %d, note: %s, objs: %v}", r.isTombstone, r.level, r.note, len(r.objs))
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

	pad *launchPad

	rt       *dbutils.Runtime
	catalog  *catalog.Catalog
	rc       *resourceController
	executor *executor
}

func NewMergeScheduler(rt *dbutils.Runtime, cata *catalog.Catalog, cnSched *CNMergeScheduler) *MergeScheduler {
	sched := &MergeScheduler{
		rt:       rt,
		catalog:  cata,
		rc:       new(resourceController),
		executor: newMergeExecutor(rt, cnSched),

		supps:     make(map[uint64]*todoSupporter),
		heartbeat: time.NewTicker(time.Second * 10),

		stopCh:   make(chan struct{}),
		stopRecv: make(chan struct{}, 1),
		stopped:  atomic.Bool{},
		msgChan:  make(chan *MMsg, 4096),

		pad: newLaunchPad(),
	}

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
		go a.handleLoop()
		// go a.IoWorkerLoop()
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
	MMsgKindVacuum
	MMsgKindOverlap
	MMsgKindTableChange
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
	NextCheckDue    time.Time
	MergedCnt       int
	PendingMergeCnt int
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

type MMsgTaskTrigger struct {
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
	objectOperations int
	totalMergeCnt    int
	paused           bool
	nextDue          time.Duration
	todo             *todoItem
	observer         *taskObserver
	actions          []func()
}

func (m *todoSupporter) AddMergingTaskCnt(cnt int) {
	m.mergingTaskCnt += cnt
}

func (m *todoSupporter) DoneTask() {
	m.mergingTaskCnt--
	if m.mergingTaskCnt < 0 {
		logutil.Error("MergeExecutorEvent", zap.String("event", "mergingTaskCnt < 0"), zap.String("table", m.todo.table.GetNameDesc()))
		m.mergingTaskCnt = 0
	}
}

func (m *todoSupporter) AddObjectOperations(cnt int) {
	m.objectOperations += cnt
}

func (m *todoSupporter) NextAfter() time.Time {
	return time.Now().Add(m.nextDue)
}

func (a *MergeScheduler) handleLoop() {
	var nextReadyAtTimer = time.NewTimer(time.Hour * 24)
	never := make(<-chan time.Time)

	for {

		now := time.Now()
		nextReadyAt := never

		if !a.allPaused {
			// handle tasks in the priority queue
			for a.pq.Len() > 0 {
				if len(a.msgChan) > 50 {
					break // handle msg first, because it can change the priority queue
				}
				if a.rc.availableMem() < 500*common.Const1MBytes {
					break // let's pause for a while to avoid OOM
				}
				todo := a.pq.Peek()
				if todo.readyAt.After(now) {
					break
				}
				// DO NOT pop the task from the priority queue, because the task may be updated
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
			logutil.Info("MergeExecutorEvent", zap.String("event", "resume table"), zap.String("table", msg.Table.GetNameDesc()))
			supp.paused = false
			a.pq.Update(supp.todo, time.Now().Add(time.Second*1))
		} else if !msg.On && !supp.paused {
			logutil.Info("MergeExecutorEvent", zap.String("event", "pause table"), zap.String("table", msg.Table.GetNameDesc()))
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
			answer.NextCheckDue = supp.todo.readyAt
			answer.MergedCnt = supp.totalMergeCnt
			answer.PendingMergeCnt = supp.mergingTaskCnt
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
			dur := a.rt.Options.CheckpointCfg.ScanInterval
			// bring the table to the top of the priority queue
			if nextEvent := time.Now().Add(dur); supp.nextDue > dur || supp.todo.readyAt.After(nextEvent) {
				supp.nextDue = dur
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

	// this table is paused, postpone the task
	if supp.paused {
		a.pq.Update(todo, time.Now().Add(time.Hour*24))
		return
	}

	// this table is merging, postpone the task
	if supp.mergingTaskCnt > 0 {
		dur := a.rt.Options.CheckpointCfg.ScanInterval
		a.pq.Update(todo, time.Now().Add(dur/2))
		return
	}

	// Gather tasks
	tasks := a.pad.GatherTasks(todo.table, a.rc, DefaultLayerZeroOpts, DefaultOverlapOpts, DefaultVacuumOpts, DefaultTombstoneOpts)

	// Schedule tasks

	for _, task := range tasks {
		task.doneCB = supp.observer
		if a.executor.executeFor(todo.table, task) {
			supp.totalMergeCnt++
			supp.mergingTaskCnt++
		}
	}

	// This table has no task to do, lower the priority of it
	if len(tasks) == 0 {
		supp.nextDue = supp.nextDue * 2
	} else {
		// reset the next due time
		supp.nextDue = a.rt.Options.CheckpointCfg.ScanInterval
	}
	a.pq.Update(todo, time.Now().Add(supp.nextDue))
	logutil.Info("yyyy", zap.String("event", "schedule check done"), zap.String("table", todo.table.GetNameDesc()), zap.Duration("nextDue", supp.nextDue), zap.Int("tasks", len(tasks)))
}

type launchPad struct {
	leveledObjects [8][]*objectio.ObjectStats
	tombstoneStats []*objectio.ObjectStats
	smallTombstone []*objectio.ObjectStats
	bigTombstone   []*objectio.ObjectStats

	table *catalog.TableEntry

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

func controlTaskMemInPlace(tasks []mergeTask, rc *resourceController, deleteLessThan int) []mergeTask {
	for _, task := range tasks {
		original := len(task.objs)
		estSize := mergesort.EstimateMergeSize(IterStats(task.objs))
		for ; !rc.resourceAvailable(int64(estSize)) && len(task.objs) > 1; estSize = mergesort.EstimateMergeSize(IterStats(task.objs)) {
			task.objs = task.objs[:len(task.objs)/2]
		}
		if original-len(task.objs) > 0 {
			task.note = task.note + fmt.Sprintf("(reduce %d)", original-len(task.objs))
		}
	}
	slices.DeleteFunc(tasks, func(task mergeTask) bool {
		return len(task.objs) < deleteLessThan
	})
	return tasks
}

func (p *launchPad) AddDataObjectStats(stat *objectio.ObjectStats) {
	p.leveledObjects[stat.GetLevel()] = append(p.leveledObjects[stat.GetLevel()], stat)
}

func (p *launchPad) AddTombstoneObjectStats(stat *objectio.ObjectStats) {
	p.tombstoneStats = append(p.tombstoneStats, stat)
}

func (p *launchPad) InitWithTable(table *catalog.TableEntry) {
	p.table = table
	readTxn := txnbase.MockTxnReaderWithNow()
	dataIt := table.MakeDataVisibleObjectIt(readTxn)
	for dataIt.Next() {
		if !ObjectValid(dataIt.Item()) {
			continue
		}
		stat := dataIt.Item().GetObjectStats()
		p.leveledObjects[stat.GetLevel()] = append(p.leveledObjects[stat.GetLevel()], stat)
	}
	tombstoneIt := table.MakeTombstoneVisibleObjectIt(readTxn)
	for tombstoneIt.Next() {
		if !ObjectValid(tombstoneIt.Item()) {
			continue
		}
		stat := tombstoneIt.Item().GetObjectStats()
		p.tombstoneStats = append(p.tombstoneStats, stat)
	}
}

func (p *launchPad) GatherTombstoneTasks(tombstoneOpts *TombstoneOpts, rc *resourceController) {
	tasks := GatherTombstoneTasks(context.Background(), IterStats(p.tombstoneStats), tombstoneOpts)
	p.revisedResults = append(p.revisedResults, tasks...)
}

func (p *launchPad) GatherDataTasks(l0Opts *LayerZeroOpts, lnOpts *OverlapOpts, rc *resourceController) {
	// if rc.cpuPercent > 80 {
	// 	return
	// }
	ctx := context.Background()

	lastMergeTime := p.table.Stats.GetLastMergeTime()
	mergeAgo := time.Since(lastMergeTime)

	layerZeroTasks, err := GatherLayerZeroMergeTasks(ctx, p.leveledObjects[0], mergeAgo, l0Opts)
	if err != nil {
		logutil.Warn("MergeExecutorEvent", zap.String("event", "GatherLayerZeroMergeTasks failed"), zap.String("table", p.table.GetNameDesc()), zap.Error(err))
	} else {
		p.revisedResults = append(p.revisedResults, controlTaskMemInPlace(layerZeroTasks, rc, 2)...)
	}

	for i := 1; i < 7; i++ {
		if len(p.leveledObjects[i]) <= 2 {
			continue
		}
		overlapTasks, err := GatherOverlapMergeTasks(ctx, p.leveledObjects[i], lnOpts, int8(i))
		if err != nil {
			logutil.Warn("MergeExecutorEvent", zap.String("event", "GatherOverlapMergeTasks failed"), zap.String("table", p.table.GetNameDesc()), zap.Error(err))
		} else {
			p.revisedResults = append(p.revisedResults, controlTaskMemInPlace(overlapTasks, rc, 2)...)
		}
	}
}

func (p *launchPad) GatherTasks(
	table *catalog.TableEntry,
	rc *resourceController,
	l0Opts *LayerZeroOpts,
	lnOpts *OverlapOpts,
	vacuumOpts *VacuumOpts,
	tombstoneOpts *TombstoneOpts,
) []mergeTask {
	p.Reset()
	p.InitWithTable(table)
	p.GatherTombstoneTasks(tombstoneOpts, rc)
	p.GatherDataTasks(l0Opts, lnOpts, rc)
	return p.revisedResults
}
