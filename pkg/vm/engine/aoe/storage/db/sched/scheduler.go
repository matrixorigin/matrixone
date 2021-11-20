// Copyright 2021 Matrix Origin
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

package sched

import (
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	tif "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/sched"
)

type CommandType = sched.CommandType

const (
	TurnOffFlushSegmentCmd = iota + sched.CustomizedCmd
	TurnOnFlushSegmentCmd
)

type metablkCommiter struct {
	sync.RWMutex
	opts       *storage.Options
	scheduler  sched.Scheduler
	pendings   []uint64
	flushdones map[uint64]*metadata.Block
}

func newMetaBlkCommiter(opts *storage.Options, scheduler sched.Scheduler) *metablkCommiter {
	c := &metablkCommiter{
		opts:       opts,
		scheduler:  scheduler,
		pendings:   make([]uint64, 0),
		flushdones: make(map[uint64]*metadata.Block),
	}
	return c
}

func (p *metablkCommiter) IsEmpty() bool {
	p.RLock()
	defer p.RUnlock()
	return len(p.pendings) == 0
}

func (p *metablkCommiter) Register(blkid uint64) {
	p.Lock()
	p.pendings = append(p.pendings, blkid)
	p.Unlock()
}

func (p *metablkCommiter) doSchedule(meta *metadata.Block) {
	ctx := &Context{Opts: p.opts}
	commit := NewCommitBlkEvent(ctx, meta)
	p.scheduler.Schedule(commit)
}

func (p *metablkCommiter) Accept(meta *metadata.Block) {
	// TODO: retry logic
	// if err := e.GetError(); err != nil {
	// 	panic(err)
	// }
	if meta == nil {
		return
	}
	p.Lock()
	if p.pendings[0] != meta.Id {
		p.flushdones[meta.Id] = meta
	} else {
		p.doSchedule(meta)
		var i int
		for i = 1; i < len(p.pendings); i++ {
			meta, ok := p.flushdones[p.pendings[i]]
			if !ok {
				break
			}
			delete(p.flushdones, p.pendings[i])
			p.doSchedule(meta)
		}
		p.pendings = p.pendings[i:]
	}
	p.Unlock()
}

type controller struct {
	cmu  sync.RWMutex
	mask *roaring64.Bitmap
}

func newController() *controller {
	return &controller{
		mask: roaring64.NewBitmap(),
	}
}

func (c *controller) turnOnFlushSegment() {
	c.cmu.Lock()
	defer c.cmu.Unlock()
	c.mask.Remove(1)
}

func (c *controller) turnOffFlushSegment() {
	c.cmu.Lock()
	defer c.cmu.Unlock()
	c.mask.Add(1)
}

func (c *controller) canFlushSegment() bool {
	c.cmu.RLock()
	defer c.cmu.RLock()
	return !c.mask.Contains(1)
}

// scheduler is the global event scheduler for AOE. It wraps the
// BaseScheduler with some DB metadata and block committers.
//
// This directory mainly focus on different events scheduled under
// DB space. Basically you can refer to code under sched/ for more
// implementation details for scheduler itself.
type scheduler struct {
	*controller
	sched.BaseScheduler
	opts      *storage.Options
	tables    *table.Tables
	commiters struct {
		mu     sync.RWMutex
		blkmap map[uint64]*metablkCommiter
	}
}

func NewScheduler(opts *storage.Options, tables *table.Tables) *scheduler {
	s := &scheduler{
		BaseScheduler: *sched.NewBaseScheduler("scheduler"),
		opts:          opts,
		tables:        tables,
		controller:    newController(),
	}
	s.commiters.blkmap = make(map[uint64]*metablkCommiter)

	// Start different type of handlers
	dispatcher := sched.NewBaseDispatcher()
	flushtblkHandler := sched.NewPoolHandler(4, nil)
	flushtblkHandler.Start()
	flushblkHandler := sched.NewPoolHandler(int(opts.SchedulerCfg.BlockWriters), nil)
	flushblkHandler.Start()
	flushsegHandler := sched.NewPoolHandler(int(opts.SchedulerCfg.SegmentWriters), nil)
	flushsegHandler.Start()
	metaHandler := sched.NewSingleWorkerHandler("metaHandler")
	metaHandler.Start()
	memdataHandler := sched.NewSingleWorkerHandler("memdataHandler")
	memdataHandler.Start()
	statelessHandler := sched.NewPoolHandler(int(opts.SchedulerCfg.StatelessWorkers), nil)
	statelessHandler.Start()

	// Register different events to its belonged handler
	dispatcher.RegisterHandler(sched.StatelessEvent, statelessHandler)
	dispatcher.RegisterHandler(sched.FlushSegTask, flushsegHandler)
	dispatcher.RegisterHandler(sched.FlushBlkTask, flushblkHandler)
	dispatcher.RegisterHandler(sched.CommitBlkTask, metaHandler)
	dispatcher.RegisterHandler(sched.UpgradeBlkTask, memdataHandler)
	dispatcher.RegisterHandler(sched.UpgradeSegTask, memdataHandler)
	dispatcher.RegisterHandler(sched.MetaCreateTableTask, metaHandler)
	dispatcher.RegisterHandler(sched.MetaDropTableTask, metaHandler)
	dispatcher.RegisterHandler(sched.MetaCreateBlkTask, metaHandler)
	dispatcher.RegisterHandler(sched.MemdataUpdateEvent, memdataHandler)
	dispatcher.RegisterHandler(sched.FlushTableMetaTask, metaHandler)
	dispatcher.RegisterHandler(sched.PrecommitBlkMetaTask, metaHandler)
	dispatcher.RegisterHandler(sched.FlushTBlkTask, flushtblkHandler)

	// Register dispatcher
	s.RegisterDispatcher(sched.StatelessEvent, dispatcher)
	s.RegisterDispatcher(sched.FlushSegTask, dispatcher)
	s.RegisterDispatcher(sched.FlushBlkTask, dispatcher)
	s.RegisterDispatcher(sched.CommitBlkTask, dispatcher)
	s.RegisterDispatcher(sched.UpgradeBlkTask, dispatcher)
	s.RegisterDispatcher(sched.UpgradeSegTask, dispatcher)
	s.RegisterDispatcher(sched.MetaCreateTableTask, dispatcher)
	s.RegisterDispatcher(sched.MetaDropTableTask, dispatcher)
	s.RegisterDispatcher(sched.MetaCreateBlkTask, dispatcher)
	s.RegisterDispatcher(sched.MemdataUpdateEvent, dispatcher)
	s.RegisterDispatcher(sched.FlushTableMetaTask, dispatcher)
	s.RegisterDispatcher(sched.PrecommitBlkMetaTask, dispatcher)
	s.RegisterDispatcher(sched.FlushTBlkTask, dispatcher)
	s.Start()
	return s
}

// onPreCommitBlkDone gets the block committer for the given table, and
// register the given preCommit event for the committer.
func (s *scheduler) onPrecommitBlkDone(e sched.Event) {
	event := e.(*precommitBlockEvent)
	s.commiters.mu.Lock()
	commiter, ok := s.commiters.blkmap[event.Id.TableID]
	if !ok {
		commiter = newMetaBlkCommiter(s.opts, s)
		s.commiters.blkmap[event.Id.TableID] = commiter
	}
	commiter.Register(event.Id.BlockID)
	s.commiters.mu.Unlock()
}

func (s *scheduler) onFlushBlkDone(e sched.Event) {
	event := e.(*flushMemblockEvent)
	s.commiters.mu.RLock()
	commiter := s.commiters.blkmap[event.Meta.Segment.Table.Id]
	s.commiters.mu.RUnlock()
	commiter.Accept(event.Meta)
	s.commiters.mu.Lock()
	if commiter.IsEmpty() {
		delete(s.commiters.blkmap, event.Meta.Segment.Table.Id)
	}
	s.commiters.mu.Unlock()
}

// onCommitBlkDone handles the finished commit block event, schedules a
// new flush table event and an upgrade block event.
func (s *scheduler) onCommitBlkDone(e sched.Event) {
	if err := e.GetError(); err != nil {
		s.opts.EventListener.BackgroundErrorCB(err)
		return
	}
	event := e.(*commitBlkEvent)
	if !event.Ctx.HasDataScope() {
		return
	}
	newMeta := event.Meta
	mctx := &Context{Opts: s.opts}
	tableData, err := s.tables.StrongRefTable(newMeta.Segment.Table.Id)
	if err != nil {
		s.opts.EventListener.BackgroundErrorCB(err)
		return
	}
	logutil.Infof(" %s | Block %d | UpgradeBlkEvent | Started", sched.EventPrefix, newMeta.Id)
	newevent := NewUpgradeBlkEvent(mctx, newMeta, tableData)
	s.Schedule(newevent)
}

// onUpgradeBlkDone handles the finished upgrade block event, and if segment
// was closed, start a new flush segment event and schedule it.
func (s *scheduler) onUpgradeBlkDone(e sched.Event) {
	event := e.(*upgradeBlkEvent)
	defer event.TableData.Unref()
	if err := e.GetError(); err != nil {
		s.opts.EventListener.BackgroundErrorCB(err)
		return
	}
	if !event.Ctx.HasDataScope() {
		return
	}
	defer event.Data.Unref()
	if !event.SegmentClosed {
		return
	}
	if !s.canFlushSegment() {
		logutil.Warn("[Scheduler] Flush Segment Is Turned-Off")
		return
	}
	segment := event.TableData.StrongRefSegment(event.Meta.Segment.Id)
	if segment == nil {
		logutil.Warnf("Probably table %d is dropped", event.Meta.Segment.Table.Id)
		return
	}
	logutil.Infof(" %s | Segment %d | FlushSegEvent | Started", sched.EventPrefix, event.Meta.Segment.Id)
	flushCtx := &Context{Opts: s.opts}
	flushEvent := NewFlushSegEvent(flushCtx, segment)
	s.Schedule(flushEvent)
}

// onFlushSegDone handles the finished flush segment event, generates a new
// upgrade segment event and schedules it.
func (s *scheduler) onFlushSegDone(e sched.Event) {
	event := e.(*flushSegEvent)
	if err := e.GetError(); err != nil {
		// s.opts.EventListener.BackgroundErrorCB(err)
		event.Segment.Unref()
		return
	}
	ctx := &Context{Opts: s.opts}
	meta := event.Segment.GetMeta()
	td, err := s.tables.StrongRefTable(meta.Table.Id)
	if err != nil {
		// s.opts.EventListener.BackgroundErrorCB(err)
		event.Segment.Unref()
		return
	}
	logutil.Infof(" %s | Segment %d | UpgradeSegEvent | Started", sched.EventPrefix, meta.Id)
	newevent := NewUpgradeSegEvent(ctx, event.Segment, td)
	s.Schedule(newevent)
}

// onUpgradeSegDone handles the finished upgrade segment event and releases the
// occupied resources.
func (s *scheduler) onUpgradeSegDone(e sched.Event) {
	event := e.(*upgradeSegEvent)
	defer event.TableData.Unref()
	defer event.OldSegment.Unref()
	if err := e.GetError(); err != nil {
		s.opts.EventListener.BackgroundErrorCB(err)
		return
	}
	event.Segment.Unref()
}

func (s *scheduler) OnExecDone(op interface{}) {
	e := op.(sched.Event)
	switch e.Type() {
	case sched.FlushBlkTask:
		s.onFlushBlkDone(e)
	case sched.CommitBlkTask:
		s.onCommitBlkDone(e)
	case sched.UpgradeBlkTask:
		s.onUpgradeBlkDone(e)
	case sched.FlushSegTask:
		s.onFlushSegDone(e)
	case sched.UpgradeSegTask:
		s.onUpgradeSegDone(e)
	case sched.PrecommitBlkMetaTask:
		s.onPrecommitBlkDone(e)
	}
}

func (s *scheduler) onPreScheduleFlushBlkTask(e sched.Event) {
	event := e.(*flushMemblockEvent)
	s.commiters.mu.Lock()
	commiter, ok := s.commiters.blkmap[event.Meta.Segment.Table.Id]
	if !ok {
		commiter = newMetaBlkCommiter(s.opts, s)
		s.commiters.blkmap[event.Meta.Segment.Table.Id] = commiter
	}
	commiter.Register(event.Meta.Id)
	s.commiters.mu.Unlock()
}

func (s *scheduler) preprocess(e sched.Event) {
	switch e.Type() {
	case sched.FlushBlkTask:
		s.onPreScheduleFlushBlkTask(e)
	}
	e.AddObserver(s)
}

// Schedule schedules the given event via the internal scheduler.
func (s *scheduler) Schedule(e sched.Event) error {
	s.preprocess(e)
	return s.BaseScheduler.Schedule(e)
}

func (s *scheduler) ExecCmd(cmd CommandType) error {
	switch cmd {
	case sched.NoopCmd:
		return nil
	case TurnOnFlushSegmentCmd:
		s.turnOnFlushSegment()
		return nil
	case TurnOffFlushSegmentCmd:
		s.turnOffFlushSegment()
		return nil
	}
	panic("not supported")
}

func (s *scheduler) InstallBlock(meta *metadata.Block, table tif.ITableData) (block tif.IBlock, err error) {
	segment := table.StrongRefSegment(meta.Segment.Id)
	if segment == nil {
		if segment, err = table.RegisterSegment(meta.Segment); err != nil {
			return
		}
	}
	defer segment.Unref()
	block, err = table.RegisterBlock(meta)
	return
}
