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

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/sched"
)

type CommandType = sched.CommandType

const (
	TurnOffFlushSegmentCmd = iota + sched.CustomizedCmd
	TurnOnFlushSegmentCmd
	TurnOffUpgradeSegmentCmd
	TurnOnUpgradeSegmentCmd
	TurnOnUpgradeSegmentMetaCmd
	TurnOffUpgradeSegmentMetaCmd
)

type CmdMask = uint64

const (
	FlushSegMask CmdMask = iota
	UpgradeSegMask
	UpgradeSegMetaMask
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

type Controller struct {
	cmu  sync.RWMutex
	mask *roaring64.Bitmap
}

func NewController() *Controller {
	return &Controller{
		mask: roaring64.NewBitmap(),
	}
}

func (c *Controller) UpdateMask(mask CmdMask, on bool) {
	c.cmu.Lock()
	defer c.cmu.Unlock()
	if on {
		c.mask.Remove(mask)
	} else {
		c.mask.Add(mask)
	}
}

func (c *Controller) IsOn(mask CmdMask) bool {
	if c == nil {
		return true
	}
	c.cmu.RLock()
	defer c.cmu.RUnlock()
	return !c.mask.Contains(mask)
}

// scheduler is the global event scheduler for AOE. It wraps the
// BaseScheduler with some DB metadata and block committers.
//
// This directory mainly focus on different events scheduled under
// DB space. Basically you can refer to code under sched/ for more
// implementation details for scheduler itself.
type scheduler struct {
	*Controller
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
		Controller:    NewController(),
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
	dispatcher.RegisterHandler(StatelessEvent, statelessHandler)
	dispatcher.RegisterHandler(FlushSegTask, flushsegHandler)
	dispatcher.RegisterHandler(FlushIndexTask, flushsegHandler)
	dispatcher.RegisterHandler(FlushBlkTask, flushblkHandler)
	dispatcher.RegisterHandler(CommitBlkTask, metaHandler)
	dispatcher.RegisterHandler(UpgradeBlkTask, memdataHandler)
	dispatcher.RegisterHandler(UpgradeSegTask, memdataHandler)
	dispatcher.RegisterHandler(MetaCreateTableTask, metaHandler)
	dispatcher.RegisterHandler(MetaDropTableTask, metaHandler)
	dispatcher.RegisterHandler(MetaCreateBlkTask, metaHandler)
	dispatcher.RegisterHandler(MemdataUpdateEvent, memdataHandler)
	dispatcher.RegisterHandler(FlushTableMetaTask, metaHandler)
	dispatcher.RegisterHandler(PrecommitBlkMetaTask, metaHandler)
	dispatcher.RegisterHandler(FlushTBlkTask, flushtblkHandler)

	// Register dispatcher
	s.RegisterDispatcher(StatelessEvent, dispatcher)
	s.RegisterDispatcher(FlushSegTask, dispatcher)
	s.RegisterDispatcher(FlushIndexTask, dispatcher)
	s.RegisterDispatcher(FlushBlkTask, dispatcher)
	s.RegisterDispatcher(CommitBlkTask, dispatcher)
	s.RegisterDispatcher(UpgradeBlkTask, dispatcher)
	s.RegisterDispatcher(UpgradeSegTask, dispatcher)
	s.RegisterDispatcher(MetaCreateTableTask, dispatcher)
	s.RegisterDispatcher(MetaDropTableTask, dispatcher)
	s.RegisterDispatcher(MetaCreateBlkTask, dispatcher)
	s.RegisterDispatcher(MemdataUpdateEvent, dispatcher)
	s.RegisterDispatcher(FlushTableMetaTask, dispatcher)
	s.RegisterDispatcher(PrecommitBlkMetaTask, dispatcher)
	s.RegisterDispatcher(FlushTBlkTask, dispatcher)
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
		err := s.opts.EventListener.OnBackgroundError(err)
		if err != nil {
			panic(err)
		}
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
		s.opts.EventListener.OnBackgroundError(err)
		return
	}
	logutil.Infof(" %s | Block %d | UpgradeBlkEvent | Started", sched.EventPrefix, newMeta.Id)
	newevent := NewUpgradeBlkEvent(mctx, newMeta, tableData)
	err = s.Schedule(newevent)
	if err != nil && err != sched.ErrSchedule {
		panic(err)
	}
}

// onUpgradeBlkDone handles the finished upgrade block event, and if segment
// was closed, start a new flush segment event and schedule it.
func (s *scheduler) onUpgradeBlkDone(e sched.Event) {
	event := e.(*upgradeBlkEvent)
	defer event.TableData.Unref()
	if err := e.GetError(); err != nil {
		err := s.opts.EventListener.OnBackgroundError(err)
		if err != nil {
			panic(err)
		}
		return
	}
	if !event.Ctx.HasDataScope() {
		return
	}
	defer event.Data.Unref()
	// TODO(zzl): thread safe?
	if event.Data.GetType() == base.PERSISTENT_BLK {
		flushIdxEvent := NewFlushBlockIndexEvent(&Context{Opts: s.opts}, event.Data)
		flushIdxEvent.FlushAll = true
		err := s.Schedule(flushIdxEvent)
		if err != nil && err != sched.ErrSchedule {
			panic(err)
		}
	}
	if !event.SegmentClosed {
		return
	}
	if !s.IsOn(FlushSegMask) {
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
	err := s.Schedule(flushEvent)
	if err != nil && err != sched.ErrSchedule {
		panic(err)
	}
}

// onFlushSegDone handles the finished flush segment event, generates a new
// upgrade segment event and schedules it.
func (s *scheduler) onFlushSegDone(e sched.Event) {
	event := e.(*flushSegEvent)
	if err := e.GetError(); err != nil {
		event.Segment.Unref()
		return
	}
	if !s.IsOn(UpgradeSegMask) {
		logutil.Warn("[Scheduler] Upgrade Segment Is Turned-Off")
		event.Segment.Unref()
		return
	}
	ctx := &Context{Opts: s.opts}
	ctx.Controller = s.Controller
	meta := event.Segment.GetMeta()
	td, err := s.tables.StrongRefTable(meta.Table.Id)
	if err != nil {
		event.Segment.Unref()
		event.Rollback("Rollback-TableNotExist")
		return
	}
	logutil.Infof(" %s | Segment %d | UpgradeSegEvent | Started", sched.EventPrefix, meta.Id)
	newevent := NewUpgradeSegEvent(ctx, event.Segment, td)
	err = s.Schedule(newevent)
	if err != nil && err != sched.ErrSchedule {
		panic(err)
	}
}

// onUpgradeSegDone handles the finished upgrade segment event and releases the
// occupied resources.
func (s *scheduler) onUpgradeSegDone(e sched.Event) {
	event := e.(*upgradeSegEvent)
	defer event.TableData.Unref()
	defer event.OldSegment.Unref()
	if err := e.GetError(); err != nil {
		s.opts.EventListener.OnBackgroundError(err)
		return
	}
	event.Segment.Unref()
	// start flush index
	flushCtx := &Context{Opts: s.opts}
	newevent := NewFlushSegIndexEvent(flushCtx, event.Segment)
	newevent.FlushAll = true
	err := s.Schedule(newevent)
	if err != nil && err != sched.ErrSchedule {
		panic(err)
	}
}

func (s *scheduler) OnExecDone(op interface{}) {
	e := op.(sched.Event)
	switch e.Type() {
	case FlushBlkTask:
		s.onFlushBlkDone(e)
	case CommitBlkTask:
		s.onCommitBlkDone(e)
	case UpgradeBlkTask:
		s.onUpgradeBlkDone(e)
	case FlushSegTask:
		s.onFlushSegDone(e)
	case UpgradeSegTask:
		s.onUpgradeSegDone(e)
	case PrecommitBlkMetaTask:
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
	case FlushBlkTask:
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
		s.UpdateMask(FlushSegMask, true)
		return nil
	case TurnOffFlushSegmentCmd:
		s.UpdateMask(FlushSegMask, false)
		return nil
	case TurnOnUpgradeSegmentCmd:
		s.UpdateMask(UpgradeSegMask, true)
		return nil
	case TurnOffUpgradeSegmentCmd:
		s.UpdateMask(UpgradeSegMask, false)
		return nil
	case TurnOnUpgradeSegmentMetaCmd:
		s.UpdateMask(UpgradeSegMetaMask, true)
		return nil
	case TurnOffUpgradeSegmentMetaCmd:
		s.UpdateMask(UpgradeSegMetaMask, false)
		return nil
	}
	panic("not supported")
}

func (s *scheduler) InstallBlock(meta *metadata.Block, tableData iface.ITableData) (block iface.IBlock, err error) {
	ctx := &Context{Opts: s.opts, Waitable: true}
	e := NewInstallBlockEvent(ctx, meta, tableData)
	err = s.Schedule(e)
	if err != nil && err != sched.ErrSchedule {
		panic(err)
	}
	if err = e.WaitDone(); err != nil {
		return
	}
	block = e.Block
	return
}

func (s *scheduler) AsyncFlushBlock(block iface.IMutBlock) {
	ctx := &Context{Opts: s.opts}
	e := NewFlushMemBlockEvent(ctx, block)
	err := s.Schedule(e)
	if err != nil && err != sched.ErrSchedule {
		panic(err)
	}
}
