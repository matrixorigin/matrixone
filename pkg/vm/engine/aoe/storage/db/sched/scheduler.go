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
	"matrixone/pkg/logutil"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	"sync"
)

type metablkCommiter struct {
	sync.RWMutex
	scheduler  sched.Scheduler
	pendings   []uint64
	flushdones map[uint64]*flushMemtableEvent
}

func newMetaBlkCommiter(scheduler sched.Scheduler) *metablkCommiter {
	c := &metablkCommiter{
		scheduler:  scheduler,
		pendings:   make([]uint64, 0),
		flushdones: make(map[uint64]*flushMemtableEvent),
	}
	return c
}

func (p *metablkCommiter) IsEmpty() bool {
	p.RLock()
	defer p.RUnlock()
	return len(p.pendings) == 0
}

func (p *metablkCommiter) Register(e *precommitBlockEvent) {
	p.Lock()
	p.pendings = append(p.pendings, e.Id.BlockID)
	p.Unlock()
}

func (p *metablkCommiter) doSchedule(e *flushMemtableEvent) {
	ctx := &Context{Opts: e.Ctx.Opts}
	commit := NewCommitBlkEvent(ctx, e.Meta)
	p.scheduler.Schedule(commit)
}

func (p *metablkCommiter) Accept(e *flushMemtableEvent) {
	// TODO: retry logic
	if err := e.GetError(); err != nil {
		panic(err)
	}
	if e.Meta == nil {
		return
	}
	p.Lock()
	if p.pendings[0] != e.Meta.ID {
		p.flushdones[e.Meta.ID] = e
	} else {
		p.doSchedule(e)
		var i int
		for i = 1; i < len(p.pendings); i++ {
			flushe, ok := p.flushdones[p.pendings[i]]
			if !ok {
				break
			}
			delete(p.flushdones, p.pendings[i])
			p.doSchedule(flushe)
		}
		p.pendings = p.pendings[i:]
	}
	p.Unlock()
}

type scheduler struct {
	sched.BaseScheduler
	opts      *e.Options
	tables    *table.Tables
	commiters struct {
		mu     sync.RWMutex
		blkmap map[uint64]*metablkCommiter
	}
}

func NewScheduler(opts *e.Options, tables *table.Tables) *scheduler {
	s := &scheduler{
		BaseScheduler: *sched.NewBaseScheduler("scheduler"),
		opts:          opts,
		tables:        tables,
	}
	s.commiters.blkmap = make(map[uint64]*metablkCommiter)

	dispatcher := sched.NewBaseDispatcher()
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

	dispatcher.RegisterHandler(sched.StatelessEvent, statelessHandler)
	dispatcher.RegisterHandler(sched.FlushSegTask, flushsegHandler)
	dispatcher.RegisterHandler(sched.FlushMemtableTask, flushblkHandler)
	dispatcher.RegisterHandler(sched.CommitBlkTask, metaHandler)
	dispatcher.RegisterHandler(sched.UpgradeBlkTask, memdataHandler)
	dispatcher.RegisterHandler(sched.UpgradeSegTask, memdataHandler)
	dispatcher.RegisterHandler(sched.MetaCreateTableTask, metaHandler)
	dispatcher.RegisterHandler(sched.MetaDropTableTask, metaHandler)
	dispatcher.RegisterHandler(sched.MetaCreateBlkTask, metaHandler)
	dispatcher.RegisterHandler(sched.MemdataUpdateEvent, memdataHandler)
	dispatcher.RegisterHandler(sched.FlushTableMetaTask, metaHandler)
	dispatcher.RegisterHandler(sched.PrecommitBlkMetaTask, metaHandler)

	s.RegisterDispatcher(sched.StatelessEvent, dispatcher)
	s.RegisterDispatcher(sched.FlushSegTask, dispatcher)
	s.RegisterDispatcher(sched.FlushMemtableTask, dispatcher)
	s.RegisterDispatcher(sched.CommitBlkTask, dispatcher)
	s.RegisterDispatcher(sched.UpgradeBlkTask, dispatcher)
	s.RegisterDispatcher(sched.UpgradeSegTask, dispatcher)
	s.RegisterDispatcher(sched.MetaCreateTableTask, dispatcher)
	s.RegisterDispatcher(sched.MetaDropTableTask, dispatcher)
	s.RegisterDispatcher(sched.MetaCreateBlkTask, dispatcher)
	s.RegisterDispatcher(sched.MemdataUpdateEvent, dispatcher)
	s.RegisterDispatcher(sched.FlushTableMetaTask, dispatcher)
	s.RegisterDispatcher(sched.PrecommitBlkMetaTask, dispatcher)
	s.Start()
	return s
}

func (s *scheduler) onPrecommitBlkDone(e sched.Event) {
	event := e.(*precommitBlockEvent)
	s.commiters.mu.Lock()
	commiter, ok := s.commiters.blkmap[event.Id.TableID]
	if !ok {
		commiter = newMetaBlkCommiter(s)
		s.commiters.blkmap[event.Id.TableID] = commiter
	}
	commiter.Register(event)
	s.commiters.mu.Unlock()
}

func (s *scheduler) onFlushMemtableDone(e sched.Event) {
	event := e.(*flushMemtableEvent)
	s.commiters.mu.RLock()
	commiter := s.commiters.blkmap[event.Meta.Segment.Table.ID]
	s.commiters.mu.RUnlock()
	commiter.Accept(event)
	s.commiters.mu.Lock()
	if commiter.IsEmpty() {
		delete(s.commiters.blkmap, event.Meta.Segment.Table.ID)
	}
	s.commiters.mu.Unlock()
}

func (s *scheduler) onCommitBlkDone(e sched.Event) {
	event := e.(*commitBlkEvent)
	newMeta := event.NewMeta
	cpCtx := md.CopyCtx{Ts: md.NowMicro(), Attached: true}
	newMeta.Segment.Table.RLock()
	tblMetaCpy := newMeta.Segment.Table.Copy(cpCtx)
	newMeta.Segment.Table.RUnlock()

	ctx := &Context{Opts: s.opts}
	flushEvent := NewFlushTableEvent(ctx, tblMetaCpy)
	s.Schedule(flushEvent)

	if !event.Ctx.HasDataScope() {
		return
	}
	mctx := &Context{Opts: s.opts}
	tableData, err := s.tables.StrongRefTable(newMeta.Segment.Table.ID)
	if err != nil {
		s.opts.EventListener.BackgroundErrorCB(err)
		return
	}
	newevent := NewUpgradeBlkEvent(mctx, newMeta, tableData)
	s.Schedule(newevent)
}

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
	segment := event.TableData.StrongRefSegment(event.Meta.Segment.ID)
	if segment == nil {
		logutil.Warnf("Probably table %d is dropped", event.Meta.Segment.Table.ID)
		return
	}
	logutil.Infof(" %s | Segment %d | FlushSegEvent | Started", sched.EventPrefix, event.Meta.Segment.ID)
	flushCtx := &Context{Opts: s.opts}
	flushEvent := NewFlushSegEvent(flushCtx, segment)
	s.Schedule(flushEvent)
}

func (s *scheduler) onFlushSegDone(e sched.Event) {
	event := e.(*flushSegEvent)
	if err := e.GetError(); err != nil {
		// s.opts.EventListener.BackgroundErrorCB(err)
		event.Segment.Unref()
		return
	}
	ctx := &Context{Opts: s.opts}
	meta := event.Segment.GetMeta()
	td, err := s.tables.StrongRefTable(meta.Table.ID)
	if err != nil {
		// s.opts.EventListener.BackgroundErrorCB(err)
		event.Segment.Unref()
		return
	}
	logutil.Infof(" %s | Segment %d | UpgradeSegEvent | Started", sched.EventPrefix, meta.ID)
	newevent := NewUpgradeSegEvent(ctx, event.Segment, td)
	s.Schedule(newevent)
}

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
	case sched.FlushMemtableTask:
		s.onFlushMemtableDone(e)
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

func (s *scheduler) Schedule(e sched.Event) error {
	e.AddObserver(s)
	return s.BaseScheduler.Schedule(e)
}
