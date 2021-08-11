package db

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/sched"

	log "github.com/sirupsen/logrus"
)

type scheduler struct {
	sched.BaseScheduler
	opts   *e.Options
	tables *table.Tables
}

func NewScheduler(opts *e.Options, tables *table.Tables) *scheduler {
	s := &scheduler{
		BaseScheduler: *sched.NewBaseScheduler("scheduler"),
		opts:          opts,
		tables:        tables,
	}

	dispatcher := sched.NewBaseDispatcher()
	flushblkHandler := sched.NewPoolHandler(1, nil)
	flushblkHandler.Start()
	flushsegHandler := sched.NewPoolHandler(1, nil)
	flushsegHandler.Start()
	metaHandler := sched.NewSingleWorkerHandler("metaHandler")
	metaHandler.Start()
	memdataHandler := sched.NewSingleWorkerHandler("memdataHandler")
	memdataHandler.Start()
	statelessHandler := sched.NewPoolHandler(1, nil)
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
	s.Start()
	return s
}

func (s *scheduler) onFlushMemtableDone(e sched.Event) {
	event := e.(*flushMemtableEvent)
	if event.Meta == nil {
		return
	}
	ctx := &Context{
		Opts: s.opts,
	}
	newevent := NewCommitBlkEvent(ctx, event.Meta)
	s.Schedule(newevent)
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
	defer event.Data.Unref()
	if !event.SegmentClosed {
		return
	}
	segment := event.TableData.StrongRefSegment(event.Meta.Segment.ID)
	if segment == nil {
		log.Warnf("Probably table %d is dropped", event.Meta.Segment.Table.ID)
		return
	}
	log.Infof(" %s | Segment %d | FlushSegEvent | Started", sched.EventPrefix, event.Meta.Segment.ID)
	flushCtx := &Context{Opts: s.opts}
	flushEvent := NewFlushSegEvent(flushCtx, segment)
	s.Schedule(flushEvent)
}

func (s *scheduler) onFlushSegDone(e sched.Event) {
	event := e.(*flushSegEvent)
	if err := e.GetError(); err != nil {
		s.opts.EventListener.BackgroundErrorCB(err)
		event.Segment.Unref()
		return
	}
	ctx := &Context{Opts: s.opts}
	meta := event.Segment.GetMeta()
	td, err := s.tables.StrongRefTable(meta.Table.ID)
	if err != nil {
		s.opts.EventListener.BackgroundErrorCB(err)
		return
	}
	log.Infof(" %s | Segment %d | UpgradeSegEvent | Started", sched.EventPrefix, meta.ID)
	newevent := NewUpgradeSegEvent(ctx, event.Segment, td)
	s.Schedule(newevent)
}

func (s *scheduler) onUpgradeSegDone(e sched.Event) {
	event := e.(*upgradeSegEvent)
	// defer event.TableData.Unref()
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
	}
}

func (s *scheduler) Schedule(e sched.Event) error {
	e.AddObserver(s)
	return s.BaseScheduler.Schedule(e)
}
