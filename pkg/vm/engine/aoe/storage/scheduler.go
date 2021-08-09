package engine

import "matrixone/pkg/vm/engine/aoe/storage/sched"

type scheduler struct {
	sched.BaseScheduler
	opts *Options
}

func NewScheduler(opts *Options) *scheduler {
	sch := &scheduler{
		BaseScheduler: *sched.NewBaseScheduler(SchedulerName),
		opts:          opts,
	}

	dispatcher := sched.NewBaseDispatcher()
	ioHandler := sched.NewPoolHandler(1)
	ioHandler.Start()
	cpuHandler := sched.NewPoolHandler(1)
	cpuHandler.Start()
	sortHandler := sched.NewPoolHandler(1)
	sortHandler.Start()
	statelessHandler := sched.NewPoolHandler(2)
	statelessHandler.Start()
	metaUpdateHandler := sched.NewSingleWorkerHandler("metaUpdateHandler")
	metaUpdateHandler.Start()
	memdataUpdateHandler := sched.NewSingleWorkerHandler("memdataUpdateEvent")
	memdataUpdateHandler.Start()
	dispatcher.RegisterHandler(sched.IOBoundEvent, ioHandler)
	dispatcher.RegisterHandler(sched.CpuBoundEvent, cpuHandler)
	dispatcher.RegisterHandler(sched.MetaUpdateEvent, metaUpdateHandler)
	dispatcher.RegisterHandler(sched.MemdataUpdateEvent, memdataUpdateHandler)
	dispatcher.RegisterHandler(sched.StatelessEvent, statelessHandler)
	dispatcher.RegisterHandler(sched.MergeSortEvent, sortHandler)

	sch.RegisterDispatcher(sched.IOBoundEvent, dispatcher)
	sch.RegisterDispatcher(sched.CpuBoundEvent, dispatcher)
	sch.RegisterDispatcher(sched.MetaUpdateEvent, dispatcher)
	sch.RegisterDispatcher(sched.MemdataUpdateEvent, dispatcher)
	sch.RegisterDispatcher(sched.StatelessEvent, dispatcher)
	sch.RegisterDispatcher(sched.MergeSortEvent, dispatcher)
	sch.Start()
	return sch
}
