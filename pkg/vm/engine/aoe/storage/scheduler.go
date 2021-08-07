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
	cpuHandler := sched.NewPoolHandler(1)
	metaUpdateHandler := sched.NewSingleWorkerHandler("metaUpdateHandler")
	memdataUpdateHandler := sched.NewSingleWorkerHandler("memdataUpdateEvent")
	dispatcher.RegisterHandler(sched.IOBoundEvent, ioHandler)
	dispatcher.RegisterHandler(sched.CpuBoundEvent, cpuHandler)
	dispatcher.RegisterHandler(sched.MetaUpdateEvent, metaUpdateHandler)
	dispatcher.RegisterHandler(sched.MemdataUpdateEvent, memdataUpdateHandler)

	sch.RegisterDispatcher(sched.IOBoundEvent, dispatcher)
	sch.RegisterDispatcher(sched.CpuBoundEvent, dispatcher)
	sch.RegisterDispatcher(sched.MetaUpdateEvent, dispatcher)
	sch.RegisterDispatcher(sched.MemdataUpdateEvent, dispatcher)
	sch.Start()
	return sch
}
