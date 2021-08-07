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
	dispatcher.RegisterHandler(sched.IOBoundEvent, ioHandler)
	dispatcher.RegisterHandler(sched.CpuBoundEvent, cpuHandler)
	sch.RegisterDispatcher(sched.IOBoundEvent, dispatcher)
	sch.RegisterDispatcher(sched.CpuBoundEvent, dispatcher)
	sch.Start()
	return sch
}
