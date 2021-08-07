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
	ioHandler := sched.NewPoolHandler(4)
	cpuHandler := sched.NewPoolHandler(2)
	dispatcher.RegisterHandler(sched.DataIOBoundEvent, ioHandler)
	dispatcher.RegisterHandler(sched.DataCpuBoundEvent, cpuHandler)
	sch.RegisterDispatcher(sched.DataIOBoundEvent, dispatcher)
	sch.RegisterDispatcher(sched.DataCpuBoundEvent, dispatcher)
	sch.Start()
	return sch
}
