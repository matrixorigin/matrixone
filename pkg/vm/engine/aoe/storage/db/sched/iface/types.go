package iface

import "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/sched"

type DBScheduler interface {
	sched.Scheduler
}
