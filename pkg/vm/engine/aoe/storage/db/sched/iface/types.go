package iface

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/sched"
)

type DBScheduler interface {
	sched.Scheduler
	InstallBlock(meta *metadata.Block, table iface.ITableData) (iface.IBlock, error)
	AsyncFlushBlock(block iface.IMutBlock)
}
