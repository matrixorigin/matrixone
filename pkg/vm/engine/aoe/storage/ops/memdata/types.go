package memdata

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table"
	mtif "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"matrixone/pkg/vm/engine/aoe/storage/ops"
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	iworker "matrixone/pkg/vm/engine/aoe/storage/worker/base"
	// log "github.com/sirupsen/logrus"
)

type OpCtx struct {
	Opts      *e.Options
	Tables    *table.Tables
	MTManager mtif.IManager
	MTBufMgr  bmgrif.IBufferManager
	SSTBufMgr bmgrif.IBufferManager
	FsMgr     base.IManager
	TableMeta *md.Table
}

type Op struct {
	ops.Op
	Ctx *OpCtx
}

func NewOp(impl iops.IOpInternal, ctx *OpCtx, w iworker.IOpWorker) *Op {
	op := &Op{
		Ctx: ctx,
		Op: ops.Op{
			Impl:   impl,
			ErrorC: make(chan error),
			Worker: w,
		},
	}
	return op
}
