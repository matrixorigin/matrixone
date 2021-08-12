package meta

import (
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	// log "github.com/sirupsen/logrus"
)

func NewFlushInfoOp(ctx *OpCtx, info *md.MetaInfo) *FlushInfoOp {
	op := new(FlushInfoOp)
	op.Info = info
	op.Op = *NewOp(op, ctx, ctx.Opts.Meta.Flusher)
	return op
}

type FlushInfoOp struct {
	Op
	Info *md.MetaInfo
}

func (op *FlushInfoOp) Execute() (err error) {
	ck := op.Ctx.Opts.Meta.CKFactory.Create()
	err = ck.PreCommit(op.Info)
	if err != nil {
		return err
	}
	err = ck.Commit(op.Info)
	if err != nil {
		return err
	}
	op.Ctx.Opts.Meta.Info.UpdateCheckpointTime(op.Info.CkpTime)

	return err
}
