package meta

import (
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	// log "github.com/sirupsen/logrus"
)

func NewCheckpointOp(ctx *OpCtx, info *md.MetaInfo) *CheckpointOp {
	op := new(CheckpointOp)
	op.Info = info
	op.Op = *NewOp(op, ctx, ctx.Opts.Meta.Flusher)
	return op
}

type CheckpointOp struct {
	Op
	Info *md.MetaInfo
}

func (op *CheckpointOp) Execute() (err error) {
	op.Info.CheckPoint += 1
	ck := op.Ctx.Opts.Meta.CKFactory.Create()
	err = ck.PreCommit(op.Info)
	if err != nil {
		return err
	}
	err = ck.Commit(op.Info)
	if err != nil {
		return err
	}
	err = op.Ctx.Opts.Meta.Info.UpdateCheckpoint(op.Info.CheckPoint)
	if err != nil {
		panic(err)
	}

	return err
}
