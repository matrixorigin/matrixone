package meta

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
)

func NewCheckpointOp(ctx *OpCtx) *CheckpointOp {
	op := new(CheckpointOp)
	op.Op = *NewOp(op, ctx, ctx.Opts.Meta.Flusher)
	return op
}

type CheckpointOp struct {
	Op
}

func (op *CheckpointOp) Execute() (err error) {
	ts := md.NowMicro()
	meta := op.Ctx.Opts.Meta.Info.Copy(ts)
	if meta == nil {
		errMsg := fmt.Sprintf("CheckPoint error")
		log.Error(errMsg)
		err = errors.New(errMsg)
		return err
	}
	meta.CheckPoint += 1
	err = op.Ctx.Opts.Meta.Checkpointer.PreCommit(meta)
	if err != nil {
		return err
	}
	err = op.Ctx.Opts.Meta.Checkpointer.Commit()
	if err != nil {
		return err
	}
	err = op.Ctx.Opts.Meta.Info.UpdateCheckpoint(meta.CheckPoint)
	if err != nil {
		panic(err)
	}

	return err
}
