package meta

import (
	"github.com/pkg/errors"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	// log "github.com/sirupsen/logrus"
)

func NewGetSSOp(ctx *OpCtx) *GetSSOp {
	op := &GetSSOp{}
	op.Op = *NewOp(op, ctx, ctx.Opts.Meta.Updater)
	return op
}

type GetSSOp struct {
	Op
	SS *md.MetaInfo
}

func (op *GetSSOp) Execute() error {
	ctx := md.CopyCtx{Ts: md.NowMicro(), Attached: true}
	op.SS = op.Ctx.Opts.Meta.Info.Copy(ctx)
	if op.SS == nil {
		return errors.New("empty metainfo")
	}
	return nil
}
