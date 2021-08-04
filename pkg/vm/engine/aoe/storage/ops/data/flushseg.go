package data

import (
	// log "github.com/sirupsen/logrus"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
)

type FlushSegOp struct {
	Op
	Segment iface.ISegment
}

func NewFlushSegOp(ctx *OpCtx, seg iface.ISegment) *FlushSegOp {
	op := &FlushSegOp{Segment: seg}
	op.Op = *NewOp(op, ctx, ctx.Opts.Data.Sorter)
	return op
}

func (op *FlushSegOp) Execute() error {
	defer op.Segment.Unref()
	ids := op.Segment.BlockIds()
	meta := op.Segment.GetMeta()
	batches := make([]*batch.Batch, len(ids))
	colCnt := len(meta.Table.Schema.ColDefs)
	nodes := make([]*common.MemNode, 0)
	for i := 0; i < len(ids); i++ {
		blk := op.Segment.WeakRefBlock(ids[i])
		bat := &batch.Batch{}
		for colIdx := 0; colIdx < colCnt; colIdx++ {
			vec, err := blk.GetVectorWrapper(colIdx)
			if err != nil {
				panic(err)
			}
			bat.Vecs = append(bat.Vecs, &vec.Vector)
			if vec.Vector.Length() == 0 {
				panic("")
			}
			nodes = append(nodes, vec.MNode)
		}
		batches[i] = bat
	}

	release := func() {
		for _, node := range nodes {
			common.GPool.Free(node)
		}
	}
	defer release()

	w := dataio.NewSegmentWriter(batches, meta, op.Ctx.Opts.Meta.Conf.Dir)
	// w.SetPreExecutor(func() {
	// 	log.Infof("%s | Segment | Flushing", w.GetFileName())
	// })
	// w.SetPostExecutor(func() {
	// 	log.Infof("%s | Segment | Flushed", w.GetFileName())
	// })

	return w.Execute()
}
