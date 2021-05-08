package block

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/process"
)

func (b *Block) GetBatch(proc *process.Process) (*batch.Batch, error) {
	if b.Bat != nil {
		return b.Bat, nil
	}
	seg := b.R.Segment(b.Seg, proc)
	return seg.Block(seg.Blocks()[0], proc).Prefetch(b.Cs, b.Attrs, proc)
}
