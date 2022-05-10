package moengine

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

func newBlock(h handle.Block) *txnBlock {
	return &txnBlock{
		handle: h,
	}
}

func (blk *txnBlock) Read(cs []uint64, attrs []string, compressed []*bytes.Buffer, deCompressed []*bytes.Buffer) (*batch.Batch, error) {
	var view *model.ColumnView
	var err error
	bat := batch.New(true, attrs)
	bat.Vecs = make([]*vector.Vector, len(attrs))
	for i, attr := range attrs {
		view, err = blk.handle.GetColumnDataByName(attr, compressed[i], deCompressed[i])
		if err != nil {
			return nil, err
		}
		view.AppliedVec.Ref = cs[i]
		bat.Vecs[i] = view.AppliedVec
	}
	return bat, nil
}
