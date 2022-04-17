package moengine

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

func newBlock(h handle.Block) *txnBlock {
	return &txnBlock{
		handle: h,
	}
}

func (blk *txnBlock) Read(cs []uint64, attrs []string, compressed []*bytes.Buffer, deCompressed []*bytes.Buffer) (*batch.Batch, error) {
	bat := batch.New(true, attrs)
	bat.Vecs = make([]*vector.Vector, len(attrs))
	for i, attr := range attrs {
		vec, _, err := blk.handle.GetVectorCopy(attr, compressed[i], deCompressed[i])
		if err != nil {
			return nil, err
		}
		vec.Ref = cs[i]
		bat.Vecs[i] = vec
	}
	return bat, nil
}
