package updates

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
)

type BlockView struct {
	Ts          uint64
	Raw         batch.IBatch
	UpdateMasks map[uint16]*roaring.Bitmap
	UpdateVals  map[uint16]map[uint32]interface{}
	DeleteMask  *roaring.Bitmap
	Applied     batch.IBatch
}

func NewBlockView(ts uint64) *BlockView {
	return &BlockView{
		Ts:          ts,
		UpdateMasks: make(map[uint16]*roaring.Bitmap),
		UpdateVals:  make(map[uint16]map[uint32]interface{}),
	}
}

func (view *BlockView) Eval() {
	if len(view.UpdateMasks) == 0 {
		view.Applied = view.Raw
		view.Raw = nil
		return
	}
	// 1. Copy Raw to ibat
	// 2. Apply updates to ibat
	// 3. Set view.Applied as ibat
	// 4. Set view.Raw as nil

	// attrs := make([]string, 0)
	// vecs := make([]*vector.Vector, 0)
	// for colIdx, mask := range view.UpdateMasks {
	// 	attrs = append(attrs, view.Raw.Attrs[colIdx])
	// 	vals := view.UpdateVals[colIdx]
	// 	vec := compute.ApplyUpdateToVector(view.Raw.Vecs[colIdx], mask, vals)
	// 	vecs = append(vecs, vec)
	// }
	// view.Applied = batch.New(true, attrs)
	// for i, vec := range vecs {
	// 	view.Applied.Vecs[i] = vec
	// }
	// view.Raw = nil
}

func (view *BlockView) Marshal() (buf []byte, err error) {
	// TODO
	return
}

func (view *BlockView) Unmarshal(buf []byte) (err error) {
	// TODO
	return
}
