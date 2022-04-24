package updates

import (
	"bytes"

	"github.com/RoaringBitmap/roaring"
	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
)

type BlockView struct {
	Ts           uint64
	Raw          batch.IBatch
	RawBatch     *gbat.Batch
	UpdateMasks  map[uint16]*roaring.Bitmap
	UpdateVals   map[uint16]map[uint32]interface{}
	DeleteMask   *roaring.Bitmap
	Applied      batch.IBatch
	AppliedBatch *gbat.Batch
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

	var err error
	attrs := view.Raw.GetAttrs()
	vecs := make([]vector.IVector, len(attrs))
	for i, attr := range attrs {
		vecs[i], err = view.Raw.GetVectorByAttr(attr)
		if err != nil {
			panic(err)
		}
	}
	for colIdx, mask := range view.UpdateMasks {
		vals := view.UpdateVals[colIdx]
		vec, err := view.Raw.GetVectorByAttr(int(colIdx))
		if err != nil {
			panic(err)
		}
		vec = compute.ApplyUpdateToIVector(vec, mask, vals)

		vecs[colIdx] = vec
	}
	view.Applied, err = batch.NewBatch(attrs, vecs)
	if err != nil {
		panic(err)
	}
	view.Raw = nil
}

// update data.offset
func UpdateOffsets(data *types.Bytes, start, end int) {
	if start == -1 {
		data.Offsets[0] = 0
		start++
	}
	for i := start; i < end; i++ {
		data.Offsets[i+1] = data.Offsets[i] + data.Lengths[i]
	}
}

func (view *BlockView) Marshal() (buf []byte, err error) {
	var byteBuf bytes.Buffer
	// Ts
	byteBuf.Write(encoding.EncodeUint64(view.Ts))
	// DeleteMask
	if view.DeleteMask == nil {
		cardinality := uint64(0)
		byteBuf.Write(encoding.EncodeUint64(cardinality))
	} else {
		cardinality := view.DeleteMask.GetCardinality()
		byteBuf.Write(encoding.EncodeUint64(cardinality))
		iterator := view.DeleteMask.Iterator()
		for iterator.HasNext() {
			idx := iterator.Next()
			byteBuf.Write(encoding.EncodeUint32(idx))
		}
	}
	// Applied
	if view.Applied == nil {
		batLength := 0
		byteBuf.Write(encoding.EncodeUint64(uint64(batLength)))
	} else {
		batBuf, err := view.Applied.Marshal()
		if err != nil {
			return nil, err
		}
		batLength := len(batBuf)
		byteBuf.Write(encoding.EncodeUint64(uint64(batLength)))
		byteBuf.Write(batBuf)
	}
	buf = byteBuf.Bytes()
	return
}

func (view *BlockView) Unmarshal(buf []byte) (err error) {
	pos := 0
	// Ts
	view.Ts = encoding.DecodeUint64(buf[pos : pos+8])
	pos += 8
	// DeleteMask
	cardinality := encoding.DecodeUint64(buf[pos : pos+8])
	pos += 8
	view.DeleteMask = roaring.NewBitmap()
	for i := 0; i < int(cardinality); i++ {
		idx := encoding.DecodeUint32(buf[pos : pos+4])
		pos += 4
		view.DeleteMask.Add(idx)
	}
	// Applied
	batLength := encoding.DecodeUint64(buf[pos : pos+8])
	pos += 8
	if batLength == uint64(0) {
		return
	}
	view.Applied = &batch.Batch{}
	view.Applied.Unmarshal(buf[pos : pos+int(batLength)])
	return
}
