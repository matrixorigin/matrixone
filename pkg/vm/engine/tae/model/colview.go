package model

import (
	"github.com/RoaringBitmap/roaring"
	movec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
)

type ColumnView struct {
	ColIdx      int
	Ts          uint64
	RawVec      *movec.Vector
	RawIVec     vector.IVector
	UpdateMask  *roaring.Bitmap
	UpdateVals  map[uint32]interface{}
	DeleteMask  *roaring.Bitmap
	AppliedVec  *movec.Vector
	AppliedIVec vector.IVector
}

func NewColumnView(ts uint64, colIdx int) *ColumnView {
	return &ColumnView{
		Ts:     ts,
		ColIdx: colIdx,
	}
}

func (view *ColumnView) ApplyDeletes() *movec.Vector {
	view.AppliedVec = compute.ApplyDeleteToVector(view.AppliedVec, view.DeleteMask)
	return view.AppliedVec
}

func (view *ColumnView) Eval(clear bool) error {
	if view.RawIVec != nil {
		view.AppliedIVec = compute.ApplyUpdateToIVector(view.RawIVec, view.UpdateMask, view.UpdateVals)
		if clear {
			view.RawIVec = nil
			view.UpdateMask = nil
			view.UpdateVals = nil
		}
		return nil
	}

	view.AppliedVec = compute.ApplyUpdateToVector(view.RawVec, view.UpdateMask, view.UpdateVals)
	if clear {
		view.RawVec = nil
		view.UpdateMask = nil
		view.UpdateVals = nil
	}
	return nil
}

func (view *ColumnView) GetColumnData() *movec.Vector {
	return view.AppliedVec
}

func (view *ColumnView) Length() int {
	return movec.Length(view.AppliedVec)
}

func (view *ColumnView) GetValue(row uint32) interface{} {
	return compute.GetValue(view.AppliedVec, row)
}
