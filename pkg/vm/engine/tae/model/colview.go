// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
