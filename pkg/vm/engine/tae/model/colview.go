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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type ColumnView struct {
	ColIdx     int
	Ts         uint64
	data       containers.Vector
	dataView   containers.Vector
	UpdateMask *roaring.Bitmap
	UpdateVals map[uint32]any
	DeleteMask *roaring.Bitmap
	MemNode    *common.MemNode
	LogIndexes []*wal.Index
}

func NewColumnView(ts uint64, colIdx int) *ColumnView {
	return &ColumnView{
		Ts:     ts,
		ColIdx: colIdx,
	}
}

func (view *ColumnView) Orhpan() containers.Vector {
	data := view.data
	view.data = nil
	return data
}

func (view *ColumnView) SetData(data containers.Vector) {
	view.data = data
	view.dataView = data.GetView()
}

func (view *ColumnView) ApplyDeletes() containers.Vector {
	if view.DeleteMask == nil {
		return view.data
	}
	it := view.DeleteMask.Iterator()
	for it.HasNext() {
		row := it.Next()
		view.data.Delete(int(row))
	}
	return view.dataView
}

func (view *ColumnView) Eval(clear bool) (err error) {
	if view.UpdateMask == nil {
		return
	}
	it := view.UpdateMask.Iterator()
	for it.HasNext() {
		row := it.Next()
		view.data.Update(int(row), view.UpdateVals[row])
	}
	if clear {
		view.UpdateMask = nil
		view.UpdateVals = nil
	}
	return
}

func (view *ColumnView) GetData() containers.Vector {
	return view.dataView
}

func (view *ColumnView) Length() int {
	return view.data.Length()
}

func (view *ColumnView) String() string {
	return view.data.String()
}

func (view *ColumnView) GetValue(row int) any {
	return view.data.Get(row)
}

func (view *ColumnView) Close() {
	if view.MemNode != nil {
		common.GPool.Free(view.MemNode)
		view.MemNode = nil
	}
	if view.data != nil {
		view.data.Close()
	}
	view.data = nil
	view.dataView = nil
	view.UpdateMask = nil
	view.UpdateVals = nil
	view.DeleteMask = nil
}
