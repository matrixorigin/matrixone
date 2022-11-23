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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type ColumnView struct {
	*BaseView
	ColIdx     int
	data       containers.Vector
	UpdateMask *roaring.Bitmap
	UpdateVals map[uint32]any
	LogIndexes []*wal.Index
}

func NewColumnView(ts types.TS, colIdx int) *ColumnView {
	return &ColumnView{
		BaseView: &BaseView{
			Ts: ts,
		},
		ColIdx: colIdx,
	}
}

func (view *ColumnView) Orphan() containers.Vector {
	data := view.data
	view.data = nil
	return data
}

func (view *ColumnView) SetData(data containers.Vector) {
	view.data = data
}

func (view *ColumnView) ApplyDeletes() containers.Vector {
	if view.DeleteMask == nil {
		return view.data
	}
	view.data.Compact(view.DeleteMask)
	view.DeleteMask = nil
	return view.data
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
	return view.data
}

func (view *ColumnView) Length() int {
	return view.data.Length()
}

func (view *ColumnView) String() string {
	if view.data != nil {
		return view.data.String()
	}
	return "empty"
}

func (view *ColumnView) GetValue(row int) any {
	return view.data.Get(row)
}

func (view *ColumnView) IsDeleted(row int) bool {
	if view.DeleteMask == nil {
		return false
	}
	return view.DeleteMask.ContainsInt(row)
}

func (view *ColumnView) Close() {
	if view.data != nil {
		view.data.Close()
	}
	view.data = nil
	view.UpdateMask = nil
	view.UpdateVals = nil
	view.DeleteMask = nil
}
