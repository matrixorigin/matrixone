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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type BaseView struct {
	DeleteMask *roaring.Bitmap
}

type BlockView struct {
	*BaseView
	Columns          map[int]*ColumnView
	DeleteLogIndexes []*wal.Index
}

func NewBlockView() *BlockView {
	return &BlockView{
		BaseView: &BaseView{},
		Columns:  make(map[int]*ColumnView),
	}
}

func (view *BlockView) Orphan(i int) containers.Vector {
	col := view.Columns[i]
	return col.Orphan()
}

func (view *BlockView) SetBatch(bat *containers.Batch) {
	for i, col := range bat.Vecs {
		view.SetData(i, col)
	}
}

func (view *BlockView) GetColumnData(i int) containers.Vector {
	return view.Columns[i].GetData()
}

func (view *BlockView) SetData(i int, data containers.Vector) {
	col := view.Columns[i]
	if col == nil {
		col = NewColumnView(i)
		view.Columns[i] = col
	}
	col.SetData(data)
}

func (view *BlockView) SetUpdates(i int, mask *roaring.Bitmap, vals map[uint32]any) {
	col := view.Columns[i]
	if col == nil {
		col = NewColumnView(i)
		view.Columns[i] = col
	}
	col.UpdateMask = mask
	col.UpdateVals = vals
}

func (view *BlockView) SetLogIndexes(i int, indexes []*wal.Index) {
	col := view.Columns[i]
	if col == nil {
		col = NewColumnView(i)
		view.Columns[i] = col
	}
	col.LogIndexes = indexes
}

func (view *BlockView) Eval(clear bool) (err error) {
	for _, col := range view.Columns {
		if err = col.Eval(clear); err != nil {
			break
		}
	}
	return
}

func (view *BlockView) ApplyDeletes() {
	if view.DeleteMask == nil {
		return
	}
	for _, col := range view.Columns {
		col.data.Compact(view.DeleteMask)
	}
	view.DeleteMask = nil
}

func (view *BlockView) Close() {
	for _, col := range view.Columns {
		col.Close()
	}
	view.DeleteMask = nil
}
