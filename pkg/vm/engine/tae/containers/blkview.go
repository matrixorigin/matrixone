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

package containers

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
)

type BaseView struct {
	DeleteMask *nulls.Bitmap
}

// TODO: remove this BlockView later
// Use Batch instead
type BlockView struct {
	*BaseView
	Columns map[int]*ColumnView
}

func NewBlockView() *BlockView {
	return &BlockView{
		BaseView: &BaseView{},
		Columns:  make(map[int]*ColumnView),
	}
}

func (view *BlockView) Orphan(i int) Vector {
	col := view.Columns[i]
	return col.Orphan()
}

func (view *BlockView) SetBatch(bat *Batch) {
	for i, col := range bat.Vecs {
		view.SetData(i, col)
	}
}

func (view *BlockView) GetColumnData(i int) Vector {
	return view.Columns[i].GetData()
}

// FIXME: i should not be idx in schema
func (view *BlockView) SetData(i int, data Vector) {
	col := view.Columns[i]
	if col == nil {
		col = NewColumnView(i)
		view.Columns[i] = col
	}
	col.SetData(data)
}

func (view *BlockView) ApplyDeletes() {
	if view.DeleteMask.IsEmpty() {
		return
	}
	for _, col := range view.Columns {
		col.data.CompactByBitmap(view.DeleteMask)
	}
	view.DeleteMask = nil
}

func (view *BlockView) Close() {
	for _, col := range view.Columns {
		col.Close()
	}
	view.DeleteMask = nil
}
