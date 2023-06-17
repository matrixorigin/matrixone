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

type ColumnView struct {
	*BaseView
	ColIdx int
	data   Vector
}

func NewColumnView(colIdx int) *ColumnView {
	return &ColumnView{
		BaseView: &BaseView{},
		ColIdx:   colIdx,
	}
}

func (view *ColumnView) Orphan() Vector {
	data := view.data
	view.data = nil
	return data
}

func (view *ColumnView) SetData(data Vector) {
	view.data = data
}

func (view *ColumnView) ApplyDeletes() Vector {
	if view.DeleteMask.IsEmpty() {
		return view.data
	}
	view.data.CompactByBitmap(view.DeleteMask)
	view.DeleteMask = nil
	return view.data
}

func (view *ColumnView) GetData() Vector {
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

func (view *ColumnView) GetValue(row int) (any, bool) {
	return view.data.Get(row), view.data.IsNull(row)
}

func (view *ColumnView) IsDeleted(row int) bool {
	return view.DeleteMask.Contains(uint64(row))
}

func (view *ColumnView) Close() {
	if view.data != nil {
		view.data.Close()
	}
	view.data = nil
	view.DeleteMask = nil
}
