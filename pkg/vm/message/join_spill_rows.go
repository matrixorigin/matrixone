// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package message

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"math"
)

// WriteJoinSpillRows compacts selected rows before encoding. Only one bucket's
// buffer is live, and the full replacement allocation is checked before Union.
// A row larger than the target is a singleton; its irreducible workspace is
// admitted by the caller's query ticket rather than being a new SQL row limit.
func WriteJoinSpillRows(mp *mpool.MPool, src *batch.Batch, rows []int32, target int64, flush func(*batch.Batch) error) error {
	if len(rows) == 0 {
		return nil
	}
	var buf *batch.Batch
	makeBuffer := func() {
		buf = batch.NewOffHeapWithSize(len(src.Vecs))
		for i, v := range src.Vecs {
			buf.Vecs[i] = vector.NewOffHeapVecWithType(*v.GetType())
		}
	}
	makeBuffer()
	defer func() {
		if buf != nil {
			buf.Clean(mp)
		}
	}()
	for _, selected := range rows {
		row := int(selected)
		growth, allocated := int64(0), int64(0)
		for i, v := range src.Vecs {
			growth += buf.Vecs[i].AppendWindowAllocation(v, row, row+1)
			allocated += int64(buf.Vecs[i].Allocated())
		}
		if buf.RowCount() > 0 && (buf.RowCount() >= 8192 || growth > target-allocated) {
			if err := flush(buf); err != nil {
				return err
			}
			buf.Clean(mp)
			buf = nil
			makeBuffer()
		}
		for i, v := range src.Vecs {
			area := v.WindowSize(row, row+1) - int64(v.GetType().TypeSize())
			if err := buf.Vecs[i].PreExtendWithArea(1, int(area), mp); err != nil {
				return err
			}
			if err := buf.Vecs[i].UnionOne(v, int64(row), mp); err != nil {
				return err
			}
		}
		buf.AddRowCount(1)
	}
	return flush(buf)
}

// ForEachJoinSpillWindow bounds referenced input before expression evaluation.
// A flat vector can repeat a shared varlen value in every row while owning just
// one physical area; WindowSize counts those references rather than that area.
func ForEachJoinSpillWindow(src *batch.Batch, target int64, visit func(*batch.Batch) error) error {
	if src.RowCount() == 0 {
		return nil
	}
	start, bytes := 0, int64(0)
	for row := 0; row < src.RowCount(); row++ {
		next := int64(0)
		for _, vec := range src.Vecs {
			n := vec.WindowSize(row, row+1)
			if n > math.MaxInt64-next {
				return moerr.NewInternalErrorNoCtx("spill window size overflows")
			}
			next += n
		}
		if row > start && (row-start >= 8192 || next > target-bytes) {
			view, err := src.Window(start, row)
			if err != nil {
				return err
			}
			err = visit(view)
			// Window borrows vector storage; cleaning it retires only the view.
			view.Clean(nil)
			if err != nil {
				return err
			}
			start, bytes = row, 0
		}
		if next > math.MaxInt64-bytes {
			return moerr.NewInternalErrorNoCtx("spill window size overflows")
		}
		bytes += next
	}
	if start == 0 {
		return visit(src)
	}
	view, err := src.Window(start, src.RowCount())
	if err != nil {
		return err
	}
	err = visit(view)
	view.Clean(nil)
	return err
}
