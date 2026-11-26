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

package mergesort

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/remaplist"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

// mergeGatherChunk bounds the per-call scratch of the column gathers.
const mergeGatherChunk = 8192

// mergeOrder records the rows a merge keeps, in output order. Global row ids
// number the rows of all input batches one after another (accRowCnt[b] is the
// id of batch b's first row); they are what the old → new map is keyed by.
type mergeOrder struct {
	accRowCnt []int64
	rows      remaplist.List // global row id per output row
	batches   remaplist.List // input batch per output row
}

func (o *mergeOrder) init(vpool DisposableVecPool, accRowCnt []int64, capacity int) error {
	o.accRowCnt = accRowCnt
	if err := o.rows.Init(vpool.GetMPool(), capacity); err != nil {
		return err
	}
	return o.batches.Init(vpool.GetMPool(), capacity)
}

func (o *mergeOrder) free() {
	o.rows.Free()
	o.batches.Free()
}

func (o *mergeOrder) add(batIdx int, row int64) error {
	if err := o.rows.Append(o.accRowCnt[batIdx] + row); err != nil {
		return err
	}
	return o.batches.Append(int64(batIdx))
}

// gather builds the output of a merge: block b takes the next toLayout[b]
// rows of the order, column by column. A block that gets no rows is left nil.
// The order must not hold more rows than the layout.
func (o *mergeOrder) gather(
	bats []*containers.Batch, toLayout []uint32, vpool DisposableVecPool,
) ([]*batch.Batch, []func(), error) {
	ret := make([]*batch.Batch, len(toLayout))
	rfs := make([]func(), len(toLayout))
	releaseAll := func() {
		for _, rf := range rfs {
			if rf != nil {
				rf()
			}
		}
	}
	total := o.rows.Len()
	scratch := min(total, mergeGatherChunk)
	rows := make([]int64, scratch)
	srcs := make([]*vector.Vector, scratch)
	downs := make([]*vector.Vector, len(bats))
	cnBat := containers.ToCNBatch(bats[0])
	mp := vpool.GetMPool()
	start := 0
	for b, size := range toLayout {
		n := min(int(size), total-start)
		if n <= 0 {
			break
		}
		ret[b], rfs[b] = getSimilarBatch(cnBat, int(size), vpool)
		for at := start; at < start+n; at += mergeGatherChunk {
			m := min(mergeGatherChunk, start+n-at)
			global := o.rows.Rows()[at : at+m]
			from := o.batches.Rows()[at : at+m]
			for i, g := range global {
				rows[i] = g - o.accRowCnt[from[i]]
			}
			for col, dst := range ret[b].Vecs {
				for j, bat := range bats {
					downs[j] = bat.Vecs[col].GetDownstreamVector()
				}
				for i, batIdx := range from {
					srcs[i] = downs[batIdx]
				}
				if err := dst.UnionRowsFrom(srcs[:m], rows[:m], mp); err != nil {
					releaseAll()
					return nil, nil, err
				}
			}
		}
		ret[b].SetRowCount(n)
		start += n
	}
	if start < total {
		releaseAll()
		return nil, nil, moerr.NewInternalErrorNoCtxf("merge keeps %d rows, layout holds %d", total, start)
	}
	return ret, rfs, nil
}

// mapping returns the old → new map of the merge, keyed by global row id over
// srcRows rows, with -1 for every row the merge dropped. The map is allocated
// from the pool and freed by the returned function.
func (o *mergeOrder) mapping(vpool DisposableVecPool, srcRows int) ([]int64, func(), error) {
	var inv remaplist.List
	if err := inv.Init(vpool.GetMPool(), srcRows); err != nil {
		return nil, nil, err
	}
	if err := o.rows.InvertInto(srcRows, &inv); err != nil {
		inv.Free()
		return nil, nil, err
	}
	return inv.Rows(), inv.Free, nil
}
