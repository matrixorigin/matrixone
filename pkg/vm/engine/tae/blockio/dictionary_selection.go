// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package blockio

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// publishReaderSelection commits all loaded column headers together. A declined
// pin uses the already computed rows for an owned gather, never another Apply.
func publishReaderSelection(output *batch.Batch, positions []int, rows []int64, request *engine.ReaderFilterRequest, mp *mpool.MPool) (retErr error) {
	if len(rows) == 0 {
		for _, pos := range positions {
			output.Vecs[pos].CleanOnlyData()
		}
		output.SetRowCount(0)
		return nil
	}
	sources := make([]*vector.Vector, len(positions))
	selected := make([]*vector.Vector, len(positions))
	for i, pos := range positions {
		sources[i] = output.Vecs[pos]
	}
	defer func() {
		if retErr != nil {
			for _, vec := range selected {
				if vec != nil {
					vec.Free(mp)
				}
			}
		}
	}()
	var reuse []*vector.Vector
	if len(request.ReusableHeaders) == len(output.Vecs) {
		reuse = make([]*vector.Vector, len(positions))
		for i, pos := range positions {
			reuse[i] = request.ReusableHeaders[pos]
			request.ReusableHeaders[pos] = nil
		}
		defer func() {
			for i, pos := range positions {
				request.ReusableHeaders[pos] = reuse[i]
			}
		}()
	}
	var snapshots []*vector.DictionaryVectorSlot
	if len(request.SnapshotSlots) == len(output.Vecs) {
		snapshots = make([]*vector.DictionaryVectorSlot, len(positions))
		for i, pos := range positions {
			snapshots[i] = request.SnapshotSlots[pos]
		}
	}
	admitted, err := vector.TrySelectDictionaryVectorsWithWorkspace(sources, selected, rows, output.RowCount(), mp, request.OutputAllocation, reuse, snapshots, request.SelectionSlot)
	if err != nil {
		return err
	}
	if !admitted {
		for i, source := range sources {
			selected[i], err = vector.NewOffHeapVecWithTypeAndAllocation(*source.GetType(), request.OutputAllocation)
			if err != nil {
				return err
			}
			if err = selected[i].Union(source, rows, mp); err != nil {
				return err
			}
			source.CopyExpressionMetadataTo(selected[i])
		}
	}
	columns := 0
	var logical int64
	for i, pos := range positions {
		if selected[i].IsDictionary() {
			columns++
			logical += int64(selected[i].Size())
		}
		output.Vecs[pos] = selected[i]
		if pos < len(request.Workspace) {
			request.Workspace[pos] = sources[i]
		} else {
			sources[i].Free(mp)
		}
	}
	output.SetRowCount(len(rows))
	if request.ObserveSelection != nil {
		request.ObserveSelection(len(rows), columns, logical)
	}
	return nil
}
