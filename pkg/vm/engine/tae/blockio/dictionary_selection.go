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

	scratch := request.SelectionScratch(len(positions))
	defer scratch.Clear()
	width := len(output.Vecs)
	retain := true
	for _, pos := range positions {
		source := output.Vecs[pos]
		use := request.ColumnUse(pos, width)
		if use == engine.ReaderColumnUnused {
			continue
		}
		if use == engine.ReaderColumnOwned {
			if !source.IsDictionary() && !source.HasBorrowedBacking() && source.CanDetach(vector.BackingData) && source.CanDetach(vector.BackingArea) {
				continue
			}
			// An owned destination must not gain a borrowed dictionary header.
			// Use the existing owned fallback for this exceptional mixed batch.
			retain = false
		}
		scratch.Columns = append(scratch.Columns, pos)
		scratch.Sources = append(scratch.Sources, source)
		scratch.Vectors = append(scratch.Vectors, nil)
	}
	sources, selected := scratch.Sources, scratch.Vectors
	defer func() {
		if retErr != nil {
			for _, v := range selected {
				if v != nil {
					v.Free(mp)
				}
			}
		}
	}()
	var reuse []*vector.Vector
	if len(request.ReusableHeaders) == width {
		for _, pos := range scratch.Columns {
			scratch.Headers = append(scratch.Headers, request.ReusableHeaders[pos])
			request.ReusableHeaders[pos] = nil
		}
		reuse = scratch.Headers
		defer func() {
			for i, pos := range scratch.Columns {
				request.ReusableHeaders[pos] = reuse[i]
			}
		}()
	}
	var snapshots []*vector.DictionaryVectorSlot
	if len(request.SnapshotSlots) == width {
		for _, pos := range scratch.Columns {
			scratch.Snapshots = append(scratch.Snapshots, request.SnapshotSlots[pos])
		}
		snapshots = scratch.Snapshots
	}
	if len(sources) > 0 {
		admitted := false
		var err error
		if retain {
			admitted, err = vector.TrySelectDictionaryVectorsWithWorkspace(sources, selected, rows, output.RowCount(), mp, request.OutputAllocation, reuse, snapshots, request.SelectionSlot)
			if err != nil {
				return err
			}
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
	}
	// Classification was made before construction. Aliases can gain leases during
	// construction; Shrink's existing COW admission still protects those snapshots.
	nextRetained := 0
	for _, pos := range positions {
		if nextRetained < len(scratch.Columns) && scratch.Columns[nextRetained] == pos {
			nextRetained++
			continue
		}
		source := output.Vecs[pos]
		if request.ColumnUse(pos, width) == engine.ReaderColumnUnused {
			continue
		}
		if err := source.Shrink(rows, false, mp); err != nil {
			return err
		}
	}
	for _, pos := range positions {
		if request.ColumnUse(pos, width) == engine.ReaderColumnUnused {
			output.Vecs[pos].CleanOnlyData()
		}
	}
	columns := 0
	var logical int64
	for i, pos := range scratch.Columns {
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
