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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// publishReaderSelection compacts the selected rows of each read column in
// place. Columns the caller marked unused are cleared instead.
func publishReaderSelection(output *batch.Batch, positions []int, rows []int64, request *engine.ReaderFilterRequest, mp *mpool.MPool) error {
	width := len(output.Vecs)
	for _, pos := range positions {
		vec := output.Vecs[pos]
		if len(rows) == 0 || request.ColumnUse(pos, width) == engine.ReaderColumnUnused {
			vec.CleanOnlyData()
			continue
		}
		if vec.IsReadOnly() {
			// A column borrowed from the cache cannot shrink in place: publish
			// an owned copy of the selected rows instead.
			owner := mp
			if request != nil && request.OutputAllocation != nil {
				owner = request.OutputAllocation
			}
			owned := vector.NewVec(*vec.GetType(), owner)
			if err := owned.Union(vec, rows, owner); err != nil {
				owned.Free(owner)
				return err
			}
			owned.SetSorted(vec.GetSorted())
			vec.Free(nil)
			output.Vecs[pos] = owned
			continue
		}
		if err := vec.Shrink(rows, false, vec.MPool()); err != nil {
			return err
		}
	}
	output.SetRowCount(len(rows))
	return nil
}

// publishBatchSelection keeps the read columns' physical rows and publishes
// rows as output's selection, in the caller-owned request.PublishSelection.
// Columns the caller marked unused are cleared.
func publishBatchSelection(output *batch.Batch, positions []int, rows []int64, physicalRows int, request *engine.ReaderFilterRequest) {
	width := len(output.Vecs)
	for _, pos := range positions {
		if request.ColumnUse(pos, width) == engine.ReaderColumnUnused {
			output.Vecs[pos].CleanOnlyData()
		}
	}
	sel := request.PublishSelection
	sel.InitWithRows(int64(physicalRows), rows)
	output.SetSelection(sel)
}

// loadBorrowedLateColumns loads every column not read for the filter in
// full, as read-only references pinned by output, so they stay aligned with
// the early columns' physical rows.
func loadBorrowedLateColumns(
	ctx context.Context,
	info *objectio.BlockInfo,
	columns []uint16,
	colTypes []types.Type,
	early []int,
	policy fileservice.Policy,
	output *batch.Batch,
	mp *mpool.MPool,
	fs fileservice.FileService,
	request *engine.ReaderFilterRequest,
) error {
	late := columnPositionsComplement(early, len(columns))
	if len(late) == 0 {
		return nil
	}
	seqnums := make([]uint16, len(late))
	typs := make([]types.Type, len(late))
	dest := make([]*vector.Vector, len(late))
	for i, pos := range late {
		seqnums[i], typs[i], dest[i] = columns[pos], colTypes[pos], output.Vecs[pos]
	}
	if _, err := ioutil.LoadColumnsDataIntoBorrowed(ctx, seqnums, typs, fs, info.MetaLocation(), dest, mp, policy,
		request.BorrowColumns(late, len(output.Vecs)), output); err != nil {
		return err
	}
	physical := output.PhysicalRowCount()
	for _, pos := range late {
		if output.Vecs[pos].Length() != physical {
			return moerr.NewInvalidInputNoCtxf("late column %d has %d rows for %d physical rows",
				pos, output.Vecs[pos].Length(), physical)
		}
	}
	return nil
}
