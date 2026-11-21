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
	"slices"
)

func scopedOwnedColumns(request *engine.ReaderFilterRequest, width int) bool {
	if len(request.ColumnUses) != width {
		return false
	}
	for column := 0; column < width; column++ {
		if request.ColumnUse(column, width) == engine.ReaderColumnRetain {
			return false
		}
	}
	return true
}

// blockDataReadScopedOwned is admitted only without visibility/tombstone/RowID/
// storage-selection/TopK work. Every escaping value is owned; scoped cache bytes
// stay protected by the loader's IOVector until filtering and gathering finish.
func blockDataReadScopedOwned(ctx context.Context, info *objectio.BlockInfo, columns []uint16, typs []types.Type, early []int, policy fileservice.Policy, output *batch.Batch, mp *mpool.MPool, fs fileservice.FileService, request *engine.ReaderFilterRequest, preFilterRows *int) error {
	scratch := request.SelectionScratch(len(columns))
	defer scratch.Clear()
	scratch.Seqnums = slices.Grow(scratch.Seqnums, len(columns))
	scratch.Types = slices.Grow(scratch.Types, len(columns))
	for _, pos := range early {
		scratch.Seqnums = append(scratch.Seqnums, columns[pos])
		scratch.Types = append(scratch.Types, typs[pos])
		scratch.Sources = append(scratch.Sources, output.Vecs[pos])
	}
	destinations := scratch.Sources
	var selected engine.ReaderFilterResult
	_, err := ioutil.WithColumnsDataInto(ctx, scratch.Seqnums, scratch.Types, fs, info.MetaLocation(), destinations, request.ColumnScope(), mp, policy, func(inputs []*vector.Vector, marked []bool) error {
		// Restore original destination headers before any error crosses the scope.
		for i, pos := range early {
			output.Vecs[pos] = inputs[i]
		}
		defer func() {
			for i, pos := range early {
				output.Vecs[pos] = destinations[i]
			}
		}()
		rows := inputs[0].Length()
		for _, v := range inputs {
			if v.Length() != rows {
				return moerr.NewInvalidInputNoCtx("scoped early column row mismatch")
			}
		}
		output.SetRowCount(rows)
		if preFilterRows != nil {
			*preFilterRows = rows
		}
		if rows == 0 {
			return nil
		}
		var err error
		selected, err = request.Select(output, early)
		if err != nil {
			return err
		}
		count := rows
		if !selected.All {
			count = len(selected.Sels)
		}
		if err = validateReaderFilterResult(selected, rows, count); err != nil {
			return err
		}
		for i, pos := range early {
			destination := destinations[i]
			if request.ColumnUse(pos, len(output.Vecs)) == engine.ReaderColumnUnused {
				destination.CleanOnlyData()
				continue
			}
			if marked[i] {
				if count == 0 {
					destination.CleanOnlyData()
					continue
				}
				if selected.All {
					err = destination.UnionBatch(inputs[i], 0, rows, nil, mp)
				} else {
					err = destination.Union(inputs[i], selected.Sels, mp)
				}
				if err != nil {
					return err
				}
				inputs[i].CopyExpressionMetadataTo(destination)
			} else if !selected.All {
				if err = destination.Shrink(selected.Sels, false, mp); err != nil {
					return err
				}
			}
		}
		output.SetRowCount(count)
		if request.ObserveSelection != nil {
			request.ObserveSelection(count, 0, 0)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if output.RowCount() == 0 {
		return nil
	}
	var physicalRows []int64
	if !selected.All {
		physicalRows = selected.Sels
	}
	// The pinned early read has ended; reuse its non-owning operands for the
	// same late materialization helper, retaining only schema-sized storage.
	clear(scratch.Sources)
	scratch.Sources = scratch.Sources[:0]
	scratch.Seqnums = scratch.Seqnums[:0]
	scratch.Types = scratch.Types[:0]
	earlyIndex := 0
	for pos := range columns {
		if earlyIndex < len(early) && early[earlyIndex] == pos {
			earlyIndex++
			continue
		}
		scratch.Columns = append(scratch.Columns, pos)
		scratch.Seqnums = append(scratch.Seqnums, columns[pos])
		scratch.Types = append(scratch.Types, typs[pos])
		scratch.Sources = append(scratch.Sources, output.Vecs[pos])
	}
	if len(scratch.Columns) == 0 {
		return nil
	}
	lateOutput := batch.Batch{Vecs: scratch.Sources}
	ignored, err := materializeBlockData(ctx, info, scratch.Seqnums, scratch.Types, -1, physicalRows, nil, policy, &lateOutput, mp, fs)
	ignored.Release()
	if err != nil {
		return err
	}
	for _, pos := range scratch.Columns {
		if output.Vecs[pos].Length() != output.RowCount() {
			return moerr.NewInvalidInputNoCtx("scoped late column row mismatch")
		}
	}
	return nil
}
