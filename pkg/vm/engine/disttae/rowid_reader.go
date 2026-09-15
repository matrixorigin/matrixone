// Copyright 2026 Matrix Origin
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

package disttae

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
)

var _ engine.RowIDReader = (*txnTableDelegate)(nil)

// ReadRowsByRowID reads the requested source rows at snapshot. The scan is
// served by disttae's partition-state backed snapshot reader, so committed
// in-memory rows and persisted blocks use the same historical visibility.
// The current implementation scans the snapshot and filters rowids in the
// reader callback; it deliberately keeps the engine boundary independent of
// the private PartitionState representation.
func (tbl *txnTableDelegate) ReadRowsByRowID(
	ctx context.Context,
	rowids []types.Rowid,
	snapshot types.TS,
	attrs []string,
	mp *mpool.MPool,
	budget *engine.RowIDReadBudget,
) ([][]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(rowids) > engine.MaxRowIDReadRows {
		return nil, engine.ErrRowIDReadLimit
	}
	if tbl.combined.is {
		return nil, moerr.NewInternalErrorNoCtx("rowid lookup is not supported for combined relations")
	}
	if len(rowids) == 0 {
		return nil, nil
	}
	if mp == nil {
		mp = tbl.origin.proc.Load().Mp()
	}

	def := tbl.origin.GetTableDef(ctx)
	colTypes := make([]types.Type, 0, len(attrs)+1)
	for _, attr := range attrs {
		idx, ok := def.Name2ColIndex[attr]
		if !ok {
			idx, ok = def.Name2ColIndex[strings.ToLower(attr)]
		}
		if !ok || idx < 0 || int(idx) >= len(def.Cols) {
			return nil, moerr.NewInternalErrorNoCtxf("rowid lookup column %q not found", attr)
		}
		colTypes = append(colTypes, plan2.ExprType2Type(&def.Cols[idx].Typ))
	}
	// The physical rowid is returned first and is not part of TableDef.Cols.
	scanAttrs := append([]string{catalog.Row_ID}, attrs...)
	scanTypes := append([]types.Type{types.T_Rowid.ToType()}, colTypes...)
	wanted := make(map[types.Rowid]struct{}, len(rowids))
	for _, rowid := range rowids {
		wanted[rowid] = struct{}{}
	}
	found := make(map[types.Rowid][]any, len(rowids))
	// Use the same immutable partition state for row versions and disk ranges.
	pState, err := tbl.origin.getPartitionState(ctx)
	if err != nil {
		return nil, err
	}
	if err = func() error {
		iter := pState.NewRowsIter(snapshot, nil, false)
		defer iter.Close()
		for iter.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			entry := iter.Entry()
			if _, ok := wanted[entry.RowID]; !ok || entry.Batch == nil {
				continue
			}
			if _, ok := found[entry.RowID]; ok {
				continue
			}
			vecs := make([]*vector.Vector, len(attrs))
			for i, attr := range attrs {
				for j, name := range entry.Batch.Attrs {
					if strings.EqualFold(name, attr) && j < len(entry.Batch.Vecs) {
						vecs[i] = entry.Batch.Vecs[j]
						break
					}
				}
				if vecs[i] == nil {
					return moerr.NewInternalErrorNoCtxf("historical row is missing column %q", attr)
				}
			}
			row, err := copyRowIDReaderRow(vecs, int(entry.Offset), budget)
			if err != nil {
				return err
			}
			found[entry.RowID] = row
		}
		return nil
	}(); err != nil {
		return nil, err
	}

	if len(found) == len(wanted) {
		rows := make([][]any, 0, len(rowids))
		for _, rowid := range rowids {
			rows = append(rows, found[rowid])
		}
		return rows, nil
	}
	// A nil range list scans memory only. Build ranges from objects visible at
	// the historical snapshot, including objects retired by a later merge.
	// Only blocks containing requested rowids enter the bounded range list.
	blocks := make(map[types.Blockid]bool, len(wanted))
	for id := range wanted {
		if _, ok := found[id]; !ok {
			blocks[id.CloneBlockID()] = true
		}
	}
	ranges := readutil.NewBlockListRelationData(0, readutil.WithPartitionState(pState))
	if err = func() error {
		iter, err := pState.NewObjectsIter(snapshot, true, false)
		if err != nil {
			return err
		}
		defer iter.Close()
		for iter.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			obj := iter.Entry()
			var meta objectio.ObjectDataMeta
			if obj.Rows() == 0 {
				loc := obj.ObjectLocation()
				loaded, err := objectio.FastLoadObjectMeta(ctx, &loc, false, tbl.origin.getTxn().engine.fs)
				if err != nil {
					return err
				}
				meta = loaded.MustDataMeta()
			}
			objectio.ForeachBlkInObjStatsList(true, meta, func(blk objectio.BlockInfo, _ objectio.BlockObject) bool {
				if blocks[blk.BlockID] {
					blk.SetFlagByObjStats(&obj.ObjectStats)
					ranges.AppendBlockInfo(&blk)
					delete(blocks, blk.BlockID)
				}
				return true
			}, obj.ObjectStats)
		}
		return nil
	}(); err != nil {
		return nil, err
	}
	err = ScanSnapshotWithCurrentRanges(ctx, "materialized-view-rowid-lookup", tbl, ranges, snapshot, scanAttrs, scanTypes, nil, 1, mp,
		func(bat *batch.Batch) error {
			rowidVec := bat.Vecs[0]
			for i := 0; i < rowidVec.Length(); i++ {
				if err := ctx.Err(); err != nil {
					return err
				}
				rowid := vector.GetFixedAtNoTypeCheck[types.Rowid](rowidVec, i)
				if _, ok := wanted[rowid]; !ok {
					continue
				}
				if _, ok := found[rowid]; ok {
					continue
				}
				row, err := copyRowIDReaderRow(bat.Vecs[1:], i, budget)
				if err != nil {
					return err
				}
				found[rowid] = row
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	rows := make([][]any, 0, len(rowids))
	for _, rowid := range rowids {
		row, ok := found[rowid]
		if !ok {
			return nil, moerr.NewInternalErrorNoCtxf("rowid %s not found at snapshot %s", rowid.String(), snapshot.String())
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func rowIDReaderValue(vec *vector.Vector, row int) any {
	if vec == nil || vec.IsConstNull() || vec.GetNulls().Contains(uint64(row)) {
		return nil
	}
	if vec.IsConst() {
		row = 0
	}
	return vector.GetAny(vec, row, true)
}

// Include map/slice/interface overhead as well as variable-width payloads.
// Charge before allocating the result or copying a vector's borrowed bytes.
func copyRowIDReaderRow(vecs []*vector.Vector, row int, budget *engine.RowIDReadBudget) ([]any, error) {
	bytes := 128 + 128*len(vecs)
	for _, vec := range vecs {
		if vec == nil || vec.IsConstNull() {
			continue
		}
		offset := row
		if vec.IsConst() {
			offset = 0
		}
		if !vec.GetNulls().Contains(uint64(offset)) && vec.GetType().IsVarlen() {
			size := len(vec.GetBytesAt(offset))
			if size > engine.MaxRowIDReadBytes/2 {
				return nil, engine.ErrRowIDReadLimit
			}
			bytes += 2 * size
		}
	}
	if err := budget.Charge(bytes); err != nil {
		return nil, err
	}
	result := make([]any, len(vecs))
	for i, vec := range vecs {
		result[i] = rowIDReaderValue(vec, row)
	}
	return result, nil
}
