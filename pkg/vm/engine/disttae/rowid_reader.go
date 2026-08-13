package disttae

import (
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
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
) ([][]any, error) {
	if tbl.combined.is {
		return nil, fmt.Errorf("rowid lookup is not supported for combined relations")
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
			return nil, fmt.Errorf("rowid lookup column %q not found", attr)
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
	// Prefer the partition state's versioned row entries.  This path retains
	// the exact historical row version (including rows that have since been
	// tombstoned) and avoids depending on the current block ranges.
	if pState, err := tbl.origin.getPartitionState(ctx); err == nil && pState != nil {
		iter := pState.NewRowsIter(snapshot, nil, false)
		for iter.Next() {
			entry := iter.Entry()
			if _, ok := wanted[entry.RowID]; !ok || entry.Batch == nil {
				continue
			}
			row := make([]any, len(attrs))
			valid := true
			for i, attr := range attrs {
				idx := -1
				for j, name := range entry.Batch.Attrs {
					if strings.EqualFold(name, attr) {
						idx = j
						break
					}
				}
				if idx < 0 || idx >= len(entry.Batch.Vecs) {
					valid = false
					break
				}
				row[i] = vector.GetAny(entry.Batch.Vecs[idx], int(entry.Offset), false)
			}
			if valid {
				found[entry.RowID] = row
			}
		}
		_ = iter.Close()
	}
	if len(found) == len(wanted) {
		rows := make([][]any, 0, len(rowids))
		for _, rowid := range rowids {
			rows = append(rows, found[rowid])
		}
		return rows, nil
	}
	err := ScanSnapshotWithCurrentRanges(ctx, "materialized-view-rowid-lookup", tbl, nil, snapshot, scanAttrs, scanTypes, nil, 1, mp,
		func(bat *batch.Batch) error {
			rowidVec := bat.Vecs[0]
			for i := 0; i < rowidVec.Length(); i++ {
				rowid := vector.GetFixedAtNoTypeCheck[types.Rowid](rowidVec, i)
				if _, ok := wanted[rowid]; !ok {
					continue
				}
				row := make([]any, len(attrs))
				for j := range attrs {
					row[j] = vector.GetAny(bat.Vecs[j+1], i, false)
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
			return nil, fmt.Errorf("rowid %s not found at snapshot %s", rowid.String(), snapshot.String())
		}
		rows = append(rows, row)
	}
	return rows, nil
}
