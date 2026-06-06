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

package checkpointtool

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	objectioutil "github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
)

// BuildLogicalTableView materializes a tombstone-applied logical table view.
func (r *CheckpointReader) BuildLogicalTableView(
	ctx context.Context,
	snapshotTS types.TS,
	dataEntries []*ObjectEntryInfo,
	tombEntries []*ObjectEntryInfo,
) (*LogicalTableView, error) {
	view := &LogicalTableView{
		Headers: []string{"object", "block", "row"},
		Rows:    make([][]string, 0),
	}
	if len(dataEntries) == 0 {
		return view, nil
	}

	tombstoneStats := dedupeObjectStats(tombEntries)
	for _, entry := range dataEntries {
		objName := entry.Range.ObjectStats.ObjectName().String()
		reader, err := objecttool.OpenWithFS(ctx, r.fs, objName, objName)
		if err != nil {
			return nil, err
		}

		if len(view.Headers) == 3 {
			cols := reader.Columns()
			for _, col := range cols {
				view.Headers = append(view.Headers, fmt.Sprintf("col_%d", col.Idx))
			}
		}

		relevantTombstones, err := r.filterTombstonesForObject(ctx, entry.Range.ObjectStats.ObjectName().ObjectId(), tombstoneStats)
		if err != nil {
			_ = reader.Close()
			return nil, err
		}

		startBlock := int(entry.Range.Start.GetBlockOffset())
		endBlock := int(entry.Range.End.GetBlockOffset())
		for blockIdx := startBlock; blockIdx <= endBlock; blockIdx++ {
			bat, release, err := reader.ReadBlock(ctx, uint32(blockIdx))
			if err != nil {
				_ = reader.Close()
				return nil, err
			}

			startRow := 0
			endRow := bat.RowCount() - 1
			if blockIdx == startBlock {
				startRow = int(entry.Range.Start.GetRowOffset())
			}
			if blockIdx == endBlock {
				endRow = int(entry.Range.End.GetRowOffset())
			}
			if startRow < 0 {
				startRow = 0
			}
			if endRow >= bat.RowCount() {
				endRow = bat.RowCount() - 1
			}
			if startRow > endRow || startRow >= bat.RowCount() {
				release()
				continue
			}

			view.PhysicalRows += endRow - startRow + 1

			deleteMask, err := r.buildDeleteMaskForBlock(ctx, &snapshotTS, entry.Range.ObjectStats, uint16(blockIdx), relevantTombstones)
			if err != nil {
				release()
				_ = reader.Close()
				return nil, err
			}

			for rowIdx := startRow; rowIdx <= endRow; rowIdx++ {
				if deleteMask.IsValid() && deleteMask.Contains(uint64(rowIdx)) {
					view.DeletedRows++
					continue
				}
				row := make([]string, 0, len(bat.Vecs)+3)
				row = append(row,
					entry.Range.ObjectStats.ObjectName().Short().ShortString(),
					fmt.Sprintf("%d", blockIdx),
					fmt.Sprintf("%d", rowIdx),
				)
				for _, vec := range bat.Vecs {
					row = append(row, vecValueToString(vec, rowIdx))
				}
				view.Rows = append(view.Rows, row)
			}

			if deleteMask.IsValid() {
				deleteMask.Release()
			}
			release()
		}

		if err := reader.Close(); err != nil {
			return nil, err
		}
	}

	view.VisibleRows = len(view.Rows)
	return view, nil
}

func dedupeObjectStats(entries []*ObjectEntryInfo) []objectio.ObjectStats {
	seen := make(map[string]struct{})
	stats := make([]objectio.ObjectStats, 0, len(entries))
	for _, entry := range entries {
		name := entry.Range.ObjectStats.ObjectName().String()
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		stats = append(stats, entry.Range.ObjectStats)
	}
	return stats
}

func (r *CheckpointReader) filterTombstonesForObject(
	ctx context.Context,
	objectID *objectio.ObjectId,
	tombstoneStats []objectio.ObjectStats,
) ([]objectio.ObjectStats, error) {
	if len(tombstoneStats) == 0 {
		return nil, nil
	}
	selection, err := objectioutil.FindTombstonesOfObject(ctx, objectID, tombstoneStats, r.fs)
	if err != nil {
		return nil, err
	}
	relevant := make([]objectio.ObjectStats, 0, selection.Count())
	iter := selection.Iterator()
	for iter.HasNext() {
		idx := iter.Next()
		if idx >= uint64(len(tombstoneStats)) {
			return nil, moerr.NewInternalErrorf(ctx, "tombstone selection index out of range: %d", idx)
		}
		relevant = append(relevant, tombstoneStats[int(idx)])
	}
	return relevant, nil
}

func (r *CheckpointReader) buildDeleteMaskForBlock(
	ctx context.Context,
	snapshotTS *types.TS,
	stats objectio.ObjectStats,
	blockIdx uint16,
	tombstoneStats []objectio.ObjectStats,
) (objectio.Bitmap, error) {
	if len(tombstoneStats) == 0 {
		return objectio.NullBitmap, nil
	}

	mask := objectio.GetReusableBitmap()
	blockID := stats.ConstructBlockId(blockIdx)
	current := 0
	getTombstone := func() (*objectio.ObjectStats, error) {
		if current >= len(tombstoneStats) {
			return nil, nil
		}
		stat := &tombstoneStats[current]
		current++
		return stat, nil
	}
	if err := objectioutil.GetTombstonesByBlockId(ctx, snapshotTS, &blockID, getTombstone, &mask, r.fs); err != nil {
		mask.Release()
		return objectio.NullBitmap, err
	}
	return mask, nil
}
