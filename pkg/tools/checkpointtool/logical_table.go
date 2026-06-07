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
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	objectioutil "github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
)

type logicalTableStats struct {
	PhysicalRows int
	DeletedRows  int
	VisibleRows  int
}

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
	stats, err := r.scanLogicalTable(ctx, snapshotTS, dataEntries, tombEntries,
		func(cols []objecttool.ColInfo) error {
			if len(view.Headers) != 3 {
				return nil
			}
			for _, col := range cols {
				view.Headers = append(view.Headers, fmt.Sprintf("col_%d", col.Idx))
				view.ColTypes = append(view.ColTypes, col.Type)
			}
			return nil
		},
		func(objShort string, blockIdx int, rowIdx int, values []string, _ []bool) error {
			row := make([]string, 0, len(values)+3)
			row = append(row, objShort, fmt.Sprintf("%d", blockIdx), fmt.Sprintf("%d", rowIdx))
			row = append(row, values...)
			view.Rows = append(view.Rows, row)
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	view.PhysicalRows = stats.PhysicalRows
	view.DeletedRows = stats.DeletedRows
	view.VisibleRows = stats.VisibleRows
	return view, nil
}

func (r *CheckpointReader) scanLogicalTable(
	ctx context.Context,
	snapshotTS types.TS,
	dataEntries []*ObjectEntryInfo,
	tombEntries []*ObjectEntryInfo,
	onColumns func([]objecttool.ColInfo) error,
	onRow func(objShort string, blockIdx int, rowIdx int, values []string, nulls []bool) error,
) (logicalTableStats, error) {
	stats := logicalTableStats{}
	if len(dataEntries) == 0 {
		return stats, nil
	}

	visibleDataEntries := visibleObjectEntries(dataEntries, snapshotTS)
	visibleTombEntries := visibleObjectEntries(tombEntries, snapshotTS)
	tombstoneStats := dedupeObjectStats(visibleTombEntries)
	columnsSent := false

	for _, entry := range visibleDataEntries {
		objName := entry.ObjectStats.ObjectName().String()
		reader, err := objecttool.OpenWithFS(ctx, r.fs, objName, objName)
		if err != nil {
			if isDataFileNotFound(err) {
				continue
			}
			return stats, err
		}

		if !columnsSent && onColumns != nil {
			if err := onColumns(reader.Columns()); err != nil {
				_ = reader.Close()
				return stats, err
			}
			columnsSent = true
		}

		relevantTombstones, err := r.filterTombstonesForObject(ctx, entry.ObjectStats.ObjectName().ObjectId(), tombstoneStats)
		if err != nil {
			_ = reader.Close()
			return stats, err
		}

		for blockIdx := 0; blockIdx < int(entry.ObjectStats.BlkCnt()); blockIdx++ {
			bat, release, err := reader.ReadBlock(ctx, uint32(blockIdx))
			if err != nil {
				_ = reader.Close()
				return stats, err
			}

			if bat.RowCount() == 0 {
				release()
				continue
			}

			stats.PhysicalRows += bat.RowCount()

			deleteMask, err := r.buildDeleteMaskForBlock(ctx, &snapshotTS, entry.ObjectStats, uint16(blockIdx), relevantTombstones)
			if err != nil {
				release()
				_ = reader.Close()
				return stats, err
			}

			for rowIdx := 0; rowIdx < bat.RowCount(); rowIdx++ {
				if deleteMask.IsValid() && deleteMask.Contains(uint64(rowIdx)) {
					stats.DeletedRows++
					continue
				}
				if onRow != nil {
					values := make([]string, len(bat.Vecs))
					nulls := make([]bool, len(bat.Vecs))
					for i, vec := range bat.Vecs {
						nulls[i] = vec.IsNull(uint64(rowIdx))
						values[i] = vecValueToString(vec, rowIdx)
					}
					if err := onRow(entry.ObjectStats.ObjectName().Short().ShortString(), blockIdx, rowIdx, values, nulls); err != nil {
						if deleteMask.IsValid() {
							deleteMask.Release()
						}
						release()
						_ = reader.Close()
						return stats, err
					}
				}
				stats.VisibleRows++
			}

			if deleteMask.IsValid() {
				deleteMask.Release()
			}
			release()
		}

		if err := reader.Close(); err != nil {
			return stats, err
		}
	}

	return stats, nil
}

func dedupeObjectStats(entries []*ObjectEntryInfo) []objectio.ObjectStats {
	seen := make(map[string]struct{})
	stats := make([]objectio.ObjectStats, 0, len(entries))
	for _, entry := range entries {
		name := entry.ObjectStats.ObjectName().String()
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		stats = append(stats, entry.ObjectStats)
	}
	return stats
}

func visibleObjectEntries(entries []*ObjectEntryInfo, snapshotTS types.TS) []*ObjectEntryInfo {
	if len(entries) == 0 {
		return nil
	}

	merged := make(map[string]*ObjectEntryInfo, len(entries))
	for _, entry := range entries {
		name := entry.ObjectStats.ObjectName().String()
		existing, ok := merged[name]
		if !ok {
			copyEntry := *entry
			merged[name] = &copyEntry
			continue
		}
		if existing.CreateTime.IsEmpty() || (!entry.CreateTime.IsEmpty() && entry.CreateTime.LT(&existing.CreateTime)) {
			existing.CreateTime = entry.CreateTime
		}
		if existing.DeleteTime.IsEmpty() || (!entry.DeleteTime.IsEmpty() && existing.DeleteTime.LT(&entry.DeleteTime)) {
			existing.DeleteTime = entry.DeleteTime
		}
	}

	visible := make([]*ObjectEntryInfo, 0, len(merged))
	for _, entry := range merged {
		obj := objectio.ObjectEntry{
			ObjectStats: entry.ObjectStats,
			CreateTime:  entry.CreateTime,
			DeleteTime:  entry.DeleteTime,
		}
		if obj.Visible(snapshotTS) {
			visible = append(visible, entry)
		}
	}
	sort.Slice(visible, func(i, j int) bool {
		return visible[i].ObjectStats.ObjectName().String() < visible[j].ObjectStats.ObjectName().String()
	})
	return visible
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
