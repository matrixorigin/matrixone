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
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	objectioutil "github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
)

const (
	DefaultInteractiveLogicalViewMaxRows  = 10_000
	DefaultInteractiveLogicalViewMaxBytes = 64 << 20
)

var errLogicalTableViewLimit = moerr.NewInternalErrorNoCtx("logical table view materialization limit reached")

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
	return r.BuildLogicalTableViewLimited(ctx, snapshotTS, dataEntries, tombEntries, 0, 0)
}

// BuildLogicalTableViewLimited materializes at most maxRows and maxBytes. Zero
// limits are unlimited. A limited result is returned with Truncated set.
func (r *CheckpointReader) BuildLogicalTableViewLimited(
	ctx context.Context,
	snapshotTS types.TS,
	dataEntries []*ObjectEntryInfo,
	tombEntries []*ObjectEntryInfo,
	maxRows int,
	maxBytes int64,
) (*LogicalTableView, error) {
	view := newLogicalTableView()
	stats, err := r.scanLogicalTable(ctx, snapshotTS, dataEntries, tombEntries,
		func(cols []objecttool.ColInfo) error {
			if len(view.Headers) != view.MetaWidth() {
				return nil
			}
			for _, col := range cols {
				view.Headers = append(view.Headers, fmt.Sprintf("col_%d", col.Idx))
				view.ColTypes = append(view.ColTypes, col.Type)
				view.ColSeqNums = append(view.ColSeqNums, col.SeqNum)
			}
			return nil
		},
		func(objShort string, blockIdx int, rowIdx int, values []string, _ []bool) error {
			if maxRows > 0 && len(view.Rows) >= maxRows {
				view.Truncated = true
				return errLogicalTableViewLimit
			}
			rowBytes := int64(len(objShort))
			for _, value := range values {
				rowBytes += int64(len(value))
			}
			if maxBytes > 0 && view.MaterializedBytes+rowBytes > maxBytes {
				view.Truncated = true
				return errLogicalTableViewLimit
			}
			row := make([]string, 0, len(values)+view.MetaWidth())
			row = append(row, objShort, fmt.Sprintf("%d", blockIdx), fmt.Sprintf("%d", rowIdx))
			row = append(row, values...)
			view.Rows = append(view.Rows, row)
			view.MaterializedBytes += rowBytes
			return nil
		},
	)
	if err != nil && !errors.Is(err, errLogicalTableViewLimit) {
		return nil, err
	}
	view.PhysicalRows = stats.PhysicalRows
	view.DeletedRows = stats.DeletedRows
	view.VisibleRows = stats.VisibleRows
	return view, nil
}

// BuildLogicalTableViewComposedLimited builds the complete selected snapshot
// (base GCKP plus all required ICKPs) before applying interactive limits.
func (r *CheckpointReader) BuildLogicalTableViewComposedLimited(
	ctx context.Context,
	tableID uint64,
	snapshotTS types.TS,
	maxRows int,
	maxBytes int64,
) (*LogicalTableView, error) {
	dataEntries, tombEntries, err := r.getTableEntriesAt(ctx, tableID, snapshotTS)
	if err != nil {
		return nil, err
	}
	return r.BuildLogicalTableViewLimited(ctx, snapshotTS, dataEntries, tombEntries, maxRows, maxBytes)
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
	canonicalCols, err := r.logicalTableColumns(ctx, visibleDataEntries)
	if err != nil {
		return stats, err
	}
	canonicalSeqNums := columnSeqNums(canonicalCols)
	if len(canonicalCols) > 0 && onColumns != nil {
		if err := onColumns(canonicalCols); err != nil {
			return stats, err
		}
	}

	for _, entry := range visibleDataEntries {
		objName := entry.ObjectStats.ObjectName().String()
		reader, err := objecttool.OpenWithFS(ctx, r.fs, objName, objName)
		if err != nil {
			if isDataFileNotFound(err) {
				return stats, moerr.NewFileNotFoundErrorf(
					ctx,
					"visible data object not found (checkpoint snapshot is incomplete): %s",
					objName,
				)
			}
			return stats, err
		}
		debugLogicalObjectColumns(entry.ObjectStats, reader)
		cols := reader.Columns()

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

			commitTSVec, releaseCommitTS, err := reader.ReadBlockCommitTS(ctx, uint32(blockIdx))
			if err != nil {
				release()
				_ = reader.Close()
				return stats, err
			}
			var commitTSs []types.TS
			if commitTSVec != nil {
				commitTSs = vector.MustFixedColWithTypeCheck[types.TS](commitTSVec)
			}

			deleteMask, err := r.buildDeleteMaskForBlock(ctx, &snapshotTS, entry.ObjectStats, uint16(blockIdx), relevantTombstones)
			if err != nil {
				if releaseCommitTS != nil {
					releaseCommitTS()
				}
				release()
				_ = reader.Close()
				return stats, err
			}

			for rowIdx := 0; rowIdx < bat.RowCount(); rowIdx++ {
				if commitTSs != nil && rowIdx < len(commitTSs) &&
					!commitTSVec.IsNull(uint64(rowIdx)) &&
					commitTSs[rowIdx].GT(&snapshotTS) {
					continue
				}
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
					if len(canonicalSeqNums) > 0 {
						values, nulls = alignRowValuesBySeqNums(cols, canonicalSeqNums, values, nulls)
					}
					if err := onRow(entry.ObjectStats.ObjectName().Short().ShortString(), blockIdx, rowIdx, values, nulls); err != nil {
						if deleteMask.IsValid() {
							deleteMask.Release()
						}
						if releaseCommitTS != nil {
							releaseCommitTS()
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
			if releaseCommitTS != nil {
				releaseCommitTS()
			}
			release()
		}

		if err := reader.Close(); err != nil {
			return stats, err
		}
	}

	return stats, nil
}

func (r *CheckpointReader) logicalTableColumns(
	ctx context.Context,
	entries []*ObjectEntryInfo,
) ([]objecttool.ColInfo, error) {
	type versionedColumn struct {
		col        objecttool.ColInfo
		createTime types.TS
	}
	bySeqNum := make(map[uint16]versionedColumn)
	for _, entry := range entries {
		objName := entry.ObjectStats.ObjectName().String()
		reader, err := objecttool.OpenWithFS(ctx, r.fs, objName, objName)
		if err != nil {
			if isDataFileNotFound(err) {
				return nil, moerr.NewFileNotFoundErrorf(
					ctx,
					"visible data object not found (checkpoint snapshot is incomplete): %s",
					objName,
				)
			}
			return nil, err
		}
		for _, col := range reader.Columns() {
			current, ok := bySeqNum[col.SeqNum]
			if !ok || current.createTime.LT(&entry.CreateTime) {
				bySeqNum[col.SeqNum] = versionedColumn{col: col, createTime: entry.CreateTime}
			}
		}
		if err := reader.Close(); err != nil {
			return nil, err
		}
	}

	seqNums := make([]int, 0, len(bySeqNum))
	for seqNum := range bySeqNum {
		seqNums = append(seqNums, int(seqNum))
	}
	sort.Ints(seqNums)
	cols := make([]objecttool.ColInfo, 0, len(seqNums))
	for idx, seqNum := range seqNums {
		col := bySeqNum[uint16(seqNum)].col
		col.Idx = uint16(idx)
		cols = append(cols, col)
	}
	return cols, nil
}

func columnSeqNums(cols []objecttool.ColInfo) []uint16 {
	seqNums := make([]uint16, len(cols))
	for i, col := range cols {
		seqNums[i] = col.SeqNum
	}
	return seqNums
}

func alignRowValuesBySeqNums(
	cols []objecttool.ColInfo,
	targetSeqNums []uint16,
	values []string,
	nulls []bool,
) ([]string, []bool) {
	if len(cols) == 0 || len(targetSeqNums) == 0 {
		return values, nulls
	}
	indexBySeqNum := make(map[uint16]int, len(cols))
	for idx, col := range cols {
		indexBySeqNum[col.SeqNum] = idx
	}
	alignedValues := make([]string, len(targetSeqNums))
	alignedNulls := make([]bool, len(targetSeqNums))
	for targetIdx, seqNum := range targetSeqNums {
		sourceIdx, ok := indexBySeqNum[seqNum]
		if !ok || sourceIdx >= len(values) {
			alignedNulls[targetIdx] = true
			continue
		}
		alignedValues[targetIdx] = values[sourceIdx]
		if sourceIdx < len(nulls) {
			alignedNulls[targetIdx] = nulls[sourceIdx]
		}
	}
	return alignedValues, alignedNulls
}

func debugLogicalObjectColumns(stats objectio.ObjectStats, reader *objecttool.ObjectReader) {
	if reader == nil {
		return
	}
	info := reader.Info()
	meta := reader.Meta()
	header := meta.BlockHeader()
	var cols strings.Builder
	for i, col := range reader.Columns() {
		if i > 0 {
			cols.WriteString(",")
		}
		cols.WriteString(fmt.Sprintf("%d->seq%d:%s", col.Idx, col.SeqNum, col.Type.Oid.String()))
	}
	ckpDebugSchemaf(
		"object columns object=%s stats_appendable=%v meta_appendable=%v blocks=%d rows=%d column_count=%d meta_column_count=%d max_seqnum=%d columns=[%s]",
		stats.ObjectName().Short().ShortString(),
		stats.GetAppendable(),
		info != nil && info.IsAppendable,
		meta.BlockCount(),
		header.Rows(),
		header.ColumnCount(),
		header.MetaColumnCount(),
		header.MaxSeqnum(),
		cols.String(),
	)
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
	if r.filterTombstonesForTest != nil {
		return r.filterTombstonesForTest(objectID, tombstoneStats)
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
	if r.buildDeleteMaskForTest != nil {
		return r.buildDeleteMaskForTest(snapshotTS, stats, blockIdx, tombstoneStats)
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
