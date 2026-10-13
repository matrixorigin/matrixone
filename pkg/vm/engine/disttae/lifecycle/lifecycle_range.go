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

package lifecycle

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// ArchiveLifecycleRange is the exact range of the archived rows, expressed in
// the frozen MO physical representation of the Lifecycle DATE/DATETIME/
// TIMESTAMP column. It is verified by full readback before Dataset publication.
type ArchiveLifecycleRange struct {
	SourceColumnID uint64
	TypeID         int32
	Min            int64
	Max            int64
}

func lifecycleRangeColumnOrdinal(
	schema SchemaDescriptor,
	value ArchiveLifecycleRange,
) (int, error) {
	for ordinal, column := range schema.Columns {
		if column.SourceColumnID != value.SourceColumnID {
			continue
		}
		if column.TypeID != value.TypeID || !column.NotNull ||
			!isLifecycleRangeType(types.T(value.TypeID)) {
			return 0, moerr.NewInternalErrorNoCtxf(
				"Lifecycle archive range column identity is invalid",
			)
		}
		return ordinal, nil
	}
	return 0, moerr.NewInternalErrorNoCtxf(
		"Lifecycle archive range column does not exist in the frozen schema",
	)
}

func isLifecycleRangeType(oid types.T) bool {
	switch oid {
	case types.T_date, types.T_datetime, types.T_timestamp:
		return true
	default:
		return false
	}
}

func lifecycleRangeCellValue(cell CanonicalCell) (int64, error) {
	if cell.Null {
		return 0, moerr.NewInternalErrorNoCtxf(
			"Lifecycle archive range column contains NULL",
		)
	}
	switch cell.Type.Oid {
	case types.T_date:
		value, ok := cell.Value.(types.Date)
		if ok {
			return int64(value), nil
		}
	case types.T_datetime:
		value, ok := cell.Value.(types.Datetime)
		if ok {
			return int64(value), nil
		}
	case types.T_timestamp:
		value, ok := cell.Value.(types.Timestamp)
		if ok {
			return int64(value), nil
		}
	}
	return 0, moerr.NewInternalErrorNoCtxf(
		"Lifecycle archive range value type %s is invalid",
		cell.Type.Oid,
	)
}

func updateLifecycleRange(
	current *ArchiveLifecycleRange,
	set *bool,
	value int64,
) {
	if !*set {
		current.Min = value
		current.Max = value
		*set = true
		return
	}
	if value < current.Min {
		current.Min = value
	}
	if value > current.Max {
		current.Max = value
	}
}

// FilterCanonicalRowsByLifecycleRange applies the user-visible half-open
// interval [start,end) to already verified canonical Archive rows. It neither
// changes row order nor reconstructs values, so the ordinary Restore write path
// receives the same canonical representation as single-Dataset Restore.
func FilterCanonicalRowsByLifecycleRange(
	ctx context.Context,
	schema SchemaDescriptor,
	lifecycleRange ArchiveLifecycleRange,
	start int64,
	end int64,
	rows [][]CanonicalCell,
) ([][]CanonicalCell, error) {
	if start >= end {
		return nil, moerr.NewInvalidInput(ctx, "Lifecycle Restore range must be non-empty")
	}
	ordinal, err := lifecycleRangeColumnOrdinal(schema, lifecycleRange)
	if err != nil {
		return nil, err
	}
	filtered := make([][]CanonicalCell, 0, len(rows))
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if ordinal >= len(row) {
			return nil, moerr.NewInternalErrorNoCtxf(
				"Lifecycle Restore row does not match the frozen schema",
			)
		}
		value, err := lifecycleRangeCellValue(row[ordinal])
		if err != nil {
			return nil, err
		}
		if value >= start && value < end {
			filtered = append(filtered, row)
		}
	}
	return filtered, nil
}

// SelectRestoreDatasetsForRange parses the user boundary against each frozen
// Dataset type, ignores non-overlapping historical generations, and rejects a
// request only when the selected interval itself crosses Lifecycle column
// generations. Dataset order is preserved for deterministic chunk ordinals.
func SelectRestoreDatasetsForRange(
	ctx context.Context,
	datasets []RestoreDataset,
	from string,
	to string,
) ([]RestoreDataset, int64, int64, error) {
	selected := make([]RestoreDataset, 0, len(datasets))
	var selectedRange ArchiveLifecycleRange
	var selectedStart int64
	var selectedEnd int64
	for _, dataset := range datasets {
		if !dataset.HasLifecycleRange {
			return nil, 0, 0, moerr.NewInternalErrorNoCtxf(
				"Lifecycle Dataset has no verified range identity",
			)
		}
		start, err := ParseLifecycleRestoreBoundary(
			ctx,
			from,
			types.T(dataset.LifecycleRange.TypeID),
		)
		if err != nil {
			return nil, 0, 0, err
		}
		end, err := ParseLifecycleRestoreBoundary(
			ctx,
			to,
			types.T(dataset.LifecycleRange.TypeID),
		)
		if err != nil {
			return nil, 0, 0, err
		}
		if start >= end {
			return nil, 0, 0, moerr.NewInvalidInput(
				ctx,
				"Lifecycle Restore range must be non-empty",
			)
		}
		if dataset.LifecycleRange.Max < start ||
			dataset.LifecycleRange.Min >= end {
			continue
		}
		if len(selected) == 0 {
			selectedRange = dataset.LifecycleRange
			selectedStart = start
			selectedEnd = end
		} else if dataset.LifecycleRange.SourceColumnID != selectedRange.SourceColumnID ||
			dataset.LifecycleRange.TypeID != selectedRange.TypeID ||
			start != selectedStart || end != selectedEnd {
			return nil, 0, 0, moerr.NewNotSupportedNoCtx(
				"Lifecycle range Restore across Lifecycle column generations",
			)
		}
		selected = append(selected, dataset)
	}
	if len(selected) == 0 {
		return nil, 0, 0, moerr.NewInvalidInput(
			ctx,
			"Lifecycle Archive has no Dataset overlapping the requested range",
		)
	}
	return selected, selectedStart, selectedEnd, nil
}
