// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"errors"
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
)

// VectorLocalDOPEnabled uses exactly the planner's hint spelling, whitespace,
// invalid-entry and last-value rules. Execution compilation rechecks this gate.
func VectorLocalDOPEnabled(hints string) bool {
	value := 0
	for _, part := range strings.Split(hints, ",") {
		if key, parsed, ok := splitOptimizerHint(part); ok && key == "vectorLocalDOP" {
			value = parsed
		}
	}
	return value == 1
}

func (builder *QueryBuilder) estimateIvfScanWork(source *plan.ObjectRef, snapshot *plan.Snapshot, entriesName string, lists, probes int64) (*plan.VectorIndexScanWork, error) {
	if err := builder.GetContext().Err(); err != nil {
		return nil, err
	}
	obj, table, err := builder.compCtx.ResolveIndexTableByRef(source, entriesName, snapshot)
	if ctxErr := builder.GetContext().Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if err != nil || obj == nil || table == nil {
		return nil, scanWorkLookupError(err)
	}
	stats, err := statsForTableDef(builder.compCtx, obj, table, snapshot)
	if ctxErr := builder.GetContext().Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if err != nil {
		return nil, scanWorkLookupError(err)
	}
	return ivfScanWorkFromStats(table, stats, lists, probes), nil
}

func scanWorkLookupError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return nil
}

func ivfScanWorkFromStats(table *plan.TableDef, stats *statsinfo.StatsInfo, lists, probes int64) *plan.VectorIndexScanWork {
	if table == nil || stats == nil || lists <= 0 || !positiveScanWork(stats.TableCnt) || stats.BlockNumber <= 0 ||
		stats.AccurateObjectNumber < 0 || stats.ApproxObjectNumber < 0 {
		return nil
	}
	var vectorBytes float64
	for _, col := range table.Cols {
		if col == nil || col.Name != catalog.SystemSI_IVFFLAT_TblCol_Entries_entry {
			continue
		}
		if !types.T(col.Typ.Id).IsArrayRelate() {
			return nil
		}
		if size := stats.SizeMap[col.Name]; size > 0 {
			vectorBytes = float64(size) / stats.TableCnt
		} else if col.Typ.Width > 0 {
			vectorBytes = float64(col.Typ.Width) * float64((types.Type{Oid: types.T(col.Typ.Id)}).GetArrayElementSize())
		}
		break
	}
	if !positiveScanWork(vectorBytes) {
		return nil
	}
	fraction := math.Min(1, float64(max(int64(1), probes))/float64(lists))
	rows := stats.TableCnt * fraction
	if !positiveScanWork(rows) {
		return nil
	}
	return &plan.VectorIndexScanWork{
		Rows:              rows,
		Blocks:            int32(math.Min(math.MaxInt32, math.Max(1, math.Ceil(float64(stats.BlockNumber)*fraction)))),
		VectorBytesPerRow: vectorBytes,
		Objects:           int32(min(int64(math.MaxInt32), max(int64(0), stats.AccurateObjectNumber, stats.ApproxObjectNumber))),
	}
}

func positiveScanWork(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func vectorScanDOP(ncpu int32, spec *plan.VectorIndexScan, isPrepare bool) int32 {
	work := spec.GetScanWork()
	if work == nil || spec.FirstRoundLimit != nil || spec.BucketExpandStep > 0 ||
		!positiveScanWork(work.Rows) || !positiveScanWork(work.VectorBytesPerRow) || work.Blocks <= 0 || work.Objects < 0 {
		return 1
	}
	physical := &plan.Stats{BlockNum: work.Blocks, Rowsize: work.VectorBytesPerRow}
	dop := min(calcDOP(ncpu, physical, isPrepare), work.Blocks)
	if work.Objects > 0 {
		dop = min(dop, work.Objects)
	}
	return max(1, dop)
}
