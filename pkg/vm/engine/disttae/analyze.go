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
	"errors"
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pbstats "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	analyzestats "github.com/matrixorigin/matrixone/pkg/statistics/analyze"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

const (
	// Incidence aggregation sees every row in the selected blocks. This bound
	// can represent the worst-case all-distinct contents of the minimum spatial
	// sample while ColumnsPerPass=1 keeps the peak independent of table width.
	defaultAnalyzeDistinctValues = uint64(objectio.BlockMaxRows) * analyzestats.DefaultMinBlocks
	defaultAnalyzeColumnsPerPass = uint32(1)
	analyzeCoverageV1            = "SNAPSHOT_VISIBLE_V1"
)

type analyzeSelectedRange struct {
	block              objectio.BlockInfo
	rowThreshold       [16]byte
	rowThresholdAll    bool
	incidenceThreshold [16]byte
	incidenceAll       bool
}

type analyzeColumnState struct {
	name        string
	typ         types.Type
	ndv         *analyzestats.NDVAccumulator
	sampleNulls uint64
	sampleBytes uint64
	canonical   []byte
	zoneMap     index.ZM
}

var _ engine.AnalyzableRelation = (*txnTable)(nil)
var _ engine.AnalyzableRelation = (*txnTableDelegate)(nil)

func (tbl *txnTableDelegate) AnalyzeTable(
	ctx context.Context,
	request engine.AnalyzeTableRequest,
) (*engine.AnalyzeTableResult, error) {
	if tbl.combined.is {
		return nil, moerr.NewNotSupported(ctx, "manual ANALYZE partition execution is not enabled")
	}
	local, err := tbl.isLocal()
	if err != nil {
		return nil, err
	}
	if !local {
		return nil, moerr.NewNotSupported(ctx, "manual ANALYZE remote shard execution is not enabled")
	}
	return tbl.origin.AnalyzeTable(ctx, request)
}

// AnalyzeTable reads only the admitted physical ranges and computes statistics
// from visible values. It does not publish: the caller owns the all-or-nothing
// publication boundary after every selected column group succeeds.
func (tbl *txnTable) AnalyzeTable(
	ctx context.Context,
	request engine.AnalyzeTableRequest,
) (*engine.AnalyzeTableResult, error) {
	proc, ok := request.Process.(*process.Process)
	if !ok || proc == nil {
		proc = tbl.proc.Load()
	}
	if proc == nil {
		return nil, moerr.NewInternalErrorNoCtx("ANALYZE requires an execution process")
	}
	columns, err := resolveAnalyzeColumns(tbl.GetTableDef(ctx), request.Columns)
	if err != nil {
		return nil, err
	}
	applyAnalyzeDefaults(&request)

	populationRows, err := tbl.StarCount(ctx)
	if err != nil {
		return nil, err
	}
	ranges, err := tbl.Ranges(ctx, engine.RangesParam{
		PreAllocBlocks:     int(min(request.MaxBlocks, uint64(math.MaxInt))),
		TxnOffset:          0,
		Policy:             engine.Policy_CollectAllData,
		DontSupportRelData: false,
	})
	if err != nil {
		return nil, err
	}
	selected, populationBlocks, q, qBlocks, err := selectAnalyzeRanges(
		ranges.GetBlockInfoSlice(), populationRows, request)
	if err != nil {
		return nil, err
	}

	selectedData := ranges.BuildEmptyRelData(len(selected))
	selectedBlocks := make([]objectio.Blockid, 0, len(selected))
	for i := range selected {
		selectedData.AppendBlockInfo(&selected[i].block)
		if !selected[i].block.IsMemBlk() {
			selectedBlocks = append(selectedBlocks, selected[i].block.BlockID)
		}
	}
	var tombstones engine.Tombstoner
	if request.FullScan {
		tombstones, err = tbl.CollectTombstones(ctx, 0, engine.Policy_CollectAllTombstones)
	} else {
		tombstones, err = tbl.collectTombstones(
			ctx, 0, engine.Policy_CollectAllTombstones, selectedBlocks)
	}
	if err != nil {
		return nil, err
	}
	if err = selectedData.AttachTombstones(tombstones); err != nil {
		return nil, err
	}

	stats := plan2.NewStatsInfo()
	stats.TableName = tbl.tableName
	stats.TableCnt = float64(populationRows)
	stats.BlockNumber = int64(populationBlocks)
	result := &engine.AnalyzeTableResult{
		Stats:             stats,
		Mode:              "AUTO",
		Coverage:          analyzeCoverageV1,
		PopulationRows:    populationRows,
		PopulationExact:   true,
		PopulationBlocks:  populationBlocks,
		SampleBlocks:      uint64(len(selected)),
		ColumnsAnalyzed:   uint32(len(columns)),
		SampleNumerator:   q.Numerator,
		SampleDenominator: q.Denominator,
	}
	if request.FullScan {
		result.Mode = "FULLSCAN"
	}

	columnsPerPass := int(request.ColumnsPerPass)
	for first := 0; first < len(columns); first += columnsPerPass {
		last := min(first+columnsPerPass, len(columns))
		states := make([]analyzeColumnState, last-first)
		for i, column := range columns[first:last] {
			ndv := analyzestats.NewNDVAccumulator(request.MaxDistinctValues)
			if request.FullScan {
				ndv = analyzestats.NewFullScanNDVAccumulator()
			}
			states[i] = analyzeColumnState{
				name: column.Name,
				typ: types.New(
					types.T(column.Typ.Id), column.Typ.Width, column.Typ.Scale),
				ndv: ndv,
			}
			states[i].zoneMap = index.NewZM(states[i].typ.Oid, states[i].typ.Scale)
		}
		passRows, passBytes, err := tbl.scanAnalyzeColumnGroup(
			ctx, proc, selectedData, selected, states, request.Seed)
		if err != nil {
			return nil, err
		}
		if first == 0 {
			result.SampleRows = passRows
		} else if result.SampleRows != passRows {
			return nil, moerr.NewInternalErrorNoCtxf(
				"ANALYZE snapshot changed between column groups: retained rows %d and %d",
				result.SampleRows, passRows)
		}
		if math.MaxUint64-result.SampleBytes < passBytes {
			return nil, moerr.NewInternalErrorNoCtx("ANALYZE sample byte counter overflow")
		}
		result.SampleBytes += passBytes
		if err = finalizeAnalyzeColumns(
			stats, states, populationRows, passRows, qBlocks,
			q.Numerator == q.Denominator); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func applyAnalyzeDefaults(request *engine.AnalyzeTableRequest) {
	defaults := analyzestats.DefaultSampleConfig()
	if request.TargetRows == 0 {
		request.TargetRows = defaults.TargetRows
	}
	if request.MaxBlocks == 0 {
		request.MaxBlocks = defaults.MaxBlocks
	}
	if request.MinBlocks == 0 {
		request.MinBlocks = defaults.MinBlocks
	}
	if request.MaxStrata == 0 {
		request.MaxStrata = defaults.MaxStrata
	}
	if request.MaxDistinctValues == 0 {
		request.MaxDistinctValues = defaultAnalyzeDistinctValues
	}
	if request.ColumnsPerPass == 0 {
		request.ColumnsPerPass = defaultAnalyzeColumnsPerPass
	}
}

func resolveAnalyzeColumns(tableDef *plan.TableDef, requested []string) ([]*plan.ColDef, error) {
	if tableDef == nil || len(requested) == 0 {
		return nil, moerr.NewInvalidInputNoCtx("ANALYZE requires at least one column")
	}
	columns := make([]*plan.ColDef, 0, len(requested))
	seen := make(map[uint32]struct{}, len(requested))
	for _, name := range requested {
		var matched *plan.ColDef
		for _, column := range tableDef.Cols {
			if column.Hidden {
				continue
			}
			if strings.EqualFold(column.Name, name) || strings.EqualFold(column.OriginName, name) {
				matched = column
				break
			}
		}
		if matched == nil {
			return nil, moerr.NewInvalidInputNoCtxf("column %s does not exist", name)
		}
		if _, duplicate := seen[matched.Seqnum]; duplicate {
			return nil, moerr.NewInvalidInputNoCtxf("duplicate ANALYZE column %s", name)
		}
		seen[matched.Seqnum] = struct{}{}
		columns = append(columns, matched)
	}
	return columns, nil
}

func selectAnalyzeRanges(
	all objectio.BlockInfoSlice,
	populationRows uint64,
	request engine.AnalyzeTableRequest,
) ([]analyzeSelectedRange, uint64, analyzestats.Fraction, analyzestats.Fraction, error) {
	var memory *objectio.BlockInfo
	persisted := make([]objectio.BlockInfo, 0, all.Len())
	for i := 0; i < all.Len(); i++ {
		block := *all.Get(i)
		if block.IsMemBlk() {
			if memory == nil {
				memory = &block
			}
			continue
		}
		persisted = append(persisted, block)
	}
	populationBlocks := uint64(len(persisted))
	one := analyzestats.MustFraction(1, 1)
	if populationBlocks == 0 {
		selected := make([]analyzeSelectedRange, 0, 1)
		if memory != nil {
			q := one
			if !request.FullScan && populationRows > request.TargetRows {
				q = analyzestats.MustFraction(request.TargetRows, populationRows)
			}
			rowThreshold, rowAll, err := q.Threshold128()
			if err != nil {
				return nil, 0, analyzestats.Fraction{}, analyzestats.Fraction{}, err
			}
			selected = append(selected, analyzeSelectedRange{
				block: *memory, rowThreshold: rowThreshold,
				rowThresholdAll: rowAll, incidenceAll: true,
			})
			return selected, 0, q, one, nil
		}
		return selected, 0, one, one, nil
	}

	if request.FullScan {
		selected := make([]analyzeSelectedRange, 0, len(persisted)+1)
		if memory != nil {
			selected = append(selected, analyzeSelectedRange{
				block: *memory, rowThresholdAll: true, incidenceAll: true,
			})
		}
		for i := range persisted {
			selected = append(selected, analyzeSelectedRange{
				block: persisted[i], rowThresholdAll: true, incidenceAll: true,
			})
		}
		return selected, populationBlocks, one, one, nil
	}

	samplePlan, err := analyzestats.PlanBlockSample(
		populationRows,
		populationBlocks,
		analyzestats.SampleConfig{
			TargetRows: request.TargetRows,
			MinBlocks:  request.MinBlocks,
			MaxBlocks:  request.MaxBlocks,
			MaxStrata:  request.MaxStrata,
		},
		request.Seed,
	)
	if err != nil {
		return nil, 0, analyzestats.Fraction{}, analyzestats.Fraction{}, err
	}
	selected := make([]analyzeSelectedRange, 0, len(samplePlan.Blocks)+1)
	if memory != nil {
		rowThreshold, rowAll, thresholdErr := samplePlan.Q.Threshold128()
		if thresholdErr != nil {
			return nil, 0, analyzestats.Fraction{}, analyzestats.Fraction{}, thresholdErr
		}
		incidenceThreshold, incidenceAll, thresholdErr := samplePlan.QBlocks.Threshold128()
		if thresholdErr != nil {
			return nil, 0, analyzestats.Fraction{}, analyzestats.Fraction{}, thresholdErr
		}
		selected = append(selected, analyzeSelectedRange{
			block:        *memory,
			rowThreshold: rowThreshold, rowThresholdAll: rowAll,
			incidenceThreshold: incidenceThreshold, incidenceAll: incidenceAll,
		})
	}
	for _, sampled := range samplePlan.Blocks {
		selected = append(selected, analyzeSelectedRange{
			block:              persisted[sampled.LogicalOrdinal],
			rowThreshold:       sampled.RowThreshold,
			rowThresholdAll:    sampled.RowThresholdAll,
			incidenceThreshold: sampled.IncidenceThreshold,
			incidenceAll:       sampled.IncidenceAll,
		})
	}
	return selected, populationBlocks, samplePlan.Q, samplePlan.QBlocks, nil
}

func (tbl *txnTable) scanAnalyzeColumnGroup(
	ctx context.Context,
	proc *process.Process,
	selectedData engine.RelData,
	selected []analyzeSelectedRange,
	states []analyzeColumnState,
	seed [32]byte,
) (retainedRows, readBytes uint64, err error) {
	attrs := make([]string, 0, len(states)+1)
	attrTypes := make([]types.Type, 0, len(states)+1)
	for i := range states {
		attrs = append(attrs, states[i].name)
		attrTypes = append(attrTypes, states[i].typ)
	}
	attrs = append(attrs, catalog.Row_ID)
	attrTypes = append(attrTypes, objectio.RowidType)

	data := batch.NewWithSchema(true, attrs, attrTypes)
	defer data.Clean(proc.Mp())
	for rangeIndex := range selected {
		descriptor := selected[rangeIndex]
		incidence := analyzestats.RetainIncidenceBlock(
			seed,
			descriptor.block.BlockID[:],
			descriptor.incidenceThreshold,
			descriptor.incidenceAll,
		)
		if incidence {
			for i := range states {
				if beginErr := states[i].ndv.BeginIncidenceBlock(); beginErr != nil {
					return 0, 0, beginErr
				}
			}
		}
		readers, buildErr := tbl.BuildReaders(
			ctx,
			proc,
			nil,
			selectedData.DataSlice(rangeIndex, rangeIndex+1),
			1,
			0,
			false,
			engine.Policy_CheckAll,
			engine.FilterHint{},
		)
		if buildErr != nil {
			return 0, 0, buildErr
		}
		if len(readers) != 1 {
			for _, opened := range readers {
				_ = opened.Close()
			}
			return 0, 0, moerr.NewInternalErrorNoCtxf(
				"ANALYZE expected one reader, got %d", len(readers))
		}
		reader := readers[0]
		for {
			data.CleanOnlyData()
			end, readErr := reader.Read(ctx, attrs, nil, proc.Mp(), data)
			if readErr != nil {
				_ = reader.Close()
				return 0, 0, readErr
			}
			if end {
				break
			}
			if data.Size() > 0 {
				if math.MaxUint64-readBytes < uint64(data.Size()) {
					_ = reader.Close()
					return 0, 0, moerr.NewInternalErrorNoCtx("ANALYZE sample byte counter overflow")
				}
				readBytes += uint64(data.Size())
			}
			rowIDs := data.Vecs[len(states)]
			for row := 0; row < data.RowCount(); row++ {
				identity := rowIDs.GetRawBytesAt(row)
				retainRow := analyzestats.RetainRow(
					seed, identity, descriptor.rowThreshold, descriptor.rowThresholdAll)
				if retainRow {
					retainedRows++
				}
				for columnIndex := range states {
					valueVector := data.Vecs[columnIndex]
					isNull := valueVector.IsNull(uint64(row))
					var raw []byte
					if !isNull {
						raw = valueVector.GetRawBytesAt(row)
					}
					if retainRow {
						// StatsInfo.SizeMap is consumed as the uncompressed
						// vector footprint: every row owns one fixed-width slot,
						// and non-inline varlen payloads occupy the vector area.
						originWidth := analyzeValueWidth(states[columnIndex].typ, raw, isNull)
						if math.MaxUint64-states[columnIndex].sampleBytes < originWidth {
							_ = reader.Close()
							return 0, 0, moerr.NewInternalErrorNoCtx("ANALYZE logical byte counter overflow")
						}
						states[columnIndex].sampleBytes += originWidth
					}
					if isNull {
						if retainRow {
							states[columnIndex].sampleNulls++
						}
						continue
					}
					canonical, reusable := keycodec.CanonicalBytesAt(
						valueVector, row, states[columnIndex].canonical[:0])
					states[columnIndex].canonical = reusable
					valueHash := analyzestats.HashTypedValue(
						uint32(states[columnIndex].typ.Oid),
						states[columnIndex].typ.Width,
						states[columnIndex].typ.Scale,
						canonical,
					)
					if incidence {
						if observeErr := states[columnIndex].ndv.ObserveIncidenceValue(valueHash); observeErr != nil &&
							!errors.Is(observeErr, analyzestats.ErrAccumulatorLimit) {
							_ = reader.Close()
							return 0, 0, observeErr
						}
					}
					if retainRow {
						index.UpdateZM(states[columnIndex].zoneMap, raw)
						if observeErr := states[columnIndex].ndv.ObserveSampleValue(valueHash); observeErr != nil &&
							!errors.Is(observeErr, analyzestats.ErrAccumulatorLimit) {
							_ = reader.Close()
							return 0, 0, observeErr
						}
					}
				}
			}
		}
		if closeErr := reader.Close(); closeErr != nil {
			return 0, 0, closeErr
		}
		if incidence {
			for i := range states {
				if endErr := states[i].ndv.EndIncidenceBlock(); endErr != nil &&
					!errors.Is(endErr, analyzestats.ErrAccumulatorLimit) {
					return 0, 0, endErr
				}
			}
		}
	}
	return retainedRows, readBytes, nil
}

func analyzeValueWidth(typ types.Type, raw []byte, isNull bool) uint64 {
	width := uint64(typ.TypeSize())
	if !isNull && typ.IsVarlen() && len(raw) > types.VarlenaInlineSize {
		width += uint64(len(raw))
	}
	return width
}

func finalizeAnalyzeColumns(
	stats *pbstats.StatsInfo,
	states []analyzeColumnState,
	populationRows uint64,
	sampleRows uint64,
	blockSample analyzestats.Fraction,
	fullCoverage bool,
) error {
	if populationRows > 0 && sampleRows == 0 {
		return moerr.NewInternalErrorNoCtx("ANALYZE retained no visible sample rows")
	}
	for i := range states {
		state := &states[i]
		if incidenceErr := state.ndv.IncidenceStateError(); incidenceErr != nil {
			return incidenceErr
		}
		nullCount := scaleSampleRatio(state.sampleNulls, sampleRows, populationRows)
		populationNonNull := populationRows - min(nullCount, populationRows)
		estimate, err := state.ndv.Estimate(float64(populationNonNull), blockSample)
		if err != nil {
			return err
		}
		logutil.Info("manual-analyze-column-stats",
			zap.String("column", state.name),
			zap.Float64("ndv", estimate.Point),
			zap.Float64("observed-ndv", estimate.ObservedLower),
			zap.Float64("duj1", estimate.Duj1),
			zap.Bool("has-duj1", estimate.HasDuj1),
			zap.Float64("collapsed-duj1", estimate.CollapsedDuj1),
			zap.Bool("has-collapsed-duj1", estimate.HasCollapsedDuj1),
			zap.Uint64("sample-rows", sampleRows),
			zap.Uint64("population-rows", populationRows))
		stats.NdvMap[state.name] = estimate.Point
		stats.NullCntMap[state.name] = nullCount
		stats.SizeMap[state.name] = scaleSampleRatio(state.sampleBytes, sampleRows, populationRows)
		stats.DataTypeMap[state.name] = uint64(state.typ.Oid)
		if fullCoverage && state.zoneMap != nil && state.zoneMap.IsInited() {
			minValue, minOK := tryGetMinMaxValueByFloat64(state.typ, state.zoneMap.GetMinBuf())
			maxValue, maxOK := tryGetMinMaxValueByFloat64(state.typ, state.zoneMap.GetMaxBuf())
			if minOK && maxOK {
				stats.MinValMap[state.name] = minValue
				stats.MaxValMap[state.name] = maxValue
			}
		}
	}
	return nil
}

func scaleSampleRatio(sampleValue, sampleRows, populationRows uint64) uint64 {
	if sampleValue == 0 || sampleRows == 0 || populationRows == 0 {
		return 0
	}
	value := float64(sampleValue) / float64(sampleRows) * float64(populationRows)
	if value >= float64(math.MaxUint64) {
		return math.MaxUint64
	}
	return uint64(math.Round(value))
}
