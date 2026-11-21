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
	"runtime"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
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

// Column passes read the same snapshot but need not return rows in the same
// order. Memoize the V1 decision by physical row ID, not by batch position.
type analyzeRowDecisions struct {
	known, retained [objectio.BlockMaxRows / 64]uint64
}

// The cache may end in the middle of a persisted block shard. Cap the
// returned slice so even an append cannot write into a neighbor's bitmap.
func analyzeDecisionShard(decisions []analyzeRowDecisions, lo, hi int) []analyzeRowDecisions {
	end := min(hi, len(decisions))
	return decisions[min(lo, end):end:end]
}

func (r *analyzeSelectedRange) retainRow(seed *[32]byte, identity []byte, cache *analyzeRowDecisions) bool {
	if r.rowThresholdAll {
		return true
	}
	if cache != nil && len(identity) == types.RowidSize {
		id := (*types.Rowid)(identity)
		block, offset := id.Decode()
		if *block == r.block.BlockID && offset < objectio.BlockMaxRows {
			word, bit := offset/64, uint64(1)<<(offset%64)
			if cache.known[word]&bit == 0 {
				if analyzestats.RetainRow(*seed, identity, r.rowThreshold, false) {
					cache.retained[word] |= bit
				}
				cache.known[word] |= bit
			}
			return cache.retained[word]&bit != 0
		}
	}
	return analyzestats.RetainRow(*seed, identity, r.rowThreshold, false)
}

type analyzeColumnState struct {
	distribution *analyzestats.ByteRangeSampler
	column       *plan.ColDef
	tableVersion uint32
	name         string
	typ          types.Type
	ndv          *analyzestats.NDVAccumulator
	sampleNulls  uint64
	sampleBytes  uint64
	canonical    []byte
	zoneMap      index.ZM
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
	tableDef := tbl.GetTableDef(ctx)
	columns, err := resolveAnalyzeColumns(tableDef, request.Columns)
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

	stringSlots := analyzeByteRangeSlots(columns, qBlocks)
	columnsPerPass := int(request.ColumnsPerPass)
	var domains []analyzeIntegerDomain
	var decisions []analyzeRowDecisions
	if columnsPerPass < len(columns) && q.Numerator < q.Denominator {
		// Bound even a custom over-sized sample to 8 MiB of memoization.
		decisions = make([]analyzeRowDecisions, min(len(selected), int(analyzestats.DefaultMaxBlocks)))
	}
	// At most one column pass owns a reservation. Release on all returns.
	var held *process.ExecutionTransientMemoryReservation
	defer func() {
		if held != nil {
			held.Release()
		}
	}()
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
				typ:  types.New(types.T(column.Typ.Id), column.Typ.Width, column.Typ.Scale),
				ndv:  ndv,
			}
			if stringSlots > 0 && !column.Typ.PadSpace && analyzestats.SupportsByteRange(types.NewWithCharset(types.T(column.Typ.Id), column.Typ.Width, column.Typ.Scale, uint8(column.Typ.Charset))) {
				states[i].distribution = analyzestats.NewByteRangeSampler(stringSlots)
				states[i].column = column
				states[i].tableVersion = tableDef.Version
			}
			states[i].zoneMap = index.NewZM(states[i].typ.Oid, states[i].typ.Scale)
		}
		workers, sessionDOP := 1, -1
		scanCPU := min(system.GoMaxProcs(), runtime.GOMAXPROCS(0))
		if stmt, ok := request.ResourceProcess.(*process.Process); ok && stmt != nil && stmt.Base != nil && len(states) == 1 && analyzeParallelFixedType(states[0].typ.Oid) && !request.FullScan && request.MaxDistinctValues >= analyzestats.SampleSketchCapacity {
			if resolve := stmt.GetResolveVariableFunc(); resolve != nil {
				v, e := resolve("max_dop", true, false)
				if e != nil {
					return nil, e
				}
				if n, ok := v.(int64); ok && n >= 0 {
					sessionDOP = int(n)
				}
			}
			if sessionDOP >= 0 {
				reserve, e := stmt.GetExecutionTransientMemoryAdmission()
				if e != nil {
					return nil, e
				}
				blockCount := len(selected)
				if blockCount > 0 && selected[0].block.IsMemBlk() {
					blockCount--
				}
				workers, held, e = admitAnalyzeWorkers(ctx, scanCPU, sessionDOP, blockCount, reserve)
				if e != nil {
					return nil, e
				}
			}
		}
		var passRows, passBytes uint64
		if workers > 1 {
			passRows, passBytes, err = tbl.scanAnalyzeColumnParallel(ctx, proc, selectedData, selected, states, request.Seed, workers, decisions)
		} else {
			passRows, passBytes, err = tbl.scanAnalyzeColumnGroup(ctx, proc, selectedData, selected, states, request.Seed, decisions, nil)
		}
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
		for i, state := range states {
			if request.FullScan {
				break // Leave FULLSCAN's HLL result unchanged.
			}
			point := stats.NdvMap[state.name]
			if point <= integerDomainWidth(state.zoneMap) {
				continue
			}
			if q.Numerator == q.Denominator {
				stats.NdvMap[state.name] = integerDomainWidth(state.zoneMap)
			} else {
				domains = append(domains, analyzeIntegerDomain{state.name, uint16(columns[first+i].Seqnum), state.zoneMap})
			}
		}
		if held != nil {
			held.Release()
			held = nil
		}
	}
	if len(domains) > 0 {
		fs, fsErr := fileservice.Get[fileservice.FileService](proc.Base.FileService, defines.SharedFileServiceName)
		if fsErr == nil {
			boundAnalyzeDomains(ctx, ranges.GetBlockInfoSlice(), fs, analyzeDomainMetaBytes, domains, stats.NdvMap)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// Fixed K=32768: each sketch owns 32-byte nodes, uint32 heap slots,
// and two uint32 bucket heads per slot. Round up for small allocations.
const analyzeSketchCharge = 1536 << 10

// Keep admission limited to tested fixed-width readers. In particular, do
// not let IsDecimal expand this path to DECIMAL128/256 or affect NDV bounds.
func analyzeParallelFixedType(t types.T) bool {
	return t.IsInteger() || t == types.T_decimal64
}

func admitAnalyzeWorkers(ctx context.Context, cpu, dop, blocks int, reserve func(uint64) (*process.ExecutionTransientMemoryReservation, error)) (int, *process.ExecutionTransientMemoryReservation, error) {
	n := max(1, min(cpu, 8, blocks/16))
	if dop > 0 {
		n = min(n, dop)
	}
	for {
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}
		states := n
		if n > 1 {
			states++
		} // coordinator merges while workers' sketches are live
		r, err := reserve(uint64(states) * analyzeSketchCharge)
		if err == nil {
			// Probe headroom for reader batches without holding a second charge
			// against the same buffers during execution. This isn't a hard RSS cap.
			var headroom *process.ExecutionTransientMemoryReservation
			headroom, err = reserve(uint64(n) * 512 << 10)
			if err == nil {
				headroom.Release()
				return n, r, nil
			}
			r.Release()
		}
		if n == 1 || !errors.Is(err, process.ErrExecutionResourceAdmission) {
			return 0, nil, err
		}
		n = max(1, n/2)
	}
}

type analyzeIntegerDomain struct {
	name string
	seq  uint16
	zm   index.ZM
}

// An unknown or lossy width must never tighten a cardinality estimate.
func integerDomainWidth(zm index.ZM) float64 {
	if !zm.IsInited() || !zm.GetType().IsInteger() {
		return math.Inf(1)
	}
	typ := zm.GetType().ToType()
	lo, loOK := tryGetMinMaxValueByFloat64(typ, zm.GetMinBuf())
	hi, hiOK := tryGetMinMaxValueByFloat64(typ, zm.GetMaxBuf())
	if !loOK || !hiOK || lo > hi || math.Abs(lo) >= 1<<52 || math.Abs(hi) >= 1<<52 {
		return math.Inf(1)
	}
	return hi - lo + 1
}

// The retained-row range is only a trigger. Union every visible object's
// bounds with ALL in-memory values (collected by scanAnalyzeColumnGroup).
// Deletes only widen this superset. Preflight the entire object list before
// any optional I/O; projecting fewer columns does not change its cost.
func boundAnalyzeDomains(ctx context.Context, blocks objectio.BlockInfoSlice, fs fileservice.FileService, budget uint64, domains []analyzeIntegerDomain, ndvs map[string]float64) {
	locs, ok := analyzeDomainLocations(ctx, blocks, budget, analyzeDomainMetaObjects)
	if !ok {
		return
	}
	for _, loc := range locs {
		if len(domains) == 0 || ctx.Err() != nil {
			return
		}
		meta, err := objectio.FastLoadObjectMeta(ctx, &loc, false, fs)
		if err != nil {
			return
		}
		data, ok := meta.DataMeta()
		if !ok {
			return
		}
		remaining := domains[:0]
		for _, d := range domains {
			if d.seq >= data.BlockHeader().MetaColumnCount() || d.seq > data.BlockHeader().MaxSeqnum() {
				continue
			}
			z := data.MustGetColumn(d.seq).ZoneMap()
			if !z.IsInited() || z.GetType() != d.zm.GetType() {
				continue
			}
			index.MergeZM(d.zm, z)
			if integerDomainWidth(d.zm) < ndvs[d.name] {
				remaining = append(remaining, d)
			}
		}
		domains = remaining
	}
	for _, d := range domains {
		ndvs[d.name] = integerDomainWidth(d.zm)
	}
}

const (
	analyzeDomainMetaBytes   = 256 << 20
	analyzeDomainMetaObjects = 4096
)

// The locations borrow the immutable block list for the duration of this
// collection. Reject the whole list on the first excess, never its prefix.
func analyzeDomainLocations(ctx context.Context, blocks objectio.BlockInfoSlice, bytesLeft uint64, objectLimit int) ([]objectio.Location, bool) {
	seen := make(map[types.Objectid]struct{})
	var locs []objectio.Location
	for i := 0; i < blocks.Len(); i++ {
		if i%1024 == 0 && ctx.Err() != nil {
			return nil, false
		}
		block := blocks.Get(i)
		if block.IsMemBlk() {
			continue
		}
		id := *block.BlockID.Object()
		if _, ok := seen[id]; ok {
			continue
		}
		loc := block.MetaLocation()
		size := uint64(loc.Extent().Length())
		if size == 0 || size > bytesLeft || len(locs) >= objectLimit {
			return nil, false
		}
		bytesLeft -= size
		seen[id] = struct{}{}
		locs = append(locs, loc)
	}
	return locs, true
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

// Only disjoint persisted ranges run concurrently; the coordinator owns
// the memory range and merges partial columns only after all readers finish.
func (tbl *txnTable) scanAnalyzeColumnParallel(ctx context.Context, proc *process.Process, data engine.RelData, selected []analyzeSelectedRange, states []analyzeColumnState, seed [32]byte, n int, decisions []analyzeRowDecisions) (uint64, uint64, error) {
	var rows, bytes uint64
	var err error
	first := 0
	if selected[0].block.IsMemBlk() {
		rows, bytes, err = tbl.scanAnalyzeColumnGroup(ctx, proc, data.DataSlice(0, 1), selected[:1], states, seed, nil, nil)
		if err != nil {
			return 0, 0, err
		}
		first++
	}
	type part struct {
		s           []analyzeColumnState
		rows, bytes uint64
	}
	parts := make([]part, n)
	var builds sync.Mutex
	g, child := errgroup.WithContext(ctx)
	for i := range parts {
		lo := first + (len(selected)-first)*i/n
		hi := first + (len(selected)-first)*(i+1)/n
		if lo == hi {
			continue
		}
		p := &parts[i]
		typ := states[0].typ
		p.s = []analyzeColumnState{{name: states[0].name, typ: typ, ndv: analyzestats.NewNDVAccumulator(defaultAnalyzeDistinctValues), zoneMap: index.NewZM(typ.Oid, typ.Scale)}}
		shard := data.DataSlice(lo, hi)
		memo := analyzeDecisionShard(decisions, lo, hi)
		g.Go(func() error {
			var e error
			p.rows, p.bytes, e = tbl.scanAnalyzeColumnGroup(child, proc, shard, selected[lo:hi], p.s, seed, memo, &builds)
			return e
		})
	}
	if err := g.Wait(); err != nil {
		return 0, 0, err
	}
	for _, p := range parts {
		if p.s == nil {
			continue
		}
		s := p.s[0]
		if err := states[0].ndv.Merge(s.ndv); err != nil {
			return 0, 0, err
		}
		rows += p.rows
		bytes += p.bytes
		states[0].sampleNulls += s.sampleNulls
		states[0].sampleBytes += s.sampleBytes
		index.MergeZM(states[0].zoneMap, s.zoneMap)
	}
	return rows, bytes, nil
}

func (tbl *txnTable) scanAnalyzeColumnGroup(
	ctx context.Context,
	proc *process.Process,
	selectedData engine.RelData,
	selected []analyzeSelectedRange,
	states []analyzeColumnState,
	seed [32]byte,
	decisions []analyzeRowDecisions,
	builds *sync.Mutex,
) (retainedRows, readBytes uint64, err error) {
	hasDistribution := false
	attrs := make([]string, 0, len(states)+1)
	attrTypes := make([]types.Type, 0, len(states)+1)
	for i := range states {
		hasDistribution = hasDistribution || states[i].distribution != nil
		attrs = append(attrs, states[i].name)
		attrTypes = append(attrTypes, states[i].typ)
	}
	attrs = append(attrs, catalog.Row_ID)
	attrTypes = append(attrTypes, objectio.RowidType)

	data := batch.NewWithSchema(true, attrs, attrTypes)
	defer data.Clean(proc.Mp())
	for rangeIndex := range selected {
		descriptor := &selected[rangeIndex]
		var cache *analyzeRowDecisions
		if rangeIndex < len(decisions) && !descriptor.block.IsMemBlk() {
			cache = &decisions[rangeIndex]
		}
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
		if err := ctx.Err(); err != nil {
			return 0, 0, err
		}
		if builds != nil {
			builds.Lock()
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
		if builds != nil {
			builds.Unlock()
		}
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
				retainRow := descriptor.retainRow(&seed, identity, cache)
				if retainRow {
					retainedRows++
				}
				var distributionPriority [16]byte
				if retainRow && hasDistribution {
					distributionPriority = analyzestats.ByteRangePriority(seed, types.Rowid(identity))
				}

				for columnIndex := range states {
					valueVector := data.Vecs[columnIndex]
					isNull := valueVector.IsNull(uint64(row))
					var raw []byte
					if !isNull {
						raw = valueVector.GetRawBytesAt(row)
					}
					if retainRow && states[columnIndex].distribution != nil {
						if sampleErr := states[columnIndex].distribution.Observe(distributionPriority, identity, raw, isNull); sampleErr != nil {
							_ = reader.Close()
							return 0, 0, sampleErr
						}
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
					if retainRow || (descriptor.block.IsMemBlk() && states[columnIndex].typ.Oid.IsInteger()) {
						index.UpdateZM(states[columnIndex].zoneMap, raw)
					}
					if retainRow {
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

// A block-cluster sample is not a uniform row sample. Distribute the budget
// across the entire selected schema before splitting it into column passes.
func analyzeByteRangeSlots(columns []*plan.ColDef, blocks analyzestats.Fraction) int {
	if blocks.Denominator == 0 || blocks.Numerator != blocks.Denominator {
		return 0
	}
	eligible := 0
	for _, col := range columns {
		if !col.Typ.PadSpace && analyzestats.SupportsByteRange(types.NewWithCharset(types.T(col.Typ.Id), col.Typ.Width, col.Typ.Scale, uint8(col.Typ.Charset))) {
			eligible++
		}
	}
	return analyzestats.ByteRangeSampleCapacity(eligible)
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
		if d := state.distribution.Distribution(populationRows); d != nil {
			d.TypeId = uint32(state.typ.Oid)
			d.Charset = state.column.Typ.Charset
			d.CollationVersion = state.column.Typ.CollationVersion
			d.ColumnSeqnum = uint32(state.column.Seqnum)
			d.TableVersion = state.tableVersion
			if stats.ByteRangeMap == nil {
				stats.ByteRangeMap = make(map[string]*pbstats.ByteRangeDistribution)
			}
			stats.ByteRangeMap[state.name] = d
		}
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
