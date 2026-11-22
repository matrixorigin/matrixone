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
	"crypto/sha256"
	"math"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pbstats "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	analyzestats "github.com/matrixorigin/matrixone/pkg/statistics/analyze"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeCacheYieldsToScan(t *testing.T) {
	// 4096 slots take 8 MiB; 1024 fit initially (2 MiB) but would
	// crowd out the serial sketch and its reader headroom under this cap.
	for _, blocks := range []int{4096, 1024, 16} {
		budget := process.MustNewExecutionResourceBudget(6<<20, 6<<20)
		g, err := budget.OpenGeneration(1)
		require.NoError(t, err)
		var cache analyzeDecisionCache
		require.NoError(t, cache.allocate(blocks, g.ReserveTransientMemory))
		require.Equal(t, blocks != 4096, cache.held != nil)
		require.Equal(t, uint64(len(cache.rows))*2048, g.Snapshot().Used)
		workers, r, err := admitAnalyzeWorkers(t.Context(), 1, 1, blocks, g.ReserveTransientMemory, &cache)
		require.NoError(t, err)
		require.Equal(t, 1, workers)
		require.Equal(t, blocks == 16, cache.held != nil)
		r.Release()
		cache.release()
		require.Zero(t, g.Snapshot().Used)
		require.Zero(t, budget.AggregateUsed())
	}
}

func TestAnalyzeRowDecisionsMatchVisibleIdentity(t *testing.T) {
	seed := sha256.Sum256([]byte("sample across column passes"))
	r := analyzeSelectedRange{rowThreshold: [16]byte{128}}
	cache := new(analyzeRowDecisions)
	r.block.BlockID[0] = 1
	foreign := r.block.BlockID
	foreign[0]++
	require.Equal(t, analyzestats.RetainRow(seed, nil, r.rowThreshold, false), r.retainRow(&seed, nil, cache))
	// Gaps and different order model readers after visibility filtering. IDs
	// that cannot be represented by this block's bitmap must never alias it.
	for pass := range 2 {
		for i := range 201 {
			offset := uint32(2 * i)
			if pass == 1 {
				offset = 400 - offset
			}
			for _, id := range []types.Rowid{
				types.NewRowid(&r.block.BlockID, offset),
				types.NewRowid(&foreign, offset),
				types.NewRowid(&r.block.BlockID, objectio.BlockMaxRows+offset),
			} {
				want := analyzestats.RetainRow(seed, id[:], r.rowThreshold, false)
				require.Equal(t, want, r.retainRow(&seed, id[:], cache))
			}
		}
	}
}

func BenchmarkAnalyzeRowDecisions(b *testing.B) {
	seed := sha256.Sum256([]byte("column passes"))
	r := analyzeSelectedRange{rowThreshold: [16]byte{17}}
	ids := make([]types.Rowid, objectio.BlockMaxRows)
	for i := range ids {
		ids[i] = types.NewRowid(&r.block.BlockID, uint32(i))
	}
	for _, mode := range []string{"uncached", "cached"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var cache *analyzeRowDecisions
				if mode == "cached" {
					cache = new(analyzeRowDecisions)
				}
				for range 23 {
					for i := range ids {
						r.retainRow(&seed, ids[i][:], cache)
					}
				}
			}
		})
	}
}

func TestAnalyzeIntegerDomainRequiresCompleteCoverage(t *testing.T) {
	zm := index.NewZM(types.T_int64, 0)
	newValue := func(v int64) { index.UpdateZM(zm, types.EncodeInt64(&v)) }
	require.True(t, math.IsInf(integerDomainWidth(zm), 1))
	newValue(-2)
	newValue(3)
	ndvs := map[string]float64{"k": 20}
	domains := []analyzeIntegerDomain{{name: "k", zm: zm}}
	id := objectio.NewObjectid()
	obj := objectio.NewObjectStatsWithObjectID(&id, false, true, false)
	objectio.SetObjectStatsExtent(obj, objectio.NewExtent(1, 2, 3, 4))
	objectio.SetObjectStatsBlkCnt(obj, 1)
	objectio.SetObjectStatsRowCnt(obj, 2)
	blocks := objectio.ObjectStatsToBlockInfoSlice(obj, true)
	// Logical cost is per distinct persisted object, not per block or column.
	id2 := objectio.NewObjectid()
	obj2 := objectio.NewObjectStatsWithObjectID(&id2, false, true, false)
	objectio.SetObjectStatsExtent(obj2, objectio.NewExtent(1, 5, 7, 9))
	objectio.SetObjectStatsBlkCnt(obj2, 1)
	objectio.SetObjectStatsRowCnt(obj2, 2)
	blocks.AppendBlockInfo(blocks.Get(1))
	second := objectio.ObjectStatsToBlockInfoSlice(obj2, false)
	blocks.AppendBlockInfo(second.Get(0))
	locs, ok := analyzeDomainLocations(t.Context(), blocks, 10, 2)
	require.True(t, ok)
	require.Len(t, locs, 2)
	for _, b := range []struct {
		bytes   uint64
		objects int
	}{{9, 2}, {10, 1}} {
		locs, ok = analyzeDomainLocations(t.Context(), blocks, b.bytes, b.objects)
		require.False(t, ok)
		require.Empty(t, locs)
	}
	// Uninspected persisted rows could contain new values.
	boundAnalyzeDomains(t.Context(), blocks, nil, 0, domains, ndvs)
	require.Equal(t, 20., ndvs["k"])
	// With only the fully traversed memory range, the inclusive width is safe.
	require.True(t, blocks.Get(0).IsMemBlk())
	boundAnalyzeDomains(t.Context(), blocks[:objectio.BlockInfoSize], nil, 0, domains, ndvs)
	require.Equal(t, 6., ndvs["k"])
	newValue(math.MaxInt64)
	require.True(t, math.IsInf(integerDomainWidth(zm), 1))
}

func TestAnalyzeDecisionShardAcrossPasses(t *testing.T) {
	// Slot zero represents the mem range, which stays uncached. Vary the
	// partition between passes, and end the budget inside a worker's slice.
	seed := sha256.Sum256([]byte("same snapshot"))
	for _, limit := range []int{0, 4, 8} {
		cache := make([]analyzeRowDecisions, limit)
		for pass, spans := range [][][2]int{{{1, 3}, {3, 6}, {6, 8}}, {{1, 5}, {5, 8}}} {
			var group sync.WaitGroup
			for _, span := range spans {
				group.Go(func() {
					shard := analyzeDecisionShard(cache, span[0], span[1])
					if len(shard) != cap(shard) {
						t.Error("shard includes capacity owned by another worker")
					}
					for j := span[0]; j < span[1]; j++ {
						r := analyzeSelectedRange{rowThreshold: [16]byte{117}}
						r.block.BlockID = types.BuildTestBlockid(int64(j), 0)
						var memo *analyzeRowDecisions
						if j-span[0] < len(shard) {
							memo = &shard[j-span[0]]
						}
						for i := range 64 {
							offset := uint32(i)
							if pass != 0 {
								offset = 63 - offset
							}
							id := types.NewRowid(&r.block.BlockID, offset)
							want := analyzestats.RetainRow(seed, id[:], r.rowThreshold, false)
							if got := r.retainRow(&seed, id[:], memo); got != want {
								t.Errorf("block=%d row=%d want=%t got=%t", j, offset, want, got)
								return
							}
						}
					}
				})
			}
			group.Wait()
			for i := range cache {
				want := ^uint64(0)
				if i == 0 {
					want = 0
				}
				if cache[i].known[0] != want {
					t.Fatalf("limit=%d block=%d known=%x", limit, i, cache[i].known[0])
				}
			}
		}
	}
}

func TestSelectAnalyzeRangesIsBoundedAndDeterministic(t *testing.T) {
	objectID := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&objectID, false, true, false)
	objectio.SetObjectStatsExtent(stats, objectio.NewExtent(1, 2, 3, 4))
	objectio.SetObjectStatsBlkCnt(stats, 100)
	objectio.SetObjectStatsRowCnt(stats, objectio.BlockMaxRows*100)
	all := objectio.ObjectStatsToBlockInfoSlice(stats, true)
	request := engine.AnalyzeTableRequest{
		Seed:       sha256.Sum256([]byte("table-generation")),
		TargetRows: 10_000,
		MaxBlocks:  16,
		MaxStrata:  8,
	}

	first, blocks, q, qBlocks, err := selectAnalyzeRanges(
		all, uint64(objectio.BlockMaxRows)*100, request)
	require.NoError(t, err)
	second, _, secondQ, secondQBlocks, err := selectAnalyzeRanges(
		all, uint64(objectio.BlockMaxRows)*100, request)
	require.NoError(t, err)
	require.Equal(t, uint64(100), blocks)
	require.Equal(t, first, second)
	require.Equal(t, q, secondQ)
	require.Equal(t, qBlocks, secondQBlocks)
	require.Len(t, first, 9) // eight persisted strata plus the memory marker
	require.True(t, first[0].block.IsMemBlk())
	require.Less(t, q.Numerator, q.Denominator)
	require.Less(t, qBlocks.Numerator, qBlocks.Denominator)

	seen := make(map[types.Blockid]struct{}, len(first)-1)
	for _, selected := range first[1:] {
		require.False(t, selected.block.IsMemBlk())
		_, duplicate := seen[selected.block.BlockID]
		require.False(t, duplicate)
		seen[selected.block.BlockID] = struct{}{}
	}
}

func TestSelectAnalyzeRangesFullscanIncludesEveryRange(t *testing.T) {
	objectID := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&objectID, false, true, false)
	objectio.SetObjectStatsExtent(stats, objectio.NewExtent(1, 2, 3, 4))
	objectio.SetObjectStatsBlkCnt(stats, 3)
	objectio.SetObjectStatsRowCnt(stats, objectio.BlockMaxRows*3)
	all := objectio.ObjectStatsToBlockInfoSlice(stats, true)
	selected, blocks, q, qBlocks, err := selectAnalyzeRanges(
		all,
		uint64(objectio.BlockMaxRows)*3,
		engine.AnalyzeTableRequest{FullScan: true},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(3), blocks)
	require.Equal(t, analyzestats.MustFraction(1, 1), q)
	require.Equal(t, analyzestats.MustFraction(1, 1), qBlocks)
	require.Len(t, selected, 4)
	for _, descriptor := range selected {
		require.True(t, descriptor.rowThresholdAll)
		require.True(t, descriptor.incidenceAll)
	}
}

func TestFinalizeAnalyzeColumnsUsesVisiblePopulationBounds(t *testing.T) {
	accumulator := analyzestats.NewNDVAccumulator(16)
	require.NoError(t, accumulator.BeginIncidenceBlock())
	a := analyzestats.HashTypedValue(uint32(types.T_int64), 64, 0, types.EncodeInt64(new(int64)))
	bValue := int64(2)
	b := analyzestats.HashTypedValue(uint32(types.T_int64), 64, 0, types.EncodeInt64(&bValue))
	require.NoError(t, accumulator.ObserveIncidenceValue(a))
	require.NoError(t, accumulator.ObserveIncidenceValue(b))
	require.NoError(t, accumulator.EndIncidenceBlock())
	require.NoError(t, accumulator.ObserveSampleValue(a))
	require.NoError(t, accumulator.ObserveSampleValue(a))
	require.NoError(t, accumulator.ObserveSampleValue(b))
	zoneMap := index.NewZM(types.T_int64, 0)
	minValue := int64(-5)
	maxValue := int64(12)
	index.UpdateZM(zoneMap, types.EncodeInt64(&minValue))
	index.UpdateZM(zoneMap, types.EncodeInt64(&maxValue))

	stats := plan2.NewStatsInfo()
	err := finalizeAnalyzeColumns(stats, []analyzeColumnState{{
		name: "c", typ: types.T_int64.ToType(), ndv: accumulator,
		sampleNulls: 1, sampleBytes: 24, zoneMap: zoneMap,
	}}, 100, 4, analyzestats.MustFraction(1, 2), false)
	require.NoError(t, err)
	require.Equal(t, uint64(25), stats.NullCntMap["c"])
	require.Equal(t, uint64(600), stats.SizeMap["c"])
	require.Equal(t, float64(4), stats.NdvMap["c"])
	require.Equal(t, uint64(types.T_int64), stats.DataTypeMap["c"])
	require.Empty(t, stats.MinValMap, "partial sample extrema are not table bounds")
	require.Empty(t, stats.MaxValMap, "partial sample extrema are not table bounds")

	fullStats := plan2.NewStatsInfo()
	err = finalizeAnalyzeColumns(fullStats, []analyzeColumnState{{
		name: "c", typ: types.T_int64.ToType(), ndv: accumulator,
		sampleNulls: 1, sampleBytes: 24, zoneMap: zoneMap,
	}}, 100, 4, analyzestats.MustFraction(1, 2), true)
	require.NoError(t, err)
	require.Equal(t, float64(minValue), fullStats.MinValMap["c"])
	require.Equal(t, float64(maxValue), fullStats.MaxValMap["c"])

	err = finalizeAnalyzeColumns(plan2.NewStatsInfo(), []analyzeColumnState{{
		name: "c", typ: types.T_int64.ToType(), ndv: analyzestats.NewNDVAccumulator(1),
	}}, 1, 0, analyzestats.MustFraction(1, 1), true)
	require.Error(t, err)
}

func TestFinalizeAnalyzeColumnsFailsClosedOnIncidenceOverflow(t *testing.T) {
	accumulator := analyzestats.NewNDVAccumulator(1)
	one := analyzestats.HashValue([]byte("one"))
	two := analyzestats.HashValue([]byte("two"))
	require.NoError(t, accumulator.ObserveSampleValue(one))
	require.NoError(t, accumulator.BeginIncidenceBlock())
	require.NoError(t, accumulator.ObserveIncidenceValue(one))
	require.ErrorIs(t, accumulator.ObserveIncidenceValue(two), analyzestats.ErrAccumulatorLimit)
	require.ErrorIs(t, accumulator.EndIncidenceBlock(), analyzestats.ErrAccumulatorLimit)

	stats := plan2.NewStatsInfo()
	err := finalizeAnalyzeColumns(stats, []analyzeColumnState{{
		name: "c", typ: types.T_int64.ToType(), ndv: accumulator,
	}}, 10, 1, analyzestats.MustFraction(1, 2), false)
	require.ErrorIs(t, err, analyzestats.ErrAccumulatorLimit)
	_, published := stats.NdvMap["c"]
	require.False(t, published)
}

func TestResolveAnalyzeColumnsRejectsMissingAndDuplicate(t *testing.T) {
	table := &plan.TableDef{Cols: []*plan.ColDef{
		{Name: "a", OriginName: "A", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: "hidden", Seqnum: 2, Hidden: true},
	}}
	columns, err := resolveAnalyzeColumns(table, []string{"A"})
	require.NoError(t, err)
	require.Equal(t, "a", columns[0].Name)
	_, err = resolveAnalyzeColumns(table, []string{"a", "A"})
	require.Error(t, err)
	_, err = resolveAnalyzeColumns(table, []string{"missing"})
	require.Error(t, err)
	_, err = resolveAnalyzeColumns(table, []string{"hidden"})
	require.Error(t, err)
}

func TestComposeAnalyzedStatsGenerationKeepsFreshPhysicalDistribution(t *testing.T) {
	metadata := plan2.NewStatsInfo()
	metadata.TableCnt = 120
	metadata.BlockNumber = 9
	metadata.AccurateObjectNumber = 3
	metadata.NdvMap["a"] = 12
	metadata.NdvMap["b"] = 200
	metadata.NullCntMap["b"] = 130
	metadata.MinValMap["a"] = 1
	metadata.MaxValMap["a"] = 99
	metadata.ShuffleRangeMap["a"] = &pbstats.ShuffleRange{Overlap: 0.25}
	metadata.ShuffleRangeMap["url"] = &pbstats.ShuffleRange{
		IsStrType: true,
		Result:    []float64{10, 20},
	}

	collected := plan2.NewStatsInfo()
	collected.TableCnt = 100
	collected.BlockNumber = 7
	collected.NdvMap["a"] = 40
	collected.NullCntMap["a"] = 2
	collected.SizeMap["a"] = 800
	collected.DataTypeMap["a"] = uint64(types.T_int64)
	collected.ShuffleRangeMap["url"] = &pbstats.ShuffleRange{
		IsStrType: true,
		Result:    []float64{30, 40},
	}

	metadata.ByteRangeMap = map[string]*pbstats.ByteRangeDistribution{"old": {Version: 1}}
	collected.ByteRangeMap = map[string]*pbstats.ByteRangeDistribution{"url": {Version: 1, Values: [][]byte{[]byte("secret")}}}
	published := composeAnalyzedStatsGeneration(metadata, collected)
	require.NotContains(t, published.ByteRangeMap, "old")
	collected.ByteRangeMap["url"].Values[0][0] = 'x'
	require.Equal(t, []byte("secret"), published.ByteRangeMap["url"].Values[0])
	require.Empty(t, composeAnalyzedStatsGeneration(published, plan2.NewStatsInfo()).ByteRangeMap, "a later partial ANALYZE cannot retain an uncollected distribution")
	require.Equal(t, float64(100), published.TableCnt)
	require.Equal(t, int64(7), published.BlockNumber)
	require.Equal(t, int64(3), published.AccurateObjectNumber)
	require.Equal(t, float64(40), published.NdvMap["a"])
	require.Equal(t, float64(100), published.NdvMap["b"], "metadata NDV must follow the exact row-count bound")
	require.Equal(t, uint64(100), published.NullCntMap["b"], "metadata NULL count must follow the exact row-count bound")
	require.Equal(t, float64(1), published.MinValMap["a"])
	require.Equal(t, float64(99), published.MaxValMap["a"])
	require.Equal(t, 0.25, published.ShuffleRangeMap["a"].Overlap)
	require.Equal(t, []float64{30, 40}, published.ShuffleRangeMap["url"].Result,
		"fresh string range metadata must remain available to non-aggregate consumers")

	collected.NdvMap["a"] = 1
	metadata.ShuffleRangeMap["a"].Overlap = 1
	require.Equal(t, float64(40), published.NdvMap["a"], "published generation must own its maps")
	require.Equal(t, 0.25, published.ShuffleRangeMap["a"].Overlap)
}

func TestAnalyzeValueWidthIncludesOnlyOutOfLinePayload(t *testing.T) {
	varlen := types.T_varchar.ToType()
	require.Equal(t, uint64(types.VarlenaSize), analyzeValueWidth(varlen, []byte("short"), false))
	require.Equal(t, uint64(types.VarlenaSize), analyzeValueWidth(
		varlen, make([]byte, types.VarlenaInlineSize), false))
	require.Equal(t, uint64(types.VarlenaSize+types.VarlenaInlineSize+1), analyzeValueWidth(
		varlen, make([]byte, types.VarlenaInlineSize+1), false))
	require.Equal(t, uint64(types.VarlenaSize), analyzeValueWidth(
		varlen, make([]byte, types.VarlenaInlineSize+1), true))
	require.Equal(t, uint64(8), analyzeValueWidth(types.T_int64.ToType(), make([]byte, 8), false))
}

func TestByteRangePatchPublishesNewOwner(t *testing.T) {
	gs := newTestGlobalStats(context.Background(), nil, nil)
	defer gs.Close()
	key := pbstats.StatsInfoKey{TableID: 42}
	original := plan2.NewStatsInfo()
	original.TableCnt = 10
	original.ByteRangeMap = map[string]*pbstats.ByteRangeDistribution{"name": {Version: 1, PopulationRows: 10, SampleRows: 10, CompleteSample: true}}
	gs.cache.Lock()
	gs.cache.statsInfoMap[key] = original
	gs.cache.Unlock()
	n := 20.0
	require.NoError(t, gs.PatchStats(key, &PatchArgs{TableCnt: &n}))
	gs.cache.RLock()
	published := gs.cache.statsInfoMap[key]
	remaining := len(published.ByteRangeMap)
	gs.cache.RUnlock()
	require.NotSame(t, original, published)
	require.Zero(t, remaining)
	require.Equal(t, 10.0, original.TableCnt)
	require.NotEmpty(t, original.ByteRangeMap, "existing readers keep an immutable sampled generation")
}

func TestAnalyzeByteRangeSamplingDomain(t *testing.T) {
	columns := []*plan.ColDef{
		{Name: "v", Seqnum: 7, Typ: plan.Type{Id: int32(types.T_varchar)}},
		{Name: "c", Typ: plan.Type{Id: int32(types.T_char)}},
		{Name: "padded", Typ: plan.Type{Id: int32(types.T_varchar), PadSpace: true}},
		{Name: "uca", Typ: plan.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB4UnicodeCI)}},
		{Name: "i", Typ: plan.Type{Id: int32(types.T_int64)}},
	}
	require.Zero(t, analyzeByteRangeSlots(columns, analyzestats.MustFraction(1, 2)))
	require.Equal(t, 2048, analyzeByteRangeSlots(columns, analyzestats.MustFraction(1, 1)))
	require.Zero(t, analyzeByteRangeSlots(columns[1:], analyzestats.MustFraction(1, 1)))
	many := make([]*plan.ColDef, 32)
	for i := range many {
		many[i] = columns[0]
	}
	require.Equal(t, 512, analyzeByteRangeSlots(many, analyzestats.MustFraction(1, 1)))
	sample := analyzestats.NewByteRangeSampler(10)
	ndv := analyzestats.NewFullScanNDVAccumulator()
	var id [types.RowidSize]byte
	require.NoError(t, sample.Observe(analyzestats.ByteRangePriority([32]byte{}, id), id[:], []byte("a"), false))
	id[0] = 1
	require.NoError(t, sample.Observe(analyzestats.ByteRangePriority([32]byte{}, id), id[:], nil, true))
	require.NoError(t, ndv.ObserveSampleValue(analyzestats.HashValue([]byte("a"))))
	stats := plan2.NewStatsInfo()
	require.NoError(t, finalizeAnalyzeColumns(stats, []analyzeColumnState{{name: "v", column: columns[0], tableVersion: 5, typ: types.T_varchar.ToType(), ndv: ndv, distribution: sample, sampleNulls: 1}}, 2, 2, analyzestats.MustFraction(1, 1), true))
	d := stats.ByteRangeMap["v"]
	require.NotNil(t, d)
	require.Equal(t, uint32(0), d.Charset, "wire identity follows the catalog, not a reconstructed default charset")
	require.Equal(t, uint32(7), d.ColumnSeqnum)
	require.Equal(t, uint32(5), d.TableVersion)
	require.Equal(t, uint64(2), d.SampleRows)
	require.True(t, d.CompleteSample)
	require.Equal(t, [][]byte{[]byte("a")}, d.Values)
}
