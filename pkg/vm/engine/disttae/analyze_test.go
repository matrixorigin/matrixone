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
	"crypto/sha256"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	analyzestats "github.com/matrixorigin/matrixone/pkg/statistics/analyze"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

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

func TestNewAnalyzedStatsGenerationDoesNotMixEpochs(t *testing.T) {
	src := plan2.NewStatsInfo()
	src.TableCnt = 100
	src.BlockNumber = 7
	src.NdvMap["a"] = 40
	src.NullCntMap["a"] = 2
	src.SizeMap["a"] = 800
	src.DataTypeMap["a"] = uint64(types.T_int64)

	published := newAnalyzedStatsGeneration(src)
	require.Equal(t, float64(100), published.TableCnt)
	require.Equal(t, int64(7), published.BlockNumber)
	require.Equal(t, float64(40), published.NdvMap["a"])
	require.NotContains(t, published.NdvMap, "b")
	require.Empty(t, published.MinValMap)
	require.Empty(t, published.MaxValMap)

	src.NdvMap["a"] = 1
	require.Equal(t, float64(40), published.NdvMap["a"], "published generation must own its maps")
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
