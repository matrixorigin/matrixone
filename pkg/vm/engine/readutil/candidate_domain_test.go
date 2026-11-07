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

package readutil

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestCandidateReaderAndRawKeyUseSeparateProofs(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	typ := plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 4}
	table := decimalTableDef(typ, true)
	table.Pkey = &plan.PrimaryKeyDef{PkeyColName: "amount", Names: []string{"amount"}}
	filter := decimalFoldedFilter(t, typ, "<=", decimalBound{"2", 0})
	filter.CandidateFilter = true
	_, _, _, block, _, compiled, _ := CompileFilterExpr(filter, table, nil)
	require.True(t, compiled, "the zone-map matcher can compare decimal scales")
	_, matches, err := block(0, makeDecimalBlockMeta(t, typ, "1.5000"), nil)
	require.NoError(t, err)
	require.True(t, matches)
	base, err := ConstructBasePKFilter(filter, table, proc.Mp())
	require.NoError(t, err)
	defer base.Cleanup()
	require.False(t, base.Valid, "raw PK bytes with unequal scales would discard 1.5000")

	exact := decimalFoldedFilter(t, typ, "<=", decimalBound{"2.0000", 4})
	exact.CandidateFilter = true
	base, err = ConstructBasePKFilter(exact, table, proc.Mp())
	require.NoError(t, err)
	defer base.Cleanup()
	require.True(t, base.Valid, "same-scale ordinary decimal bounds remain usable")
}

func TestCandidateOrdinaryIntegerAccess(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	typ := plan.Type{Id: int32(types.T_int64)}
	table := decimalTableDef(typ, true)
	table.Pkey = &plan.PrimaryKeyDef{PkeyColName: "amount", Names: []string{"amount"}}
	filter := int64FoldedFilter(">=", 2)
	filter.CandidateFilter = true
	_, _, _, block, _, compiled, _ := CompileFilterExpr(filter, table, nil)
	require.True(t, compiled)
	meta := objectio.BuildMetaData(1, 1)
	zm := index.NewZM(types.T_int64, 0)
	value := int64(3)
	index.UpdateZM(zm, types.EncodeInt64(&value))
	meta.GetBlockMeta(0).MustGetColumn(0).SetZoneMap(zm)
	_, matches, err := block(0, meta.GetBlockMeta(0), nil)
	require.NoError(t, err)
	require.True(t, matches)
	base, err := ConstructBasePKFilter(filter, table, proc.Mp())
	require.NoError(t, err)
	defer base.Cleanup()
	require.True(t, base.Valid)
}

func TestCandidateUnsupportedPhysicalBoundsFailOpen(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, tc := range []struct {
		name, op string
		kind     types.T
		bound    []byte
	}{
		{"padded char", "=", types.T_char, []byte("MO")},
		{"float domain", "=", types.T_float64, types.EncodeFloat64(new(float64))},
		{"malformed integer", "=", types.T_int64, []byte{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := plan.Type{Id: int32(tc.kind), Width: 64}
			table := decimalTableDef(typ, true)
			table.Pkey = &plan.PrimaryKeyDef{PkeyColName: "amount", Names: []string{"amount"}}
			col := &plan.Expr{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "amount"}}}
			bound := &plan.Expr{Typ: typ, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: tc.bound}}}
			filter := foldedFunction(tc.op, []*plan.Expr{col, bound})
			filter.CandidateFilter = true
			_, _, _, _, _, compiled, _ := CompileFilterExpr(filter, table, nil)
			require.False(t, compiled)
			base, err := ConstructBasePKFilter(filter, table, proc.Mp())
			require.NoError(t, err)
			defer base.Cleanup()
			require.False(t, base.Valid, "unsupported optional raw filtering must not discard rows")
		})
	}
}

func TestCandidateSupportedBytePrefixesRemainUsable(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	typ := plan.Type{Id: int32(types.T_varchar), Width: 64}
	table := decimalTableDef(typ, true)
	table.Pkey = &plan.PrimaryKeyDef{PkeyColName: "amount", Names: []string{"amount"}}
	for _, value := range [][]byte{bytes.Repeat([]byte{0xff}, 30), append(bytes.Repeat([]byte{0xff}, 30), 1), bytes.Repeat([]byte{'a'}, 31)} {
		col := &plan.Expr{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "amount"}}}
		bound := &plan.Expr{Typ: typ, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: value}}}
		filter := foldedFunction("prefix_eq", []*plan.Expr{col, bound})
		filter.CandidateFilter = true
		_, _, _, _, _, compiled, _ := CompileFilterExpr(filter, table, nil)
		require.True(t, compiled)
		base, err := ConstructBasePKFilter(filter, table, proc.Mp())
		require.NoError(t, err)
		require.True(t, base.Valid)
		base.Cleanup()
	}
}

func TestByteCandidateReaderAndPKResults(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	typ := plan.Type{Id: int32(types.T_varchar), Width: 64}
	table := decimalTableDef(typ, true)
	table.Pkey = &plan.PrimaryKeyDef{PkeyColName: "amount", Names: []string{"amount"}}
	prefix := append(bytes.Repeat([]byte{0xff}, 30), 0)
	values := [][]byte{prefix, append(bytes.Clone(prefix), 0), append(bytes.Repeat([]byte{0xff}, 30), 1), append(bytes.Repeat([]byte{0xff}, 30), 2)}
	input := vector.NewVec(types.T_varchar.ToType())
	defer input.Free(proc.Mp())
	meta := objectio.BuildMetaData(uint16(len(values)), 1)
	all := index.NewZM(types.T_varchar, 0)
	for i, value := range values {
		require.NoError(t, vector.AppendBytes(input, value, false, proc.Mp()))
		zm := index.BuildZM(types.T_varchar, value)
		meta.GetBlockMeta(uint32(i)).MustGetColumn(0).SetZoneMap(zm)
		index.UpdateZM(all, value)
	}
	// Object summaries from older writers could lose a block's truncation bit.
	// The actual block metadata retains its flag; exercise the legacy summary
	// together with real block pruning and ordered seeks.
	legacy := index.NewZM(types.T_varchar, 0)
	index.UpdateZM(legacy, all.GetMinBuf())
	index.UpdateZM(legacy, all.GetMaxBuf())
	require.False(t, legacy.MaxTruncated())
	meta.MustGetColumn(0).SetZoneMap(legacy)
	for _, tc := range []struct {
		name   string
		bounds [][]byte
		want   []int64
	}{
		{"<", [][]byte{values[2]}, []int64{0, 1}},
		{">", [][]byte{values[2]}, []int64{3}},
		{"prefix_eq", [][]byte{prefix}, []int64{0, 1}},
		{"prefix_between", [][]byte{prefix, values[2]}, []int64{0, 1, 2}},
	} {
		for _, marked := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/marked=%t", tc.name, marked), func(t *testing.T) {
				args := []*plan.Expr{{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "amount"}}}}
				for _, value := range tc.bounds {
					args = append(args, &plan.Expr{Typ: typ, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: value}}})
				}
				expr := foldedFunction(tc.name, args)
				expr.CandidateFilter = marked
				_, _, _, block, seek, compiled, _ := CompileFilterExpr(expr, table, nil)
				require.True(t, compiled)
				for _, row := range tc.want {
					stop, match, err := block(int(row), meta.GetBlockMeta(uint32(row)), nil)
					require.NoError(t, err)
					require.False(t, stop)
					require.True(t, match, "reader must retain matching block %d", row)
				}
				if seek != nil {
					require.LessOrEqual(t, seek(meta), int(tc.want[0]))
				}
				base, err := ConstructBasePKFilter(expr, table, proc.Mp())
				require.NoError(t, err)
				defer base.Cleanup()
				require.True(t, base.Valid)
				persisted, err := ConstructBlockPKFilter(false, base, nil)
				require.NoError(t, err)
				require.True(t, persisted.Valid)
				require.Equal(t, tc.want, persisted.SortedSearchFunc(containers.Vectors{*input}))
				if persisted.Cleanup != nil {
					defer persisted.Cleanup()
				}
				memory, err := NewMemPKFilter(table, types.MaxTs().ToTimestamp(), newTestPackerPool(), base, engine.FilterHint{})
				require.NoError(t, err)
				require.True(t, memory.Valid())
				packer := types.NewPacker()
				defer packer.Close()
				skipped := objectio.GetReusableBitmap()
				defer skipped.Release()
				memory.FilterVector(input, packer, &skipped)
				for i := range values {
					want := false
					for _, row := range tc.want {
						want = want || row == int64(i)
					}
					require.Equal(t, !want, skipped.Contains(uint64(i)), "memory row %d", i)
				}
			})
		}
	}
}

func TestByteEqualityPrunesAmbiguousObjectBeforeLoad(t *testing.T) {
	typ := plan.Type{Id: int32(types.T_varbinary), Width: 64}
	table := decimalTableDef(typ, true)
	table.Pkey = &plan.PrimaryKeyDef{PkeyColName: "amount", Names: []string{"amount"}}
	ff := bytes.Repeat([]byte{0xff}, 30)
	for _, tc := range []struct {
		name  string
		value []byte
	}{
		{"flagged", append(bytes.Clone(ff), 2)},
		{"exact", ff},
		{"rounded", append(append(bytes.Clone(ff[:29]), 0xfe), 1)},
	} {
		for _, marked := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/marked=%t", tc.name, marked), func(t *testing.T) {
				col := &plan.Expr{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "amount"}}}
				bound := &plan.Expr{Typ: typ, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: []byte("a")}}}
				expr := foldedFunction("=", []*plan.Expr{col, bound})
				expr.CandidateFilter = marked
				fast, _, _, _, _, compiled, _ := CompileFilterExprs([]*plan.Expr{expr}, table, nil)
				require.True(t, compiled)
				require.NotNil(t, fast)
				stats := objectio.NewObjectStats()
				require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 1))
				require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
				require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(stats, index.BuildZM(types.T_varbinary, tc.value)))
				match, err := fast(stats)
				require.NoError(t, err)
				require.False(t, match)
				var blocks objectio.BlockInfoSlice
				_, loads, _, _, _, _, fastTotal, fastHits, err := FilterObjects(t.Context(), engine.RangesParam{}, fast, nil, nil, nil, nil, nil, []objectio.ObjectStats{*stats}, nil, &blocks, false, nil, nil)
				require.NoError(t, err)
				require.Zero(t, loads)
				require.Equal(t, 1, fastTotal)
				require.Equal(t, 1, fastHits)
				require.Zero(t, blocks.Len())
			})
		}
	}
}

func TestCandidateVectorWithoutProducerProofFailsOpen(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	typ := plan.Type{Id: int32(types.T_int64)}
	table := decimalTableDef(typ, true)
	table.Pkey = &plan.PrimaryKeyDef{PkeyColName: "amount", Names: []string{"amount"}}
	column := &plan.Expr{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "amount"}}}
	vector := &plan.Expr{Typ: typ, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{Data: []byte{1}}}}
	filter := foldedFunction("in", []*plan.Expr{column, vector})
	filter.CandidateFilter = true
	_, _, _, _, _, compiled, _ := CompileFilterExpr(filter, table, nil)
	require.False(t, compiled)
	base, err := ConstructBasePKFilter(filter, table, proc.Mp())
	require.NoError(t, err)
	defer base.Cleanup()
	require.False(t, base.Valid)
}

func TestCandidateNullArmWithMissingColumnFailsOpen(t *testing.T) {
	typ := plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 4}
	table := decimalTableDef(typ, false)
	valid := decimalFoldedFilter(t, typ, "<", decimalBound{"1", 0})
	missing := &plan.Expr{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "missing"}}}
	filter := foldedFunction("or", []*plan.Expr{valid, foldedFunction("isnull", []*plan.Expr{missing})})
	filter.CandidateFilter = true
	_, _, _, _, _, compiled, _ := CompileFilterExpr(filter, table, nil)
	require.False(t, compiled)
}

func TestUnmarkedORPreservesMetadataError(t *testing.T) {
	typ := plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 4}
	table := decimalTableDef(typ, false)
	table.Cols[0].ClusterBy = true
	less := decimalFoldedFilter(t, typ, "<", decimalBound{"1", 0})
	greater := decimalFoldedFilter(t, typ, ">", decimalBound{"3", 0})
	expr := foldedFunction("or", []*plan.Expr{less, greater})
	stats := decimalObjectStats(t, typ, "2.0000")
	want := errors.New("metadata read failed")
	for _, marked := range []bool{false, true} {
		expr.CandidateFilter = marked
		fast, _, _, _, _, compiled, _ := CompileFilterExpr(expr, table, nil)
		require.True(t, compiled)
		loads := 0
		load := func(context.Context, *objectio.ObjectStats, objectio.ObjectMeta, objectio.BloomFilter) (objectio.ObjectMeta, objectio.BloomFilter, error) {
			loads++
			return nil, nil, want
		}
		var blocks objectio.BlockInfoSlice
		_, _, _, _, _, _, _, _, err := FilterObjects(t.Context(), engine.RangesParam{}, fast, load, nil, nil, nil, nil, []objectio.ObjectStats{*stats}, nil, &blocks, false, nil, nil)
		if marked {
			require.NoError(t, err)
			require.Zero(t, loads)
		} else {
			require.ErrorIs(t, err, want)
			require.Equal(t, 1, loads)
		}
	}
}
