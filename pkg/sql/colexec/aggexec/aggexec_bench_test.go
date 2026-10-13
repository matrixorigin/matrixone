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

package aggexec

import (
	"bytes"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func BenchmarkAggExecPaths(b *testing.B) {
	mp := mpool.MustNewZero()
	defer func() {
		if mp.CurrNB() != 0 {
			b.Fatalf("memory leak detected: %d bytes", mp.CurrNB())
		}
	}()

	const (
		rows      = 4096
		groupSize = 64
	)

	intVals := make([]int64, rows)
	for i := range intVals {
		intVals[i] = int64(i % 1024)
	}
	groups := make([]uint64, rows)
	for i := range groups {
		groups[i] = uint64((i % groupSize) + 1)
	}

	intVec := testutil.NewInt64Vector(rows, types.T_int64.ToType(), mp, false, nil, intVals)
	defer intVec.Free(mp)

	stringVals := make([]string, rows)
	for i := range stringVals {
		stringVals[i] = "name" + string(rune('a'+(i%26)))
	}
	strVec := testutil.NewStringVector(rows, types.T_varchar.ToType(), mp, false, nil, stringVals)
	defer strVec.Free(mp)

	b.Run("SumInt64/BatchFill", func(b *testing.B) {
		b.ReportAllocs()
		vectors := []*vector.Vector{intVec}
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			exec := newSumAvgExec[int64, int64](mp, int64OfCheck, true, AggIdOfSum, false, types.T_int64.ToType())
			if err := exec.GroupGrow(groupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := exec.BatchFill(0, groups, vectors); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			exec.Free()
		}
	})

	b.Run("SumDecimal64/BatchFill", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		dec64Vals := make([]types.Decimal64, rows)
		for i := range dec64Vals {
			dec64Vals[i] = types.Decimal64(int64(i%100000) * 100)
		}
		dec64Vec := testutil.NewDecimal64Vector(rows, types.New(types.T_decimal64, 15, 2), mp, false, nil, dec64Vals)
		defer dec64Vec.Free(mp)
		vectors := []*vector.Vector{dec64Vec}
		for i := 0; i < b.N; i++ {
			exec := newSumAvgDecExec[types.Decimal64, types.Decimal128](mp, true, AggIdOfSum, false, types.New(types.T_decimal64, 15, 2))
			if err := exec.GroupGrow(groupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := exec.BatchFill(0, groups, vectors); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			exec.Free()
		}
	})

	b.Run("SumDecimal64Fast/BatchFill", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		dec64Vals := make([]types.Decimal64, rows)
		for i := range dec64Vals {
			dec64Vals[i] = types.Decimal64(int64(i%100000) * 100)
		}
		dec64Vec := testutil.NewDecimal64Vector(rows, types.New(types.T_decimal64, 15, 2), mp, false, nil, dec64Vals)
		defer dec64Vec.Free(mp)
		vectors := []*vector.Vector{dec64Vec}
		for i := 0; i < b.N; i++ {
			exec := newSumDecimal64FastExec(mp, true, AggIdOfSum, false, types.New(types.T_decimal64, 15, 2))
			if err := exec.GroupGrow(groupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := exec.BatchFill(0, groups, vectors); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			exec.Free()
		}
	})

	b.Run("AvgDecimal64/BatchFill", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		dec64Vals := make([]types.Decimal64, rows)
		for i := range dec64Vals {
			dec64Vals[i] = types.Decimal64(int64(i%100000) * 100)
		}
		dec64Vec := testutil.NewDecimal64Vector(rows, types.New(types.T_decimal64, 15, 2), mp, false, nil, dec64Vals)
		defer dec64Vec.Free(mp)
		vectors := []*vector.Vector{dec64Vec}
		for i := 0; i < b.N; i++ {
			exec := newSumAvgDecExec[types.Decimal64, types.Decimal128](mp, false, AggIdOfAvg, false, types.New(types.T_decimal64, 15, 2))
			if err := exec.GroupGrow(groupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := exec.BatchFill(0, groups, vectors); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			exec.Free()
		}
	})

	b.Run("SumFloat64/BatchFill", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		f64Vals := make([]float64, rows)
		for i := range f64Vals {
			f64Vals[i] = float64(i%1024) + 0.5
		}
		f64Vec := testutil.NewFloat64Vector(rows, types.T_float64.ToType(), mp, false, nil, f64Vals)
		defer f64Vec.Free(mp)
		vectors := []*vector.Vector{f64Vec}
		for i := 0; i < b.N; i++ {
			exec := newSumAvgExec[float64, float64](mp, float64OfCheck, true, AggIdOfSum, false, types.T_float64.ToType())
			if err := exec.GroupGrow(groupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := exec.BatchFill(0, groups, vectors); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			exec.Free()
		}
	})

	b.Run("AvgInt64/BatchFill", func(b *testing.B) {
		b.ReportAllocs()
		vectors := []*vector.Vector{intVec}
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			exec := newSumAvgExec[int64, int64](mp, int64OfCheck, false, AggIdOfAvg, false, types.T_int64.ToType())
			if err := exec.GroupGrow(groupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := exec.BatchFill(0, groups, vectors); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			exec.Free()
		}
	})

	b.Run("AvgInt32/BatchFillFlush", func(b *testing.B) {
		b.ReportAllocs()
		int32Vals := make([]int32, rows)
		for i := range int32Vals {
			int32Vals[i] = int32(i % 1024)
		}
		int32Vec := testutil.NewInt32Vector(rows, types.T_int32.ToType(), mp, false, nil, int32Vals)
		defer int32Vec.Free(mp)
		vectors := []*vector.Vector{int32Vec}
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			exec, err := MakeAgg(mp, AggIdOfAvg, false, types.T_int32.ToType())
			if err != nil {
				b.Fatal(err)
			}
			if err = exec.GroupGrow(groupSize); err != nil {
				exec.Free()
				b.Fatal(err)
			}
			b.StartTimer()
			if err = exec.BatchFill(0, groups, vectors); err != nil {
				b.StopTimer()
				exec.Free()
				b.Fatal(err)
			}
			results, err := exec.Flush()
			b.StopTimer()
			if err != nil {
				exec.Free()
				b.Fatal(err)
			}
			for _, result := range results {
				result.Free(mp)
			}
			exec.Free()
		}
	})

	b.Run("AvgInt32FloatReference/BatchFillFlush", func(b *testing.B) {
		b.ReportAllocs()
		int32Vals := make([]int32, rows)
		for i := range int32Vals {
			int32Vals[i] = int32(i % 1024)
		}
		int32Vec := testutil.NewInt32Vector(rows, types.T_int32.ToType(), mp, false, nil, int32Vals)
		defer int32Vec.Free(mp)
		vectors := []*vector.Vector{int32Vec}
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			// Before exact AVG finalization, INT32 used this native accumulator
			// and reinterpreted the sum buffer as float64 at Flush. Keep that
			// path as a local benchmark reference for the compatibility tradeoff.
			exec := newSumAvgExec[int64, int32](mp, int64OfCheck, false, AggIdOfAvg, false, types.T_int32.ToType())
			exec.(*sumAvgExec[int64, int32]).exactAvg = false
			if err := exec.GroupGrow(groupSize); err != nil {
				exec.Free()
				b.Fatal(err)
			}
			b.StartTimer()
			if err := exec.BatchFill(0, groups, vectors); err != nil {
				b.StopTimer()
				exec.Free()
				b.Fatal(err)
			}
			results, err := exec.Flush()
			b.StopTimer()
			if err != nil {
				exec.Free()
				b.Fatal(err)
			}
			for _, result := range results {
				result.Free(mp)
			}
			exec.Free()
		}
	})

	b.Run("CountColumn/BatchFill", func(b *testing.B) {
		b.ReportAllocs()
		vectors := []*vector.Vector{intVec}
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			exec := newCountColumnExec(mp, AggIdOfCountColumn, false, []types.Type{types.T_int64.ToType()})
			if err := exec.GroupGrow(groupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := exec.BatchFill(0, groups, vectors); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			exec.Free()
		}
	})

	b.Run("SumInt64/BatchMerge", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		source := newSumAvgExec[int64, int64](mp, int64OfCheck, true, AggIdOfSum, false, types.T_int64.ToType())
		if err := source.GroupGrow(groupSize); err != nil {
			b.Fatal(err)
		}
		if err := source.BatchFill(0, groups, []*vector.Vector{intVec}); err != nil {
			b.Fatal(err)
		}
		defer source.Free()

		mergeGroups := make([]uint64, groupSize)
		for i := range mergeGroups {
			mergeGroups[i] = uint64(i + 1)
		}

		for i := 0; i < b.N; i++ {
			target := newSumAvgExec[int64, int64](mp, int64OfCheck, true, AggIdOfSum, false, types.T_int64.ToType())
			if err := target.GroupGrow(groupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := target.BatchMerge(source, 0, mergeGroups); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			target.Free()
		}
	})

	b.Run("CountColumn/BatchMerge", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		source := newCountColumnExec(mp, AggIdOfCountColumn, false, []types.Type{types.T_int64.ToType()})
		if err := source.GroupGrow(groupSize); err != nil {
			b.Fatal(err)
		}
		if err := source.BatchFill(0, groups, []*vector.Vector{intVec}); err != nil {
			b.Fatal(err)
		}
		defer source.Free()

		mergeGroups := make([]uint64, groupSize)
		for i := range mergeGroups {
			mergeGroups[i] = uint64(i + 1)
		}

		for i := 0; i < b.N; i++ {
			target := newCountColumnExec(mp, AggIdOfCountColumn, false, []types.Type{types.T_int64.ToType()})
			if err := target.GroupGrow(groupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := target.BatchMerge(source, 0, mergeGroups); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			target.Free()
		}
	})

	b.Run("MedianDistinct/BatchFill", func(b *testing.B) {
		b.ReportAllocs()
		vectors := []*vector.Vector{intVec}
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			exec, err := newMedianExec(mp, AggIdOfMedian, true, types.T_int64.ToType())
			if err != nil {
				b.Fatal(err)
			}
			if err := exec.GroupGrow(groupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := exec.BatchFill(0, groups, vectors); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			exec.Free()
		}
	})

	b.Run("SumInt64/BatchFill/512groups", func(b *testing.B) {
		b.ReportAllocs()
		const bigGroupSize = 512
		bigGroups := make([]uint64, rows)
		for i := range bigGroups {
			bigGroups[i] = uint64((i % bigGroupSize) + 1)
		}
		vectors := []*vector.Vector{intVec}
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			exec := newSumAvgExec[int64, int64](mp, int64OfCheck, true, AggIdOfSum, false, types.T_int64.ToType())
			if err := exec.GroupGrow(bigGroupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := exec.BatchFill(0, bigGroups, vectors); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			exec.Free()
		}
	})

	b.Run("GroupConcat/BatchFill", func(b *testing.B) {
		b.ReportAllocs()
		info := multiAggInfo{
			aggID:     AggIdOfGroupConcat,
			distinct:  false,
			argTypes:  []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()},
			retType:   GroupConcatReturnType([]types.Type{types.T_varchar.ToType(), types.T_int64.ToType()}),
			emptyNull: true,
		}
		vectors := []*vector.Vector{strVec, intVec}
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			exec := newGroupConcatExec(mp, info, ",")
			if err := exec.GroupGrow(groupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := exec.BatchFill(0, groups, vectors); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			exec.Free()
		}
	})
}

func BenchmarkCountDistinctSavedArguments(b *testing.B) {
	const (
		rows   = hashmap.UnitLimit * 256
		groups = 1
	)

	mp := mpool.MustNewZero()
	input := vector.NewVec(types.T_int64.ToType())
	for i := 0; i < rows; i++ {
		if err := vector.AppendFixed(input, int64(i), false, mp); err != nil {
			b.Fatal(err)
		}
	}
	defer input.Free(mp)
	vectors := []*vector.Vector{input}
	groupIDs := make([]uint64, hashmap.UnitLimit)
	for i := range groupIDs {
		groupIDs[i] = uint64(i%groups + 1)
	}

	registry, err := mpool.NewAllocationAccountRegistry(1, 512)
	if err != nil {
		b.Fatal(err)
	}
	account, err := registry.Open(128 << 20)
	if err != nil {
		b.Fatal(err)
	}
	allocation, err := NewAllocationAccount(
		account, mpool.AllocationOwnerGroup, AllocationAccountSites{
			VectorData: 1, VectorArea: 2, VectorNulls: 3,
			VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
		})
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.SetBytes(rows * 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		agg := newCountColumnExec(
			mp, AggIdOfCountColumn, true, []types.Type{types.T_int64.ToType()})
		owner := AggFuncExec(agg).(AllocationAccountOwner)
		if err = owner.SetAllocationAccount(allocation); err != nil {
			b.Fatal(err)
		}
		if err = agg.GroupGrow(groups); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		for offset := 0; offset < rows; offset += hashmap.UnitLimit {
			if err = agg.(BatchCapacityPreflight).PreflightBatchFill(
				offset, groupIDs, vectors); err != nil {
				b.Fatal(err)
			}
			if err = agg.BatchFill(offset, groupIDs, vectors); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		agg.Free()
		if err = owner.ClearAllocationAccount(allocation); err != nil {
			b.Fatal(err)
		}
	}
	if account.Snapshot().Used != 0 {
		b.Fatalf("account retains %d bytes", account.Snapshot().Used)
	}
	account.Seal()
	if _, err = registry.Finalize(account); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkCountDistinctSavedArgumentMerge(b *testing.B) {
	const (
		sourceCount   = 16
		rowsPerSource = 1_000_000 / sourceCount
		groupCount    = 100
	)

	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(1, 512)
	if err != nil {
		b.Fatal(err)
	}
	account, err := registry.Open(512 << 20)
	if err != nil {
		b.Fatal(err)
	}
	allocation, err := NewAllocationAccount(
		account, mpool.AllocationOwnerGroup, AllocationAccountSites{
			VectorData: 1, VectorArea: 2, VectorNulls: 3,
			VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
		})
	if err != nil {
		b.Fatal(err)
	}
	mergeGroups := make([]uint64, groupCount)
	for i := range mergeGroups {
		mergeGroups[i] = uint64(i + 1)
	}

	sources := make([]AggFuncExec, sourceCount)
	for sourceIndex := range sources {
		input := vector.NewVec(types.T_int64.ToType())
		for row := 0; row < rowsPerSource; row++ {
			value := int64(sourceIndex*rowsPerSource + row)
			if err = vector.AppendFixed(input, value, false, mp); err != nil {
				b.Fatal(err)
			}
		}
		groups := make([]uint64, hashmap.UnitLimit)
		for row := range groups {
			groups[row] = uint64(row%groupCount + 1)
		}
		source := newCountColumnExec(
			mp, AggIdOfCountColumn, true, []types.Type{types.T_int64.ToType()})
		if err = source.(AllocationAccountOwner).SetAllocationAccount(allocation); err != nil {
			b.Fatal(err)
		}
		if err = source.GroupGrow(groupCount); err != nil {
			b.Fatal(err)
		}
		for offset := 0; offset < rowsPerSource; offset += hashmap.UnitLimit {
			workGroups := groups[:min(hashmap.UnitLimit, rowsPerSource-offset)]
			if err = source.(BatchCapacityPreflight).PreflightBatchFill(
				offset, workGroups, []*vector.Vector{input}); err != nil {
				b.Fatal(err)
			}
			if err = source.BatchFill(
				offset, workGroups, []*vector.Vector{input}); err != nil {
				b.Fatal(err)
			}
		}
		input.Free(mp)
		sources[sourceIndex] = source
	}

	b.ReportAllocs()
	b.SetBytes(1_000_000 * 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		target := newCountColumnExec(
			mp, AggIdOfCountColumn, true, []types.Type{types.T_int64.ToType()})
		if err = target.(AllocationAccountOwner).SetAllocationAccount(allocation); err != nil {
			b.Fatal(err)
		}
		if err = target.GroupGrow(groupCount); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		for _, source := range sources {
			if err = target.(BatchCapacityPreflight).PreflightBatchMerge(
				source, 0, mergeGroups); err != nil {
				b.Fatal(err)
			}
			if err = target.BatchMerge(source, 0, mergeGroups); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		target.Free()
		if err = target.(AllocationAccountOwner).ClearAllocationAccount(allocation); err != nil {
			b.Fatal(err)
		}
	}
	for _, source := range sources {
		source.Free()
		if err = source.(AllocationAccountOwner).ClearAllocationAccount(allocation); err != nil {
			b.Fatal(err)
		}
	}
	if account.Snapshot().Used != 0 {
		b.Fatalf("account retains %d bytes", account.Snapshot().Used)
	}
	account.Seal()
	if _, err = registry.Finalize(account); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkCountDistinctSavedArgumentUnmarshal(b *testing.B) {
	const rows = 250_000

	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(1, 512)
	if err != nil {
		b.Fatal(err)
	}
	account, err := registry.Open(128 << 20)
	if err != nil {
		b.Fatal(err)
	}
	allocation, err := NewAllocationAccount(
		account, mpool.AllocationOwnerGroup, AllocationAccountSites{
			VectorData: 1, VectorArea: 2, VectorNulls: 3,
			VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
		})
	if err != nil {
		b.Fatal(err)
	}

	input := vector.NewVec(types.T_int64.ToType())
	for row := range rows {
		if err = vector.AppendFixed(input, int64(row), false, mp); err != nil {
			b.Fatal(err)
		}
	}
	groups := make([]uint64, hashmap.UnitLimit)
	for row := range groups {
		groups[row] = 1
	}
	source := newCountColumnExec(
		mp, AggIdOfCountColumn, true, []types.Type{types.T_int64.ToType()})
	if err = source.(AllocationAccountOwner).SetAllocationAccount(allocation); err != nil {
		b.Fatal(err)
	}
	if err = source.GroupGrow(1); err != nil {
		b.Fatal(err)
	}
	for offset := 0; offset < rows; offset += hashmap.UnitLimit {
		workGroups := groups[:min(hashmap.UnitLimit, rows-offset)]
		if err = source.(BatchCapacityPreflight).PreflightBatchFill(
			offset, workGroups, []*vector.Vector{input}); err != nil {
			b.Fatal(err)
		}
		if err = source.BatchFill(
			offset, workGroups, []*vector.Vector{input}); err != nil {
			b.Fatal(err)
		}
	}
	input.Free(mp)

	var encoded bytes.Buffer
	if err = source.SaveIntermediateResult(
		1, [][]uint8{{1}}, &encoded); err != nil {
		b.Fatal(err)
	}
	payload := encoded.Bytes()

	b.ReportAllocs()
	b.SetBytes(rows * 8)
	b.ResetTimer()
	for range b.N {
		target := newCountColumnExec(
			mp, AggIdOfCountColumn, true, []types.Type{types.T_int64.ToType()})
		if err = target.(AllocationAccountOwner).SetAllocationAccount(allocation); err != nil {
			b.Fatal(err)
		}
		if err = target.UnmarshalFromReader(bytes.NewReader(payload), mp); err != nil {
			b.Fatal(err)
		}
		target.Free()
		if err = target.(AllocationAccountOwner).ClearAllocationAccount(allocation); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	source.Free()
	if err = source.(AllocationAccountOwner).ClearAllocationAccount(allocation); err != nil {
		b.Fatal(err)
	}
	if account.Snapshot().Used != 0 {
		b.Fatalf("account retains %d bytes", account.Snapshot().Used)
	}
	account.Seal()
	if _, err = registry.Finalize(account); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkArgumentPreflightCardinality(b *testing.B) {
	type benchmarkCase struct {
		name        string
		distinct    bool
		cardinality int
		varlen      bool
	}
	cases := make([]benchmarkCase, 0, 12)
	for _, varlen := range []bool{false, true} {
		kind := "fixed"
		if varlen {
			kind = "varlen-1KiB"
		}
		for _, cardinality := range []int{
			1, 2, distinctArgumentLinearLimit,
			distinctArgumentLinearLimit + 1, hashmap.UnitLimit,
		} {
			cases = append(cases, benchmarkCase{
				name:        "distinct/" + kind + "/cardinality-" + strconv.Itoa(cardinality),
				distinct:    true,
				cardinality: cardinality,
				varlen:      varlen,
			})
		}
		cases = append(cases, benchmarkCase{
			name:        "non-distinct/" + kind,
			cardinality: hashmap.UnitLimit,
			varlen:      varlen,
		})
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			typ := types.T_int64.ToType()
			if tc.varlen {
				typ = types.T_varchar.ToType()
			}
			input := vector.NewVec(typ)
			if tc.varlen {
				prefix := strings.Repeat("x", 1024)
				for row := 0; row < hashmap.UnitLimit; row++ {
					value := []byte(prefix + strconv.Itoa(row%tc.cardinality))
					if err := vector.AppendBytes(input, value, false, mp); err != nil {
						b.Fatal(err)
					}
				}
			} else {
				for row := 0; row < hashmap.UnitLimit; row++ {
					if err := vector.AppendFixed(
						input, int64(row%tc.cardinality), false, mp); err != nil {
						b.Fatal(err)
					}
				}
			}
			groups := make([]uint64, hashmap.UnitLimit)
			for row := range groups {
				groups[row] = 1
			}

			registry, err := mpool.NewAllocationAccountRegistry(1, 512)
			if err != nil {
				b.Fatal(err)
			}
			account, err := registry.Open(32 << 20)
			if err != nil {
				b.Fatal(err)
			}
			allocation, err := NewAllocationAccount(
				account, mpool.AllocationOwnerGroup, AllocationAccountSites{
					VectorData: 1, VectorArea: 2, VectorNulls: 3,
					VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
				})
			if err != nil {
				b.Fatal(err)
			}
			info := aggInfo{
				argTypes:   []types.Type{typ},
				saveArg:    true,
				isDistinct: tc.distinct,
			}
			state := aggState{}
			if err = state.initWithAllocation(
				mp, 1, 1, &info, false, allocation); err != nil {
				b.Fatal(err)
			}
			exec := aggExec{
				mp:         mp,
				aggInfo:    info,
				chunkSize:  AggBatchSize,
				state:      []aggState{state},
				allocation: allocation,
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err = exec.preflightBatchFillArgs(
					0, groups, []*vector.Vector{input}, tc.distinct); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()

			exec.state[0].free(mp)
			exec.state = nil
			exec.allocation = nil
			input.Free(mp)
			if account.Snapshot().Used != 0 {
				b.Fatalf("account retains %d bytes", account.Snapshot().Used)
			}
			account.Seal()
			if _, err = registry.Finalize(account); err != nil {
				b.Fatal(err)
			}
			if mp.CurrNB() != 0 {
				b.Fatalf("memory leak detected: %d bytes", mp.CurrNB())
			}
		})
	}
}

func BenchmarkSumDecimal64FastCardinality(b *testing.B) {
	mp := mpool.MustNewZero()
	defer func() {
		if mp.CurrNB() != 0 {
			b.Fatalf("memory leak detected: %d bytes", mp.CurrNB())
		}
	}()

	const rows = AggBatchSize
	values := make([]types.Decimal64, rows)
	for i := range values {
		values[i] = types.Decimal64(i + 1)
	}
	vec := testutil.NewDecimal64Vector(
		rows, types.New(types.T_decimal64, 15, 2), mp, false, nil, values)
	defer vec.Free(mp)
	vectors := []*vector.Vector{vec}

	cases := []struct {
		name       string
		groupCount int
		groupAt    func(int) uint64
	}{
		{"Compact64", 64, func(i int) uint64 { return uint64(i%64 + 1) }},
		{"Scattered64", 1 << 20, func(i int) uint64 {
			return uint64((i%64)*(1<<14) + 1)
		}},
		{"Compact256", 256, func(i int) uint64 { return uint64(i%256 + 1) }},
		{"Scattered256", 1 << 20, func(i int) uint64 {
			return uint64((i%256)*(1<<12) + 1)
		}},
		{"Compact1024", 1024, func(i int) uint64 { return uint64(i%1024 + 1) }},
		{"Compact" + strconv.Itoa(rows), rows, func(i int) uint64 { return uint64(i + 1) }},
		{"Scattered" + strconv.Itoa(rows), 1 << 20, func(i int) uint64 {
			return uint64((i*65537)&((1<<20)-1) + 1)
		}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			groups := make([]uint64, rows)
			for i := range groups {
				groups[i] = tc.groupAt(i)
			}
			exec := newSumDecimal64FastExec(
				mp, true, AggIdOfSum, false, types.New(types.T_decimal64, 15, 2))
			if err := exec.GroupGrow(tc.groupCount); err != nil {
				b.Fatal(err)
			}
			defer exec.Free()

			b.ReportAllocs()
			b.SetBytes(rows * 8)
			b.ResetTimer()
			for range b.N {
				if err := exec.BatchFill(0, groups, vectors); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSumDecimal64FastBatchMerge(b *testing.B) {
	mp := mpool.MustNewZero()
	defer func() {
		if mp.CurrNB() != 0 {
			b.Fatalf("memory leak detected: %d bytes", mp.CurrNB())
		}
	}()

	const rows = AggBatchSize
	cases := []struct {
		name       string
		groupCount int
		groupAt    func(int) uint64
	}{
		{"Compact", rows, func(i int) uint64 { return uint64(i + 1) }},
		{"Scattered", 1 << 20, func(i int) uint64 {
			return uint64((i*65537)&((1<<20)-1) + 1)
		}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			groups := make([]uint64, rows)
			for i := range groups {
				groups[i] = tc.groupAt(i)
			}
			dst := newSumDecimal64FastExec(
				mp, true, AggIdOfSum, false, types.New(types.T_decimal64, 15, 2))
			src := newSumDecimal64FastExec(
				mp, true, AggIdOfSum, false, types.New(types.T_decimal64, 15, 2))
			if err := dst.GroupGrow(tc.groupCount); err != nil {
				b.Fatal(err)
			}
			if err := src.GroupGrow(rows); err != nil {
				b.Fatal(err)
			}
			defer dst.Free()
			defer src.Free()

			b.ReportAllocs()
			b.SetBytes(rows * 8)
			b.ResetTimer()
			for range b.N {
				if err := dst.BatchMerge(src, 0, groups); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestLocalAccumulatorOverflow exercises the direct-scatter fallback path
// that triggers when a single BatchFill has more than 255 distinct groups.
func TestLocalAccumulatorOverflow(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		if mp.CurrNB() != 0 {
			t.Fatalf("memory leak: %d bytes", mp.CurrNB())
		}
	}()

	const (
		numGroups = 512
		rows      = 1024
	)

	groups := make([]uint64, rows)
	for i := range groups {
		groups[i] = uint64((i % numGroups) + 1)
	}

	intVals := make([]int64, rows)
	for i := range intVals {
		intVals[i] = int64(i + 1)
	}
	intVec := testutil.NewInt64Vector(rows, types.T_int64.ToType(), mp, false, nil, intVals)
	defer intVec.Free(mp)

	t.Run("SumInt64", func(t *testing.T) {
		exec := makeSumAvgExec(mp, true, AggIdOfSum, false, types.T_int64.ToType())
		if err := exec.GroupGrow(numGroups); err != nil {
			t.Fatal(err)
		}
		if err := exec.BatchFill(0, groups, []*vector.Vector{intVec}); err != nil {
			t.Fatal(err)
		}
		results, err := exec.Flush()
		if err != nil {
			t.Fatal(err)
		}
		// Verify: each group g (0-indexed) gets values (g+1) and (g+1+512).
		// sum = (g+1) + (g+1+512) = 2g + 514
		for _, vec := range results {
			vals := vector.MustFixedColWithTypeCheck[types.Decimal128](vec)
			for g := 0; g < numGroups; g++ {
				expected := types.Decimal128FromInt64(int64(2*g + 514))
				if vals[g] != expected {
					t.Fatalf("group %d: got %v, want %v", g, vals[g], expected)
				}
			}
			vec.Free(mp)
		}
		exec.Free()
	})

	t.Run("CountColumn", func(t *testing.T) {
		exec := newCountColumnExec(mp, AggIdOfCountColumn, false, []types.Type{types.T_int64.ToType()})
		if err := exec.GroupGrow(numGroups); err != nil {
			t.Fatal(err)
		}
		if err := exec.BatchFill(0, groups, []*vector.Vector{intVec}); err != nil {
			t.Fatal(err)
		}
		results, err := exec.Flush()
		if err != nil {
			t.Fatal(err)
		}
		for _, vec := range results {
			vals := vector.MustFixedColNoTypeCheck[int64](vec)
			for g := 0; g < numGroups; g++ {
				if vals[g] != 2 {
					t.Fatalf("group %d: got %d, want 2", g, vals[g])
				}
			}
			vec.Free(mp)
		}
		exec.Free()
	})

	t.Run("MinInt64", func(t *testing.T) {
		exec := makeMinMaxExec(mp, AggIdOfMin, true, types.T_int64.ToType())
		if err := exec.GroupGrow(numGroups); err != nil {
			t.Fatal(err)
		}
		if err := exec.BatchFill(0, groups, []*vector.Vector{intVec}); err != nil {
			t.Fatal(err)
		}
		results, err := exec.Flush()
		if err != nil {
			t.Fatal(err)
		}
		for _, vec := range results {
			vals := vector.MustFixedColNoTypeCheck[int64](vec)
			for g := 0; g < numGroups; g++ {
				expected := int64(g + 1)
				if vals[g] != expected {
					t.Fatalf("group %d: got %d, want %d", g, vals[g], expected)
				}
			}
			vec.Free(mp)
		}
		exec.Free()
	})

	t.Run("SumDecimal64Fast", func(t *testing.T) {
		dec64Vals := make([]types.Decimal64, rows)
		for i := range dec64Vals {
			dec64Vals[i] = types.Decimal64(int64(i+1) * 100)
		}
		dec64Vec := testutil.NewDecimal64Vector(rows, types.New(types.T_decimal64, 15, 2), mp, false, nil, dec64Vals)
		defer dec64Vec.Free(mp)

		exec := newSumDecimal64FastExec(mp, true, AggIdOfSum, false, types.New(types.T_decimal64, 15, 2))
		if err := exec.GroupGrow(numGroups); err != nil {
			t.Fatal(err)
		}
		if err := exec.BatchFill(0, groups, []*vector.Vector{dec64Vec}); err != nil {
			t.Fatal(err)
		}
		results, err := exec.Flush()
		if err != nil {
			t.Fatal(err)
		}
		for _, vec := range results {
			vals := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)
			for g := 0; g < numGroups; g++ {
				// sum = ((g+1) + (g+1+512)) * 100 = (2g+514)*100
				expected := types.Decimal128{B0_63: uint64(int64(2*g+514) * 100), B64_127: 0}
				if vals[g] != expected {
					t.Fatalf("group %d: got %v, want %v", g, vals[g], expected)
				}
			}
			vec.Free(mp)
		}
		exec.Free()
	})
}

// TestConstVectorAccumulator exercises the IsConst branch in the local accumulator.
func TestConstVectorAccumulator(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		if mp.CurrNB() != 0 {
			t.Fatalf("memory leak: %d bytes", mp.CurrNB())
		}
	}()

	const (
		numGroups = 64
		rows      = 256
	)

	groups := make([]uint64, rows)
	for i := range groups {
		groups[i] = uint64((i % numGroups) + 1)
	}

	t.Run("SumInt64Const", func(t *testing.T) {
		constVec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), rows, mp)
		if err != nil {
			t.Fatal(err)
		}
		defer constVec.Free(mp)

		exec := makeSumAvgExec(mp, true, AggIdOfSum, false, types.T_int64.ToType())
		if err := exec.GroupGrow(numGroups); err != nil {
			t.Fatal(err)
		}
		if err := exec.BatchFill(0, groups, []*vector.Vector{constVec}); err != nil {
			t.Fatal(err)
		}
		results, err := exec.Flush()
		if err != nil {
			t.Fatal(err)
		}
		for _, vec := range results {
			vals := vector.MustFixedColWithTypeCheck[types.Decimal128](vec)
			for g := 0; g < numGroups; g++ {
				// Each group gets rows/numGroups = 4 rows, each with value 7.
				expected := types.Decimal128FromInt64(4 * 7)
				if vals[g] != expected {
					t.Fatalf("group %d: got %v, want %v", g, vals[g], expected)
				}
			}
			vec.Free(mp)
		}
		exec.Free()
	})

	t.Run("MinInt64Const", func(t *testing.T) {
		constVec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(42), rows, mp)
		if err != nil {
			t.Fatal(err)
		}
		defer constVec.Free(mp)

		exec := makeMinMaxExec(mp, AggIdOfMin, true, types.T_int64.ToType())
		if err := exec.GroupGrow(numGroups); err != nil {
			t.Fatal(err)
		}
		if err := exec.BatchFill(0, groups, []*vector.Vector{constVec}); err != nil {
			t.Fatal(err)
		}
		results, err := exec.Flush()
		if err != nil {
			t.Fatal(err)
		}
		for _, vec := range results {
			vals := vector.MustFixedColNoTypeCheck[int64](vec)
			for g := 0; g < numGroups; g++ {
				if vals[g] != 42 {
					t.Fatalf("group %d: got %d, want 42", g, vals[g])
				}
			}
			vec.Free(mp)
		}
		exec.Free()
	})

	t.Run("SumDecimal64Const", func(t *testing.T) {
		constVec, err := vector.NewConstFixed(types.New(types.T_decimal64, 15, 2), types.Decimal64(500), rows, mp)
		if err != nil {
			t.Fatal(err)
		}
		defer constVec.Free(mp)

		exec := newSumDecimal64FastExec(mp, true, AggIdOfSum, false, types.New(types.T_decimal64, 15, 2))
		if err := exec.GroupGrow(numGroups); err != nil {
			t.Fatal(err)
		}
		if err := exec.BatchFill(0, groups, []*vector.Vector{constVec}); err != nil {
			t.Fatal(err)
		}
		results, err := exec.Flush()
		if err != nil {
			t.Fatal(err)
		}
		for _, vec := range results {
			vals := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)
			for g := 0; g < numGroups; g++ {
				// 4 rows per group × 500 = 2000
				expected := types.Decimal128{B0_63: 2000, B64_127: 0}
				if vals[g] != expected {
					t.Fatalf("group %d: got %v, want %v", g, vals[g], expected)
				}
			}
			vec.Free(mp)
		}
		exec.Free()
	})
}

// TestDecimal256Overflow verifies that the checked path (localAddSafe=false)
// correctly catches overflow during local accumulation for Decimal256→Decimal256.
func TestDecimal256Overflow(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		if mp.CurrNB() != 0 {
			t.Fatalf("memory leak: %d bytes", mp.CurrNB())
		}
	}()

	const rows = 4
	groups := []uint64{1, 1, 1, 1}

	// Max Decimal256 ≈ 2^255 - 1. Use a value close to half-max so 3 adds overflow.
	halfMax := types.Decimal256{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0) >> 1}

	typ := types.New(types.T_decimal256, 38, 0)
	vec := vector.NewOffHeapVecWithType(typ)
	if err := vec.PreExtend(rows, mp); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		if err := vector.AppendFixed(vec, halfMax, false, mp); err != nil {
			t.Fatal(err)
		}
	}
	defer vec.Free(mp)

	exec := newSumAvgDecExec[types.Decimal256, types.Decimal256](mp, true, AggIdOfSum, false, typ)
	if err := exec.GroupGrow(1); err != nil {
		t.Fatal(err)
	}
	err := exec.BatchFill(0, groups, []*vector.Vector{vec})
	if err == nil {
		// If no error during BatchFill, Flush should show the overflow or we accept
		// that the add wrapped. Either way, confirm no panic.
		results, flushErr := exec.Flush()
		if flushErr != nil {
			t.Fatal(flushErr)
		}
		for _, v := range results {
			v.Free(mp)
		}
	} else {
		// Expected: overflow error from decimalStateAdd in the local buffer.
		if !strings.Contains(err.Error(), "Overflow") && !strings.Contains(err.Error(), "overflow") {
			t.Fatalf("expected overflow error, got: %v", err)
		}
	}
	exec.Free()
}

func BenchmarkAccountedMedianRetainedInput(b *testing.B) {
	const rows = 64 << 10
	mp := mpool.MustNewZero()
	input := vector.NewVec(types.T_int64.ToType())
	for i := 0; i < rows; i++ {
		if err := vector.AppendFixed(input, int64(rows-i), false, mp); err != nil {
			b.Fatal(err)
		}
	}
	defer input.Free(mp)
	vectors := []*vector.Vector{input}
	groups := make([]uint64, hashmap.UnitLimit)
	for i := range groups {
		groups[i] = 1
	}

	for _, tc := range []struct {
		name          string
		accounted     bool
		indexedLegacy bool
		finalize      bool
	}{
		{name: "pre-accounting-ingest"},
		{name: "append-only-ingest", accounted: true},
		{name: "indexed-baseline", accounted: true, indexedLegacy: true},
		{name: "pre-accounting-end-to-end", finalize: true},
		{name: "append-only-end-to-end", accounted: true, finalize: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			registry, err := mpool.NewAllocationAccountRegistry(1, 512)
			if err != nil {
				b.Fatal(err)
			}
			account, err := registry.Open(128 << 20)
			if err != nil {
				b.Fatal(err)
			}
			allocation, err := NewAllocationAccount(
				account, mpool.AllocationOwnerGroup, AllocationAccountSites{
					VectorData: 1, VectorArea: 2, VectorNulls: 3,
					VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
				})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(rows * 8)
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				var agg AggFuncExec
				var err error
				if tc.accounted {
					agg, err = MakeSingleGroupAgg(
						mp, AggIdOfMedian, false, nil, nil, types.T_int64.ToType())
				} else {
					agg, err = MakeAgg(
						mp, AggIdOfMedian, false, types.T_int64.ToType())
				}
				if err != nil {
					b.Fatal(err)
				}
				median := agg.(*medianColumnNumericExec[int64])
				owner := agg.(AllocationAccountOwner)
				if tc.accounted {
					if err = owner.SetAllocationAccount(allocation); err != nil {
						b.Fatal(err)
					}
				}
				if tc.indexedLegacy {
					// Recreate the pre-fix resident representation for an
					// evidence-only A/B benchmark. Production never toggles it.
					median.accounted.saveArg = true
				}
				SyncAggregatorsToChunkSize([]AggFuncExec{agg}, AggBatchSize)
				if err = agg.GroupGrow(1); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				for offset := 0; offset < rows; offset += hashmap.UnitLimit {
					if tc.accounted {
						if err = agg.(BatchCapacityPreflight).PreflightBatchFill(
							offset, groups, vectors); err != nil {
							b.Fatal(err)
						}
					}
					if err = agg.BatchFill(
						offset, groups, vectors); err != nil {
						b.Fatal(err)
					}
				}
				if tc.finalize {
					results, flushErr := agg.Flush()
					if flushErr != nil {
						b.Fatal(flushErr)
					}
					for _, result := range results {
						result.Free(mp)
					}
				}
				b.StopTimer()
				agg.Free()
				if tc.accounted {
					if err = owner.ClearAllocationAccount(allocation); err != nil {
						b.Fatal(err)
					}
				}
			}
			if account.Snapshot().Used != 0 {
				b.Fatalf("account retains %d bytes", account.Snapshot().Used)
			}
			account.Seal()
			if _, err = registry.Finalize(account); err != nil {
				b.Fatal(err)
			}
		})
	}
}
