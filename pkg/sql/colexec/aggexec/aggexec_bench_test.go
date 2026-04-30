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
	"testing"

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
		dec64Vals := make([]types.Decimal64, rows)
		for i := range dec64Vals {
			dec64Vals[i] = types.Decimal64(int64(i%100000) * 100)
		}
		dec64Vec := testutil.NewDecimal64Vector(rows, types.New(types.T_decimal64, 15, 2), mp, false, nil, dec64Vals)
		defer dec64Vec.Free(mp)
		vectors := []*vector.Vector{dec64Vec}
		b.StopTimer()
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

	b.Run("AvgDecimal64/BatchFill", func(b *testing.B) {
		b.ReportAllocs()
		dec64Vals := make([]types.Decimal64, rows)
		for i := range dec64Vals {
			dec64Vals[i] = types.Decimal64(int64(i%100000) * 100)
		}
		dec64Vec := testutil.NewDecimal64Vector(rows, types.New(types.T_decimal64, 15, 2), mp, false, nil, dec64Vals)
		defer dec64Vec.Free(mp)
		vectors := []*vector.Vector{dec64Vec}
		b.StopTimer()
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
		f64Vals := make([]float64, rows)
		for i := range f64Vals {
			f64Vals[i] = float64(i%1024) + 0.5
		}
		f64Vec := testutil.NewFloat64Vector(rows, types.T_float64.ToType(), mp, false, nil, f64Vals)
		defer f64Vec.Free(mp)
		vectors := []*vector.Vector{f64Vec}
		b.StopTimer()
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

	b.Run("CountColumn/BatchFill", func(b *testing.B) {
		b.ReportAllocs()
		vectors := []*vector.Vector{intVec}
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			exec := newCountColumnExec(mp, AggIdOfCountColumn, false, types.T_int64.ToType())
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

		b.StopTimer()
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
		source := newCountColumnExec(mp, AggIdOfCountColumn, false, types.T_int64.ToType())
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

		b.StopTimer()
		for i := 0; i < b.N; i++ {
			target := newCountColumnExec(mp, AggIdOfCountColumn, false, types.T_int64.ToType())
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
