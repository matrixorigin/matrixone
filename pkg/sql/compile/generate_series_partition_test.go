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

package compile

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func TestGenerateSeriesOffsetsPreserveSequence(t *testing.T) {
	tests := []struct {
		name         string
		start        int64
		end          int64
		step         int64
		parallelSize int
		want         [][2]int64
		wantOK       bool
	}{
		{
			name:         "positive step ending on bound",
			start:        0,
			end:          20,
			step:         2,
			parallelSize: 3,
			want:         [][2]int64{{0, 6}, {8, 14}, {16, 20}},
			wantOK:       true,
		},
		{
			name:         "positive step not ending on bound",
			start:        1,
			end:          10,
			step:         4,
			parallelSize: 2,
			want:         [][2]int64{{1, 5}, {9, 9}},
			wantOK:       true,
		},
		{
			name:         "negative step",
			start:        10,
			end:          -2,
			step:         -3,
			parallelSize: 3,
			want:         [][2]int64{{10, 7}, {4, 1}, {-2, -2}},
			wantOK:       true,
		},
		{
			name:         "negative step not ending on bound",
			start:        10,
			end:          0,
			step:         -3,
			parallelSize: 2,
			want:         [][2]int64{{10, 7}, {4, 1}},
			wantOK:       true,
		},
		{
			name:         "crosses signed boundary",
			start:        math.MinInt64,
			end:          math.MaxInt64,
			step:         math.MaxInt64,
			parallelSize: 3,
			want: [][2]int64{
				{math.MinInt64, math.MinInt64},
				{-1, -1},
				{math.MaxInt64 - 1, math.MaxInt64 - 1},
			},
			wantOK: true,
		},
		{
			name:         "descending across signed boundary",
			start:        math.MaxInt64,
			end:          math.MinInt64,
			step:         math.MinInt64,
			parallelSize: 2,
			want: [][2]int64{
				{math.MaxInt64, math.MaxInt64},
				{-1, -1},
			},
			wantOK: true,
		},
		{
			name:         "wrong direction",
			start:        1,
			end:          10,
			step:         -1,
			parallelSize: 2,
		},
		{
			name:         "fewer values than workers",
			start:        1,
			end:          3,
			step:         1,
			parallelSize: 4,
		},
		{
			name:         "cardinality overflow",
			start:        math.MinInt64,
			end:          math.MaxInt64,
			step:         1,
			parallelSize: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := generateSeriesOffsets(
				test.start, test.end, test.step, test.parallelSize)
			require.Equal(t, test.wantOK, ok)
			require.Equal(t, test.want, got)
			if !ok {
				return
			}
			stepMagnitude := uint64(test.step)
			if test.step < 0 {
				stepMagnitude = uint64(-(test.step + 1)) + 1
			}
			for i, bounds := range got {
				for _, bound := range bounds {
					var distance uint64
					if test.step > 0 {
						distance = uint64(bound) - uint64(test.start)
					} else {
						distance = uint64(test.start) - uint64(bound)
					}
					require.Zero(t, distance%stepMagnitude)
				}
				if i > 0 {
					require.Equal(t, got[i-1][1]+test.step, bounds[0])
				}
			}
		})
	}
}

func TestCompileGenerateSeriesParallelPartitionsOffsetsAcrossCNs(t *testing.T) {
	offsets := [][2]int64{
		{1, 100_000},
		{100_001, 200_000},
		{200_001, 300_000},
		{300_001, 400_000},
	}
	c := NewMockCompile(t)
	c.addr = "ingress:6001"
	c.anal = &AnalyzeModule{}
	c.cnList = engine.Nodes{
		{Addr: "cn-1:6001", Mcpu: 2},
		{Addr: "cn-2:6001", Mcpu: 3},
	}
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{}}}
	node := &plan.Node{TableDef: &plan.TableDef{
		Cols:    []*plan.ColDef{{Name: "result"}},
		TblFunc: &plan.TableFunction{Name: "generate_series"},
	}}

	scopes, err := c.compileGenerateSeriesParallel(
		node,
		nil,
		len(offsets),
		true,
		offsets,
		1,
	)
	require.NoError(t, err)
	require.Len(t, scopes, 2)

	first := scopes[0].RootOp.(*table_function.TableFunction)
	second := scopes[1].RootOp.(*table_function.TableFunction)
	t.Cleanup(first.Release)
	t.Cleanup(second.Release)

	require.Equal(t, offsets[:2], first.OffsetTotal)
	require.Equal(t, offsets[2:], second.OffsetTotal)
}
