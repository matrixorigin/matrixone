// Copyright 2021 Matrix Origin
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

package aggut

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// TODO: add decimal128 distinct test
func TestVariance(t *testing.T) {
	int8Typ := types.New(types.T_int8, 0, 0)
	decimal64Typ := types.New(types.T_decimal64, 18, 0)
	decimal128Typ := types.New(types.T_decimal128, 38, 0)
	decimal128Typ_3 := types.New(types.T_decimal128, 38, 3)

	testCases := []testCase{
		// int8 variance test
		{
			op:         agg.AggregateVariance,
			isDistinct: false,
			inputTyp:   int8Typ,

			input:    []int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			inputNsp: nil,
			expected: []float64{8.25},

			mergeInput:  []int8{10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
			mergeNsp:    nil,
			mergeExpect: []float64{33.25},

			testMarshal: true,
		},
		// int8 variance test
		{
			op:         agg.AggregateVariance,
			isDistinct: true,
			inputTyp:   int8Typ,

			input:    []int8{1, 1, 2, 2, 3, 3, 4, 4, 5, 5},
			inputNsp: nil,
			expected: []float64{2},

			mergeInput:  []int8{6, 6, 7, 7, 8, 8, 9, 9, 10, 10},
			mergeNsp:    nil,
			mergeExpect: []float64{8.25},

			testMarshal: false,
		},
		// decimal64 variance test
		{
			op:         agg.AggregateVariance,
			isDistinct: false,
			inputTyp:   decimal64Typ,

			input:    []int64{9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
			inputNsp: nil,
			expected: []float64{8.25},

			mergeInput:  []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			mergeNsp:    nil,
			mergeExpect: []float64{8.25},

			testMarshal: true,
		},
		{
			op:         agg.AggregateVariance,
			isDistinct: true,
			inputTyp:   decimal64Typ,

			input:    []int64{9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
			inputNsp: nil,
			expected: []float64{8.25},

			mergeInput:  []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			mergeNsp:    nil,
			mergeExpect: []float64{8.25},

			testMarshal: false,
		},
		// decimal128 variance test
		{
			op:         agg.AggregateVariance,
			isDistinct: false,
			inputTyp:   decimal128Typ,

			input:    []int64{9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
			inputNsp: nil,
			expected: []float64{8.25},

			mergeInput:  []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			mergeNsp:    nil,
			mergeExpect: []float64{8.25},

			testMarshal: true,
		},
		{
			op:         agg.AggregateVariance,
			isDistinct: true,
			inputTyp:   decimal128Typ,

			input:    []int64{9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
			inputNsp: nil,
			expected: []float64{8.25},

			mergeInput:  []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			mergeNsp:    nil,
			mergeExpect: []float64{8.25},

			testMarshal: false,
		},
		{
			op:         agg.AggregateVariance,
			isDistinct: true,
			inputTyp:   decimal128Typ_3,

			input:    []int64{14314, 15314, 14394, 124314},
			inputNsp: nil,
			expected: []float64{2254.078700000000},

			testMarshal: false,
		},
	}
	RunTest(t, testCases)
}
