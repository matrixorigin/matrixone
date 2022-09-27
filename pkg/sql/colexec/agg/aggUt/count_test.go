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

func TestCount(t *testing.T) {
	int8TestTyp := types.New(types.T_int8, 0, 0, 0)
	boolTestTyp := types.New(types.T_bool, 0, 0, 0)
	varcharTestTyp := types.New(types.T_varchar, 0, 0, 0)
	decimalTestTyp := types.New(types.T_decimal128, 0, 0, 0)
	uuidTestTyp := types.New(types.T_uuid, 0, 0, 0)

	testCases := []testCase{
		// int8 count test
		{
			op:         agg.AggregateCount,
			isDistinct: false,
			inputTyp:   int8TestTyp,

			input:    []int8{9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
			inputNsp: nil,
			expected: []int64{10},

			mergeInput:  []int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			mergeNsp:    nil,
			mergeExpect: []int64{20},

			testMarshal: true,
		},
		{
			op:         agg.AggregateCount,
			isDistinct: true,
			inputTyp:   int8TestTyp,

			input:    []int8{9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
			inputNsp: nil,
			expected: []int64{10},

			mergeInput:  []int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			mergeNsp:    nil,
			mergeExpect: []int64{10},

			testMarshal: false,
		},
		{
			op:         agg.AggregateCount,
			isDistinct: true,
			inputTyp:   int8TestTyp,

			input:    []int8{1, 1, 2, 2, 3, 3, 4, 4, 5, 5},
			inputNsp: nil,
			expected: []int64{5},

			mergeInput:  []int8{6, 6, 7, 7, 8, 8, 9, 9, 10, 10},
			mergeNsp:    nil,
			mergeExpect: []int64{10},

			testMarshal: false,
		},
		// bool count test
		{
			op:         agg.AggregateCount,
			isDistinct: false,
			inputTyp:   boolTestTyp,

			input:    []bool{true, true, false, true, false, true, false, true, false, true},
			inputNsp: nil,
			expected: []int64{10},

			mergeInput:  []bool{false, false, false, false, false, false, false, false, false, false},
			mergeNsp:    nil,
			mergeExpect: []int64{20},

			testMarshal: true,
		},
		{
			op:         agg.AggregateCount,
			isDistinct: false,
			inputTyp:   varcharTestTyp,

			input:    []string{"aa", "bb", "cc"},
			inputNsp: nil,
			expected: []int64{3},

			mergeInput:  []string{"aa", "bb", "cc"},
			mergeNsp:    nil,
			mergeExpect: []int64{6},

			testMarshal: true,
		},
		// varchar count test
		{
			op:         agg.AggregateCount,
			isDistinct: true,
			inputTyp:   varcharTestTyp,

			input:    []string{"aa", "bb", "cc"},
			inputNsp: nil,
			expected: []int64{3},

			mergeInput:  []string{"aa", "bb", "cc"},
			mergeNsp:    nil,
			mergeExpect: []int64{3},

			testMarshal: false,
		},
		// decimal128 count test
		{
			op:         agg.AggregateCount,
			isDistinct: false,
			inputTyp:   decimalTestTyp,

			input:    []int64{9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
			inputNsp: nil,
			expected: []int64{10},

			mergeInput:  []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			mergeNsp:    nil,
			mergeExpect: []int64{20},

			testMarshal: true,
		},
		{
			op:         agg.AggregateCount,
			isDistinct: true,
			inputTyp:   decimalTestTyp,

			input:    []int64{9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
			inputNsp: nil,
			expected: []int64{10},

			mergeInput:  []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			mergeNsp:    nil,
			mergeExpect: []int64{10},

			testMarshal: false,
		},
		{
			op:         agg.AggregateCount,
			isDistinct: false,
			inputTyp:   uuidTestTyp,
			input: []string{
				"f6355110-2d0c-11ed-940f-000c29847904",
				"1ef96142-2d0d-11ed-940f-000c29847904",
				"117a0bd5-2d0d-11ed-940f-000c29847904",
				"18b21c70-2d0d-11ed-940f-000c29847904",
				"1b50c129-2dba-11ed-940f-000c29847904",
			},
			inputNsp: nil,
			expected: []int64{5},
			mergeInput: []string{
				"f6355110-2d0c-11ed-940f-000c29847904",
				"1ef96142-2d0d-11ed-940f-000c29847904",
				"117a0bd5-2d0d-11ed-940f-000c29847904",
				"18b21c70-2d0d-11ed-940f-000c29847904",
				"1b50c129-2dba-11ed-940f-000c29847904",
			},
			mergeNsp:    nil,
			mergeExpect: []int64{10},
			testMarshal: true,
		},
		{
			op:         agg.AggregateCount,
			isDistinct: true,
			inputTyp:   uuidTestTyp,
			input: []string{
				"f6355110-2d0c-11ed-940f-000c29847904",
				"1ef96142-2d0d-11ed-940f-000c29847904",
				"117a0bd5-2d0d-11ed-940f-000c29847904",
				"18b21c70-2d0d-11ed-940f-000c29847904",
				"1b50c129-2dba-11ed-940f-000c29847904",
			},
			inputNsp: nil,
			expected: []int64{5},
			mergeInput: []string{
				"f6355110-2d0c-11ed-940f-000c29847904",
				"1ef96142-2d0d-11ed-940f-000c29847904",
				"117a0bd5-2d0d-11ed-940f-000c29847904",
				"18b21c70-2d0d-11ed-940f-000c29847904",
				"1b50c129-2dba-11ed-940f-000c29847904",
			},
			mergeNsp:    nil,
			mergeExpect: []int64{5},
			testMarshal: false,
		},
	}

	RunTest(t, testCases)
}
