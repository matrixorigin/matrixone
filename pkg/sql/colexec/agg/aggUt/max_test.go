// Copyright 2022 Matrix Origin
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

// TODO: add distinc decimal128 test
func TestMax(t *testing.T) {
	int8TestTyp := types.New(types.T_int8, 0, 0, 0)
	decimalTestTyp := types.New(types.T_decimal128, 0, 0, 0)
	boolTestTyp := types.New(types.T_bool, 0, 0, 0)
	varcharTestTyp := types.New(types.T_varchar, 0, 0, 0)
	uuidTestTyp := types.New(types.T_uuid, 0, 0, 0)

	testCases := []testCase{
		{
			op:         agg.AggregateMax,
			isDistinct: false,
			inputTyp:   int8TestTyp,

			input:    []int8{9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
			inputNsp: nil,
			expected: []int8{9},

			mergeInput:  []int8{0, 1, 2, 33, 4, 5, 6, 7, 8, 9},
			mergeNsp:    nil,
			mergeExpect: []int8{33},

			testMarshal: true,
		},
		{
			op:         agg.AggregateMax,
			isDistinct: true,
			inputTyp:   int8TestTyp,

			input:    []int8{9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
			inputNsp: nil,
			expected: []int8{9},

			mergeInput:  []int8{0, 1, 2, 33, 33, 5, 6, 7, 8, 9},
			mergeNsp:    nil,
			mergeExpect: []int8{33},

			testMarshal: false,
		},
		{
			op:         agg.AggregateMax,
			isDistinct: false,
			inputTyp:   decimalTestTyp,

			input:    []int64{9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
			inputNsp: nil,
			expected: []int64{9},

			mergeInput:  []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			mergeNsp:    nil,
			mergeExpect: []int64{9},

			testMarshal: true,
		},
		{
			op:         agg.AggregateMax,
			isDistinct: false,
			inputTyp:   boolTestTyp,

			input:    []bool{true, true, false, true, false, true, false, true, false, true},
			inputNsp: nil,
			expected: []bool{true},

			mergeInput:  []bool{false, false, false, false, false, false, false, false, false, false},
			mergeNsp:    nil,
			mergeExpect: []bool{true},

			testMarshal: true,
		},
		{
			op:         agg.AggregateMax,
			isDistinct: false,
			inputTyp:   varcharTestTyp,

			input:    []string{"ab", "ac", "bc", "bcdd", "c", "za", "mo", "momo", "zb", "z"},
			inputNsp: nil,
			expected: []string{"zb"},

			mergeInput:  []string{"ss", "ac", "bc", "bcdd", "c", "za", "mo", "momo", "zb", "zzz"},
			mergeNsp:    nil,
			mergeExpect: []string{"zzz"},

			testMarshal: true,
		},
		{
			op:         agg.AggregateMax,
			isDistinct: false,
			inputTyp:   uuidTestTyp,

			input: []string{
				"f6355110-2d0c-11ed-940f-000c29847904",
				"1ef96142-2d0d-11ed-940f-000c29847904",
				"117a0bd5-2d0d-11ed-940f-000c29847904",
				"18b21c70-2d0d-11ed-940f-000c29847904",
				"1b50c129-2dba-11ed-940f-000c29847904",
				"ad9f83eb-2dbd-11ed-940f-000c29847904",
				"6d1b1fdb-2dbf-11ed-940f-000c29847904",
				"6d1b1fdb-2dbf-11ed-940f-000c29847904",
				"1b50c129-2dba-11ed-940f-000c29847904",
				"ad9f83eb-2dbd-11ed-940f-000c29847904",
			},
			inputNsp: nil,
			expected: []string{"f6355110-2d0c-11ed-940f-000c29847904"},

			mergeInput: []string{
				"550e8400-e29b-41d4-a716-446655440000",
				"3e350a5c-222a-11eb-abef-0242ac110002",
				"9e7862b3-2f69-11ed-8ec0-000c29847904",
				"6d1b1f73-2dbf-11ed-940f-000c29847904",
				"ad9f809f-2dbd-11ed-940f-000c29847904",
				"1b50c137-2dba-11ed-940f-000c29847904",
				"149e3f0f-2de4-11ed-940f-000c29847904",
				"1b50c137-2dba-11ed-940f-000c29847904",
				"9e7862b3-2f69-11ed-8ec0-000c29847904",
				"3F2504E0-4F89-11D3-9A0C-0305E82C3301",
			},
			mergeNsp:    nil,
			mergeExpect: []string{"f6355110-2d0c-11ed-940f-000c29847904"},

			testMarshal: true,
		},
		{
			op:         agg.AggregateMax,
			isDistinct: true,
			inputTyp:   uuidTestTyp,

			input: []string{
				"f6355110-2d0c-11ed-940f-000c29847904",
				"1ef96142-2d0d-11ed-940f-000c29847904",
				"117a0bd5-2d0d-11ed-940f-000c29847904",
				"18b21c70-2d0d-11ed-940f-000c29847904",
				"1b50c129-2dba-11ed-940f-000c29847904",
				"ad9f83eb-2dbd-11ed-940f-000c29847904",
				"6d1b1fdb-2dbf-11ed-940f-000c29847904",
				"6d1b1fdb-2dbf-11ed-940f-000c29847904",
				"1b50c129-2dba-11ed-940f-000c29847904",
				"ad9f83eb-2dbd-11ed-940f-000c29847904",
			},
			inputNsp: nil,
			expected: []string{"f6355110-2d0c-11ed-940f-000c29847904"},

			mergeInput: []string{
				"550e8400-e29b-41d4-a716-446655440000",
				"3e350a5c-222a-11eb-abef-0242ac110002",
				"9e7862b3-2f69-11ed-8ec0-000c29847904",
				"6d1b1f73-2dbf-11ed-940f-000c29847904",
				"ad9f809f-2dbd-11ed-940f-000c29847904",
				"1b50c137-2dba-11ed-940f-000c29847904",
				"149e3f0f-2de4-11ed-940f-000c29847904",
				"1b50c137-2dba-11ed-940f-000c29847904",
				"9e7862b3-2f69-11ed-8ec0-000c29847904",
				"3F2504E0-4F89-11D3-9A0C-0305E82C3301",
			},
			mergeNsp:    nil,
			mergeExpect: []string{"f6355110-2d0c-11ed-940f-000c29847904"},

			testMarshal: false,
		},
	}

	RunTest(t, testCases)
}
