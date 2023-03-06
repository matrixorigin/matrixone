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

func TestBitOr(t *testing.T) {
	int32Typ := types.New(types.T_int32, 0, 0)
	float64Typ := types.New(types.T_float64, 0, 0)

	testCases := []testCase{
		// int8 bit-or test
		{
			op:         agg.AggregateBitOr,
			isDistinct: false,
			inputTyp:   int32Typ,

			input:    []int32{2, 0},
			inputNsp: nil,
			expected: []uint64{2},

			mergeInput:  []int32{1},
			mergeNsp:    nil,
			mergeExpect: []uint64{3},

			testMarshal: true,
		},
		// int8 bit-or test
		{
			op:         agg.AggregateBitOr,
			isDistinct: true,
			inputTyp:   int32Typ,

			input:    []int32{2, 0},
			inputNsp: nil,
			expected: []uint64{2},

			mergeInput:  []int32{1},
			mergeNsp:    nil,
			mergeExpect: []uint64{3},

			testMarshal: false,
		},
		// float64 bit-or test
		{
			op:         agg.AggregateBitOr,
			isDistinct: false,
			inputTyp:   float64Typ,

			input:    []float64{2, 0, 0},
			inputNsp: nil,
			expected: []uint64{2},

			mergeInput:  []float64{1, 1},
			mergeNsp:    nil,
			mergeExpect: []uint64{3},

			testMarshal: true,
		},
	}

	RunTest(t, testCases)
}
