// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package operator

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestIsTrueFalse(t *testing.T) {
	procs := testutil.NewProc()

	testVecs := [][]*vector.Vector{
		{testutil.MakeScalarNull(types.T_any, 1)},
		{testutil.MakeScalarBool(false, 1)},
		{testutil.MakeBooleanlVector([]bool{false, false, true, true}, []uint64{1, 3})},
	}

	isTrueExpect := []*vector.Vector{
		testutil.MakeScalarBool(false, 1),
		testutil.MakeScalarBool(false, 1),
		testutil.MakeBooleanlVector([]bool{false, false, true, false}, []uint64{}),
	}

	isNotTrueExpect := []*vector.Vector{
		testutil.MakeScalarBool(true, 1),
		testutil.MakeScalarBool(true, 1),
		testutil.MakeBooleanlVector([]bool{true, true, false, true}, []uint64{}),
	}

	isFalseExpect := []*vector.Vector{
		testutil.MakeScalarBool(false, 1),
		testutil.MakeScalarBool(true, 1),
		testutil.MakeBooleanlVector([]bool{true, false, false, false}, []uint64{}),
	}

	isNotFalseExpect := []*vector.Vector{
		testutil.MakeScalarBool(true, 1),
		testutil.MakeScalarBool(false, 1),
		testutil.MakeBooleanlVector([]bool{false, true, true, true}, []uint64{}),
	}

	for i := 0; i < len(testVecs); i++ {
		result, err := IsTrue(testVecs[i], procs)
		if err != nil {
			t.Fatal(err)
		}
		require.True(t, testutil.CompareVectors(isTrueExpect[i], result), fmt.Sprintf("got IsTrue vector[%d] is different with expected", i))

		result, err = IsNotTrue(testVecs[i], procs)
		if err != nil {
			t.Fatal(err)
		}
		require.True(t, testutil.CompareVectors(isNotTrueExpect[i], result), fmt.Sprintf("got IsNotTrue vector[%d] is different with expected", i))

		result, err = IsFalse(testVecs[i], procs)
		if err != nil {
			t.Fatal(err)
		}
		require.True(t, testutil.CompareVectors(isFalseExpect[i], result), fmt.Sprintf("got IsFalse vector[%d] is different with expected", i))

		result, err = IsNotFalse(testVecs[i], procs)
		if err != nil {
			t.Fatal(err)
		}
		require.True(t, testutil.CompareVectors(isNotFalseExpect[i], result), fmt.Sprintf("got IsNotFalse vector[%d] is different with expected", i))
	}
}
