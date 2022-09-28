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

func TestUnkown(t *testing.T) {
	procs := testutil.NewProc()

	testVecs := [][]*vector.Vector{
		{testutil.MakeScalarNull(types.T_any, 0)},
		{testutil.MakeScalarBool(false, 0)},
		{testutil.MakeBooleanlVector([]bool{false, false, true, true}, []uint64{1, 3})},
	}

	isUnknownExpect := []*vector.Vector{
		testutil.MakeScalarBool(true, 1),
		testutil.MakeScalarBool(false, 1),
		testutil.MakeBooleanlVector([]bool{false, true, false, true}, []uint64{}),
	}

	isNotUnknownExpect := []*vector.Vector{
		testutil.MakeScalarBool(false, 1),
		testutil.MakeScalarBool(true, 1),
		testutil.MakeBooleanlVector([]bool{true, false, true, false}, []uint64{}),
	}

	for i := 0; i < len(testVecs); i++ {
		result, err := IsUnknown(testVecs[i], procs)
		if err != nil {
			t.Fatal(err)
		}
		require.True(t, testutil.CompareVectors(isUnknownExpect[i], result), fmt.Sprintf("got IsUnknown vector[%d] is different with expected", i))

		result, err = IsNotUnknown(testVecs[i], procs)
		if err != nil {
			t.Fatal(err)
		}
		require.True(t, testutil.CompareVectors(isNotUnknownExpect[i], result), fmt.Sprintf("got IsNotUnknown vector[%d] is different with expected", i))
	}
}
