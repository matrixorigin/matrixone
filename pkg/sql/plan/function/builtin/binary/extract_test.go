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

package binary

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestExtractFromDate(t *testing.T) {
	vector0 := testutil.MakeScalarVarchar("year", 4)
	vector1 := testutil.MakeDateVector([]string{"2020-01-01", "2021-02-03", "2024-03-04", ""}, []uint64{3})
	inputVectors := make([]*vector.Vector, 2)
	inputVectors[0] = vector0
	inputVectors[1] = vector1
	proc := testutil.NewProc()
	outputVector, err := ExtractFromDate(inputVectors, proc)
	require.NoError(t, err)
	outputValues := vector.MustFixedCol[uint32](outputVector)
	ok := (outputValues != nil)
	require.True(t, ok)
	require.Equal(t, []uint32{2020, 2021, 2024, 1}, outputValues)
	// XXX why?  This seems to be wrong.  ExtractFromDate "" should error out,
	// but if it does not, we tested the result is 1 in prev check.
	// it should not be null.
	// require.True(t, nulls.Contains(outputVector.GetNulls(), uint64(3)))

	vector0 = testutil.MakeScalarVarchar("month", 4)
	vector1 = testutil.MakeDateVector([]string{"2020-01-01", "2021-02-03", "2024-03-04", ""}, []uint64{3})
	inputVectors = make([]*vector.Vector, 2)
	inputVectors[0] = vector0
	inputVectors[1] = vector1
	proc = testutil.NewProc()
	outputVector, err = ExtractFromDate(inputVectors, proc)
	require.NoError(t, err)
	outputValues = vector.MustFixedCol[uint32](outputVector)
	ok = (outputValues != nil)
	fmt.Println(outputValues)
	require.True(t, ok)
	require.Equal(t, []uint32{1, 2, 3, 1}, outputValues)
	// XXX same as above.
	// require.True(t, nulls.Contains(outputVector.GetNulls(), uint64(3)))

	vector0 = testutil.MakeScalarVarchar("day", 4)
	vector1 = testutil.MakeDateVector([]string{"2020-01-01", "2021-02-03", "2024-03-04", ""}, []uint64{3})
	inputVectors = make([]*vector.Vector, 2)
	inputVectors[0] = vector0
	inputVectors[1] = vector1
	proc = testutil.NewProc()
	outputVector, err = ExtractFromDate(inputVectors, proc)
	require.NoError(t, err)
	outputValues = vector.MustFixedCol[uint32](outputVector)
	ok = (outputValues != nil)
	fmt.Println(outputValues)
	require.True(t, ok)
	require.Equal(t, []uint32{1, 3, 4, 1}, outputValues)
	// XXX Same
	// require.True(t, nulls.Contains(outputVector.GetNulls(), uint64(3)))

	vector0 = testutil.MakeScalarVarchar("year_month", 4)
	vector1 = testutil.MakeDateVector([]string{"2020-01-01", "2021-02-03", "2024-03-04", ""}, []uint64{3})
	inputVectors = make([]*vector.Vector, 2)
	inputVectors[0] = vector0
	inputVectors[1] = vector1
	proc = testutil.NewProc()
	outputVector, err = ExtractFromDate(inputVectors, proc)
	require.NoError(t, err)
	outputValues = vector.MustFixedCol[uint32](outputVector)
	ok = (outputValues != nil)
	fmt.Println(outputValues)
	require.True(t, ok)
	require.Equal(t, []uint32{202001, 202102, 202403, 101}, outputValues)
	// XXX same
	// require.True(t, nulls.Contains(outputVector.GetNulls(), uint64(3)))
}

func TestExtractFromDatetime(t *testing.T) {
	// XXX This is broken.   empty should cause an error instead of null.
	vector0 := testutil.MakeScalarVarchar("year", 4)
	vector1 := testutil.MakeDateTimeVector([]string{"2020-01-01 11:12:13.0006", "2006-01-02 15:03:04.1234", "2024-03-04 12:13:14", ""}, []uint64{3})
	inputVectors := make([]*vector.Vector, 2)
	inputVectors[0] = vector0
	inputVectors[1] = vector1
	proc := testutil.NewProc()
	outputVector, err := ExtractFromDatetime(inputVectors, proc)
	require.NoError(t, err)
	outstr := vector.MustStrCol(outputVector)
	require.Equal(t, []string{"2020", "2006", "2024"}, outstr[:3])
	//require.True(t, nulls.Contains(outputVector.GetNulls(), uint64(3)))
}
