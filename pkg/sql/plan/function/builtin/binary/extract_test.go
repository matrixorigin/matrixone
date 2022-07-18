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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
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
	outputValues, ok := outputVector.Col.([]uint32)
	require.True(t, ok)
	require.Equal(t, []uint32{2020, 2021, 2024, 1}, outputValues)
	require.True(t, nulls.Contains(outputVector.Nsp, uint64(3)))

	vector0 = testutil.MakeScalarVarchar("month", 4)
	vector1 = testutil.MakeDateVector([]string{"2020-01-01", "2021-02-03", "2024-03-04", ""}, []uint64{3})
	inputVectors = make([]*vector.Vector, 2)
	inputVectors[0] = vector0
	inputVectors[1] = vector1
	proc = testutil.NewProc()
	outputVector, err = ExtractFromDate(inputVectors, proc)
	require.NoError(t, err)
	outputValues, ok = outputVector.Col.([]uint32)
	fmt.Println(outputValues)
	require.True(t, ok)
	require.Equal(t, []uint32{1, 2, 3, 1}, outputValues)
	require.True(t, nulls.Contains(outputVector.Nsp, uint64(3)))

	vector0 = testutil.MakeScalarVarchar("day", 4)
	vector1 = testutil.MakeDateVector([]string{"2020-01-01", "2021-02-03", "2024-03-04", ""}, []uint64{3})
	inputVectors = make([]*vector.Vector, 2)
	inputVectors[0] = vector0
	inputVectors[1] = vector1
	proc = testutil.NewProc()
	outputVector, err = ExtractFromDate(inputVectors, proc)
	require.NoError(t, err)
	outputValues, ok = outputVector.Col.([]uint32)
	fmt.Println(outputValues)
	require.True(t, ok)
	require.Equal(t, []uint32{1, 3, 4, 1}, outputValues)
	require.True(t, nulls.Contains(outputVector.Nsp, uint64(3)))

	vector0 = testutil.MakeScalarVarchar("year_month", 4)
	vector1 = testutil.MakeDateVector([]string{"2020-01-01", "2021-02-03", "2024-03-04", ""}, []uint64{3})
	inputVectors = make([]*vector.Vector, 2)
	inputVectors[0] = vector0
	inputVectors[1] = vector1
	proc = testutil.NewProc()
	outputVector, err = ExtractFromDate(inputVectors, proc)
	require.NoError(t, err)
	outputValues, ok = outputVector.Col.([]uint32)
	fmt.Println(outputValues)
	require.True(t, ok)
	require.Equal(t, []uint32{202001, 202102, 202403, 101}, outputValues)
	require.True(t, nulls.Contains(outputVector.Nsp, uint64(3)))
}

func TestExtractFromDatetime(t *testing.T) {
	vector0 := testutil.MakeScalarVarchar("year", 4)
	vector1 := testutil.MakeDateTimeVector([]string{"2020-01-01 11:12:13.0006", "2006-01-02 15:03:04.1234", "2024-03-04 12:13:14", ""}, []uint64{3})
	inputVectors := make([]*vector.Vector, 2)
	inputVectors[0] = vector0
	inputVectors[1] = vector1
	proc := testutil.NewProc()
	outputVector, err := ExtractFromDatetime(inputVectors, proc)
	require.NoError(t, err)
	outputValues, ok := outputVector.Col.(*types.Bytes)
	require.True(t, ok)
	require.Equal(t, "2020200620240001", string(outputValues.Data))
	require.Equal(t, []uint32{0, 4, 8, 12}, outputValues.Offsets)
	require.Equal(t, []uint32{4, 4, 4, 4}, outputValues.Lengths)
	require.True(t, nulls.Contains(outputVector.Nsp, uint64(3)))

}
