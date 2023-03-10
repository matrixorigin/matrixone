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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestToDate(t *testing.T) {
	inputVec0 := testutil.MakeVarcharVector([]string{"20200103", "20190102", ""}, []uint64{2})
	inputVec1 := testutil.MakeScalarVarchar("YYYYMMDD", 3)
	inputVecs := []*vector.Vector{inputVec0, inputVec1}
	proc := testutil.NewProc()
	outputVec, err := ToDate(inputVecs, proc)
	require.NoError(t, err)
	require.Equal(t, []types.Date{types.DateFromCalendar(2020, 1, 3), types.DateFromCalendar(2019, 1, 2)}, vector.MustFixedCol[types.Date](outputVec)[:2])
	require.True(t, nulls.Contains(outputVec.GetNulls(), 2))

	inputVec0 = testutil.MakeVarcharVector([]string{"01032020", "01022019", ""}, []uint64{2})
	inputVec1 = testutil.MakeScalarVarchar("MMDDYYYY", 3)
	inputVecs = []*vector.Vector{inputVec0, inputVec1}
	proc = testutil.NewProc()
	outputVec, err = ToDate(inputVecs, proc)
	require.NoError(t, err)
	require.Equal(t, []types.Date{types.DateFromCalendar(2020, 1, 3), types.DateFromCalendar(2019, 1, 2)}, vector.MustFixedCol[types.Date](outputVec)[:2])
	require.True(t, nulls.Contains(outputVec.GetNulls(), 2))

	inputVec0 = testutil.MakeVarcharVector([]string{"03012020", "02012019", ""}, []uint64{2})
	inputVec1 = testutil.MakeScalarVarchar("DDMMYYYY", 3)
	inputVecs = []*vector.Vector{inputVec0, inputVec1}
	proc = testutil.NewProc()
	outputVec, err = ToDate(inputVecs, proc)
	require.NoError(t, err)
	require.Equal(t, []types.Date{types.DateFromCalendar(2020, 1, 3), types.DateFromCalendar(2019, 1, 2)}, vector.MustFixedCol[types.Date](outputVec)[:2])
	require.True(t, nulls.Contains(outputVec.GetNulls(), 2))

	inputVec0 = testutil.MakeVarcharVector([]string{"01-03-2020", "01-02-2019", ""}, []uint64{2})
	inputVec1 = testutil.MakeScalarVarchar("MM-DD-YYYY", 3)
	inputVecs = []*vector.Vector{inputVec0, inputVec1}
	proc = testutil.NewProc()
	outputVec, err = ToDate(inputVecs, proc)
	require.NoError(t, err)
	require.Equal(t, []types.Date{types.DateFromCalendar(2020, 1, 3), types.DateFromCalendar(2019, 1, 2)}, vector.MustFixedCol[types.Date](outputVec)[:2])
	require.True(t, nulls.Contains(outputVec.GetNulls(), 2))

	inputVec0 = testutil.MakeVarcharVector([]string{"03-01-2020", "02-01-2019", ""}, []uint64{2})
	inputVec1 = testutil.MakeScalarVarchar("DD-MM-YYYY", 3)
	inputVecs = []*vector.Vector{inputVec0, inputVec1}
	proc = testutil.NewProc()
	outputVec, err = ToDate(inputVecs, proc)
	require.NoError(t, err)
	require.Equal(t, []types.Date{types.DateFromCalendar(2020, 1, 3), types.DateFromCalendar(2019, 1, 2)}, vector.MustFixedCol[types.Date](outputVec)[:2])
	require.True(t, nulls.Contains(outputVec.GetNulls(), 2))

	inputVec0 = testutil.MakeVarcharVector([]string{"32-01-2020", "02-01-2019", ""}, []uint64{2})
	inputVec1 = testutil.MakeScalarVarchar("DD-MM-YYYY", 3)
	inputVecs = []*vector.Vector{inputVec0, inputVec1}
	proc = testutil.NewProc()
	_, err = ToDate(inputVecs, proc)
	require.Error(t, err)

	inputVec0 = testutil.MakeVarcharVector([]string{"03-01-2020", "02-01-2019", ""}, []uint64{2})
	inputVec1 = testutil.MakeScalarVarchar("DD-DD-YYYY", 3)
	inputVecs = []*vector.Vector{inputVec0, inputVec1}
	proc = testutil.NewProc()
	_, err = ToDate(inputVecs, proc)
	require.Error(t, err)

	inputVec0 = testutil.MakeVarcharVector([]string{"03-01-2020", "02-01-2019", ""}, []uint64{2})
	inputVec1 = testutil.MakeScalarVarchar("YYYY", 3)
	inputVecs = []*vector.Vector{inputVec0, inputVec1}
	proc = testutil.NewProc()
	_, err = ToDate(inputVecs, proc)
	require.Error(t, err)

	inputVec0 = testutil.MakeVarcharVector([]string{"03-01-2020", "02-01-2019", ""}, []uint64{2})
	inputVec1 = testutil.MakeScalarVarchar("YYYY", 3)
	inputVecs = []*vector.Vector{inputVec0, inputVec1}
	proc = testutil.NewProc()
	_, err = ToDate(inputVecs, proc)
	require.Error(t, err)
}
