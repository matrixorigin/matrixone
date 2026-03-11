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

package unary

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestDateToWeekFunc(t *testing.T) {
	procs := testutil.NewProc()
	cases := []struct {
		name     string
		proc     *process.Process
		inputstr []string
		inputNsp []uint64
		expected []uint8
		isScalar bool
	}{
		{
			name:     "Date to week test - first and last week",
			proc:     procs,
			inputstr: []string{"2003-12-30", "2004-01-02", "2004-12-31", "2005-01-01"},
			expected: []uint8{1, 1, 53, 53},
			isScalar: false,
		},
		{
			name:     "Date to week test - normal",
			proc:     procs,
			inputstr: []string{"2001-02-16", "2012-06-18", "2015-09-25", "2022-12-05"},
			expected: []uint8{7, 25, 39, 49},
			isScalar: false,
		},
		{
			name:     "Date to week test - scalar",
			proc:     procs,
			inputstr: []string{"2003-12-30"},
			expected: []uint8{1},
			isScalar: true,
		},
		{
			name:     "Date to week test - null",
			proc:     procs,
			isScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			vecs := make([]*vector.Vector, 1)
			if c.inputstr != nil {
				vecs[0] = testutil.MakeDateVector(c.inputstr, c.inputNsp)
				if c.isScalar {
					vecs[0].SetClass(vector.CONSTANT)
					vecs[0].SetLength(1)
				}
			} else {
				vecs[0] = testutil.MakeScalarNull(types.T_date, 1)
			}

			result, err := DateToWeek(vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			col := vector.MustFixedCol[uint8](result)
			require.Equal(t, c.expected, col)
			require.Equal(t, c.isScalar, result.IsConst())
		})
	}
}

func TestDatetimeToWeekFunc(t *testing.T) {
	procs := testutil.NewProc()
	cases := []struct {
		name     string
		proc     *process.Process
		inputstr []string
		inputNsp []uint64
		expected []uint8
		isScalar bool
	}{
		{
			name:     "Datetime to week test - first and last week",
			proc:     procs,
			inputstr: []string{"2003-12-30 13:11:10", "2004-01-02 19:22:10", "2004-12-31 00:00:00", "2005-01-01 04:05:06"},
			expected: []uint8{1, 1, 53, 53},
			isScalar: false,
		},
		{
			name:     "Datetime to week test - normal",
			proc:     procs,
			inputstr: []string{"2001-02-16 13:11:10", "2012-06-18 19:22:10", "2015-09-25 00:00:00", "2022-12-05 04:05:06"},
			expected: []uint8{7, 25, 39, 49},
			isScalar: false,
		},
		{
			name:     "Datetime to week test - scalar",
			proc:     procs,
			inputstr: []string{"2003-12-30 00:00:00"},
			expected: []uint8{1},
			isScalar: true,
		},
		{
			name:     "Datetime to week test - null",
			proc:     procs,
			isScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			vecs := make([]*vector.Vector, 1)
			if c.inputstr != nil {
				vecs[0] = testutil.MakeDateTimeVector(c.inputstr, c.inputNsp)
				if c.isScalar {
					vecs[0].SetClass(vector.CONSTANT)
					vecs[0].SetLength(1)
				}
			} else {
				vecs[0] = testutil.MakeScalarNull(types.T_datetime, 1)
			}

			result, err := DatetimeToWeek(vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			col := vector.MustFixedCol[uint8](result)
			require.Equal(t, c.expected, col)
			require.Equal(t, c.isScalar, result.IsConst())
		})
	}
}
