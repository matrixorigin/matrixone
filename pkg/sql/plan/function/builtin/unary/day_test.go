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

func TestDateToDayFunc(t *testing.T) {
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
			name:     "Date to day test - normal",
			proc:     procs,
			inputstr: []string{"2021-01-01", "2022-06-05", "2022-12-31"},
			expected: []uint8{1, 5, 31},
			isScalar: false,
		},
		{
			name:     "Date to day test - normal with null",
			proc:     procs,
			inputstr: []string{"2021-01-01", "2022-06-05", "2022-10-18", "2022-12-31"},
			inputNsp: []uint64{1, 2},
			expected: []uint8{1, 1, 1, 31},
			isScalar: false,
		},
		{
			name:     "Date to day test - scalar",
			proc:     procs,
			inputstr: []string{"2021-01-01"},
			expected: []uint8{1},
			isScalar: true,
		},
		{
			name:     "Date to day test - null",
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
				vecs[0] = testutil.MakeScalarNull(types.T_date, 0)
			}

			result, err := DateToDay(vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			col := vector.MustFixedCol[uint8](result)
			require.Equal(t, c.expected, col)
			require.Equal(t, c.isScalar, result.IsConst())
		})
	}
}

func TestDatetimeToDayFunc(t *testing.T) {
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
			name:     "Datetime to day test - normal",
			proc:     procs,
			inputstr: []string{"2021-01-01 13:11:10", "2022-06-05 07:00:22", "2022-12-31 22:53:40"},
			expected: []uint8{1, 5, 31},
			isScalar: false,
		},
		{
			name:     "Datetime to day test - normal with null",
			proc:     procs,
			inputstr: []string{"2021-01-01 13:11:10", "2022-06-05 07:00:22", "2022-12-31 22:53:40"},
			inputNsp: []uint64{1},
			expected: []uint8{1, 1, 31},
			isScalar: false,
		},
		{
			name:     "Datetime to day test - scalar",
			proc:     procs,
			inputstr: []string{"2021-01-01 13:11:10"},
			expected: []uint8{1},
			isScalar: true,
		},
		{
			name:     "Datetime to day test - null",
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
				vecs[0] = testutil.MakeScalarNull(types.T_datetime, 0)
			}

			result, err := DatetimeToDay(vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			col := vector.MustFixedCol[uint8](result)
			require.Equal(t, c.expected, col)
			require.Equal(t, c.isScalar, result.IsConst())
		})
	}
}
