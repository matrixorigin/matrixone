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

package frontend

import (
	"context"
	"testing"

	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestExtractRowFromVector(t *testing.T) {
	mp := mpool.MustNewZero()
	values := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	rowCount := len(values)
	vec := testutil.NewVector(rowCount, types.New(types.T_bit, 10, 0), mp, false, values)

	for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
		columnIdx := 0
		row := make([]interface{}, 1)
		err := extractRowFromVector(context.TODO(), nil, vec, columnIdx, row, rowIdx, false)
		require.NoError(t, err)
		require.Equal(t, row[columnIdx].(uint64), values[rowIdx])
	}
}

func BenchmarkName(b *testing.B) {

	mp := mpool.MustNewZero()
	values := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	rowCount := len(values)
	vec := testutil.NewVector(rowCount, types.New(types.T_int32, 10, 0), mp, false, values)

	colSlices := &ColumnSlices{
		colIdx2SliceIdx: make([]int, 1),
	}
	err := convertVectorToSlice(context.TODO(), nil, vec, 0, colSlices)
	assert.NoError(b, err)
	row := make([]any, 1)

	for i := 0; i < b.N; i++ {
		for j := 0; j < rowCount; j++ {
			_ = extractRowFromVector2(context.TODO(), nil, vec, 0, row, j, false, colSlices)
		}
	}
}

func BenchmarkName2(b *testing.B) {

	mp := mpool.MustNewZero()
	values := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	rowCount := len(values)
	vec := testutil.NewVector(rowCount, types.New(types.T_varchar, 10, 0), mp, false, values)

	colSlices := &ColumnSlices{
		colIdx2SliceIdx: make([]int, 1),
	}
	err := convertVectorToSlice(context.TODO(), nil, vec, 0, colSlices)
	assert.NoError(b, err)
	row := make([]any, 1)

	for i := 0; i < b.N; i++ {
		for j := 0; j < rowCount; j++ {
			_ = extractRowFromVector2(context.TODO(), nil, vec, 0, row, j, false, colSlices)
		}
	}
}

func Test_extractRowFromVector2(t *testing.T) {
	ctx := context.TODO()
	convey.Convey("append result set", t, func() {
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		pu.SV.KillRountinesInterval = 0
		setSessionAlloc("", NewLeakCheckAllocator())
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.ShouldBeNil(err)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)

		ses := NewSession(ctx, "", proto, nil)
		proto.ses = ses

		kases := makeKases()
		for _, kse := range kases {
			bat := kse.bat
			row := make([]any, len(bat.Vecs))

			fun := func() {
				colSlices := &ColumnSlices{
					ctx:             context.TODO(),
					colIdx2SliceIdx: make([]int, len(bat.Vecs)),
					dataSet:         bat,
				}
				defer colSlices.Close()
				err = convertBatchToSlices(context.TODO(), ses, bat, colSlices)
				convey.So(err, convey.ShouldBeNil)

				err = extractRowFromEveryVector2(context.TODO(), ses, bat, 0, row, false, colSlices)
				convey.So(err, convey.ShouldBeNil)
			}
			fun()
		}
	})
}

// TestColumnSlicesGetDate tests GetDate function with comprehensive coverage
func TestColumnSlicesGetDate(t *testing.T) {
	ctx := context.TODO()
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	testCases := []struct {
		name          string
		setupFunc     func() (*ColumnSlices, *batch.Batch, uint64)
		expectedDate  types.Date
		expectError   bool
		errorContains string
	}{
		{
			name: "Normal T_date type",
			setupFunc: func() (*ColumnSlices, *batch.Batch, uint64) {
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = vector.NewVec(types.New(types.T_date, 0, 0))
				date := types.Date(types.DateFromCalendar(2024, 1, 15))
				vector.AppendFixed(bat.Vecs[0], date, false, mp)
				bat.SetRowCount(1)

				colSlices := &ColumnSlices{
					ctx:             ctx,
					colIdx2SliceIdx: make([]int, 1),
					dataSet:         bat,
				}
				// Manually set up slices for T_date
				colSlices.colIdx2SliceIdx[0] = 0
				colSlices.arrDate = append(colSlices.arrDate, vector.ToSliceNoTypeCheck2[types.Date](bat.Vecs[0]))

				return colSlices, bat, 0
			},
			expectedDate: types.Date(types.DateFromCalendar(2024, 1, 15)),
			expectError:  false,
		},
		{
			name: "Normal T_datetime type",
			setupFunc: func() (*ColumnSlices, *batch.Batch, uint64) {
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = vector.NewVec(types.New(types.T_datetime, 0, 0))
				dt, _ := types.ParseDatetime("2024-01-15 10:20:30", 0)
				vector.AppendFixed(bat.Vecs[0], dt, false, mp)
				bat.SetRowCount(1)

				colSlices := &ColumnSlices{
					ctx:             ctx,
					colIdx2SliceIdx: make([]int, 1),
					dataSet:         bat,
				}
				// Manually set up slices for T_datetime
				colSlices.colIdx2SliceIdx[0] = 0
				colSlices.arrDatetime = append(colSlices.arrDatetime, vector.ToSliceNoTypeCheck2[types.Datetime](bat.Vecs[0]))

				return colSlices, bat, 0
			},
			expectedDate: types.Date(types.DateFromCalendar(2024, 1, 15)),
			expectError:  false,
		},
		{
			name: "Const T_date type",
			setupFunc: func() (*ColumnSlices, *batch.Batch, uint64) {
				bat := batch.NewWithSize(1)
				vec, err := vector.NewConstFixed(types.New(types.T_date, 0, 0), types.Date(types.DateFromCalendar(2024, 1, 15)), 1, mp)
				require.NoError(t, err)
				bat.Vecs[0] = vec
				bat.SetRowCount(1)

				colSlices := &ColumnSlices{
					ctx:             ctx,
					colIdx2SliceIdx: make([]int, 1),
					dataSet:         bat,
				}
				// Manually set up slices for T_date
				colSlices.colIdx2SliceIdx[0] = 0
				colSlices.arrDate = append(colSlices.arrDate, vector.ToSliceNoTypeCheck2[types.Date](bat.Vecs[0]))

				return colSlices, bat, 5 // Use row index 5, but should use 0 for const
			},
			expectedDate: types.Date(types.DateFromCalendar(2024, 1, 15)),
			expectError:  false,
		},
		{
			name: "Invalid type (default case)",
			setupFunc: func() (*ColumnSlices, *batch.Batch, uint64) {
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = vector.NewVec(types.New(types.T_int64, 0, 0))
				vector.AppendFixed(bat.Vecs[0], int64(12345), false, mp)
				bat.SetRowCount(1)

				colSlices := &ColumnSlices{
					ctx:             ctx,
					colIdx2SliceIdx: make([]int, 1),
					dataSet:         bat,
				}
				// Set up slices for T_int64
				colSlices.colIdx2SliceIdx[0] = 0
				colSlices.arrInt64 = append(colSlices.arrInt64, vector.ToSliceNoTypeCheck2[int64](bat.Vecs[0]))

				return colSlices, bat, 0
			},
			expectError:   true,
			errorContains: "invalid date slice",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			colSlices, bat, rowIdx := tc.setupFunc()
			defer bat.Clean(mp)
			defer colSlices.Close()

			date, err := colSlices.GetDate(rowIdx, 0)

			if tc.expectError {
				require.Error(t, err)
				if tc.errorContains != "" {
					require.Contains(t, err.Error(), tc.errorContains)
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedDate, date)
			}
		})
	}
}

// TestColumnSlicesGetDatetime tests GetDatetime function with comprehensive coverage
func TestColumnSlicesGetDatetime(t *testing.T) {
	ctx := context.TODO()
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	testCases := []struct {
		name          string
		setupFunc     func() (*ColumnSlices, *batch.Batch, uint64)
		expectedStr   string
		expectError   bool
		errorContains string
	}{
		{
			name: "Normal T_datetime type with scale 0",
			setupFunc: func() (*ColumnSlices, *batch.Batch, uint64) {
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = vector.NewVec(types.New(types.T_datetime, 0, 0))
				dt, _ := types.ParseDatetime("2024-01-15 10:20:30", 0)
				vector.AppendFixed(bat.Vecs[0], dt, false, mp)
				bat.SetRowCount(1)

				colSlices := &ColumnSlices{
					ctx:             ctx,
					colIdx2SliceIdx: make([]int, 1),
					dataSet:         bat,
				}
				colSlices.colIdx2SliceIdx[0] = 0
				colSlices.arrDatetime = append(colSlices.arrDatetime, vector.ToSliceNoTypeCheck2[types.Datetime](bat.Vecs[0]))

				return colSlices, bat, 0
			},
			expectedStr: "2024-01-15 10:20:30",
			expectError: false,
		},
		{
			name: "Normal T_datetime type with scale > 0 and MicroSec == 0",
			setupFunc: func() (*ColumnSlices, *batch.Batch, uint64) {
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = vector.NewVec(types.New(types.T_datetime, 0, 6))
				dt, _ := types.ParseDatetime("2024-01-15 10:20:30", 6)
				vector.AppendFixed(bat.Vecs[0], dt, false, mp)
				bat.SetRowCount(1)

				colSlices := &ColumnSlices{
					ctx:             ctx,
					colIdx2SliceIdx: make([]int, 1),
					dataSet:         bat,
				}
				colSlices.colIdx2SliceIdx[0] = 0
				colSlices.arrDatetime = append(colSlices.arrDatetime, vector.ToSliceNoTypeCheck2[types.Datetime](bat.Vecs[0]))

				return colSlices, bat, 0
			},
			expectedStr: "2024-01-15 10:20:30", // Should format without fractional part when MicroSec == 0
			expectError: false,
		},
		{
			name: "Normal T_datetime type with scale > 0 and MicroSec != 0",
			setupFunc: func() (*ColumnSlices, *batch.Batch, uint64) {
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = vector.NewVec(types.New(types.T_datetime, 0, 6))
				dt, _ := types.ParseDatetime("2024-01-15 10:20:30.123456", 6)
				vector.AppendFixed(bat.Vecs[0], dt, false, mp)
				bat.SetRowCount(1)

				colSlices := &ColumnSlices{
					ctx:             ctx,
					colIdx2SliceIdx: make([]int, 1),
					dataSet:         bat,
				}
				colSlices.colIdx2SliceIdx[0] = 0
				colSlices.arrDatetime = append(colSlices.arrDatetime, vector.ToSliceNoTypeCheck2[types.Datetime](bat.Vecs[0]))

				return colSlices, bat, 0
			},
			expectedStr: "2024-01-15 10:20:30.123456",
			expectError: false,
		},
		{
			name: "Const T_datetime type",
			setupFunc: func() (*ColumnSlices, *batch.Batch, uint64) {
				bat := batch.NewWithSize(1)
				dt, _ := types.ParseDatetime("2024-01-15 10:20:30", 0)
				vec, err := vector.NewConstFixed(types.New(types.T_datetime, 0, 0), dt, 1, mp)
				require.NoError(t, err)
				bat.Vecs[0] = vec
				bat.SetRowCount(1)

				colSlices := &ColumnSlices{
					ctx:             ctx,
					colIdx2SliceIdx: make([]int, 1),
					dataSet:         bat,
				}
				colSlices.colIdx2SliceIdx[0] = 0
				colSlices.arrDatetime = append(colSlices.arrDatetime, vector.ToSliceNoTypeCheck2[types.Datetime](bat.Vecs[0]))

				return colSlices, bat, 5 // Use row index 5, but should use 0 for const
			},
			expectedStr: "2024-01-15 10:20:30",
			expectError: false,
		},
		{
			name: "Invalid type (default case)",
			setupFunc: func() (*ColumnSlices, *batch.Batch, uint64) {
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = vector.NewVec(types.New(types.T_int64, 0, 0))
				vector.AppendFixed(bat.Vecs[0], int64(12345), false, mp)
				bat.SetRowCount(1)

				colSlices := &ColumnSlices{
					ctx:             ctx,
					colIdx2SliceIdx: make([]int, 1),
					dataSet:         bat,
				}
				colSlices.colIdx2SliceIdx[0] = 0
				colSlices.arrInt64 = append(colSlices.arrInt64, vector.ToSliceNoTypeCheck2[int64](bat.Vecs[0]))

				return colSlices, bat, 0
			},
			expectError:   true,
			errorContains: "invalid datetime slice",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			colSlices, bat, rowIdx := tc.setupFunc()
			defer bat.Clean(mp)
			defer colSlices.Close()

			dtStr, err := colSlices.GetDatetime(rowIdx, 0)

			if tc.expectError {
				require.Error(t, err)
				if tc.errorContains != "" {
					require.Contains(t, err.Error(), tc.errorContains)
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedStr, dtStr)
			}
		})
	}
}
