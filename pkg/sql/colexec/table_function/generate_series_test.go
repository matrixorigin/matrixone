// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type Kase[T int32 | int64] struct {
	start T
	end   T
	step  T
	res   []T
	err   bool
}

func TestDoGenerateInt32(t *testing.T) {
	numTest[int32](t, types.T_int32)
}
func TestDoGenerateInt64(t *testing.T) {
	numTest[int64](t, types.T_int64)
}

func numTest[T int32 | int64](t *testing.T, typ types.T) {
	kases := []Kase[T]{
		{
			start: 1,
			end:   10,
			step:  1,
			res:   []T{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			start: 1,
			end:   10,
			step:  2,
			res:   []T{1, 3, 5, 7, 9},
		},
		{
			start: 1,
			end:   10,
			step:  -1,
			res:   []T{},
		},
		{
			start: 10,
			end:   1,
			step:  -1,
			res:   []T{10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		},
		{
			start: 10,
			end:   1,
			step:  -2,
			res:   []T{10, 8, 6, 4, 2},
		},
		{
			start: 10,
			end:   1,
			step:  1,
			res:   []T{},
		},
		{
			start: 1,
			end:   10,
			step:  0,
			err:   true,
		},
		{
			start: 1,
			end:   10,
			step:  -1,
			res:   []T{},
		},
		{
			start: 1,
			end:   1,
			step:  0,
			err:   true,
		},
		{
			start: 1,
			end:   1,
			step:  1,
			res:   []T{1},
		},
		{
			start: math.MaxInt32 - 1,
			end:   math.MaxInt32,
			step:  1,
			res:   []T{math.MaxInt32 - 1, math.MaxInt32},
		},
		{
			start: math.MaxInt32,
			end:   math.MaxInt32 - 1,
			step:  -1,
			res:   []T{math.MaxInt32, math.MaxInt32 - 1},
		},
		{
			start: math.MinInt32,
			end:   math.MinInt32 + 100,
			step:  19,
			res:   []T{math.MinInt32, math.MinInt32 + 19, math.MinInt32 + 38, math.MinInt32 + 57, math.MinInt32 + 76, math.MinInt32 + 95},
		},
		{
			start: math.MinInt32 + 100,
			end:   math.MinInt32,
			step:  -19,
			res:   []T{math.MinInt32 + 100, math.MinInt32 + 81, math.MinInt32 + 62, math.MinInt32 + 43, math.MinInt32 + 24, math.MinInt32 + 5},
		},
	}

	proc := testutil.NewProc()
	for _, kase := range kases {
		var gs genNumState[int64]
		startVec, _ := vector.NewConstFixed[T](typ.ToType(), kase.start, 1, proc.Mp())
		endVec, _ := vector.NewConstFixed[T](typ.ToType(), kase.end, 1, proc.Mp())
		stepVec, _ := vector.NewConstFixed[T](typ.ToType(), kase.step, 1, proc.Mp())

		err := initStartAndEndNumNoTypeCheck(&gs, proc, startVec, endVec, stepVec, 0)
		if err != nil {
			require.True(t, kase.err)
			continue
		}

		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())

		var result []T
		for {
			bat.CleanOnlyData()
			buildNextNumBatch[T](&gs, bat, 3, proc)
			if bat.RowCount() == 0 {
				break
			}
			vec := bat.GetVector(0)
			for i := 0; i < bat.RowCount(); i++ {
				val := vector.GetFixedAtWithTypeCheck[int64](vec, i)
				result = append(result, T(val))
			}
		}
		if len(kase.res) == 0 {
			require.True(t, len(result) == 0)
		} else {
			require.Equal(t, kase.res, result)
		}
	}
}

func TestGenerateTimestamp(t *testing.T) {
	kases := []struct {
		start string
		end   string
		step  string
		res   []types.Datetime
		err   bool
	}{
		{
			start: "2019-01-01 00:00:00",
			end:   "2019-01-01 00:01:00",
			step:  "30 second",
			res: []types.Datetime{
				transStr2Datetime("2019-01-01 00:00:00"),
				transStr2Datetime("2019-01-01 00:00:30"),
				transStr2Datetime("2019-01-01 00:01:00"),
			},
		},
		{
			start: "2019-01-01 00:01:00",
			end:   "2019-01-01 00:00:00",
			step:  "-30 second",
			res: []types.Datetime{
				transStr2Datetime("2019-01-01 00:01:00"),
				transStr2Datetime("2019-01-01 00:00:30"),
				transStr2Datetime("2019-01-01 00:00:00"),
			},
		},
		{
			start: "2019-01-01 00:00:00",
			end:   "2019-01-01 00:01:00",
			step:  "30 minute",
			res: []types.Datetime{
				transStr2Datetime("2019-01-01 00:00:00"),
			},
		},
		{
			start: "2020-02-29 00:01:00",
			end:   "2021-03-01 00:00:00",
			step:  "1 year",
			res: []types.Datetime{
				transStr2Datetime("2020-02-29 00:01:00"),
				transStr2Datetime("2021-02-28 00:01:00"),
			},
		},
		{
			start: "2020-02-29 00:01:00",
			end:   "2021-03-01 00:00:00",
			step:  "1 month",
			res: []types.Datetime{
				transStr2Datetime("2020-02-29 00:01:00"),
				transStr2Datetime("2020-03-29 00:01:00"),
				transStr2Datetime("2020-04-29 00:01:00"),
				transStr2Datetime("2020-05-29 00:01:00"),
				transStr2Datetime("2020-06-29 00:01:00"),
				transStr2Datetime("2020-07-29 00:01:00"),
				transStr2Datetime("2020-08-29 00:01:00"),
				transStr2Datetime("2020-09-29 00:01:00"),
				transStr2Datetime("2020-10-29 00:01:00"),
				transStr2Datetime("2020-11-29 00:01:00"),
				transStr2Datetime("2020-12-29 00:01:00"),
				transStr2Datetime("2021-01-29 00:01:00"),
				transStr2Datetime("2021-02-28 00:01:00"),
			},
		},
		{
			start: "2020-02-29 00:01:00",
			end:   "2021-03-01 00:00:00",
			step:  "1 year",
			res: []types.Datetime{
				transStr2Datetime("2020-02-29 00:01:00"),
				transStr2Datetime("2021-02-28 00:01:00"),
			},
		},
		{
			start: "2020-02-28 00:01:00",
			end:   "2021-03-01 00:00:00",
			step:  "1 year",
			res: []types.Datetime{
				transStr2Datetime("2020-02-28 00:01:00"),
				transStr2Datetime("2021-02-28 00:01:00"),
			},
		},
	}

	proc := testutil.NewProc()
	for _, kase := range kases {
		var err error
		var gs genDatetimeState

		var scale int32
		p1, p2 := getScale(kase.start), getScale(kase.end)
		if p1 > p2 {
			scale = p1
		} else {
			scale = p2
		}

		start, err := types.ParseDatetime(kase.start, scale)
		require.Nil(t, err)
		end, err := types.ParseDatetime(kase.end, scale)
		require.Nil(t, err)

		startVec, _ := vector.NewConstFixed[types.Datetime](types.T_datetime.ToType(), start, 1, proc.Mp())
		endVec, _ := vector.NewConstFixed[types.Datetime](types.T_datetime.ToType(), end, 1, proc.Mp())
		stepVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte(kase.step), 1, proc.Mp())

		err = initDateTimeStep(&gs, proc, stepVec, 0)
		if err != nil {
			require.True(t, kase.err)
			continue
		}
		err = initStartAndEndDatetime(&gs, startVec, endVec, 0)
		if err != nil {
			require.True(t, kase.err)
			continue
		}

		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_datetime.ToType())
		var res []types.Datetime

		for {
			bat.CleanOnlyData()
			buildNextDatetimeBatch(&gs, bat, 3, proc)
			if bat.RowCount() == 0 {
				break
			}
			vec := bat.GetVector(0)
			for i := 0; i < bat.RowCount(); i++ {
				val := vector.GetFixedAtWithTypeCheck[types.Datetime](vec, i)
				res = append(res, val)
			}
		}
		if len(kase.res) == 0 {
			require.True(t, len(res) == 0)
		} else {
			require.Equal(t, kase.res, res)
		}
	}
}

func transStr2Datetime(s string) types.Datetime {
	scale := getScale(s)
	t, err := types.ParseDatetime(s, scale)
	if err != nil {
		logutil.Errorf("parse timestamp '%s' failed", s)
	}
	return t
}

func getScale(s string) int32 {
	var scale int32
	ss := strings.Split(s, ".")
	if len(ss) > 1 {
		scale = int32(len(ss[1]))
	}
	return scale
}
