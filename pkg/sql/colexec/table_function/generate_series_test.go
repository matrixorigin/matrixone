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
	"bytes"
	"context"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
	kases := []Kase[int32]{
		{
			start: 1,
			end:   10,
			step:  1,
			res:   []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			start: 1,
			end:   10,
			step:  2,
			res:   []int32{1, 3, 5, 7, 9},
		},
		{
			start: 1,
			end:   10,
			step:  -1,
			res:   []int32{},
		},
		{
			start: 10,
			end:   1,
			step:  -1,
			res:   []int32{10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		},
		{
			start: 10,
			end:   1,
			step:  -2,
			res:   []int32{10, 8, 6, 4, 2},
		},
		{
			start: 10,
			end:   1,
			step:  1,
			res:   []int32{},
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
			res:   []int32{},
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
			res:   []int32{1},
		},
		{
			start: math.MaxInt32 - 1,
			end:   math.MaxInt32,
			step:  1,
			res:   []int32{math.MaxInt32 - 1, math.MaxInt32},
		},
		{
			start: math.MaxInt32,
			end:   math.MaxInt32 - 1,
			step:  -1,
			res:   []int32{math.MaxInt32, math.MaxInt32 - 1},
		},
		{
			start: math.MinInt32,
			end:   math.MinInt32 + 100,
			step:  19,
			res:   []int32{math.MinInt32, math.MinInt32 + 19, math.MinInt32 + 38, math.MinInt32 + 57, math.MinInt32 + 76, math.MinInt32 + 95},
		},
		{
			start: math.MinInt32 + 100,
			end:   math.MinInt32,
			step:  -19,
			res:   []int32{math.MinInt32 + 100, math.MinInt32 + 81, math.MinInt32 + 62, math.MinInt32 + 43, math.MinInt32 + 24, math.MinInt32 + 5},
		},
	}
	for _, kase := range kases {
		res, err := generateInt32(context.TODO(), kase.start, kase.end, kase.step)
		if kase.err {
			require.NotNil(t, err)
			continue
		}
		require.Nil(t, err)
		require.Equal(t, kase.res, res)
	}
}

func TestDoGenerateInt64(t *testing.T) {
	kases := []Kase[int64]{
		{
			start: 1,
			end:   10,
			step:  1,
			res:   []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			start: 1,
			end:   10,
			step:  2,
			res:   []int64{1, 3, 5, 7, 9},
		},
		{
			start: 1,
			end:   10,
			step:  -1,
			res:   []int64{},
		},
		{
			start: 10,
			end:   1,
			step:  -1,
			res:   []int64{10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		},
		{
			start: 10,
			end:   1,
			step:  -2,
			res:   []int64{10, 8, 6, 4, 2},
		},
		{
			start: 10,
			end:   1,
			step:  1,
			res:   []int64{},
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
			res:   []int64{},
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
			res:   []int64{1},
		},
		{
			start: math.MaxInt32 - 1,
			end:   math.MaxInt32,
			step:  1,
			res:   []int64{math.MaxInt32 - 1, math.MaxInt32},
		},
		{
			start: math.MaxInt32,
			end:   math.MaxInt32 - 1,
			step:  -1,
			res:   []int64{math.MaxInt32, math.MaxInt32 - 1},
		},
		{
			start: math.MinInt32,
			end:   math.MinInt32 + 100,
			step:  19,
			res:   []int64{math.MinInt32, math.MinInt32 + 19, math.MinInt32 + 38, math.MinInt32 + 57, math.MinInt32 + 76, math.MinInt32 + 95},
		},
		{
			start: math.MinInt32 + 100,
			end:   math.MinInt32,
			step:  -19,
			res:   []int64{math.MinInt32 + 100, math.MinInt32 + 81, math.MinInt32 + 62, math.MinInt32 + 43, math.MinInt32 + 24, math.MinInt32 + 5},
		},
		// int64
		{
			start: math.MaxInt32,
			end:   math.MaxInt32 + 100,
			step:  19,
			res:   []int64{math.MaxInt32, math.MaxInt32 + 19, math.MaxInt32 + 38, math.MaxInt32 + 57, math.MaxInt32 + 76, math.MaxInt32 + 95},
		},
		{
			start: math.MaxInt32 + 100,
			end:   math.MaxInt32,
			step:  -19,
			res:   []int64{math.MaxInt32 + 100, math.MaxInt32 + 81, math.MaxInt32 + 62, math.MaxInt32 + 43, math.MaxInt32 + 24, math.MaxInt32 + 5},
		},
		{
			start: math.MinInt32,
			end:   math.MinInt32 - 100,
			step:  -19,
			res:   []int64{math.MinInt32, math.MinInt32 - 19, math.MinInt32 - 38, math.MinInt32 - 57, math.MinInt32 - 76, math.MinInt32 - 95},
		},
		{
			start: math.MinInt32 - 100,
			end:   math.MinInt32,
			step:  19,
			res:   []int64{math.MinInt32 - 100, math.MinInt32 - 81, math.MinInt32 - 62, math.MinInt32 - 43, math.MinInt32 - 24, math.MinInt32 - 5},
		},
		{
			start: math.MaxInt64 - 1,
			end:   math.MaxInt64,
			step:  1,
			res:   []int64{math.MaxInt64 - 1, math.MaxInt64},
		},
		{
			start: math.MaxInt64,
			end:   math.MaxInt64 - 1,
			step:  -1,
			res:   []int64{math.MaxInt64, math.MaxInt64 - 1},
		},
		{
			start: math.MaxInt64 - 100,
			end:   math.MaxInt64,
			step:  19,
			res:   []int64{math.MaxInt64 - 100, math.MaxInt64 - 81, math.MaxInt64 - 62, math.MaxInt64 - 43, math.MaxInt64 - 24, math.MaxInt64 - 5},
		},
		{
			start: math.MaxInt64,
			end:   math.MaxInt64 - 100,
			step:  -19,
			res:   []int64{math.MaxInt64, math.MaxInt64 - 19, math.MaxInt64 - 38, math.MaxInt64 - 57, math.MaxInt64 - 76, math.MaxInt64 - 95},
		},
		{
			start: math.MinInt64,
			end:   math.MinInt64 + 100,
			step:  19,
			res:   []int64{math.MinInt64, math.MinInt64 + 19, math.MinInt64 + 38, math.MinInt64 + 57, math.MinInt64 + 76, math.MinInt64 + 95},
		},
		{
			start: math.MinInt64 + 100,
			end:   math.MinInt64,
			step:  -19,
			res:   []int64{math.MinInt64 + 100, math.MinInt64 + 81, math.MinInt64 + 62, math.MinInt64 + 43, math.MinInt64 + 24, math.MinInt64 + 5},
		},
	}
	for _, kase := range kases {
		res, err := generateInt64(context.TODO(), kase.start, kase.end, kase.step)
		if kase.err {
			require.NotNil(t, err)
			continue
		}
		require.Nil(t, err)
		require.Equal(t, kase.res, res)
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
	for _, kase := range kases {
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
		res, err := generateDatetime(context.TODO(), start, end, kase.step)
		if kase.err {
			require.NotNil(t, err)
			continue
		}
		require.Nil(t, err)
		require.Equal(t, kase.res, res)
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

func TestGenerateSeriesString(t *testing.T) {
	generateSeriesString(new(bytes.Buffer))
}

func TestGenerateSeriesPrepare(t *testing.T) {
	err := generateSeriesPrepare(nil, &Argument{
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			}}})
	require.Nil(t, err)
}
func TestGenStep(t *testing.T) {
	ctx := context.TODO()
	kase := "10 hour"
	num, tp, err := genStep(ctx, kase)
	require.Nil(t, err)
	require.Equal(t, int64(10), num)
	require.Equal(t, types.Hour, tp)
	kase = "10 houx"
	_, _, err = genStep(ctx, kase)
	require.NotNil(t, err)
	kase = "hour"
	_, _, err = genStep(ctx, kase)
	require.NotNil(t, err)
	kase = "989829829129131939147193 hour"
	_, _, err = genStep(ctx, kase)
	require.NotNil(t, err)
}

func TestGenerateSeriesCall(t *testing.T) {
	proc := testutil.NewProc()
	beforeCall := proc.Mp().CurrNB()
	arg := &Argument{
		Attrs:    []string{"result"},
		FuncName: "generate_series",
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			},
		},
	}

	arg.Args = makeInt64List(1, 3, 1)
	arg.Rets = plan.GSColDefs[0]
	err := arg.Prepare(proc)
	require.Nil(t, err)
	bat := makeGenerateSeriesBatch(proc)
	result := vm.NewCallResult()
	result.Batch = bat
	end, err := generateSeriesCall(0, proc, arg, &result)
	require.Nil(t, err)
	require.Equal(t, false, end)
	require.Equal(t, 3, result.Batch.GetVector(0).Length())
	cleanResult(&result, proc)

	arg.Args = makeDatetimeList("2020-01-01 00:00:00", "2020-01-01 00:00:59", "1 second", 0)
	arg.Rets = plan.GSColDefs[1]

	err = arg.Prepare(proc)
	require.Nil(t, err)

	result = vm.NewCallResult()
	result.Batch = bat
	end, err = generateSeriesCall(0, proc, arg, &result)
	require.Nil(t, err)
	require.Equal(t, false, end)
	require.Equal(t, 60, result.Batch.GetVector(0).Length())
	cleanResult(&result, proc)

	arg.Args = makeVarcharList("2020-01-01 00:00:00", "2020-01-01 00:00:59", "1 second")
	arg.Rets = plan.GSColDefs[2]

	err = arg.Prepare(proc)
	require.Nil(t, err)

	result = vm.NewCallResult()
	result.Batch = bat
	end, err = generateSeriesCall(0, proc, arg, &result)
	require.Nil(t, err)
	require.Equal(t, false, end)
	require.Equal(t, 60, result.Batch.GetVector(0).Length())
	cleanResult(&result, proc)

	bat.Clean(proc.Mp())
	require.Equal(t, beforeCall, proc.Mp().CurrNB())

}

func makeGenerateSeriesBatch(proc *process.Process) *batch.Batch {
	bat := batch.NewWithSize(1)
	bat.Vecs[0], _ = vector.NewConstFixed(types.T_int64.ToType(), int64(0), 1, proc.Mp())
	bat.SetRowCount(1)
	return bat
}

func makeInt64List(start, end, step int64) []*plan.Expr {
	ret := make([]*plan.Expr, 3)
	ret[0] = makeInt64Expr(start)
	ret[1] = makeInt64Expr(end)
	ret[2] = makeInt64Expr(step)
	return ret
}

func makeDatetimeList(start, end, step string, scale int32) []*plan.Expr {
	ret := make([]*plan.Expr, 3)
	ret[0] = makeDatetimeExpr(start, scale)
	ret[1] = makeDatetimeExpr(end, scale)
	ret[2] = makeVarcharExpr(step)
	return ret
}
func makeVarcharList(start, end, step string) []*plan.Expr {
	ret := make([]*plan.Expr, 3)
	ret[0] = makeVarcharExpr(start)
	ret[1] = makeVarcharExpr(end)
	ret[2] = makeVarcharExpr(step)
	return ret
}

func makeInt64Expr(val int64) *plan.Expr {
	return &plan.Expr{
		Typ: &plan.Type{
			Id: int32(types.T_int64),
		},
		Expr: &plan2.Expr_Lit{
			Lit: &plan.Const{
				Value: &plan2.Literal_I64Val{
					I64Val: val,
				},
			},
		},
	}
}
func makeVarcharExpr(val string) *plan.Expr {
	return &plan.Expr{
		Typ: &plan.Type{
			Id: int32(types.T_varchar),
		},
		Expr: &plan2.Expr_Lit{
			Lit: &plan.Const{
				Value: &plan2.Literal_Sval{
					Sval: val,
				},
			},
		},
	}
}

func makeDatetimeExpr(s string, p int32) *plan.Expr {
	dt, _ := types.ParseDatetime(s, p)
	return &plan.Expr{
		Typ: &plan.Type{
			Id:    int32(types.T_datetime),
			Scale: p,
		},
		Expr: &plan2.Expr_Lit{
			Lit: &plan.Const{
				Value: &plan2.Literal_Datetimeval{
					Datetimeval: int64(dt),
				},
			},
		},
	}
}
