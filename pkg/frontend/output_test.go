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
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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

func TestExtractRowFromVector2(t *testing.T) {
	mp := mpool.MustNewZero()

	colSlices := &ColumnSlices{
		colIdx2SliceIdx: make([]int, 1),
	}

	t.Run("A", func(t *testing.T) {
		var (
			vecf32 [][]float32
			vec    = vector.NewVec(types.T_array_float32.ToType())
		)

		for i := 0; i < 10; i++ {
			vecf32 = append(vecf32, make([]float32, 10))
			for j := 0; j < 10; j++ {
				vecf32[i][j] = rand.Float32()
			}
			bb := types.ArrayToBytes[float32](vecf32[i])
			err := vector.AppendAny(vec, bb, false, mp)
			require.NoError(t, err)
		}

		err := convertVectorToSlice(context.TODO(), nil, vec, 0, colSlices)
		require.NoError(t, err)
		row := make([]any, 1)

		for i := range 10 {
			err = extractRowFromVector2(context.Background(), nil, vec, 0, row, i, false, colSlices)
			require.NoError(t, err)

			require.Equal(t, row[0].([]float32), vecf32[i])

			err = extractRowFromVector2(context.Background(), nil, vec, 0, row, i, true, colSlices)
			require.NoError(t, err)

			require.Equal(t, row[0].([]float32), vecf32[i])
		}
	})

	t.Run("B", func(t *testing.T) {
		var (
			vecf64 [][]float64
			vec    = vector.NewVec(types.T_array_float64.ToType())
		)

		for i := 0; i < 10; i++ {
			vecf64 = append(vecf64, make([]float64, 10))
			for j := 0; j < 10; j++ {
				vecf64[i][j] = rand.Float64()
			}
			bb := types.ArrayToBytes[float64](vecf64[i])
			err := vector.AppendAny(vec, bb, false, mp)
			require.NoError(t, err)
		}

		err := convertVectorToSlice(context.TODO(), nil, vec, 0, colSlices)
		require.NoError(t, err)
		row := make([]any, 1)

		for i := range 10 {
			err = extractRowFromVector2(context.Background(), nil, vec, 0, row, i, false, colSlices)
			require.NoError(t, err)

			require.Equal(t, row[0].([]float64), vecf64[i])

			err = extractRowFromVector2(context.Background(), nil, vec, 0, row, i, true, colSlices)
			require.NoError(t, err)

			require.Equal(t, row[0].([]float64), vecf64[i])
		}
	})

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
