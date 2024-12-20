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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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
