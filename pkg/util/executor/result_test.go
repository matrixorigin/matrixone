// Copyright 2023 Matrix Origin
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

package executor

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadRows(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	memRes := NewMemResult(
		[]types.Type{types.New(types.T_int32, 0, 0), types.New(types.T_varchar, 2, 0)},
		mp)
	memRes.NewBatch()
	require.NoError(t, AppendFixedRows(memRes, 0, []int32{1, 2, 3, 4}))
	require.NoError(t, AppendStringRows(memRes, 1, []string{"s1", "s2", "s3", "s4"}))

	memRes.NewBatch()
	require.NoError(t, AppendFixedRows(memRes, 0, []int32{5, 6, 7, 8}))
	require.NoError(t, AppendStringRows(memRes, 1, []string{"s5", "s6", "s7", "s8"}))

	res := memRes.GetResult()
	defer res.Close()

	var col1 []int32
	var col2 [][]byte
	var cols2WithString []string
	res.ReadRows(func(_ int, cols []*vector.Vector) bool {
		col1 = append(col1, GetFixedRows[int32](cols[0])...)
		col2 = append(col2, GetBytesRows(cols[1])...)
		cols2WithString = append(cols2WithString, GetStringRows(cols[1])...)
		return true
	})
	assert.Equal(t, []int32{1, 2, 3, 4, 5, 6, 7, 8}, col1)
	assert.Equal(t, [][]byte{[]byte("s1"), []byte("s2"), []byte("s3"), []byte("s4"), []byte("s5"), []byte("s6"), []byte("s7"), []byte("s8")}, col2)
	assert.Equal(t, []string{"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"}, cols2WithString)
}
