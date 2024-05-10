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
