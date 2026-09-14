// Copyright 2026 Matrix Origin
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

package mergeorder

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestSortBatchSpillsAndPreservesOrder(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := newValuesBatch(proc, []int8{7, 1, 5, 1, 3, 9})
	analyzer := process.NewAnalyzer(0, false, false, "window-sort-spill")
	fs := []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8)}}

	got, err := SortBatch(proc, input, fs, 1, analyzer)
	require.NoError(t, err)
	require.Equal(t, []int8{1, 1, 3, 5, 7, 9}, vector.MustFixedColWithTypeCheck[int8](got.Vecs[0]))
	require.Positive(t, analyzer.GetOpStats().SpillRows)
	require.Positive(t, analyzer.GetOpStats().SpillSize)

	got.Clean(proc.Mp())
	input.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestSortBatchFreesSingleBatchExpressionKey(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := newValuesBatch(proc, []int8{3, 1, 2})
	key := testutil.NewVector(3, types.T_int8.ToType(), proc.Mp(), false, []int8{1, 2, 3})
	ctr := &container{
		batchList: []*batch.Batch{input},
		orderCols: [][]*vector.Vector{{key}},
	}
	analyzer := process.NewAnalyzer(0, false, false, "single-batch-expression-key")
	fs := []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8)}}

	got, err := ctr.collectSortedBatch(proc, fs, analyzer)
	require.NoError(t, err)
	got.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}
