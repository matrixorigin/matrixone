// Copyright 2025 Matrix Origin
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

package group

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// newTestProcess creates a new process with a temporary file service for spilling.
func newTestProcess(t *testing.T) *process.Process {
	mp := mpool.MustNewZero()
	fs := testutil.NewFS()

	proc := testutil.NewProcess(
		testutil.WithMPool(mp),
		testutil.WithFileService(fs),
	)
	proc.Ctx = context.Background()
	return proc
}

// createTestBatch generates a batch with random data for testing.
func createTestBatch(numRows int, ts []types.Type, mp *mpool.MPool) *batch.Batch {
	bat := batch.NewWithSize(len(ts))
	bat.Attrs = make([]string, len(ts))
	for i, typ := range ts {
		bat.Attrs[i] = fmt.Sprintf("col%d", i)
		switch typ.Oid {
		case types.T_int64:
			vec := vector.NewVec(typ)
			for j := 0; j < numRows; j++ {
				_ = vector.AppendFixed(vec, int64(rand.Intn(100)), false, mp)
			}
			bat.Vecs[i] = vec
		case types.T_varchar:
			vec := vector.NewVec(typ)
			for j := 0; j < numRows; j++ {
				_ = vector.AppendBytes(vec, []byte(fmt.Sprintf("str%d", rand.Intn(10))), false, mp)
			}
			bat.Vecs[i] = vec
		default:
			panic(fmt.Sprintf("unsupported type for test batch: %s", typ.String()))
		}
	}
	bat.SetRowCount(numRows)
	return bat
}

// createGroupOperator creates a group operator with specified expressions and aggregations.
func createGroupOperator(t *testing.T, exprs []*plan.Expr, aggs []aggexec.AggFuncExecExpression, needEval bool) *Group {
	op := NewArgument()
	op.Exprs = exprs
	op.Aggs = aggs
	op.NeedEval = needEval
	op.PreAllocSize = 0 // No pre-allocation for spill tests
	return op
}

// compareBatches compares two batches for deep equality.
func compareBatches(t *testing.T, expected, actual *batch.Batch) {
	require.Equal(t, expected.RowCount(), actual.RowCount(), "row count mismatch")
	require.Equal(t, len(expected.Vecs), len(actual.Vecs), "vector count mismatch")
	require.Equal(t, len(expected.Attrs), len(actual.Attrs), "attribute count mismatch")

	for i := range expected.Attrs {
		require.Equal(t, expected.Attrs[i], actual.Attrs[i], "attribute name mismatch at index %d", i)
	}

	for i := range expected.Vecs {
		expectedVec := expected.Vecs[i]
		actualVec := actual.Vecs[i]
		require.True(t, testutil.CompareVectors(expectedVec, actualVec), "vector content mismatch at index %d", i)
	}
}

// compareAggExecs compares two slices of AggFuncExec by marshaling their states.
func compareAggExecs(t *testing.T, expected, actual []aggexec.AggFuncExec) {
	require.Equal(t, len(expected), len(actual), "aggregator count mismatch")
	for i := range expected {
		expectedBytes, err := aggexec.MarshalAggFuncExec(expected[i])
		require.NoError(t, err)
		actualBytes, err := aggexec.MarshalAggFuncExec(actual[i])
		require.NoError(t, err)
		require.Equal(t, expectedBytes, actualBytes, "aggregator state mismatch at index %d", i)
	}
}

func TestGroupOperatorBasicSpillAndRecall(t *testing.T) {
	//TODO
}
