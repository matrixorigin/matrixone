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

package partition

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

// Latest timestamp, rather than latest non-NULL metric, defines the winner.
// The last input row replaces a non-NULL retained payload across both an input
// batch and a hash chunk boundary. Reuse also checks NULL -> non-NULL cleanup.
func TestPartitionTopOneRunsLatestNullPayload(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
	t.Cleanup(proc.Free)
	arg := topOneRunArgument(true, true)
	arg.OrderBySpecs = arg.OrderBySpecs[:2] // metric payload is not an ORDER key
	t.Cleanup(func() { arg.Free(proc, false, nil) })
	for _, latestNull := range []bool{true, false} {
		child := colexec.NewMockOperator()
		t.Cleanup(func() { child.Free(proc, false, nil) })
		arg.Children = nil
		arg.AppendChild(child)
		rows := make([]topOneRunRow, 514)
		for i := range rows {
			rows[i] = topOneRunRow{group: 1, score: int64(i), secondary: int64(i + 10), id: int64(i)}
		}
		rows[513].secondaryNull = latestNull
		if latestNull {
			rows[513].secondary = 0
		}
		child.WithBatchs([]*batch.Batch{
			topOneRunBatch(t, proc, rows[:256]),
			topOneRunBatch(t, proc, rows[256:]),
		})
		require.NoError(t, arg.Prepare(proc))
		result, err := arg.Call(proc)
		require.NoError(t, err)
		require.NotNil(t, result.Batch)
		require.Equal(t, 1, result.Batch.RowCount())
		require.Equal(t, int64(513), vector.GetFixedAtWithTypeCheck[int64](result.Batch.Vecs[1], 0))
		require.Equal(t, int64(513), vector.GetFixedAtWithTypeCheck[int64](result.Batch.Vecs[3], 0))
		require.Equal(t, latestNull, result.Batch.Vecs[2].IsNull(0))
		if !latestNull {
			require.Equal(t, int64(523), vector.GetFixedAtWithTypeCheck[int64](result.Batch.Vecs[2], 0))
		}
		result, err = arg.Call(proc)
		require.NoError(t, err)
		require.True(t, result.Batch == nil || result.Status == vm.ExecStop)
		arg.Reset(proc, false, nil)
		child.Free(proc, false, nil)
	}
}
